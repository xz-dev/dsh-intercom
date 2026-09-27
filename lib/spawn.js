/**
 * Spawn the broker daemon if no live one owns the socket. Port of
 * pi-intercom's broker/spawn.ts (liveness probe + spawn lock + detached
 * spawn), simplified: the broker is plain ESM JavaScript shipped in this
 * package, so it runs with `process.execPath` directly — no tsx/npx
 * brokerCommand indirection.
 */
import { spawn } from "child_process";
import { existsSync, readFileSync, unlinkSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import net from "net";
import { createMessageReader, writeMessage } from "./framing.js";
import {
  getBrokerPidPath,
  getBrokerSocketPath,
  getIntercomDirPath,
  ensureIntercomRuntimeDir,
  INTERCOM_RUNTIME_FILE_MODE,
  INTERCOM_PROTOCOL_NAME,
  INTERCOM_PROTOCOL_VERSION,
  restrictIntercomRuntimeFile,
} from "./shared.js";

const INTERCOM_DIR = getIntercomDirPath();
const BROKER_PID = getBrokerPidPath();
const BROKER_SPAWN_LOCK = join(INTERCOM_DIR, "broker.spawn.lock");
const BROKER_ENTRY = join(dirname(fileURLToPath(import.meta.url)), "broker-main.js");

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function checkSocketConnectable() {
  return new Promise(resolve => {
    let settled = false;
    const finish = (ok) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try {
        socket.destroy();
      } catch {
        // Already destroyed.
      }
      resolve(ok);
    };
    const socket = net.connect(getBrokerSocketPath());
    // A broker answers the hello probe with a hello_ok frame; any frame
    // (even an error) proves a live intercom broker rather than a random
    // unix socket that happens to accept connections.
    const reader = createMessageReader(() => finish(true), () => finish(false));
    socket.on("connect", () => {
      try {
        writeMessage(socket, {
          type: "hello",
          protocol: INTERCOM_PROTOCOL_NAME,
          version: INTERCOM_PROTOCOL_VERSION,
        });
      } catch {
        finish(false);
      }
    });
    socket.on("data", reader);
    socket.on("error", () => finish(false));
    const timeout = setTimeout(() => finish(false), 1000);
    timeout.unref?.();
  });
}

async function isBrokerRunning() {
  if (await checkSocketConnectable()) return true;
  if (!existsSync(BROKER_PID)) return false;
  try {
    const pid = parseInt(readFileSync(BROKER_PID, "utf8").trim(), 10);
    if (!Number.isFinite(pid)) return false;
    process.kill(pid, 0);
    return checkSocketConnectable();
  } catch {
    return false;
  }
}

function acquireSpawnLock() {
  const maxRetries = 5;
  for (let attempt = 0; attempt < maxRetries; attempt += 1) {
    try {
      writeFileSync(BROKER_SPAWN_LOCK, `${process.pid}\n${Date.now()}\n`, {
        flag: "wx",
        mode: INTERCOM_RUNTIME_FILE_MODE,
      });
      restrictIntercomRuntimeFile(BROKER_SPAWN_LOCK);
      return true;
    } catch (error) {
      if (!(error instanceof Error) || error.code !== "EEXIST") {
        throw error;
      }
      if (isSpawnLockStale()) {
        try {
          unlinkSync(BROKER_SPAWN_LOCK);
        } catch {
          // Retry if another holder removed it first.
        }
        continue;
      }
      return false;
    }
  }
  return false;
}

function isSpawnLockStale() {
  if (!existsSync(BROKER_SPAWN_LOCK)) return false;
  try {
    const [pidLine = "", createdAtLine = "0"] = readFileSync(BROKER_SPAWN_LOCK, "utf8").trim().split("\n");
    const pid = Number.parseInt(pidLine, 10);
    const createdAt = Number.parseInt(createdAtLine, 10);
    const ageMs = Date.now() - createdAt;
    if (Number.isFinite(pid)) {
      try {
        process.kill(pid, 0);
      } catch {
        return true;
      }
    }
    return !Number.isFinite(createdAt) || ageMs > 10_000;
  } catch {
    return true;
  }
}

function releaseSpawnLock() {
  try {
    unlinkSync(BROKER_SPAWN_LOCK);
  } catch {
    // Another cleanup path may already have removed the lock.
  }
}

async function waitForBroker(timeoutMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await checkSocketConnectable()) return;
    await sleep(100);
  }
  throw new Error("Broker failed to start within timeout");
}

export async function spawnBrokerIfNeeded() {
  ensureIntercomRuntimeDir(INTERCOM_DIR);

  if (await isBrokerRunning()) return;

  const ownsLock = acquireSpawnLock();
  if (!ownsLock) {
    await waitForBroker();
    return;
  }

  try {
    if (await isBrokerRunning()) return;

    const child = spawn(process.execPath, [BROKER_ENTRY], {
      detached: true,
      stdio: "ignore",
      cwd: INTERCOM_DIR,
      env: { ...process.env },
      windowsHide: true,
    });
    child.unref();

    await new Promise((resolve, reject) => {
      const cleanup = () => {
        child.off("error", onError);
        child.off("exit", onExit);
      };
      const onError = (error) => {
        cleanup();
        reject(new Error(`Failed to spawn intercom broker: ${error.message}`));
      };
      const onExit = (code, signal) => {
        cleanup();
        if (signal) {
          reject(new Error(`Intercom broker exited before startup with signal ${signal}`));
          return;
        }
        reject(new Error(`Intercom broker exited before startup with code ${code ?? "unknown"}`));
      };
      child.once("error", onError);
      child.once("exit", onExit);
      waitForBroker().then(() => {
        cleanup();
        resolve();
      }, (error) => {
        cleanup();
        reject(error instanceof Error ? error : new Error(String(error)));
      });
    });
  } finally {
    releaseSpawnLock();
  }
}
