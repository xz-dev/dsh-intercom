/**
 * Shared protocol constants and runtime paths for dsh-intercom.
 *
 * Frame format and message shapes mirror pi-intercom's broker protocol, but
 * the broker lives under the DSH home ($DSH_HOME/intercom), so DSH sessions
 * never share a broker with pi sessions (pi uses ~/.pi/agent/intercom).
 */
import { chmodSync, mkdirSync, realpathSync } from "fs";
import { join, resolve } from "path";
import { homedir } from "os";
import { resolveDshHome } from "@deepseek-ai/dsh-home-paths";

export const INTERCOM_DIR_MODE = 0o700;
export const INTERCOM_RUNTIME_FILE_MODE = 0o600;
export const INTERCOM_PROTOCOL_NAME = "dsh-intercom";
export const INTERCOM_PROTOCOL_VERSION = 1;

/** Default ask timeout, matching pi-intercom's DEFAULT_ASK_TIMEOUT_MS. */
export const DEFAULT_ASK_TIMEOUT_MS = 10 * 60 * 1000;

export function getAskTimeoutMs(env = process.env) {
  const raw = env.DSH_INTERCOM_ASK_TIMEOUT_MS ?? env.PI_INTERCOM_ASK_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") {
    return DEFAULT_ASK_TIMEOUT_MS;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error("DSH_INTERCOM_ASK_TIMEOUT_MS must be a positive integer number of milliseconds");
  }
  return value;
}

export function getIntercomDirPath(env = process.env) {
  return join(resolveDshHome(undefined, env), "intercom");
}

export function getBrokerSocketPath(env = process.env) {
  return join(getIntercomDirPath(env), "broker.sock");
}

export function getBrokerPidPath(env = process.env) {
  return join(getIntercomDirPath(env), "broker.pid");
}

export function ensureIntercomRuntimeDir(intercomDir) {
  mkdirSync(intercomDir, { recursive: true, mode: INTERCOM_DIR_MODE });
  chmodSync(intercomDir, INTERCOM_DIR_MODE);
}

export function restrictIntercomRuntimeFile(filePath) {
  chmodSync(filePath, INTERCOM_RUNTIME_FILE_MODE);
}

/** Normalize a cwd for same-directory comparison (resolve + realpath, memoized). */
const normalizeCache = new Map();
export function normalizeCwd(cwd) {
  const cached = normalizeCache.get(cwd);
  if (cached !== undefined) return cached;
  const resolved = resolve(cwd);
  let normalized;
  try {
    normalized = realpathSync(resolved);
  } catch {
    normalized = resolved;
  }
  normalizeCache.set(cwd, normalized);
  return normalized;
}

export function sameCwd(a, b) {
  return normalizeCwd(a) === normalizeCwd(b);
}
