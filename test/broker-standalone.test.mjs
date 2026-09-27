import assert from "node:assert/strict";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, cpSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Regression: the broker is spawned detached with plain `node lib/broker-main.js`,
// where harness peers (e.g. @deepseek-ai/dsh-home-paths) do not resolve. Copy lib/
// to an isolated dir with no node_modules and require the broker graph to load.
test("broker entry loads with no node_modules (detached daemon)", () => {
  const dir = mkdtempSync(join(tmpdir(), "intercom-broker-"));
  try {
    cpSync(new URL("../lib", import.meta.url), join(dir, "lib"), { recursive: true });
    cpSync(new URL("../package.json", import.meta.url), join(dir, "package.json"));
    const r = spawnSync(process.execPath, ["--input-type=module", "-e",
      "await import('./lib/broker.js'); await import('./lib/shared.js'); console.log('LOADED')"],
      { cwd: dir, encoding: "utf8", env: { PATH: process.env.PATH } });
    assert.equal(r.status, 0, r.stderr);
    assert.match(r.stdout, /LOADED/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("resolveDshHome precedence matches dsh-home-paths", async () => {
  const { getIntercomDirPath } = await import("../lib/shared.js");
  assert.equal(getIntercomDirPath({ DSH_HOME: "/x/h" }), "/x/h/intercom");
  assert.match(getIntercomDirPath({}), /\/\.dsh\/intercom$/);
});
