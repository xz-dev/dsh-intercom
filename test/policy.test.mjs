import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventEmitter } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const home = mkdtempSync(join(tmpdir(), 'dsh-intercom-policy-'));
process.env.DSH_HOME = home;
const [{ IntercomBroker }, { default: plugin }] = await Promise.all([import('../lib/broker.js'), import('../index.js')]);

for (const [policy, replyTo, expected] of [
  ['always', undefined, 'followup'],
  ['replies', undefined, 'send'],
  ['replies', 'original', 'followup'],
  ['never', 'original', 'send'],
]) {
  test(`idle ${policy} policy ${replyTo ? 'reply' : 'ordinary'} routes via ${expected}`, async () => {
    const broker = new IntercomBroker();
    broker.start();
    await new Promise(resolve => broker.server.once('listening', resolve));
    const events = new EventEmitter();
    const deliveries = [];
    const agent = {
      id: `agent-${policy}-${replyTo ?? 'ordinary'}`, status: 'idle', options: { model: 'test' },
      session: { id: 's1', header: { cwd: home }, snapshotEvents: () => [] },
      followup: () => deliveries.push(['followup']), steer: () => deliveries.push(['steer']),
      send: (_msg, target, wakeup) => deliveries.push(['send', target, wakeup]),
    };
    const ctx = { on: (name, fn) => events.on(name, fn), get: () => null, tools: { register() {} } };
    plugin.apply(ctx, { inboundTrigger: policy });
    try {
      events.emit('agent/created', { agent });
      for (let n = 0; n < 80 && broker.sessions.size === 0; n++) await new Promise(r => setTimeout(r, 20));
      assert.equal(broker.sessions.size, 1);
      const from = { id: 'sender', name: 'Sender', cwd: home };
      const message = { id: crypto.randomUUID(), timestamp: Date.now(), content: { text: 'test' }, ...(replyTo ? { replyTo } : {}) };
      const target = broker.sessions.values().next().value;
      // Deliver over real framed broker socket, exercising registered client's message handler.
      const { writeMessage } = await import('../lib/framing.js');
      writeMessage(target.socket, { type: 'message', from: { ...from, model: 'test', pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, message });
      for (let n = 0; n < 80 && deliveries.length === 0; n++) await new Promise(r => setTimeout(r, 20));
      assert.equal(deliveries[0][0], expected);
      if (expected === 'send') assert.deepEqual(deliveries[0], ['send', 'next-step', false]);
    } finally {
      events.emit('agent/disposed', { agent });
      await new Promise(r => setTimeout(r, 30));
      broker.shutdown();
    }
  });
}

test.after(() => rmSync(home, { recursive: true, force: true }));
