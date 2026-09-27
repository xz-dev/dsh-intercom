import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventEmitter } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const home = mkdtempSync(join(tmpdir(), 'dsh-intercom-test-'));
process.env.DSH_HOME = home;
const [{ IntercomBroker }, { default: plugin }, { IntercomClient }] = await Promise.all([
  import('../lib/broker.js'), import('../index.js'), import('../lib/client.js'),
]);
const wait = (ms = 40) => new Promise(resolve => setTimeout(resolve, ms));
const eventually = async (fn) => {
  for (let n = 0; n < 100; n++) {
    try { return await fn(); } catch { await wait(); }
  }
  return fn();
};

test('two local DSH agents: discovery, named delivery, busy steer, ask/reply, pending, cancel', async () => {
  const broker = new IntercomBroker();
  broker.start();
  await new Promise(resolve => broker.server.once('listening', resolve));
  const events = new EventEmitter();
  let tool;
  const names = new Map();
  const ctx = {
    on: (event, fn) => events.on(event, fn),
    tools: { register: definition => { tool = definition; } },
    get: key => key === 'sessionTitle' ? { get: session => ({ title: names.get(session.id) }) } : null,
  };
  plugin.apply(ctx, { enabled: true, inboundTrigger: 'always', replyHint: true, askTimeoutMs: 1000 });
  const agents = ['alice', 'bob'].map(id => ({
    id, status: 'idle', options: { model: 'test-model' },
    session: { id, header: { cwd: home }, snapshotEvents: () => [] },
    followups: [], steers: [], parked: [],
    followup(msg) { this.followups.push(msg); }, steer(msg) { this.steers.push(msg); },
    send(msg, target, wakeup) { this.parked.push({ msg, target, wakeup }); },
  }));
  names.set('alice', 'Alice'); names.set('bob', 'Bob');
  const [alice, bob] = agents;
  const call = (agent, action, extra = {}, signal = new AbortController().signal) =>
    tool.execute({ action, ...extra }, { agent, signal, callId: crypto.randomUUID() });
  try {
    for (const agent of agents) events.emit('agent/created', { agent });
    await eventually(async () => assert.match((await call(alice, 'list')).text, /Bob/));
    assert.match((await call(alice, 'status')).text, /Active sessions: 2/);
    assert.match((await call(alice, 'list-cwd')).text, /Bob/);
    assert.doesNotMatch((await call(alice, 'list-cwd', { cwd: tmpdir() })).text, /\*\*Other sessions.*\n.*Bob/);

    const first = await call(alice, 'send', { to: 'Bob', message: 'Hello' });
    assert.equal(first.ok, true, first.text);
    await eventually(() => assert.equal(bob.followups.length, 1));
    assert.match(bob.followups[0].content[0].text, /Hello/);
    bob.status = 'running';
    const second = await call(alice, 'send', { to: 'Bob', message: 'Busy message' });
    assert.equal(second.ok, true, second.text);
    await eventually(() => assert.equal(bob.steers.length, 1));
    assert.equal(bob.followups.length, 1);
    assert.match(bob.steers[0].content[0].text, /Busy message/);

    const asking = call(alice, 'ask', { to: 'Bob', message: 'Can you review?' });
    await eventually(async () => assert.match((await call(bob, 'pending')).text, /Can you review\?/));
    const answer = await call(bob, 'reply', { message: 'Yes' });
    assert.equal(answer.ok, true, answer.text);
    assert.match((await asking).text, /Reply from Bob:[\s\S]*Yes/);
    assert.equal((await call(bob, 'pending')).text, 'No unresolved inbound asks.');
    assert.equal((await call(alice, 'send', { to: 'Alice', message: 'No' })).ok, false);
    assert.equal((await call(bob, 'reply', { message: 'no pending ask' })).ok, false);

    names.set('bob', 'Renamed Bob');
    events.emit('session/event', bob.session, { type: 'session/title', data: { title: 'Renamed Bob' } });
    await eventually(async () => assert.match((await call(alice, 'list')).text, /Renamed Bob/));
    const received = [];
    const listener = (_, m) => received.push(m);
    const observer = new IntercomClient();
    observer.on('message', listener);
    await observer.connect({ name: 'Observer', cwd: home, model: 'test', pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, 'observer');
    const probe = new IntercomClient();
    await probe.connect({ name: 'Probe', cwd: home, model: 'test', pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, 'probe');
    const outgoing = await probe.send('Observer', { text: 'Cancel me' });
    assert.equal(outgoing.delivered, true);
    const cancelled = await probe.cancelMessage(outgoing.id);
    assert.equal(cancelled.delivered, true);
    await eventually(() => assert.equal(received.length, 1));
    await probe.disconnect(); await observer.disconnect();
  } finally {
    for (const agent of agents) events.emit('agent/disposed', { agent });
    await wait(80);
    broker.shutdown();
    rmSync(home, { recursive: true, force: true });
  }
});
