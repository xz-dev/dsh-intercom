import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { IntercomBroker } from '../lib/broker.js';
import { IntercomClient } from '../lib/client.js';
import { ReplyTracker } from '../lib/reply-tracker.js';
import { createMessageReader, writeMessage } from '../lib/framing.js';

const home = mkdtempSync(join(tmpdir(), 'dsh-intercom-protocol-'));
process.env.DSH_HOME = home;
const registration = name => ({ name, cwd: home, model: 'test', pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() });

// Broker is shared across connections; tests deliberately serial to avoid socket collisions.
test('broker targets exact name/ID, refuses ambiguous names and forged replies, routes cancellation', async () => {
  const broker = new IntercomBroker();
  broker.start();
  await new Promise(resolve => broker.server.once('listening', resolve));
  const peers = ['sender', 'recipient', 'duplicate'].map(() => new IntercomClient());
  const [sender, recipient, duplicate] = peers;
  const messages = [], controls = [], receipts = [];
  recipient.on('message', (_, message) => messages.push(message));
  recipient.on('message_control', (_, control) => controls.push(control));
  sender.on('message_receipt', (_, receipt) => receipts.push(receipt));
  try {
    await sender.connect(registration('Sender'), 'sender');
    await recipient.connect(registration('Recipient'), 'recipient');
    await duplicate.connect(registration('Recipient'), 'duplicate');
    assert.match((await sender.send('Recipient', { text: 'ambiguous' })).reason, /Multiple sessions/);
    const sent = await sender.send('recip', { text: 'hello' });
    assert.equal(sent.delivered, true);
    assert.equal(messages.length, 1);
    assert.equal(messages[0].content.text, 'hello');
    assert.match((await recipient.send('sender', { text: 'forged', replyTo: 'missing-id' })).reason, /Reply target/);
    assert.equal((await sender.cancelMessage(sent.id)).delivered, true);
    assert.equal(controls[0].action, 'cancel');
    assert.equal(receipts[0].status, 'cancellation_requested');
    assert.equal((await duplicate.cancelMessage(sent.id)).delivered, false);
    assert.equal((await sender.send('recipient', { text: 'supersedes', supersedes: sent.id })).delivered, true);
    assert.equal(controls[1].action, 'supersede');
  } finally {
    for (const peer of peers) await peer.disconnect();
    broker.shutdown();
    rmSync(home, { recursive: true, force: true });
  }
});

test('reply tracker: active turn, single pending, ambiguous pending and explicit targeting', () => {
  const tracker = new ReplyTracker(1000);
  const from = { id: 'a', name: 'Alice' };
  tracker.recordIncomingMessage(from, { id: 'm1', expectsReply: true, content: { text: 'one' } }, 100);
  assert.equal(tracker.resolveReplyTarget({}, 101).message.id, 'm1');
  tracker.recordIncomingMessage(from, { id: 'm2', expectsReply: true, content: { text: 'two' } }, 102);
  assert.throws(() => tracker.resolveReplyTarget({}, 103), /Multiple pending asks/);
  assert.equal(tracker.resolveReplyTarget({ replyTo: 'm2' }, 103).message.id, 'm2');
  tracker.dismissPendingAsk('m1');
  assert.equal(tracker.listPending(103).length, 1);
  tracker.pruneExpired(1200);
  assert.equal(tracker.listPending(1200).length, 0);
});

test('framing: fragmented messages and oversized frame fail closed', () => {
  const chunks = [], seen = [], errors = [];
  writeMessage({ write: data => chunks.push(data) }, { type: 'hello' });
  const reader = createMessageReader(message => seen.push(message), error => errors.push(error), 100);
  reader(chunks[0].subarray(0, 3));
  reader(chunks[0].subarray(3));
  assert.deepEqual(seen, [{ type: 'hello' }]);
  const oversized = Buffer.alloc(4); oversized.writeUInt32BE(101);
  reader(oversized);
  assert.match(errors[0].message, /exceeds maximum/);
});
