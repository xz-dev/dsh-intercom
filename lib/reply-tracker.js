/**
 * Reply tracker: pending inbound asks, turn context, and reply targeting.
 * Faithful port of pi-intercom's reply-tracker.ts.
 */
import { getAskTimeoutMs } from "./shared.js";

function matchesPendingSender(context, to) {
  if (context.from.id === to) return true;
  return context.from.name?.toLowerCase() === to.toLowerCase();
}

export class ReplyTracker {
  constructor(askTimeoutMs = getAskTimeoutMs()) {
    this.askTimeoutMs = askTimeoutMs;
    this.pendingAsks = new Map();
    this.pendingTurnContexts = [];
    this.currentTurnContext = null;
  }

  recordIncomingMessage(from, message, receivedAt = Date.now()) {
    const context = { from, message, receivedAt };
    if (message.expectsReply) {
      this.pendingAsks.set(message.id, context);
    }
    return context;
  }

  queueTurnContext(context) {
    this.pendingTurnContexts.push(context);
  }

  beginTurn(now = Date.now()) {
    this.pruneExpired(now);
    this.currentTurnContext = this.pendingTurnContexts.shift() ?? null;
  }

  endTurn() {
    this.currentTurnContext = null;
  }

  reset() {
    this.pendingAsks.clear();
    this.pendingTurnContexts.length = 0;
    this.currentTurnContext = null;
  }

  resolveReplyTarget(options, now = Date.now()) {
    this.pruneExpired(now);

    if (options.replyTo) {
      const target = this.pendingAsks.get(options.replyTo);
      if (!target) {
        throw new Error(`No pending ask with message ID "${options.replyTo}"`);
      }
      if (options.to && !matchesPendingSender(target, options.to)) {
        throw new Error(`Pending ask "${options.replyTo}" is not from "${options.to}"`);
      }
      return target;
    }

    const pending = Array.from(this.pendingAsks.values());
    if (options.to) {
      const matches = pending.filter((context) => matchesPendingSender(context, options.to));
      if (matches.length === 1) return matches[0];
      if (matches.length > 1) {
        throw new Error(`Multiple pending asks from "${options.to}" — use the sender session ID instead.`);
      }
      throw new Error(`No pending ask from "${options.to}"`);
    }

    if (this.currentTurnContext) return this.currentTurnContext;

    if (pending.length === 1) return pending[0];
    if (pending.length === 0) throw new Error("No active intercom context to reply to");
    throw new Error("Multiple pending asks — specify `to`");
  }

  dismissPendingAsk(replyTo) {
    this.pendingAsks.delete(replyTo);
    for (let index = this.pendingTurnContexts.length - 1; index >= 0; index -= 1) {
      if (this.pendingTurnContexts[index]?.message.id === replyTo) {
        this.pendingTurnContexts.splice(index, 1);
      }
    }
    if (this.currentTurnContext?.message.id === replyTo) {
      this.currentTurnContext = null;
    }
  }

  listPending(now = Date.now()) {
    this.pruneExpired(now);
    return Array.from(this.pendingAsks.values()).sort((a, b) => a.receivedAt - b.receivedAt);
  }

  pruneExpired(now) {
    for (const [messageId, context] of this.pendingAsks) {
      if (now - context.receivedAt > this.askTimeoutMs) {
        this.dismissPendingAsk(messageId);
      }
    }
  }
}
