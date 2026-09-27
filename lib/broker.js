/**
 * dsh-intercom broker: a standalone daemon over a unix socket inside the
 * DSH home. Behaviour port of pi-intercom's broker for the daily subset:
 * session registry with stable-id re-registration, presence broadcasts,
 * point-to-point send with name/id/prefix resolution, ask edges (expectsReply
 * + replyTo validation + mutual-ask refusal), offline mailbox keyed by
 * name+cwd, message receipts, cancel/supersede controls, idle shutdown.
 *
 * The extension bus is deliberately NOT advertised: `registered` carries no
 * features, so pi-intercom peers (which gate on supportsFeature) never send
 * extension operations to this broker.
 */
import net from "net";
import { writeFileSync, unlinkSync, existsSync, readFileSync } from "fs";
import { join } from "path";
import { randomUUID } from "crypto";
import { writeMessage, createMessageReader } from "./framing.js";
import {
  ensureIntercomRuntimeDir,
  getBrokerSocketPath,
  getIntercomDirPath,
  getAskTimeoutMs,
  restrictIntercomRuntimeFile,
  sameCwd,
} from "./shared.js";

const MAX_SESSIONS = 128;
const MAX_UNREGISTERED_CONNECTIONS = 32;
const REGISTRATION_TIMEOUT_MS = 1000;
const RATE_LIMIT_CAPACITY = 240;
const RATE_LIMIT_REFILL_PER_SECOND = 120;
const PRESENCE_HEARTBEAT_MS = 1000;
const MESSAGE_RECEIPT_ROUTE_RETENTION_MS = 60 * 60 * 1000;
const DISCONNECTED_SESSION_RETENTION_MS = 24 * 60 * 60 * 1000;
const MAILBOX_MESSAGE_RETENTION_MS = 24 * 60 * 60 * 1000;
const MAX_MAILBOX_MESSAGES = 256;

function isAttachment(value) {
  if (typeof value !== "object" || value === null) return false;
  if (value.type !== "file" && value.type !== "snippet" && value.type !== "context") return false;
  if (typeof value.name !== "string" || typeof value.content !== "string") return false;
  return value.language === undefined || typeof value.language === "string";
}

export function isMessage(value) {
  if (typeof value !== "object" || value === null) return false;
  if (typeof value.id !== "string" || typeof value.timestamp !== "number") return false;
  for (const key of ["senderSequence", "brokerReceivedAt", "brokerDeliveredAt", "receiverReceivedAt", "injectedAt"]) {
    if (value[key] !== undefined && typeof value[key] !== "number") return false;
  }
  if (value.supersedes !== undefined && typeof value.supersedes !== "string") return false;
  if (value.retryOf !== undefined && typeof value.retryOf !== "string") return false;
  if (value.replyTo !== undefined && typeof value.replyTo !== "string") return false;
  if (value.expectsReply !== undefined && typeof value.expectsReply !== "boolean") return false;
  if (typeof value.content !== "object" || value.content === null) return false;
  const content = value.content;
  if (typeof content.text !== "string") return false;
  return content.attachments === undefined
    || (Array.isArray(content.attachments) && content.attachments.every(isAttachment));
}

export function isSessionRegistration(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const session = value;
  if (
    typeof session.cwd !== "string"
    || typeof session.model !== "string"
    || typeof session.pid !== "number"
    || typeof session.startedAt !== "number"
    || typeof session.lastActivity !== "number"
  ) {
    return false;
  }
  if (session.name !== undefined && typeof session.name !== "string") return false;
  return session.status === undefined || typeof session.status === "string";
}

export class IntercomBroker {
  constructor(env = process.env) {
    this.sessions = new Map();
    this.askEdges = new Map();
    this.messageReceiptRoutes = new Map();
    this.disconnectedSessions = new Map();
    this.mailboxMessages = [];
    this.connections = new Set();
    this.unregisteredConnections = new Set();
    this.shutdownTimer = null;
    this.nextOwnerOrder = 1;
    this.intercomDir = getIntercomDirPath(env);
    this.socketPath = getBrokerSocketPath(env);
    this.pidPath = join(this.intercomDir, "broker.pid");
    this.askTimeoutMs = getAskTimeoutMs(env);
    ensureIntercomRuntimeDir(this.intercomDir);
    assertNoLiveBroker(this.pidPath, this.socketPath);
    try {
      unlinkSync(this.socketPath);
    } catch {
      // No stale socket on a clean startup.
    }
    this.server = net.createServer(this.handleConnection.bind(this));
  }

  start() {
    this.server.listen(this.socketPath, () => {
      restrictIntercomRuntimeFile(this.socketPath);
      writeFileSync(this.pidPath, `${process.pid}\n`, { mode: 0o600 });
      restrictIntercomRuntimeFile(this.pidPath);
      process.stdout.write(`dsh-intercom broker started (pid: ${process.pid})\n`);
    });
    process.on("SIGTERM", () => this.shutdown());
    process.on("SIGINT", () => this.shutdown());
  }

  handleConnection(socket) {
    this.connections.add(socket);
    let sessionId = null;
    let registrationTimeout = null;
    const armRegistrationTimeout = () => {
      if (registrationTimeout) clearTimeout(registrationTimeout);
      this.unregisteredConnections.delete(socket);
      this.unregisteredConnections.add(socket);
      this.evictOldestUnregisteredConnections(socket);
      registrationTimeout = setTimeout(() => {
        if (!sessionId) socket.destroy();
      }, REGISTRATION_TIMEOUT_MS);
      registrationTimeout.unref?.();
    };
    const clearRegistrationTimeout = () => {
      if (registrationTimeout) {
        clearTimeout(registrationTimeout);
        registrationTimeout = null;
      }
      this.unregisteredConnections.delete(socket);
    };
    armRegistrationTimeout();
    const connection = {
      socket,
      tokens: RATE_LIMIT_CAPACITY,
      lastRefillAt: Date.now(),
    };

    const reader = createMessageReader((msg) => {
      if (!this.consumeToken(connection)) {
        writeMessage(socket, { type: "error", error: "Intercom broker rate limit exceeded" });
        socket.destroy(new Error("Intercom broker rate limit exceeded"));
        return;
      }
      this.handleMessage(socket, msg, sessionId, (id) => {
        sessionId = id;
        if (id) clearRegistrationTimeout();
        else armRegistrationTimeout();
      });
    }, (error) => {
      socket.destroy(error);
    });

    socket.on("data", reader);

    socket.on("close", () => {
      clearRegistrationTimeout();
      this.connections.delete(socket);
      if (sessionId) {
        const existing = this.sessions.get(sessionId);
        if (existing?.socket === socket) {
          this.rememberDisconnectedSession(existing.info);
          this.sessions.delete(sessionId);
          this.clearMessageReceiptRoutesForSession(sessionId);
          this.broadcast({ type: "session_left", sessionId }, sessionId);
          this.scheduleShutdownCheck();
        }
      }
    });

    socket.on("error", (error) => {
      process.stderr.write(`dsh-intercom broker socket error: ${String(error)}\n`);
    });
  }

  evictOldestUnregisteredConnections(currentSocket) {
    while (this.unregisteredConnections.size > MAX_UNREGISTERED_CONNECTIONS) {
      const [oldest] = this.unregisteredConnections;
      if (!oldest) return;
      if (oldest === currentSocket && this.unregisteredConnections.size === 1) return;
      this.unregisteredConnections.delete(oldest);
      oldest.destroy();
    }
  }

  consumeToken(connection, now = Date.now()) {
    const elapsedMs = now - connection.lastRefillAt;
    if (elapsedMs > 0) {
      connection.tokens = Math.min(
        RATE_LIMIT_CAPACITY,
        connection.tokens + elapsedMs * RATE_LIMIT_REFILL_PER_SECOND / 1000,
      );
      connection.lastRefillAt = now;
    }
    if (connection.tokens < 1) return false;
    connection.tokens -= 1;
    return true;
  }

  scheduleShutdownCheck() {
    if (this.shutdownTimer) return;
    this.shutdownTimer = setTimeout(() => {
      this.shutdownTimer = null;
      if (this.sessions.size === 0) this.shutdown();
    }, 5000);
  }

  handleMessage(socket, msg, currentId, setId) {
    if (typeof msg !== "object" || msg === null || !("type" in msg) || typeof msg.type !== "string") {
      throw new Error("Invalid client message");
    }
    const clientMessage = msg;

    switch (clientMessage.type) {
      case "hello": {
        // Liveness probe from spawn.js: any frame back proves a live broker.
        writeMessage(socket, { type: "hello_ok" });
        break;
      }

      case "register": {
        if (!isSessionRegistration(clientMessage.session)) {
          throw new Error("Invalid register message");
        }
        if (currentId) {
          throw new Error("Received duplicate register message");
        }
        let id = randomUUID();
        if (clientMessage.sessionId !== undefined) {
          if (typeof clientMessage.sessionId !== "string" || clientMessage.sessionId.trim().length === 0) {
            throw new Error("Invalid register sessionId");
          }
          id = clientMessage.sessionId;
        }
        const session = clientMessage.session;
        this.pruneDisconnectedSessions();
        this.pruneMailboxMessages();
        const previous = this.sessions.get(id);
        if (!previous && this.sessions.size >= MAX_SESSIONS) {
          writeMessage(socket, { type: "error", error: "Too many registered intercom sessions" });
          socket.destroy();
          break;
        }
        if (previous) {
          this.clearAskEdgesForSession(id);
          this.clearMessageReceiptRoutesForSession(id);
          previous.socket.end();
        }
        setId(id);
        const info = {
          id,
          ...(session.name !== undefined ? { name: session.name } : {}),
          cwd: session.cwd,
          model: session.model,
          pid: session.pid,
          startedAt: session.startedAt,
          lastActivity: session.lastActivity,
          ...(session.status !== undefined ? { status: session.status } : {}),
          trustedLocal: true,
        };
        const connectedSession = {
          socket,
          info,
          lastPresenceBroadcastAt: Date.now(),
          ownerOrder: previous?.ownerOrder ?? this.nextOwnerOrder++,
        };
        this.sessions.set(id, connectedSession);
        this.disconnectedSessions.delete(id);
        if (this.shutdownTimer) {
          clearTimeout(this.shutdownTimer);
          this.shutdownTimer = null;
        }
        // No features advertised: the extension bus is not served here.
        writeMessage(socket, { type: "registered", sessionId: id });
        this.broadcast({ type: "session_joined", session: info }, id);
        this.flushMailboxForSession(connectedSession);
        break;
      }

      case "unregister": {
        if (!currentId) {
          throw new Error("Received unregister before register");
        }
        const existing = this.sessions.get(currentId);
        if (existing?.socket === socket) {
          this.rememberDisconnectedSession(existing.info);
          this.sessions.delete(currentId);
          this.clearMessageReceiptRoutesForSession(currentId);
          this.broadcast({ type: "session_left", sessionId: currentId }, currentId);
          this.scheduleShutdownCheck();
        }
        setId(null);
        break;
      }

      case "list": {
        if (typeof clientMessage.requestId !== "string") {
          throw new Error("Invalid list message");
        }
        const sessions = Array.from(this.sessions.values()).map(s => s.info);
        writeMessage(socket, { type: "sessions", requestId: clientMessage.requestId, sessions });
        break;
      }

      case "send": {
        if (!currentId) {
          throw new Error("Received send before register");
        }
        const message = clientMessage.message;
        const messageId = isMessage(message) ? message.id : "unknown";
        if (typeof clientMessage.to !== "string" || !isMessage(message)) {
          writeMessage(socket, { type: "delivery_failed", messageId, reason: "Invalid message format" });
          break;
        }

        const brokerReceivedAt = Date.now();
        this.pruneAskEdges();
        this.pruneMessageReceiptRoutes(brokerReceivedAt);
        const replyEdge = message.replyTo ? this.askEdges.get(message.replyTo) : undefined;

        const targets = this.findSessions(clientMessage.to);
        if (targets.length === 1) {
          if (message.replyTo && !replyEdge) {
            writeMessage(socket, {
              type: "delivery_failed",
              messageId: message.id,
              reason: "Reply target does not match a pending ask",
            });
            break;
          }
          const fromSession = this.sessions.get(currentId);
          if (!fromSession || fromSession.socket !== socket) {
            writeMessage(socket, {
              type: "delivery_failed",
              messageId: message.id,
              reason: "Sender session not found",
            });
            break;
          }
          const target = targets[0];
          if (message.supersedes) {
            const supersededRoute = this.messageReceiptRoutes.get(message.supersedes);
            if (!supersededRoute || supersededRoute.from !== currentId || supersededRoute.to !== target.info.id) {
              writeMessage(socket, {
                type: "delivery_failed",
                messageId: message.id,
                reason: "Supersede target does not match a previous message from this sender to this receiver",
              });
              break;
            }
          }
          if (replyEdge && (replyEdge.to !== currentId || replyEdge.from !== target.info.id)) {
            writeMessage(socket, {
              type: "delivery_failed",
              messageId: message.id,
              reason: "Reply target does not match the pending ask",
            });
            break;
          }
          if (message.expectsReply) {
            const reverseEdge = Array.from(this.askEdges.entries())
              .find(([edgeMessageId, edge]) => edgeMessageId !== message.replyTo && edge.from === target.info.id && edge.to === currentId);
            if (reverseEdge) {
              writeMessage(socket, {
                type: "delivery_failed",
                messageId: message.id,
                reason: "Mutual ask refused: target session is already waiting for a reply from this session.",
              });
              break;
            }
            this.askEdges.set(message.id, { from: currentId, to: target.info.id, createdAt: Date.now() });
          }
          const deliveredMessage = {
            ...message,
            brokerReceivedAt,
            brokerDeliveredAt: Date.now(),
          };
          if (message.supersedes) {
            const control = {
              action: "supersede",
              messageId: message.supersedes,
              supersededBy: message.id,
              timestamp: Date.now(),
            };
            writeMessage(target.socket, { type: "message_control", from: fromSession.info, control });
          }
          writeMessage(target.socket, { type: "message", from: fromSession.info, message: deliveredMessage });
          if (message.replyTo) {
            this.askEdges.delete(message.replyTo);
          }
          this.messageReceiptRoutes.set(message.id, { from: currentId, to: target.info.id, createdAt: brokerReceivedAt });
          writeMessage(socket, { type: "delivered", messageId: message.id });
          break;
        }

        if (targets.length > 1) {
          writeMessage(socket, {
            type: "delivery_failed",
            messageId: message.id,
            reason: `Multiple sessions named "${clientMessage.to}" are connected. Use the session ID instead.`,
          });
          break;
        }

        const disconnectedTargets = this.findDisconnectedSessions(clientMessage.to);
        if (disconnectedTargets.length === 1) {
          if (message.replyTo && !replyEdge) {
            writeMessage(socket, {
              type: "delivery_failed",
              messageId: message.id,
              reason: "Reply target does not match a pending ask",
            });
            break;
          }
          const fromSession = this.sessions.get(currentId);
          if (!fromSession || fromSession.socket !== socket) {
            writeMessage(socket, {
              type: "delivery_failed",
              messageId: message.id,
              reason: "Sender session not found",
            });
            break;
          }
          const target = disconnectedTargets[0].info;
          if (message.supersedes) {
            writeMessage(socket, {
              type: "delivery_failed",
              messageId: message.id,
              reason: "Supersede target is not connected",
            });
            break;
          }
          if (replyEdge && (replyEdge.to !== currentId || replyEdge.from !== target.id)) {
            writeMessage(socket, {
              type: "delivery_failed",
              messageId: message.id,
              reason: "Reply target does not match the pending ask",
            });
            break;
          }
          const liveMailboxTarget = this.findUniqueLiveSessionForDisconnectedSession(target);
          const effectiveTargetId = liveMailboxTarget?.info.id ?? target.id;
          if (message.expectsReply) {
            const reverseEdge = Array.from(this.askEdges.entries())
              .find(([edgeMessageId, edge]) => edgeMessageId !== message.replyTo && edge.from === effectiveTargetId && edge.to === currentId);
            if (reverseEdge) {
              writeMessage(socket, {
                type: "delivery_failed",
                messageId: message.id,
                reason: "Mutual ask refused: target session is already waiting for a reply from this session.",
              });
              break;
            }
            this.askEdges.set(message.id, { from: currentId, to: effectiveTargetId, createdAt: Date.now() });
          }
          if (liveMailboxTarget) {
            const deliveredMessage = {
              ...message,
              brokerReceivedAt,
              brokerDeliveredAt: Date.now(),
            };
            writeMessage(liveMailboxTarget.socket, { type: "message", from: fromSession.info, message: deliveredMessage });
            this.messageReceiptRoutes.set(message.id, { from: currentId, to: liveMailboxTarget.info.id, createdAt: brokerReceivedAt });
          } else {
            this.queueMailboxMessage(fromSession.info, target, message, brokerReceivedAt);
          }
          if (message.replyTo) {
            this.askEdges.delete(message.replyTo);
          }
          writeMessage(socket, { type: "delivered", messageId: message.id });
          break;
        }

        if (disconnectedTargets.length > 1) {
          writeMessage(socket, {
            type: "delivery_failed",
            messageId: message.id,
            reason: `Multiple disconnected sessions named "${clientMessage.to}" are remembered. Use the session ID instead.`,
          });
          break;
        }

        writeMessage(socket, {
          type: "delivery_failed",
          messageId: message.id,
          reason: "Session not found",
        });
        break;
      }

      case "message_receipt": {
        if (!currentId) {
          throw new Error("Received message_receipt before register");
        }
        const receipt = clientMessage.receipt;
        if (
          typeof receipt !== "object" || receipt === null
          || typeof receipt.messageId !== "string"
          || typeof receipt.timestamp !== "number"
        ) {
          throw new Error("Invalid message_receipt message");
        }
        this.pruneMessageReceiptRoutes();
        const route = this.messageReceiptRoutes.get(receipt.messageId);
        const receiver = this.sessions.get(currentId);
        const sender = route ? this.sessions.get(route.from) : undefined;
        if (route?.to === currentId && receiver?.socket === socket && sender) {
          writeMessage(sender.socket, { type: "message_receipt", from: receiver.info, receipt });
        }
        break;
      }

      case "cancel_message": {
        if (!currentId) {
          throw new Error("Received cancel_message before register");
        }
        if (typeof clientMessage.messageId !== "string") {
          throw new Error("Invalid cancel_message message");
        }
        this.pruneMessageReceiptRoutes();
        this.pruneMailboxMessages();
        const sender = this.sessions.get(currentId);
        const queuedIndex = this.mailboxMessages.findIndex(
          entry => entry.message.id === clientMessage.messageId && entry.from.id === currentId,
        );
        if (queuedIndex >= 0 && sender?.socket === socket) {
          this.mailboxMessages.splice(queuedIndex, 1);
          const edge = this.askEdges.get(clientMessage.messageId);
          if (edge?.from === currentId) this.askEdges.delete(clientMessage.messageId);
          writeMessage(socket, { type: "delivered", messageId: clientMessage.messageId });
          break;
        }
        const route = this.messageReceiptRoutes.get(clientMessage.messageId);
        const receiver = route ? this.sessions.get(route.to) : undefined;
        if (route?.from !== currentId || sender?.socket !== socket || !receiver) {
          writeMessage(socket, {
            type: "delivery_failed",
            messageId: clientMessage.messageId,
            reason: "Message may not exist or may belong to another sender",
          });
          break;
        }
        const control = {
          messageId: clientMessage.messageId,
          action: "cancel",
          timestamp: Date.now(),
        };
        writeMessage(receiver.socket, { type: "message_control", from: sender.info, control });
        const receipt = {
          messageId: clientMessage.messageId,
          status: "cancellation_requested",
          timestamp: Date.now(),
        };
        writeMessage(socket, { type: "message_receipt", from: receiver.info, receipt });
        const edge = this.askEdges.get(clientMessage.messageId);
        if (edge?.from === currentId) this.askEdges.delete(clientMessage.messageId);
        writeMessage(socket, { type: "delivered", messageId: clientMessage.messageId });
        break;
      }

      case "cancel_ask": {
        if (!currentId) {
          throw new Error("Received cancel_ask before register");
        }
        if (typeof clientMessage.messageId !== "string") {
          throw new Error("Invalid cancel_ask message");
        }
        const session = this.sessions.get(currentId);
        const edge = this.askEdges.get(clientMessage.messageId);
        if (session?.socket === socket && edge?.from === currentId) {
          this.askEdges.delete(clientMessage.messageId);
        }
        break;
      }

      case "presence": {
        if (!currentId) {
          throw new Error("Received presence before register");
        }
        const session = this.sessions.get(currentId);
        if (session?.socket === socket) {
          let changed = false;
          if (clientMessage.name !== undefined) {
            if (typeof clientMessage.name !== "string") {
              throw new Error("Invalid presence name");
            }
            if (session.info.name !== clientMessage.name) {
              session.info.name = clientMessage.name;
              changed = true;
            }
          }
          if (clientMessage.status !== undefined) {
            if (typeof clientMessage.status !== "string") {
              throw new Error("Invalid presence status");
            }
            if (session.info.status !== clientMessage.status) {
              session.info.status = clientMessage.status;
              changed = true;
            }
          }
          if (clientMessage.model !== undefined) {
            if (typeof clientMessage.model !== "string") {
              throw new Error("Invalid presence model");
            }
            if (session.info.model !== clientMessage.model) {
              session.info.model = clientMessage.model;
              changed = true;
            }
          }
          for (const key of ["contextPct", "contextTokens", "contextWindow"]) {
            const value = clientMessage[key];
            if (value === undefined) continue;
            if (value === null) {
              if (session.info[key] !== undefined) {
                delete session.info[key];
                changed = true;
              }
            } else if (typeof value !== "number") {
              throw new Error(`Invalid presence ${key}`);
            } else if (session.info[key] !== value) {
              session.info[key] = value;
              changed = true;
            }
          }
          const now = Date.now();
          session.info.lastActivity = now;
          if (changed || now - session.lastPresenceBroadcastAt >= PRESENCE_HEARTBEAT_MS) {
            session.lastPresenceBroadcastAt = now;
            this.broadcast({ type: "presence_update", session: session.info }, currentId);
          }
        }
        break;
      }

      case "extension_publish":
      case "extension_state_commit":
      case "extension_capabilities_update": {
        // The extension bus is not served by this broker and not advertised
        // in `registered`. Well-formed peers gate on supportsFeature() and
        // never send these; tolerate any that still do.
        break;
      }

      default:
        throw new Error(`Unknown client message type: ${clientMessage.type}`);
    }
  }

  rememberDisconnectedSession(info, now = Date.now()) {
    this.disconnectedSessions.set(info.id, { info: { ...info }, disconnectedAt: now });
    this.pruneDisconnectedSessions(now);
  }

  pruneDisconnectedSessions(now = Date.now()) {
    for (const [sessionId, session] of this.disconnectedSessions) {
      if (now - session.disconnectedAt > DISCONNECTED_SESSION_RETENTION_MS) {
        this.disconnectedSessions.delete(sessionId);
      }
    }
  }

  pruneMailboxMessages(now = Date.now()) {
    for (let index = this.mailboxMessages.length - 1; index >= 0; index -= 1) {
      const entry = this.mailboxMessages[index];
      if (now - entry.queuedAt > MAILBOX_MESSAGE_RETENTION_MS) {
        if (entry.message.expectsReply) this.askEdges.delete(entry.message.id);
        this.messageReceiptRoutes.delete(entry.message.id);
        this.mailboxMessages.splice(index, 1);
      }
    }
  }

  queueMailboxMessage(from, target, message, brokerReceivedAt) {
    this.pruneMailboxMessages(brokerReceivedAt);
    while (this.mailboxMessages.length >= MAX_MAILBOX_MESSAGES) {
      const evicted = this.mailboxMessages.shift();
      if (!evicted) break;
      if (evicted.message.expectsReply) this.askEdges.delete(evicted.message.id);
      this.messageReceiptRoutes.delete(evicted.message.id);
    }
    this.mailboxMessages.push({
      from: { ...from },
      target: { ...target },
      message: { ...message, brokerReceivedAt },
      queuedAt: brokerReceivedAt,
    });
  }

  flushMailboxForSession(session, now = Date.now()) {
    this.pruneMailboxMessages(now);
    const sessionName = session.info.name?.toLowerCase();
    const uniqueMailboxIdentity = this.findLiveSessionsSharingMailboxIdentity(session.info).length === 1;

    for (let index = 0; index < this.mailboxMessages.length;) {
      const entry = this.mailboxMessages[index];
      const matchesId = entry.target.id === session.info.id;
      const matchesUniqueName = Boolean(
        uniqueMailboxIdentity
        && sessionName
        && entry.target.name?.toLowerCase() === sessionName
        && sameCwd(entry.target.cwd, session.info.cwd),
      );
      if (!matchesId && !matchesUniqueName) {
        index += 1;
        continue;
      }
      this.mailboxMessages.splice(index, 1);
      const edge = this.askEdges.get(entry.message.id);
      if (edge?.to === entry.target.id) {
        edge.to = session.info.id;
      }
      const deliveredMessage = {
        ...entry.message,
        brokerDeliveredAt: Date.now(),
      };
      writeMessage(session.socket, { type: "message", from: entry.from, message: deliveredMessage });
      this.messageReceiptRoutes.set(entry.message.id, {
        from: entry.from.id,
        to: session.info.id,
        createdAt: entry.message.brokerReceivedAt ?? entry.queuedAt,
      });
    }
  }

  pruneAskEdges(now = Date.now()) {
    for (const [messageId, edge] of this.askEdges) {
      if (now - edge.createdAt > this.askTimeoutMs) {
        this.askEdges.delete(messageId);
      }
    }
  }

  clearAskEdgesForSession(sessionId) {
    for (const [messageId, edge] of this.askEdges) {
      if (edge.from === sessionId || edge.to === sessionId) {
        this.askEdges.delete(messageId);
      }
    }
  }

  pruneMessageReceiptRoutes(now = Date.now()) {
    for (const [messageId, route] of this.messageReceiptRoutes) {
      if (now - route.createdAt > MESSAGE_RECEIPT_ROUTE_RETENTION_MS) {
        this.messageReceiptRoutes.delete(messageId);
      }
    }
  }

  clearMessageReceiptRoutesForSession(sessionId) {
    for (const [messageId, route] of this.messageReceiptRoutes) {
      if (route.from === sessionId || route.to === sessionId) {
        this.messageReceiptRoutes.delete(messageId);
      }
    }
  }

  findSessions(nameOrId) {
    const byId = this.sessions.get(nameOrId);
    if (byId) return [byId];
    const lowerName = nameOrId.toLowerCase();
    const byName = Array.from(this.sessions.values()).filter(session => session.info.name?.toLowerCase() === lowerName);
    if (byName.length > 0) return byName;
    return Array.from(this.sessions.entries())
      .filter(([id]) => id.startsWith(nameOrId))
      .map(([, session]) => session);
  }

  findDisconnectedSessions(nameOrId) {
    this.pruneDisconnectedSessions();
    const byId = this.disconnectedSessions.get(nameOrId);
    if (byId) return [byId];
    const lowerName = nameOrId.toLowerCase();
    const byName = Array.from(this.disconnectedSessions.values()).filter(session => session.info.name?.toLowerCase() === lowerName);
    if (byName.length > 0) return byName;
    return Array.from(this.disconnectedSessions.entries())
      .filter(([id]) => id.startsWith(nameOrId))
      .map(([, session]) => session);
  }

  findUniqueLiveSessionForDisconnectedSession(info) {
    const matches = this.findLiveSessionsSharingMailboxIdentity(info);
    return matches.length === 1 ? matches[0] : null;
  }

  /** Mailbox identity is name plus directory, never name alone (pi-intercom invariant). */
  findLiveSessionsSharingMailboxIdentity(info) {
    const lowerName = info.name?.toLowerCase();
    if (!lowerName) return [];
    return Array.from(this.sessions.values()).filter(session =>
      session.info.name?.toLowerCase() === lowerName && sameCwd(session.info.cwd, info.cwd),
    );
  }

  broadcast(msg, exclude) {
    for (const [id, session] of this.sessions) {
      if (id !== exclude) writeMessage(session.socket, msg);
    }
  }

  shutdown() {
    if (this.shutdownTimer) {
      clearTimeout(this.shutdownTimer);
      this.shutdownTimer = null;
    }
    for (const session of this.sessions.values()) {
      session.socket.end();
    }
    this.sessions.clear();
    this.askEdges.clear();
    this.messageReceiptRoutes.clear();
    this.disconnectedSessions.clear();
    this.mailboxMessages.length = 0;
    try {
      unlinkSync(this.socketPath);
    } catch {
      // The socket may already be gone.
    }
    try {
      unlinkSync(this.pidPath);
    } catch {
      // The PID file may already be gone.
    }
    this.server.close();
  }
}

/**
 * Refuse to stomp a live broker: a live PID plus a present socket file means
 * another broker almost certainly owns this socket (the spawn path holds the
 * spawn lock and probes real socket liveness before ever constructing a
 * second broker, so this guard only fires on manual double-starts).
 */
export function assertNoLiveBroker(pidPath, socketPath) {
  if (!existsSync(pidPath)) return;
  let pid;
  try {
    pid = parseInt(readFileSync(pidPath, "utf8").trim(), 10);
  } catch {
    return; // Unreadable PID file: proceed.
  }
  if (!Number.isFinite(pid)) return;
  try {
    process.kill(pid, 0);
  } catch {
    return; // Stale PID file: proceed.
  }
  if (socketPath && !existsSync(socketPath)) return; // Recycled PID, no socket.
  throw new Error(`Another intercom broker appears to be running (pid ${pid}); refusing to start a second one`);
}
