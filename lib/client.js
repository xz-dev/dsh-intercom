/**
 * Intercom client: length-prefixed JSON over the broker unix socket.
 * Behaviour port of pi-intercom's broker/client.ts (register, list, send,
 * cancel, presence, receipts/controls events, reconnect-safe semantics).
 */
import { EventEmitter } from "events";
import net from "net";
import { randomUUID } from "crypto";
import { writeMessage, createMessageReader } from "./framing.js";
import { getBrokerSocketPath } from "./shared.js";

function toError(error) {
  return error instanceof Error ? error : new Error(String(error));
}

function isAttachment(value) {
  if (typeof value !== "object" || value === null) return false;
  if (value.type !== "file" && value.type !== "snippet" && value.type !== "context") return false;
  if (typeof value.name !== "string" || typeof value.content !== "string") return false;
  return value.language === undefined || typeof value.language === "string";
}

function isMessage(value) {
  if (typeof value !== "object" || value === null) return false;
  if (typeof value.id !== "string" || typeof value.timestamp !== "number") return false;
  if (typeof value.content !== "object" || value.content === null) return false;
  const content = value.content;
  if (typeof content.text !== "string") return false;
  return content.attachments === undefined
    || (Array.isArray(content.attachments) && content.attachments.every(isAttachment));
}

function isSessionInfo(value) {
  if (typeof value !== "object" || value === null) return false;
  return typeof value.id === "string"
    && typeof value.cwd === "string"
    && typeof value.model === "string"
    && typeof value.pid === "number"
    && typeof value.startedAt === "number"
    && typeof value.lastActivity === "number";
}

function isMessageReceipt(value) {
  if (typeof value !== "object" || value === null) return false;
  return typeof value.messageId === "string" && typeof value.timestamp === "number";
}

function isMessageControl(value) {
  if (typeof value !== "object" || value === null) return false;
  return typeof value.messageId === "string" && typeof value.timestamp === "number"
    && (value.action === "cancel" || value.action === "supersede");
}

export class IntercomClient extends EventEmitter {
  constructor() {
    super();
    this.socket = null;
    this._sessionId = null;
    this._features = new Set();
    this.pendingSends = new Map();
    this.pendingLists = new Map();
    this.pendingCancels = new Map();
    this.nextSenderSequence = 1;
    this.disconnecting = false;
    this.disconnectError = null;
  }

  failPending(error) {
    for (const pending of this.pendingSends.values()) pending.reject(error);
    this.pendingSends.clear();
    for (const pending of this.pendingLists.values()) pending.reject(error);
    this.pendingLists.clear();
    for (const pending of this.pendingCancels.values()) pending.reject(error);
    this.pendingCancels.clear();
  }

  get sessionId() {
    return this._sessionId;
  }

  supportsFeature(feature) {
    return this._features.has(feature);
  }

  isConnected() {
    const socket = this.socket;
    return Boolean(socket && this._sessionId && !this.disconnecting && !socket.destroyed && !socket.writableEnded && socket.writable);
  }

  requireActiveSocket() {
    if (this.disconnecting) throw new Error("Client disconnecting");
    const socket = this.socket;
    if (!socket || !this._sessionId) throw new Error("Not connected");
    if (socket.destroyed || socket.writableEnded || !socket.writable) throw new Error("Client disconnected");
    return socket;
  }

  connect(session, sessionId) {
    if (this.socket) return Promise.reject(new Error("Already connected"));
    return new Promise((resolve, reject) => {
      let socket;
      try {
        socket = net.connect(getBrokerSocketPath());
      } catch (error) {
        reject(toError(error));
        return;
      }
      this.socket = socket;
      this.disconnectError = null;
      let settled = false;
      const timeout = setTimeout(() => {
        if (!this._sessionId) {
          cleanupConnectionAttempt();
          cleanupSocketListeners();
          if (this.socket === socket) this.socket = null;
          socket.destroy();
          reject(new Error("Connection timeout"));
        }
      }, 10000);

      let connectionEstablished = false;

      const onRegistered = () => {
        settled = true;
        connectionEstablished = true;
        cleanupConnectionAttempt();
        resolve();
      };

      const onError = (err) => {
        settled = true;
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        if (this.socket === socket) this.socket = null;
        socket.destroy();
        reject(err);
      };

      const onClose = () => {
        const wasConnecting = !settled && !this._sessionId;
        const wasDisconnecting = this.disconnecting;
        const disconnectError = this.disconnectError ?? new Error("Client disconnected");
        this.disconnecting = false;
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        this.failPending(disconnectError);
        if (this.socket === socket) this.socket = null;
        this._sessionId = null;
        this._features.clear();
        this.disconnectError = null;
        if (connectionEstablished && !wasDisconnecting) {
          this.emit("disconnected", disconnectError);
        }
        if (wasConnecting) {
          reject(new Error("Connection closed before registration"));
        }
      };

      const onSocketError = (err) => {
        if (connectionEstablished) {
          this.disconnectError = err;
          this.emit("error", err);
        }
      };

      const onReaderError = (error) => {
        const protocolError = new Error(`Intercom protocol error: ${error.message}`);
        if (!connectionEstablished) {
          onError(protocolError);
          return;
        }
        this.disconnectError = protocolError;
        this.emit("error", protocolError);
        socket.destroy();
      };

      const reader = createMessageReader((msg) => {
        this.handleBrokerMessage(msg);
      }, onReaderError);

      const cleanupConnectionAttempt = () => {
        this.off("_registered", onRegistered);
        socket.off("error", onError);
        clearTimeout(timeout);
      };

      const cleanupSocketListeners = () => {
        socket.off("data", reader);
        socket.off("error", onSocketError);
        socket.off("close", onClose);
      };

      socket.on("data", reader);
      socket.on("error", onError);
      socket.on("close", onClose);
      socket.on("error", onSocketError);
      this.once("_registered", onRegistered);

      try {
        writeMessage(socket, {
          type: "register",
          session,
          ...(sessionId ? { sessionId } : {}),
        });
      } catch (error) {
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        if (this.socket === socket) this.socket = null;
        socket.destroy();
        reject(toError(error));
      }
    });
  }

  handleBrokerMessage(msg) {
    if (typeof msg !== "object" || msg === null || !("type" in msg) || typeof msg.type !== "string") {
      throw new Error("Invalid broker message");
    }
    const brokerMessage = msg;

    if (this._sessionId === null && brokerMessage.type !== "registered" && brokerMessage.type !== "error") {
      throw new Error(`Received ${brokerMessage.type} before registered`);
    }

    switch (brokerMessage.type) {
      case "registered": {
        if (typeof brokerMessage.sessionId !== "string") throw new Error("Invalid registered message");
        if (this._sessionId !== null) throw new Error("Received duplicate registered message");
        if (
          brokerMessage.features !== undefined
          && (!Array.isArray(brokerMessage.features) || !brokerMessage.features.every((feature) => typeof feature === "string"))
        ) {
          throw new Error("Invalid registered features");
        }
        this._sessionId = brokerMessage.sessionId;
        this._features = new Set(brokerMessage.features ?? []);
        const registered = {
          type: "registered",
          sessionId: brokerMessage.sessionId,
          ...(this._features.size > 0 ? { features: [...this._features] } : {}),
        };
        this.emit("broker_message", registered);
        this.emit("_registered", registered);
        break;
      }

      case "sessions": {
        const { requestId, sessions } = brokerMessage;
        if (typeof requestId !== "string" || !Array.isArray(sessions) || !sessions.every(isSessionInfo)) {
          throw new Error("Invalid sessions message");
        }
        const pending = this.pendingLists.get(requestId);
        if (!pending) return; // Late list response after caller timeout.
        this.pendingLists.delete(requestId);
        pending.resolve(sessions);
        break;
      }

      case "message": {
        const { from, message } = brokerMessage;
        if (!isSessionInfo(from) || !isMessage(message)) throw new Error("Invalid message event");
        this.emit("message", from, message);
        break;
      }

      case "delivered": {
        const { messageId } = brokerMessage;
        if (typeof messageId !== "string") throw new Error("Invalid delivered message");
        for (const store of [this.pendingSends, this.pendingCancels]) {
          const pending = store.get(messageId);
          if (!pending) continue;
          store.delete(messageId);
          pending.resolve({ id: messageId, delivered: true });
          return;
        }
        break;
      }

      case "delivery_failed": {
        const { messageId, reason } = brokerMessage;
        if (typeof messageId !== "string" || typeof reason !== "string") throw new Error("Invalid delivery_failed message");
        for (const store of [this.pendingSends, this.pendingCancels]) {
          const pending = store.get(messageId);
          if (!pending) continue;
          store.delete(messageId);
          pending.resolve({ id: messageId, delivered: false, reason });
          return;
        }
        break;
      }

      case "message_receipt": {
        if (!isSessionInfo(brokerMessage.from) || !isMessageReceipt(brokerMessage.receipt)) {
          throw new Error("Invalid message_receipt event");
        }
        this.emit("message_receipt", brokerMessage.from, brokerMessage.receipt);
        break;
      }

      case "message_control": {
        if (!isSessionInfo(brokerMessage.from) || !isMessageControl(brokerMessage.control)) {
          throw new Error("Invalid message_control event");
        }
        this.emit("message_control", brokerMessage.from, brokerMessage.control);
        break;
      }

      case "session_joined": {
        if (!isSessionInfo(brokerMessage.session)) throw new Error("Invalid session_joined message");
        this.emit("session_joined", brokerMessage.session);
        break;
      }

      case "session_left": {
        if (typeof brokerMessage.sessionId !== "string") throw new Error("Invalid session_left message");
        this.emit("session_left", brokerMessage.sessionId);
        break;
      }

      case "presence_update": {
        if (!isSessionInfo(brokerMessage.session)) throw new Error("Invalid presence_update message");
        this.emit("presence_update", brokerMessage.session);
        break;
      }

      case "error": {
        if (typeof brokerMessage.error !== "string") throw new Error("Invalid error message");
        if (this._sessionId === null) throw new Error(brokerMessage.error);
        this.emit("error", new Error(brokerMessage.error));
        break;
      }

      default:
        throw new Error(`Unknown broker message type: ${brokerMessage.type}`);
    }
  }

  async disconnect() {
    const socket = this.socket;
    if (!socket) return;
    this.disconnecting = true;
    this.disconnectError = null;
    this.failPending(new Error("Client disconnected"));
    await new Promise((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        socket.off("close", onClose);
        socket.off("error", onError);
        resolve();
      };
      const onClose = () => finish();
      const onError = () => socket.destroy();
      const timeout = setTimeout(() => socket.destroy(), 2000);
      socket.once("close", onClose);
      socket.once("error", onError);
      try {
        writeMessage(socket, { type: "unregister" });
        socket.end();
      } catch {
        socket.destroy();
      }
    });
  }

  listSessions() {
    let socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return Promise.reject(toError(error));
    }
    return new Promise((resolve, reject) => {
      const requestId = randomUUID();
      const wrappedResolve = (sessions) => {
        clearTimeout(timeout);
        resolve(sessions);
      };
      const wrappedReject = (error) => {
        clearTimeout(timeout);
        reject(error);
      };
      const timeout = setTimeout(() => {
        if (this.pendingLists.has(requestId)) {
          this.pendingLists.delete(requestId);
          wrappedReject(new Error("List sessions timeout"));
        }
      }, 5000);
      this.pendingLists.set(requestId, { resolve: wrappedResolve, reject: wrappedReject });
      try {
        writeMessage(socket, { type: "list", requestId });
      } catch (error) {
        clearTimeout(timeout);
        this.pendingLists.delete(requestId);
        reject(toError(error));
      }
    });
  }

  send(to, options) {
    let socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return Promise.reject(toError(error));
    }
    const messageId = options.messageId ?? randomUUID();
    const message = {
      id: messageId,
      timestamp: Date.now(),
      senderSequence: this.nextSenderSequence++,
      supersedes: options.supersedes,
      retryOf: options.retryOf,
      replyTo: options.replyTo,
      expectsReply: options.expectsReply,
      content: {
        text: options.text,
        attachments: options.attachments,
      },
    };
    return new Promise((resolve, reject) => {
      const wrappedResolve = (result) => {
        clearTimeout(timeout);
        resolve(result);
      };
      const wrappedReject = (error) => {
        clearTimeout(timeout);
        reject(error);
      };
      const timeout = setTimeout(() => {
        if (this.pendingSends.has(messageId)) {
          this.pendingSends.delete(messageId);
          wrappedReject(new Error("Send timeout"));
        }
      }, 10000);
      this.pendingSends.set(messageId, { resolve: wrappedResolve, reject: wrappedReject });
      try {
        writeMessage(socket, { type: "send", to, message });
      } catch (error) {
        clearTimeout(timeout);
        this.pendingSends.delete(messageId);
        reject(toError(error));
      }
    });
  }

  cancelMessage(messageId) {
    let socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return Promise.reject(toError(error));
    }
    return new Promise((resolve, reject) => {
      const wrappedResolve = (result) => {
        clearTimeout(timeout);
        resolve(result);
      };
      const wrappedReject = (error) => {
        clearTimeout(timeout);
        reject(error);
      };
      const timeout = setTimeout(() => {
        if (this.pendingCancels.has(messageId)) {
          this.pendingCancels.delete(messageId);
          wrappedReject(new Error("Cancel timeout"));
        }
      }, 10000);
      this.pendingCancels.set(messageId, { resolve: wrappedResolve, reject: wrappedReject });
      try {
        writeMessage(socket, { type: "cancel_message", messageId });
      } catch (error) {
        clearTimeout(timeout);
        this.pendingCancels.delete(messageId);
        reject(toError(error));
      }
    });
  }

  sendMessageReceipt(receipt) {
    if (this.disconnecting) return;
    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) return;
    try {
      writeMessage(socket, { type: "message_receipt", receipt });
    } catch {
      // Receipts are best-effort diagnostics.
    }
  }

  cancelAsk(messageId) {
    if (this.disconnecting) return;
    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) return;
    try {
      writeMessage(socket, { type: "cancel_ask", messageId });
    } catch {
      // Cancellation is best-effort; local waiter cleanup still proceeds.
    }
  }

  updatePresence(updates) {
    if (this.disconnecting) return;
    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) return;
    try {
      writeMessage(socket, { type: "presence", ...updates });
    } catch {
      // Presence is best-effort.
    }
  }

  onBrokerMessage(handler) {
    this.on("broker_message", handler);
    return () => this.off("broker_message", handler);
  }

  onMessageReceipt(handler) {
    this.on("message_receipt", handler);
    return () => this.off("message_receipt", handler);
  }

  onMessageControl(handler) {
    this.on("message_control", handler);
    return () => this.off("message_control", handler);
  }
}
