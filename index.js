/**
 * dsh-intercom: cross-session messaging for DSH agents on one machine.
 *
 * Behaviour port of pi-intercom (fork at xz-dev/pi-intercom, 0.9.x line) onto
 * the DSH 0.1.7 plugin seams:
 *  - the `intercom` tool (list / list-cwd / send / ask / reply / pending /
 *    status / cancel) via `ctx.tools.register`
 *  - the local broker daemon under `$DSH_HOME/intercom` (spawned on demand)
 *  - inbound messages delivered as user-role messages via `agent.followup`
 *    (pi's triggerTurn) or `agent.steer` (pi's deliverAs:"steer"), chosen by
 *    the inboundTrigger policy, exactly like pi
 *  - presence (name / model / status / context usage) derived from agent
 *    events, the sessionTitle service, and the tokenMeter service
 *
 * One plugin instance serves ALL live agents in the process: each agent gets
 * its own broker registration keyed by its DSH session id, its own reply
 * tracker, and its own inbound queue. Subagent-origin agents are skipped
 * (pi's subagent orchestrator routing is a documented gap, see BEHAVIOR.md).
 */
import { randomUUID } from "crypto";
import { resolve as resolvePath } from "path";
import z from "@deepseek-ai/schemastery";
import { createUserMessage } from "@deepseek-ai/dsh-llm";
import { defineTool } from "@deepseek-ai/dsh-tools";
import { IntercomClient } from "./lib/client.js";
import { spawnBrokerIfNeeded } from "./lib/spawn.js";
import { ReplyTracker } from "./lib/reply-tracker.js";
import { DEFAULT_ASK_TIMEOUT_MS, getAskTimeoutMs, sameCwd } from "./lib/shared.js";
import {
  firstText,
  formatAttachments,
  formatInboundDeliveryMetadata,
  formatSessionListRow,
  getErrorMessage,
  shortSessionId,
} from "./lib/format.js";

export const name = "dsh-intercom";
export const inject = ["tools"];

export const Config = z.object({
  /** Master switch; false keeps the tool registered but fails every action. */
  enabled: z.boolean().default(true),
  /**
   * Which inbound broker messages may automatically trigger a model turn:
   * "always" (every message), "replies" (only replies to our asks), "never"
   * (everything is delivered as steering, consumed at the next step boundary
   * or when the session next wakes). Matches pi-intercom's inboundTrigger.
   */
  inboundTrigger: z.union([z.const("always"), z.const("replies"), z.const("never")]).default("always"),
  /** Append a reply hint to incoming asks. Matches pi-intercom's replyHint. */
  replyHint: z.boolean().default(true),
  /** Optional custom status suffix shown after the automatic lifecycle status. */
  status: z.string().default(""),
  /** Ask-waiter timeout in ms; env DSH_INTERCOM_ASK_TIMEOUT_MS overrides. */
  askTimeoutMs: z.number().default(DEFAULT_ASK_TIMEOUT_MS),
});

const INBOUND_MESSAGE_DEDUPE_MAX = 1000;
const INBOUND_MESSAGE_DEDUPE_RETENTION_MS = 60 * 60 * 1000;
const RECONNECT_BACKOFF_MS = [1000, 2000, 5000, 10000, 30000];
const CONTEXT_USAGE_CACHE_MS = 5000;
const UNNAMED_SESSION_ALIAS_PREFIX = "subagent-chat";

function readSessionTitle(ctx, agent) {
  try {
    const titles = ctx.get("sessionTitle");
    if (!titles) return undefined;
    const snapshot = titles.get(agent.session);
    const title = snapshot?.title;
    return typeof title === "string" && title.trim() ? title.trim() : undefined;
  } catch {
    return undefined;
  }
}

function presenceNameFor(ctx, agent) {
  const title = readSessionTitle(ctx, agent);
  if (title) return title;
  const normalizedId = agent.id.startsWith("session-") ? agent.id.slice("session-".length) : agent.id;
  return `${UNNAMED_SESSION_ALIAS_PREFIX}-${normalizedId.slice(0, 8)}`;
}

function agentCwd(agent) {
  return agent.session.header.cwd || process.cwd();
}

function agentModel(agent) {
  return agent.options?.model || "unknown";
}

/** Latest advertised context window from the session's request/context events. */
function readContextWindow(agent) {
  try {
    if (typeof agent.session.snapshotEvents !== "function") return undefined;
    const events = agent.session.snapshotEvents();
    for (let index = events.length - 1; index >= 0; index -= 1) {
      const event = events[index];
      if (event?.type === "request/context" && typeof event.data?.contextWindow === "number") {
        return event.data.contextWindow;
      }
    }
  } catch {
    // Best-effort presence metadata.
  }
  return undefined;
}

function apply(ctx, config) {
  const cfg = config ?? {};
  const enabled = cfg.enabled !== false;
  const inboundTrigger = cfg.inboundTrigger ?? "always";
  const replyHint = cfg.replyHint !== false;
  const statusSuffix = typeof cfg.status === "string" && cfg.status.trim() ? cfg.status.trim() : undefined;
  let askTimeoutMs = typeof cfg.askTimeoutMs === "number" && cfg.askTimeoutMs > 0 ? cfg.askTimeoutMs : DEFAULT_ASK_TIMEOUT_MS;
  if (process.env.DSH_INTERCOM_ASK_TIMEOUT_MS !== undefined) {
    askTimeoutMs = getAskTimeoutMs();
  }

  const runtimes = new Map();

  function currentStatus(runtime) {
    const activeToolName = runtime.activeTools.values().next().value;
    const lifecycleStatus = activeToolName ? `tool:${activeToolName}` : runtime.agent.status === "running" ? "thinking" : "idle";
    return statusSuffix ? `${lifecycleStatus} · ${statusSuffix}` : lifecycleStatus;
  }

  /**
   * Live context usage for presence, mirroring pi's getContextUsage() push:
   * tokens from the tokenMeter service's current pressure measurement, window
   * from the latest logged request/context. All fields optional — unknown
   * values are pushed as null so the broker clears them rather than showing
   * a stale percentage.
   */
  function currentContextUsage(runtime) {
    const now = Date.now();
    if (now - runtime.contextUsageComputedAt < CONTEXT_USAGE_CACHE_MS) {
      return runtime.contextUsage;
    }
    runtime.contextUsageComputedAt = now;
    let usage = { contextPct: null, contextTokens: null, contextWindow: null };
    try {
      const meter = ctx.get("tokenMeter");
      if (meter && typeof meter.measure === "function") {
        const measurement = meter.measure(runtime.agent.session);
        const tokens = typeof measurement?.totalTokens === "number" ? measurement.totalTokens : undefined;
        const window = readContextWindow(runtime.agent);
        if (typeof tokens === "number") {
          usage = {
            contextTokens: tokens,
            contextWindow: typeof window === "number" ? window : null,
            contextPct: typeof window === "number" && window > 0 ? Math.round(tokens / window * 100) : null,
          };
        }
      }
    } catch {
      // Context usage is best-effort; absence renders nothing, like pi.
    }
    runtime.contextUsage = usage;
    return usage;
  }

  function syncPresence(runtime, extra = {}) {
    const client = runtime.client;
    if (!client || !client.isConnected()) return;
    client.updatePresence({
      name: extra.name ?? presenceNameFor(ctx, runtime.agent),
      status: extra.status ?? currentStatus(runtime),
      model: extra.model ?? agentModel(runtime.agent),
      ...currentContextUsage(runtime),
    });
  }

  function buildRegistration(runtime) {
    const agent = runtime.agent;
    return {
      name: presenceNameFor(ctx, agent),
      cwd: agentCwd(agent),
      model: agentModel(agent),
      pid: process.pid,
      startedAt: runtime.sessionStartedAt,
      lastActivity: Date.now(),
      status: currentStatus(runtime),
    };
  }

  function hasSeenInboundMessage(runtime, from, message, now) {
    for (const [seenId, seenAt] of runtime.inboundDedupe) {
      if (now - seenAt > INBOUND_MESSAGE_DEDUPE_RETENTION_MS) {
        runtime.inboundDedupe.delete(seenId);
      }
    }
    while (runtime.inboundDedupe.size >= INBOUND_MESSAGE_DEDUPE_MAX) {
      const oldest = runtime.inboundDedupe.keys().next().value;
      if (oldest === undefined) break;
      runtime.inboundDedupe.delete(oldest);
    }
    const key = `${from.id}:${message.id}`;
    if (runtime.inboundDedupe.has(key)) return true;
    runtime.inboundDedupe.set(key, now);
    return false;
  }

  function emitMessageReceipt(runtime, messageId, status, detail) {
    runtime.client?.sendMessageReceipt({
      messageId,
      status,
      timestamp: Date.now(),
      ...(detail !== undefined ? { detail } : {}),
    });
  }

  function dismissIncomingAsk(runtime, messageId) {
    runtime.replyTracker.dismissPendingAsk(messageId);
  }

  /** pi's shouldTriggerInboundMessage: policy decides turn vs steer delivery. */
  function shouldTriggerInboundMessage(message) {
    if (inboundTrigger === "always") return true;
    if (inboundTrigger === "replies") return Boolean(message.replyTo);
    return false;
  }

  function inboundUserMessage(from, message, replyCommand, bodyText) {
    const senderDisplay = from.name || from.id.slice(0, 8);
    const replyInstruction = replyCommand ? `\n\nTo reply, use the intercom tool: ${replyCommand}` : "";
    const deliveryMetadata = formatInboundDeliveryMetadata(message);
    const text = `**📨 From ${senderDisplay}** (${from.cwd})${replyInstruction}\n\n_${deliveryMetadata}_\n\n${bodyText}`;
    return createUserMessage({
      content: [{ type: "text", text }],
      source: { kind: "user" },
    });
  }

  function sendIncomingMessage(runtime, from, message, replyCommand, bodyText) {
    const agent = runtime.agent;
    const injected = { ...message, injectedAt: Date.now() };
    emitMessageReceipt(runtime, injected.id, "injected");
    runtime.replyTracker.queueTurnContext({ from, message: injected, receivedAt: Date.now() });
    const userMessage = inboundUserMessage(from, injected, replyCommand, bodyText);
    if (agent.status === "running") {
      agent.steer(userMessage);
    } else if (shouldTriggerInboundMessage(injected)) {
      agent.followup(userMessage);
    } else {
      // Unlike steer(), send(..., false) parks idle work until another turn
      // wakes the agent; this preserves Pi's non-triggering inbound policy.
      agent.send(userMessage, "next-step", false);
    }
  }

  function handleIncomingMessage(runtime, from, message) {
    const receiverReceivedAt = Date.now();
    if (runtime.disposed) return;
    if (hasSeenInboundMessage(runtime, from, message, receiverReceivedAt)) {
      emitMessageReceipt(runtime, message.id, "acknowledged", "duplicate message id suppressed");
      return;
    }
    const receivedMessage = { ...message, receiverReceivedAt };
    emitMessageReceipt(runtime, receivedMessage.id, "receiver_received");

    if (runtime.replyWaiter) {
      const senderTarget = from.name || from.id;
      const fromMatches = senderTarget.toLowerCase() === runtime.replyWaiter.from.toLowerCase()
        || from.id === runtime.replyWaiter.from;
      const replyMatches = receivedMessage.replyTo === runtime.replyWaiter.replyTo;
      if (fromMatches && replyMatches) {
        emitMessageReceipt(runtime, receivedMessage.id, "acknowledged", "matched reply waiter");
        runtime.replyWaiter.resolve(receivedMessage);
        return;
      }
    }

    const attachmentText = receivedMessage.content.attachments?.length
      ? formatAttachments(receivedMessage.content.attachments)
      : "";
    const bodyText = `${receivedMessage.content.text}${attachmentText}`;
    const replyCommand = replyHint && receivedMessage.expectsReply
      ? `intercom({ action: "reply", message: "..." })`
      : undefined;
    runtime.replyTracker.recordIncomingMessage(from, receivedMessage, receiverReceivedAt);
    emitMessageReceipt(runtime, receivedMessage.id, "acknowledged", "accepted by receiver");
    sendIncomingMessage(runtime, from, receivedMessage, replyCommand, bodyText);
  }

  function attachClientHandlers(runtime, client) {
    client.on("message", (from, message) => {
      if (runtime.client !== client || runtime.disposed) return;
      handleIncomingMessage(runtime, from, message);
    });
    client.on("message_control", (from, control) => {
      if (runtime.client !== client || runtime.disposed) return;
      if (control.action === "cancel") {
        dismissIncomingAsk(runtime, control.messageId);
        emitMessageReceipt(runtime, control.messageId, "cancelled", `cancelled by sender at ${new Date(control.timestamp).toISOString()}`);
      } else if (control.action === "supersede" && control.supersededBy) {
        dismissIncomingAsk(runtime, control.messageId);
        emitMessageReceipt(runtime, control.messageId, "superseded", `superseded by ${control.supersededBy}`);
      }
    });
    client.on("disconnected", (error) => {
      if (runtime.client !== client || runtime.disposed) return;
      runtime.client = null;
      runtime.replyWaiter?.reject(new Error(`Disconnected while waiting for reply: ${error.message}`));
      scheduleReconnect(runtime);
    });
    client.on("error", () => {
      // Broker/socket noise stays out of the agent log; reconnect handles it.
    });
  }

  const reconnectAttempts = new WeakMap();
  function backoffAttempt(runtime) {
    const attempt = reconnectAttempts.get(runtime) ?? 0;
    reconnectAttempts.set(runtime, attempt + 1);
    return attempt;
  }

  function scheduleReconnect(runtime) {
    if (runtime.disposed || runtime.reconnectTimer) return;
    const delay = RECONNECT_BACKOFF_MS[Math.min(backoffAttempt(runtime), RECONNECT_BACKOFF_MS.length - 1)];
    runtime.reconnectTimer = setTimeout(() => {
      runtime.reconnectTimer = null;
      ensureConnected(runtime).catch(() => scheduleReconnect(runtime));
    }, delay);
    runtime.reconnectTimer.unref?.();
  }

  async function ensureConnected(runtime) {
    if (runtime.disposed) throw new Error("Intercom runtime disposed");
    if (runtime.client && runtime.client.isConnected()) return runtime.client;
    if (runtime.reconnectPromise) return runtime.reconnectPromise;
    const nextReconnectPromise = (async () => {
      const nextClient = new IntercomClient();
      attachClientHandlers(runtime, nextClient);
      try {
        await spawnBrokerIfNeeded();
        runtime.client = nextClient;
        await nextClient.connect(buildRegistration(runtime), runtime.agent.id);
        if (runtime.disposed) {
          await nextClient.disconnect();
          throw new Error("Intercom runtime disposed");
        }
        runtime.client = nextClient;
        reconnectAttempts.set(runtime, 0);
        return nextClient;
      } catch (error) {
        if (runtime.client === nextClient) runtime.client = null;
        throw error instanceof Error ? error : new Error(String(error));
      } finally {
        if (runtime.reconnectPromise === nextReconnectPromise) {
          runtime.reconnectPromise = null;
        }
      }
    })();
    runtime.reconnectPromise = nextReconnectPromise;
    return nextReconnectPromise;
  }

  async function resolveSessionTarget(activeClient, nameOrId) {
    const sessions = await activeClient.listSessions();
    const byId = sessions.find(s => s.id === nameOrId);
    if (byId) return byId.id;
    const lowerName = nameOrId.toLowerCase();
    const byName = sessions.filter(s => s.name?.toLowerCase() === lowerName);
    if (byName.length > 1) {
      const ids = byName.map(s => shortSessionId(s.id)).join(", ");
      throw new Error(`Multiple sessions named "${nameOrId}" are connected. Address one by the id shown in parentheses by "list" (${ids}).`);
    }
    if (byName.length === 1) return byName[0].id;
    const byIdPrefix = sessions.filter(s => s.id.startsWith(nameOrId));
    if (byIdPrefix.length === 1) return byIdPrefix[0].id;
    if (byIdPrefix.length > 1) {
      throw new Error(`Multiple sessions match ID prefix "${nameOrId}". Use a longer session ID prefix.`);
    }
    return null;
  }

  function waitForReply(runtime, from, replyTo, signal, cancelOnAbort, getDeliveryState = () => "unknown") {
    if (runtime.replyWaiter) {
      return Promise.reject(new Error("Already waiting for a reply"));
    }
    if (signal?.aborted) {
      return Promise.reject(new Error("Cancelled"));
    }
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const timeoutDescription = askTimeoutMs % 60000 === 0 ? `${askTimeoutMs / 60000} minutes` : `${askTimeoutMs}ms`;
        runtime.replyWaiter?.reject(new Error(`No reply from "${from}" for message ${replyTo} within ${timeoutDescription}. Last known delivery state: ${getDeliveryState()}. This waiter timeout is not cancellation; the delivered message may still be queued or actionable in the recipient session.`));
      }, askTimeoutMs);
      timeout.unref?.();
      const cleanup = () => {
        clearTimeout(timeout);
        signal?.removeEventListener("abort", onAbort);
        if (runtime.replyWaiter?.replyTo === replyTo) {
          runtime.replyWaiter = null;
        }
      };
      const onAbort = () => {
        cancelOnAbort?.();
        cleanup();
        reject(new Error("Cancelled"));
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      runtime.replyWaiter = {
        from,
        replyTo,
        resolve: (message) => {
          cleanup();
          resolve(message);
        },
        reject: (error) => {
          cleanup();
          reject(error);
        },
      };
    });
  }

  // ------------------------------------------------------------------
  // Agent lifecycle wiring
  // ------------------------------------------------------------------

  ctx.on("agent/created", ({ agent }) => {
    if (!enabled || agent.session.header.origin === "subagent") return; // Subagent routing is a documented gap.
    const runtime = {
      agent,
      client: null,
      reconnectPromise: null,
      reconnectTimer: null,
      replyTracker: new ReplyTracker(askTimeoutMs),
      replyWaiter: null,
      sessionStartedAt: Date.now(),
      activeTools: new Map(),
      inboundDedupe: new Map(),
      contextUsage: { contextPct: null, contextTokens: null, contextWindow: null },
      contextUsageComputedAt: 0,
      disposed: false,
    };
    runtimes.set(agent.id, runtime);
    runtime.nameTimer = setInterval(() => syncPresence(runtime), 1000);
    runtime.nameTimer.unref?.();
    // Connect in the background so agent creation never blocks on the broker.
    ensureConnected(runtime).catch(() => scheduleReconnect(runtime));
  });

  ctx.on("agent/disposed", ({ agent }) => {
    const runtime = runtimes.get(agent.id);
    if (!runtime) return;
    runtime.disposed = true;
    runtimes.delete(agent.id);
    clearInterval(runtime.nameTimer);
    if (runtime.reconnectTimer) {
      clearTimeout(runtime.reconnectTimer);
      runtime.reconnectTimer = null;
    }
    runtime.replyWaiter?.reject(new Error("Agent disposed"));
    runtime.replyTracker.reset();
    const client = runtime.client;
    runtime.client = null;
    void client?.disconnect().catch(() => undefined);
  });

  ctx.on("agent/status", ({ agent }) => {
    const runtime = runtimes.get(agent.id);
    if (!runtime) return;
    if (agent.status === "idle") {
      runtime.activeTools.clear();
    }
    syncPresence(runtime);
  });

  ctx.on("session/event", (session, event) => {
    if (event.type !== "turn/start" && event.type !== "turn/end" && event.type !== "session/title") return;
    for (const runtime of runtimes.values()) {
      if (runtime.agent.session !== session) continue;
      // Turn context rotation for reply targeting, matching pi's turn_start/turn_end.
      if (event.type === "turn/start") runtime.replyTracker.beginTurn();
      else if (event.type === "turn/end") runtime.replyTracker.endTurn();
      else syncPresence(runtime); // session/title: the presence name may have changed
    }
  });

  ctx.on("tools/pre-execute", async (exec, next) => {
    const decision = await next();
    const runtime = exec.agent ? runtimes.get(exec.agent.id) : undefined;
    if (runtime && decision.kind === "allow") {
      runtime.activeTools.set(exec.callId, exec.name);
      syncPresence(runtime);
    }
    return decision;
  });

  ctx.on("tools/result", (exec) => {
    const runtime = exec.agent ? runtimes.get(exec.agent.id) : undefined;
    if (!runtime) return;
    if (runtime.activeTools.delete(exec.callId)) {
      syncPresence(runtime);
    }
  });

  // ------------------------------------------------------------------
  // The intercom tool
  // ------------------------------------------------------------------

  ctx.tools.register(defineTool({
    name: "intercom",
    description: `Send a message to another agent session running on this machine.
Use this to communicate findings, request help, or coordinate work with other sessions.

Target a session by name, full session ID, or the short id shown in parentheses
by "list" (a leading prefix of the ID is enough). Prefer the short id when two
sessions share a name.

Usage:
  intercom({ action: "list" })                    → List active sessions
  intercom({ action: "list-cwd" })                → List sessions in the current working directory
  intercom({ action: "list-cwd", cwd: "/path" })  → List sessions in a specific directory
  intercom({ action: "send", to: "name-or-id", message: "..." })  → Send message
  intercom({ action: "ask", to: "name-or-id", message: "..." })   → Ask and wait for reply
  intercom({ action: "cancel", messageId: "..." })                 → Request cancellation of a sent message
  intercom({ action: "reply", message: "..." })                      → Reply to the active/single pending ask
  intercom({ action: "pending" })                                      → List unresolved inbound asks
  intercom({ action: "status" })                  → Show connection status`,
    parameters: {
      action: {
        type: "string",
        enum: ["list", "list-cwd", "send", "ask", "reply", "pending", "status", "cancel"],
        required: true,
        description: "Action: 'list', 'list-cwd', 'send', 'ask', 'reply', 'pending', 'status', or 'cancel'",
      },
      to: {
        type: "string",
        description: "Target session: name, full session ID, or the short id shown in parentheses by 'list' (a leading ID prefix resolves). For send/ask with cwd, scopes target lookup to that directory; omit to target the sole live session in that cwd or the newly opened project-pane session. For 'reply', disambiguates the pending ask.",
      },
      message: {
        type: "string",
        description: "Message to send (for 'send', 'ask', or 'reply' action)",
      },
      attachments: {
        type: "array",
        description: "Optional attachments (file paths, code snippets, or context blocks) to send with the message",
        items: {
          type: "object",
          additionalProperties: false,
          properties: {
            type: { type: "string", enum: ["file", "snippet", "context"], required: true },
            name: { type: "string", required: true },
            content: { type: "string", required: true },
            language: { type: "string" },
          },
        },
      },
      replyTo: {
        type: "string",
        description: "Message ID to reply to (for threading or responding to an 'ask')",
      },
      messageId: {
        type: "string",
        description: "Message ID for actions that operate on an existing message, such as 'cancel'.",
      },
      supersedes: {
        type: "string",
        description: "Previous message ID this send/ask explicitly supersedes. Only works for the same sender and receiver.",
      },
      retryOf: {
        type: "string",
        description: "Previous message ID this send/ask is a user-authored retry of. Retries always send a new message ID.",
      },
      cwd: {
        type: "string",
        description: "Working directory filter for 'list-cwd'. For send/ask, scopes target lookup to that directory; omit to target the sole live session in that cwd or the newly opened project-pane session. Absolute, or relative to the current session's cwd; '.' means the current cwd.",
      },
    },
    output: {
      schema: {
        type: "object",
        additionalProperties: false,
        properties: {
          text: { type: "string", required: true },
          ok: { type: "boolean", required: true },
        },
      },
      render: (_args, value) => [{ type: "text", text: value.text }],
      presentationMeta: (_args, value) => ({ ok: value.ok }),
    },
    isConcurrencySafe: () => false,
    async execute(args, exec) {
      const ok = (text) => ({ text, ok: true });
      const fail = (text) => ({ text, ok: false });

      if (!enabled) {
        return fail("Intercom is disabled by plugin configuration.");
      }

      const runtime = exec.agent ? runtimes.get(exec.agent.id) : undefined;
      if (!runtime) {
        return fail("Intercom unavailable: no live agent context for this session.");
      }

      let connectedClient;
      try {
        connectedClient = await ensureConnected(runtime);
      } catch (error) {
        return fail(`Intercom not connected: ${getErrorMessage(error)}`);
      }

      syncPresence(runtime);

      const { action, to, message, attachments, replyTo, messageId, supersedes, retryOf, cwd } = args;

      switch (action) {
        case "list": {
          try {
            const mySessionId = connectedClient.sessionId;
            const sessions = await connectedClient.listSessions();
            const currentSession = sessions.find(s => s.id === mySessionId);
            if (!currentSession) {
              return fail("Current session is missing from intercom session list.");
            }
            const currentSection = `**Current session:**\n${formatSessionListRow(currentSession, currentSession.cwd, true)}`;
            const otherSessions = sessions.filter(s => s.id !== mySessionId);
            const otherSection = otherSessions.length === 0
              ? "**Other sessions:**\nNo other sessions connected."
              : `**Other sessions:**\n${otherSessions.map(s => formatSessionListRow(s, currentSession.cwd, false)).join("\n")}`;
            return ok(`${currentSection}\n\n${otherSection}`);
          } catch (error) {
            return fail(`Failed to list sessions: ${getErrorMessage(error)}`);
          }
        }

        case "list-cwd": {
          try {
            const mySessionId = connectedClient.sessionId;
            const sessions = await connectedClient.listSessions();
            const currentSession = sessions.find(s => s.id === mySessionId);
            if (!currentSession) {
              return fail("Current session is missing from intercom session list.");
            }
            const filterCwd = cwd && cwd !== "."
              ? resolvePath(currentSession.cwd, cwd)
              : currentSession.cwd;
            const otherSessions = sessions.filter(
              s => s.id !== mySessionId && sameCwd(s.cwd, filterCwd),
            );
            // Fail loud: filtering by a directory with no peers while the
            // session's OWN cwd has some otherwise reads as a misleading empty
            // result (common when a caller passes a guessed parent cwd).
            let emptyNote = "No other sessions in this directory.";
            if (otherSessions.length === 0 && !sameCwd(filterCwd, currentSession.cwd)) {
              const here = sessions.filter(
                s => s.id !== mySessionId && sameCwd(s.cwd, currentSession.cwd),
              ).length;
              if (here > 0) {
                emptyNote += ` Your session's cwd is ${currentSession.cwd} (${here} peer${here === 1 ? "" : "s"} there) — call list-cwd without a cwd argument to list them.`;
              }
            }
            const currentSection = `**Current session:**\n${formatSessionListRow(currentSession, currentSession.cwd, true)}`;
            const otherSection = otherSessions.length === 0
              ? `**Other sessions (cwd: ${filterCwd}):**\n${emptyNote}`
              : `**Other sessions (cwd: ${filterCwd}):**\n${otherSessions.map(s => formatSessionListRow(s, currentSession.cwd, false)).join("\n")}`;
            return ok(`${currentSection}\n\n${otherSection}`);
          } catch (error) {
            return fail(`Failed to list sessions: ${getErrorMessage(error)}`);
          }
        }

        case "cancel": {
          if (!messageId) return fail("Missing 'messageId' parameter");
          try {
            const result = await connectedClient.cancelMessage(messageId);
            if (!result.delivered) {
              const errorText = result.reason ?? "Message may not exist or may belong to another sender.";
              return fail(`Cancellation for ${messageId} was not delivered: ${errorText}`);
            }
            return ok(`Cancellation requested for ${messageId}`);
          } catch (error) {
            return fail(`Failed to cancel message: ${getErrorMessage(error)}`);
          }
        }

        case "send": {
          if (!to || !message) return fail("Missing 'to' or 'message' parameter");
          try {
            const sendTo = (await resolveSessionTarget(connectedClient, to)) ?? to;
            if (sendTo === connectedClient.sessionId) {
              return fail("Cannot message the current session");
            }
            const result = await connectedClient.send(sendTo, {
              text: message,
              attachments,
              replyTo,
              supersedes,
              retryOf,
            });
            if (!result.delivered) {
              const errorText = result.reason ?? "Session may not exist or has disconnected.";
              return fail(`Message to "${to}" was not delivered: ${errorText}`);
            }
            if (replyTo) {
              dismissIncomingAsk(runtime, replyTo);
            }
            return ok(`Message sent to ${to}`);
          } catch (error) {
            return fail(`Failed to send: ${getErrorMessage(error)}`);
          }
        }

        case "ask": {
          if (!to || !message) return fail("Missing 'to' or 'message' parameter");
          if (runtime.replyWaiter) return fail("Already waiting for a reply");
          if (exec.signal.aborted) return fail("Cancelled");

          let deliveryState = "created";
          let questionId = null;
          let replyPromise = null;
          try {
            const sendTo = (await resolveSessionTarget(connectedClient, to)) ?? to;
            if (exec.signal.aborted) return fail("Cancelled");
            if (sendTo === connectedClient.sessionId) {
              return fail("Cannot message the current session");
            }
            if (runtime.replyWaiter) return fail("Already waiting for a reply");

            questionId = randomUUID();
            replyPromise = waitForReply(
              runtime,
              sendTo,
              questionId,
              exec.signal,
              () => connectedClient.cancelAsk(questionId),
              () => deliveryState,
            );
            replyPromise.catch(() => undefined);

            const sendResult = await connectedClient.send(sendTo, {
              messageId: questionId,
              text: message,
              attachments,
              replyTo,
              expectsReply: true,
              supersedes,
              retryOf,
            });
            deliveryState = sendResult.delivered ? "socket_delivered" : "delivery_failed";
            if (!sendResult.delivered) {
              const errorText = sendResult.reason ?? "Session may not exist or has disconnected.";
              runtime.replyWaiter?.reject(new Error(`Message to "${to}" was not delivered: ${errorText}`));
              try {
                await replyPromise;
              } catch {
                // The waiter was already rejected above.
              }
              return fail(`Message to "${to}" was not delivered: ${errorText}`);
            }

            const replyMessage = await replyPromise;
            const replyText = replyMessage.content.text;
            const replyAttachments = replyMessage.content.attachments?.length
              ? formatAttachments(replyMessage.content.attachments)
              : "";
            return ok(`**Reply from ${to}:**\n${replyText}${replyAttachments}`);
          } catch (error) {
            runtime.replyWaiter?.reject(error instanceof Error ? error : new Error(String(error)));
            if (replyPromise) {
              try {
                await replyPromise;
              } catch {
                // Cleanup-only; the outer error is the real failure.
              }
            }
            return fail(`Failed: ${getErrorMessage(error)}`);
          }
        }

        case "reply": {
          if (!message) return fail("Missing 'message' parameter");
          try {
            const target = runtime.replyTracker.resolveReplyTarget({ to, replyTo });
            if (target.from.id === connectedClient.sessionId) {
              return fail("Cannot message the current session");
            }
            const result = await connectedClient.send(target.from.id, {
              text: message,
              replyTo: target.message.id,
            });
            if (!result.delivered) {
              const errorText = result.reason ?? "Session may not exist or has disconnected.";
              if (result.reason === "Session not found") {
                dismissIncomingAsk(runtime, target.message.id);
              }
              return fail(`Reply to "${target.from.name || target.from.id}" was not delivered: ${errorText}`);
            }
            dismissIncomingAsk(runtime, target.message.id);
            return ok(`Reply sent to ${target.from.name || target.from.id}`);
          } catch (error) {
            return fail(`Failed to reply: ${getErrorMessage(error)}`);
          }
        }

        case "pending": {
          const pendingAsks = runtime.replyTracker.listPending();
          if (pendingAsks.length === 0) return ok("No unresolved inbound asks.");
          const now = Date.now();
          const lines = pendingAsks.map(({ from, message: pendingMessage, receivedAt }) => {
            const preview = pendingMessage.content.text.replace(/\s+/g, " ").slice(0, 80);
            const elapsedSeconds = Math.max(0, Math.floor((now - receivedAt) / 1000));
            return `- ${from.name || from.id} · ${pendingMessage.id} · ${elapsedSeconds}s ago · ${preview}`;
          });
          return ok(`**Pending asks:**\n${lines.join("\n")}`);
        }

        case "status": {
          try {
            const mySessionId = connectedClient.sessionId;
            const sessions = await connectedClient.listSessions();
            return ok(`**Intercom Status:**\nConnected: Yes\nSession ID: ${mySessionId}\nActive sessions: ${sessions.length}`);
          } catch (error) {
            return fail(`Failed to get status: ${getErrorMessage(error)}`);
          }
        }

        default:
          return fail(`Unknown action: ${action}`);
      }
    },
    presentCall(args) {
      const action = typeof args.action === "string" ? args.action : "intercom";
      const target = typeof args.to === "string" && args.to.trim() ? args.to.trim() : undefined;
      const preview = typeof args.message === "string"
        ? (args.message.replace(/\s+/g, " ").trim().slice(0, 96) || undefined)
        : undefined;
      const attachmentCount = Array.isArray(args.attachments) ? args.attachments.length : 0;
      const title = [
        `intercom ${action}`,
        target ? `→ ${target}` : undefined,
        attachmentCount > 0 ? `(${attachmentCount} attachment${attachmentCount === 1 ? "" : "s"})` : undefined,
      ].filter(Boolean).join(" ");
      return {
        card: "generic",
        title,
        kind: "other",
        ...(preview ? { rawInput: preview } : {}),
      };
    },
    presentResult(_args, result) {
      const meta = result.meta;
      const text = firstText(result.content) || "(no output)";
      const failed = result.isError || meta?.ok === false;
      return {
        card: "generic",
        title: failed ? `✗ ${text.split("\n")[0]}` : `✓ ${text.split("\n")[0]}`,
      };
    },
  }));
}

export default { name, inject, Config, apply };
