/**
 * Formatting helpers shared by the tool and inbound delivery. Ported from
 * pi-intercom's index.ts / format-context.ts presentation logic (kept
 * text-identical where the model sees it).
 */

export function shortSessionId(sessionId) {
  return sessionId.slice(0, 8);
}

export function duplicateSessionNames(sessions) {
  return new Set(
    sessions
      .map(s => s.name?.toLowerCase())
      .filter(name => Boolean(name))
      .filter((name, index, names) => names.indexOf(name) !== index),
  );
}

// Compact token count for display: 1432 -> "1.4k", 144000 -> "144k".
export function formatTokenCount(tokens) {
  if (tokens < 1000) {
    return String(Math.max(0, Math.round(tokens)));
  }
  const k = tokens / 1000;
  const value = k >= 100 ? String(Math.round(k)) : k.toFixed(1).replace(/\.0$/, "");
  return `${value}k`;
}

// " · 72% ctx (144k/200k)" — renders nothing when the percent is unknown.
export function formatContextUsage(session) {
  if (typeof session.contextPct !== "number") {
    return "";
  }
  let out = ` · ${session.contextPct}% ctx`;
  if (
    typeof session.contextTokens === "number"
    && typeof session.contextWindow === "number"
    && session.contextWindow > 0
  ) {
    out += ` (${formatTokenCount(session.contextTokens)}/${formatTokenCount(session.contextWindow)})`;
  }
  return out;
}

export function formatSessionListRow(session, currentCwd, isSelf) {
  const name = session.name || "Unnamed session";
  const tags = [isSelf ? "self" : session.cwd === currentCwd ? "same cwd" : undefined, session.status]
    .filter(tag => Boolean(tag));
  const suffix = tags.length ? ` [${tags.join(", ")}]` : "";
  return `• ${name} (${shortSessionId(session.id)}) — ${session.cwd} (${session.model}${formatContextUsage(session)})${suffix}`;
}

export function formatAttachments(attachments) {
  const parts = attachments.map((attachment) => {
    const header = attachment.type === "file"
      ? `File: ${attachment.name}`
      : attachment.type === "snippet"
        ? `Snippet: ${attachment.name}${attachment.language ? ` (${attachment.language})` : ""}`
        : `Context: ${attachment.name}`;
    return `${header}\n\`\`\`\n${attachment.content}\n\`\`\``;
  });
  return `\n\n${parts.join("\n\n")}`;
}

export function formatMessageTimestamp(timestamp) {
  if (typeof timestamp !== "number") return undefined;
  return new Date(timestamp).toISOString();
}

export function formatInboundDeliveryMetadata(message) {
  const parts = [`id ${message.id}`];
  if (typeof message.senderSequence === "number") parts.push(`seq ${message.senderSequence}`);
  if (message.supersedes) parts.push(`supersedes ${message.supersedes}`);
  if (message.retryOf) parts.push(`retry of ${message.retryOf}`);
  const sentAt = formatMessageTimestamp(message.timestamp);
  if (sentAt) parts.push(`sent ${sentAt}`);
  const brokerDeliveredAt = formatMessageTimestamp(message.brokerDeliveredAt);
  if (brokerDeliveredAt) parts.push(`broker delivered ${brokerDeliveredAt}`);
  const receiverReceivedAt = formatMessageTimestamp(message.receiverReceivedAt);
  if (receiverReceivedAt) parts.push(`receiver received ${receiverReceivedAt}`);
  const injectedAt = formatMessageTimestamp(message.injectedAt);
  if (injectedAt) parts.push(`injected ${injectedAt}`);
  return parts.join(" · ");
}

export function firstText(blocks) {
  return blocks.find(b => b.type === "text")?.text ?? "";
}

export function getErrorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}
