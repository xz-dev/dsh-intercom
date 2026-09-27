/** Length-prefixed JSON framing, identical to pi-intercom's broker/framing.ts. */

export const MAX_FRAME_BYTES = 1024 * 1024;

/** Write a 4-byte big-endian length + JSON payload frame. */
export function writeMessage(socket, msg) {
  const json = JSON.stringify(msg);
  const payloadLength = Buffer.byteLength(json, "utf8");
  const frame = Buffer.allocUnsafe(4 + payloadLength);
  frame.writeUInt32BE(payloadLength, 0);
  frame.write(json, 4, payloadLength, "utf8");
  socket.write(frame);
}

/**
 * Incremental frame reader handling partial reads. Calls onError (and stops)
 * on protocol errors; the caller closes the connection.
 */
export function createMessageReader(onMessage, onError, maxFrameBytes = MAX_FRAME_BYTES) {
  let buffer = null;

  return (data) => {
    const chunk = buffer ? Buffer.concat([buffer, data]) : data;
    buffer = null;
    let offset = 0;
    while (offset + 4 <= chunk.length) {
      const payloadLength = chunk.readUInt32BE(offset);
      if (payloadLength > maxFrameBytes) {
        onError(new Error(`Intercom frame length ${payloadLength} exceeds maximum ${maxFrameBytes} bytes`));
        return;
      }
      const end = offset + 4 + payloadLength;
      if (end > chunk.length) break;
      const payload = chunk.subarray(offset + 4, end);
      offset = end;
      let msg;
      try {
        msg = JSON.parse(payload.toString("utf8"));
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        onError(new Error(`Failed to parse intercom message: ${message}`));
        return;
      }
      try {
        onMessage(msg);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        onError(new Error(`Failed to handle intercom message: ${message}`));
        return;
      }
    }
    if (offset < chunk.length) {
      buffer = chunk.subarray(offset);
    }
  };
}
