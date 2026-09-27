# dsh-intercom

Local one-to-one messaging for DeepSeek Harness. Port of [pi-intercom](https://github.com/nicobailon/pi-intercom) (user fork: [xz-dev/pi-intercom](https://github.com/xz-dev/pi-intercom)). Original author Nico Bailon; upstream MIT license retained in `LICENSE`. New DSH integration copyright Xiangzhe, MIT.

Supports `intercom` tool (`list`, `list-cwd`, `send`, `ask`, `reply`, `pending`, `status`, `cancel`) between local DSH sessions using same `DSH_HOME`, with session-title names, idle follow-up and busy steer, bounded Unix-socket broker, mailbox, receipts and reconnection. Requires DSH 0.1.7-rc.2; dsh-TUI 0.11.1 target.

## Install

```sh
node ~/.local/share/dsh/npm/node_modules/@deepseek-ai/dsh/lib/bin.js plugin --profile tui add file:/absolute/path/to/dsh-intercom-0.1.0.tgz --ignore-scripts
```

Bundle patch inserts `dsh-intercom` with Pi defaults (`enabled: true`, `inboundTrigger: always`, `replyHint: true`). Do **not** also add a manual duplicate row. Restart DSH sessions. This plugin never connects to Pi's own broker; it uses `$DSH_HOME/intercom`. Sessions can call `intercom({action:"list"})` to discover each other, `intercom({action:"send",to:"name-or-id",message:"..."})` to send, `intercom({action:"ask",to:"name-or-id",message:"..."})` to block for reply, and `intercom({action:"reply",message:"..."})` to respond to a unique pending ask.

Explicitly missing Pi UI/Herdr/subagent extension-channel features listed in [BEHAVIOR.md](BEHAVIOR.md). For `inboundTrigger: never`, idle inbound messages remain parked until another turn wakes agent via DSH's non-waking inbox API. No API key, auth token or network port used. Socket/state directory is private to local user (`0700`); other local same-UID processes can communicate, same trust boundary as Pi.

Run `node --test test/*.test.mjs` after install. Broker exits five seconds after final registered client disconnects. For diagnostics, `intercom({action:"status"})` reports connection and active count.
