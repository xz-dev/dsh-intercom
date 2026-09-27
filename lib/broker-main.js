#!/usr/bin/env node
/**
 * Broker daemon entrypoint: `node lib/broker-main.js` runs standalone.
 * Spawned detached by the plugin on first use (lib/spawn.js).
 */
import { IntercomBroker } from "./broker.js";

new IntercomBroker(process.env).start();
