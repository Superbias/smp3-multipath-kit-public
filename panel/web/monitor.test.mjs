import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";

const files = {
  html: await readFile(new URL("./index.html", import.meta.url), "utf8"),
  js: await readFile(new URL("./monitor.js", import.meta.url), "utf8"),
  css: await readFile(new URL("./monitor.css", import.meta.url), "utf8"),
};
const source = `${files.html}\n${files.js}\n${files.css}`;

test("dashboard is same-origin, read-only, and dependency-free", () => {
  assert.doesNotMatch(source, /https?:\/\//);
  assert.doesNotMatch(source, /\b(?:POST|PUT|PATCH|DELETE)\b/);
  assert.doesNotMatch(files.html, /<(?:style|script)>/i);
  assert.doesNotMatch(files.html, /\s(?:style|onclick)=/i);
  assert.match(files.js, /EventSource\("\/api\/monitor\/stream"\)/);
  assert.match(files.js, /fetch\(path/);
  assert.match(source, /Live/);
  assert.match(source, /Reconnecting/);
  assert.match(source, /Disconnected/);
  assert.match(files.css, /prefers-reduced-motion/);
  assert.match(files.js, /getContext\("2d"\)/);
  assert.match(files.html, /chart-window/);
});

test("dashboard exposes required read-only monitoring surfaces", () => {
  assert.match(files.html, /Leg comparison/);
  assert.match(source, /Useful ACK/);
  assert.match(source, /TRAFFIC SHARE/);
  assert.match(source, /Process \/ listener probes/);
  assert.match(source, /Events/);
  assert.match(files.js, /scope=all/);
  assert.match(files.js, /telemetry_dropped/);
  assert.match(source, /LINE_PATH/);
  assert.match(source, /VLESS \+ Reality/);
  assert.match(source, /preferred/);
  assert.match(source, /80 Mbps/);
  assert.match(source, /500ms/);
  assert.match(files.html, /throughput-chart/);
  assert.match(files.html, /ack-chart/);
  assert.match(files.html, /share-chart/);
});
