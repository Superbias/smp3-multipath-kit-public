(() => {
  "use strict";
  const state = { snapshot: null, history: [], events: [], source: null };
  const $ = (id) => document.getElementById(id);
  const text = (node, value) => { if (node) node.textContent = value; };
  const esc = (value) => String(value).replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[character]));
  const num = (value) => Number.isFinite(Number(value)) ? Number(value) : 0;
  const valid = (value) => value !== null && value !== undefined && Number.isFinite(Number(value));
  const bytes = (value) => { const n = num(value); if (!n) return "0 B"; const units = ["B", "KiB", "MiB", "GiB"]; const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1); return `${(n / Math.pow(1024, i)).toFixed(i ? 2 : 0)} ${units[i]}`; };
  const rate = (value) => { if (!valid(value)) return "N/A"; const n = num(value); if (!n) return "0 bps"; const units = ["bps", "Kbps", "Mbps", "Gbps"]; const i = Math.min(Math.floor(Math.log(n) / Math.log(1000)), units.length - 1); return `${(n / Math.pow(1000, i)).toFixed(i ? 1 : 0)} ${units[i]}`; };
  const percent = (value) => valid(value) ? `${(num(value) * 100).toFixed(1)}%` : "N/A";
  const stamp = (value) => value ? new Date(value).toLocaleTimeString() : "N/A";
  const shareBar = (value, label) => { if (!valid(value)) return `<span class="na">N/A</span>`; return `<progress max="1" value="${Math.max(0, Math.min(1, num(value)))}" aria-label="${label}"></progress>`; };

  function connection(value) {
    const node = $("connection");
    node.className = `connection ${value.toLowerCase()}`;
    text($("connection-label"), value);
  }

  async function get(path) {
    const response = await fetch(path, { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error("monitor request failed");
    return response.json();
  }

  function kpi(label, value) {
    const card = document.createElement("div"); card.className = "kpi";
    const name = document.createElement("span"); name.className = "kpi-label"; name.textContent = label;
    const result = document.createElement("strong"); result.className = "kpi-value"; result.textContent = value;
    card.append(name, result); return card;
  }

  function renderSnapshot() {
    const snapshot = state.snapshot; if (!snapshot) return;
    text($("sample-time"), `Sample ${stamp(snapshot.at)}`);
    const health = $("overall-health"); text(health, snapshot.health.overall); health.className = `health-pill ${snapshot.health.overall.toLowerCase()}`;
    const kpis = $("kpis"); kpis.replaceChildren(
      kpi("Active sessions", String(snapshot.active_sessions)),
      kpi("Total wire RX", bytes(snapshot.total_wire_rx_bytes)),
      kpi("Total wire TX", bytes(snapshot.total_wire_tx_bytes)),
      kpi("Current RX", rate(snapshot.total_wire_rx_rate_bps)),
      kpi("Current TX", rate(snapshot.total_wire_tx_rate_bps)),
      kpi("Active legs", String(snapshot.active_legs)),
      kpi("Telemetry drops", String(snapshot.telemetry_dropped)),
      kpi("Health", snapshot.health.overall)
    );
    $("telemetry-state").textContent = snapshot.health.telemetry === "UP" ? "Telemetry healthy" : "Telemetry unavailable";
    renderLegs(); renderShares(); renderListeners(); renderCharts();
  }

  function renderLegs() {
    const root = $("legs"); root.replaceChildren();
    state.snapshot.legs.forEach((leg) => {
      const card = document.createElement("article"); card.className = `leg-card leg-${leg.leg_id}`;
      card.innerHTML = `<div class="leg-head"><div><span class="leg-name">LEG ${esc(leg.leg_id)}</span><span class="leg-sub">carrier</span></div><span class="state ${esc(leg.state)}">${esc(String(leg.state).toUpperCase())}</span></div><div class="rate"><strong>${esc(rate(num(leg.wire_tx_rate_bps) + num(leg.wire_rx_rate_bps)))}</strong><span>wire rate</span></div><div class="leg-stats"><div><span>Useful ACK</span><strong>${esc(bytes(leg.useful_ack_bytes))}</strong></div><div><span>Useful ACK rate</span><strong>${esc(rate(leg.useful_ack_rate_bps))}</strong></div><div><span>RX unique</span><strong>${esc(bytes(leg.rx_unique_bytes))}</strong></div><div><span>RX rate</span><strong>${esc(rate(leg.rx_unique_rate_bps))}</strong></div><div><span>Wire TX</span><strong>${esc(bytes(leg.wire_tx_bytes))}</strong></div><div><span>Wire RX</span><strong>${esc(bytes(leg.wire_rx_bytes))}</strong></div></div>`;
      root.appendChild(card);
    });
  }

  function shareRow(label, share) {
    const row = document.createElement("div"); row.className = "share-row";
    const title = document.createElement("span"); title.textContent = label;
    const values = document.createElement("div"); values.className = "share-values";
    values.innerHTML = `<span>Leg0 ${esc(percent(share.leg0))}</span><span>Leg1 ${esc(percent(share.leg1))}</span>`;
    const bars = document.createElement("div"); bars.className = "share-bars";
    bars.innerHTML = `<span class="leg0-fill">${shareBar(share.leg0, `${esc(label)} Leg0`)}</span><span class="leg1-fill">${shareBar(share.leg1, `${esc(label)} Leg1`)}</span>`;
    row.append(title, values, bars); return row;
  }

  function renderShares() {
    const root = $("shares"); root.replaceChildren();
    [["Wire TX", state.snapshot.traffic.wire_tx], ["Wire RX", state.snapshot.traffic.wire_rx], ["Useful ACK", state.snapshot.traffic.useful_ack], ["RX unique", state.snapshot.traffic.rx_unique]].forEach(([label, share]) => root.appendChild(shareRow(label, share)));
    const note = document.createElement("p"); note.className = "muted share-note"; note.textContent = "Shares are calculated from valid counter deltas."; root.appendChild(note);
  }

  function chartValue(sample, leg, kind) {
    if (kind === "throughput") {
      if (!valid(sample.legs[leg].wire_tx_rate_bps) || !valid(sample.legs[leg].wire_rx_rate_bps)) return null;
      return (num(sample.legs[leg].wire_tx_rate_bps) + num(sample.legs[leg].wire_rx_rate_bps)) / 1000000;
    }
    if (kind === "ack") return valid(sample.legs[leg].useful_ack_rate_bps) ? num(sample.legs[leg].useful_ack_rate_bps) / 1000000 : null;
    const share = sample.traffic && sample.traffic.useful_ack ? sample.traffic.useful_ack[`leg${leg}`] : null;
    return valid(share) ? num(share) * 100 : null;
  }

  function drawChart(id, kind) {
    const canvas = $(id); if (!canvas) return;
    const context = canvas.getContext && canvas.getContext("2d"); if (!context) return;
    const width = canvas.width; const height = canvas.height; context.clearRect(0, 0, width, height);
    const select = $("chart-window"); const windowMinutes = Math.max(1, num(select && select.value) || 5);
    const samples = state.history.filter((sample) => sample && sample.at && Number.isFinite(Date.parse(sample.at)));
    const end = samples.length ? Date.parse(samples[samples.length - 1].at) : Date.now();
    const start = end - windowMinutes * 60000;
    const visible = samples.filter((sample) => Date.parse(sample.at) >= start);
    const series = [0, 1].map((leg) => visible.map((sample) => chartValue(sample, leg, kind)));
    const values = series.flat().filter((value) => value !== null && Number.isFinite(value));
    const left = 8; const right = width - 8; const top = 10; const bottom = height - 12;
    context.strokeStyle = "#273543"; context.lineWidth = 1;
    for (let index = 0; index < 4; index += 1) { const y = top + ((bottom - top) * index) / 3; context.beginPath(); context.moveTo(left, y); context.lineTo(right, y); context.stroke(); }
    if (!values.length) {
      context.fillStyle = "#91a1af"; context.font = "12px system-ui"; context.fillText("No valid samples", 16, height / 2 + 4); return;
    }
    const maximum = kind === "share" ? 100 : Math.max(1, ...values);
    const colors = ["#5eead4", "#8ab4ff"];
    series.forEach((points, leg) => {
      context.strokeStyle = colors[leg]; context.lineWidth = 2; let drawing = false;
      points.forEach((value, index) => {
        if (value === null || !Number.isFinite(value)) { drawing = false; return; }
        const x = visible.length < 2 ? (left + right) / 2 : left + ((right - left) * index) / (visible.length - 1);
        const y = bottom - (Math.max(0, Math.min(maximum, value)) / maximum) * (bottom - top);
        if (!drawing) { context.beginPath(); context.moveTo(x, y); drawing = true; } else context.lineTo(x, y);
        context.stroke();
      });
    });
  }

  function renderCharts() {
    drawChart("throughput-chart", "throughput");
    drawChart("ack-chart", "ack");
    drawChart("share-chart", "share");
  }

  function renderHistory() {
    const root = $("history"); root.replaceChildren();
    const samples = state.history.slice(-12);
    text($("history-count"), `${state.history.length} samples`);
    if (!samples.length) { const empty = document.createElement("p"); empty.className = "empty"; empty.textContent = "Waiting for samples"; root.appendChild(empty); return; }
    samples.forEach((sample) => { const row = document.createElement("div"); row.className = "history-row"; row.innerHTML = `<span>${esc(stamp(sample.at))}</span><span>RX ${esc(rate(sample.total_wire_rx_rate_bps))}</span><span>TX ${esc(rate(sample.total_wire_tx_rate_bps))}</span><span class="history-health">${esc(sample.health.overall)}</span>`; root.appendChild(row); });
  }

  function renderListeners() {
    const root = $("listeners"); root.replaceChildren();
    state.snapshot.health.listeners.forEach((listener) => { const row = document.createElement("div"); row.className = "listener-row"; row.innerHTML = `<div><strong>${esc(listener.name)}</strong><span>${esc(listener.address)}</span></div><span class="state ${listener.listener_up && listener.process_up ? "up" : "down"}">${listener.listener_up && listener.process_up ? "UP" : "DOWN"}</span>`; root.appendChild(row); });
  }

  function renderEvents() {
    const root = $("events"); root.replaceChildren(); text($("event-count"), `${state.events.length} events`);
    if (!state.events.length) { const empty = document.createElement("p"); empty.className = "empty"; empty.textContent = "No operational events"; root.appendChild(empty); return; }
    state.events.slice(-12).reverse().forEach((event) => { const row = document.createElement("div"); row.className = "event-row"; row.innerHTML = `<span class="event-severity ${esc(event.severity)}">${esc(String(event.severity).toUpperCase())}</span><span class="event-time">${esc(stamp(event.at))}</span><strong>${esc(event.source)}</strong><span>${esc(event.message)}</span>`; root.appendChild(row); });
  }

  function applySnapshot(snapshot) { state.snapshot = snapshot; state.history.push(snapshot); if (state.history.length > 3600) state.history.shift(); renderSnapshot(); renderHistory(); }

  async function loadInitial() {
    try {
      const [snapshot, history, events] = await Promise.all([get("/api/monitor/status"), get("/api/monitor/history?scope=all&limit=3600"), get("/api/monitor/events?limit=256")]);
      state.snapshot = snapshot; state.history = history.items || []; state.events = events.items || []; renderSnapshot(); renderHistory(); renderEvents();
    } catch (_) { connection("Disconnected"); text($("telemetry-state"), "Telemetry unavailable"); }
  }

  function connect() {
    const source = new EventSource("/api/monitor/stream"); state.source = source;
    source.onopen = () => connection("Live");
    source.onerror = () => connection("Reconnecting");
    source.addEventListener("snapshot", (event) => { try { applySnapshot(JSON.parse(event.data)); connection("Live"); } catch (_) {} });
    source.addEventListener("event", (event) => { try { state.events.push(JSON.parse(event.data)); if (state.events.length > 256) state.events.shift(); renderEvents(); } catch (_) {} });
  }

  document.addEventListener("DOMContentLoaded", () => { $("chart-window").addEventListener("change", renderCharts); loadInitial().finally(connect); });
})();
