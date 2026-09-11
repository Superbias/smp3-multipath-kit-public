(() => {
  "use strict";

  const state = { status: null, legs: [], sessions: [], nextCursor: null, previousCursor: null, refreshTimer: null, eventSource: null, detailID: null };
  const $ = (selector) => document.querySelector(selector);
  const text = (node, value) => { if (node) node.textContent = value; };
  const esc = (value) => String(value).replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[character]));
  const na = (value) => value === null || value === undefined || value === "" ? "N/A" : String(value);
  const bytes = (value) => { const n = Number(value || 0); if (!n) return "0 B"; const units = ["B", "KiB", "MiB", "GiB", "TiB"]; const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1); return `${(n / Math.pow(1024, i)).toFixed(i ? 2 : 0)} ${units[i]}`; };
  const rate = (value) => { const n = Number(value || 0); if (!n) return "0 bps"; const units = ["bps", "Kbps", "Mbps", "Gbps"]; const i = Math.min(Math.floor(Math.log(n) / Math.log(1000)), units.length - 1); return `${(n / Math.pow(1000, i)).toFixed(i ? 1 : 0)} ${units[i]}`; };
  const percent = (value) => value === null || value === undefined ? "N/A" : `${(Number(value) * 100).toFixed(1)}%`;
  const ratio = (value) => { if (value === null || value === undefined || value === "") return null; const n = Number(value); return Number.isFinite(n) ? n : null; };
  const progress = (value, label, className) => { const n = ratio(value); return n === null ? `<span class="bar-na">N/A</span>` : `<progress class="${className}" max="1" value="${n}" aria-label="${esc(label)}"></progress>`; };
  const segment = (value, label) => { const n = ratio(value); return n === null ? "" : `<progress class="distribution-segment" max="1" value="${n}" aria-label="${esc(label)}"></progress>`; };
  const timeValue = (value) => value ? new Date(value).toLocaleString() : "N/A";
  const duration = (value) => { const n = Number(value || 0); if (n < 1000) return `${n} ms`; return `${(n / 1000).toFixed(1)} s`; };
  const releaseReasonLabels = {
    preferred_already_attached: "Preferred already attached",
    preferred_attached_before_nonpreferred: "Preferred attached first",
    preferred_arrived_within_grace: "Preferred arrived within grace",
    grace_expired: "Grace expired",
    preferred_terminal_unavailable: "Preferred leg unavailable",
    session_closed_before_release: "Session closed before release",
  };
  const startupPolicyLabel = (value) => value === "preferred" ? "Preferred" : value === "first-ready" ? "First ready" : "Unknown";
  const legLabel = (value) => value === 0 || value === 1 ? `Leg ${value}` : "N/A";
  const elapsed = (created, value) => {
    if (!value) return "N/A";
    const createdAt = Date.parse(created || "");
    const eventAt = Date.parse(value);
    if (!Number.isFinite(createdAt) || !Number.isFinite(eventAt)) return "Unknown";
    const milliseconds = Math.round(eventAt - createdAt);
    return `${milliseconds >= 0 ? "+" : ""}${milliseconds} ms`;
  };
  const startupReasonLabel = (value) => releaseReasonLabels[value] || (value ? "Unknown" : "N/A");

  function setConnectionState(value) {
    const node = $("#connection-state");
    text(node && node.querySelector(".state-label"), value);
    if (node) node.className = `connection-state ${value.toLowerCase()}`;
  }

  async function getJSON(path) {
    const response = await fetch(path, { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error("request failed");
    return response.json();
  }

  function metric(label, value, tone = "") { return `<div class="metric ${tone}"><div class="metric-label">${esc(label)}</div><div class="metric-value">${esc(value)}</div></div>`; }

  function legCard(leg) {
    const up = Number(leg.active_connections || 0) > 0;
    const stateText = up ? "UP" : "DOWN";
    const wire = percent(leg.wire_share);
    const logical = percent(leg.logical_rx_share);
    const accent = Number(leg.leg_id) === 0 ? "leg0-card" : "leg1-card";
    return `<article class="leg-card ${accent}"><h3><span class="leg-name"><span class="legend-dot ${Number(leg.leg_id) === 0 ? "leg0" : "leg1"}" aria-hidden="true"></span>LEG ${esc(leg.leg_id)} <span class="leg-badge">carrier</span></span><span class="leg-state ${up ? "up" : ""}">${stateText}</span></h3><div class="rate-emphasis"><span class="rate-number">${esc(rate(Number(leg.wire_tx_rate_bps || 0) + Number(leg.wire_rx_rate_bps || 0)))}</span><span class="rate-unit">current wire rate</span></div><div class="stat-list"><div><div class="stat-label">Wire share</div><div class="stat-value">${esc(wire)}</div></div><div><div class="stat-label">Logical RX</div><div class="stat-value">${esc(logical)}</div></div><div><div class="stat-label">Wire TX</div><div class="stat-value">${esc(bytes(leg.wire_tx_bytes))}</div></div><div><div class="stat-label">Wire RX</div><div class="stat-value">${esc(bytes(leg.wire_rx_bytes))}</div></div><div><div class="stat-label">Retransmit</div><div class="stat-value">${esc(bytes(leg.retransmit_bytes))}</div></div><div><div class="stat-label">Rescue</div><div class="stat-value">${esc(na(leg.rescue_frames))}</div></div></div><div class="share"><div class="share-row"><span>Wire Share</span><strong>${esc(wire)}</strong></div>${progress(leg.wire_share, "Wire share", "bar-progress")}<div class="share-row"><span>Logical RX Share</span><strong>${esc(logical)}</strong></div>${progress(leg.logical_rx_share, "Logical RX share", "bar-progress logical")}</div></article>`;
  }

  function renderTraffic() {
    const legs = state.legs;
    if (!legs.length) { $("#traffic-distribution").innerHTML = '<div class="empty-state">Waiting for traffic telemetry</div>'; return; }
    const row = (label, left, right) => `<div class="distribution-row"><span class="distribution-label">${label}</span><div class="distribution-bar">${segment(left, `${label} Leg0`)}${segment(right, `${label} Leg1`)}</div><span class="distribution-value">${percent(left)} / ${percent(right)}</span></div>`;
    $("#traffic-distribution").innerHTML = `<div class="distribution">${row("Wire", legs[0].wire_share, legs[1].wire_share)}${row("Logical RX", legs[0].logical_rx_share, legs[1].logical_rx_share)}${row("Logical TX", legs[0].logical_tx_share, legs[1].logical_tx_share)}<div class="distribution-legend"><span><i class="legend-dot leg0"></i>Leg0</span><span><i class="legend-dot leg1"></i>Leg1</span></div></div>`;
  }

  function renderStatus() {
    const s = state.status;
    if (!s) return;
    text($("#snapshot-time"), timeValue(s.snapshot_at));
    text($("#server-version"), `v${na(s.version)}`);
    text($("#server-uptime"), `Uptime ${duration(s.uptime_ms)}`);
    text($("#server-dropped"), `Dropped events ${na(s.telemetry_dropped_events)}`);
    text($("#telemetry-health"), Number(s.telemetry_dropped_events || 0) === 0 ? "Telemetry healthy" : "Telemetry warnings");
    $("#overview-metrics").innerHTML = [metric("Active sessions", s.active_sessions), metric("Current download", rate(s.wire_rx_rate_bps)), metric("Current upload", rate(s.wire_tx_rate_bps)), metric("Active legs", s.active_legs), metric("Total wire RX", bytes(s.wire_rx_bytes)), metric("Total wire TX", bytes(s.wire_tx_bytes)), metric("Total sessions", s.total_sessions), metric("Telemetry", Number(s.telemetry_dropped_events || 0) === 0 ? "Healthy" : "Dropped events", Number(s.telemetry_dropped_events || 0) === 0 ? "" : "warning")].join("");
  }

  function renderLegs() { const html = state.legs.map(legCard).join(""); $("#overview-legs").innerHTML = html; $("#legs-content").innerHTML = html; renderTraffic(); }

  function renderSessions() {
    const body = $("#sessions-body");
    if (!body) return;
    body.innerHTML = "";
    text($("#session-count"), `${state.sessions.length} active rows`);
    if (!state.sessions.length) { body.innerHTML = '<tr><td colspan="9" class="empty-row">No active SMP3 sessions</td></tr>'; }
    state.sessions.forEach((session) => { const row = document.createElement("tr"); row.innerHTML = `<td class="session-id">${esc(session.display_session_id)}</td><td>${esc(session.mode)}</td><td>${esc(duration(session.duration_ms))}</td><td><span class="state-text ${session.leg0_state === "up" ? "up" : "down"}">${esc(na(session.leg0_state))}</span></td><td><span class="state-text ${session.leg1_state === "up" ? "up" : "down"}">${esc(na(session.leg1_state))}</span></td><td>${esc(percent(session.leg0_wire_share))} / ${esc(percent(session.leg1_wire_share))}</td><td>${esc(bytes(Number(session.leg0_logical_rx_bytes || 0) + Number(session.leg1_logical_rx_bytes || 0)))}</td><td>${esc(timeValue(session.first_data_seen_at))}</td><td><button class="secondary detail-button">Open</button></td>`; row.querySelector(".detail-button").addEventListener("click", () => showDetail(session.display_session_id)); body.appendChild(row); });
    $("#sessions-next").disabled = !state.nextCursor;
    $("#sessions-prev").disabled = !state.previousCursor;
  }

  function startupField(label, value, late = false) {
    return `<div><div class="stat-label">${esc(label)}</div><div class="stat-value">${esc(value)}${late ? ' <span class="late-marker">Late</span>' : ""}</div></div>`;
  }

  function renderStartup(session) {
    const startup = session.startup;
    if (!startup) {
      return `<article class="detail-card startup-card"><h3>Startup</h3><div class="startup-not-applicable">Startup policy not applicable to Datagram</div></article>`;
    }
    if (!startup.applied) {
      return `<article class="detail-card startup-card"><h3>Startup</h3><div class="stat-list">${startupField("Policy", startupPolicyLabel(startup.policy))}${startupField("Gate", "Not applied")}</div></article>`;
    }
    const released = Boolean(startup.released_at);
    const waiting = !released && !startup.release_reason;
    const late = startup.preferred_attached_at && startup.released_at && Date.parse(startup.preferred_attached_at) > Date.parse(startup.released_at);
    const status = waiting ? "Waiting" : released ? "Released" : "Not released";
    const outcome = startup.release_reason ? startupReasonLabel(startup.release_reason) : "N/A";
    const graceValue = startup.grace_ms === null || startup.grace_ms === undefined ? "N/A" : Number.isFinite(Number(startup.grace_ms)) ? `${Number(startup.grace_ms)} ms` : "Unknown";
    return `<article class="detail-card startup-card"><div class="startup-heading"><div><h3>Startup</h3><div class="stat-label startup-status-label">Startup Status</div></div><span class="startup-status ${waiting ? "waiting" : released ? "released" : "closed"}">${esc(status)}</span></div><div class="stat-list startup-meta">${startupField("Policy", startupPolicyLabel(startup.policy))}${startupField("Preferred Leg", legLabel(startup.preferred_leg))}${startupField("Grace", graceValue)}</div><div class="stat-list startup-timeline">${startupField("First DATA Waiting", elapsed(session.created_at, startup.first_data_waiting_at))}${startupField("Non-preferred Attached", elapsed(session.created_at, startup.nonpreferred_attached_at))}${startupField("Grace Started", elapsed(session.created_at, startup.grace_started_at))}${startupField("Preferred Attached", elapsed(session.created_at, startup.preferred_attached_at), late)}${startupField("Released", elapsed(session.created_at, startup.released_at))}</div><div class="stat-list startup-outcome">${startupField("Release Reason", outcome)}${startupField("First DATA Leg", legLabel(startup.first_data_leg))}</div></article>`;
  }

  function renderDetail(session) {
    const field = (label, value) => `<div><div class="stat-label">${esc(label)}</div><div class="stat-value">${esc(na(value))}</div></div>`;
    const leg = (id) => `<article class="detail-card"><h3>LEG ${id} <span class="leg-state ${session[`leg${id}_state`] === "up" ? "up" : ""}">${esc(na(session[`leg${id}_state`]))}</span></h3><div class="stat-list">${field("Attach", timeValue(session[`leg${id}_attach_at`]))}${field("Wire TX", bytes(session[`leg${id}_wire_tx_bytes`]))}${field("Wire RX", bytes(session[`leg${id}_wire_rx_bytes`]))}${field("Logical TX", bytes(session[`leg${id}_logical_tx_bytes`]))}${field("Logical RX", bytes(session[`leg${id}_logical_rx_bytes`]))}${field("First DATA", timeValue(session[`leg${id}_first_data_at`]))}</div></article>`;
    $("#detail-title").textContent = session.display_session_id || "Session";
    $("#detail-content").innerHTML = `${leg(0)}${leg(1)}<article class="detail-card"><h3>Session</h3><div class="stat-list">${field("Created", timeValue(session.created_at))}${field("Duration", duration(session.duration_ms))}${field("First leg ready", timeValue(session.first_leg_ready_at))}${field("First DATA", timeValue(session.first_data_seen_at))}</div></article>${renderStartup(session)}`;
  }

  function openDetail() { $("#detail-drawer").classList.add("is-open"); $("#detail-drawer").setAttribute("aria-hidden", "false"); $("#detail-backdrop").hidden = false; $("#detail-close").focus(); }
  function closeDetail() { state.detailID = null; $("#detail-drawer").classList.remove("is-open"); $("#detail-drawer").setAttribute("aria-hidden", "true"); $("#detail-backdrop").hidden = true; }
  async function refreshDetail() { if (!state.detailID) return; try { const response = await getJSON(`/api/v1/sessions/${encodeURIComponent(state.detailID)}`); renderDetail(response.session); $("#detail-error").hidden = true; } catch (_) { $("#detail-error").hidden = false; text($("#detail-error"), "Session detail unavailable"); } }
  async function showDetail(id) { state.detailID = id; try { const response = await getJSON(`/api/v1/sessions/${encodeURIComponent(id)}`); renderDetail(response.session); openDetail(); } catch (_) { $("#detail-error").hidden = false; text($("#detail-error"), "Session detail unavailable"); } }
  function showView(name) { document.querySelectorAll(".view").forEach((view) => view.classList.remove("is-visible")); $(`#view-${name}`).classList.add("is-visible"); document.querySelectorAll(".nav-item").forEach((item) => item.classList.toggle("is-active", item.dataset.view === name)); text($("#page-title"), name.charAt(0).toUpperCase() + name.slice(1)); }
  function scheduleSessionsRefresh() { clearTimeout(state.refreshTimer); state.refreshTimer = setTimeout(() => { loadSessions(); refreshDetail(); }, 250); }
  function handleSnapshot(snapshot) { state.status = { ...state.status, ...snapshot }; state.legs = snapshot.legs || state.legs; renderStatus(); renderLegs(); }
  async function loadSessions(cursor = null, direction = "next") { try { const query = new URLSearchParams({ limit: "50" }); if (cursor) query.set("cursor", cursor); const response = await getJSON(`/api/v1/sessions?${query}`); state.sessions = response.items || []; state.nextCursor = response.next_cursor || null; if (direction === "next") state.previousCursor = cursor; renderSessions(); $("#sessions-error").hidden = true; } catch (_) { $("#sessions-error").hidden = false; text($("#sessions-error"), "Sessions unavailable"); } }
  async function loadInitial() { try { const [status, legs, sessions] = await Promise.all([getJSON("/api/v1/status"), getJSON("/api/v1/legs"), getJSON("/api/v1/sessions?limit=50")]); state.status = status; state.legs = legs.items || []; state.sessions = sessions.items || []; state.nextCursor = sessions.next_cursor || null; renderStatus(); renderLegs(); renderSessions(); } catch (_) { $("#sessions-error").hidden = false; text($("#sessions-error"), "Telemetry unavailable"); } }
  function connectEvents() { const source = new EventSource("/api/v1/events"); state.eventSource = source; source.onopen = () => setConnectionState("Live"); source.onerror = () => setConnectionState(source.readyState === EventSource.CLOSED ? "Disconnected" : "Reconnecting"); source.addEventListener("snapshot", (event) => { try { handleSnapshot(JSON.parse(event.data)); setConnectionState("Live"); } catch (_) {} }); source.addEventListener("lifecycle", () => scheduleSessionsRefresh()); source.addEventListener("reset", () => loadInitial()); }
  document.addEventListener("keydown", (event) => { if (event.key === "Escape" && $("#detail-drawer").classList.contains("is-open")) closeDetail(); });
  document.addEventListener("DOMContentLoaded", () => { document.querySelectorAll(".nav-item").forEach((item) => item.addEventListener("click", () => showView(item.dataset.view))); $("#detail-close").addEventListener("click", closeDetail); $("#detail-backdrop").addEventListener("click", closeDetail); $("#sessions-next").addEventListener("click", () => loadSessions(state.nextCursor, "next")); $("#sessions-prev").addEventListener("click", () => loadSessions(state.previousCursor, "previous")); loadInitial().finally(connectEvents); });
})();
