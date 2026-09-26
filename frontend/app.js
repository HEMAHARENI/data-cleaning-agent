/* DataGuard frontend — vanilla JS, no build step. */
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const API = "/api";
const STEPS = ["load", "profile", "normalize", "duplicates", "outliers", "imputation", "validate", "write"];
const STEP_LABELS = { load: "Load", profile: "Profile", normalize: "Normalize", duplicates: "Duplicates", outliers: "Outliers", imputation: "Impute", validate: "Validate", write: "Write" };

const state = { dataset: null, keys: new Set(), rules: [], colStrategies: {}, job: null, poll: null, report: null };

/* ------------------------------------------------------------ utilities */
const esc = (v) => String(v).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmtInt = (n) => (n === null || n === undefined ? "—" : Number(n).toLocaleString());
const fmtNum = (n) => {
  if (n === null || n === undefined || n === "") return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return esc(n);
  return Math.abs(x) >= 1000 ? x.toLocaleString(undefined, { maximumFractionDigits: 0 }) : x.toLocaleString(undefined, { maximumFractionDigits: 2 });
};
const cell = (v) => (v === null || v === undefined ? '<span class="null">null</span>' : esc(v));

function toast(msg, isError = false) {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast" + (isError ? " error" : "");
  t.hidden = false;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => (t.hidden = true), isError ? 6000 : 3000);
}

async function api(path, opts = {}) {
  const res = await fetch(API + path, { headers: { "Content-Type": "application/json" }, ...opts });
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) { /* not JSON */ }
    throw new Error(detail);
  }
  return res.json();
}

function tableHTML(columns, rows, numericCols = new Set()) {
  const head = "<thead><tr>" + columns.map((c) => `<th>${esc(c)}</th>`).join("") + "</tr></thead>";
  const body = "<tbody>" + rows.map((r) => "<tr>" + r.map((v, i) => `<td class="${numericCols.has(columns[i]) ? "num" : ""}">${cell(v)}</td>`).join("") + "</tr>").join("") + "</tbody>";
  return head + body;
}

/* ------------------------------------------------------- cluster status */
async function refreshHealth() {
  try {
    const h = await api("/health");
    const d = h.dask;
    $("#thrLabel").textContent = h.threshold_mb;
    const pill = $("#clusterPill");
    if (d.status === "running") {
      pill.className = "pill running";
      pill.innerHTML = `Dask: ${d.workers} workers · ${d.threads} threads · ${d.memory_gb} GB`;
      pill.title = `${d.mode} cluster at ${d.scheduler}\nDashboard: ${d.dashboard}`;
    } else {
      pill.className = "pill";
      pill.textContent = `Dask: idle (${d.mode}) · switch at ${h.threshold_mb} MB`;
      pill.title = d.note || "";
    }
  } catch (_) {
    $("#clusterPill").textContent = "API offline";
  }
}

/* ------------------------------------------------------------- step 1 */
function uploadFile(file) {
  const status = $("#uploadStatus");
  const bar = $(".upload-bar");
  bar.hidden = false;
  status.textContent = `Uploading ${file.name}…`;
  const xhr = new XMLHttpRequest();
  xhr.open("POST", API + "/datasets");
  xhr.upload.onprogress = (e) => {
    if (e.lengthComputable) $("#uploadFill").style.width = `${(100 * e.loaded) / e.total}%`;
  };
  xhr.onload = () => {
    bar.hidden = true;
    $("#uploadFill").style.width = "0";
    let data = {};
    try { data = JSON.parse(xhr.responseText); } catch (_) { /* ignore */ }
    if (xhr.status >= 200 && xhr.status < 300) {
      status.textContent = `Uploaded ${file.name}`;
      setDataset(data);
    } else {
      status.textContent = "Upload failed";
      toast(data.detail || `Upload failed (${xhr.status})`, true);
    }
  };
  xhr.onerror = () => { bar.hidden = true; toast("Network error during upload", true); };
  const fd = new FormData();
  fd.append("file", file);
  xhr.send(fd);
}

async function generateSample() {
  const rows = $("#sampleRows").value;
  const btn = $("#sampleBtn");
  btn.disabled = true;
  btn.textContent = rows > 500000 ? "Generating (≈30s)…" : "Generating…";
  try {
    setDataset(await api(`/datasets/sample?rows=${rows}`, { method: "POST" }));
    $("#uploadStatus").textContent = "Sample dataset generated";
  } catch (e) {
    toast(e.message, true);
  } finally {
    btn.disabled = false;
    btn.textContent = "Generate";
  }
}

function setDataset(ds) {
  state.dataset = ds;
  state.keys.clear();
  state.rules = [];
  state.colStrategies = {};
  $("#datasetInfo").hidden = false;
  $("#dsName").textContent = ds.filename;
  $("#dsName").title = ds.filename;
  $("#dsSize").textContent = ds.size_mb < 0.01 ? `${ds.size_bytes} B` : `${ds.size_mb} MB`;
  $("#dsCols").textContent = ds.columns.length;
  const eng = $("#dsEngine");
  eng.textContent = ds.engine;
  eng.className = "badge" + (ds.engine === "dask" ? " dask" : "");
  $("#dsEngineReason").textContent = ds.engine_reason;
  $("#schemaList").innerHTML = ds.columns.map((c) =>
    `<span class="${c.original !== c.name ? "renamed" : ""}" title="${c.original !== c.name ? `renamed from “${esc(c.original)}”` : ""}"><b>${esc(c.name)}</b> <i>${esc(c.inferred_type)}</i></span>`).join("");
  $("#rawPreview").innerHTML = tableHTML(ds.preview.columns, ds.preview.rows);
  buildConfigForColumns();
  $("#stepConfig").setAttribute("aria-disabled", "false");
  $("#runBtn").disabled = false;
  $("#runHint").textContent = `Ready: ${ds.filename} will run on ${ds.engine === "dask" ? "the Dask cluster" : "Pandas"} unless you force an engine.`;
}

/* ------------------------------------------------------------- step 2 */
function colNames() { return state.dataset ? state.dataset.columns.map((c) => c.name) : []; }
function colType(name) { const c = state.dataset.columns.find((x) => x.name === name); return c ? c.inferred_type : ""; }

function buildConfigForColumns() {
  const names = colNames();
  $("#keyChips").innerHTML = names.map((n) => `<span class="chip" data-col="${esc(n)}" role="button" tabindex="0">${esc(n)}</span>`).join("");
  $$("#keyChips .chip").forEach((ch) => {
    const toggle = () => {
      const c = ch.dataset.col;
      state.keys.has(c) ? state.keys.delete(c) : state.keys.add(c);
      ch.classList.toggle("on", state.keys.has(c));
    };
    ch.addEventListener("click", toggle);
    ch.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); } });
  });
  $("#colStrategies").innerHTML = names.map((n) => `
    <label>${esc(n)}
      <select data-col="${esc(n)}">
        <option value="">default</option><option value="mean">mean</option><option value="median">median</option>
        <option value="mode">mode</option><option value="ffill">forward fill</option><option value="knn">KNN</option>
        <option value="drop_rows">drop rows</option><option value="none">leave missing</option><option value="constant">constant…</option>
      </select></label>`).join("");
  $$("#colStrategies select").forEach((sel) => sel.addEventListener("change", () => {
    const c = sel.dataset.col;
    if (sel.value === "constant") {
      const v = prompt(`Constant fill value for ${c}:`, "Unknown");
      if (v === null) { sel.value = ""; delete state.colStrategies[c]; return; }
      state.colStrategies[c] = `constant:${v}`;
      sel.options[sel.selectedIndex].textContent = `constant: ${v}`;
    } else if (sel.value) state.colStrategies[c] = sel.value;
    else delete state.colStrategies[c];
  }));
  renderRules();
}

const RULE_PARAMS = {
  not_null: [], unique: [],
  range: [["min", "min"], ["max", "max"]],
  regex: [["pattern", "regex pattern"]],
  allowed_values: [["values", "comma-separated values"]],
  min_length: [["min", "min length"]],
  less_or_equal: [["other", "other column (must be ≥)"]],
};

function renderRules() {
  const names = colNames();
  const box = $("#rules");
  if (!state.rules.length) {
    box.innerHTML = '<p class="muted small-text">No custom rules yet.</p>';
    return;
  }
  box.innerHTML = state.rules.map((r, i) => `
    <div class="rule" data-i="${i}">
      <select data-f="column" aria-label="Column">${names.map((n) => `<option ${n === r.column ? "selected" : ""}>${esc(n)}</option>`).join("")}</select>
      <select data-f="rule" aria-label="Rule type">${Object.keys(RULE_PARAMS).map((t) => `<option value="${t}" ${t === r.rule ? "selected" : ""}>${t.replace("_", " ")}</option>`).join("")}</select>
      <div class="params">${RULE_PARAMS[r.rule].map(([k, ph]) => `<input data-p="${k}" placeholder="${ph}" value="${esc(Array.isArray(r[k]) ? r[k].join(", ") : r[k] ?? "")}" />`).join("") || '<span class="muted small-text">no parameters</span>'}</div>
      <button class="btn ghost small" data-remove type="button" aria-label="Remove rule">✕</button>
    </div>`).join("");
  $$(".rule", box).forEach((el) => {
    const r = state.rules[+el.dataset.i];
    $$("select", el).forEach((s) => s.addEventListener("change", () => { r[s.dataset.f] = s.value; if (s.dataset.f === "rule") renderRules(); }));
    $$("input", el).forEach((inp) => inp.addEventListener("input", () => {
      r[inp.dataset.p] = inp.dataset.p === "values" ? inp.value.split(",").map((x) => x.trim()).filter(Boolean) : inp.value;
    }));
    $("[data-remove]", el).addEventListener("click", () => { state.rules.splice(+el.dataset.i, 1); renderRules(); });
  });
}

function suggestRules() {
  if (!state.dataset) return;
  const out = [];
  for (const c of state.dataset.columns) {
    const n = c.name;
    if (n === "id" || n.endsWith("_id")) out.push({ column: n, rule: "unique" });
    if (c.inferred_type === "email") out.push({ column: n, rule: "regex", pattern: "[^@\\s]+@[^@\\s]+\\.[a-z]{2,}" });
    if (c.inferred_type === "numeric" && /age/.test(n)) out.push({ column: n, rule: "range", min: "0", max: "120" });
    else if (c.inferred_type === "numeric" && /(price|income|amount|salary|qty|quantity|revenue|cost)/.test(n)) out.push({ column: n, rule: "range", min: "0", max: "" });
    if (c.inferred_type === "numeric" && /(rating|score)/.test(n)) out.push({ column: n, rule: "range", min: "0", max: "5" });
    if (c.inferred_type === "text" && /name/.test(n)) out.push({ column: n, rule: "min_length", min: "2" });
  }
  const seen = new Set(state.rules.map((r) => r.column + r.rule));
  const added = out.filter((r) => !seen.has(r.column + r.rule));
  state.rules.push(...added);
  renderRules();
  toast(added.length ? `Added ${added.length} suggested rule(s) — review them before running` : "No new rules to suggest");
}

function collectConfig() {
  const k = $("#cfgK").value;
  return {
    engine: $("#cfgEngine").value,
    normalize: { standardize_columns: $("#cfgStdCols").checked, coerce_types: $("#cfgCoerce").checked, canonicalize_categories: $("#cfgCanon").checked },
    duplicates: { exact: $("#cfgExact").checked, near: $("#cfgNear").checked, ignore_id_columns: $("#cfgIgnoreIds").checked, key_columns: [...state.keys] },
    outliers: { enabled: $("#cfgOutliers").checked, method: $("#cfgMethod").value, action: $("#cfgAction").value, threshold: k ? Number(k) : null },
    imputation: { enabled: $("#cfgImpute").checked, strategy: $("#cfgStrategy").value, drop_threshold: Number($("#cfgDrop").value), column_strategies: state.colStrategies },
    validation: { rules: state.rules.map((r) => ({ ...r })), quarantine: $("#cfgQuarantine").checked },
    output: { format: $("#cfgFormat").value },
  };
}

/* ------------------------------------------------------------- step 3 */
function renderStepper(current, status) {
  const idx = current === "done" ? STEPS.length : Math.max(0, STEPS.indexOf(current));
  $("#stepper").innerHTML = STEPS.map((s, i) => {
    let cls = "";
    if (status === "failed" && i === idx) cls = "failed";
    else if (i < idx || status === "completed") cls = "done";
    else if (i === idx) cls = "active";
    return `<li class="${cls}">${STEP_LABELS[s]}</li>`;
  }).join("");
}

async function runJob() {
  if (!state.dataset) return;
  const cfg = collectConfig();
  const bad = cfg.validation.rules.find((r) => r.rule === "range" && r.min === "" && r.max === "");
  if (bad) { toast(`Range rule on ${bad.column} needs a min or max`, true); return; }
  $("#runBtn").disabled = true;
  try {
    const job = await api("/jobs", { method: "POST", body: JSON.stringify({ dataset_id: state.dataset.dataset_id, config: cfg }) });
    state.job = job;
    $("#stepRun").hidden = false;
    $("#results").hidden = true;
    $("#logBox").innerHTML = "";
    $("#progressFill").style.width = "0";
    renderStepper("load", "running");
    $("#stepRun").scrollIntoView({ behavior: "smooth", block: "start" });
    pollJob(job.id);
  } catch (e) {
    toast(e.message, true);
    $("#runBtn").disabled = false;
  }
}

function pollJob(id) {
  clearInterval(state.poll);
  const tick = async () => {
    let j;
    try { j = await api(`/jobs/${id}`); } catch (e) { toast(e.message, true); clearInterval(state.poll); return; }
    $("#progressFill").style.width = `${j.progress}%`;
    const eng = $("#runEngine");
    eng.textContent = j.engine || "";
    eng.className = "badge" + (j.engine === "dask" ? " dask" : "");
    $("#runSub").textContent = `${j.filename} · job ${j.id} · ${j.status}`;
    renderStepper(j.step, j.status);
    $("#logBox").innerHTML = j.logs.map((l) =>
      `<span class="t">[${String(l.t.toFixed ? l.t.toFixed(1) : l.t).padStart(6)}s]</span> <span class="${l.step === "error" || l.step === "trace" ? "err" : ""}">${esc(l.message)}</span>`).join("\n");
    $("#logBox").scrollTop = $("#logBox").scrollHeight;
    if (j.status === "completed" || j.status === "failed") {
      clearInterval(state.poll);
      $("#runBtn").disabled = false;
      refreshHealth();
      if (j.status === "completed") await showReport(id);
      else toast(`Job failed: ${j.error}`, true);
    }
  };
  tick();
  state.poll = setInterval(tick, 700);
}

/* -------------------------------------------------------------- results */
function gauge(value, label) {
  const r = 58, c = 2 * Math.PI * r, v = Math.max(0, Math.min(100, value));
  const color = v >= 95 ? "var(--good)" : v >= 80 ? "var(--warn)" : "var(--bad)";
  return `<svg viewBox="0 0 140 140" role="img" aria-label="${label} ${v}">
      <circle cx="70" cy="70" r="${r}" fill="none" stroke="var(--surface-2)" stroke-width="12"/>
      <circle cx="70" cy="70" r="${r}" fill="none" stroke="${color}" stroke-width="12" stroke-linecap="round"
        stroke-dasharray="${(c * v) / 100} ${c}" transform="rotate(-90 70 70)"/>
      <text x="70" y="68" text-anchor="middle" font-size="28" font-weight="700" fill="var(--text)">${v.toFixed(1)}</text>
      <text x="70" y="90" text-anchor="middle" font-size="12" fill="var(--muted)">/ 100</text>
    </svg><div class="lbl">${label}</div>`;
}

async function showReport(id) {
  let rep;
  try { rep = await api(`/jobs/${id}/report`); } catch (e) { toast(e.message, true); return; }
  state.report = rep;
  const s = rep.summary;
  $("#results").hidden = false;
  $("#resSub").innerHTML = `Processed on <span class="badge ${rep.engine === "dask" ? "dask" : ""}">${rep.engine}</span> (${rep.partitions} partition${rep.partitions === 1 ? "" : "s"}) in ${rep.timings_s.total}s · ${esc(rep.engine_reason)}`;
  $("#dlCleaned").href = `${API}/jobs/${id}/download/cleaned`;
  $("#dlReport").href = `${API}/jobs/${id}/download/report`;
  const q = $("#dlQuarantine");
  q.href = `${API}/jobs/${id}/download/quarantine`;
  q.classList.toggle("disabled", !rep.outputs.quarantine);
  q.textContent = rep.outputs.quarantine ? `Quarantined rows (${fmtInt(s.rows_quarantined)})` : "No quarantined rows";

  $("#scoreBefore").innerHTML = gauge(rep.score_before.overall, "Quality score — raw");
  $("#scoreAfter").innerHTML = gauge(rep.score_after.overall, "Quality score — validated");
  $("#metricBars").innerHTML = ["completeness", "uniqueness", "validity"].map((m) => `
    <div class="mbar"><div class="top"><span>${m[0].toUpperCase() + m.slice(1)}</span><span>${rep.score_before[m]} → <b>${rep.score_after[m]}</b></span></div>
      <div class="track"><div class="before" style="width:${rep.score_before[m]}%"></div><div class="after" style="width:${rep.score_after[m]}%"></div></div></div>`).join("") +
    '<div class="legend"><span><i style="background:var(--warn);opacity:.55"></i>raw</span><span><i style="background:var(--good)"></i>validated</span></div>';

  const kpi = (v, k) => `<div class="kpi"><div class="v">${v}</div><div class="k">${k}</div></div>`;
  $("#kpis").innerHTML = [
    kpi(`${fmtInt(s.rows_in)} → ${fmtInt(s.rows_out)}`, "rows in → validated"),
    kpi(fmtInt(s.duplicates_removed), "duplicate rows removed"),
    kpi(fmtInt(s.cells_normalized), "cells normalised"),
    kpi(fmtInt(s.outliers_treated), `outliers (${rep.steps.outliers ? rep.steps.outliers.action : "off"})`),
    kpi(fmtInt(s.cells_imputed), "cells imputed"),
    kpi(fmtInt(s.rows_quarantined), "rows quarantined"),
  ].join("");

  renderIssues();
  renderColumns(rep);
  renderValidation(rep);
  renderPreview(rep);
  renderPerf(rep);
  const steps = [...new Set(rep.issues.map((i) => i.step))];
  $("#issueFilter").innerHTML = '<option value="">All steps</option>' + steps.map((x) => `<option>${x}</option>`).join("");
  $("#results").scrollIntoView({ behavior: "smooth", block: "start" });
}

function renderIssues() {
  const rep = state.report;
  if (!rep) return;
  const f = $("#issueFilter").value, q = $("#issueSearch").value.toLowerCase();
  const rows = rep.issues.filter((i) => (!f || i.step === f) && (!q || `${i.column} ${i.issue} ${i.action}`.toLowerCase().includes(q)));
  $("#issuesTable").innerHTML = "<thead><tr><th>Step</th><th>Column</th><th>Issue detected</th><th>Count</th><th>Resolution</th></tr></thead><tbody>" +
    (rows.length ? rows.map((i) => `<tr><td><span class="tag ${i.step}">${i.step}</span></td><td>${esc(i.column)}</td><td class="wrap">${esc(i.issue)}</td><td class="num">${fmtInt(i.count)}</td><td class="wrap">${esc(i.action)}</td></tr>`).join("")
      : '<tr><td colspan="5" class="muted">No matching issues.</td></tr>') + "</tbody>";
}

function missBar(pct, after = false) {
  return `<div class="miss"><div class="bar ${after ? "after" : ""}"><div style="width:${Math.min(100, pct)}%"></div></div><span>${pct}%</span></div>`;
}

function renderColumns(rep) {
  const renamed = rep.steps.normalize.renamed || {};
  const types = rep.steps.normalize.column_types || {};
  const after = Object.fromEntries(rep.profile_after.columns.map((c) => [c.name, c]));
  const dropped = new Set((rep.steps.imputation || {}).dropped_columns || []);
  const approx = rep.profile_after.columns.some((c) => c.unique_approx) ? " ≈" : "";
  const rows = rep.profile_before.columns.map((b) => {
    const name = renamed[b.name] || b.name;
    const a = after[name];
    return `<tr>
      <td><b>${esc(name)}</b>${name !== b.name ? `<br><small class="muted">was “${esc(b.name)}”</small>` : ""}</td>
      <td>${esc(types[name] || b.dtype)}${a ? ` → <code>${esc(a.dtype)}</code>` : ""}</td>
      <td>${missBar(b.missing_pct)}</td>
      <td>${a ? missBar(a.missing_pct, true) : '<span class="tag">dropped</span>'}</td>
      <td class="num">${fmtInt(b.unique)} → ${a ? fmtInt(a.unique) : "—"}${approx}</td>
      <td class="num">${a && a.min !== undefined ? `${fmtNum(a.min)} – ${fmtNum(a.max)}` : "—"}</td>
      <td class="num">${a && a.mean !== undefined ? fmtNum(a.mean) : "—"}</td>
      <td>${dropped.has(name) ? "dropped (too sparse)" : esc(((rep.steps.imputation || {}).columns || {})[name]?.strategy || "")}</td>
    </tr>`;
  }).join("");
  $("#colsTable").innerHTML = "<thead><tr><th>Column</th><th>Type</th><th>Missing (raw)</th><th>Missing (clean)</th><th>Distinct</th><th>Range (clean)</th><th>Mean (clean)</th><th>Imputation</th></tr></thead><tbody>" + rows + "</tbody>";
}

function renderValidation(rep) {
  const v = rep.steps.validation;
  $("#validationList").innerHTML = `<p class="muted small-text">${v.passed}/${v.total} checks passed on the output · ${fmtInt(v.rows_valid)} valid rows · ${fmtInt(v.rows_rejected)} quarantined</p>` +
    v.results.map((r) => {
      const failTxt = r.error ? esc(r.error) : r.passed ? "passed" : `${fmtInt(r.failed)} failing value(s)${r.failed_pct !== undefined ? ` (${r.failed_pct}%)` : ""}`;
      const ex = r.examples && r.examples.length ? `<div class="small-text muted">examples: ${r.examples.map((x) => `<code>${cell(x)}</code>`).join(", ")}</div>` : "";
      const note = !r.passed && r.type !== "builtin" && rep.config.validation.quarantine && !r.error ? '<div class="small-text muted">Checked on the observed values before outlier capping and imputation; failing rows were moved to the quarantine file.</div>' : "";
      return `<div class="vrule ${r.passed ? "pass" : "fail"}"><span class="icon">${r.passed ? "✓" : "!"}</span><div><code>${esc(r.rule)}</code> <span class="tag">${esc(r.type)}</span><div class="small-text">${failTxt}</div>${ex}${note}</div></div>`;
    }).join("");
}

function renderPreview(rep) {
  const numeric = new Set(rep.profile_after.columns.filter((c) => c.mean !== undefined).map((c) => c.name));
  $("#cleanPreview").innerHTML = tableHTML(rep.preview.columns, rep.preview.rows, numeric);
}

function renderPerf(rep) {
  const t = { ...rep.timings_s };
  const total = t.total;
  delete t.total;
  const max = Math.max(...Object.values(t), 0.001);
  $("#perfChart").innerHTML = Object.entries(t).map(([k, v]) =>
    `<div class="perf-row ${rep.engine === "dask" ? "dask" : ""}"><span>${k.replace("_", " ")}</span><div class="track"><div style="width:${(100 * v) / max}%"></div></div><span class="num">${v.toFixed(2)}s</span></div>`).join("");
  $("#perfNote").textContent = `Total ${total}s for ${rep.file_size_mb} MB on ${rep.engine}. On Dask, intermediate results are persisted in cluster memory after each step, so each stage is one distributed pass.`;
}

/* -------------------------------------------------------------- history */
async function openHistory() {
  $("#history").hidden = false;
  const list = await api("/jobs").catch(() => []);
  $("#historyList").innerHTML = list.length ? list.map((j) => `
    <div class="hist" data-id="${j.id}" data-status="${j.status}" tabindex="0">
      <div class="row"><b>${esc(j.filename)}</b><span class="status-${j.status}">${j.status}</span></div>
      <div class="small-text muted">${new Date(j.created_at * 1000).toLocaleString()} · ${j.engine || "?"}${j.score_after ? ` · score ${j.score_before.overall} → ${j.score_after.overall}` : ""}</div>
    </div>`).join("") : '<p class="muted">No jobs yet in this server session.</p>';
  $$(".hist").forEach((h) => h.addEventListener("click", () => {
    $("#history").hidden = true;
    if (h.dataset.status === "completed") showReport(h.dataset.id);
    else { $("#stepRun").hidden = false; pollJob(h.dataset.id); }
  }));
}

/* ----------------------------------------------------------------- wiring */
function wire() {
  const dz = $("#dropzone");
  $("#fileInput").addEventListener("change", (e) => e.target.files[0] && uploadFile(e.target.files[0]));
  ["dragenter", "dragover"].forEach((ev) => dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.add("drag"); }));
  ["dragleave", "drop"].forEach((ev) => dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.remove("drag"); }));
  dz.addEventListener("drop", (e) => e.dataTransfer.files[0] && uploadFile(e.dataTransfer.files[0]));
  $("#sampleBtn").addEventListener("click", generateSample);
  $("#addRule").addEventListener("click", () => {
    if (!state.dataset) return;
    state.rules.push({ column: colNames()[0], rule: "not_null" });
    renderRules();
  });
  $("#suggestRules").addEventListener("click", suggestRules);
  $("#runBtn").addEventListener("click", runJob);
  $("#cfgDrop").addEventListener("input", (e) => ($("#dropVal").textContent = `${Math.round(e.target.value * 100)}%`));
  $("#cfgMethod").addEventListener("change", (e) => ($("#thrHint").textContent = `(k = ${{ iqr: "3.0 for IQR", zscore: "3.0 σ", mad: "3.5 robust z" }[e.target.value]})`));
  $$(".tab").forEach((t) => t.addEventListener("click", () => {
    $$(".tab").forEach((x) => x.classList.toggle("active", x === t));
    $$(".tab-panel").forEach((p) => (p.hidden = p.id !== `tab-${t.dataset.tab}`));
  }));
  $("#issueFilter").addEventListener("change", renderIssues);
  $("#issueSearch").addEventListener("input", renderIssues);
  $("#historyBtn").addEventListener("click", openHistory);
  $("#closeHistory").addEventListener("click", () => ($("#history").hidden = true));
  renderRules();
  refreshHealth();
  setInterval(refreshHealth, 8000);
}

document.addEventListener("DOMContentLoaded", wire);
