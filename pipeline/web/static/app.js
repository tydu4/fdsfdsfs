const runBtn = document.getElementById("runBtn");
const uploadForm = document.getElementById("uploadForm");
const uploadBtn = document.getElementById("uploadBtn");
const uploadMessage = document.getElementById("uploadMessage");
const summary = document.getElementById("summary");
const pipeline = document.getElementById("pipeline");
const tables = document.getElementById("tables");
const decisionOverview = document.getElementById("decisionOverview");
const gateGrid = document.getElementById("gateGrid");
const profiles = document.getElementById("profiles");
const modelQuality = document.getElementById("modelQuality");
const stateText = document.getElementById("stateText");
const errorText = document.getElementById("errorText");
const debtorsMeta = document.getElementById("debtorsMeta");
const debtorsBody = document.getElementById("debtorsBody");
const debtorsEmpty = document.getElementById("debtorsEmpty");
const debtorsPrev = document.getElementById("debtorsPrev");
const debtorsNext = document.getElementById("debtorsNext");
const debtorsPageInfo = document.getElementById("debtorsPageInfo");
const debtorsSearch = document.getElementById("debtorsSearch");
const debtorsLimit = document.getElementById("debtorsLimit");

let currentRunId = null;
let latestStatus = null;
let searchTimer = null;
let refreshInFlight = false;

const debtorState = {
  query: "",
  offset: 0,
  limit: Number(debtorsLimit?.value || 50),
  total: 0,
  datasetKey: "",
  lastRequestKey: "",
  loading: false
};

const labels = {
  not_started: "ожидание",
  running: "в работе",
  completed: "готово",
  failed: "ошибка"
};

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function fmtRows(value) {
  if (value === null || value === undefined) return "n/a";
  return Number(value).toLocaleString("ru-RU");
}

function fmtMoney(value) {
  if (value === null || value === undefined) return "n/a";
  return `${Number(value).toLocaleString("ru-RU", { maximumFractionDigits: 0 })} ₽`;
}

function fmtPct(value) {
  if (value === null || value === undefined) return "n/a";
  return `${Number(value).toLocaleString("ru-RU", { maximumFractionDigits: 2 })}%`;
}

function pageCount(total, limit) {
  if (!total || !limit) return 0;
  return Math.max(1, Math.ceil(total / limit));
}

function humanizeFetchError(error) {
  const message = String(error?.message || error || "").trim();
  if (!message) return "Не удалось обновить данные.";
  if (/failed to fetch/i.test(message)) {
    return "Не удалось получить статус расчета. Проверьте, что сервер интерфейса запущен.";
  }
  return message;
}

function renderSummary(data) {
  const decision = data.decision || {};
  const overview = decision.overview || {};
  const visibleStages = data.stages.filter(stage => !["build_presentation_viz", "build_presentation_notes"].includes(stage.name));
  const completed = visibleStages.filter(s => s.status === "completed").length;
  const hasRun = Boolean(data.started_at || data.finished_at);
  const month = decision.score_month || data.month || "—";
  const runLabel = data.run_id && data.run_id !== "default" ? data.run_id : "Базовый";

  summary.innerHTML = `
    <div class="metric"><span>Статус</span><strong>${data.running ? "Идет расчет" : hasRun ? "Готово" : "Ожидание"}</strong></div>
    <div class="metric"><span>Запуск</span><strong>${esc(runLabel)}</strong></div>
    <div class="metric"><span>Период</span><strong>${esc(month)}</strong></div>
    <div class="metric"><span>ЛС в расчете</span><strong>${fmtRows(overview.total_accounts || 0)}</strong></div>
  `;

  if (data.running) {
    stateText.textContent = "Идет расчет. Данные обновляются автоматически.";
  } else if (!hasRun) {
    stateText.textContent = "Ожидание запуска.";
  } else {
    stateText.textContent = `Этапов выполнено: ${completed}/${visibleStages.length}`;
  }

  errorText.textContent = data.last_error || "";
  runBtn.disabled = data.running;
  uploadBtn.disabled = data.running;
  runBtn.textContent = data.running ? "Расчет выполняется" : hasRun ? "Пересчитать" : "Запустить расчет";
}

function renderDecision(data) {
  const decision = data.decision || {};
  const overview = decision.overview || {};
  const mix = decision.recommendation_mix || [];
  const topAction = mix.find(item => item.measure !== "no_action") || {};

  decisionOverview.innerHTML = `
    <article class="panel">
      <h3>Активные меры</h3>
      <p>${fmtRows(overview.positive_actions || 0)} ЛС получили активную меру. ${fmtRows(overview.no_action || 0)} ЛС без активного действия.</p>
      <div class="profile-meta">
        <span class="pill action-pill">${fmtPct(overview.positive_actions_share_pct || 0)} активных</span>
        <span class="pill no-action">${fmtPct(overview.no_action_share_pct || 0)} без действия</span>
      </div>
    </article>
    <article class="panel">
      <h3>Ожидаемый эффект</h3>
      <p>Прогноз: около ${fmtRows(overview.expected_incremental_payers || 0)} дополнительных оплат в следующем месяце.</p>
      <div class="profile-meta">
        <span class="pill">Горизонт: 1 месяц</span>
        <span class="pill">Цель: факт оплаты</span>
      </div>
    </article>
    <article class="panel">
      <h3>Главная мера</h3>
      <p>${esc(topAction.label || "Не выбрана")}${topAction.accounts ? `: ${fmtRows(topAction.accounts)} ЛС` : ""}</p>
      <div class="profile-meta">
        <span class="pill">${fmtRows(overview.rule_blocked || 0)} по правилам</span>
        <span class="pill">${fmtRows(overview.non_positive_uplift || 0)} без эффекта</span>
      </div>
    </article>
  `;
}

function renderGates(gates) {
  if (!gates || !gates.length) {
    gateGrid.innerHTML = `<div class="empty">Сегменты появятся после расчета.</div>`;
    return;
  }
  gateGrid.innerHTML = gates.map(gate => `
    <article class="panel">
      <h3>${esc(gate.label)}</h3>
      <p>${esc(gate.rationale)}</p>
      <div class="profile-meta">
        <span class="pill">${fmtRows(gate.accounts)} ЛС</span>
        <span class="pill">${fmtPct(gate.share_pct)} портфеля</span>
        <span class="pill">${fmtPct(gate.ml_eligible_share_pct)} ML-доступно</span>
      </div>
    </article>
  `).join("");
}

function renderProfiles(items) {
  if (!items || !items.length) {
    profiles.innerHTML = `<div class="empty">Профили появятся после расчета.</div>`;
    return;
  }
  profiles.innerHTML = items.slice(0, 12).map(item => {
    const isAction = item.top_recommendation && item.top_recommendation !== "no_action";
    return `
      <article class="profile-card">
        <div class="profile-head">
          <div>
            <h3 class="profile-title">${esc(item.business_gate_label)}</h3>
            <div class="profile-meta">
              <span class="pill">${esc(item.profile_label)}</span>
              <span class="pill">${fmtRows(item.accounts)} ЛС</span>
              <span class="pill">${fmtPct(item.share_pct)} портфеля</span>
            </div>
          </div>
          <span class="pill ${isAction ? "action-pill" : "no-action"}">${esc(item.top_recommendation_label)}</span>
        </div>
        <div class="profile-body">
          <p><strong>Обоснование.</strong> ${esc(item.rationale)}</p>
          <p><strong>Экономика.</strong> ${esc(item.economic_effect)} ${esc(item.business_constraints)}</p>
        </div>
        <div class="mini-metrics">
          <div class="mini"><span>Доля меры</span><strong>${fmtPct(item.top_recommendation_share_pct)}</strong></div>
          <div class="mini"><span>Средний долг</span><strong>${fmtMoney(item.avg_debt)}</strong></div>
          <div class="mini"><span>Оплаты 3м</span><strong>${fmtMoney(item.avg_payment_3m)}</strong></div>
          <div class="mini"><span>Uplift</span><strong>${fmtPct(item.avg_positive_uplift_pct)}</strong></div>
        </div>
      </article>
    `;
  }).join("");
}

function renderModelQuality(items) {
  if (!items || !items.length) {
    modelQuality.innerHTML = `<div class="empty">Метрики появятся после этапа uplift.</div>`;
    return;
  }
  modelQuality.innerHTML = `
    <table class="model-table">
      <thead>
        <tr>
          <th>Мера</th>
          <th>DR ATE</th>
          <th>Naive ATE</th>
          <th>Обучение</th>
          <th>Вердикт</th>
        </tr>
      </thead>
      <tbody>
        ${items.map(item => `
          <tr>
            <td><strong>${esc(item.measure_label)}</strong></td>
            <td>${fmtPct(item.ate_dr_pct)}</td>
            <td>${fmtPct(item.naive_ate_pct)}</td>
            <td>${fmtRows(item.train_rows)} строк, treatment ${fmtPct(item.treated_share_pct)}</td>
            <td>${esc(item.verdict)}. ${esc(item.constraints)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderStages(stages) {
  const visibleStages = stages.filter(stage => !["build_presentation_viz", "build_presentation_notes"].includes(stage.name));
  pipeline.innerHTML = visibleStages.map(stage => `
    <article class="stage ${stage.status}">
      <h3>${esc(stage.title)}</h3>
      <p>${esc(stage.description)}</p>
      <span class="badge ${stage.status}">${labels[stage.status] || stage.status}</span>
      <div class="duration">${stage.duration_sec ? `${stage.duration_sec} сек.` : ""}</div>
      ${stage.error ? `<div class="stage-error">${esc(stage.error)}</div>` : ""}
    </article>
  `).join("");
}

function renderTables(items) {
  const visible = (items || []).filter(item => ["segmentation", "uplift", "mart", "normalized"].includes(item.layer));
  if (!visible.length) {
    tables.innerHTML = `<div class="empty">Файлы появятся после запуска.</div>`;
    return;
  }
  tables.innerHTML = visible.map(item => `
    <div class="row">
      <a href="${item.url}" target="_blank">${esc(item.name)}</a>
      <span>${esc(item.layer)}</span>
      <span>${fmtRows(item.rows)} строк</span>
      <span>${item.size_mb} MB</span>
    </div>
  `).join("");
}

function renderDebtors(payload) {
  const rows = payload.rows || [];
  debtorState.total = Number(payload.total || 0);

  if (!rows.length) {
    debtorsBody.innerHTML = "";
    debtorsEmpty.style.display = "block";
    debtorsMeta.textContent = debtorState.total ? `Найдено: ${fmtRows(debtorState.total)}` : "Нет данных";
    debtorsPageInfo.textContent = "0 / 0";
    debtorsPrev.disabled = true;
    debtorsNext.disabled = true;
    return;
  }

  debtorsEmpty.style.display = "none";
  debtorsBody.innerHTML = rows.map(item => `
    <tr>
      <td><strong>${esc(item.ls_id)}</strong></td>
      <td>${esc(item.business_gate_label)}</td>
      <td>
        <span class="debtor-badge ${item.is_action ? "action-pill" : "no-action"}">${esc(item.recommended_measure_label)}</span>
      </td>
      <td class="${item.recommended_uplift_pct > 0 ? "uplift-positive" : "uplift-neutral"}">${fmtPct(item.recommended_uplift_pct)}</td>
      <td>${fmtMoney(item.debt_start_t)}</td>
      <td>${fmtMoney(item.payment_amount_l3m)}</td>
      <td>${fmtRows(item.contact_channels_count)}</td>
    </tr>
  `).join("");

  const currentPage = Math.floor(debtorState.offset / debtorState.limit) + 1;
  const totalPages = pageCount(debtorState.total, debtorState.limit);
  debtorsPageInfo.textContent = `${currentPage} / ${totalPages}`;
  debtorsMeta.textContent = `Показано ${fmtRows(rows.length)} из ${fmtRows(debtorState.total)}`;
  debtorsPrev.disabled = debtorState.offset <= 0;
  debtorsNext.disabled = debtorState.offset + debtorState.limit >= debtorState.total;
}

function renderDebtorsPlaceholder(message) {
  debtorsBody.innerHTML = "";
  debtorsEmpty.style.display = "block";
  debtorsEmpty.textContent = message;
  debtorsMeta.textContent = "Нет данных";
  debtorsPageInfo.textContent = "0 / 0";
  debtorsPrev.disabled = true;
  debtorsNext.disabled = true;
}

function debtorsBaseUrl() {
  return currentRunId ? `/api/runs/${currentRunId}/debtors` : "/api/debtors";
}

function debtorsDatasetKey(status) {
  return `${status.run_id || "default"}|${status.finished_at || status.started_at || ""}|${status.decision?.overview?.total_accounts || 0}`;
}

async function refreshDebtors(force = false) {
  if (!latestStatus) return;
  if (debtorState.loading) return;
  const hasRows = Number(latestStatus?.decision?.overview?.total_accounts || 0) > 0;
  if (latestStatus.running) {
    renderDebtorsPlaceholder("Расчет выполняется. Таблица обновится после завершения.");
    debtorState.lastRequestKey = "";
    return;
  }
  if (!hasRows) {
    renderDebtorsPlaceholder("Таблица появится после первого расчета.");
    debtorState.lastRequestKey = "";
    return;
  }

  const datasetKey = debtorsDatasetKey(latestStatus);
  if (datasetKey !== debtorState.datasetKey) {
    debtorState.datasetKey = datasetKey;
    debtorState.offset = 0;
    debtorState.lastRequestKey = "";
  }

  const requestKey = `${debtorsBaseUrl()}|${datasetKey}|${debtorState.offset}|${debtorState.limit}|${debtorState.query}`;
  if (!force && debtorState.lastRequestKey === requestKey) return;
  debtorState.lastRequestKey = requestKey;
  debtorState.loading = true;
  debtorsMeta.textContent = "Загрузка таблицы...";

  try {
    const params = new URLSearchParams({
      offset: String(debtorState.offset),
      limit: String(debtorState.limit),
      q: debtorState.query
    });

    const response = await fetch(`${debtorsBaseUrl()}?${params.toString()}`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Не удалось загрузить таблицу должников");
    }
    renderDebtors(payload);
  } catch (error) {
    renderDebtorsPlaceholder(error.message || "Ошибка загрузки таблицы");
  } finally {
    debtorState.loading = false;
  }
}

async function refresh() {
  if (refreshInFlight) return;
  refreshInFlight = true;
  try {
    const statusUrl = currentRunId ? `/api/runs/${currentRunId}/status` : "/api/status";
    const response = await fetch(statusUrl);
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || "Не удалось загрузить статус");
    }

    latestStatus = data;
    renderSummary(data);
    renderDecision(data);
    renderGates(data.decision.gates);
    renderProfiles(data.decision.profiles);
    renderModelQuality(data.decision.model_quality);
    renderStages(data.stages);
    renderTables(data.outputs.tables);
    await refreshDebtors();
  } catch (error) {
    errorText.textContent = humanizeFetchError(error);
    runBtn.disabled = false;
    uploadBtn.disabled = false;
  } finally {
    refreshInFlight = false;
  }
}

runBtn.addEventListener("click", async () => {
  currentRunId = null;
  debtorState.datasetKey = "";
  debtorState.lastRequestKey = "";
  runBtn.disabled = true;
  uploadMessage.textContent = "Запуск расчета...";
  uploadMessage.classList.remove("error");
  try {
    const response = await fetch("/api/run", { method: "POST" });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Не удалось запустить расчет");
    }
    await refresh();
  } catch (error) {
    uploadMessage.textContent = error.message || String(error);
    uploadMessage.classList.add("error");
    runBtn.disabled = false;
  }
});

uploadForm.addEventListener("submit", async event => {
  event.preventDefault();
  let createdRun = false;
  uploadBtn.disabled = true;
  uploadMessage.textContent = "Файлы загружаются...";
  uploadMessage.classList.remove("error");
  try {
    const response = await fetch("/api/runs", {
      method: "POST",
      body: new FormData(uploadForm)
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || payload.error || "Не удалось создать запуск");
    }
    currentRunId = payload.run_id;
    createdRun = true;
    debtorState.datasetKey = "";
    debtorState.lastRequestKey = "";
    uploadForm.reset();
    uploadMessage.textContent = `Запуск ${payload.run_id} создан.`;
    await refresh();
  } catch (error) {
    uploadMessage.textContent = error.message || String(error);
    uploadMessage.classList.add("error");
  } finally {
    if (!createdRun) uploadBtn.disabled = false;
  }
});

debtorsPrev.addEventListener("click", async () => {
  debtorState.offset = Math.max(debtorState.offset - debtorState.limit, 0);
  await refreshDebtors(true);
});

debtorsNext.addEventListener("click", async () => {
  if (debtorState.offset + debtorState.limit >= debtorState.total) return;
  debtorState.offset += debtorState.limit;
  await refreshDebtors(true);
});

debtorsLimit.addEventListener("change", async () => {
  debtorState.limit = Math.max(Number(debtorsLimit.value || 50), 1);
  debtorState.offset = 0;
  await refreshDebtors(true);
});

debtorsSearch.addEventListener("input", () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(async () => {
    debtorState.query = debtorsSearch.value.trim();
    debtorState.offset = 0;
    await refreshDebtors(true);
  }, 280);
});

refresh();
setInterval(refresh, 2000);
