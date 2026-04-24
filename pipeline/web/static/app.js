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
let currentRunId = null;

const labels = {
  not_started: "не запускался",
  running: "в работе",
  completed: "готово",
  failed: "ошибка"
};

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

function renderSummary(data) {
  const decision = data.decision || {};
  const overview = decision.overview || {};
  const visibleStages = data.stages.filter(stage => !["build_presentation_viz", "build_presentation_notes"].includes(stage.name));
  const completed = visibleStages.filter(s => s.status === "completed").length;
  const month = decision.score_month || data.month || "auto";
  const runLabel = data.run_id && data.run_id !== "default" ? data.run_id : "Демо";
  summary.innerHTML = `
    <div class="metric"><span>Статус</span><strong>${data.running ? "В работе" : "Готов к запуску"}</strong></div>
    <div class="metric"><span>Запуск</span><strong>${runLabel}</strong></div>
    <div class="metric"><span>Период</span><strong>${month}</strong></div>
    <div class="metric"><span>ЛС в скоринге</span><strong>${fmtRows(overview.total_accounts || 0)}</strong></div>
  `;
  stateText.textContent = data.running ? "Расчет выполняется, рекомендации обновятся автоматически" : `Расчет завершен: ${completed}/${visibleStages.length} этапов`;
  errorText.textContent = data.last_error || "";
  runBtn.disabled = data.running;
  uploadBtn.disabled = data.running;
  runBtn.textContent = data.running ? "Расчет выполняется" : "Пересчитать рекомендации";
}

function renderDecision(data) {
  const decision = data.decision || {};
  const overview = decision.overview || {};
  const mix = decision.recommendation_mix || [];
  const topAction = mix.find(item => item.measure !== "no_action") || {};
  decisionOverview.innerHTML = `
    <article class="panel">
      <h3>Решение на ближайший месяц</h3>
      <p>${fmtRows(overview.positive_actions || 0)} ЛС получили активную меру. ${fmtRows(overview.no_action || 0)} ЛС оставлены без активного воздействия.</p>
      <div class="profile-meta">
        <span class="pill action-pill">${fmtPct(overview.positive_actions_share_pct || 0)} активных мер</span>
        <span class="pill no-action">${fmtPct(overview.no_action_share_pct || 0)} no action</span>
      </div>
    </article>
    <article class="panel">
      <h3>Ожидаемый эффект</h3>
      <p>Суммарный прогнозный прирост: около ${fmtRows(overview.expected_incremental_payers || 0)} вероятностных оплат в следующем месяце.</p>
      <div class="profile-meta">
        <span class="pill">Горизонт: T+1</span>
        <span class="pill">Цель: факт оплаты</span>
      </div>
    </article>
    <article class="panel">
      <h3>Главная активная мера</h3>
      <p>${topAction.label || "Активная мера не выбрана"}${topAction.accounts ? `: ${fmtRows(topAction.accounts)} ЛС` : ""}</p>
      <div class="profile-meta">
        <span class="pill">${fmtRows(overview.rule_blocked || 0)} rule-only</span>
        <span class="pill">${fmtRows(overview.non_positive_uplift || 0)} без положительного uplift</span>
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
      <h3>${gate.label}</h3>
      <p>${gate.rationale}</p>
      <div class="profile-meta">
        <span class="pill">${fmtRows(gate.accounts)} ЛС</span>
        <span class="pill">${fmtPct(gate.share_pct)} портфеля</span>
        <span class="pill">${fmtPct(gate.ml_eligible_share_pct)} ML-eligible</span>
      </div>
    </article>
  `).join("");
}

function renderProfiles(items) {
  if (!items || !items.length) {
    profiles.innerHTML = `<div class="empty">Профили появятся после расчета рекомендаций.</div>`;
    return;
  }
  profiles.innerHTML = items.slice(0, 12).map(item => {
    const isAction = item.top_recommendation && item.top_recommendation !== "no_action";
    return `
      <article class="profile-card">
        <div class="profile-head">
          <div>
            <h3 class="profile-title">${item.business_gate_label}</h3>
            <div class="profile-meta">
              <span class="pill">${item.profile_label}</span>
              <span class="pill">${fmtRows(item.accounts)} ЛС</span>
              <span class="pill">${fmtPct(item.share_pct)} портфеля</span>
            </div>
          </div>
          <span class="pill ${isAction ? "action-pill" : "no-action"}">${item.top_recommendation_label}</span>
        </div>
        <div class="profile-body">
          <p><strong>Обоснование.</strong> ${item.rationale}</p>
          <p><strong>Экономика и риск.</strong> ${item.economic_effect} ${item.business_constraints}</p>
        </div>
        <div class="mini-metrics">
          <div class="mini"><span>Рекомендация</span><strong>${fmtPct(item.top_recommendation_share_pct)}</strong></div>
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
    modelQuality.innerHTML = `<div class="empty">Метрики модели появятся после uplift-этапа.</div>`;
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
          <th>Вердикт и ограничения</th>
        </tr>
      </thead>
      <tbody>
        ${items.map(item => `
          <tr>
            <td><strong>${item.measure_label}</strong></td>
            <td>${fmtPct(item.ate_dr_pct)}</td>
            <td>${fmtPct(item.naive_ate_pct)}</td>
            <td>${fmtRows(item.train_rows)} строк, treatment ${fmtPct(item.treated_share_pct)}</td>
            <td>${item.verdict}. ${item.constraints}</td>
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
      <h3>${stage.title}</h3>
      <p>${stage.description}</p>
      <span class="badge ${stage.status}">${labels[stage.status] || stage.status}</span>
      <div class="duration">${stage.duration_sec ? `${stage.duration_sec} сек.` : ""}</div>
      ${stage.error ? `<div class="error">${stage.error}</div>` : ""}
    </article>
  `).join("");
}

function renderTables(items) {
  const visible = (items || []).filter(item => ["segmentation", "uplift", "mart", "normalized"].includes(item.layer));
  if (!visible.length) {
    tables.innerHTML = `<div class="empty">Таблицы появятся после запуска.</div>`;
    return;
  }
  tables.innerHTML = visible.map(item => `
    <div class="row">
      <a href="${item.url}" target="_blank">${item.name}</a>
      <span>${item.layer}</span>
      <span>${fmtRows(item.rows)} строк</span>
      <span>${item.size_mb} MB</span>
    </div>
  `).join("");
}

async function refresh() {
  const statusUrl = currentRunId ? `/api/runs/${currentRunId}/status` : "/api/status";
  const response = await fetch(statusUrl);
  const data = await response.json();
  renderSummary(data);
  renderDecision(data);
  renderGates(data.decision.gates);
  renderProfiles(data.decision.profiles);
  renderModelQuality(data.decision.model_quality);
  renderStages(data.stages);
  renderTables(data.outputs.tables);
}

runBtn.addEventListener("click", async () => {
  currentRunId = null;
  runBtn.disabled = true;
  uploadMessage.textContent = "Запущен расчет на базовом пакете.";
  uploadMessage.classList.remove("error");
  await fetch("/api/run", { method: "POST" });
  refresh();
});

uploadForm.addEventListener("submit", async event => {
  event.preventDefault();
  let createdRun = false;
  uploadBtn.disabled = true;
  uploadMessage.textContent = "Файлы загружаются, создается отдельный запуск...";
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
    uploadForm.reset();
    uploadMessage.textContent = `Запуск ${payload.run_id} создан. Рекомендации обновятся после завершения расчета.`;
    await refresh();
  } catch (error) {
    uploadMessage.textContent = error.message;
    uploadMessage.classList.add("error");
  } finally {
    if (!createdRun) uploadBtn.disabled = false;
  }
});

refresh();
setInterval(refresh, 2000);
