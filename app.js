const DATA_PATH = "./data/four_miners.json";

const state = {
  data: null,
  trendMode: "absolute",
  trendWindow: "all",
};

const charts = {};
const chartByDom = new WeakMap();
const chartResizeObserver =
  typeof ResizeObserver !== "undefined"
    ? new ResizeObserver((entries) => {
        entries.forEach((entry) => {
          const chart = chartByDom.get(entry.target);
          if (chart && typeof chart.resize === "function") {
            chart.resize();
          }
        });
      })
    : null;

function isNum(v) {
  return v !== null && v !== undefined && !Number.isNaN(Number(v));
}

function formatNum(v, digits = 1) {
  if (!isNum(v)) return "暂未披露";
  return Number(v).toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function num(v, digits = 1) {
  if (!isNum(v)) return "暂未披露";
  return Number(v).toFixed(digits);
}

function pct(v, digits = 1) {
  if (!isNum(v)) return "暂未披露";
  return `${Number(v).toFixed(digits)}%`;
}

function wt(v, digits = 1, withUnit = true) {
  if (!isNum(v)) return "暂未披露";
  const out = formatNum(Number(v), digits);
  return withUnit ? `${out} 万吨` : out;
}

function toWtValue(v, digits = 1) {
  if (!isNum(v)) return null;
  return Number((Number(v)).toFixed(digits));
}

function usd(v, unit = "美元/吨", digits = 2) {
  if (!isNum(v)) return "暂未披露";
  return `${Number(v).toFixed(digits)} ${unit}`;
}

function toneClass(value) {
  if (!isNum(value)) return "neutral";
  if (Number(value) > 0) return "up";
  if (Number(value) < 0) return "down";
  return "neutral";
}

function escapeHtml(text) {
  return String(text ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function setTextIfExists(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function compactText(text, maxLen = 14) {
  const src = String(text || "").trim();
  if (!src) return "暂未披露";
  const first = src.split(/[；。]/)[0].trim();
  if (first.length <= maxLen) return first;
  return `${first.slice(0, maxLen)}…`;
}

function bindChartResize(chart, dom) {
  if (!chart || !dom || !chartResizeObserver) return;
  if (chartByDom.get(dom) === chart) return;
  chartByDom.set(dom, chart);
  chartResizeObserver.observe(dom);
}

function scrollToId(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.scrollIntoView({ behavior: "smooth", block: "start" });
}

async function loadData() {
  const res = await fetch(DATA_PATH, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`数据文件读取失败: HTTP ${res.status}`);
  }
  return res.json();
}

function renderSummaryCards(data) {
  const aggregate = data.aggregate || {};
  const container = document.getElementById("summaryCards");
  const guidanceMain = compactText(aggregate.total_guidance_change, 16);
  const guidanceDetail = aggregate.total_guidance_change && aggregate.total_guidance_change !== guidanceMain
    ? aggregate.total_guidance_change
    : "";

  const cards = [
    {
      k: "四巨头合计产量同比变化",
      v: pct(aggregate.total_production_yoy),
      tone: toneClass(aggregate.total_production_yoy),
      sub: `合计产量 ${wt(aggregate.total_production)}`,
      variant: "number",
    },
    {
      k: "四巨头合计销量同比变化",
      v: pct(aggregate.total_sales_yoy),
      tone: toneClass(aggregate.total_sales_yoy),
      sub: `合计销量 ${wt(aggregate.total_sales)}`,
      variant: "number",
    },
    {
      k: "全年年度指引合计变化",
      v: escapeHtml(guidanceMain),
      tone: "neutral",
      sub: `年度指引中值合计 ${wt(aggregate.total_guidance_mid)}`,
      detail: guidanceDetail,
      variant: "text",
    },
    {
      k: "当前完成率",
      v: pct(aggregate.completion_rate),
      tone: toneClass((aggregate.completion_rate || 0) - 70),
      sub: `后续目标 ${wt(aggregate.required_output_next_quarters)}`,
      variant: "number",
    },
    {
      k: "供给扰动等级",
      v: escapeHtml(aggregate.disruption_level || "暂未披露"),
      tone: "neutral",
      sub: "天气/港口/铁路/事故等标签聚合",
      variant: "text",
    },
    {
      k: "对盘面的综合影响",
      v: escapeHtml(aggregate.market_impact || "暂未披露"),
      tone: "neutral",
      sub: "供给端视角，不构成投资建议",
      variant: "text",
    },
  ];

  container.innerHTML = cards
    .map(
      (item) => `
      <div class="summary-card ${item.variant === "text" ? "is-text-card" : "is-number-card"}">
        <div class="k">${item.k}</div>
        <div class="v ${item.tone} ${item.variant === "text" ? "is-text-value" : "is-number-value"}">${item.v}</div>
        ${item.detail ? `<div class="detail">${escapeHtml(item.detail)}</div>` : ""}
        <div class="sub">${item.sub}</div>
      </div>
    `,
    )
    .join("");
}

function renderOverviewTable(data) {
  const tbody = document.querySelector("#overviewTable tbody");
  tbody.innerHTML = "";
  const labels = [
    "公司",
    "报告期",
    "产量(万吨)",
    "产量同比",
    "销量/发运(万吨)",
    "销量同比",
    "年度指引(万吨)",
    "年度指引变化",
    "当前完成率",
    "单位成本",
    "Realized Price",
    "扰动标签",
    "来源",
  ];

  (data.companies || []).forEach((c) => {
    const tr = document.createElement("tr");
    tr.className = "clickable-row";
    tr.dataset.target = `detail-${c.company}`;

    const tags = (c.disruption_tags || []).length
      ? c.disruption_tags.map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("")
      : '<span class="tag">无</span>';

    const source = c.source_url
      ? `<a href="${escapeHtml(c.source_url)}" target="_blank" rel="noopener noreferrer">原文</a>`
      : '<span class="neutral">未提取成功</span>';

    const vals = [
      `<b>${escapeHtml(c.company_cn || c.company)}</b>`,
      `${escapeHtml(c.report_period || "暂未披露")}`,
      `${wt(c.production, 1, false)}`,
      `<span class="${toneClass(c.production_yoy)}">${pct(c.production_yoy)}</span>`,
      `${wt(c.sales, 1, false)}`,
      `<span class="${toneClass(c.sales_yoy)}">${pct(c.sales_yoy)}</span>`,
      `${isNum(c.guidance_low) && isNum(c.guidance_high) ? `${wt(c.guidance_low, 0, false)} - ${wt(c.guidance_high, 0, false)}` : "暂未披露"}`,
      `${escapeHtml(c.guidance_change || "暂未披露")}`,
      `${pct(c.completion_rate)}`,
      `${usd(c.unit_cost)}`,
      `${usd(c.realized_price)}`,
      `<div class="tag-list">${tags}</div>`,
      `${source}`,
    ];

    tr.innerHTML = vals
      .map((val, idx) => `<td data-label="${labels[idx]}">${val}</td>`)
      .join("");

    tr.addEventListener("click", () => scrollToId(tr.dataset.target));
    tbody.appendChild(tr);
  });
}

function renderCompanyJumpCards(data) {
  const container = document.getElementById("companyCards");
  container.innerHTML = "";
  (data.companies || []).forEach((c) => {
    const div = document.createElement("div");
    div.className = "company-jump-card";
    div.innerHTML = `
      <div><b>${escapeHtml(c.company_cn || c.company)}</b></div>
      <div class="note">${escapeHtml(c.report_period || "暂未披露")}</div>
      <div class="note">产量 ${wt(c.production)} | 销量 ${wt(c.sales)}</div>
    `;
    div.addEventListener("click", () => scrollToId(`detail-${c.company}`));
    container.appendChild(div);
  });
}

function buildDetailCharts(c, idx) {
  const trendId = `detail-trend-${idx}`;
  const costId = `detail-cost-${idx}`;
  const trendDom = document.getElementById(trendId);
  const costDom = document.getElementById(costId);

  const history = c.history || [];
  const x = history.map((i) => i.quarter);
  const p = history.map((i) => toWtValue(i.production));
  const s = history.map((i) => toWtValue(i.sales));

  const trendChart = echarts.init(trendDom);
  trendChart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis" },
    legend: { textStyle: { color: "#3b4a61" } },
    grid: { left: 50, right: 20, top: 40, bottom: 40 },
    xAxis: { type: "category", data: x, axisLine: { lineStyle: { color: "#9fb2cc" } } },
    yAxis: {
      type: "value",
      name: "万吨",
      axisLine: { lineStyle: { color: "#9fb2cc" } },
      splitLine: { lineStyle: { color: "#e5ebf2" } },
    },
    series: [
      { name: "产量", type: "line", smooth: true, data: p, color: "#2e6fd8" },
      { name: "销量/发运", type: "line", smooth: true, data: s, color: "#1f9f7a" },
    ],
  });

  const costChart = echarts.init(costDom);
  const hasCost = c.unit_cost !== null || c.realized_price !== null;
  if (!hasCost) {
    costChart.setOption({
      backgroundColor: "transparent",
      graphic: {
        type: "text",
        left: "center",
        top: "middle",
        style: { fill: "#6b7f99", text: "成本/价格字段暂未披露或未提取成功", fontSize: 14 },
      },
    });
  } else {
    costChart.setOption({
      backgroundColor: "transparent",
      tooltip: { trigger: "axis" },
      grid: { left: 40, right: 20, top: 30, bottom: 30 },
      xAxis: { type: "category", data: ["单位成本", "Realized Price"], axisLine: { lineStyle: { color: "#9fb2cc" } } },
      yAxis: { type: "value", axisLine: { lineStyle: { color: "#9fb2cc" } }, splitLine: { lineStyle: { color: "#e5ebf2" } } },
      series: [
        {
          type: "bar",
          data: [c.unit_cost, c.realized_price],
          barWidth: 32,
          itemStyle: {
            color: (params) => (params.dataIndex === 0 ? "#dca63a" : "#2e6fd8"),
          },
          label: { show: true, position: "top", color: "#425675" },
        },
      ],
    });
  }

  charts[`detail-${idx}-trend`] = trendChart;
  charts[`detail-${idx}-cost`] = costChart;
  bindChartResize(trendChart, trendDom);
  bindChartResize(costChart, costDom);
}

function renderCompanyDetails(data) {
  const container = document.getElementById("companyDetailContainer");
  container.innerHTML = "";

  (data.companies || []).forEach((c, idx) => {
    const progress = Math.max(0, Math.min(130, Number(c.completion_rate || 0)));
    const sourceLinks = (c.source_links || [])
      .map((s) => `<a href="${escapeHtml(s.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(s.label || "来源")}</a>`)
      .join(" | ");

    const events = (c.event_timeline || [])
      .map((e) => `<li><b>${escapeHtml(e.date || "")}</b> ${escapeHtml(e.event || "")}（${escapeHtml(e.impact || "中性")}）</li>`)
      .join("");

    const block = document.createElement("article");
    block.className = "detail-card";
    block.id = `detail-${c.company}`;
    block.innerHTML = `
      <div class="detail-top">
        <div>
          <h3>${escapeHtml(c.company_cn || c.company)} <span class="note">${escapeHtml(c.report_period || "")}</span></h3>
          <p class="note">${escapeHtml(c.summary || "暂未披露")}</p>
        </div>
        <div class="detail-kpis">
          <div class="kpi"><div class="k">产量</div><div class="v">${wt(c.production)}</div></div>
          <div class="kpi"><div class="k">销量/发运</div><div class="v">${wt(c.sales)}</div></div>
          <div class="kpi"><div class="k">年度指引中值</div><div class="v">${wt(c.guidance_mid)}</div></div>
        </div>
      </div>

      <div class="progress-wrap">
        <div class="note">年度指引完成率：${pct(c.completion_rate)}；后续需完成 ${wt(c.required_output_next_quarters)}</div>
        <div class="progress-track"><div class="progress-bar" style="width:${progress}%"></div></div>
      </div>

      <div class="note" style="margin-top:10px;">扰动标签：${(c.disruption_tags || []).map((t) => `<span class="tag">${escapeHtml(t)}</span>`).join(" ") || "无"}</div>

      <div class="detail-charts">
        <div>
          <div class="note">产量与销量趋势（万吨）</div>
          <div id="detail-trend-${idx}" class="small-chart"></div>
        </div>
        <div>
          <div class="note">单位成本与Realized Price</div>
          <div id="detail-cost-${idx}" class="small-chart"></div>
        </div>
      </div>

      <h4>关键扰动事件时间轴</h4>
      <ul class="timeline">${events || '<li>暂无显著扰动事件披露</li>'}</ul>

      <h4>管理层表述摘要</h4>
      <p class="note">${escapeHtml(c.management_commentary || "暂未披露")}</p>

      <p class="note">来源：${sourceLinks || "暂未披露"}</p>
    `;

    container.appendChild(block);
    buildDetailCharts(c, idx);
  });
}

function renderTrendChart(data) {
  const dom = document.getElementById("trendChart");
  const chart = charts.trend || echarts.init(dom);
  charts.trend = chart;
  bindChartResize(chart, dom);

  if (state.trendMode === "absolute") {
    const allRows = data.aggregate?.quarterly_trend || [];
    const rows = state.trendWindow === "latest" && allRows.length ? [allRows[allRows.length - 1]] : allRows;
    chart.setOption({
      backgroundColor: "transparent",
      tooltip: { trigger: "axis" },
      legend: { textStyle: { color: "#3b4a61" } },
      grid: { left: 48, right: 20, top: 50, bottom: 40 },
      xAxis: { type: "category", data: rows.map((r) => r.quarter), axisLine: { lineStyle: { color: "#9fb2cc" } } },
      yAxis: {
        type: "value",
        name: "万吨",
        axisLine: { lineStyle: { color: "#9fb2cc" } },
        splitLine: { lineStyle: { color: "#e5ebf2" } },
      },
      series: [
        { name: "合计季度产量", type: "line", smooth: true, data: rows.map((r) => toWtValue(r.production)), color: "#2e6fd8" },
        { name: "合计季度销量", type: "line", smooth: true, data: rows.map((r) => toWtValue(r.sales)), color: "#1f9f7a" },
        { name: "合计年度指引中值", type: "bar", data: rows.map((r) => toWtValue(r.guidance_mid)), color: "#dca63a", barWidth: 24 },
      ],
    });
  } else {
    const yoy = data.aggregate?.yoy_mode || [];
    chart.setOption({
      backgroundColor: "transparent",
      tooltip: { trigger: "axis" },
      grid: { left: 48, right: 20, top: 40, bottom: 40 },
      xAxis: { type: "category", data: yoy.map((r) => r.metric), axisLine: { lineStyle: { color: "#9fb2cc" } } },
      yAxis: { type: "value", name: "%", axisLine: { lineStyle: { color: "#9fb2cc" } }, splitLine: { lineStyle: { color: "#e5ebf2" } } },
      series: [
        {
          type: "bar",
          data: yoy.map((r) => (isNum(r.value) ? r.value : null)),
          barWidth: 36,
          itemStyle: {
            color: (p) => (p.value >= 0 ? "#1f9f7a" : "#cf4f4f"),
          },
          label: { show: true, position: "top", color: "#425675", formatter: (p) => (isNum(p.value) ? `${p.value}%` : "") },
        },
      ],
    });
  }
}

function renderConclusions(data) {
  const c = data.conclusions || {};
  setTextIfExists("conclusionSupply", c.supply || "暂未形成供给结论。");
  setTextIfExists("conclusionGap", c.expectation_gap || "暂未形成预期差结论。");
  setTextIfExists("conclusionFutures", c.futures_mapping || "暂未形成盘面映射结论。");
}

function bindUiEvents() {
  const modal = document.getElementById("dataInfoModal");
  const dataInfoBtn = document.getElementById("dataInfoBtn");
  const closeModalBtn = document.getElementById("closeModalBtn");
  if (modal && dataInfoBtn) {
    dataInfoBtn.addEventListener("click", () => modal.classList.remove("hidden"));
  }
  if (modal && closeModalBtn) {
    closeModalBtn.addEventListener("click", () => modal.classList.add("hidden"));
  }

  document.querySelectorAll("#trendMetricSwitch button").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("#trendMetricSwitch button").forEach((x) => x.classList.remove("active"));
      btn.classList.add("active");
      state.trendMode = btn.dataset.mode;
      renderTrendChart(state.data);
    });
  });

  document.querySelectorAll("#trendQuarterSwitch button").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("#trendQuarterSwitch button").forEach((x) => x.classList.remove("active"));
      btn.classList.add("active");
      state.trendWindow = btn.dataset.window;
      renderTrendChart(state.data);
    });
  });

  window.addEventListener("resize", () => {
    Object.values(charts).forEach((chart) => {
      if (chart && typeof chart.resize === "function") chart.resize();
    });
  });
}

function renderFatal(message) {
  document.body.innerHTML = `<div style="padding:24px;color:#b33b3b;font-family:monospace;background:#f8fafc;min-height:100vh;">${escapeHtml(message)}</div>`;
}

async function bootstrap() {
  try {
    const data = await loadData();
    state.data = data;

    renderSummaryCards(data);
    renderOverviewTable(data);
    renderCompanyJumpCards(data);
    renderCompanyDetails(data);
    renderTrendChart(data);
    renderConclusions(data);
    bindUiEvents();
  } catch (err) {
    renderFatal(`页面加载失败：${err.message}`);
  }
}

bootstrap();
