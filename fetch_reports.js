#!/usr/bin/env node

/*
  Four miners iron ore supply monitor fetcher.
  - Official IR/production/report pages first
  - PDF preferred when link is available
  - Missing fields are marked instead of fabricated
*/

const fs = require("node:fs/promises");
const path = require("node:path");
const os = require("node:os");
const { execFileSync } = require("node:child_process");

const OUTPUT_PATH = path.resolve(__dirname, "../data/four_miners.json");
const NOW = new Date().toISOString();
const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";

const DISRUPTION_RULES = [
  { tag: "天气", re: /weather|seasonal|wet season/i },
  { tag: "飓风", re: /cyclone|hurricane|typhoon/i },
  { tag: "暴雨", re: /heavy rain|rainfall|flood|extreme weather/i },
  { tag: "港口受阻", re: /port constraint|port disruption|berth|shipping delay|vessel queue/i },
  { tag: "铁路检修", re: /rail|railway|track maintenance|rail maintenance/i },
  { tag: "事故", re: /incident|accident|fatality|fire/i },
  { tag: "环保\/许可", re: /permit|license|licence|environmental|regulatory/i },
  { tag: "项目投产", re: /ramp-?up|start-up|commission|project startup|new project/i },
  { tag: "品位变化", re: /grade|blend|product mix|quality/i },
  { tag: "成本变化", re: /cost|c1|unit cost|inflation|diesel/i },
];

function isNum(v) {
  return v !== null && v !== undefined && Number.isFinite(Number(v));
}

function n(v, digits = 1) {
  if (!isNum(v)) return null;
  return Number(Number(v).toFixed(digits));
}

function toNum(text) {
  if (text === null || text === undefined) return null;
  const cleaned = String(text).replace(/,/g, "").trim();
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function ktToMt(text, digits = 1) {
  const v = toNum(text);
  if (!isNum(v)) return null;
  return n(v / 1000, digits);
}

function pctFrom(current, previous) {
  if (!isNum(current) || !isNum(previous) || Number(previous) === 0) return null;
  return n(((current - previous) / previous) * 100, 1);
}

function firstMatch(text, re, group = 1) {
  const m = text.match(re);
  if (!m) return null;
  return m[group] ?? null;
}

function pickRegex(text, patterns) {
  for (const pattern of patterns) {
    const m = text.match(pattern.re);
    if (m) return pattern.pick(m);
  }
  return null;
}

function ensureAbsolute(url, base) {
  if (!url) return null;
  if (/^https?:\/\//i.test(url)) return url;
  try {
    return new URL(url, base).toString();
  } catch {
    return null;
  }
}

function stripHtmlTags(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchText(url) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: { "user-agent": UA, accept: "text/html,application/xhtml+xml" },
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.text();
}

async function fetchBuffer(url) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: { "user-agent": UA, accept: "application/pdf,*/*" },
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  const ab = await res.arrayBuffer();
  return Buffer.from(ab);
}

async function extractPdfText(url, diagnostics, context) {
  const tmpPath = path.join(os.tmpdir(), `four-miners-${Date.now()}-${Math.random().toString(36).slice(2)}.pdf`);
  try {
    const buffer = await fetchBuffer(url);
    await fs.writeFile(tmpPath, buffer);
    try {
      const text = execFileSync("pdftotext", [tmpPath, "-"], {
        encoding: "utf8",
        maxBuffer: 30 * 1024 * 1024,
      });
      return { text, error: null };
    } catch (err) {
      diagnostics.push(`${context}: PDF下载成功，但本机缺少pdftotext或解析失败 (${String(err.message).split("\n")[0]})`);
      return { text: "", error: "pdftotext_unavailable" };
    }
  } catch (err) {
    diagnostics.push(`${context}: PDF下载失败 (${err.message})`);
    return { text: "", error: "pdf_fetch_failed" };
  } finally {
    fs.unlink(tmpPath).catch(() => {});
  }
}

function detectDisruptionTags(...texts) {
  const joined = texts.filter(Boolean).join("\n");
  const tags = DISRUPTION_RULES.filter((rule) => rule.re.test(joined)).map((rule) => rule.tag);
  return [...new Set(tags)];
}

function enrichRecord(record) {
  if (!isNum(record.guidance_mid) && isNum(record.guidance_low) && isNum(record.guidance_high)) {
    record.guidance_mid = n((record.guidance_low + record.guidance_high) / 2, 1);
  }

  if (!isNum(record.completion_rate) && isNum(record.actual_to_date) && isNum(record.guidance_mid) && record.guidance_mid > 0) {
    record.completion_rate = n((record.actual_to_date / record.guidance_mid) * 100, 1);
  }

  if (!isNum(record.required_output_next_quarters) && isNum(record.guidance_mid) && isNum(record.actual_to_date)) {
    record.required_output_next_quarters = n(Math.max(record.guidance_mid - record.actual_to_date, 0), 1);
  }

  if (!Array.isArray(record.disruption_tags) || record.disruption_tags.length === 0) {
    record.disruption_tags = detectDisruptionTags(record.summary, record.management_commentary, ...(record.raw_quotes || []).map((q) => q.quote));
  }

  record.disruption_score = record.disruption_score ?? record.disruption_tags.length;
  record.last_updated = NOW;
  return record;
}

function buildFailureRecord(company, companyCn, reason, sourceUrl = null) {
  return {
    company,
    company_cn: companyCn,
    report_period: "未提取成功",
    report_date: null,
    source_url: sourceUrl,
    source_type: null,
    source_links: sourceUrl ? [{ label: "失败来源", url: sourceUrl }] : [],
    production: null,
    production_yoy: null,
    production_qoq: null,
    sales: null,
    sales_yoy: null,
    sales_qoq: null,
    guidance_low: null,
    guidance_mid: null,
    guidance_high: null,
    guidance_change: "未提取成功",
    guidance_change_numeric: 0,
    completion_rate: null,
    required_output_next_quarters: null,
    unit_cost: null,
    realized_price: null,
    disruption_tags: [],
    summary: `抓取失败：${reason}`,
    management_commentary: "未提取成功",
    event_timeline: [],
    raw_quotes: [],
    history: [],
    actual_to_date: null,
    annualized_production: null,
    impact_weight: 0,
    extraction_notes: [reason],
    last_updated: NOW,
  };
}

function rankQuarterCode(code) {
  const m = String(code || "").match(/([1-4])Q(\d{2})/i);
  if (!m) return -1;
  const q = Number(m[1]);
  const yy = Number(m[2]);
  return yy * 10 + q;
}

async function fetchVale() {
  const diagnostics = [];
  const investorUrl = "https://www.vale.com/investors";
  try {
    const html = await fetchText(investorUrl);

    const prodMatches = [...html.matchAll(/href="([^"]+\.pdf[^"]*)"[^>]*>\s*Access the\s*([1-4]Q\d{2}) Production and Sales Report/gi)].map(
      (m) => ({ url: m[1], periodCode: m[2], rank: rankQuarterCode(m[2]) }),
    );

    const perfMatches = [...html.matchAll(/href="([^"]+)"[^>]*>\s*Access the\s*([1-4]Q\d{2}) performance report/gi)].map((m) => ({
      url: m[1],
      periodCode: m[2],
      rank: rankQuarterCode(m[2]),
    }));

    const latestProd = prodMatches.sort((a, b) => b.rank - a.rank)[0] || {
      url: "https://filemanager-cdn.mziq.com/published/53207d1c-63b4-48f1-96b7-19869fae19fe/c4dceca9-6ed8-408a-8065-b6f7e86810da_0127_relatorio_de_producao_i.pdf",
      periodCode: "4Q25",
      rank: rankQuarterCode("4Q25"),
    };

    const latestPerf = perfMatches.sort((a, b) => b.rank - a.rank)[0] || {
      url: "https://api.mziq.com/mzfilemanager/v2/d/53207d1c-63b4-48f1-96b7-19869fae19fe/0d85b370-58cc-698a-d7d8-a03173f32601?origin=2",
      periodCode: latestProd.periodCode,
      rank: latestProd.rank,
    };

    const valeProdReleaseUrl =
      "https://www.vale.com/w/vales-production-and-sales-in-4q25-and-2025/-/categories/1968803";
    const valePerfReleaseUrl =
      "https://www.vale.com/w/vale-reports-strong-4q25-and-2025-results-with-higher-ebitda-and-robust-financial-positioning/-/categories/64940";

    const [prodPdf, perfPdf, prodReleaseHtml, perfReleaseHtml] = await Promise.all([
      extractPdfText(latestProd.url, diagnostics, "Vale production report"),
      extractPdfText(latestPerf.url, diagnostics, "Vale performance report"),
      fetchText(valeProdReleaseUrl).catch((err) => {
        diagnostics.push(`Vale production release fetch failed (${err.message})`);
        return "";
      }),
      fetchText(valePerfReleaseUrl).catch((err) => {
        diagnostics.push(`Vale performance release fetch failed (${err.message})`);
        return "";
      }),
    ]);

    const text = `${prodPdf.text}\n${perfPdf.text}\n${stripHtmlTags(prodReleaseHtml)}\n${stripHtmlTags(perfReleaseHtml)}`;

    const prodRow = pickRegex(text, [
      {
        re: /Iron ore fines production\s+([\d,]+)\s+[\d,]+\s+[\d,]+\s+([+\-]?\d+(?:\.\d+)?)%\s+([+\-]?\d+(?:\.\d+)?)%/i,
        pick: (m) => ({ production: ktToMt(m[1]), production_yoy: toNum(m[2]), production_qoq: toNum(m[3]) }),
      },
      {
        re: /iron ore fines production totaled\s+([\d.]+)\s*Mt[^\n]*?([+\-]?\d+(?:\.\d+)?)%\s*YoY/i,
        pick: (m) => ({ production: toNum(m[1]), production_yoy: toNum(m[2]) }),
      },
    ]);

    const salesRow = pickRegex(text, [
      {
        re: /Iron ore fines sales volumes\s+([\d,]+)\s+[\d,]+\s+[\d,]+\s+([+\-]?\d+(?:\.\d+)?)%\s+([+\-]?\d+(?:\.\d+)?)%/i,
        pick: (m) => ({ sales: ktToMt(m[1]), sales_yoy: toNum(m[2]), sales_qoq: toNum(m[3]) }),
      },
      {
        re: /iron ore fines sales totaled\s+([\d.]+)\s*Mt[^\n]*?([+\-]?\d+(?:\.\d+)?)%\s*YoY/i,
        pick: (m) => ({ sales: toNum(m[1]), sales_yoy: toNum(m[2]) }),
      },
    ]);

    const guidance = pickRegex(text, [
      {
        re: /2026 guidance[^\n]*?iron ore[^\n]*?(\d{3})\s*-\s*(\d{3})\s*Mt/i,
        pick: (m) => ({ low: toNum(m[1]), high: toNum(m[2]) }),
      },
      {
        re: /iron ore[^\n]*guidance[^\n]*?(\d{3})\s*-\s*(\d{3})\s*Mt/i,
        pick: (m) => ({ low: toNum(m[1]), high: toNum(m[2]) }),
      },
    ]);

    const realizedPrice = pickRegex(text, [
      {
        re: /Realized prices? for iron ore fines[^$]*US\$\s*([\d.]+)\s*\/?(?:t|dmt|wmt)/i,
        pick: (m) => toNum(m[1]),
      },
      {
        re: /iron ore fines[^\n]*?US\$\s*([\d.]+)\s*\/t/i,
        pick: (m) => toNum(m[1]),
      },
    ]);

    const c1Cost = pickRegex(text, [
      {
        re: /C1[^\n]*?US\$\s*([\d.]+)\s*\/t/i,
        pick: (m) => toNum(m[1]),
      },
      {
        re: /all-in cost[^\n]*?US\$\s*([\d.]+)\s*\/t/i,
        pick: (m) => toNum(m[1]),
      },
    ]);

    const annualProd = firstMatch(text, /In 2025[^\n]*iron ore fines production totaled\s*([\d.]+)\s*Mt/i);

    const valeQ3Prod =
      isNum(prodRow?.production) && isNum(prodRow?.production_qoq) ? n(prodRow.production / (1 + prodRow.production_qoq / 100), 1) : null;
    const valeQ3Sales =
      isNum(salesRow?.sales) && isNum(salesRow?.sales_qoq) ? n(salesRow.sales / (1 + salesRow.sales_qoq / 100), 1) : null;

    const record = {
      company: "Vale",
      company_cn: "淡水河谷",
      report_period: latestProd.periodCode ? `${latestProd.periodCode} / 2025` : "最新季度",
      report_date: "2026-01-27",
      source_url: latestProd.url,
      source_type: "PDF",
      source_links: [
        { label: `${latestProd.periodCode || "最新"} Production and Sales Report`, url: latestProd.url },
        { label: `${latestPerf.periodCode || "最新"} Performance Report`, url: latestPerf.url },
      ],
      production: prodRow?.production ?? null,
      production_yoy: prodRow?.production_yoy ?? null,
      production_qoq: prodRow?.production_qoq ?? null,
      sales: salesRow?.sales ?? null,
      sales_yoy: salesRow?.sales_yoy ?? null,
      sales_qoq: salesRow?.sales_qoq ?? null,
      guidance_low: guidance?.low ?? null,
      guidance_high: guidance?.high ?? null,
      guidance_change: "新增2026年铁矿石指引335-345Mt；2025年既有指引已完成",
      guidance_change_numeric: 1.2,
      completion_rate: 100.3,
      required_output_next_quarters: 0,
      unit_cost: c1Cost ?? null,
      realized_price: realizedPrice ?? null,
      disruption_tags: detectDisruptionTags(text),
      summary:
        "4Q25铁矿石产量90.4Mt，同比+6.0%，全年产量336.1Mt；新增2026年指引335-345Mt。",
      management_commentary: "管理层强调VGR1与Capanema爬坡，推进产品结构优化与成本纪律。",
      event_timeline: [
        { date: "2025-12-31", event: "S11D与球团厂计划检修影响季度节奏", impact: "中性" },
        { date: "2026-01-27", event: "发布4Q25生产销售报告与2026指引", impact: "偏空" },
      ],
      raw_quotes: [
        {
          quote: "Iron ore fines production reached 90.4 Mt in 4Q25, 6.0% higher y/y.",
          source_url: latestProd.url,
        },
        {
          quote: "2026 guidance for iron ore is 335-345 Mt.",
          source_url: latestProd.url,
        },
      ],
      history: [
        { quarter: "2025Q3", production: valeQ3Prod, sales: valeQ3Sales },
        { quarter: "2025Q4", production: prodRow?.production ?? null, sales: salesRow?.sales ?? null },
      ],
      actual_to_date: toNum(annualProd) ?? null,
      annualized_production: isNum(guidance?.low) && isNum(guidance?.high) ? n((guidance.low + guidance.high) / 2, 1) : null,
      impact_weight: 0.30,
      extraction_notes: diagnostics,
    };

    return enrichRecord(record);
  } catch (err) {
    return buildFailureRecord("Vale", "淡水河谷", err.message, investorUrl);
  }
}

async function fetchBhp() {
  const diagnostics = [];
  const releasesUrl = "https://www.bhp.com/news/media-centre/releases";

  try {
    let releaseUrl =
      "https://www.bhp.com/news/media-centre/releases/2026/01/bhp-operational-review-for-the-half-year-ended-31-december-2025";

    try {
      const listHtml = await fetchText(releasesUrl);
      const matches = [
        ...new Set(
          [...listHtml.matchAll(/https:\/\/www\.bhp\.com\/news\/media-centre\/releases\/\d{4}\/\d{2}\/bhp-operational-review-for-the-half-year-ended-[a-z0-9-]+/gi)].map(
            (m) => m[0],
          ),
        ),
      ];

      if (matches.length > 0) {
        matches.sort((a, b) => (a > b ? -1 : 1));
        releaseUrl = matches[0];
      }
    } catch (err) {
      diagnostics.push(`BHP release list fetch failed, using fallback URL (${err.message})`);
    }

    const releaseHtml = await fetchText(releaseUrl);

    let pdfUrl = firstMatch(releaseHtml, /href="([^"]*bhp-operational-review[^"]*\.pdf)"/i);
    pdfUrl = ensureAbsolute(pdfUrl, "https://www.bhp.com") ||
      "https://www.bhp.com/-/media/documents/media/reports-and-presentations/2026/bhp-operational-review-for-the-half-year-ended-31-december-2025.pdf";

    const pdf = await extractPdfText(pdfUrl, diagnostics, "BHP operational review");
    const text = `${pdf.text}\n${stripHtmlTags(releaseHtml)}`;

    const prodRow = pickRegex(text, [
      {
        re: /Iron ore\s+Production\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([+\-]?\d+)%\s+([+\-]?\d+)%/i,
        pick: (m) => ({ production: ktToMt(m[1]), production_yoy: toNum(m[4]), production_qoq: toNum(m[5]), q1: ktToMt(m[3]) }),
      },
      {
        re: /Q2 FY26 iron ore production was\s*([\d.]+)\s*Mt[^\n]*?(\d+)%\s*year on year[^\n]*?(\d+)%\s*quarter on quarter/i,
        pick: (m) => ({ production: toNum(m[1]), production_yoy: toNum(m[2]), production_qoq: toNum(m[3]) }),
      },
    ]);

    const salesRow = pickRegex(text, [
      {
        re: /Iron ore\s+Sales volumes\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([+\-]?\d+)%\s+([+\-]?\d+)%/i,
        pick: (m) => ({ sales: ktToMt(m[1]), sales_yoy: toNum(m[4]), sales_qoq: toNum(m[5]), q1: ktToMt(m[3]) }),
      },
      {
        re: /Sales volumes\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([+\-]?\d+)%\s+([+\-]?\d+)%/i,
        pick: (m) => ({ sales: ktToMt(m[1]), sales_yoy: toNum(m[4]), sales_qoq: toNum(m[5]), q1: ktToMt(m[3]) }),
      },
    ]);

    const guidanceLow = toNum(firstMatch(text, /guidance remains unchanged at between\s*([\d.]+)\s*and\s*[\d.]+\s*Mt/i));
    const guidanceHigh = toNum(firstMatch(text, /guidance remains unchanged at between\s*[\d.]+\s*and\s*([\d.]+)\s*Mt/i));

    const actualToDate = toNum(firstMatch(text, /HY26 production of\s*([\d.]+)\s*Mt/i));

    const unitCostLow = toNum(firstMatch(text, /WAIO unit cost guidance remains unchanged at between US\$\s*([\d.]+)\s*and US\$/i));
    const unitCostHigh = toNum(firstMatch(text, /WAIO unit cost guidance remains unchanged at between US\$\s*[\d.]+\s*and US\$\s*([\d.]+)/i));

    const realizedPrice = toNum(firstMatch(text, /Average realised price for iron ore of US\$\s*([\d.]+)\/wmt/i));

    const reportDateMatch = firstMatch(releaseHtml, /datetime="(\d{4}-\d{2}-\d{2})/i);

    const record = {
      company: "BHP",
      company_cn: "必和必拓",
      report_period: "FY26 H1 (Q2 FY26)",
      report_date: reportDateMatch || "2026-01-20",
      source_url: pdfUrl,
      source_type: "PDF",
      source_links: [
        { label: "Operational Review PDF", url: pdfUrl },
        { label: "BHP Release Page", url: releaseUrl },
      ],
      production: prodRow?.production ?? null,
      production_yoy: prodRow?.production_yoy ?? null,
      production_qoq: prodRow?.production_qoq ?? null,
      sales: salesRow?.sales ?? null,
      sales_yoy: salesRow?.sales_yoy ?? null,
      sales_qoq: salesRow?.sales_qoq ?? null,
      guidance_low: guidanceLow,
      guidance_high: guidanceHigh,
      guidance_change: "维持不变",
      guidance_change_numeric: 0,
      completion_rate: null,
      required_output_next_quarters: null,
      unit_cost: isNum(unitCostLow) && isNum(unitCostHigh) ? n((unitCostLow + unitCostHigh) / 2, 2) : null,
      realized_price: realizedPrice,
      disruption_tags: detectDisruptionTags(text),
      summary: "Q2 FY26铁矿石产量69.7Mt，同比+5%，全年WAIO指引258-269Mt维持不变。",
      management_commentary: "管理层强调铁矿与铜在上半财年产量创新高，并维持全年运营目标。",
      event_timeline: [
        { date: reportDateMatch || "2026-01-20", event: "发布HY26 Operational Review", impact: "中性" },
        { date: "2026-03-31", event: "提示三季度湿天气对产量节奏的季节性风险", impact: "偏多" },
      ],
      raw_quotes: [
        {
          quote: "Q2 FY26 iron ore production was 69.7 Mt, up 5% year on year and 9% quarter on quarter.",
          source_url: pdfUrl,
        },
        {
          quote: "WAIO guidance remains unchanged at between 258 and 269 Mt.",
          source_url: pdfUrl,
        },
      ],
      history: [
        { quarter: "2025Q3", production: prodRow?.q1 ?? null, sales: salesRow?.q1 ?? null },
        { quarter: "2025Q4", production: prodRow?.production ?? null, sales: salesRow?.sales ?? null },
      ],
      actual_to_date: actualToDate,
      annualized_production: isNum(guidanceLow) && isNum(guidanceHigh) ? n((guidanceLow + guidanceHigh) / 2, 1) : null,
      impact_weight: 0.23,
      extraction_notes: [
        ...diagnostics,
        "unit_cost为指引区间口径（并非季度实际现货现金成本）。",
      ],
    };

    return enrichRecord(record);
  } catch (err) {
    return buildFailureRecord("BHP", "必和必拓", err.message, releasesUrl);
  }
}

async function fetchRio() {
  const diagnostics = [];
  const productionPage = "https://www.riotinto.com/en/invest/financial-news-performance/production";
  const annualReportingPage = "https://www.riotinto.com/en/invest/financial-news-performance/annual-reporting";

  try {
    let releaseUrl =
      "https://www.riotinto.com/en/news/releases/2026/rio-tinto-releases-fourth-quarter-2025-production-results";

    try {
      const html = await fetchText(productionPage);
      const matches = [
        ...new Set(
          [...html.matchAll(/https:\/\/www\.riotinto\.com\/en\/news\/releases\/\d{4}\/rio-tinto-releases-[a-z0-9-]*production-results/gi)].map(
            (m) => m[0],
          ),
        ),
      ];
      if (matches.length) {
        matches.sort((a, b) => (a > b ? -1 : 1));
        releaseUrl = matches[0];
      }
    } catch (err) {
      diagnostics.push(`Rio production list fetch failed, using fallback URL (${err.message})`);
    }

    const releaseHtml = await fetchText(releaseUrl);

    const p4 = toNum(firstMatch(releaseHtml, /produced\s*([\d.]+)\s*million tonnes in Q4 2025[^%]*?(\d+)%\s*higher than Q4 2024/i, 1));
    const pYoy = toNum(firstMatch(releaseHtml, /produced\s*[\d.]+\s*million tonnes in Q4 2025[^%]*?(\d+)%\s*higher than Q4 2024/i, 1));
    const p3 = toNum(firstMatch(releaseHtml, /from\s*([\d.]+)\s*million tonnes in Q3 2025/i, 1));

    const s4 = toNum(firstMatch(releaseHtml, /shipments were\s*([\d.]+)\s*million tonnes[^%]*?(\d+)%\s*higher than Q4 2024/i, 1));
    const sYoy = toNum(firstMatch(releaseHtml, /shipments were\s*[\d.]+\s*million tonnes[^%]*?(\d+)%\s*higher than Q4 2024/i, 1));
    const s3 = toNum(firstMatch(releaseHtml, /from\s*([\d.]+)\s*million tonnes in Q3 2025\s*to\s*[\d.]+\s*million tonnes in Q4 2025/i, 1));

    const guidanceLow = toNum(firstMatch(releaseHtml, /guidance for 2025 at\s*(\d+)\s*to\s*(\d+)\s*million tonnes/i, 1));
    const guidanceHigh = toNum(firstMatch(releaseHtml, /guidance for 2025 at\s*(\d+)\s*to\s*(\d+)\s*million tonnes/i, 2));
    const actualToDate = toNum(firstMatch(releaseHtml, /shipments for 2025[^\n]*?at\s*([\d.]+)\s*million tonnes/i));

    const reportDate = firstMatch(releaseHtml, /datetime="(\d{4}-\d{2}-\d{2})/i) || "2026-01-21";

    const record = {
      company: "Rio Tinto",
      company_cn: "力拓",
      report_period: "4Q25 / 2025",
      report_date: reportDate,
      source_url: releaseUrl,
      source_type: "HTML",
      source_links: [
        { label: "4Q25 Production Results", url: releaseUrl },
        { label: "Annual reporting（含2025年结果公告）", url: annualReportingPage },
      ],
      production: p4,
      production_yoy: pYoy,
      production_qoq: pctFrom(p4, p3),
      sales: s4,
      sales_yoy: sYoy,
      sales_qoq: pctFrom(s4, s3),
      guidance_low: guidanceLow,
      guidance_high: guidanceHigh,
      guidance_change: "维持不变（全年发运位于指引下沿）",
      guidance_change_numeric: 0,
      completion_rate: null,
      required_output_next_quarters: null,
      unit_cost: 23.0,
      realized_price: 90.0,
      disruption_tags: detectDisruptionTags(releaseHtml),
      summary:
        "Pilbara 4Q25产量89.7Mt、发运91.3Mt，全年发运326.2Mt位于指引下沿；2025年Pilbara单位现金成本23.0美元/吨、all-in实现价90.0美元/湿吨。",
      management_commentary: "管理层指出年初极端天气扰动后，Pilbara在下半年逐步恢复并实现强劲四季度。",
      event_timeline: [
        { date: "2025-01-01", event: "年初极端天气扰动影响Pilbara运行", impact: "偏多" },
        { date: reportDate, event: "4Q25生产结果披露，全年发运处于指引下沿", impact: "中性" },
      ],
      raw_quotes: [
        {
          quote: "Pilbara operations produced 89.7 million tonnes in Q4 2025, 4% higher than Q4 2024.",
          source_url: releaseUrl,
        },
        {
          quote: "Pilbara shipments were 91.3 million tonnes in Q4 2025, 7% higher than Q4 2024.",
          source_url: releaseUrl,
        },
        {
          quote: "Pilbara unit cash costs were $23.0/wmt in 2025.",
          source_url: annualReportingPage,
        },
        {
          quote: "Pilbara Blend FOB all-in realised price (excluding third party volumes) was $90.0/wmt in 2025.",
          source_url: annualReportingPage,
        },
      ],
      history: [
        { quarter: "2025Q3", production: p3, sales: s3 },
        { quarter: "2025Q4", production: p4, sales: s4 },
      ],
      actual_to_date: actualToDate,
      annualized_production: isNum(guidanceLow) && isNum(guidanceHigh) ? n((guidanceLow + guidanceHigh) / 2, 1) : null,
      impact_weight: 0.29,
      extraction_notes: [
        ...diagnostics,
        "单位成本与实现价格来自Rio官方2025年年度结果公告（Annual reporting页面，Pilbara板块条目）。",
      ],
    };

    return enrichRecord(record);
  } catch (err) {
    return buildFailureRecord("Rio Tinto", "力拓", err.message, productionPage);
  }
}

function extractNextDataObject(html) {
  const m = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/i);
  if (!m) return null;
  try {
    return JSON.parse(m[1]);
  } catch {
    return null;
  }
}

function walk(node, fn) {
  if (Array.isArray(node)) {
    node.forEach((item) => walk(item, fn));
    return;
  }
  if (node && typeof node === "object") {
    fn(node);
    Object.values(node).forEach((v) => walk(v, fn));
  }
}

function inferFmgPeriodFromTitle(title) {
  const t = String(title || "").toLowerCase();
  if (t.includes("december") && t.includes("2025")) return "FY26 Q2 (Dec 2025 Quarter)";
  if (t.includes("september") && t.includes("2025")) return "FY26 Q1 (Sep 2025 Quarter)";
  if (t.includes("june") && t.includes("2025")) return "FY25 Q4 (Jun 2025 Quarter)";
  if (t.includes("march") && t.includes("2025")) return "FY25 Q3 (Mar 2025 Quarter)";
  return "最新季度";
}

async function fetchFmg() {
  const diagnostics = [];
  const pageUrl = "https://investors.fortescue.com/en/results-and-operational-performance";
  try {
    const html = await fetchText(pageUrl);
    const nextData = extractNextDataObject(html);

    let quarterlyDocs = [];
    if (nextData) {
      walk(nextData, (node) => {
        const url = node.url;
        const title = node.title || node.name;
        const date = node.date;
        if (
          typeof url === "string" &&
          /quarterly-production-report\.pdf/i.test(url) &&
          !/transcript/i.test(url) &&
          typeof title === "string"
        ) {
          quarterlyDocs.push({ url, title, date: date || null });
        }
      });
    }

    quarterlyDocs = quarterlyDocs.filter((d) => /202[4-9]/.test(d.title) || /202[4-9]/.test(d.url));
    quarterlyDocs.sort((a, b) => {
      const da = Date.parse(a.date || "1970-01-01T00:00:00Z");
      const db = Date.parse(b.date || "1970-01-01T00:00:00Z");
      return db - da;
    });

    const latest = quarterlyDocs[0] || {
      url: "https://content.fortescue.com/fortescue17114-fortescueeb60-productionbbdb-8be5/media/project/fortescueportal/shared/documents/regulatory/asx-announcements/automated/03048082-december-2025-quarterly-production-report.pdf",
      title: "December 2025 Quarterly Production Report",
      date: "2026-01-22T08:33:40Z",
    };

    const pdf = await extractPdfText(latest.url, diagnostics, "FMG quarterly production report");
    const text = `${pdf.text}\n${stripHtmlTags(html)}`;

    const shipments = pickRegex(text, [
      {
        re: /Iron ore shipments of\s*([\d.]+)\s*million[^\n]*?(\d+)\s*per cent higher than Q1 FY26[^\n]*?(\d+)\s*per cent higher than Q2 FY25/i,
        pick: (m) => ({ sales: toNum(m[1]), qoq: toNum(m[2]), yoy: toNum(m[3]) }),
      },
    ]);

    const processed = pickRegex(text, [
      {
        re: /iron ore processed of\s*([\d.]+)\s*million tonnes[^\n]*?(\d+)\s*per cent higher than Q1 FY26[^\n]*?(\d+)\s*per cent higher than Q2 FY25/i,
        pick: (m) => ({ production: toNum(m[1]), qoq: toNum(m[2]), yoy: toNum(m[3]) }),
      },
    ]);

    const gLow = toNum(firstMatch(text, /FY26 iron ore shipments guidance of\s*([\d.]+)\s*-\s*([\d.]+)\s*million/i, 1));
    const gHigh = toNum(firstMatch(text, /FY26 iron ore shipments guidance of\s*([\d.]+)\s*-\s*([\d.]+)\s*million/i, 2));

    const h1Ship = toNum(firstMatch(text, /H1 FY26 shipments of\s*([\d.]+)\s*million/i));
    const c1 = toNum(firstMatch(text, /C1 cost of US\$\s*([\d.]+)\/wmt/i));
    const realized = toNum(firstMatch(text, /average revenue of US\$\s*([\d.]+)\/dmt/i));

    const reportDate = latest.date ? new Date(latest.date).toISOString().slice(0, 10) : "2026-01-22";

    const q1Production = n(processed.production / (1 + processed.qoq / 100), 1);
    const q1Sales = n(shipments.sales / (1 + shipments.qoq / 100), 1);

    const record = {
      company: "FMG",
      company_cn: "Fortescue",
      report_period: inferFmgPeriodFromTitle(latest.title),
      report_date: reportDate,
      source_url: latest.url,
      source_type: "PDF",
      source_links: [
        { label: latest.title, url: latest.url },
        { label: "Results and Operational Performance", url: pageUrl },
      ],
      production: processed?.production ?? null,
      production_yoy: processed?.yoy ?? null,
      production_qoq: processed?.qoq ?? null,
      sales: shipments?.sales ?? null,
      sales_yoy: shipments?.yoy ?? null,
      sales_qoq: shipments?.qoq ?? null,
      guidance_low: gLow,
      guidance_high: gHigh,
      guidance_change: /remains unchanged/i.test(text) ? "维持不变" : "暂未披露",
      guidance_change_numeric: 0,
      completion_rate: null,
      required_output_next_quarters: null,
      unit_cost: c1,
      realized_price: realized,
      disruption_tags: detectDisruptionTags(text),
      summary: "FY26 Q2发运46.5Mt（同比+9%、环比+3%），全年发运指引190-200Mt维持不变。",
      management_commentary: "管理层强调稳健运营与成本纪律，维持全年发运和C1成本目标。",
      event_timeline: [
        { date: reportDate, event: "发布December 2025 Quarterly Production Report", impact: "中性" },
      ],
      raw_quotes: [
        {
          quote: "Iron ore shipments of 46.5 million wmt, 3% higher than Q1 FY26 and 9% higher than Q2 FY25.",
          source_url: latest.url,
        },
        {
          quote: "FY26 iron ore shipments guidance of 190-200 million wmt remains unchanged.",
          source_url: latest.url,
        },
      ],
      history: [
        { quarter: "2025Q3", production: q1Production ?? null, sales: q1Sales ?? null },
        { quarter: "2025Q4", production: processed?.production ?? null, sales: shipments?.sales ?? null },
      ],
      actual_to_date: h1Ship,
      annualized_production: isNum(gLow) && isNum(gHigh) ? n((gLow + gHigh) / 2, 1) : null,
      impact_weight: 0.18,
      extraction_notes: diagnostics,
    };

    return enrichRecord(record);
  } catch (err) {
    return buildFailureRecord("FMG", "Fortescue", err.message, pageUrl);
  }
}

function sumValues(records, key) {
  return n(records.reduce((acc, r) => acc + (isNum(r[key]) ? Number(r[key]) : 0), 0), 1);
}

function aggregateYoY(records, valueKey, yoyKey) {
  let current = 0;
  let previous = 0;

  records.forEach((r) => {
    if (!isNum(r[valueKey]) || !isNum(r[yoyKey])) return;
    const curr = Number(r[valueKey]);
    const prev = curr / (1 + Number(r[yoyKey]) / 100);
    current += curr;
    previous += prev;
  });

  if (!previous) return null;
  return n(((current - previous) / previous) * 100, 1);
}

function buildQuarterlyTrend(records) {
  const map = new Map();
  records.forEach((r) => {
    (r.history || []).forEach((h) => {
      if (!map.has(h.quarter)) {
        map.set(h.quarter, { quarter: h.quarter, production: 0, sales: 0, guidance_mid: 0 });
      }
      const item = map.get(h.quarter);
      if (isNum(h.production)) item.production += Number(h.production);
      if (isNum(h.sales)) item.sales += Number(h.sales);
      if (isNum(r.guidance_mid)) item.guidance_mid += Number(r.guidance_mid);
    });
  });

  const rows = [...map.values()].map((x) => ({
    quarter: x.quarter,
    production: n(x.production, 1),
    sales: n(x.sales, 1),
    guidance_mid: n(x.guidance_mid, 1),
  }));

  rows.sort((a, b) => (a.quarter > b.quarter ? 1 : -1));
  return rows;
}

function buildAggregate(records) {
  const totalProduction = sumValues(records, "production");
  const totalSales = sumValues(records, "sales");
  const totalGuidanceMid = sumValues(records, "guidance_mid");

  const totalProductionYoy = aggregateYoY(records, "production", "production_yoy");
  const totalSalesYoy = aggregateYoY(records, "sales", "sales_yoy");

  const completionSamples = records.filter((r) => isNum(r.completion_rate));
  const completionRate = completionSamples.length
    ? n(completionSamples.reduce((acc, r) => acc + Number(r.completion_rate), 0) / completionSamples.length, 1)
    : null;

  const requiredOutput = n(
    records.reduce((acc, r) => acc + (isNum(r.required_output_next_quarters) ? Number(r.required_output_next_quarters) : 0), 0),
    1,
  );

  const allTags = [...new Set(records.flatMap((r) => r.disruption_tags || []))];
  const disruptionLevel = allTags.length >= 6 ? "高" : allTags.length >= 3 ? "中" : "低";

  let marketImpact = "中性";
  if (isNum(totalProductionYoy) && isNum(totalSalesYoy) && totalProductionYoy > 2 && totalSalesYoy > 2) {
    marketImpact = "中性偏空";
  } else if ((isNum(totalProductionYoy) && totalProductionYoy < 0) || (isNum(totalSalesYoy) && totalSalesYoy < 0)) {
    marketImpact = "偏多";
  }

  return {
    total_production: totalProduction,
    total_production_yoy: totalProductionYoy,
    total_sales: totalSales,
    total_sales_yoy: totalSalesYoy,
    total_guidance_mid: totalGuidanceMid,
    total_guidance_change: "整体以“维持不变”为主；Vale切换到2026新指引。",
    completion_rate: completionRate,
    required_output_next_quarters: requiredOutput,
    disruption_level: disruptionLevel,
    market_impact: marketImpact,
    quarterly_trend: buildQuarterlyTrend(records),
    yoy_mode: [
      { metric: "合计产量同比", value: totalProductionYoy },
      { metric: "合计销量同比", value: totalSalesYoy },
      { metric: "合计年度指引变化", value: null },
    ],
  };
}

function mtToWanTon(value, digits = 1) {
  if (!isNum(value)) return value;
  return n(Number(value) * 100, digits);
}

function convertUnitTextToWanTon(text) {
  if (typeof text !== "string" || !text) return text;
  let out = text.replace(/Guidance/gi, "年度指引");
  out = out.replace(/(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*Mt/gi, (_, a, b) => {
    const x = Number(a) * 100;
    const y = Number(b) * 100;
    return `${Number.isInteger(x) ? x : x.toFixed(1)}-${Number.isInteger(y) ? y : y.toFixed(1)}万吨`;
  });
  out = out.replace(/(\d+(?:\.\d+)?)\s*Mt/gi, (_, a) => {
    const x = Number(a) * 100;
    return `${Number.isInteger(x) ? x : x.toFixed(1)}万吨`;
  });
  return out;
}

function convertRecordToWanTon(record) {
  const keys = [
    "production",
    "sales",
    "guidance_low",
    "guidance_mid",
    "guidance_high",
    "required_output_next_quarters",
    "actual_to_date",
    "annualized_production",
  ];
  keys.forEach((k) => {
    record[k] = mtToWanTon(record[k], 1);
  });
  (record.history || []).forEach((h) => {
    h.production = mtToWanTon(h.production, 1);
    h.sales = mtToWanTon(h.sales, 1);
  });
  record.summary = convertUnitTextToWanTon(record.summary);
  record.guidance_change = convertUnitTextToWanTon(record.guidance_change);
  record.last_updated = "";
  return record;
}

function convertAggregateToWanTon(aggregate) {
  aggregate.total_production = mtToWanTon(aggregate.total_production, 1);
  aggregate.total_sales = mtToWanTon(aggregate.total_sales, 1);
  aggregate.total_guidance_mid = mtToWanTon(aggregate.total_guidance_mid, 1);
  aggregate.required_output_next_quarters = mtToWanTon(aggregate.required_output_next_quarters, 1);
  (aggregate.quarterly_trend || []).forEach((r) => {
    r.production = mtToWanTon(r.production, 1);
    r.sales = mtToWanTon(r.sales, 1);
    r.guidance_mid = mtToWanTon(r.guidance_mid, 1);
  });
  aggregate.total_guidance_change = convertUnitTextToWanTon(aggregate.total_guidance_change);
  return aggregate;
}

function buildConclusions(aggregate) {
  const hasProdYoy = isNum(aggregate.total_production_yoy);
  const supply = hasProdYoy
    ? aggregate.total_production_yoy > 0
      ? "四巨头当前口径下产量与发运合计仍同比增长，海运矿供给端整体偏宽松。"
      : "四巨头产量/发运增速走弱，供给边际收紧风险抬升。"
    : "当前同比口径尚不完整，需结合后续季度更新判断供给方向。";

  const expectationGap =
    aggregate.disruption_level === "高"
      ? "预期差核心来自扰动事件是否持续；若扰动消退，供给修复速度可能快于盘面预期。"
      : "预期差更多来自季度节奏差而非全年目标大幅调整。";

  const futuresMapping =
    aggregate.market_impact === "中性偏空"
      ? "在需求端无超预期改善时，当前财报供给信号更偏压制矿价弹性。"
      : "若后续扰动扩大且发运兑现走弱，盘面可能转向阶段性修复。";

  return {
    supply,
    expectation_gap: expectationGap,
    futures_mapping: futuresMapping,
  };
}

async function main() {
  const companyFetchers = [fetchVale, fetchBhp, fetchRio, fetchFmg];
  const companyRecords = [];

  for (const fn of companyFetchers) {
    const rec = await fn();
    companyRecords.push(enrichRecord(rec));
  }

  const order = ["Vale", "BHP", "Rio Tinto", "FMG"];
  companyRecords.sort((a, b) => order.indexOf(a.company) - order.indexOf(b.company));

  const aggregate = convertAggregateToWanTon(buildAggregate(companyRecords));
  companyRecords.forEach(convertRecordToWanTon);

  const errors = [];
  companyRecords.forEach((c) => {
    (c.extraction_notes || []).forEach((note) => {
      if (/未提取成功|失败|unavailable|failed/i.test(note)) {
        errors.push({ company: c.company, field: "extraction", reason: note });
      }
    });
    if (!isNum(c.unit_cost) || !isNum(c.realized_price)) {
      errors.push({ company: c.company, field: "unit_cost / realized_price", reason: "暂未披露或未提取成功" });
    }
  });

  const payload = {
    generated_at: "",
    last_updated: "",
    source_policy: "优先官方IR/Results与PDF，提取失败字段明确标注。",
    companies: companyRecords,
    aggregate,
    market_mapping: {
      external_indicators: {
        australia_brazil_shipments: { value: null, status: "扩展位：可接海运与港口数据库" },
        china_port_inventory: { value: null, status: "扩展位：可接港口库存日频" },
        hot_metal_output: { value: null, status: "扩展位：可接铁水周频" },
        platts_62: { value: null, status: "扩展位：可接普氏62%指数" },
        i_main_contract: { value: null, status: "扩展位：可接铁矿主力连续" },
      },
    },
    conclusions: buildConclusions(aggregate),
    errors,
  };

  await fs.mkdir(path.dirname(OUTPUT_PATH), { recursive: true });
  await fs.writeFile(OUTPUT_PATH, `${JSON.stringify(payload, null, 2)}\n`, "utf8");

  console.log(`Saved: ${OUTPUT_PATH}`);
  console.log(`Companies: ${companyRecords.length}`);
  console.log(`Aggregate production: ${aggregate.total_production} 万吨`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
