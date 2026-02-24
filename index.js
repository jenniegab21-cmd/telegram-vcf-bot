import TelegramBot from "node-telegram-bot-api";
import { google } from "googleapis";
import http from "http";
import url from "url";

/* =======================
   CONFIG
======================= */
const DB_SIZE = 250; // 1 DB = 250 nomor (termasuk nomor jagaan)
const STOCK_TAKE_PER_DB = DB_SIZE - 1; // yang diambil dari stok per DB (karena 1 slot diisi jagaan)
const SLEEP_BETWEEN_FILES_MS = 1200;
const CLEAR_DELETE_DELAY_MS = 220; // biar ga kena rate limit delete

/* =======================
   ENV
======================= */
const BOT_TOKEN = process.env.BOT_TOKEN;
const SHEET_ID = process.env.SHEET_ID; // ID saja, bukan URL
const GOOGLE_CREDENTIALS = process.env.GOOGLE_CREDENTIALS; // JSON service account full
const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.RENDER_EXTERNAL_URL;

const WEBHOOK_PATH = "/webhook";

// Tabs (PASTIKAN SAMA PERSIS)
const SHEET_NAME = "DB GDS"; // stok: A=FRESH, D=FU, G1:G10=nomor jagaan
const REPORT_SHEET = "REPORT"; // harian (DB) - optional masih dipakai buat /report
const STAFF_SHEET = "STAFF_REPORT"; // staff (DB)
const LASTREQ_SHEET = "LAST_REQUEST"; // last request log
const STAFF_USERS_SHEET = "STAFF_USERS"; // mapping telegram id -> staff code

// Cells config (di sheet DB GDS)
const GUARD_RANGE = `${SHEET_NAME}!G1:G10`; // max 10 nomor jagaan
const GUARD_PTR_CELL = `${SHEET_NAME}!H1`; // pointer jagaan (0..9) (kalau kosong auto 0)
const CFG_RANGE = `${SHEET_NAME}!I1:I4`; // I1=Nama DB Fresh, I2=Nama Contact Fresh, I3=Nama DB FU, I4=Nama Contact FU
const PIN_CELL = `${SHEET_NAME}!K1`; // PIN clear

if (!BOT_TOKEN || !SHEET_ID || !GOOGLE_CREDENTIALS || !BASE_URL) {
  console.error("❌ ENV belum lengkap. Wajib: BOT_TOKEN, SHEET_ID, GOOGLE_CREDENTIALS, RENDER_EXTERNAL_URL");
  process.exit(1);
}

/* =======================
   GOOGLE SHEETS
======================= */
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(GOOGLE_CREDENTIALS),
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

/* =======================
   TELEGRAM BOT (WEBHOOK)
======================= */
const bot = new TelegramBot(BOT_TOKEN, { polling: false });

await bot.setWebHook(`${BASE_URL}${WEBHOOK_PATH}`, {
  allowed_updates: ["message"],
});

/* =======================
   HTTP SERVER
======================= */
http
  .createServer((req, res) => {
    const parsed = url.parse(req.url, true);

    if (req.method === "POST" && parsed.pathname === WEBHOOK_PATH) {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", async () => {
        try {
          await bot.processUpdate(JSON.parse(body));
          res.end("OK");
        } catch (e) {
          console.error(e);
          res.end("ERROR");
        }
      });
    } else {
      res.end("Bot running");
    }
  })
  .listen(PORT);

/* =======================
   UTIL
======================= */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const parseNumberLoose = (v) => {
  const digits = String(v ?? "").replace(/\D/g, "");
  return digits ? parseInt(digits, 10) : 0;
};

function formatID(n) {
  const s = String(Math.trunc(Number(n) || 0));
  return s.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
}

function pad3(n) {
  const x = Math.max(0, Math.trunc(Number(n) || 0));
  return String(x).padStart(3, "0");
}

function nowWIBIso() {
  const d = new Date();
  const wib = new Date(d.getTime() + 7 * 60 * 60 * 1000);
  const yyyy = wib.getUTCFullYear();
  const mm = String(wib.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(wib.getUTCDate()).padStart(2, "0");
  const hh = String(wib.getUTCHours()).padStart(2, "0");
  const mi = String(wib.getUTCMinutes()).padStart(2, "0");
  const ss = String(wib.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss} WIB`;
}

const todayKeyWIB = () => {
  const d = new Date();
  const wib = new Date(d.getTime() + 7 * 60 * 60 * 1000);
  const yyyy = wib.getUTCFullYear();
  const mm = String(wib.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(wib.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
};

function getTelegramDisplayName(msg) {
  const u = msg?.from;
  const parts = [u?.first_name, u?.last_name].filter(Boolean);
  const full = parts.join(" ").trim();
  if (u?.username) return `${full ? full + " " : ""}(@${u.username})`;
  return full || "UNKNOWN";
}

async function safeSend(chatId, text, opts = {}) {
  try {
    const m = await bot.sendMessage(chatId, text, opts);
    if (m?.message_id != null) trackBotMessage(chatId, m.message_id);
    return m;
  } catch (e) {
    console.error("❌ sendMessage failed:", e?.message || e);
    return null;
  }
}

function sanitizeName(s) {
  return String(s ?? "")
    .trim()
    .replace(/[^\w\-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 40);
}

function isPrivateChat(msg) {
  return msg?.chat?.type === "private";
}

/* =======================
   CONFIG READERS (Sheet)
======================= */
async function getPinClear() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: PIN_CELL,
  });
  const raw = res.data.values?.[0]?.[0] ?? "";
  return String(raw).trim();
}

async function getNamingConfig() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: CFG_RANGE,
  });
  const v = res.data.values || [];
  const i1 = String(v?.[0]?.[0] ?? "").trim(); // nama db fresh
  const i2 = String(v?.[1]?.[0] ?? "").trim(); // nama contact fresh
  const i3 = String(v?.[2]?.[0] ?? "").trim(); // nama db fu
  const i4 = String(v?.[3]?.[0] ?? "").trim(); // nama contact fu

  return {
    dbFresh: i1 || "FRESH",
    contactFresh: i2 || "FRESH",
    dbFu: i3 || "FU",
    contactFu: i4 || "FU",
  };
}

async function getGuardList() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: GUARD_RANGE,
  });
  const raw = res.data.values || [];
  const guards = raw
    .map((r) => String(r?.[0] ?? "").replace(/\D/g, ""))
    .filter((x) => x.length >= 10);
  return guards.slice(0, 10);
}

async function getGuardPointer() {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: GUARD_PTR_CELL,
    });
    const raw = res.data.values?.[0]?.[0] ?? "";
    const n = parseInt(String(raw).replace(/\D/g, ""), 10);
    return Number.isFinite(n) ? Math.max(0, Math.min(9, n)) : 0;
  } catch {
    return 0;
  }
}

async function setGuardPointer(ptr) {
  const p = Math.max(0, Math.min(9, Math.trunc(Number(ptr) || 0)));
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: GUARD_PTR_CELL,
    valueInputOption: "RAW",
    requestBody: { values: [[String(p)]] },
  });
}

/* =======================
   STAFF CODE (mapping)
======================= */
const staffCodeCache = new Map(); // userId -> staffCode
const pendingStaffCode = new Set(); // userId yang lagi diminta input staff code

function normalizeStaffCode(input) {
  const s = String(input || "").trim().replace(/\s+/g, " ");
  return s.toUpperCase();
}

async function getStaffCode(userId) {
  const key = String(userId);
  if (staffCodeCache.has(key)) return staffCodeCache.get(key);

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${STAFF_USERS_SHEET}!A2:D`,
  });

  const rows = res.data.values || [];
  for (let i = 0; i < rows.length; i++) {
    const [uid, staffCode] = rows[i] || [];
    if (String(uid || "").trim() === key) {
      const code = String(staffCode || "").trim();
      if (code) staffCodeCache.set(key, code);
      return code || null;
    }
  }
  return null;
}

async function upsertStaffCode(userId, staffCode, telegramName) {
  const uidStr = String(userId);
  const code = normalizeStaffCode(staffCode);
  const updatedAt = nowWIBIso();

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${STAFF_USERS_SHEET}!A2:D`,
  });

  const rows = res.data.values || [];
  let rowIndex = null;
  for (let i = 0; i < rows.length; i++) {
    const [uid] = rows[i] || [];
    if (String(uid || "").trim() === uidStr) {
      rowIndex = i + 2;
      break;
    }
  }

  if (!rowIndex) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${STAFF_USERS_SHEET}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [[uidStr, code, telegramName, updatedAt]] },
    });
  } else {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${STAFF_USERS_SHEET}!A${rowIndex}:D${rowIndex}`,
      valueInputOption: "RAW",
      requestBody: { values: [[uidStr, code, telegramName, updatedAt]] },
    });
  }

  staffCodeCache.set(uidStr, code);
}

async function listStaffUsers(limit = 50) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${STAFF_USERS_SHEET}!A2:D`,
  });

  const rows = res.data.values || [];
  const data = rows
    .map((r) => ({
      userId: String(r?.[0] || "").trim(),
      staffCode: String(r?.[1] || "").trim(),
      name: String(r?.[2] || "").trim(),
      updatedAt: String(r?.[3] || "").trim(),
    }))
    .filter((x) => x.userId && x.staffCode);

  data.sort((a, b) => (b.updatedAt || "").localeCompare(a.updatedAt || ""));
  return data.slice(0, limit);
}

/* =======================
   Guard (old single guard) - removed
======================= */

/* =======================
   Stock counts
======================= */
async function getStockCounts() {
  const [aRes, dRes] = await Promise.all([
    sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A:A`,
    }),
    sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!D:D`,
    }),
  ]);

  const cleanCount = (values) =>
    (values || [])
      .map((v) => String(v?.[0] || "").replace(/\D/g, ""))
      .filter((v) => v.length >= 10).length;

  const freshLeft = cleanCount(aRes.data.values);
  const fuLeft = cleanCount(dRes.data.values);

  return {
    freshLeft,
    fuLeft,
    freshDBPossible: Math.floor(freshLeft / STOCK_TAKE_PER_DB),
    fuDBPossible: Math.floor(fuLeft / STOCK_TAKE_PER_DB),
  };
}

/* =======================
   REPORT (PER DB)
======================= */
async function getReportRow(dateStr) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${REPORT_SHEET}!A2:C`,
  });

  const rows = res.data.values || [];
  for (let i = 0; i < rows.length; i++) {
    const [date, fresh, fu] = rows[i] || [];
    if (String(date || "").trim() === dateStr) {
      return { rowIndex: i + 2, fresh: parseNumberLoose(fresh), fu: parseNumberLoose(fu) };
    }
  }
  return { rowIndex: null, fresh: 0, fu: 0 };
}

async function addToReport(type, dbCount) {
  const dateStr = todayKeyWIB();
  const row = await getReportRow(dateStr);

  const freshAdd = type === "vcardfresh" ? dbCount : 0;
  const fuAdd = type === "vcardfu" ? dbCount : 0;

  if (!row.rowIndex) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${REPORT_SHEET}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [[dateStr, freshAdd, fuAdd]] },
    });
    return { dateStr, fresh: freshAdd, fu: fuAdd };
  }

  const newFresh = row.fresh + freshAdd;
  const newFu = row.fu + fuAdd;

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${REPORT_SHEET}!A${row.rowIndex}:C${row.rowIndex}`,
    valueInputOption: "RAW",
    requestBody: { values: [[dateStr, newFresh, newFu]] },
  });

  return { dateStr, fresh: newFresh, fu: newFu };
}

async function getReportToday() {
  const dateStr = todayKeyWIB();
  const row = await getReportRow(dateStr);
  return { dateStr, fresh: row.fresh, fu: row.fu };
}

async function getReportByDate(dateStr) {
  const row = await getReportRow(dateStr);
  return { dateStr, fresh: row.fresh, fu: row.fu, found: row.rowIndex !== null };
}

async function resetReportToday() {
  const dateStr = todayKeyWIB();
  const row = await getReportRow(dateStr);

  if (!row.rowIndex) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${REPORT_SHEET}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [[dateStr, 0, 0]] },
    });
    return { dateStr, fresh: 0, fu: 0 };
  }

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${REPORT_SHEET}!A${row.rowIndex}:C${row.rowIndex}`,
    valueInputOption: "RAW",
    requestBody: { values: [[dateStr, 0, 0]] },
  });

  return { dateStr, fresh: 0, fu: 0 };
}

async function getReportMonth(month, year) {
  const mm = String(month).padStart(2, "0");
  const prefix = `${year}-${mm}-`;

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${REPORT_SHEET}!A2:C`,
  });

  const rows = res.data.values || [];
  let freshSum = 0;
  let fuSum = 0;
  let daysCount = 0;

  for (const r of rows) {
    const [date, fresh, fu] = r || [];
    const ds = String(date || "").trim();
    if (!ds.startsWith(prefix)) continue;

    freshSum += parseNumberLoose(fresh);
    fuSum += parseNumberLoose(fu);
    daysCount += 1;
  }

  return { year, month: mm, fresh: freshSum, fu: fuSum, days: daysCount };
}

/* =======================
   STAFF REPORT (PER DB)
======================= */
async function getStaffRow(dateStr, staffCode) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${STAFF_SHEET}!A2:D`,
  });

  const rows = res.data.values || [];
  for (let i = 0; i < rows.length; i++) {
    const [date, s, fresh, fu] = rows[i] || [];
    if (String(date || "").trim() === dateStr && String(s || "").trim() === staffCode) {
      return { rowIndex: i + 2, fresh: parseNumberLoose(fresh), fu: parseNumberLoose(fu) };
    }
  }
  return { rowIndex: null, fresh: 0, fu: 0 };
}

async function addToStaffReport(dateStr, staffCode, type, dbCount) {
  const row = await getStaffRow(dateStr, staffCode);

  const freshAdd = type === "vcardfresh" ? dbCount : 0;
  const fuAdd = type === "vcardfu" ? dbCount : 0;

  if (!row.rowIndex) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${STAFF_SHEET}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [[dateStr, staffCode, freshAdd, fuAdd]] },
    });
    return;
  }

  const newFresh = row.fresh + freshAdd;
  const newFu = row.fu + fuAdd;

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${STAFF_SHEET}!A${row.rowIndex}:D${row.rowIndex}`,
    valueInputOption: "RAW",
    requestBody: { values: [[dateStr, staffCode, newFresh, newFu]] },
  });
}

async function getStaffReportByDate(dateStr) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${STAFF_SHEET}!A2:D`,
  });

  const rows = res.data.values || [];
  const map = new Map();

  for (const r of rows) {
    const [date, staffCode, fresh, fu] = r || [];
    if (String(date || "").trim() !== dateStr) continue;

    const key = String(staffCode || "").trim() || "UNKNOWN";
    const cur = map.get(key) || { fresh: 0, fu: 0 };
    cur.fresh += parseNumberLoose(fresh);
    cur.fu += parseNumberLoose(fu);
    map.set(key, cur);
  }

  const arr = [...map.entries()].map(([staffCode, v]) => ({
    staffCode,
    fresh: v.fresh,
    fu: v.fu,
    totalDB: v.fresh + v.fu,
  }));
  arr.sort((a, b) => b.totalDB - a.totalDB);
  return arr;
}

async function getStaffReportByMonth(month, year) {
  const mm = String(month).padStart(2, "0");
  const prefix = `${year}-${mm}-`;

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${STAFF_SHEET}!A2:D`,
  });

  const rows = res.data.values || [];
  const map = new Map();

  for (const r of rows) {
    const [date, staffCode, fresh, fu] = r || [];
    const ds = String(date || "").trim();
    if (!ds.startsWith(prefix)) continue;

    const key = String(staffCode || "").trim() || "UNKNOWN";
    const cur = map.get(key) || { fresh: 0, fu: 0 };
    cur.fresh += parseNumberLoose(fresh);
    cur.fu += parseNumberLoose(fu);
    map.set(key, cur);
  }

  const arr = [...map.entries()].map(([staffCode, v]) => ({
    staffCode,
    fresh: v.fresh,
    fu: v.fu,
    totalDB: v.fresh + v.fu,
  }));
  arr.sort((a, b) => b.totalDB - a.totalDB);
  return { year, month: mm, rows: arr };
}

/* =======================
   STAFF REPORT RENDER (ENAK DIBACA)
======================= */
function renderStaffList(title, rows) {
  if (!rows.length) return `${title}\n\nBelum ada data.`;

  const out = [];
  out.push(`${title}`);
  out.push("────────────────────");

  rows.forEach((r, i) => {
    const staff = String(r.staffCode || "UNKNOWN").trim() || "UNKNOWN";

    const freshDB = Number(r.fresh) || 0;
    const fuDB = Number(r.fu) || 0;
    const totalDB = Number(r.totalDB ?? (freshDB + fuDB)) || 0;

    const freshNum = freshDB * DB_SIZE;
    const fuNum = fuDB * DB_SIZE;
    const totalNum = totalDB * DB_SIZE;

    out.push(`${i + 1}. ${staff}`);
    out.push(`   • FRESH : ${formatID(freshDB)} DB (${formatID(freshNum)} Nomor)`);
    out.push(`   • FU    : ${formatID(fuDB)} DB (${formatID(fuNum)} Nomor)`);
    out.push(`   • TOTAL : ${formatID(totalDB)} DB (${formatID(totalNum)} Nomor)`);
    out.push("");
  });

  const sumFresh = rows.reduce((a, r) => a + (Number(r.fresh) || 0), 0);
  const sumFu = rows.reduce((a, r) => a + (Number(r.fu) || 0), 0);
  const sumTotal = sumFresh + sumFu;

  out.push("────────────────────");
  out.push("TOTAL KESELURUHAN");
  out.push(`• FRESH : ${formatID(sumFresh)} DB (${formatID(sumFresh * DB_SIZE)} Nomor)`);
  out.push(`• FU    : ${formatID(sumFu)} DB (${formatID(sumFu * DB_SIZE)} Nomor)`);
  out.push(`• TOTAL : ${formatID(sumTotal)} DB (${formatID(sumTotal * DB_SIZE)} Nomor)`);

  return out.join("\n");
}

/* =======================
   LAST REQUEST (rapihin kayak staff list)
   Sheet columns:
   A USER_ID
   B STAFF_CODE
   C LAST_AT
   D TYPE
   E DB_COUNT
   F CHAT_ID (optional)
   G MSG_IDS (optional, comma-separated)
======================= */
async function upsertLastRequest(userId, staffCode, type, dbCount, chatId, msgIds = []) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${LASTREQ_SHEET}!A2:G`,
  });

  const rows = res.data.values || [];
  const uidStr = String(userId);

  let rowIndex = null;
  for (let i = 0; i < rows.length; i++) {
    const [uid] = rows[i] || [];
    if (String(uid || "").trim() === uidStr) {
      rowIndex = i + 2;
      break;
    }
  }

  const lastAt = nowWIBIso();
  const chatIdStr = String(chatId ?? "");
  const msgStr = (msgIds || []).map(String).join(",");

  const payload = [[uidStr, staffCode, lastAt, type, dbCount, chatIdStr, msgStr]];

  if (!rowIndex) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${LASTREQ_SHEET}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: payload },
    });
    return;
  }

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${LASTREQ_SHEET}!A${rowIndex}:G${rowIndex}`,
    valueInputOption: "RAW",
    requestBody: { values: payload },
  });
}

async function getLastRequests(limit = 30) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${LASTREQ_SHEET}!A2:G`,
  });

  const rows = res.data.values || [];
  const data = rows
    .map((r) => ({
      userId: String(r?.[0] || "").trim(),
      staffCode: String(r?.[1] || "").trim(),
      lastAt: String(r?.[2] || "").trim(),
      type: String(r?.[3] || "").trim(),
      dbCount: parseNumberLoose(r?.[4]),
      chatId: String(r?.[5] || "").trim(),
      msgIds: String(r?.[6] || "")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean)
        .map((x) => parseInt(x, 10))
        .filter((n) => Number.isFinite(n)),
    }))
    .filter((x) => x.userId);

  data.sort((a, b) => (b.lastAt || "").localeCompare(a.lastAt || ""));
  return data.slice(0, limit);
}

function renderLastReqList(rows) {
  if (!rows.length) return "📌 LAST REQUEST\n\nBelum ada data.";

  const out = [];
  out.push("📌 LAST REQUEST");
  out.push("────────────────────");

  rows.forEach((r, i) => {
    const t = (r.type || "").toLowerCase();
    const typeLabel = t === "vcardfresh" ? "FRESH" : t === "vcardfu" ? "FU" : (r.type || "UNKNOWN");
    const nomor = (Number(r.dbCount) || 0) * DB_SIZE;

    out.push(`${i + 1}. ${r.staffCode || "UNKNOWN"}`);
    out.push(`   • TYPE  : ${typeLabel}`);
    out.push(`   • DB    : ${formatID(r.dbCount)} DB`);
    out.push(`   • NOMOR : ${formatID(nomor)} Nomor`);
    out.push(`   • WAKTU : ${r.lastAt || "-"}`);
    out.push("");
  });

  return out.join("\n");
}

/* =======================
   COMMAND MAP (stok)
======================= */
const COMMANDS = {
  vcardfresh: { col: "A", typeLabel: "FRESH" },
  vcardfu: { col: "D", typeLabel: "FU" },
};

/* =======================
   QUEUE
======================= */
const queue = [];
let busy = false;

/* =======================
   Bot message tracking (for /clear)
======================= */
const botMsgMemory = new Map(); // chatId -> Set(messageId)

function trackBotMessage(chatId, messageId) {
  const key = String(chatId);
  const set = botMsgMemory.get(key) || new Set();
  set.add(Number(messageId));
  // batasi biar gak kebanyakan
  if (set.size > 250) {
    const arr = Array.from(set);
    arr.slice(0, arr.length - 250).forEach((x) => set.delete(x));
  }
  botMsgMemory.set(key, set);
}

/* =======================
   /clear flow
======================= */
const pendingClearPin = new Set(); // userId waiting pin

async function clearBotMessagesInChat(chatId, msgIds = []) {
  const ids = []
    .concat(msgIds || [])
    .concat(Array.from(botMsgMemory.get(String(chatId)) || []))
    .filter((x) => Number.isFinite(Number(x)))
    .map((x) => Number(x));

  // unique
  const uniq = Array.from(new Set(ids));

  // delete from newest-ish (descending) to reduce "message to delete not found" issues
  uniq.sort((a, b) => b - a);

  let ok = 0;
  let fail = 0;

  for (const mid of uniq) {
    try {
      // deleteMessage(chatId, messageId)
      await bot.deleteMessage(chatId, String(mid));
      ok += 1;
      await sleep(CLEAR_DELETE_DELAY_MS);
    } catch {
      fail += 1;
    }
  }

  // reset memory for that chat
  botMsgMemory.delete(String(chatId));

  return { ok, fail, total: uniq.length };
}

/* =======================
   PROCESS QUEUE
======================= */
async function processQueue() {
  if (busy || queue.length === 0) return;
  busy = true;

  const job = queue.shift();
  const { chatId, userId, dbCount, type, staffCode } = job;
  const { col, typeLabel } = COMMANDS[type];

  let filesSent = 0;
  let reportUpdated = false;
  const msgIdsSentInPrivate = []; // message_id docs + final text

  try {
    await safeSend(chatId, "✅ Cek japri bro...");
    await safeSend(userId, "⏳ Sebentar bro...");

    // Read configs
    const guards = await getGuardList();
    if (guards.length === 0) {
      await safeSend(chatId, `❌ Nomor jagaan kosong. Isi dulu di ${SHEET_NAME}!G1:G10`);
      busy = false;
      return processQueue();
    }

    const ptr0 = await getGuardPointer();
    const needGuards = dbCount;
    if (needGuards > 10) {
      await safeSend(chatId, `❌ Max request 10 DB (karena jagaan max 10).`);
      busy = false;
      return processQueue();
    }
    if (guards.length < 10) {
      // boleh, tapi tetap cek availability for this request with wrap
    }

    // Determine guard per DB sequential with pointer (wrap 0..9)
    const guardPerDb = [];
    for (let i = 0; i < dbCount; i++) {
      const idx = (ptr0 + i) % 10;
      const g = guards[idx];
      if (!g || g.length < 10) {
        await safeSend(
          chatId,
          `❌ Nomor jagaan G${idx + 1} kosong/invalid.\nIsi dulu semua jagaan di ${SHEET_NAME}!G1:G10`
        );
        busy = false;
        return processQueue();
      }
      guardPerDb.push(g);
    }

    // Update pointer for next request
    const ptrNext = (ptr0 + dbCount) % 10;
    await setGuardPointer(ptrNext);

    const naming = await getNamingConfig();
    const dbName = type === "vcardfresh" ? naming.dbFresh : naming.dbFu;
    const contactPrefix = type === "vcardfresh" ? naming.contactFresh : naming.contactFu;

    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!${col}:${col}`,
    });

    const numbers = (res.data.values || [])
      .map((v) => String(v?.[0] || "").replace(/\D/g, ""))
      .filter((v) => v.length >= 10);

    const required = STOCK_TAKE_PER_DB * dbCount;
    if (numbers.length < required) {
      await safeSend(
        chatId,
        `❌ Stok tidak cukup.\nButuh: ${required} nomor stok (${STOCK_TAKE_PER_DB}×${dbCount} DB)\nTersedia: ${numbers.length}`
      );
      busy = false;
      return processQueue();
    }

    const selected = numbers.slice(0, required);
    const remain = numbers.slice(required);

    // Build packs (each pack: 1 guard + 249 stok)
    const dbPacks = [];
    for (let i = 0; i < dbCount; i++) {
      const start = i * STOCK_TAKE_PER_DB;
      const end = start + STOCK_TAKE_PER_DB;
      const packStok = selected.slice(start, end);
      dbPacks.push([guardPerDb[i], ...packStok]);
    }

    // Contact numbering: reset each request, sequential across all contacts in request (not per DB)
    // Example: PREFIX-001 .. PREFIX-1250
    let contactCounter = 0;

    for (let i = 0; i < dbPacks.length; i++) {
      const pack = dbPacks[i];

      const vcardText = pack
        .map((n) => {
          contactCounter += 1;
          const fn = `${contactPrefix}-${pad3(contactCounter)}`;
          return `BEGIN:VCARD
VERSION:3.0
FN:${fn}
TEL;TYPE=CELL:${n}
END:VCARD`;
        })
        .join("\n");

      const buffer = Buffer.from(vcardText, "utf8");

      const fname = `${sanitizeName(dbName || typeLabel)}_DB_${i + 1}.vcf`;

      const m = await bot.sendDocument(
        userId,
        buffer,
        {},
        { filename: fname, contentType: "text/vcard" }
      );

      if (m?.message_id != null) {
        msgIdsSentInPrivate.push(Number(m.message_id));
        trackBotMessage(userId, Number(m.message_id));
      }

      filesSent++;
      await sleep(SLEEP_BETWEEN_FILES_MS);
    }

    // Clear stock column then append remain
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!${col}:${col}`,
    });

    if (remain.length) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!${col}1`,
        valueInputOption: "RAW",
        requestBody: { values: remain.map((v) => [v]) },
      });
    }

    // Update reports
    const rep = await addToReport(type, dbCount);
    const dateStr = rep?.dateStr || todayKeyWIB();
    await addToStaffReport(dateStr, staffCode, type, dbCount);

    // ✅ FINAL TEMPLATE (no report today), + warning line
    const doneMsg = await safeSend(
      userId,
      `✅ BERES!\n` +
        `👤 Staff: ${staffCode}\n` +
        `📦 Request: ${formatID(dbCount)} DB\n` +
        `📇 Total Kontak: ${formatID(dbCount * DB_SIZE)} Nomor (termasuk jagaan)\n\n` +
        `⚠️ PASTIKAN JANGAN SALAH TEMPLATE`
    );
    if (doneMsg?.message_id != null) msgIdsSentInPrivate.push(Number(doneMsg.message_id));

    // Upsert last request (store chatId japri + msgIds)
    await upsertLastRequest(userId, staffCode, type, dbCount, userId, msgIdsSentInPrivate);
    reportUpdated = true;
  } catch (e) {
    console.error("❌ ERROR:", e);

    if (filesSent > 0) {
      await safeSend(
        chatId,
        `⚠️ File DB sudah terkirim (${filesSent}/${dbCount} file).\nTapi ada error saat update laporan.\nCek tab REPORT / STAFF_REPORT / LAST_REQUEST atau Render Logs.`
      );
      if (!reportUpdated) {
        await safeSend(
          userId,
          `⚠️ File DB sudah terkirim, tapi report belum ke-update.\nCek tab REPORT / STAFF_REPORT / LAST_REQUEST sudah ada & izin Editor.`
        );
      }
    } else {
      await safeSend(chatId, "❌ Gagal proses. Pastikan kamu sudah /start bot di japri.");
    }
  }

  busy = false;
  processQueue();
}

/* =======================
   MESSAGE HANDLER
======================= */
bot.on("message", async (msg) => {
  if (!msg.text) return;

  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text.trim();

  // ===== /clear (manual, tidak ditulis di /start) =====
  if (pendingClearPin.has(String(userId)) && !text.startsWith("/")) {
    // user sedang input PIN
    pendingClearPin.delete(String(userId));

    const pin = String(text || "").trim();
    try {
      const realPin = await getPinClear();
      if (!realPin) {
        await safeSend(chatId, "❌ PIN belum diset di sheet (K1).");
        return;
      }
      if (pin !== String(realPin).trim()) {
        await safeSend(chatId, "❌ PIN salah.");
        return;
      }

      if (!isPrivateChat(msg)) {
        await safeSend(chatId, "❌ /clear hanya bisa di japri.");
        return;
      }

      // Ambil msgIds terakhir user dari LAST_REQUEST (kalau ada)
      const rows = await getLastRequests(200);
      const last = rows.find((r) => String(r.userId) === String(userId)) || null;
      const msgIds = last?.msgIds || [];

      const result = await clearBotMessagesInChat(userId, msgIds);

      // Kirim 1 pesan konfirmasi (ini akan muncul lagi, tapi ya minimal 1 baris)
      await safeSend(
        chatId,
        `🧹 CLEAR SELESAI\n• Deleted: ${result.ok}/${result.total}\n• Failed: ${result.fail}`
      );
    } catch (e) {
      console.error("❌ /clear pin error:", e);
      await safeSend(chatId, "❌ Gagal clear chat (cek Render Logs).");
    }
    return;
  }

  if (text === "/clear") {
    // tidak muncul di /start menu, tapi bisa dipakai manual
    if (!isPrivateChat(msg)) {
      await safeSend(chatId, "❌ /clear hanya bisa di japri.");
      return;
    }
    pendingClearPin.add(String(userId));
    await safeSend(chatId, "🔐 Masukkan PIN untuk CLEAR (lihat K1):");
    return;
  }

  // ===== staff code pending =====
  if (pendingStaffCode.has(String(userId)) && !text.startsWith("/")) {
    const code = normalizeStaffCode(text);
    const display = getTelegramDisplayName(msg);

    if (code.length < 3) {
      await safeSend(chatId, "❌ Kode staff kependekan. Contoh: GDS 01");
      return;
    }

    try {
      await upsertStaffCode(userId, code, display);
      pendingStaffCode.delete(String(userId));
      await safeSend(chatId, `✅ Oke, staff kamu diset: ${code}\nSekarang bisa request DB.`);
    } catch (e) {
      console.error("❌ set staff code error:", e);
      await safeSend(chatId, "❌ Gagal simpan staff code. Coba lagi.");
    }
    return;
  }

  // ===== /start =====
  if (text === "/start") {
    try {
      const code = await getStaffCode(userId);
      if (!code) {
        pendingStaffCode.add(String(userId));
        await safeSend(
          chatId,
          "✅ Bot aktif.\n\nPertama, kirim KODE STAFF kamu ya.\nContoh:\nGDS 01\n\n(ketik kode saja, tanpa #, tanpa /)"
        );
        return;
      }

      // NOTE: /clear TIDAK ditampilkan di sini (sesuai request)
      await safeSend(
        chatId,
        `✅ Bot aktif.\n👤 Staff: ${code}\n\nREQUEST (per DB, 1 DB = ${DB_SIZE} nomor):\n#vcardfresh JUMLAH_DB\n#vcardfu JUMLAH_DB\n\nNomor jagaan: ${SHEET_NAME}!G1:G10 (max 10, dipakai berurut)\nNama DB & Contact: ${SHEET_NAME}!I1:I4\n\nLaporan:\n/report\n/reportdate YYYY-MM-DD\n/reportmonth BULAN TAHUN\n/reportstaff\n/reportstaffdate YYYY-MM-DD\n/reportstaffmonth BULAN TAHUN\n/lastrequest\n/stafflist\n/setstaff KODE (ganti)\n/reset (opsional)\n\nContoh:\n#vcardfu 5`
      );
    } catch (e) {
      console.error("❌ /start error:", e);
      await safeSend(chatId, "❌ Error /start. Coba lagi.");
    }
    return;
  }

  // ===== /setstaff =====
  const setm = text.match(/^\/setstaff\s+(.+)$/i);
  if (setm) {
    const code = normalizeStaffCode(setm[1]);
    const display = getTelegramDisplayName(msg);
    try {
      await upsertStaffCode(userId, code, display);
      pendingStaffCode.delete(String(userId));
      await safeSend(chatId, `✅ Staff code kamu sekarang: ${code}`);
    } catch (e) {
      console.error("❌ /setstaff error:", e);
      await safeSend(chatId, "❌ Gagal set staff code.");
    }
    return;
  }

  // ===== /stafflist =====
  if (text === "/stafflist") {
    try {
      const rows = await listStaffUsers(50);
      if (!rows.length) {
        await safeSend(chatId, "📋 STAFF LIST\nBelum ada data.");
        return;
      }
      const lines = rows.map(
        (r, i) => `${i + 1}. ${r.staffCode} — ${r.name} — ID:${r.userId} — ${r.updatedAt}`
      );
      await safeSend(chatId, "📋 STAFF LIST (terbaru)\n" + lines.join("\n"));
    } catch (e) {
      console.error("❌ /stafflist error:", e);
      await safeSend(chatId, "❌ Gagal ambil staff list.");
    }
    return;
  }

  // ===== /report =====
  if (text === "/report") {
    try {
      const rep = await getReportToday();
      const stock = await getStockCounts();
      await safeSend(
        chatId,
        `📊 REPORT HARI INI (${rep.dateStr})\n✅ FRESH(DB): ${rep.fresh} (≈${formatID(rep.fresh * DB_SIZE)} nomor)\n✅ FU(DB): ${rep.fu} (≈${formatID(rep.fu * DB_SIZE)} nomor)\n\n📦 SISA STOK (nomor)\nFRESH(A): ${formatID(stock.freshLeft)} (≈${stock.freshDBPossible} DB)\nFU(D): ${formatID(stock.fuLeft)} (≈${stock.fuDBPossible} DB)`
      );
    } catch (e) {
      console.error("❌ /report ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report.");
    }
    return;
  }

  // ===== /reportdate =====
  const rd = text.match(/^\/reportdate\s+(\d{4}-\d{2}-\d{2})$/i);
  if (rd) {
    const dateStr = rd[1];
    try {
      const rep = await getReportByDate(dateStr);
      if (!rep.found) {
        await safeSend(chatId, `📊 REPORT ${dateStr}\nData tidak ditemukan.`);
      } else {
        await safeSend(
          chatId,
          `📊 REPORT ${dateStr}\n✅ FRESH(DB): ${rep.fresh} (≈${formatID(rep.fresh * DB_SIZE)} nomor)\n✅ FU(DB): ${rep.fu} (≈${formatID(rep.fu * DB_SIZE)} nomor)`
        );
      }
    } catch (e) {
      console.error("❌ /reportdate ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report tanggal.");
    }
    return;
  }

  // ===== /reportmonth =====
  const rm = text.match(/^\/reportmonth\s+(\d{1,2})\s+(\d{4})$/i);
  if (rm) {
    const month = parseInt(rm[1], 10);
    const year = parseInt(rm[2], 10);
    if (month < 1 || month > 12) {
      await safeSend(chatId, "❌ Format salah. Contoh: /reportmonth 2 2026");
      return;
    }
    try {
      const rep = await getReportMonth(month, year);
      await safeSend(
        chatId,
        `📅 REPORT BULAN ${rep.year}-${rep.month}\n✅ Total hari: ${rep.days}\n✅ FRESH(DB): ${rep.fresh}\n✅ FU(DB): ${rep.fu}`
      );
    } catch (e) {
      console.error("❌ /reportmonth ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report bulanan.");
    }
    return;
  }

  // ===== /reportstaff =====
  if (text === "/reportstaff") {
    try {
      const dateStr = todayKeyWIB();
      const rows = await getStaffReportByDate(dateStr);
      await safeSend(chatId, renderStaffList(`📋 REPORT STAFF (${dateStr})`, rows));
    } catch (e) {
      console.error("❌ /reportstaff ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report staff.");
    }
    return;
  }

  // ===== /reportstaffdate =====
  const rsd = text.match(/^\/reportstaffdate\s+(\d{4}-\d{2}-\d{2})$/i);
  if (rsd) {
    const dateStr = rsd[1];
    try {
      const rows = await getStaffReportByDate(dateStr);
      await safeSend(chatId, renderStaffList(`📋 REPORT STAFF (${dateStr})`, rows));
    } catch (e) {
      console.error("❌ /reportstaffdate ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report staff tanggal.");
    }
    return;
  }

  // ===== /reportstaffmonth =====
  const rsm = text.match(/^\/reportstaffmonth\s+(\d{1,2})\s+(\d{4})$/i);
  if (rsm) {
    const month = parseInt(rsm[1], 10);
    const year = parseInt(rsm[2], 10);
    if (month < 1 || month > 12) {
      await safeSend(chatId, "❌ Format salah. Contoh: /reportstaffmonth 2 2026");
      return;
    }
    try {
      const rep = await getStaffReportByMonth(month, year);
      await safeSend(chatId, renderStaffList(`📋 REPORT STAFF BULAN ${rep.year}-${rep.month}`, rep.rows));
    } catch (e) {
      console.error("❌ /reportstaffmonth ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report staff bulanan.");
    }
    return;
  }

  // ===== /lastrequest =====
  if (text === "/lastrequest") {
    try {
      const rows = await getLastRequests(30);
      await safeSend(chatId, renderLastReqList(rows));
    } catch (e) {
      console.error("❌ /lastrequest ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil last request.");
    }
    return;
  }

  // ===== /reset =====
  if (text === "/reset") {
    try {
      const rep = await resetReportToday();
      await safeSend(chatId, `♻️ Report hari ini di-reset (${rep.dateStr}).`);
    } catch (e) {
      console.error("❌ /reset ERROR:", e);
      await safeSend(chatId, "❌ Gagal reset report.");
    }
    return;
  }

  // ===== REQUEST (per DB) =====
  const m = text.match(/^#(vcardfresh|vcardfu)\s+(\d+)$/i);
  if (!m) return;

  const type = m[1].toLowerCase();
  const dbCount = parseInt(m[2], 10);

  if (!Number.isFinite(dbCount) || dbCount <= 0) {
    await safeSend(chatId, "❌ JUMLAH_DB harus angka > 0");
    return;
  }

  const staffCode = await getStaffCode(userId);
  if (!staffCode) {
    pendingStaffCode.add(String(userId));
    await safeSend(chatId, "❌ Kamu belum set KODE STAFF.\nKetik dulu contoh: GDS 01");
    return;
  }

  queue.push({ chatId, userId, staffCode, type, dbCount });
  await safeSend(chatId, "📥 Cek japri bro...");
  processQueue();
});

console.log("🤖 BOT FINAL — DB GDS + Guard G1:G10 (berurut) + Nama DB/Contact (I1:I4) + /clear PIN (K1) + /lastrequest rapih");
