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

/* =======================
   ENV
======================= */
const BOT_TOKEN = process.env.BOT_TOKEN;
const SHEET_ID = process.env.SHEET_ID; // WAJIB: hanya ID (bukan URL)
const GOOGLE_CREDENTIALS = process.env.GOOGLE_CREDENTIALS; // WAJIB: JSON service account full
const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.RENDER_EXTERNAL_URL;

const WEBHOOK_PATH = "/webhook";

// Tabs (PASTIKAN NAMA TAB SAMA PERSIS DI GOOGLE SHEET)
const SHEET_NAME = "DB GDS"; // stok: A=FRESH, D=FU, G1=nomor jagaan
const REPORT_SHEET = "REPORT"; // harian (DB)
const STAFF_SHEET = "STAFF_REPORT"; // staff (DB)
const LASTREQ_SHEET = "LAST_REQUEST"; // last request log
const STAFF_USERS_SHEET = "STAFF_USERS"; // mapping telegram id -> staff code

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
    await bot.sendMessage(chatId, text, opts);
    return true;
  } catch (e) {
    console.error("❌ sendMessage failed:", e?.message || e);
    return false;
  }
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
   Guard number
======================= */
async function getGuardNumber() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!G1`,
  });

  const raw = res.data.values?.[0]?.[0] ?? "";
  return String(raw).replace(/\D/g, "");
}

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
      return {
        rowIndex: i + 2,
        fresh: parseNumberLoose(fresh),
        fu: parseNumberLoose(fu),
      };
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
   STAFF REPORT RENDER (ENAK DIBACA DI TELEGRAM)
======================= */
function renderStaffTable(title, rows) {
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
   LAST REQUEST
======================= */
async function upsertLastRequest(userId, staffCode, type, dbCount) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${LASTREQ_SHEET}!A2:E`,
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

  if (!rowIndex) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${LASTREQ_SHEET}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [[uidStr, staffCode, lastAt, type, dbCount]] },
    });
    return;
  }

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${LASTREQ_SHEET}!A${rowIndex}:E${rowIndex}`,
    valueInputOption: "RAW",
    requestBody: { values: [[uidStr, staffCode, lastAt, type, dbCount]] },
  });
}

async function getLastRequests(limit = 30) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${LASTREQ_SHEET}!A2:E`,
  });

  const rows = res.data.values || [];
  const data = rows
    .map((r) => ({
      userId: String(r?.[0] || "").trim(),
      staffCode: String(r?.[1] || "").trim(),
      lastAt: String(r?.[2] || "").trim(),
      type: String(r?.[3] || "").trim(),
      dbCount: parseNumberLoose(r?.[4]),
    }))
    .filter((x) => x.userId);

  data.sort((a, b) => (b.lastAt || "").localeCompare(a.lastAt || ""));
  return data.slice(0, limit);
}

/* =======================
   COMMAND MAP (stok)
======================= */
const COMMANDS = {
  vcardfresh: { col: "A", label: "FRESH" },
  vcardfu: { col: "D", label: "FU" },
};

/* =======================
   QUEUE
======================= */
const queue = [];
let busy = false;

async function processQueue() {
  if (busy || queue.length === 0) return;
  busy = true;

  const { chatId, userId, dbCount, type, staffCode } = queue.shift();
  const { col, label } = COMMANDS[type];

  let filesSent = 0;
  let reportUpdated = false;

  try {
    await safeSend(chatId, "✅ Cek japri bro...");
    await safeSend(userId, "⏳ Sebentar bro...");

    const guard = await getGuardNumber();
    if (!guard || guard.length < 10) {
      await safeSend(chatId, `❌ Nomor jagaan kosong / invalid. Isi dulu di ${SHEET_NAME}!G1`);
      busy = false;
      return processQueue();
    }

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

    const dbPacks = [];
    for (let i = 0; i < dbCount; i++) {
      const start = i * STOCK_TAKE_PER_DB;
      const end = start + STOCK_TAKE_PER_DB;
      const pack = selected.slice(start, end);
      dbPacks.push([guard, ...pack]);
    }

    for (let i = 0; i < dbPacks.length; i++) {
      const pack = dbPacks[i];

      const vcardText = pack
        .map(
          (n, idx) => `BEGIN:VCARD
VERSION:3.0
FN:${label}-DB${i + 1}-${idx + 1}
TEL;TYPE=CELL:${n}
END:VCARD`
        )
        .join("\n");

      const buffer = Buffer.from(vcardText, "utf8");

      await bot.sendDocument(
        userId,
        buffer,
        {},
        { filename: `${label}_DB_${i + 1}.vcf`, contentType: "text/vcard" }
      );

      filesSent++;
      await sleep(SLEEP_BETWEEN_FILES_MS);
    }

    // clear stok col
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!${col}:${col}`,
    });

    // append remaining back
    if (remain.length) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!${col}1`,
        valueInputOption: "RAW",
        requestBody: { values: remain.map((v) => [v]) },
      });
    }

    // update report sheets
    const rep = await addToReport(type, dbCount);
    const dateStr = rep?.dateStr || todayKeyWIB();

    await addToStaffReport(dateStr, staffCode, type, dbCount);
    await upsertLastRequest(userId, staffCode, type, dbCount);
    reportUpdated = true;

    // ✅ TEMPLATE FINAL — TANPA "REPORT HARI INI"
await safeSend(
  userId,
  `✅ BERES!\n` +
    `👤 Staff: ${staffCode}\n` +
    `📦 Request: ${formatID(dbCount)} DB\n` +
    `📇 Total Kontak: ${formatID(dbCount * DB_SIZE)} Nomor (termasuk jagaan)\n\n` +
    `⚠️ PASTIKAN JANGAN SALAH TEMPLATE`
);
  } catch (e) {
    console.error("❌ ERROR:", e);

    // Jangan misleading kalau file sudah kekirim
    if (filesSent > 0) {
      await safeSend(
        chatId,
        `⚠️ File DB sudah terkirim (${filesSent}/${dbCount} file).\nTapi ada error saat update report / notifikasi.\nCek tab REPORT/STAFF_REPORT/LAST_REQUEST atau Render Logs.`
      );
      if (!reportUpdated) {
        await safeSend(
          userId,
          `⚠️ File DB sudah terkirim, tapi report belum ke-update.\nCek tab REPORT/STAFF_REPORT/LAST_REQUEST sudah ada & izin Editor.`
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

  // kalau user sedang diminta input staff code
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

      await safeSend(
        chatId,
        `✅ Bot aktif.\n👤 Staff: ${code}\n\nREQUEST (per DB, 1 DB = ${DB_SIZE} nomor):\n#vcardfresh JUMLAH_DB\n#vcardfu JUMLAH_DB\n\nNomor jagaan: ambil dari ${SHEET_NAME}!G1 (disisipkan tiap DB)\n\nLaporan:\n/report\n/reportdate YYYY-MM-DD\n/reportmonth BULAN TAHUN\n/reportstaff\n/reportstaffdate YYYY-MM-DD\n/reportstaffmonth BULAN TAHUN\n/lastrequest\n/stafflist\n/setstaff KODE (ganti)\n/reset (opsional)\n\nContoh:\n#vcardfu 5`
      );
    } catch (e) {
      console.error("❌ /start error:", e);
      await safeSend(chatId, "❌ Error /start. Coba lagi.");
    }
    return;
  }

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

  if (text === "/reportstaff") {
    try {
      const dateStr = todayKeyWIB();
      const rows = await getStaffReportByDate(dateStr);
      await safeSend(chatId, renderStaffTable(`📋 REPORT STAFF (${dateStr})`, rows));
    } catch (e) {
      console.error("❌ /reportstaff ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report staff.");
    }
    return;
  }

  const rsd = text.match(/^\/reportstaffdate\s+(\d{4}-\d{2}-\d{2})$/i);
  if (rsd) {
    const dateStr = rsd[1];
    try {
      const rows = await getStaffReportByDate(dateStr);
      await safeSend(chatId, renderStaffTable(`📋 REPORT STAFF (${dateStr})`, rows));
    } catch (e) {
      console.error("❌ /reportstaffdate ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report staff tanggal.");
    }
    return;
  }

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
      await safeSend(
        chatId,
        renderStaffTable(`📋 REPORT STAFF BULAN ${rep.year}-${rep.month}`, rep.rows)
      );
    } catch (e) {
      console.error("❌ /reportstaffmonth ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil report staff bulanan.");
    }
    return;
  }

  if (text === "/lastrequest") {
    try {
      const rows = await getLastRequests(30);
      if (!rows.length) {
        await safeSend(chatId, "📌 LAST REQUEST\nBelum ada data.");
        return;
      }
      const lines = rows.map(
        (r, i) =>
          `${i + 1}. ${r.staffCode} (ID:${r.userId}) — ${r.type} ${r.dbCount} DB — ${r.lastAt}`
      );
      await safeSend(chatId, "📌 LAST REQUEST (terbaru)\n" + lines.join("\n"));
    } catch (e) {
      console.error("❌ /lastrequest ERROR:", e);
      await safeSend(chatId, "❌ Gagal ambil last request.");
    }
    return;
  }

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

  // REQUEST (per DB)
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

console.log("🤖 BOT FINAL — DB GDS + STAFF REPORT (LIST) + STAFF CODE + PER DB");
