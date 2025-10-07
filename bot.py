# LivePlace Telegram Bot — ULTRA STABLE v5.0
# Полностью переработан для максимальной стабильности

import os
import re
import csv
import asyncio
import logging
import random
import time
import json
import hashlib
import sys
import fcntl
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
from time import monotonic
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
import aiohttp
from io import BytesIO
from collections import Counter, defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, 
    CallbackQuery, InputMediaPhoto, Message, InputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# ===== КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ ДЛЯ СТАБИЛЬНОСТИ =====

class SingleInstance:
    """Гарантирует запуск только одного экземпляра бота"""
    def __init__(self, lockfile="/tmp/liveplace_bot.lock"):
        self.lockfile = lockfile
        self.fp = None
        
    def __enter__(self):
        try:
            self.fp = open(self.lockfile, 'w')
            fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return self
        except IOError:
            logger.error("❌ Бот уже запущен в другом процессе! Останавливаем этот экземпляр.")
            # Записываем PID текущего процесса в файл для отладки
            with open("/tmp/liveplace_current_pid.txt", "w") as f:
                f.write(f"Current PID: {os.getpid()}\n")
            sys.exit(1)
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.fp:
            try:
                fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
                self.fp.close()
                os.unlink(self.lockfile)
            except:
                pass

# 3. Улучшенная обработка ошибок Telegram
class RobustBot:
    """Обертка для бота с улучшенной обработкой ошибок"""
    def __init__(self, token):
        from aiogram.client.bot import DefaultBotProperties
        self.bot = Bot(
            token=token,
            default=DefaultBotProperties(parse_mode="HTML")
        )
        self.session = None
        
    async def ensure_session(self):
        """Гарантирует активную сессию"""
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
            self.bot.session = self.session
    
    async def safe_request(self, method, *args, **kwargs):
        """Безопасный запрос к Telegram API с повторами"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self.ensure_session()
                result = await method(*args, **kwargs)
                return result
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                wait_time = 2 ** attempt
                logger.warning(f"Ошибка запроса (попытка {attempt+1}): {e}. Ждем {wait_time}сек")
                await asyncio.sleep(wait_time)

# ===== ИНИЦИАЛИЗАЦИЯ =====

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger("liveplace")

# ---- .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---- ENV
API_TOKEN           = os.getenv("API_TOKEN", "").strip()
ADMIN_CHAT_ID       = int(os.getenv("ADMIN_CHAT_ID", "0") or "0")
FEEDBACK_CHAT_ID    = int(os.getenv("FEEDBACK_CHAT_ID", "0"))
GSHEET_ID           = os.getenv("GSHEET_ID", "").strip()
GSHEET_TAB          = os.getenv("GSHEET_TAB", "Ads").strip()
GSHEET_REFRESH_MIN  = int(os.getenv("GSHEET_REFRESH_MIN", "2"))

UTM_SOURCE          = os.getenv("UTM_SOURCE", "telegram")
UTM_MEDIUM          = os.getenv("UTM_MEDIUM", "bot")
UTM_CAMPAIGN        = os.getenv("UTM_CAMPAIGN", "bot_ads")

GSHEET_STATS_ID     = os.getenv("GSHEET_STATS_ID", "").strip()
WEEKLY_REPORT_DOW   = int(os.getenv("WEEKLY_REPORT_DOW", "1") or "1")
WEEKLY_REPORT_HOUR  = int(os.getenv("WEEKLY_REPORT_HOUR", "9") or "9")

if not API_TOKEN:
    raise RuntimeError("API_TOKEN is not set")

# --- Admins
ADMINS_RAW = os.getenv("ADMINS", "").strip()
ADMINS_SET = set(int(x) for x in ADMINS_RAW.split(",") if x.strip().isdigit())
if ADMIN_CHAT_ID:
    ADMINS_SET.add(ADMIN_CHAT_ID)

def is_admin(user_id: int) -> bool:
    return user_id in ADMINS_SET

# ---- Bot with robustness
robust_bot = RobustBot(API_TOKEN)
bot = robust_bot.bot
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ---- Google Sheets
import gspread
from google.oauth2.service_account import Credentials
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS_FILE = "credentials.json"
if not os.path.exists(CREDS_FILE):
    raise RuntimeError("credentials.json is missing next to bot.py")
creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc    = gspread.authorize(creds)

# ---------- Google Sheets helpers (sync) ----------
def open_spreadsheet():
    try:
        return gc.open_by_key(GSHEET_ID)
    except gspread.SpreadsheetNotFound as e:
        raise RuntimeError("Spreadsheet not found. Check GSHEET_ID and sharing.") from e

def get_worksheet():
    sh = open_spreadsheet()
    try:
        return sh.worksheet(GSHEET_TAB)
    except gspread.WorksheetNotFound as e:
        tabs = [w.title for w in sh.worksheets()]
        raise RuntimeError(f"Worksheet '{GSHEET_TAB}' not found. Available: {tabs}") from e

REQUIRED_COLUMNS = {
    "mode","city","district","type","rooms","price","published",
    "title_ru","title_en","title_ka",
    "description_ru","description_en","description_ka",
    "phone",
    "photo1","photo2","photo3","photo4","photo5","photo6","photo7","photo8","photo9","photo10"
}

def check_schema(ws) -> None:
    header = [h.strip() for h in ws.row_values(1)]
    missing = sorted(list(REQUIRED_COLUMNS - set(header)))
    if missing:
        raise RuntimeError(f"Missing columns: {missing}\nHeader: {header}")

_cached_rows: List[Dict[str, Any]] = []
_cache_loaded_at: float = 0.0

def _is_cache_stale() -> bool:
    if not _cached_rows:
        return True
    ttl = max(1, GSHEET_REFRESH_MIN) * 60
    return (monotonic() - _cache_loaded_at) >= ttl

def load_rows(force: bool = False) -> List[Dict[str, Any]]:
    global _cached_rows, _cache_loaded_at
    if _cached_rows and not force and not _is_cache_stale():
        return _cached_rows
    ws = get_worksheet()
    check_schema(ws)
    rows = ws.get_all_records()
    _cached_rows = rows
    _cache_loaded_at = monotonic()
    return rows

# Async wrappers for Sheets I/O
async def rows_async(force: bool=False) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(load_rows, force)

# ====== i18n ======
LANGS = ["ru", "en", "ka"]
USER_LANG: Dict[int, str] = {}
LANG_MAP = {"ru":"ru","ru-RU":"ru","en":"en","en-US":"en","en-GB":"en","ka":"ka","ka-GE":"ka"}

T = {
    "menu_title": {"ru": "Главное меню", "en": "Main menu", "ka": "მთავარი მენიუ"},
    "btn_search": {"ru": "🔎 Поиск", "en": "🔎 Search", "ka": "🔎 ძიება"},
    "btn_latest": {"ru": "🆕 Новые", "en": "🆕 Latest", "ka": "🆕 ახალი"},
    "btn_language": {"ru": "🌐 Язык", "en": "🌐 Language", "ka": "🌐 ენა"},
    "btn_about": {"ru": "ℹ️ О боте", "en": "ℹ️ About", "ka": "ℹ️ შესახებ"},
    "btn_fast": {"ru": "🟢 Быстрый подбор", "en": "🆕 Quick picks", "ka": "🆕 სწრაფი არჩევანი"},
    "btn_favs": {"ru": "❤️ Избранное", "en": "❤️ Favorites", "ka": "❤️ რჩეულები"},
    "btn_home": {"ru": "🏠 Меню", "en": "🏠 Menu", "ka": "🏠 მენიუ"},
    "btn_daily": {"ru": "🕓 Посуточно", "en": "🕓 Daily rent", "ka": "🕓 დღიურად"},

    "start": {
        "ru": (
            "<b>LivePlace</b>\n👋 Привет! Я помогу подобрать <b>идеальную недвижимость в Грузии</b>.\n\n"
            "<b>Как это работает?</b>\n"
            "— Задам 3–4 простых вопроса\n"
            "— Покажу лучшие варианты с фото и телефоном владельца\n"
            "— Просто посмотреть? Жми <b>🆕 Быстрый подбор</b>\n\n"
            "Добро пожаловать и удачного поиска! 🏡"
        ),
        "en": (
            "<b>LivePlace</b>\n👋 Hi! I'll help you find <b>your ideal home in Georgia</b>.\n\n"
            "<b>How it works:</b>\n"
            "— I ask 3–4 quick questions\n"
            "— Show top options with photos and owner phone\n"
            "— Just browsing? Tap <b>🆕 Quick picks</b>\n\n"
            "Welcome and happy hunting! 🏡"
        ),
        "ka": (
            "<b>LivePlace</b>\n👋 გამარჯობა! ერთად ვიპოვოთ <b>იდეალური ბინა საქართველოში</b>.\n\n"
            "<b>როგორ მუშაობს:</b>\n"
            "— 3–4 მარტივი კითხვა\n"
            "— საუკეთესო ვარიანტები ფოტოებითა და მფლობელის ნომრით\n"
            "— უბრალოდ გადაათვალიერე? დააჭირე <b>🆕 სწრაფი არჩევანი</b>\n\n"
            "კეთილი იყოს თქვენი მობრძანება! 🏡"
        ),
    },
    "about": {
        "ru": "LivePlace: быстрый подбор недвижимости в Грузии. Фильтры, 10 фото, телефон владельца, избранное.",
        "en": "LivePlace: fast real-estate search in Georgia. Filters, 10 photos, owner phone, favorites.",
        "ka": "LivePlace: უძრავი ქონების სწრაფი ძიება საქართველოში. ფილტრები, 10 ფოტო, მფლობელის ნომერი, რჩეულები."
    },
    "choose_lang": {"ru": "Выберите язык:", "en": "Choose language:", "ka": "აირჩიე ენა:"},

    "wiz_intro": {"ru": "Выберите режим работы:", "en": "Choose mode:", "ka": "აირჩიეთ რეჟიმი:"},
    "btn_rent": {"ru": "🏘 Аренда", "en": "🏘 Rent", "ka": "🏘 ქირავდება"},
    "btn_sale": {"ru": "🏠 Продажа", "en": "🏠 Sale", "ka": "🏠 იყიდება"},

    "ask_city": {"ru": "🏙 Выберите город:", "en": "🏙 Choose city:", "ka": "🏙 აირჩიეთ ქალაქი:"},
    "ask_district": {"ru": "📍 Выберите район:", "en": "📍 Choose district:", "ka": "📍 აირჩიეთ რაიონი:"},
    "ask_type": {"ru": "🏡 Выберите тип недвижимости:", "en": "🏡 Choose property type:", "ka": "🏡 აირჩიეთ ტიპი:"},
    "ask_rooms": {"ru": "🚪 Количество комнат:", "en": "🚪 Rooms:", "ka": "🚪 ოთახების რაოდენობა:"},
    "ask_price": {"ru": "💵 Бюджет:", "en": "💵 Budget:", "ka": "💵 ბიუჯეტი:"},

    "btn_skip": {"ru": "Пропустить", "en": "Skip", "ka": "გამოტოვება"},
    "btn_more": {"ru": "Ещё…", "en": "More…", "ka": "კიდევ…"},

    "no_results": {"ru": "Ничего не найдено.", "en": "No results.", "ka": "ვერაფერი მოიძებნა."},
    "results_found": {"ru": "Найдено объявлений: <b>{n}</b>", "en": "Listings found: <b>{n}</b>", "ka": "მოიძებნა განცხადება: <b>{n}</b>"},

    "btn_prev": {"ru": "« Назад", "en": "« Prev", "ka": "« უკან"},
    "btn_next": {"ru": "Вперёд »", "en": "Next »", "ka": "წინ »"},
    "btn_like": {"ru": "❤️ Нравится", "en": "❤️ Like", "ka": "❤️ მომეწონა"},
    "btn_dislike": {"ru": "👎 Дизлайк", "en": "👎 Dislike", "ka": "👎 არ მომწონს"},
    "btn_fav_add": {"ru": "⭐ В избранное", "en": "⭐ Favorite", "ka": "⭐ რჩეულებში"},
    "btn_fav_del": {"ru": "⭐ Удалить из избранного", "en": "⭐ Remove favorite", "ka": "⭐ წაშლა"},
    "btn_share": {"ru": "🔗 Поделиться", "en": "🔗 Share", "ka": "🔗 გაზიარება"},

    "label_price": {"ru":"Цена", "en":"Price", "ka":"ფასი"},
    "label_pub": {"ru":"Опубликовано", "en":"Published", "ka":"გამოქვეყნდა"},
    "label_phone": {"ru":"Телефон", "en":"Phone", "ka":"ტელეფონი"},

    "toast_removed": {"ru":"Удалено", "en":"Removed", "ka":"წაშლილია"},
    "toast_saved": {"ru":"Сохранено в избранное", "en":"Saved to favorites", "ka":"რჩეულებში შენახულია"},
    "toast_next": {"ru":"Следующее", "en":"Next", "ka":"შემდეგი"},
    "toast_no_more": {"ru":"Больше объявлений нет", "en":"No more listings", "ka":"სხვა განცხადება აღარ არის"},
}

LANG_FIELDS = {
    "ru": {"title": "title_ru", "desc": "description_ru"},
    "en": {"title": "title_en", "desc": "description_en"},
    "ka": {"title": "title_ka", "desc": "description_ka"},
}

def t(lang: str, key: str, **kwargs) -> str:
    lang = lang if lang in LANGS else "ru"
    val = T.get(key, {}).get(lang, T.get(key, {}).get("ru", key))
    if kwargs:
        try:
            return val.format(**kwargs)
        except Exception:
            return val
    return val

def current_lang_for(uid: int) -> str:
    return USER_LANG.get(uid, "ru") if uid in USER_LANG else "ru"

def cta_text(lang: str) -> str:
    return {"ru":"👉 Подробнее","en":"👉 Learn more","ka":"👉 დაწვრილებით"}.get(lang, "👉 Подробнее")

def build_utm_url(raw: str, ad_id: str, uid: int) -> str:
    if not raw:
        return "https://liveplace.com.ge/"
    seed = f"{uid}:{datetime.utcnow().strftime('%Y%m%d')}:{ad_id}".encode("utf-8")
    token = hashlib.sha256(seed).hexdigest()[:16]
    u = urlparse(raw); q = parse_qs(u.query)
    q["utm_source"]   = [UTM_SOURCE]
    q["utm_medium"]   = [UTM_MEDIUM]
    q["utm_campaign"] = [UTM_CAMPAIGN]
    q["utm_content"]  = [ad_id]
    q["token"]        = [token]
    new_q = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

# ---- Menus
def main_menu(lang: str) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=T["btn_fast"][lang]))
    builder.row(
        KeyboardButton(text=T["btn_search"][lang]),
        KeyboardButton(text=T["btn_latest"][lang])
    )
    builder.add(KeyboardButton(text=T["btn_favs"][lang]))
    builder.row(
        KeyboardButton(text=T["btn_language"][lang]),
        KeyboardButton(text=T["btn_about"][lang])
    )
    return builder.as_markup(resize_keyboard=True)

# ===== УЛУЧШЕННЫЕ УТИЛИТЫ =====
def norm(s: str) -> str:
    return (s or "").strip().lower()

def norm_mode(v: str) -> str:
    s = norm(v)
    if s in {"rent","аренда","long","long-term","долгосрочно","longterm"}:
        return "rent"
    if s in {"sale","продажа","buy","sell"}:
        return "sale"
    if s in {"daily","посуточно","sutki","сутки","short","short-term","shortterm","day","day-to-day"}:
        return "daily"
    return ""

def drive_direct(url: str) -> str:
    if not url:
        return url
    m = re.search(r"/d/([A-Za-z0-9_-]{25,})/", url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    m = re.search(r"[?&]id=([A-Za-z0-9_-]{25,})", url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return url

def looks_like_image(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return False
    if any(u.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")):
        return True
    if "google.com/uc?export=download" in u or "googleusercontent.com" in u:
        return True
    return False

def collect_photos(row: Dict[str, Any]) -> List[str]:
    photos: List[str] = []
    for i in range(1, 11):
        key = f"photo{i}"
        val = (row.get(key, "") or "").strip()
        if not val:
            continue
        candidates = re.split(r"[,\s]+", val)
        for token in candidates:
            token = token.strip()
            if not token:
                continue
            token = token.split()[0].strip().strip(",;")
            if not token:
                continue
            token = drive_direct(token)
            if looks_like_image(token):
                photos.append(token)
                break
    return photos

def format_card(row: Dict[str, Any], lang: str) -> str:
    title_k = LANG_FIELDS[lang]["title"]
    desc_k  = LANG_FIELDS[lang]["desc"]
    city      = str(row.get("city", "")).strip()
    district  = str(row.get("district", "")).strip()
    rtype     = str(row.get("type", "")).strip()
    rooms     = str(row.get("rooms", "")).strip()
    price     = str(row.get("price", "")).strip()
    published = str(row.get("published", "")).strip()
    phone     = str(row.get("phone", "")).strip()
    title     = str(row.get(title_k, "")).strip()
    desc      = str(row.get(desc_k, "")).strip()

    pub_txt = published
    try:
        dt = datetime.fromisoformat(published)
        pub_txt = dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    lines = []
    if title:
        lines.append(f"<b>{title}</b>")
    info_line = f"{rtype} • {rooms} • {city}, {district}".strip(" •,")
    if info_line:
        lines.append(info_line)
    if price:
        lines.append(f"{t(lang,'label_price')}: {price}")
    if pub_txt:
        lines.append(f"{t(lang,'label_pub')}: {pub_txt}")
    if desc:
        lines.append(desc)
    if phone:
        lines.append(f"<b>{t(lang,'label_phone')}:</b> {phone}")
    if not desc and not phone:
        lines.append("—")
    return "\n".join(lines)

# ===== ОСНОВНЫЕ ПЕРЕМЕННЫЕ =====
PAGE_SIZE = 8
CHOICE_CACHE: Dict[int, Dict[str, List[Tuple[str, str]]]] = {}
USER_RESULTS: Dict[int, Dict[str, Any]] = {}
USER_FAVS: Dict[int, List[str]] = {}

# ===== УЛУЧШЕННЫЙ ПОИСК =====
class Search(StatesGroup):
    mode = State()
    city = State()
    district = State()
    rtype = State()
    rooms = State()
    price = State()

def _l10n_label(row: Dict[str, Any], field: str, lang: str) -> str:
    base = str(row.get(field, "")).strip()
    if field not in ("city", "district"):
        return base
    if lang == "ru":
        return base or ""
    alt = str(row.get(f"{field}_{lang}", "")).strip()
    return alt or base

def unique_values_l10n(rows: List[Dict[str, Any]], field: str, lang: str,
                       where: Optional[List[Tuple[str, str]]] = None) -> List[Tuple[str, str]]:
    out: List[Tuple[str,str]] = []
    seen: set = set()
    for r in rows:
        ok = True
        if where:
            for f, val in where:
                if norm(r.get(f)) != norm(val):
                    ok = False
                    break
        if not ok:
            continue
        base = str(r.get(field, "")).strip()
        if not base or base in seen:
            continue
        label = _l10n_label(r, field, lang)
        seen.add(base)
        out.append((label, base))
    out.sort(key=lambda x: x[0])
    return out

# ===== УПРОЩЕННАЯ АНАЛИТИКА =====
ANALYTIC_EVENTS: List[Dict[str, Any]] = []

def log_event(event: str, uid: int, row: Dict[str,Any]=None, extra: Dict[str,Any]=None):
    try:
        ts = datetime.utcnow().isoformat(timespec="seconds")
        payload = {"ts": ts, "event": event, "uid": uid}
        if row:
            payload.update({
                "mode": norm_mode(row.get("mode","")),
                "city": str(row.get("city","")).strip(),
                "district": str(row.get("district","")).strip(),
            })
        if extra:
            payload.update(extra)
        ANALYTIC_EVENTS.append(payload)
    except Exception as e:
        logger.warning(f"Analytics error: {e}")

# ===== АВТО-ОБНОВЛЕНИЕ КЭША =====
async def _auto_refresh_loop():
    while True:
        try:
            if _is_cache_stale():
                await rows_async(force=True)
                logger.info("Sheets cache refreshed")
        except Exception as e:
            logger.warning(f"Auto refresh failed: {e}")
        await asyncio.sleep(60)  # Увеличено до 60 секунд

# ===== ОСНОВНЫЕ ОБРАБОТЧИКИ =====
@dp.message(Command("start", "menu"))
async def cmd_start(message: Message, state: FSMContext):
    if message.from_user.id not in USER_LANG:
        code = (message.from_user.language_code or "").strip()
        USER_LANG[message.from_user.id] = LANG_MAP.get(code, "ru")
    lang = USER_LANG[message.from_user.id]
    await state.clear()
    await message.answer(t(lang, "start"), reply_markup=main_menu(lang))

@dp.message(Command("home"))
async def cmd_home(message: Message, state: FSMContext):
    lang = USER_LANG.get(message.from_user.id, "ru")
    await state.clear()
    await message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))

@dp.message(lambda m: m.text in (T["btn_fast"]["ru"], T["btn_fast"]["en"], T["btn_fast"]["ka"]))
async def on_fast(message: Message):
    lang = USER_LANG.get(message.from_user.id, "ru")
    try:
        rows = await rows_async()
    except Exception as e:
        return await message.answer(f"Ошибка загрузки данных: {e}")
    
    # Берем последние 30 объявлений
    rows_sorted = sorted(rows, key=lambda x: x.get("published", ""), reverse=True)
    USER_RESULTS[message.from_user.id] = {"rows": rows_sorted[:30], "idx": 0, "context": {}}
    
    if not rows_sorted:
        return await message.answer(t(lang, "no_results"))
    
    await message.answer(t(lang, "results_found", n=len(rows_sorted[:30])))
    await show_current_card(message, message.from_user.id)

@dp.message(lambda m: m.text in (T["btn_latest"]["ru"], T["btn_latest"]["en"], T["btn_latest"]["ka"]))
async def on_latest(message: Message):
    lang = USER_LANG.get(message.from_user.id, "ru")
    try:
        rows = await rows_async()
    except Exception as e:
        return await message.answer(f"Ошибка загрузки данных: {e}")
    
    # Фильтруем последние 7 дней
    now = datetime.now()
    filtered_rows = []
    for row in rows:
        pub_str = row.get("published", "").strip()
        if not pub_str:
            continue
        try:
            if ' ' in pub_str:
                pub_date = datetime.strptime(pub_str, "%Y-%m-%d %H:%M:%S")
            else:
                pub_date = datetime.strptime(pub_str, "%Y-%m-%d")
            if (now - pub_date).days <= 7:
                filtered_rows.append(row)
        except Exception:
            continue
    
    filtered_rows.sort(key=lambda x: x.get("published", ""), reverse=True)
    USER_RESULTS[message.from_user.id] = {"rows": filtered_rows[:50], "idx": 0, "context": {}}
    
    if not filtered_rows:
        return await message.answer(t(lang, "no_results"))
    
    await message.answer(t(lang, "results_found", n=len(filtered_rows)))
    await show_current_card(message, message.from_user.id)

# ===== УЛУЧШЕННЫЙ ПОИСК =====
@dp.message(lambda m: m.text in (T["btn_search"]["ru"], T["btn_search"]["en"], T["btn_search"]["ka"]))
async def on_search(message: Message, state: FSMContext):
    lang = USER_LANG.get(message.from_user.id, "ru")
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text=T["btn_rent"][lang]),
        KeyboardButton(text=T["btn_sale"][lang]), 
        KeyboardButton(text=T["btn_daily"][lang])
    )
    builder.row(
        KeyboardButton(text=T["btn_latest"][lang]),
        KeyboardButton(text=T["btn_fast"][lang])
    )
    builder.row(
        KeyboardButton(text=T["btn_language"][lang]),
        KeyboardButton(text=T["btn_home"][lang])
    )
    await state.set_state(Search.mode)
    await message.answer(t(lang, "wiz_intro"), reply_markup=builder.as_markup(resize_keyboard=True))

@dp.message(StateFilter(Search.mode))
async def st_mode(message: Message, state: FSMContext):
    lang = USER_LANG.get(message.from_user.id, "ru")
    text = message.text or ""
    picked = ""
    if text in (T["btn_rent"]["ru"], T["btn_rent"]["en"], T["btn_rent"]["ka"]):
        picked = "rent"
    elif text in (T["btn_sale"]["ru"], T["btn_sale"]["en"], T["btn_sale"]["ka"]):
        picked = "sale"
    elif text in (T["btn_daily"]["ru"], T["btn_daily"]["en"], T["btn_daily"]["ka"]):
        picked = "daily"
    
    await state.update_data(mode=picked)
    rows = await rows_async()
    cities = unique_values_l10n(rows, "city", lang)
    await state.set_state(Search.city)
    await send_choice(message, lang, "city", cities, 0, t(lang, "ask_city"))

async def send_choice(message, lang: str, field: str, values: List[Tuple[str,str]], page: int, prompt: str, allow_skip=True):
    chat_id = message.chat.id if hasattr(message, "chat") else message.from_user.id
    CHOICE_CACHE.setdefault(chat_id, {})[field] = values

    builder = InlineKeyboardBuilder()
    start = page * PAGE_SIZE
    chunk = values[start:start+PAGE_SIZE]
    for idx, (label, _base) in enumerate(chunk, start=start):
        builder.add(InlineKeyboardButton(text=label, callback_data=f"pick:{field}:{idx}"))
    builder.adjust(1)
    
    controls = []
    if start + PAGE_SIZE < len(values):
        controls.append(InlineKeyboardButton(text=T["btn_more"][lang], callback_data=f"more:{field}:{page+1}"))
    if allow_skip:
        controls.append(InlineKeyboardButton(text=T["btn_skip"][lang], callback_data=f"pick:{field}:-1"))
    if controls:
        builder.row(*controls)
    builder.row(InlineKeyboardButton(text=T["btn_home"][lang], callback_data="home"))
    
    await message.answer(prompt, reply_markup=builder.as_markup())

# Упрощенные обработчики callback для поиска
@dp.callback_query(lambda c: c.data.startswith("pick:"))
async def cb_pick(c: CallbackQuery, state: FSMContext):
    try:
        _, field, idxs = c.data.split(":", 2)
        idx = int(idxs)
        lang = USER_LANG.get(c.from_user.id, "ru")
        chat_id = c.message.chat.id
        cache_list = CHOICE_CACHE.get(chat_id, {}).get(field, [])
        value = ""
        if 0 <= idx < len(cache_list):
            label, base = cache_list[idx]
            value = base

        await state.update_data(**{field: value})
        rows = await rows_async()

        try:
            await c.message.delete()
        except Exception:
            pass

        if field == "city":
            d_where = [("city", value)] if value else None
            dists = unique_values_l10n(rows, "district", lang, d_where)
            await state.set_state(Search.district)
            await send_choice(c.message, lang, "district", dists, 0, t(lang, "ask_district"))
        elif field == "district":
            city_val = (await state.get_data()).get("city", "")
            filters = []
            if city_val:
                filters.append(("city", city_val))
            if value:
                filters.append(("district", value))
            types = unique_values_l10n(rows, "type", lang, filters if filters else None)
            await state.set_state(Search.rtype)
            await send_choice(c.message, lang, "type", types, 0, t(lang, "ask_type"))
        elif field == "type":
            builder = InlineKeyboardBuilder()
            for r in ["1", "2", "3", "4", "5+"]:
                builder.add(InlineKeyboardButton(text=r, callback_data=f"rooms:{r}"))
            builder.adjust(3)
            builder.row(InlineKeyboardButton(text=T["btn_skip"][lang], callback_data="rooms:"))
            builder.row(InlineKeyboardButton(text=T["btn_home"][lang], callback_data="home"))
            await state.set_state(Search.rooms)
            await c.message.answer(t(lang, "ask_rooms"), reply_markup=builder.as_markup())
        elif field == "price":
            await finish_search(c.message, c.from_user.id, await state.get_data())
        await c.answer()
    except Exception as e:
        logger.exception("cb_pick failed")
        await c.answer("Ошибка, попробуйте ещё раз", show_alert=False)

@dp.callback_query(lambda c: c.data.startswith("rooms:"))
async def cb_rooms(c: CallbackQuery, state: FSMContext):
    val = c.data.split(":", 1)[1]
    if val == "5+":
        await state.update_data(rooms_min=5, rooms_max=None)
    elif val:
        try:
            n = int(val)
            await state.update_data(rooms_min=n, rooms_max=n)
        except Exception:
            pass
    lang = USER_LANG.get(c.from_user.id, "ru")
    data = await state.get_data()
    
    def price_ranges(mode: str):
        m = norm_mode(mode)
        if m == "sale":
            return [
                ("<40000", "0-40000"),
                ("40000-50000", "40000-50000"), 
                ("50000-70000", "50000-70000"),
                ("70000-90000", "70000-90000"),
                ("100000-150000", "100000-150000"),
                ("150000+", "150000-99999999"),
            ]
        return [
            ("<=500", "0-500"),
            ("500-800", "500-800"),
            ("800-1200", "800-1200"), 
            ("1200-2000", "1200-2000"),
            ("2000+", "2000-999999"),
        ]
    
    rngs = price_ranges(data.get("mode", "rent"))
    builder = InlineKeyboardBuilder()
    for label, code in rngs:
        builder.add(InlineKeyboardButton(text=label, callback_data=f"price:{code}"))
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text=T["btn_skip"][lang], callback_data="price:"))
    builder.row(InlineKeyboardButton(text=T["btn_home"][lang], callback_data="home"))
    await state.set_state(Search.price)
    await c.message.answer(t(lang, "ask_price"), reply_markup=builder.as_markup())
    await c.answer()

@dp.callback_query(lambda c: c.data.startswith("price:"))
async def cb_price(c: CallbackQuery, state: FSMContext):
    rng = c.data.split(":",1)[1]
    if rng:
        a,b = rng.split("-")
        await state.update_data(price_min=int(a), price_max=int(b))
    await finish_search(c.message, c.from_user.id, await state.get_data())
    await state.clear()
    await c.answer()

async def finish_search(message: Message, user_id: int, data: Dict[str,Any]):
    lang = USER_LANG.get(user_id, "ru")
    try:
        rows = await rows_async()
    except Exception as e:
        return await message.answer(f"Ошибка загрузки данных: {e}")

    filtered = []
    for r in rows:
        if data.get("mode") and norm_mode(r.get("mode")) != norm_mode(data["mode"]):
            continue
        if data.get("city") and norm(r.get("city")) != norm(data["city"]):
            continue
        if data.get("district") and norm(r.get("district")) != norm(data["district"]):
            continue
        if data.get("type") and norm(r.get("type")) != norm(data["type"]):
            continue
        if data.get("rooms_min") is not None:
            try:
                rr = float(r.get("rooms", 0) or 0)
            except Exception:
                continue
            mx = data.get("rooms_max") if data.get("rooms_max") is not None else rr
            if not (data["rooms_min"] <= rr <= mx):
                continue
        if data.get("price_min") is not None:
            try:
                pp = float(r.get("price", 0) or 0)
            except Exception:
                continue
            mx = data.get("price_max") if data.get("price_max") is not None else pp
            if not (data["price_min"] <= pp <= mx):
                continue
        filtered.append(r)

    log_event("search", user_id, row=(filtered[0] if filtered else None), extra={
        "found": len(filtered),
        "mode": norm_mode((data or {}).get("mode",""))
    })

    USER_RESULTS[user_id] = {"rows": filtered, "idx": 0, "context": {"mode": data.get("mode","")}}
    if not filtered:
        return await message.answer(t(lang, "no_results"), reply_markup=main_menu(lang))
    await message.answer(t(lang, "results_found", n=len(filtered)))
    await show_current_card(message, user_id)

# ===== КАРТОЧКИ ОБЪЯВЛЕНИЙ =====
def card_kb(idx: int, total: int, lang: str, fav: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    row1 = []
    if idx > 0:
        row1.append(InlineKeyboardButton(text=T["btn_prev"][lang], callback_data=f"pg:{idx-1}"))
    if idx < total-1:
        row1.append(InlineKeyboardButton(text=T["btn_next"][lang], callback_data=f"pg:{idx+1}"))
    if row1:
        builder.row(*row1)
    builder.row(
        InlineKeyboardButton(text=T["btn_like"][lang], callback_data="like"),
        InlineKeyboardButton(text=T["btn_dislike"][lang], callback_data="dislike"),
    )
    builder.row(InlineKeyboardButton(text=T["btn_fav_del"][lang] if fav else T["btn_fav_add"][lang], callback_data="fav"))
    builder.row(InlineKeyboardButton(text=T["btn_share"][lang], switch_inline_query=""))
    builder.row(InlineKeyboardButton(text=T["btn_home"][lang], callback_data="home"))
    return builder.as_markup()

def make_row_key(r: Dict[str,Any]) -> str:
    payload = "|".join([
        str(r.get("city","")), str(r.get("district","")),
        str(r.get("type","")), str(r.get("rooms","")),
        str(r.get("price","")), str(r.get("phone","")),
        str(r.get("title_ru") or r.get("title_en") or r.get("title_ka") or "")
    ])
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

async def show_current_card(message_or_cb, user_id: int):
    lang = USER_LANG.get(user_id, "ru")
    data = USER_RESULTS.get(user_id, {})
    rows = data.get("rows", [])
    idx  = data.get("idx", 0)
    total = len(rows)
    if not rows:
        return
    
    row = rows[idx]
    log_event("view", user_id, row=row)

    fav_keys = USER_FAVS.get(user_id, [])
    is_fav = make_row_key(row) in fav_keys

    text = format_card(row, lang)
    photos = collect_photos(row)[:10]
    kb = card_kb(idx, total, lang, is_fav)

    await _send_with_photos(message_or_cb, text, kb, photos)

async def _send_with_photos(msg_obj, text: str, kb: InlineKeyboardMarkup, photos: List[str]):
    if not photos:
        await msg_obj.answer(text, reply_markup=kb)
        return

    try:
        media = []
        for i, url in enumerate(photos):
            if i == 0:
                media.append(InputMediaPhoto(media=url, caption=text, parse_mode="HTML"))
            else:
                media.append(InputMediaPhoto(media=url))
        await msg_obj.answer_media_group(media)
        await msg_obj.answer("⬇️ Действия:", reply_markup=kb)
    except Exception as e:
        logger.error(f"Media group failed: {e}")
        try:
            await msg_obj.answer_photo(photos[0], caption=text, parse_mode="HTML", reply_markup=kb)
        except Exception as e2:
            logger.error(f"Single photo also failed: {e2}")
            await msg_obj.answer(text, reply_markup=kb)

# Упрощенные обработчики навигации
@dp.callback_query(lambda c: c.data.startswith("pg:"))
async def cb_page(c: CallbackQuery):
    idx = int(c.data.split(":")[1])
    user_id = c.from_user.id
    if user_id not in USER_RESULTS:
        return await c.answer(t("ru", "toast_no_more"), show_alert=False)
    data = USER_RESULTS[user_id]
    if idx < 0 or idx >= len(data["rows"]):
        return await c.answer(t("ru", "toast_no_more"), show_alert=False)
    data["idx"] = idx
    await show_current_card(c, user_id)
    await c.answer()

@dp.callback_query(lambda c: c.data == "like")
async def cb_like(c: CallbackQuery):
    user_id = c.from_user.id
    if user_id not in USER_RESULTS:
        return await c.answer("Нет активных результатов", show_alert=False)
    data = USER_RESULTS[user_id]
    row = data["rows"][data["idx"]]
    log_event("like", user_id, row=row)
    await c.answer(t(current_lang_for(user_id), "toast_next"), show_alert=False)
    if data["idx"] < len(data["rows"]) - 1:
        data["idx"] += 1
        await show_current_card(c, user_id)

@dp.callback_query(lambda c: c.data == "dislike")
async def cb_dislike(c: CallbackQuery):
    user_id = c.from_user.id
    if user_id not in USER_RESULTS:
        return await c.answer("Нет активных результатов", show_alert=False)
    data = USER_RESULTS[user_id]
    row = data["rows"][data["idx"]]
    log_event("dislike", user_id, row=row)
    await c.answer(t(current_lang_for(user_id), "toast_next"), show_alert=False)
    if data["idx"] < len(data["rows"]) - 1:
        data["idx"] += 1
        await show_current_card(c, user_id)

@dp.callback_query(lambda c: c.data == "fav")
async def cb_fav(c: CallbackQuery):
    user_id = c.from_user.id
    if user_id not in USER_RESULTS:
        return await c.answer("Нет активных результатов", show_alert=False)
    data = USER_RESULTS[user_id]
    row = data["rows"][data["idx"]]
    key = make_row_key(row)
    
    if user_id not in USER_FAVS:
        USER_FAVS[user_id] = []
    
    if key in USER_FAVS[user_id]:
        USER_FAVS[user_id].remove(key)
        log_event("fav_remove", user_id, row=row)
        await c.answer(t(current_lang_for(user_id), "toast_removed"), show_alert=False)
    else:
        USER_FAVS[user_id].append(key)
        log_event("fav_add", user_id, row=row)
        await c.answer(t(current_lang_for(user_id), "toast_saved"), show_alert=False)
    
    await show_current_card(c, user_id)

@dp.message(lambda m: m.text in (T["btn_favs"]["ru"], T["btn_favs"]["en"], T["btn_favs"]["ka"]))
async def on_favs(message: Message):
    lang = USER_LANG.get(message.from_user.id, "ru")
    user_id = message.from_user.id
    
    if user_id not in USER_FAVS or not USER_FAVS[user_id]:
        return await message.answer(t(lang, "no_results"))
    
    try:
        rows = await rows_async()
    except Exception as e:
        return await message.answer(f"Ошибка загрузки данных: {e}")
    
    fav_keys = USER_FAVS[user_id]
    fav_rows = [r for r in rows if make_row_key(r) in fav_keys]
    
    USER_RESULTS[user_id] = {"rows": fav_rows, "idx": 0, "context": {}}
    await message.answer(t(lang, "results_found", n=len(fav_rows)))
    await show_current_card(message, user_id)

# ===== УПРОЩЕННЫЙ ЗАПУСК =====
async def on_startup():
    try:
        await rows_async(force=True)
        logger.info("✅ Initial data loaded successfully")
    except Exception as e:
        logger.warning(f"Preload failed: {e}")
    asyncio.create_task(_auto_refresh_loop())
    logger.info("✅ Bot started successfully")

async def on_shutdown():
    logger.info("🛑 Bot shutting down...")
    if robust_bot.session:
        await robust_bot.session.close()

# ===== ГЛАВНЫЙ ЗАПУСК =====
async def main():
    with SingleInstance():  # Гарантия одного экземпляра
        await on_startup()
        try:
            logger.info("🔄 Starting polling...")
            await dp.start_polling(bot, skip_updates=True)
        except Exception as e:
            logger.critical(f"❌ Polling failed: {e}")
        finally:
            await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
