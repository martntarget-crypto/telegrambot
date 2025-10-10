# LivePlace Telegram Bot — FINAL v4.7.0 FIXED for Aiogram 3.x
# (complete stability rewrite + multiple instances fix + graceful degradation + Aiogram 3.x compatibility)

import os
import re
import csv
import asyncio
import logging
import random
import time
import json
import hashlib
import fcntl
import sys
import psutil
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
from time import monotonic
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from collections import Counter, defaultdict

# ---- Aiogram 3.x Imports ----
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
)
from aiogram.filters import Command, Text

# ---- Configuration and Logging ----
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("liveplace")

# ---- Enhanced Singleton Protection ----
def ensure_singleton():
    """Prevent multiple instances from running simultaneously with PID checking"""
    lock_file_path = "/tmp/liveplace_bot.lock"
    
    try:
        # Check if lock file exists and if process is still running
        if os.path.exists(lock_file_path):
            try:
                with open(lock_file_path, 'r') as f:
                    old_pid = int(f.read().strip())
                
                # Check if process with that PID is still running
                if psutil.pid_exists(old_pid):
                    try:
                        process = psutil.Process(old_pid)
                        cmdline = " ".join(process.cmdline()).lower()
                        if "python" in cmdline and "liveplace" in cmdline:
                            logger.error(f"Another bot instance is running with PID {old_pid}")
                            sys.exit(1)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # Process doesn't exist or we can't access it, remove stale lock
                        os.remove(lock_file_path)
            except (ValueError, IOError):
                # Lock file is corrupted, remove it
                os.remove(lock_file_path)
        
        # Create new lock file
        lock_file = open(lock_file_path, 'w')
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        logger.info("Singleton lock acquired successfully")
        return lock_file
    except IOError:
        logger.error("Another instance of the bot is already running!")
        sys.exit(1)
    except Exception as e:
        logger.warning(f"Could not create lock file: {e}")
        return None

_singleton_lock = ensure_singleton()

# ---- Environment Variables ----
class Config:
    API_TOKEN = os.getenv("API_TOKEN", "7539402706:AAHcwEJtDFpb0gXB9i6pc7wfq172Ivql_EI").strip()
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or "0")
    FEEDBACK_CHAT_ID = int(os.getenv("FEEDBACK_CHAT_ID", "0"))
    GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
    GSHEET_TAB = os.getenv("GSHEET_TAB", "Ads").strip()
    GSHEET_REFRESH_MIN = int(os.getenv("GSHEET_REFRESH_MIN", "2"))
    
    UTM_SOURCE = os.getenv("UTM_SOURCE", "telegram")
    UTM_MEDIUM = os.getenv("UTM_MEDIUM", "bot")
    UTM_CAMPAIGN = os.getenv("UTM_CAMPAIGN", "bot_ads")
    
    GSHEET_STATS_ID = os.getenv("GSHEET_STATS_ID", "").strip()
    WEEKLY_REPORT_DOW = int(os.getenv("WEEKLY_REPORT_DOW", "1") or "1")
    WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "9") or "9")
    
    ADS_ENABLED = os.getenv("ADS_ENABLED", "1").strip() not in {"0", "false", "False", ""}
    ADS_PROB = float(os.getenv("ADS_PROB", "0.18"))
    ADS_COOLDOWN_SEC = int(os.getenv("ADS_COOLDOWN_SEC", "180"))

if not Config.API_TOKEN:
    raise RuntimeError("API_TOKEN is not set")

# ---- Admin Management ----
ADMINS_RAW = os.getenv("ADMINS", "").strip()
ADMINS_SET = set(int(x) for x in ADMINS_RAW.split(",") if x.strip().isdigit())
if Config.ADMIN_CHAT_ID:
    ADMINS_SET.add(Config.ADMIN_CHAT_ID)

def is_admin(user_id: int) -> bool:
    return user_id in ADMINS_SET

# ---- Stable Bot Implementation ----
class StableBot(Bot):
    async def get_updates(self, *args, **kwargs):
        try:
            return await super().get_updates(*args, **kwargs)
        except Exception as e:
            logger.error(f"Get updates error: {e}")
            await asyncio.sleep(10)
            return []

# Initialize bot and dispatcher for Aiogram 3.x
bot = StableBot(token=Config.API_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ---- Robust Google Sheets Manager ----
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import RefreshError

class SheetsManager:
    def __init__(self):
        self._gc = None
        self._last_auth_time = 0
        self._auth_retry_delay = 300
        self._auth_error_count = 0
        self._max_auth_errors = 3
        
    def _reauthenticate_if_needed(self):
        now = time.time()
        
        # Don't retry too frequently on auth errors
        if self._auth_error_count >= self._max_auth_errors:
            if now - self._last_auth_time < 3600:
                raise RuntimeError("Too many authentication errors. Waiting before retry.")
            else:
                self._auth_error_count = 0
        
        if self._gc is None or (now - self._last_auth_time) > 3500:
            try:
                CREDS_FILE = "credentials.json"
                if not os.path.exists(CREDS_FILE):
                    raise RuntimeError(f"credentials.json not found at {CREDS_FILE}")
                
                SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
                creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
                self._gc = gspread.authorize(creds)
                
                self._last_auth_time = now
                self._auth_error_count = 0
                logger.info("Google Sheets authenticated successfully")
                
            except RefreshError as e:
                self._auth_error_count += 1
                logger.error(f"Google Sheets token refresh error: {e}")
                self._gc = None
                raise RuntimeError("Google Sheets authentication expired") from e
            except Exception as e:
                self._auth_error_count += 1
                logger.error(f"Google Sheets authentication failed: {e}")
                self._gc = None
                raise
    
    def get_client(self):
        self._reauthenticate_if_needed()
        return self._gc

sheets_manager = SheetsManager()

def open_spreadsheet():
    try:
        gc = sheets_manager.get_client()
        return gc.open_by_key(Config.GSHEET_ID)
    except Exception as e:
        logger.error(f"Failed to open spreadsheet: {e}")
        raise RuntimeError(f"Cannot access spreadsheet: {e}") from e

def get_worksheet():
    try:
        sh = open_spreadsheet()
        return sh.worksheet(Config.GSHEET_TAB)
    except gspread.WorksheetNotFound as e:
        tabs = [w.title for w in sh.worksheets()]
        raise RuntimeError(f"Worksheet '{Config.GSHEET_TAB}' not found. Available: {tabs}") from e
    except Exception as e:
        logger.error(f"Failed to get worksheet: {e}")
        raise

# ---- Data Management with Graceful Degradation ----
REQUIRED_COLUMNS = {
    "mode","city","district","type","rooms","price","published",
    "title_ru","title_en","title_ka","description_ru","description_en","description_ka",
    "phone","photo1","photo2","photo3","photo4","photo5","photo6","photo7","photo8","photo9","photo10"
}

_cached_rows: List[Dict[str, Any]] = []
_cache_loaded_at: float = 0.0
_cache_error_count: int = 0
_MAX_CACHE_ERRORS = 3
_LAST_SUCCESSFUL_LOAD: float = 0.0

def _is_cache_stale() -> bool:
    if not _cached_rows:
        return True
    ttl = max(1, Config.GSHEET_REFRESH_MIN) * 60
    return (monotonic() - _cache_loaded_at) >= ttl

def load_rows(force: bool = False) -> List[Dict[str, Any]]:
    global _cached_rows, _cache_loaded_at, _cache_error_count, _LAST_SUCCESSFUL_LOAD
    
    if _cached_rows and not force and not _is_cache_stale():
        return _cached_rows
        
    if _cache_error_count >= _MAX_CACHE_ERRORS:
        time_since_last_success = monotonic() - _LAST_SUCCESSFUL_LOAD
        if time_since_last_success < 3600:
            logger.warning(f"Using stale data ({len(_cached_rows)} rows) due to repeated errors")
            return _cached_rows or []
        else:
            logger.error("Too many cache errors and data is too old")
            return []
    
    try:
        ws = get_worksheet()
        
        # Basic schema check without failing
        try:
            header = [h.strip() for h in ws.row_values(1)]
            missing = sorted(list(REQUIRED_COLUMNS - set(header)))
            if missing:
                logger.warning(f"Missing columns in sheet: {missing}")
        except Exception as schema_error:
            logger.warning(f"Schema check failed: {schema_error}")
        
        rows = ws.get_all_records()
        _cached_rows = rows
        _cache_loaded_at = monotonic()
        _LAST_SUCCESSFUL_LOAD = _cache_loaded_at
        _cache_error_count = 0
        logger.info(f"Successfully loaded {len(rows)} rows from Google Sheets")
        return rows
        
    except Exception as e:
        _cache_error_count += 1
        logger.error(f"Failed to load rows (attempt {_cache_error_count}/{_MAX_CACHE_ERRORS}): {e}")
        
        if _cached_rows:
            logger.warning("Using cached data due to loading error")
            return _cached_rows
        else:
            logger.error("No cached data available")
            return []

async def rows_async(force: bool = False) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(load_rows, force)

# ---- Internationalization ----
LANGS = ["ru", "en", "ka"]
USER_LANG: Dict[int, str] = {}
LANG_MAP = {"ru":"ru","ru-RU":"ru","en":"en","en-US":"en","en-GB":"en","ka":"ka","ka-GE":"ka"}

T = {
    "menu_title": {"ru": "Главное меню", "en": "Main menu", "ka": "მთავარი მენიუ"},
    "btn_search": {"ru": "🔎 Поиск", "en": "🔎 Search", "ka": "🔎 ძიება"},
    "btn_latest": {"ru": "🆕 Новые", "en": "🆕 Latest", "ka": "🆕 ახალი"},
    "btn_language": {"ru": "🌐 Язык", "en": "🌐 Language", "ka": "🌐 ენა"},
    "btn_about": {"ru": "ℹ️ О боте", "en": "ℹ️ About", "ka": "ℹ️ შესახებ"},
    "btn_fast": {"ru": "🟢 Быстрый подбор", "en": "🟢 Quick picks", "ka": "🟢 სწრაფი არჩევანი"},
    "btn_favs": {"ru": "❤️ Избранное", "en": "❤️ Favorites", "ka": "❤️ რჩეულები"},
    "btn_home": {"ru": "🏠 Меню", "en": "🏠 Menu", "ka": "🏠 მენიუ"},
    "btn_daily": {"ru": "🕓 Посуточно 🆕", "en": "🕓 Daily rent 🆕", "ka": "🕓 დღიურად 🆕"},
    "btn_rent": {"ru": "🏘 Аренда", "en": "🏘 Rent", "ka": "🏘 ქირავდება"},
    "btn_sale": {"ru": "🏠 Продажа", "en": "🏠 Sale", "ka": "🏠 იყიდება"},
    "btn_skip": {"ru": "Пропустить", "en": "Skip", "ka": "გამოტოვება"},
    "btn_more": {"ru": "Ещё…", "en": "More…", "ka": "კიდევ…"},
    "btn_prev": {"ru": "« Назад", "en": "« Prev", "ka": "« უკან"},
    "btn_next": {"ru": "Вперёд »", "en": "Next »", "ka": "წინ »"},
    "btn_like": {"ru": "❤️ Нравится", "en": "❤️ Like", "ka": "❤️ მომეწონა"},
    "btn_dislike": {"ru": "👎 Дизлайк", "en": "👎 Dislike", "ka": "👎 არ მომწონს"},
    "btn_fav_add": {"ru": "⭐ В избранное", "en": "⭐ Favorite", "ka": "⭐ რჩეულებში"},
    "btn_fav_del": {"ru": "⭐ Удалить из избранного", "en": "⭐ Remove favorite", "ka": "⭐ წაშლა"},
    "btn_share": {"ru": "🔗 Поделиться", "en": "🔗 Share", "ka": "🔗 გაზიარება"},

    "start": {
        "ru": "<b>LivePlace</b>\n👋 Привет! Я помогу подобрать <b>идеальную недвижимость в Грузии</b>.\n\n<b>Как это работает?</b>\n— Задам 3–4 простых вопроса\n— Покажу лучшие варианты с фото и телефоном владельца\n— Просто посмотреть? Жми <b>🟢 Быстрый подбор</b>\n\nДобро пожаловать и удачного поиска! 🏡",
        "en": "<b>LivePlace</b>\n👋 Hi! I'll help you find <b>your ideal home in Georgia</b>.\n\n<b>How it works:</b>\n— I ask 3–4 quick questions\n— Show top options with photos and owner phone\n— Just browsing? Tap <b>🟢 Quick picks</b>\n\nWelcome and happy hunting! 🏡",
        "ka": "<b>LivePlace</b>\n👋 გამარჯობა! ერთად ვიპოვოთ <b>იდეალური ბინა საქართველოში</b>.\n\n<b>როგორ მუშაობს:</b>\n— 3–4 მარტივი კითხვა\n— საუკეთესო ვარიანტები ფოტოებითა და მფლობელის ნომრით\n— უბრალოდ გადაათვალიერე? დააჭირე <b>🟢 სწრაფი არჩევანი</b>\n\nკეთილი იყოს თქვენი მობრძანება! 🏡",
    },
    "about": {
        "ru": "LivePlace: быстрый подбор недвижимости в Грузии. Фильтры, 10 фото, телефон владельца, избранное.",
        "en": "LivePlace: fast real-estate search in Georgia. Filters, 10 photos, owner phone, favorites.",
        "ka": "LivePlace: უძრავი ქონების სწრაფი ძიება საქართველოში. ფილტრები, 10 ფოტო, მფლობელის ნომერი, რჩეულები."
    },
    "choose_lang": {"ru": "Выберите язык:", "en": "Choose language:", "ka": "აირჩიე ენა:"},
    "wiz_intro": {"ru": "Выберите режим работы:", "en": "Choose mode:", "ka": "აირჩიეთ რეჟიმი:"},
    "ask_city": {"ru": "🏙 Выберите город:", "en": "🏙 Choose city:", "ka": "🏙 აირჩიეთ ქალაქი:"},
    "ask_district": {"ru": "📍 Выберите район:", "en": "📍 Choose district:", "ka": "📍 აირჩიეთ რაიონი:"},
    "ask_type": {"ru": "🏡 Выберите тип недвижимости:", "en": "🏡 Choose property type:", "ka": "🏡 აირჩიეთ ტიპი:"},
    "ask_rooms": {"ru": "🚪 Количество комнат:", "en": "🚪 Rooms:", "ka": "🚪 ოთახების რაოდენობა:"},
    "ask_price": {"ru": "💵 Бюджет:", "en": "💵 Budget:", "ka": "💵 ბიუჯეტი:"},
    "no_results": {"ru": "Ничего не найдено.", "en": "No results.", "ka": "ვერაფერი მოიძებნა."},
    "results_found": {"ru": "Найдено объявлений: <b>{n}</b>", "en": "Listings found: <b>{n}</b>", "ka": "მოიძებნა განცხადება: <b>{n}</b>"},
    "lead_ask": {"ru": "Оставьте контакт (телефон или @username), и мы свяжем вас с владельцем:", "en": "Leave your contact (phone or @username), we'll connect you with the owner:", "ka": "მოგვაწოდეთ კონტაქტი (ტელეფონი ან @username), დაგაკავშირდებით მფლობელთან:"},
    "lead_ok": {"ru": "Спасибо! Передали менеджеру.", "en": "Thanks! Sent to manager.", "ka": "მადლობა! გადაგზავნილია მენეჯერთან."},
    "label_price": {"ru":"Цена", "en":"Price", "ka":"ფასი"},
    "label_pub": {"ru":"Опубликовано", "en":"Published", "ka":"გამოქვეყნდა"},
    "label_phone": {"ru":"Телефон", "en":"Phone", "ka":"ტელეფონი"},
    "toast_removed": {"ru":"Удалено", "en":"Removed", "ka":"წაშლილია"},
    "toast_saved": {"ru":"Сохранено в избранное", "en":"Saved to favorites", "ka":"რჩეულებში შენახულია"},
    "toast_next": {"ru":"Следующее", "en":"Next", "ka":"შემდეგი"},
    "toast_no_more": {"ru":"Больше объявлений нет", "en":"No more listings", "ka":"სხვა განცხადება აღარ არის"},
    "lead_invalid": {"ru":"Оставьте телефон (+995...) или @username.", "en":"Please leave a phone (+995...) or @username.", "ka":"გთხოვთ მიუთითოთ ტელეფონი (+995...) ან @username."},
    "lead_too_soon": {"ru":"Чуть позже, заявка уже отправлена.", "en":"Please wait, your request was just sent.", "ka":"გთხოვთ მოიცადოთ, თქვენი განაცხადი უკვე გაიგზავნა."},
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
    q["utm_source"] = [Config.UTM_SOURCE]
    q["utm_medium"] = [Config.UTM_MEDIUM]
    q["utm_campaign"] = [Config.UTM_CAMPAIGN]
    q["utm_content"] = [ad_id]
    q["token"] = [token]
    new_q = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

# ---- Menu System ----
def main_menu(lang: str) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(T["btn_fast"][lang]))
    kb.row(KeyboardButton(T["btn_search"][lang]), KeyboardButton(T["btn_latest"][lang]))
    kb.add(KeyboardButton(T["btn_favs"][lang]))
    kb.add(KeyboardButton(T["btn_language"][lang]), KeyboardButton(T["btn_about"][lang]))
    return kb

# ---- Utilities ----
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
    m = re.search(r"/d/([A-Za-z0-9_-]{20,})/", url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    m = re.search(r"[?&]id=([A-Za-z0-9_-]{20,})", url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return url

def looks_like_image(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return False
    if any(u.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
        return True
    if "google.com/uc?export=download" in u or "googleusercontent.com" in u:
        return True
    return False

def collect_photos(row: Dict[str, Any]) -> List[str]:
    photos = []
    for i in range(1, 11):
        url = str(row.get(f"photo{i}", "")).strip()
        if not url:
            continue
        url = drive_direct(url)
        if looks_like_image(url):
            photos.append(url)
    return photos

def parse_rooms(v: Any) -> float:
    s = str(v or "").strip().lower()
    if s in {"студия","studio","stud","სტუდიო"}:
        return 0.5
    try:
        return float(s.replace("+", ""))
    except Exception:
        return -1.0

def format_card(row: Dict[str, Any], lang: str) -> str:
    title_k = LANG_FIELDS[lang]["title"]
    desc_k = LANG_FIELDS[lang]["desc"]
    city = str(row.get("city", "")).strip()
    district = str(row.get("district", "")).strip()
    rtype = str(row.get("type", "")).strip()
    rooms = str(row.get("rooms", "")).strip()
    price = str(row.get("price", "")).strip()
    published = str(row.get("published", "")).strip()
    phone = str(row.get("phone", "")).strip()
    title = str(row.get(title_k, "")).strip()
    desc = str(row.get(desc_k, "")).strip()

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

# ---- User Data Management ----
PAGE_SIZE = 8
CHOICE_CACHE: Dict[int, Dict[str, List[Tuple[str, str]]]] = {}
CHOICE_MSG: Dict[int, Dict[str, int]] = {}
USER_RESULTS: Dict[int, Dict[str, Any]] = {}
USER_FAVS: Dict[int, List[str]] = {}
LEAD_COOLDOWN = 45
LAST_LEAD_AT: Dict[int, float] = {}
LAST_AD_ID: Dict[int, str] = {}
LAST_AD_TIME: Dict[int, float] = {}

# ---- Localization for Choices ----
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

# ---- Advertising System ----
ADS = [
    {"id":"lead_form","text_ru":"🔥 Ищете квартиру быстрее? Оставьте заявку на сайте — подберём за 24 часа!",
     "text_en":"🔥 Need a place fast? Leave a request on our website — we'll find options within 24h!",
     "text_ka":"🔥 ბინა გჭირდებათ სწრაფად? დატოვეთ განაცხადი საიტზე — 24 საათში მოვძებნით ვარიანტებს!",
     "url":"https://liveplace.com.ge/lead","photo":""},
    {"id":"mortgage_help","text_ru":"🏦 Поможем с ипотекой для нерезидентов в Грузии. Узнайте детали на сайте.",
     "text_en":"🏦 Mortgage support for non-residents in Georgia. Learn more on our website.",
     "text_ka":"🏦 იპოთეკა არარეზიდენტებისთვის საქართველოში — დეტალები საიტზე.",
     "url":"https://liveplace.com.ge/mortgage","photo":""},
    {"id":"rent_catalog","text_ru":"🏘 Посмотрите новые квартиры в аренду — актуальные предложения на сайте.",
     "text_en":"🏘 Explore new rentals — fresh listings on our website.",
     "text_ka":"🏘 ნახეთ გაქირავების ახალი ბინები — განახლებული განცხადებები საიტზე.",
     "url":"https://liveplace.com.ge/rent","photo":""},
    {"id":"sell_service","text_ru":"💼 Хотите продать квартиру? Оценим и разместим ваше объявление на LivePlace.",
     "text_en":"💼 Selling your property? We'll valuate and list it on LivePlace.",
     "text_ka":"💼 ყიდით ბინას? შევაფასებთ და დავდებთ LivePlace-ზე.",
     "url":"https://liveplace.com.ge/sell","photo":""},
]

def should_show_ad(uid: int) -> bool:
    if not Config.ADS_ENABLED or not ADS:
        return False
    now = time.time()
    last = LAST_AD_TIME.get(uid, 0.0)
    if now - last < Config.ADS_COOLDOWN_SEC:
        return False
    return random.random() < Config.ADS_PROB

def pick_ad(uid: int, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    mode = context.get("mode", "")
    pool = ADS
    if mode == "sale":
        pool = [a for a in ADS if a["id"] in {"mortgage_help", "sell_service"}] or ADS
    last_id = LAST_AD_ID.get(uid)
    cand = [a for a in pool if a.get("id") != last_id] or pool
    return random.choice(cand) if cand else None

async def maybe_show_ad(message_or_cb, uid: int, context: Dict[str, Any]):
    try:
        if not should_show_ad(uid):
            return
        ad = pick_ad(uid, context) or random.choice(ADS)
        lang = current_lang_for(uid)

        txt = ad.get(f"text_{lang}") or ad.get("text_ru") or "LivePlace"
        url = build_utm_url(ad.get("url"), ad.get("id", "ad"), uid)
        btn = InlineKeyboardMarkup().add(InlineKeyboardButton(cta_text(lang), url=url))

        target = message_or_cb.message if isinstance(message_or_cb, types.CallbackQuery) else message_or_cb

        if ad.get("photo"):
            try:
                await target.answer_photo(ad["photo"], caption=txt, reply_markup=btn)
            except Exception:
                await target.answer(txt, reply_markup=btn)
        else:
            await target.answer(txt, reply_markup=btn)

        LAST_AD_TIME[uid] = time.time()
        LAST_AD_ID[uid] = ad.get("id")
        try:
            log_event("ad_show", uid, row=None, extra={"ad_id": ad.get("id", "unknown")})
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"maybe_show_ad failed: {e}")

# ---- Search States ----
class Search(StatesGroup):
    mode = State()
    city = State()
    district = State()
    rtype = State()
    rooms = State()
    price = State()

# ---- Analytics System ----
def _today_str():
    return datetime.utcnow().strftime("%Y-%m-%d")

ANALYTIC_EVENTS: List[Dict[str, Any]] = []
AGG_BY_DAY = defaultdict(lambda: Counter())
AGG_BY_MODE = defaultdict(lambda: Counter())
AGG_CITY = defaultdict(lambda: Counter())
AGG_DISTRICT = defaultdict(lambda: Counter())
AGG_FUNNEL = defaultdict(lambda: Counter())
TOP_LISTINGS = defaultdict(lambda: Counter())
TOP_LIKES = defaultdict(lambda: Counter())
TOP_FAVS = defaultdict(lambda: Counter())

ANALYTICS_SNAPSHOT = "analytics_snapshot.json"
SNAPSHOT_INTERVAL_SEC = 300

def make_row_key(r: Dict[str,Any]) -> str:
    payload = "|".join([
        str(r.get("city","")), str(r.get("district","")),
        str(r.get("type","")), str(r.get("rooms","")),
        str(r.get("price","")), str(r.get("phone","")),
        str(r.get("title_ru") or r.get("title_en") or r.get("title_ka") or "")
    ])
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

def _row_info(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "mode": norm_mode(row.get("mode","")),
        "city": str(row.get("city","")).strip(),
        "district": str(row.get("district","")).strip(),
        "price": float(row.get("price") or 0),
        "rooms": str(row.get("rooms","")).strip(),
        "key": make_row_key(row),
        "title": str(row.get("title_ru") or row.get("title_en") or row.get("title_ka") or "").strip(),
    }

def log_event(event: str, uid: int, row: Dict[str,Any]=None, extra: Dict[str,Any]=None):
    ts = datetime.utcnow().isoformat(timespec="seconds")
    day = _today_str()
    payload = {"ts": ts, "day": day, "event": event, "uid": uid}
    info = {}
    if row:
        info = _row_info(row)
        payload.update(info)
    if extra:
        payload.update(extra)

    ANALYTIC_EVENTS.append(payload)

    AGG_BY_DAY[day][event] += 1
    if info.get("mode"):
        AGG_BY_MODE[day][f"{info['mode']}_{event}"] += 1
    if info.get("city") and event in ("view","like","lead"):
        AGG_CITY[day][info["city"]] += 1
    if info.get("district") and event in ("view","like","lead"):
        AGG_DISTRICT[day][info["district"]] += 1
    if event in ("search","view","like","lead"):
        AGG_FUNNEL[day][event] += 1
    if info.get("key"):
        if event == "view":
            TOP_LISTINGS[day][info["key"]] += 1
        if event == "like":
            TOP_LIKES[day][info["key"]] += 1
        if event == "fav_add":
            TOP_FAVS[day][info["key"]] += 1

def render_stats(day: str=None) -> str:
    day = day or _today_str()
    total = AGG_BY_DAY[day]
    mode = AGG_BY_MODE[day]
    city = AGG_CITY[day].most_common(5)
    dist = AGG_DISTRICT[day].most_common(5)
    fun = AGG_FUNNEL[day]
    top_v = TOP_LISTINGS[day].most_common(3)
    top_l = TOP_LIKES[day].most_common(3)
    top_f = TOP_FAVS[day].most_common(3)

    def pct(a,b): return f"{(a/b*100):.1f}%" if b else "0.0%"
    conv_like = pct(fun["like"], max(fun["view"],1))
    conv_lead = pct(fun["lead"], max(fun["view"],1))

    lines = []
    lines.append(f"📊 <b>Статистика за {day}</b>")
    lines.append(f"Реклама показов: {total['ad_show']}")
    lines.append(f"Всего: просмотров {total['view']}, лайков {total['like']}, дизлайков {total['dislike']}, заявок {total['lead']}, избранное +{total['fav_add']}/-{total['fav_remove']}")
    lines.append(f"Воронка: search {fun['search']} → view {fun['view']} → like {fun['like']} ({conv_like}) → lead {fun['lead']} ({conv_lead})\n")
    lines.append("По режимам:")
    for m in ("rent","daily","sale"):
        lines.append(f"  • {m}: view {mode[f'{m}_view']}, like {mode[f'{m}_like']}, lead {mode[f'{m}_lead']}")

    if city:
        lines.append("\nТоп городов: " + ", ".join([f"{c} {n}" for c,n in city]))
    if dist:
        lines.append("Топ районов: " + ", ".join([f"{d} {n}" for d,n in dist]))
    if top_v:
        lines.append("\nТоп объявлений по просмотрам:")
        for key, n in top_v:
            lines.append(f"  • {key}: {n}")
    if top_l:
        lines.append("Топ по лайкам:")
        for key, n in top_l:
            lines.append(f"  • {key}: {n}")
    if top_f:
        lines.append("Топ по избранному:")
        for key, n in top_f:
            lines.append(f"  • {key}: {n}")
    return "\n".join(lines)

def render_week_summary(end_day: str=None) -> str:
    if not end_day:
        end_day = _today_str()
    end_dt = datetime.fromisoformat(end_day)
    days = [(end_dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6,-1,-1)]
    total = Counter()
    for d in days:
        total += AGG_BY_DAY[d]
    lines = [f"📈 <b>Сводка за 7 дней (до {end_day})</b>",
             f"Просмотры {total['view']}, лайки {total['like']}, дизлайки {total['dislike']}, заявки {total['lead']}, избранное +{total['fav_add']}/-{total['fav_remove']}"]
    return "\n".join(lines)

def export_analytics_csv(path: str = "analytics_export.csv"):
    keys = ["ts","day","event","uid","mode","city","district","price","rooms","key","title","contact","found"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for ev in ANALYTIC_EVENTS:
            w.writerow({k: ev.get(k,"") for k in keys})
    return path

# ---- Analytics Persistence ----
def save_analytics_snapshot():
    try:
        data = {
          "ANALYTIC_EVENTS": ANALYTIC_EVENTS,
          "AGG_BY_DAY": {k: dict(v) for k,v in AGG_BY_DAY.items()},
          "AGG_BY_MODE": {k: dict(v) for k,v in AGG_BY_MODE.items()},
          "AGG_CITY": {k: dict(v) for k,v in AGG_CITY.items()},
          "AGG_DISTRICT": {k: dict(v) for k,v in AGG_DISTRICT.items()},
          "AGG_FUNNEL": {k: dict(v) for k,v in AGG_FUNNEL.items()},
          "TOP_LISTINGS": {k: dict(v) for k,v in TOP_LISTINGS.items()},
          "TOP_LIKES": {k: dict(v) for k,v in TOP_LIKES.items()},
          "TOP_FAVS": {k: dict(v) for k,v in TOP_FAVS.items()},
        }
        with open(ANALYTICS_SNAPSHOT, "w", encoding="utf-8") as f:
            json.dump(data, f)
        logger.debug("Analytics snapshot saved")
    except Exception as e:
        logger.error(f"Failed to save analytics snapshot: {e}")

def load_analytics_snapshot():
    if not os.path.exists(ANALYTICS_SNAPSHOT):
        return
    try:
        with open(ANALYTICS_SNAPSHOT, "r", encoding="utf-8") as f:
            data = json.load(f)
        ANALYTIC_EVENTS.extend(data.get("ANALYTIC_EVENTS", []))
        for k, d in data.get("AGG_BY_DAY", {}).items():
            AGG_BY_DAY[k].update(d)
        for k, d in data.get("AGG_BY_MODE", {}).items():
            AGG_BY_MODE[k].update(d)
        for k, d in data.get("AGG_CITY", {}).items():
            AGG_CITY[k].update(d)
        for k, d in data.get("AGG_DISTRICT", {}).items():
            AGG_DISTRICT[k].update(d)
        for k, d in data.get("AGG_FUNNEL", {}).items():
            AGG_FUNNEL[k].update(d)
        for k, d in data.get("TOP_LISTINGS", {}).items():
            TOP_LISTINGS[k].update(d)
        for k, d in data.get("TOP_LIKES", {}).items():
            TOP_LIKES[k].update(d)
        for k, d in data.get("TOP_FAVS", {}).items():
            TOP_FAVS[k].update(d)
        logger.info("Analytics snapshot loaded")
    except Exception as e:
        logger.warning(f"load snapshot failed: {e}")

# ---- Google Sheets Statistics ----
def _open_stats_book():
    if not Config.GSHEET_STATS_ID:
        raise RuntimeError("GSHEET_STATS_ID is not set")
    try:
        return sheets_manager.get_client().open_by_key(Config.GSHEET_STATS_ID)
    except Exception as e:
        raise RuntimeError("Cannot open GSHEET_STATS_ID (check sharing/ID)") from e

def _ensure_sheet(sh, title: str, header: List[str]):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=1000, cols=max(10, len(header)))
        ws.append_row(header)
    return ws

DAILY_HEADER = [
    "day","views","likes","dislikes","leads","fav_add","fav_remove",
    "rent_view","rent_like","rent_lead",
    "daily_view","daily_like","daily_lead",
    "sale_view","sale_like","sale_lead"
]
TOP_HEADER = ["day","metric","rank","key","count"]

def push_daily_to_sheet(day: str):
    try:
        sh = _open_stats_book()
        ws = _ensure_sheet(sh, "Daily", DAILY_HEADER)

        mode = AGG_BY_MODE[day]
        total = AGG_BY_DAY[day]
        row = [
            day,
            total["view"], total["like"], total["dislike"], total["lead"], total["fav_add"], total["fav_remove"],
            mode["rent_view"], mode["rent_like"], mode["rent_lead"],
            mode["daily_view"], mode["daily_like"], mode["daily_lead"],
            mode["sale_view"], mode["sale_like"], mode["sale_lead"],
        ]

        existing = ws.col_values(1)
        if day in existing:
            idx = existing.index(day) + 1
            ws.update(f"A{idx}:P{idx}", [row])
        else:
            ws.append_row(row)
    except Exception as e:
        logger.error(f"Failed to push daily stats: {e}")
        raise

def push_top_to_sheet(day: str, top_n: int = 20):
    try:
        sh = _open_stats_book()
        ws = _ensure_sheet(sh, "Top", TOP_HEADER)

        def write_block(metric: str, counter: Counter):
            rows = []
            for i, (key, cnt) in enumerate(counter.most_common(top_n), start=1):
                rows.append([day, metric, i, key, cnt])
            if rows:
                ws.append_rows(rows)

        write_block("views", TOP_LISTINGS[day])
        write_block("likes", TOP_LIKES[day])
        write_block("favorites", TOP_FAVS[day])
    except Exception as e:
        logger.error(f"Failed to push top stats: {e}")
        raise

def push_day_all(day: str):
    push_daily_to_sheet(day)
    push_top_to_sheet(day)

# ---- Background Tasks Management ----
_background_tasks = set()

async def create_background_task(coro, task_name: str):
    """Safely create and track background tasks"""
    task = asyncio.create_task(coro, name=task_name)
    _background_tasks.add(task)
    
    def remove_task(fut):
        _background_tasks.discard(task)
        if fut.exception():
            logger.error(f"Background task {task_name} failed: {fut.exception()}")
    
    task.add_done_callback(remove_task)
    return task

async def _auto_refresh_loop():
    """Improved auto-refresh with graceful degradation"""
    error_count = 0
    base_delay = 60
    max_delay = 1800
    
    while True:
        try:
            if _is_cache_stale():
                await rows_async(force=True)
                error_count = 0
                base_delay = 60
                logger.debug("Sheets cache refreshed successfully")
            else:
                await asyncio.sleep(base_delay)
                continue
                
        except Exception as e:
            error_count += 1
            delay = min(base_delay * (2 ** min(error_count, 6)), max_delay)
            delay *= random.uniform(0.8, 1.2)
            
            if error_count <= 3:
                logger.error(f"Auto refresh failed (attempt {error_count}), retrying in {delay:.1f}s: {e}")
            else:
                logger.warning(f"Auto refresh still failing (attempt {error_count}), retrying in {delay:.1f}s")
            
            await asyncio.sleep(delay)

async def _midnight_flush_loop():
    """Improved midnight flush that doesn't crash on errors"""
    already_processed = set()
    
    while True:
        try:
            now = datetime.utcnow()
            if now.hour == 0 and now.minute >= 5:
                day = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                mark = f"{day}-flush"
                
                if mark not in already_processed and Config.GSHEET_STATS_ID:
                    try:
                        await asyncio.to_thread(push_day_all, day)
                        already_processed.add(mark)
                        logger.info(f"Successfully pushed analytics for {day}")
                    except Exception as e:
                        logger.error(f"Failed to push analytics for {day}: {e}")
                        already_processed.add(mark)
            
            cutoff = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d")
            already_processed = {m for m in already_processed if m.split("-")[0] >= cutoff}
            
        except Exception as e:
            logger.error(f"Midnight flush loop error: {e}")
        
        await asyncio.sleep(300)

async def _weekly_report_loop():
    """Improved weekly report that handles errors gracefully"""
    sent_reports = set()
    
    while True:
        try:
            now = datetime.utcnow()
            dow = now.isoweekday()
            
            if (dow == Config.WEEKLY_REPORT_DOW and 
                now.hour == Config.WEEKLY_REPORT_HOUR and 
                now.minute < 5):
                
                report_key = now.strftime("%Y-%U")
                
                if report_key not in sent_reports:
                    try:
                        text = render_week_summary()
                        await bot.send_message(Config.ADMIN_CHAT_ID, text)
                        sent_reports.add(report_key)
                        logger.info("Weekly report sent successfully")
                    except Exception as e:
                        logger.error(f"Failed to send weekly report: {e}")
            
            current_year = datetime.utcnow().year
            sent_reports = {r for r in sent_reports if r.startswith(str(current_year))}
            
        except Exception as e:
            logger.error(f"Weekly report loop error: {e}")
        
        await asyncio.sleep(600)

async def _snapshot_loop():
    """Improved snapshot loop that doesn't crash on errors"""
    while True:
        try:
            save_analytics_snapshot()
        except Exception as e:
            logger.error(f"Snapshot save failed: {e}")
        await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)

# ---- Improved Startup with Better Error Recovery ----
async def startup():
    """Improved startup that doesn't crash on initial failures"""
    logger.info("Starting bot initialization...")
    
    # Try to load data but don't crash if it fails
    try:
        await rows_async(force=True)
        logger.info("Initial data loaded successfully")
    except Exception as e:
        logger.error(f"Initial data load failed: {e}")
        logger.info("Bot will start with empty cache and retry in background")
    
    # Load analytics snapshot
    try:
        load_analytics_snapshot()
    except Exception as e:
        logger.warning(f"Failed to load analytics snapshot: {e}")
    
    # Start background tasks with error handling
    tasks = [
        ("auto_refresh", _auto_refresh_loop()),
        ("midnight_flush", _midnight_flush_loop()),
        ("weekly_report", _weekly_report_loop()),
        ("snapshot", _snapshot_loop())
    ]
    
    for task_name, task_coro in tasks:
        try:
            await create_background_task(task_coro, task_name)
            logger.info(f"Started background task: {task_name}")
        except Exception as e:
            logger.error(f"Failed to start background task {task_name}: {e}")
    
    logger.info(f"Bot started successfully. Admin IDs: {sorted(ADMINS_SET)}")
    logger.info(f"Cache status: {len(_cached_rows)} rows loaded")
    
    # Send startup notification to admin
    if Config.ADMIN_CHAT_ID:
        try:
            cache_status = f"{len(_cached_rows)} rows" if _cached_rows else "EMPTY"
            await bot.send_message(
                Config.ADMIN_CHAT_ID,
                f"🤖 Bot started successfully\n"
                f"📊 Cache: {cache_status}\n"
                f"🕒 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        except Exception as e:
            logger.warning(f"Failed to send startup notification: {e}")

async def shutdown():
    """Clean shutdown handler with better cleanup"""
    logger.info("Shutting down bot...")
    
    # Cancel all background tasks
    for task in _background_tasks:
        task.cancel()
    
    if _background_tasks:
        try:
            await asyncio.wait(_background_tasks, timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Some background tasks didn't finish in time")
    
    # Save analytics before shutdown
    try:
        save_analytics_snapshot()
    except Exception as e:
        logger.error(f"Failed to save analytics on shutdown: {e}")
    
    # Close bot session
    try:
        await bot.session.close()
    except Exception as e:
        logger.error(f"Error closing bot session: {e}")
    
    # Release singleton lock
    if _singleton_lock:
        try:
            lock_file_path = "/tmp/liveplace_bot.lock"
            if os.path.exists(lock_file_path):
                os.remove(lock_file_path)
            _singleton_lock.close()
        except Exception as e:
            logger.warning(f"Error removing lock file: {e}")
    
    logger.info("Bot shutdown complete")

# ---- Handlers for Aiogram 3.x ----
@dp.message(Command("start", "menu"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in USER_LANG:
        code = (message.from_user.language_code or "").strip()
        USER_LANG[message.from_user.id] = LANG_MAP.get(code, "ru")
    lang = USER_LANG[message.from_user.id]
    await state.clear()
    await message.answer(t(lang, "start"), reply_markup=main_menu(lang))

@dp.message(Command("home"))
async def cmd_home(message: types.Message, state: FSMContext):
    lang = USER_LANG.get(message.from_user.id, "ru")
    await state.clear()
    await message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))

@dp.message(Command("lang_ru", "lang_en", "lang_ka"))
async def cmd_lang(message: types.Message):
    code = message.text.replace("/", "").replace("lang_", "")
    if code not in LANGS:
        code = "ru"
    USER_LANG[message.from_user.id] = code
    await message.answer(t(code, "menu_title"), reply_markup=main_menu(code))

@dp.message(Command("whoami"))
async def cmd_whoami(message: types.Message, state: FSMContext):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@dp.message(Command("admin_debug"))
async def cmd_admin_debug(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    await message.answer(
        "Debug:\n"
        f"ADMIN_CHAT_ID: <code>{Config.ADMIN_CHAT_ID}</code>\n"
        f"ADMINS (.env): <code>{os.getenv('ADMINS','')}</code>\n"
        f"ADMINS_SET: <code>{sorted(ADMINS_SET)}</code>\n"
        f"GSHEET_STATS_ID: <code>{Config.GSHEET_STATS_ID or '(not set)'}</code>\n"
        f"Weekly: DOW={Config.WEEKLY_REPORT_DOW}, HOUR={Config.WEEKLY_REPORT_HOUR} (UTC)\n"
        f"ADS: enabled={Config.ADS_ENABLED}, prob={Config.ADS_PROB}, cooldown={Config.ADS_COOLDOWN_SEC}s\n"
        f"Cache rows: {len(_cached_rows)}, TTLmin={Config.GSHEET_REFRESH_MIN}"
    )

@dp.message(Command("health"))
async def cmd_health(message: types.Message):
    try:
        sh = await asyncio.to_thread(open_spreadsheet)
        tabs = [w.title for w in sh.worksheets()]
        ws = await asyncio.to_thread(get_worksheet)
        header = ws.row_values(1)
        sample = ws.row_values(2)
        stats = f"stats_book={'set' if Config.GSHEET_STATS_ID else 'unset'}"
        await message.answer(
            "✅ Connected\n"
            f"Tab: <b>{Config.GSHEET_TAB}</b>\n"
            f"Tabs: {tabs}\n"
            f"Header: {header}\n"
            f"Row2: {sample}\n"
            f"{stats}\n"
            f"Cache rows: {len(_cached_rows)} (stale={_is_cache_stale()})"
        )
    except Exception as e:
        await message.answer(f"❌ {e}")

@dp.message(Command("gs"))
async def cmd_gs(message: types.Message):
    try:
        rows = await rows_async(force=True)
        await message.answer(f"GS rows: {len(rows)}")
    except Exception as e:
        await message.answer(f"GS error: {e}")

@dp.message(Command("reload", "refresh"))
async def cmd_reload(message: types.Message):
    try:
        rows = await rows_async(force=True)
        await message.answer(f"♻️ Reloaded. Rows: {len(rows)}")
    except Exception as e:
        await message.answer(f"Reload error: {e}")

# ---- Main Execution with Proper Event Loop ----
async def main():
    """Main async function for Aiogram 3.x with proper event loop handling"""
    try:
        logger.info("Starting LivePlace bot with improved stability...")
        
        # Clean up any stale webhook
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        
        # Run startup tasks
        await startup()
        
        logger.info("Starting polling...")
        # Start polling with proper event loop
        await dp.start_polling(bot)
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        
        # Try to send crash notification
        if Config.ADMIN_CHAT_ID:
            try:
                await bot.send_message(
                    Config.ADMIN_CHAT_ID,
                    f"🚨 Bot crashed:\n{str(e)}"
                )
            except Exception:
                pass
    finally:
        # Ensure cleanup happens
        await shutdown()
        logger.info("Bot process ended")

if __name__ == "__main__":
    try:
        # Proper asyncio run for Aiogram 3.x
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
