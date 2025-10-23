# -*- coding: utf-8 -*-
"""
LivePlace Telegram Bot — версия с постоянным хранением статистики и анимированными лайками
"""

import os
import re
import json
import time
import random
import asyncio
import logging
import sqlite3
from time import monotonic
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from collections import Counter, defaultdict
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from contextlib import contextmanager

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto,
    ReactionTypeEmoji
)

import gspread
from google.oauth2.service_account import Credentials

# ------ Logging ------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("liveplace")

# ------ Load .env ------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ------ Config ------
class Config:
    API_TOKEN = os.getenv("API_TOKEN", "").strip()
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or "0")
    FEEDBACK_CHAT_ID = int(os.getenv("FEEDBACK_CHAT_ID", "-4852620232") or "-4852620232")
    SHEETS_ENABLED = os.getenv("SHEETS_ENABLED", "1").strip() not in {"", "0", "false", "False"}
    GSHEET_ID = os.getenv("GSHEET_ID", "1yrB5Vy7o18B05nkJBqQe9hE9971jJsTMEKKTsDHGa8w").strip()
    GSHEET_TAB = os.getenv("GSHEET_TAB", "Ads").strip()
    GSHEET_REFRESH_SEC = int(os.getenv("GSHEET_REFRESH_SEC", "120") or "120")
    ADS_ENABLED = os.getenv("ADS_ENABLED", "1").strip() not in {"0", "false", "False", ""}
    ADS_PROB = float(os.getenv("ADS_PROB", "0.18") or 0.18)
    ADS_COOLDOWN_SEC = int(os.getenv("ADS_COOLDOWN_SEC", "180") or 180)
    UTM_SOURCE = os.getenv("UTM_SOURCE", "telegram")
    UTM_MEDIUM = os.getenv("UTM_MEDIUM", "bot")
    UTM_CAMPAIGN = os.getenv("UTM_CAMPAIGN", "bot_ads")
    MEDIA_RETRY_COUNT = 3
    MEDIA_RETRY_DELAY = 2
    DB_PATH = os.getenv("DB_PATH", "liveplace_stats.db")
    
    # Стикеры с сердечками для анимации лайков (можно заменить на свои)
    HEART_STICKERS = [
        "CAACAgIAAxkBAAEMYBZnNm7vQoE8_Hq9Q-T0AAHxAAGVMXYAAiEPAAKOXQlL0vW8kCWLvrc2BA",
    ]

if not Config.API_TOKEN:
    raise RuntimeError("API_TOKEN is not set")

# ------ Bot & Dispatcher ------
bot = Bot(token=Config.API_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())

# ------ Database Manager ------
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_valid_db()
        self.init_db()
    
    def _ensure_valid_db(self):
        """Проверяет и создаёт корректную БД если нужно"""
        if os.path.exists(self.db_path):
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
                conn.close()
                logger.info(f"✅ Database file exists and is valid: {self.db_path}")
                return
            except Exception as e:
                logger.warning(f"⚠️ Invalid database file detected: {e}")
                logger.info(f"🗑 Attempting to remove corrupted database: {self.db_path}")
                try:
                    os.remove(self.db_path)
                    logger.info("✅ Corrupted database removed, will create new one")
                except Exception as remove_error:
                    logger.error(f"❌ Failed to remove corrupted database: {remove_error}")
                    backup_name = f"{self.db_path}.backup_{int(time.time())}"
                    try:
                        os.rename(self.db_path, backup_name)
                        logger.info(f"📝 Renamed corrupted DB to: {backup_name}")
                    except Exception:
                        self.db_path = f"/tmp/liveplace_stats_{int(time.time())}.db"
                        logger.warning(f"⚠️ Using temporary database: {self.db_path}")
        else:
            logger.info(f"📝 Database file does not exist, will create new: {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def init_db(self):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_actions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uid INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        data TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS searches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uid INTEGER NOT NULL,
                        mode TEXT,
                        city TEXT,
                        district TEXT,
                        rooms TEXT,
                        price TEXT,
                        price_min REAL,
                        price_max REAL,
                        results_count INTEGER,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS leads (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uid INTEGER NOT NULL,
                        name TEXT,
                        phone TEXT,
                        ad_data TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS favorites (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uid INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        ad_data TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS first_seen (
                        uid INTEGER PRIMARY KEY,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_actions_timestamp ON user_actions(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_actions_uid ON user_actions(uid)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_searches_timestamp ON searches(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_timestamp ON leads(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_favorites_timestamp ON favorites(timestamp)")
                
                conn.commit()
                logger.info(f"✅ Database initialized successfully at {self.db_path}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize database: {e}")
            logger.error(f"Database path: {self.db_path}")
            logger.error("Trying to create database in /tmp instead...")
            
            self.db_path = f"/tmp/liveplace_stats_{int(time.time())}.db"
            logger.info(f"Using fallback path: {self.db_path}")
            
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("CREATE TABLE IF NOT EXISTS user_actions (id INTEGER PRIMARY KEY)")
                    logger.info("✅ Fallback database created successfully")
            except Exception as final_error:
                logger.critical(f"💥 Cannot create database anywhere: {final_error}")
                raise
    
    def log_action(self, uid: int, action: str, data: Optional[Dict[str, Any]] = None):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO user_actions (uid, action, data) VALUES (?, ?, ?)",
                    (uid, action, json.dumps(data) if data else None)
                )
        except Exception as e:
            logger.error(f"Failed to log action: {e}")
    
    def log_search(self, uid: int, query: Dict[str, Any], results_count: int):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO searches (uid, mode, city, district, rooms, price, price_min, price_max, results_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        uid,
                        query.get("mode", ""),
                        query.get("city", ""),
                        query.get("district", ""),
                        query.get("rooms", ""),
                        query.get("price", ""),
                        query.get("price_min"),
                        query.get("price_max"),
                        results_count
                    )
                )
        except Exception as e:
            logger.error(f"Failed to log search: {e}")
    
    def log_lead(self, uid: int, name: str, phone: str, ad_data: Dict[str, Any]):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO leads (uid, name, phone, ad_data) VALUES (?, ?, ?, ?)",
                    (uid, name, phone, json.dumps(ad_data))
                )
        except Exception as e:
            logger.error(f"Failed to log lead: {e}")
    
    def log_favorite(self, uid: int, action: str, ad_data: Dict[str, Any]):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO favorites (uid, action, ad_data) VALUES (?, ?, ?)",
                    (uid, action, json.dumps(ad_data))
                )
        except Exception as e:
            logger.error(f"Failed to log favorite: {e}")
    
    def register_user(self, uid: int):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO first_seen (uid) VALUES (?)",
                    (uid,)
                )
        except Exception as e:
            logger.error(f"Failed to register user: {e}")
    
    def get_stats(self, days: int = 1) -> Dict[str, Any]:
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    "SELECT COUNT(DISTINCT uid) FROM user_actions WHERE timestamp >= ?",
                    (cutoff_str,)
                )
                unique_users = cursor.fetchone()[0]
                
                cursor.execute(
                    "SELECT COUNT(*) FROM first_seen WHERE timestamp >= ?",
                    (cutoff_str,)
                )
                new_users = cursor.fetchone()[0]
                
                cursor.execute(
                    "SELECT COUNT(*) FROM user_actions WHERE timestamp >= ?",
                    (cutoff_str,)
                )
                total_actions = cursor.fetchone()[0]
                
                cursor.execute(
                    "SELECT COUNT(*) FROM searches WHERE timestamp >= ?",
                    (cutoff_str,)
                )
                searches_count = cursor.fetchone()[0]
                
                cursor.execute(
                    "SELECT COUNT(*) FROM leads WHERE timestamp >= ?",
                    (cutoff_str,)
                )
                leads_count = cursor.fetchone()[0]
                
                cursor.execute(
                    "SELECT COUNT(*) FROM favorites WHERE action = 'add' AND timestamp >= ?",
                    (cutoff_str,)
                )
                favorites_added = cursor.fetchone()[0]
                
                cursor.execute(
                    "SELECT COUNT(*) FROM favorites WHERE action = 'remove' AND timestamp >= ?",
                    (cutoff_str,)
                )
                favorites_removed = cursor.fetchone()[0]
                
                cursor.execute(
                    "SELECT action, COUNT(*) as count FROM user_actions WHERE timestamp >= ? GROUP BY action",
                    (cutoff_str,)
                )
                action_counts = {row['action']: row['count'] for row in cursor.fetchall()}
                
                cursor.execute(
                    "SELECT mode, COUNT(*) as count FROM searches WHERE timestamp >= ? AND mode != '' GROUP BY mode",
                    (cutoff_str,)
                )
                mode_counts = {row['mode']: row['count'] for row in cursor.fetchall()}
                
                cursor.execute(
                    "SELECT city, COUNT(*) as count FROM searches WHERE timestamp >= ? AND city != '' GROUP BY city ORDER BY count DESC LIMIT 10",
                    (cutoff_str,)
                )
                city_counts = {row['city']: row['count'] for row in cursor.fetchall()}
                
                cursor.execute(
                    "SELECT AVG(results_count) FROM searches WHERE timestamp >= ? AND results_count > 0",
                    (cutoff_str,)
                )
                avg_results = cursor.fetchone()[0] or 0
                
                conversion_rate = (leads_count / searches_count * 100) if searches_count > 0 else 0
                
                return {
                    "period_days": days,
                    "unique_users": unique_users,
                    "new_users": new_users,
                    "total_actions": total_actions,
                    "searches": searches_count,
                    "leads": leads_count,
                    "favorites_added": favorites_added,
                    "favorites_removed": favorites_removed,
                    "action_counts": action_counts,
                    "mode_counts": mode_counts,
                    "city_counts": city_counts,
                    "avg_results_per_search": round(avg_results, 1),
                    "conversion_rate": round(conversion_rate, 2)
                }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {
                "period_days": days,
                "unique_users": 0,
                "new_users": 0,
                "total_actions": 0,
                "searches": 0,
                "leads": 0,
                "favorites_added": 0,
                "favorites_removed": 0,
                "action_counts": {},
                "mode_counts": {},
                "city_counts": {},
                "avg_results_per_search": 0,
                "conversion_rate": 0
            }
    
    def export_stats_json(self, days: int = 30) -> str:
        """Экспорт статистики в JSON"""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                data = {
                    "export_date": datetime.utcnow().isoformat(),
                    "period_days": days,
                    "searches": [],
                    "leads": [],
                    "favorites": []
                }
                
                cursor.execute("SELECT * FROM searches WHERE timestamp >= ?", (cutoff_str,))
                data["searches"] = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute("SELECT * FROM leads WHERE timestamp >= ?", (cutoff_str,))
                data["leads"] = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute("SELECT * FROM favorites WHERE timestamp >= ?", (cutoff_str,))
                data["favorites"] = [dict(row) for row in cursor.fetchall()]
                
                return json.dumps(data, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to export stats: {e}")
            return json.dumps({"error": str(e)}, indent=2)

db = DatabaseManager(Config.DB_PATH)

# ------ Sheets manager ------
class SheetsManager:
    def __init__(self):
        if not Config.SHEETS_ENABLED:
            raise RuntimeError("SHEETS_ENABLED must be 1")
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            raise RuntimeError("GOOGLE_CREDENTIALS_JSON is missing")
        creds = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.readonly",
            ],
        )
        self.client = gspread.authorize(creds)
        self.sheet_id = Config.GSHEET_ID
        self.tab_name = Config.GSHEET_TAB or "Ads"

    def get_rows(self) -> List[Dict[str, Any]]:
        ws = self.client.open_by_key(self.sheet_id).worksheet(self.tab_name)
        rows = ws.get_all_records()
        logger.info(f"✅ Loaded {len(rows)} rows from Sheets [{self.tab_name}]")
        return rows

sheets = SheetsManager()

# ------ Cache rows ------
_cached_rows: List[Dict[str, Any]] = []
_cache_ts: float = 0.0

def load_rows(force: bool = False) -> List[Dict[str, Any]]:
    global _cached_rows, _cache_ts
    if not force and _cached_rows and (monotonic() - _cache_ts) < Config.GSHEET_REFRESH_SEC:
        return _cached_rows
    try:
        data = sheets.get_rows()
        _cached_rows = data
        _cache_ts = monotonic()
        logger.info(f"📦 Cache updated: {len(data)} rows")
        return data
    except Exception as e:
        logger.exception(f"❌ Failed to load rows from Sheets: {e}")
        return _cached_rows or []

async def rows_async(force: bool = False) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(load_rows, force)

# ------ Localization ------
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
    "btn_back": {"ru": "⬅️ Назад", "en": "⬅️ Back", "ka": "⬅️ უკან"},
    "btn_skip": {"ru": "Пропустить", "en": "Skip", "ka": "გამოტოვება"},
    "btn_daily": {"ru": "🕓 Посуточно", "en": "🕓 Daily rent", "ka": "🕓 დღიურად"},
    "btn_rent": {"ru": "🏘 Аренда", "en": "🏘 Rent", "ka": "🏘 ქირავდება"},
    "btn_sale": {"ru": "🏠 Продажа", "en": "🏠 Sale", "ka": "🏠 იყიდება"},
    "btn_prev": {"ru": "« Назад", "en": "« Prev", "ka": "« უკან"},
    "btn_next": {"ru": "Вперёд »", "en": "Next »", "ka": "წინ »"},
    "btn_like": {"ru": "❤️ Нравится", "en": "❤️ Like", "ka": "❤️ მომეწონა"},
    "btn_dislike": {"ru": "👎 Дизлайк", "en": "👎 Dislike", "ka": "👎 არ მომწონს"},
    "btn_fav_add": {"ru": "⭐ В избранное", "en": "⭐ Favorite", "ka": "⭐ რჩეულებში"},
    "btn_fav_del": {"ru": "⭐ Удалить", "en": "⭐ Remove", "ka": "⭐ წაშლა"},
    "btn_standard_ranges": {"ru": "📊 Стандартные диапазоны", "en": "📊 Standard ranges", "ka": "📊 სტანდარტული დიაპაზონები"},
    "btn_custom_price": {"ru": "✏️ Свой диапазон", "en": "✏️ Custom range", "ka": "✏️ ჩემი დიაპაზონი"},
    "start": {
        "ru": "<b>LivePlace</b>\n👋 Привет! Я помогу подобрать <b>идеальную недвижимость в Грузии</b>.\n\n<b>Как это работает?</b>\n— 3–4 простых вопроса\n— Покажу лучшие варианты с фото и телефоном владельца\n— Просто посмотреть? Жми <b>🟢 Быстрый подбор</b>\n\nДобро пожаловать! 🏡",
        "en": "<b>LivePlace</b>\n👋 Hi! I'll help you find <b>your ideal home in Georgia</b>.\n\n<b>How it works:</b>\n— 3–4 quick questions\n— Top options with photos & owner phone\n— Just browsing? Tap <b>🟢 Quick picks</b>\n\nWelcome! 🏡",
        "ka": "<b>LivePlace</b>\n👋 გამარჯობა! ერთად ვიპოვოთ <b>იდეალური ბინა საქართველოში</b>.\n\n<b>როგორ მუშაობს:</b>\n— 3–4 მარტივი კითხვა\n— საუკეთესო ვარიანტები ფოტოებითა და მფლობელის ნომრით\n— უბრალოდ გადაათვალიერე? დააჭირე <b>🟢 სწრაფი არჩევანი</b>\n\nკეთილი იყოს თქვენი მობრძანება! 🏡",
    },
    "about": {
        "ru": "LivePlace: быстрый подбор недвижимости в Грузии. Фильтры, 10 фото, телефон владельца, избранное.",
        "en": "LivePlace: fast real-estate search in Georgia. Filters, 10 photos, owner phone, favorites.",
        "ka": "LivePlace: უძრავი ქონების სწრაფი ძიება საქართველოში. ფილტრები, 10 ფოტო, მფლობელის ნომერი, რჩეულები."
    },
}

LANG_FIELDS = {
    "ru": {"title": "title_ru", "desc": "description_ru"},
    "en": {"title": "title_en", "desc": "description_en"},
    "ka": {"title": "title_ka", "desc": "description_ka"},
}

def t(lang: str, key: str, **kw) -> str:
    lang = lang if lang in LANGS else "ru"
    val = T.get(key, {}).get(lang, T.get(key, {}).get("ru", key))
    try:
        return val.format(**kw) if kw else val
    except Exception:
        return val

def current_lang(uid: int) -> str:
    return USER_LANG.get(uid, "ru")

def main_menu(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T["btn_fast"][lang])],
            [KeyboardButton(text=T["btn_search"][lang]), KeyboardButton(text=T["btn_latest"][lang])],
            [KeyboardButton(text=T["btn_favs"][lang])],
            [KeyboardButton(text=T["btn_language"][lang]), KeyboardButton(text=T["btn_about"][lang])]
        ],
        resize_keyboard=True
    )

# ------ Icons & price ranges ------
CITY_ICONS = {
    "тбилиси": "🏙",
    "батуми": "🌊",
    "кутаиси": "⛰",
}
PRICE_RANGES = {
    "sale": ["35000$-", "35000$-50000$", "50000$-75000$", "75000$-100000$", "100000$-150000$", "150000$+"],
    "rent": ["300$-", "300$-500$", "500$-700$", "700$-900$", "900$-1100$", "1100$+"],
    "daily": ["Пропустить"]
}

# ------ Utilities ------
def norm(s: Any) -> str:
    result = str(s or "").strip().lower()
    result = " ".join(result.split())
    return result

def norm_mode(v: Any) -> str:
    s = norm(v)
    s = re.sub(r'[^\w\s-]', '', s)
    s = s.strip()
    
    if s in {"rent","аренда","long","longterm","долгосрочно"}: 
        return "rent"
    if s in {"sale","продажа","buy","sell"}: 
        return "sale"
    if s in {"daily","посуточно","sutki","сутки","short","shortterm","day"}: 
        return "daily"
    return ""

def clean_button_text(text: str) -> str:
    text = re.sub(r"^[\U0001F300-\U0001F9FF\s]+", "", text)
    text = re.sub(r"\s*\(\d+\)\s*$", "", text)
    return text.strip()

def drive_direct(url: str) -> str:
    if not url: return url
    m = re.search(r"/d/([A-Za-z0-9_-]{20,})/", url)
    if m: return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    m = re.search(r"[?&]id=([A-Za-z0-9_-]{20,})", url)
    if m: return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return url

def looks_like_image(url: str) -> bool:
    if not url: return False
    u = url.lower()
    return any(u.endswith(ext) for ext in (".jpg",".jpeg",".png",".webp")) or \
           "googleusercontent.com" in u or "google.com/uc?export=download" in u

def is_valid_photo_url(url: str) -> bool:
    if not url or not url.strip():
        return False
    try:
        parsed = urlparse(url)
        if not parsed.scheme in ['http', 'https']:
            return False
        if not parsed.netloc:
            return False
        return looks_like_image(url)
    except Exception:
        return False

def collect_photos(row: Dict[str, Any]) -> List[str]:
    out = []
    for i in range(1, 11):
        u = str(row.get(f"photo{i}", "") or "").strip()
        if not u: 
            continue
        u = drive_direct(u)
        if is_valid_photo_url(u):
            out.append(u)
        else:
            logger.warning(f"⚠️ Invalid photo URL: {u[:50]}...")
    return out[:10]

def parse_rooms(v: Any) -> float:
    s = str(v or "").strip().lower()
    if s in {"студия","studio","stud","სტუდიო"}: return 0.5
    try:
        return float(s.replace("+",""))
    except Exception:
        return -1.0

def build_utm_url(raw: str, ad_id: str, uid: int) -> str:
    if not raw: return raw or ""
    u = urlparse(raw)
    q = parse_qs(u.query)
    q["utm_source"] = [Config.UTM_SOURCE]
    q["utm_medium"] = [Config.UTM_MEDIUM]
    q["utm_campaign"] = [Config.UTM_CAMPAIGN]
    q["utm_content"] = [ad_id]
    q["token"] = [__import__("hashlib").sha256(f"{uid}:{datetime.utcnow().strftime('%Y%m%d')}:{ad_id}".encode()).hexdigest()[:16]]
    new_q = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def format_card(row: Dict[str, Any], lang: str) -> str:
    title_k = LANG_FIELDS[lang]["title"]
    desc_k  = LANG_FIELDS[lang]["desc"]
    city     = str(row.get("city","")).strip()
    district = str(row.get("district","")).strip()
    rtype    = str(row.get("type","")).strip()
    rooms    = str(row.get("rooms","")).strip()
    price    = str(row.get("price","")).strip()
    published= str(row.get("published","")).strip()
    phone    = str(row.get("phone","")).strip()
    title    = str(row.get(title_k,"")).strip()
    desc     = str(row.get(desc_k,"")).strip()

    pub_txt = published
    try:
        dt = datetime.fromisoformat(published)
        pub_txt = dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    lines = []
    if title: lines.append(f"<b>{title}</b>")
    info_line = " • ".join([x for x in [rtype or "", rooms or "", f"{city}, {district}".strip(", ")] if x])
    if info_line: lines.append(info_line)
    if price: lines.append(f"💰 {price}")
    if pub_txt: lines.append(f"📅 {pub_txt}")
    if desc: lines.append(f"\n{desc}")
    if phone: lines.append(f"\n<b>☎️ Телефон:</b> {phone}")
    if not desc and not phone: lines.append("—")
    return "\n".join(lines)

# ------ FSM ------
class Wizard(StatesGroup):
    mode = State()
    city = State()
    district = State()
    rooms = State()
    price_method = State()
    price_min = State()
    price_max = State()
    price = State()

# ------ User data ------
PAGE_SIZE = 8
USER_RESULTS: Dict[int, Dict[str, Any]] = {}
USER_FAVS: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
USER_CURRENT_INDEX: Dict[int, int] = {}
USER_LEAD_STATE: Dict[int, str] = {}
USER_LEAD_DATA: Dict[int, Dict[str, Any]] = {}
LAST_AD_TIME: Dict[int, float] = {}
LAST_AD_ID: Dict[int, str] = {}

# ------ Ads ------
ADS = [
    {"id":"lead_form","text_ru":"🔥 Ищете квартиру быстрее? Оставьте заявку — подберём за 24 часа!","url":"https://liveplace.com.ge/lead"},
    {"id":"mortgage_help","text_ru":"🏦 Поможем с ипотекой для нерезидентов. Узнайте детали.","url":"https://liveplace.com.ge/mortgage"},
]

def should_show_ad(uid: int) -> bool:
    if not Config.ADS_ENABLED or not ADS: return False
    now = time.time()
    if now - LAST_AD_TIME.get(uid,0.0) < Config.ADS_COOLDOWN_SEC: return False
    return random.random() < Config.ADS_PROB

def pick_ad(uid: int) -> Dict[str, Any]:
    pool = [a for a in ADS if a.get("id") != LAST_AD_ID.get(uid)] or ADS
    return random.choice(pool)

async def maybe_show_ad_by_chat(chat_id: int, uid: int):
    if not should_show_ad(uid): return
    ad = pick_ad(uid)
    url = build_utm_url(ad.get("url",""), ad.get("id","ad"), uid)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👉 Подробнее", url=url)]])
    try:
        await bot.send_message(chat_id, ad.get("text_ru","LivePlace"), reply_markup=kb)
    except Exception:
        pass
    LAST_AD_TIME[uid] = time.time()
    LAST_AD_ID[uid] = ad.get("id")

# ------ 🎉 Анимация лайков с сердечками ------
async def send_like_animation(chat_id: int, message_id: int, uid: int):
    """Отправляет анимированные эффекты с сердечками при лайке"""
    
    # 1. Пробуем добавить эмодзи-реакцию к сообщению
    try:
        await bot.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction=[ReactionTypeEmoji(emoji="❤")]
        )
        logger.info(f"✅ Added heart reaction for user {uid}")
        return  # Если реакция сработала, выходим
    except Exception as e:
        logger.info(f"ℹ️ Reaction not supported, trying sticker: {e}")
    
    # 2. Если реакция не сработала, отправляем анимированный стикер
    if Config.HEART_STICKERS:
        try:
            sticker_id = random.choice(Config.HEART_STICKERS)
            msg = await bot.send_sticker(chat_id, sticker_id)
            logger.info(f"✅ Sent heart sticker for user {uid}")
            
            # Автоматически удаляем стикер через 3 секунды
            await asyncio.sleep(3)
            try:
                await bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
        except Exception as e:
            logger.error(f"❌ Failed to send sticker: {e}")

# ------ Filtering ------
def _filter_rows(rows: List[Dict[str, Any]], q: Dict[str, Any]) -> List[Dict[str, Any]]:
    def ok(r):
        if q.get("mode"):
            row_mode = norm_mode(r.get("mode"))
            query_mode = norm_mode(q["mode"])
            if row_mode != query_mode:
                return False
        
        if q.get("city") and q["city"].strip():
            row_city = norm(r.get("city"))
            query_city = norm(q["city"])
            if row_city != query_city:
                return False
        
        if q.get("district") and q["district"].strip():
            row_district = norm(r.get("district"))
            query_district = norm(q["district"])
            if row_district != query_district:
                return False
        
        if q.get("rooms") and q["rooms"].strip():
            try:
                need = float(q["rooms"].replace("+", ""))
                have = parse_rooms(r.get("rooms"))
                if have < 0:
                    return False
                if "+" in str(q["rooms"]):
                    if have < need:
                        return False
                else:
                    if int(need) != int(have) and not (need == 0.5 and have == 0.5):
                        return False
            except Exception:
                pass
        
        if q.get("price_min") is not None or q.get("price_max") is not None:
            try:
                p = float(re.sub(r"[^\d.]", "", str(r.get("price", "")) or "0") or 0)
                if p == 0:
                    return True
                
                min_val = q.get("price_min")
                max_val = q.get("price_max")
                
                if min_val is not None and p < min_val:
                    return False
                if max_val is not None and p > max_val:
                    return False
            except Exception:
                pass
        
        elif q.get("price") and q["price"].strip() and q["price"].lower() not in {"пропустить", "skip", "გამოტოვება"}:
            try:
                pr = str(q["price"])
                if "-" in pr:
                    parts = pr.split("-", 1)
                    left = parts[0]
                    right = parts[1] if len(parts) > 1 else ""
                    
                    left_val = float(re.sub(r"[^\d]", "", left) or "0")
                    right_val = float(re.sub(r"[^\d]", "", right) or "0") if right else 0
                    
                    p = float(re.sub(r"[^\d.]", "", str(r.get("price", "")) or "0") or 0)
                    
                    if p == 0:
                        return True
                    
                    if right_val == 0:
                        if p < left_val:
                            return False
                    else:
                        if p < left_val or p > right_val:
                            return False
                else:
                    cap = float(re.sub(r"[^\d.]", "", pr) or "0")
                    p = float(re.sub(r"[^\d.]", "", str(r.get("price", "")) or "0") or 0)
                    if p > cap and cap > 0:
                        return False
            except Exception:
                pass
        
        return True
    
    filtered = [r for r in rows if ok(r)]
    logger.info(f"✅ Filtered {len(filtered)}/{len(rows)} rows")
    return filtered

# ------ Safe media sending ------
async def send_media_safe(chat_id: int, photos: List[str], text: str, retry_count: int = Config.MEDIA_RETRY_COUNT) -> bool:
    if not photos:
        return False
    
    for attempt in range(retry_count):
        try:
            media = [InputMediaPhoto(media=photos[0], caption=text)]
            for p in photos[1:]:
                media.append(InputMediaPhoto(media=p))
            
            await bot.send_media_group(chat_id, media)
            return True
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Attempt {attempt + 1}/{retry_count} failed: {error_msg[:100]}")
            
            if any(err in error_msg for err in ["WEBPAGE_CURL_FAILED", "WEBPAGE_MEDIA_EMPTY", "FILE_REFERENCE"]):
                logger.warning(f"🚫 Non-recoverable error, skipping media")
                return False
            
            if attempt < retry_count - 1:
                await asyncio.sleep(Config.MEDIA_RETRY_DELAY * (attempt + 1))
    
    return False

# ------ Show single ad ------
async def show_single_ad(chat_id: int, uid: int):
    bundle = USER_RESULTS.get(uid)
    if not bundle:
        await bot.send_message(chat_id, "Список пуст.", reply_markup=main_menu(current_lang(uid)))
        return
    
    rows = bundle["rows"]
    if not rows:
        await bot.send_message(chat_id, "Нет объявлений.", reply_markup=main_menu(current_lang(uid)))
        return
    
    current_index = USER_CURRENT_INDEX.get(uid, 0)
    
    if current_index >= len(rows):
        await bot.send_message(
            chat_id, 
            "🎉 Вы просмотрели все объявления!\n\nВыберите действие:",
            reply_markup=main_menu(current_lang(uid))
        )
        return
    
    row = rows[current_index]
    photos = collect_photos(row)
    text = format_card(row, current_lang(uid))
    text += f"\n\n📊 Объявление {current_index + 1} из {len(rows)}"
    
    buttons = [
        [
            InlineKeyboardButton(text="❤️ Нравится", callback_data=f"like:{current_index}"),
            InlineKeyboardButton(text="👎 Дизлайк", callback_data=f"dislike:{current_index}")
        ],
        [
            InlineKeyboardButton(text="⭐ В избранное", callback_data=f"fav_add:{current_index}")
        ]
    ]
    
    if any(fav.get("index") == current_index for fav in USER_FAVS.get(uid, [])):
        buttons[1] = [InlineKeyboardButton(text="⭐ Удалить", callback_data=f"fav_del:{current_index}")]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    if photos:
        success = await send_media_safe(chat_id, photos, text)
        if success:
            await bot.send_message(chat_id, "Выберите действие:", reply_markup=kb)
        else:
            await bot.send_message(chat_id, f"{text}\n\n⚠️ Фото недоступны", reply_markup=kb)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb)

# ------ Commands ------
@dp.message(Command("start", "menu"))
async def cmd_start(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid not in USER_LANG:
        code = (message.from_user.language_code or "").strip()
        USER_LANG[uid] = LANG_MAP.get(code, "ru")
    lang = current_lang(uid)
    await state.clear()
    
    db.register_user(uid)
    db.log_action(uid, "start")
    
    await message.answer(t(lang, "start"), reply_markup=main_menu(lang))

@dp.message(Command("about"))
async def cmd_about(message: types.Message):
    lang = current_lang(message.from_user.id)
    await message.answer(t(lang, "about"))

@dp.message(Command("health"))
async def cmd_health(message: types.Message):
    if message.from_user.id != Config.ADMIN_CHAT_ID:
        return
    await message.answer(
        f"✅ Bot OK\n"
        f"Sheets enabled: {Config.SHEETS_ENABLED}\n"
        f"Cached rows: {len(_cached_rows)}\n"
        f"Cache age: {int(monotonic() - _cache_ts)}s\n"
        f"DB: {Config.DB_PATH}"
    )

@dp.message(Command("gs"))
async def cmd_gs(message: types.Message):
    rows = await rows_async(force=True)
    await message.answer(f"📊 Загружено строк: {len(rows)} из Google Sheets.")

@dp.message(Command("refresh","reload"))
async def cmd_refresh(message: types.Message):
    if message.from_user.id != Config.ADMIN_CHAT_ID:
        return
    rows = await rows_async(force=True)
    await message.answer(f"♻️ Перезагружено. В кэше: {len(rows)} строк.")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if message.from_user.id != Config.ADMIN_CHAT_ID:
        return
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📅 За день", callback_data="stats:1"),
                InlineKeyboardButton(text="📅 За неделю", callback_data="stats:7")
            ],
            [
                InlineKeyboardButton(text="📅 За месяц", callback_data="stats:30"),
                InlineKeyboardButton(text="📅 За всё время", callback_data="stats:365")
            ],
            [
                InlineKeyboardButton(text="📥 Экспорт JSON", callback_data="export:30")
            ]
        ]
    )
    await message.answer("📊 <b>Статистика бота</b>\n\nВыберите период:", reply_markup=kb)

@dp.callback_query(F.data.startswith("stats:"))
async def cb_stats(cb: types.CallbackQuery):
    if cb.from_user.id != Config.ADMIN_CHAT_ID:
        await cb.answer("Недостаточно прав")
        return
    
    days = int(cb.data.split(":")[1])
    
    if days == 1:
        period_name = "сегодня"
    elif days == 7:
        period_name = "за неделю"
    elif days == 30:
        period_name = "за месяц"
    else:
        period_name = "за всё время"
    
    data = db.get_stats(days)
    
    msg = f"📊 <b>Статистика {period_name}</b>\n\n"
    msg += f"👥 <b>Пользователи:</b>\n"
    msg += f"  • Уникальных: {data['unique_users']}\n"
    msg += f"  • Новых: {data['new_users']}\n\n"
    
    msg += f"🔍 <b>Активность:</b>\n"
    msg += f"  • Всего действий: {data['total_actions']}\n"
    msg += f"  • Поисков: {data['searches']}\n"
    msg += f"  • Заявок: {data['leads']}\n"
    msg += f"  • В избранное: {data['favorites_added']}\n"
    msg += f"  • Из избранного: {data['favorites_removed']}\n\n"
    
    if data['searches'] > 0:
        msg += f"📈 <b>Показатели:</b>\n"
        msg += f"  • Среднее результатов: {data['avg_results_per_search']}\n"
        msg += f"  • Конверсия в лиды: {data['conversion_rate']}%\n\n"
    
    if data['mode_counts']:
        msg += f"🏠 <b>Режимы поиска:</b>\n"
        for mode, count in sorted(data['mode_counts'].items(), key=lambda x: -x[1])[:5]:
            mode_name = {"rent": "Аренда", "sale": "Продажа", "daily": "Посуточно"}.get(mode, mode)
            msg += f"  • {mode_name}: {count}\n"
        msg += "\n"
    
    if data['city_counts']:
        msg += f"🏙 <b>Топ городов:</b>\n"
        for city, count in sorted(data['city_counts'].items(), key=lambda x: -x[1])[:5]:
            msg += f"  • {city}: {count}\n"
        msg += "\n"
    
    msg += f"💾 <b>Система:</b>\n"
    msg += f"  • Кэш: {len(_cached_rows)} объявлений\n"
    msg += f"  • БД: {Config.DB_PATH}\n"
    
    msg += f"\n⏰ Обновлено: {datetime.utcnow().strftime('%H:%M:%S')}"
    
    try:
        await cb.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔄 Обновить", callback_data=f"stats:{days}")]]
        ))
        await cb.answer("✅ Статистика обновлена")
    except Exception as e:
        if "message is not modified" in str(e):
            await cb.answer("Статистика актуальна", show_alert=False)
        else:
            logger.error(f"Error updating stats: {e}")
            await cb.answer("❌ Ошибка обновления")

@dp.callback_query(F.data.startswith("export:"))
async def cb_export(cb: types.CallbackQuery):
    if cb.from_user.id != Config.ADMIN_CHAT_ID:
        await cb.answer("Недостаточно прав")
        return
    
    days = int(cb.data.split(":")[1])
    await cb.answer("Создаю экспорт...")
    
    try:
        json_data = db.export_stats_json(days)
        
        filename = f"liveplace_stats_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(json_data)
        
        with open(filename, 'rb') as f:
            await bot.send_document(
                cb.message.chat.id,
                types.BufferedInputFile(f.read(), filename=filename),
                caption=f"📥 Экспорт статистики за {days} дней"
            )
        
        os.remove(filename)
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        await cb.message.answer(f"❌ Ошибка экспорта: {e}")

# ------ Back button handler ------
@dp.message(F.text.in_([T["btn_back"]["ru"], T["btn_back"]["en"], T["btn_back"]["ka"]]))
async def handle_back(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    lang = current_lang(message.from_user.id)
    
    if current_state == Wizard.city.state:
        await state.set_state(Wizard.mode)
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=T["btn_rent"][lang])],
                [KeyboardButton(text=T["btn_sale"][lang])],
                [KeyboardButton(text=T["btn_daily"][lang])],
                [KeyboardButton(text=T["btn_back"][lang])]
            ],
            resize_keyboard=True
        )
        await message.answer("⬅️ Выберите режим:", reply_markup=kb)
        
    elif current_state == Wizard.district.state:
        data = await state.get_data()
        mode = data.get("mode", "")
        await state.set_state(Wizard.city)
        
        rows = await rows_async()
        filtered_rows = [r for r in rows if norm_mode(r.get("mode")) == mode]
        city_counter = Counter([str(r.get("city","")).strip() for r in filtered_rows if r.get("city")])
        
        buttons = []
        for city, count in sorted(city_counter.items(), key=lambda x: (-x[1], x[0].lower())):
            icon = CITY_ICONS.get(norm(city), "🏠")
            label = f"{icon} {city} ({count})"
            buttons.append([KeyboardButton(text=label)])
        buttons.append([KeyboardButton(text=T["btn_skip"][lang])])
        buttons.append([KeyboardButton(text=T["btn_back"][lang])])
        
        kb = ReplyKeyboardMarkup(keyboard=buttons[:42], resize_keyboard=True)
        await message.answer("⬅️ Выберите город:", reply_markup=kb)
        
    elif current_state == Wizard.rooms.state:
        data = await state.get_data()
        city = data.get("city", "")
        
        if city:
            await state.set_state(Wizard.district)
            mode = data.get("mode", "")
            rows = await rows_async()
            filtered_rows = [r for r in rows if norm_mode(r.get("mode")) == mode and norm(r.get("city")) == norm(city)]
            district_counter = Counter([str(r.get("district","")).strip() for r in filtered_rows if r.get("district")])
            
            buttons = [[KeyboardButton(text=f"{d} ({c})")] for d,c in sorted(district_counter.items(), key=lambda x:(-x[1], x[0].lower()))]
            buttons.append([KeyboardButton(text=T["btn_skip"][lang])])
            buttons.append([KeyboardButton(text=T["btn_back"][lang])])
            
            kb = ReplyKeyboardMarkup(keyboard=buttons[:42], resize_keyboard=True)
            await message.answer("⬅️ Выберите район:", reply_markup=kb)
        else:
            await state.set_state(Wizard.city)
            await message.answer("⬅️ Выберите город:")
    
    elif current_state == Wizard.price_method.state:
        await state.set_state(Wizard.rooms)
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
                [KeyboardButton(text="4"), KeyboardButton(text="5+")],
                [KeyboardButton(text=T["btn_skip"][lang]), KeyboardButton(text=T["btn_back"][lang])]
            ],
            resize_keyboard=True
        )
        await message.answer("⬅️ Выберите количество комнат:", reply_markup=kb)
    
    elif current_state == Wizard.price.state:
        await state.set_state(Wizard.price_method)
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=T["btn_standard_ranges"][lang])],
                [KeyboardButton(text=T["btn_custom_price"][lang])],
                [KeyboardButton(text=T["btn_back"][lang])]
            ],
            resize_keyboard=True
        )
        await message.answer("⬅️ Как хотите указать цену?", reply_markup=kb)
    
    elif current_state == Wizard.price_min.state:
        await state.set_state(Wizard.price_method)
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=T["btn_standard_ranges"][lang])],
                [KeyboardButton(text=T["btn_custom_price"][lang])],
                [KeyboardButton(text=T["btn_back"][lang])]
            ],
            resize_keyboard=True
        )
        await message.answer("⬅️ Как хотите указать цену?", reply_markup=kb)
    
    elif current_state == Wizard.price_max.state:
        await state.set_state(Wizard.price_min)
        await message.answer("⬅️ Введите минимальную цену:")
    
    else:
        await state.clear()
        await message.answer("⬅️ Главное меню", reply_markup=main_menu(lang))

# ------ Search flow ------
@dp.message(F.text.in_([T["btn_search"]["ru"], T["btn_search"]["en"], T["btn_search"]["ka"]]))
@dp.message(Command("search"))
async def start_search(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(Wizard.mode)
    lang = current_lang(message.from_user.id)
    
    db.log_action(message.from_user.id, "search_start")
    
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T["btn_rent"][lang])],
            [KeyboardButton(text=T["btn_sale"][lang])],
            [KeyboardButton(text=T["btn_daily"][lang])],
            [KeyboardButton(text=T["btn_back"][lang])]
        ],
        resize_keyboard=True
    )
    await message.answer("Выберите режим:", reply_markup=kb)

@dp.message(Wizard.mode)
async def pick_city_mode(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    mode = norm_mode(message.text)
    
    if not mode:
        return await message.answer("Укажите rent/sale/daily")
    
    await state.update_data(mode=mode)

    rows = await rows_async()
    filtered_rows = [r for r in rows if norm_mode(r.get("mode")) == mode]
    
    city_counter = Counter([str(r.get("city","")).strip() for r in filtered_rows if r.get("city")])
    
    buttons = []
    for city, count in sorted(city_counter.items(), key=lambda x: (-x[1], x[0].lower())):
        icon = CITY_ICONS.get(norm(city), "🏠")
        label = f"{icon} {city} ({count})"
        buttons.append([KeyboardButton(text=label)])
    
    if not buttons:
        buttons = [[KeyboardButton(text=T["btn_skip"][lang])]]
    else:
        buttons.append([KeyboardButton(text=T["btn_skip"][lang])])
    
    buttons.append([KeyboardButton(text=T["btn_back"][lang])])
    
    kb = ReplyKeyboardMarkup(keyboard=buttons[:42], resize_keyboard=True)
    await state.set_state(Wizard.city)
    await message.answer("Выберите город:", reply_markup=kb)

@dp.message(Wizard.city)
async def pick_district(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    city_text = message.text.strip()
    
    if city_text.lower() in {t(lang, "btn_skip").lower(), "пропустить", "skip"}:
        await state.update_data(city="")
        await state.update_data(district="")
        await state.set_state(Wizard.rooms)
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
                [KeyboardButton(text="4"), KeyboardButton(text="5+")],
                [KeyboardButton(text=T["btn_skip"][lang]), KeyboardButton(text=T["btn_back"][lang])]
            ],
            resize_keyboard=True
        )
        await message.answer("Выберите количество комнат:", reply_markup=kb)
        return

    city = clean_button_text(city_text)
    await state.update_data(city=city)

    data = await state.get_data()
    mode = data.get("mode", "")
    
    rows = await rows_async()
    filtered_rows = [r for r in rows if norm_mode(r.get("mode")) == mode and norm(r.get("city")) == norm(city)]
    
    district_counter = Counter([str(r.get("district","")).strip() for r in filtered_rows if r.get("district")])
    
    if not district_counter:
        await state.update_data(district="")
        await state.set_state(Wizard.rooms)
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
                [KeyboardButton(text="4"), KeyboardButton(text="5+")],
                [KeyboardButton(text=T["btn_skip"][lang]), KeyboardButton(text=T["btn_back"][lang])]
            ],
            resize_keyboard=True
        )
        await message.answer("Выберите количество комнат:", reply_markup=kb)
        return

    buttons = [[KeyboardButton(text=f"{d} ({c})")] for d,c in sorted(district_counter.items(), key=lambda x:(-x[1], x[0].lower()))]
    buttons.append([KeyboardButton(text=T["btn_skip"][lang])])
    buttons.append([KeyboardButton(text=T["btn_back"][lang])])
    
    kb = ReplyKeyboardMarkup(keyboard=buttons[:42], resize_keyboard=True)
    await state.set_state(Wizard.district)
    await message.answer("Выберите район:", reply_markup=kb)

@dp.message(Wizard.district)
async def pick_rooms_or_price(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    text = message.text.strip()
    
    if text.lower() in {t(lang, "btn_skip").lower(), "пропустить", "skip"}:
        await state.update_data(district="")
    else:
        district = clean_button_text(text)
        await state.update_data(district=district)

    await state.set_state(Wizard.rooms)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5+")],
            [KeyboardButton(text=T["btn_skip"][lang]), KeyboardButton(text=T["btn_back"][lang])]
        ],
        resize_keyboard=True
    )
    await message.answer("Выберите количество комнат:", reply_markup=kb)

@dp.message(Wizard.rooms)
async def pick_price_method(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    text = message.text.strip()
    
    if text.lower() in {t(lang, "btn_skip").lower(), "пропустить", "skip"}:
        await state.update_data(rooms="")
    else:
        val = text.strip().lower()
        if val=="студия":
            val = "0.5"
        await state.update_data(rooms=val)

    await state.set_state(Wizard.price_method)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T["btn_standard_ranges"][lang])],
            [KeyboardButton(text=T["btn_custom_price"][lang])],
            [KeyboardButton(text=T["btn_back"][lang])]
        ],
        resize_keyboard=True
    )
    await message.answer("Как вы хотите указать цену?", reply_markup=kb)

@dp.message(Wizard.price_method)
async def handle_price_method(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    text = message.text.strip()
    
    if text == T["btn_standard_ranges"][lang]:
        data = await state.get_data()
        mode = data.get("mode","sale")
        ranges = PRICE_RANGES.get(mode, PRICE_RANGES["sale"])
        
        buttons = [[KeyboardButton(text=p)] for p in ranges]
        buttons.append([KeyboardButton(text=T["btn_skip"][lang])])
        buttons.append([KeyboardButton(text=T["btn_back"][lang])])
        
        kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
        await state.set_state(Wizard.price)
        await message.answer("Выберите ценовой диапазон:", reply_markup=kb)
    
    elif text == T["btn_custom_price"][lang]:
        await state.set_state(Wizard.price_min)
        await message.answer(
            "💰 <b>Укажите свой ценовой диапазон</b>\n\n"
            "Введите <b>минимальную</b> цену\n"
            "(например: 500 или 500$):"
        )

@dp.message(Wizard.price_min)
async def handle_price_min(message: types.Message, state: FSMContext):
    text = message.text.strip()
    
    try:
        price_str = re.sub(r"[^\d.]", "", text)
        min_price = float(price_str)
        
        if min_price < 0:
            await message.answer("❌ Цена не может быть отрицательной. Попробуйте снова:")
            return
        
        await state.update_data(price_min=min_price)
        await state.set_state(Wizard.price_max)
        
        await message.answer(
            f"✅ Минимальная цена: {min_price}\n\n"
            f"Теперь введите <b>максимальную</b> цену\n"
            f"(или напишите 'без ограничений'):"
        )
    
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число (например: 1000):")

@dp.message(Wizard.price_max)
async def handle_price_max(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    text = message.text.strip().lower()
    
    data = await state.get_data()
    min_price = data.get("price_min", 0)
    
    if text in ['без ограничений', 'без ограничения', 'неограниченно', 'no limit', 'unlimited']:
        max_price = None
        price_range = f"от {min_price}"
    else:
        try:
            price_str = re.sub(r"[^\d.]", "", text)
            max_price = float(price_str)
            
            if max_price < 0:
                await message.answer("❌ Цена не может быть отрицательной. Попробуйте снова:")
                return
            
            if max_price <= min_price:
                await message.answer(
                    f"❌ Максимальная цена должна быть больше минимальной ({min_price}).\n"
                    f"Попробуйте снова:"
                )
                return
            
            price_range = f"{min_price} - {max_price}"
            
        except ValueError:
            await message.answer("❌ Пожалуйста, введите число или 'без ограничений':")
            return
    
    await state.update_data(price_max=max_price)
    
    query = {
        "mode": data.get("mode", ""),
        "city": data.get("city", "").strip(),
        "district": data.get("district", "").strip(),
        "rooms": data.get("rooms", "").strip(),
        "price_min": min_price,
        "price_max": max_price
    }
    
    all_rows = await rows_async()
    rows = _filter_rows(all_rows, query)
    
    db.log_search(message.from_user.id, query, len(rows))
    
    USER_RESULTS[message.from_user.id] = {"query": query, "rows": rows, "page": 0}
    USER_CURRENT_INDEX[message.from_user.id] = 0
    
    if not rows:
        msg = f"❌ Ничего не найдено в диапазоне {price_range}\n\nПопробуйте изменить параметры."
        await message.answer(msg, reply_markup=main_menu(lang))
        await state.clear()
        return

    await message.answer(f"✅ Найдено объявлений: {len(rows)} в диапазоне {price_range}")
    await show_single_ad(message.chat.id, message.from_user.id)
    await state.clear()

@dp.message(Wizard.price)
async def show_results_handler(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    text = message.text.strip()
    
    if text.lower() in {t(lang, "btn_skip").lower(), "пропустить", "skip"}:
        price = ""
    else:
        price = text

    data = await state.get_data()
    await state.update_data(price=price)
    
    query = {
        "mode": data.get("mode", ""),
        "city": data.get("city", "").strip(),
        "district": data.get("district", "").strip(),
        "rooms": data.get("rooms", "").strip(),
        "price": price.strip()
    }

    all_rows = await rows_async()
    rows = _filter_rows(all_rows, query)
    
    db.log_search(message.from_user.id, query, len(rows))
    
    USER_RESULTS[message.from_user.id] = {"query": query, "rows": rows, "page": 0}
    USER_CURRENT_INDEX[message.from_user.id] = 0
    
    if not rows:
        msg = "❌ Ничего не найдено по вашим параметрам.\n\nПопробуйте изменить параметры поиска."
        await message.answer(msg, reply_markup=main_menu(lang))
        await state.clear()
        return

    await message.answer(f"✅ Найдено объявлений: {len(rows)}")
    await show_single_ad(message.chat.id, message.from_user.id)
    await state.clear()

# ------ Callbacks ------
@dp.callback_query(F.data.startswith("like:"))
async def cb_like(cb: types.CallbackQuery):
    uid = cb.from_user.id
    index = int(cb.data.split(":")[1])
    
    bundle = USER_RESULTS.get(uid)
    if not bundle or index >= len(bundle["rows"]):
        await cb.answer("Ошибка")
        return
    
    row = bundle["rows"][index]
    
    USER_LEAD_DATA[uid] = {
        "ad_index": index,
        "ad_data": row,
        "timestamp": datetime.utcnow().isoformat()
    }
    USER_LEAD_STATE[uid] = "awaiting_name"
    
    db.log_action(uid, "like", {"ad_id": row.get("id", "unknown")})
    
    # 🎉 АНИМИРОВАННЫЕ ЭФФЕКТЫ С СЕРДЕЧКАМИ
    await cb.answer("💕 Отлично! Это объявление вам понравилось!", show_alert=False)
    
    # Запускаем анимацию параллельно
    asyncio.create_task(send_like_animation(
        chat_id=cb.message.chat.id,
        message_id=cb.message.message_id,
        uid=uid
    ))
    
    # Небольшая задержка для визуального эффекта
    await asyncio.sleep(0.5)
    
    await cb.message.answer(
        "📝 <b>Оставьте заявку</b>\n\n"
        "Мы свяжемся с вами в ближайшее время!\n\n"
        "Пожалуйста, напишите ваше <b>имя</b>:"
    )

@dp.callback_query(F.data.startswith("dislike:"))
async def cb_dislike(cb: types.CallbackQuery):
    uid = cb.from_user.id
    index = int(cb.data.split(":")[1])
    
    USER_CURRENT_INDEX[uid] = index + 1
    
    db.log_action(uid, "dislike")
    
    await cb.answer("Понятно 👎")
    await show_single_ad(cb.message.chat.id, uid)

@dp.callback_query(F.data.startswith("fav_add:"))
async def cb_fav_add(cb: types.CallbackQuery):
    uid = cb.from_user.id
    index = int(cb.data.split(":")[1])
    
    bundle = USER_RESULTS.get(uid)
    if not bundle or index >= len(bundle["rows"]):
        await cb.answer("Ошибка")
        return
    
    row = bundle["rows"][index]
    
    if not any(fav.get("index") == index for fav in USER_FAVS[uid]):
        USER_FAVS[uid].append({"index": index, "data": row})
        
        db.log_favorite(uid, "add", row)
        db.log_action(uid, "favorite_add")
        
        await cb.answer("⭐ Добавлено!")
        
        buttons = [
            [
                InlineKeyboardButton(text="❤️ Нравится", callback_data=f"like:{index}"),
                InlineKeyboardButton(text="👎 Дизлайк", callback_data=f"dislike:{index}")
            ],
            [
                InlineKeyboardButton(text="⭐ Удалить", callback_data=f"fav_del:{index}")
            ]
        ]
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        try:
            await cb.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    else:
        await cb.answer("Уже в избранном!")

@dp.callback_query(F.data.startswith("fav_del:"))
async def cb_fav_del(cb: types.CallbackQuery):
    uid = cb.from_user.id
    index = int(cb.data.split(":")[1])
    
    row = None
    for fav in USER_FAVS[uid]:
        if fav.get("index") == index:
            row = fav.get("data")
            break
    
    USER_FAVS[uid] = [fav for fav in USER_FAVS[uid] if fav.get("index") != index]
    
    if row:
        db.log_favorite(uid, "remove", row)
        db.log_action(uid, "favorite_remove")
    
    await cb.answer("Удалено")
    
    buttons = [
        [
            InlineKeyboardButton(text="❤️ Нравится", callback_data=f"like:{index}"),
            InlineKeyboardButton(text="👎 Дизлайк", callback_data=f"dislike:{index}")
        ],
        [
            InlineKeyboardButton(text="⭐ В избранное", callback_data=f"fav_add:{index}")
        ]
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    try:
        await cb.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass

# ------ Lead form ------
async def handle_lead_form(message: types.Message):
    uid = message.from_user.id
    
    if uid not in USER_LEAD_STATE:
        return
    
    state = USER_LEAD_STATE[uid]
    
    if state == "awaiting_name":
        USER_LEAD_DATA[uid]["name"] = message.text.strip()
        USER_LEAD_STATE[uid] = "awaiting_phone"
        
        await message.answer(
            "Отлично! Теперь укажите ваш <b>номер телефона</b>:\n"
            "(например: +995 555 123 456)"
        )
        
    elif state == "awaiting_phone":
        USER_LEAD_DATA[uid]["phone"] = message.text.strip()
        
        await send_lead_to_channel(uid)
        
        del USER_LEAD_STATE[uid]
        lead_data = USER_LEAD_DATA.pop(uid)
        
        await message.answer(
            "✅ <b>Спасибо!</b> Ваша заявка принята.\n\n"
            "Мы свяжемся с вами в ближайшее время! 📞",
            reply_markup=main_menu(current_lang(uid))
        )
        
        current_index = lead_data.get("ad_index", 0)
        USER_CURRENT_INDEX[uid] = current_index + 1
        
        await asyncio.sleep(1)
        await show_single_ad(message.chat.id, uid)

async def send_lead_to_channel(uid: int):
    if uid not in USER_LEAD_DATA:
        return
    
    lead = USER_LEAD_DATA[uid]
    ad = lead.get("ad_data", {})
    
    db.log_lead(uid, lead.get('name', ''), lead.get('phone', ''), ad)
    db.log_action(uid, "lead_submitted")
    
    text = (
        "🔥 <b>НОВАЯ ЗАЯВКА</b>\n\n"
        f"👤 <b>Имя:</b> {lead.get('name', 'Не указано')}\n"
        f"📱 <b>Телефон:</b> {lead.get('phone', 'Не указано')}\n"
        f"🆔 <b>User ID:</b> {uid}\n\n"
        f"<b>Интересующее объявление:</b>\n"
        f"🏠 {ad.get('title_ru', 'Без названия')}\n"
        f"📍 {ad.get('city', '')} {ad.get('district', '')}\n"
        f"💰 {ad.get('price', 'Не указана')}\n"
        f"🛏 {ad.get('rooms', '')} комнат\n"
        f"☎️ Телефон владельца: {ad.get('phone', 'Не указан')}\n\n"
        f"⏰ {lead.get('timestamp', '')}"
    )
    
    for attempt in range(3):
        try:
            await bot.send_message(Config.FEEDBACK_CHAT_ID, text)
            logger.info(f"✅ Lead sent to channel for user {uid}")
            return
        except Exception as e:
            logger.error(f"❌ Attempt {attempt + 1}/3 failed to send lead: {e}")
            if attempt < 2:
                await asyncio.sleep(2)

# ------ Other handlers ------
@dp.message(F.text.in_([T["btn_language"]["ru"], T["btn_language"]["en"], T["btn_language"]["ka"]]))
async def choose_language(message: types.Message, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=l.upper(), callback_data=f"lang:{l}")] for l in LANGS]
    )
    await message.answer("Выберите язык / Choose language / ენა", reply_markup=kb)

@dp.callback_query(F.data.startswith("lang:"))
async def cb_set_lang(cb: types.CallbackQuery):
    uid = cb.from_user.id
    lang = cb.data.split(":")[1]
    USER_LANG[uid] = lang
    await cb.answer(f"Язык установлен: {lang.upper()}")
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.message.answer("Меню:", reply_markup=main_menu(lang))

@dp.message(F.text.in_([T["btn_fast"]["ru"], T["btn_fast"]["en"], T["btn_fast"]["ka"]]))
async def quick_pick_entry(msg: types.Message, state: FSMContext):
    await state.clear()
    rows = await rows_async()
    if not rows:
        await msg.answer("Нет доступных объявлений.")
        return
    
    db.log_action(msg.from_user.id, "quick_pick")
    
    sorted_rows = sorted(rows, key=lambda x: str(x.get("published", "")), reverse=True)[:20]
    USER_RESULTS[msg.from_user.id] = {"query": {}, "rows": sorted_rows, "page": 0}
    USER_CURRENT_INDEX[msg.from_user.id] = 0
    
    await msg.answer("🟢 <b>Быстрый подбор</b>\n\nПоказываю лучшие новые объявления:")
    await show_single_ad(msg.chat.id, msg.from_user.id)

@dp.message(F.text.in_([T["btn_favs"]["ru"], T["btn_favs"]["en"], T["btn_favs"]["ka"]]))
async def show_favorites(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    favs = USER_FAVS.get(uid, [])
    
    db.log_action(uid, "view_favorites")
    
    if not favs:
        await message.answer("У вас пока нет избранных объявлений.")
    else:
        USER_RESULTS[uid] = {"query": {}, "rows": [f["data"] for f in favs], "page": 0}
        USER_CURRENT_INDEX[uid] = 0
        await message.answer(f"У вас {len(favs)} избранных объявлений:")
        await show_single_ad(message.chat.id, uid)

@dp.message(F.text.in_([T["btn_latest"]["ru"], T["btn_latest"]["en"], T["btn_latest"]["ka"]]))
async def show_latest(message: types.Message, state: FSMContext):
    await state.clear()
    rows = await rows_async()
    if not rows:
        await message.answer("Нет доступных объявлений.")
        return
    
    db.log_action(message.from_user.id, "view_latest")
    
    sorted_rows = sorted(rows, key=lambda x: str(x.get("published", "")), reverse=True)[:20]
    USER_RESULTS[message.from_user.id] = {"query": {}, "rows": sorted_rows, "page": 0}
    USER_CURRENT_INDEX[message.from_user.id] = 0
    await show_single_ad(message.chat.id, message.from_user.id)

@dp.message(F.text.in_([T["btn_about"]["ru"], T["btn_about"]["en"], T["btn_about"]["ka"]]))
async def show_about(message: types.Message, state: FSMContext):
    await state.clear()
    lang = current_lang(message.from_user.id)
    await message.answer(t(lang, "about"))

@dp.message(F.text.in_([T["btn_home"]["ru"], T["btn_home"]["en"], T["btn_home"]["ka"]]))
async def show_menu(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    await state.clear()
    await message.answer(T["menu_title"][lang], reply_markup=main_menu(lang))

# ------ Fallback ------
@dp.message()
async def fallback_all(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    
    if uid in USER_LEAD_STATE:
        await handle_lead_form(message)
        return
    
    text = (message.text or "").strip()
    if not text:
        await message.answer("Я получил сообщение, но оно пустое.")
        return
    
    await message.answer(
        "Если хотите начать поиск — нажмите '🔎 Поиск' или '🟢 Быстрый подбор' в меню.", 
        reply_markup=main_menu(current_lang(uid))
    )

# ------ Background tasks ------
async def auto_refresh_cache():
    while True:
        try:
            await asyncio.sleep(Config.GSHEET_REFRESH_SEC)
            logger.info("🔄 Auto-refresh: loading data from Google Sheets...")
            rows = await rows_async(force=True)
            logger.info(f"✅ Auto-refresh complete: {len(rows)} rows in cache")
        except Exception as e:
            logger.exception(f"❌ Auto-refresh error: {e}")
            await asyncio.sleep(60)

async def heartbeat():
    while True:
        try:
            logger.info(f"💓 Heartbeat OK | Cache: {len(_cached_rows)} rows | Age: {int(monotonic() - _cache_ts)}s")
        except Exception:
            logger.exception("❌ Heartbeat error")
        await asyncio.sleep(600)

# ------ Startup / Shutdown ------
async def startup():
    logger.info("🚀 LivePlace bot starting...")
    
    try:
        await rows_async(force=True)
    except Exception as e:
        logger.error(f"❌ Failed to load initial data: {e}")
        logger.warning("⚠️ Bot will continue with empty cache")
    
    if Config.ADMIN_CHAT_ID:
        try:
            await bot.send_message(
                Config.ADMIN_CHAT_ID, 
                f"✅ <b>LivePlace bot started</b>\n\n"
                f"📊 Loaded: {len(_cached_rows)} ads\n"
                f"💖 Animated likes: ENABLED\n"
                f"🔄 Auto-refresh: every {Config.GSHEET_REFRESH_SEC}s\n"
                f"📢 Feedback channel: {Config.FEEDBACK_CHAT_ID}\n"
                f"💾 Database: {Config.DB_PATH}"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin on startup: {e}")
    
    asyncio.create_task(heartbeat())
    asyncio.create_task(auto_refresh_cache())
    
    logger.info("✅ Bot startup complete")

async def shutdown():
    try:
        logger.info("🛑 Bot shutting down...")
        
        if Config.ADMIN_CHAT_ID:
            try:
                await bot.send_message(
                    Config.ADMIN_CHAT_ID,
                    "⚠️ <b>LivePlace bot stopped</b>\n\nБот был остановлен"
                )
            except Exception:
                pass
        
        await bot.session.close()
        logger.info("✅ Bot shutdown complete")
    except Exception as e:
        logger.exception(f"Error during shutdown: {e}")

# ------ Main ------
async def main():
    try:
        await startup()
        logger.info("🎯 Starting polling...")
        await dp.start_polling(bot, skip_updates=True)
    except KeyboardInterrupt:
        logger.info("⌨️ Received keyboard interrupt")
    except Exception as e:
        logger.critical(f"💥 Fatal error in main: {e}", exc_info=True)
    finally:
        await shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.critical(f"💥 Fatal startup error: {e}", exc_info=True)
