# -*- coding: utf-8 -*-
"""
LivePlace Telegram Bot — исправленная версия (aiogram 3.x)
Исправления:
 - Корректная нормализация режима в фильтрах
 - Правильный подсчёт кнопок с учётом режима
 - Кнопка "Назад" во всех состояниях FSM
 - Автообновление Google Sheets каждые 2 минуты
 - Безопасная отправка медиа с retry и валидацией
 - Улучшенное логирование ошибок
"""

import os
import re
import json
import time
import random
import asyncio
import logging
from time import monotonic
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import Counter, defaultdict
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
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
    GSHEET_REFRESH_SEC = int(os.getenv("GSHEET_REFRESH_SEC", "120") or "120")  # 2 минуты
    ADS_ENABLED = os.getenv("ADS_ENABLED", "1").strip() not in {"0", "false", "False", ""}
    ADS_PROB = float(os.getenv("ADS_PROB", "0.18") or 0.18)
    ADS_COOLDOWN_SEC = int(os.getenv("ADS_COOLDOWN_SEC", "180") or 180)
    UTM_SOURCE = os.getenv("UTM_SOURCE", "telegram")
    UTM_MEDIUM = os.getenv("UTM_MEDIUM", "bot")
    UTM_CAMPAIGN = os.getenv("UTM_CAMPAIGN", "bot_ads")
    MEDIA_RETRY_COUNT = 3
    MEDIA_RETRY_DELAY = 2

if not Config.API_TOKEN:
    raise RuntimeError("API_TOKEN is not set")

# ------ Bot & Dispatcher ------
bot = Bot(token=Config.API_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())

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
    """Нормализация строки для сравнения"""
    result = str(s or "").strip().lower()
    result = " ".join(result.split())
    return result

def norm_mode(v: Any) -> str:
    """ИСПРАВЛЕНО: Нормализация режима"""
    s = norm(v)
    # Убираем эмодзи и лишние символы
    s = re.sub(r'[^\w\s-]', '', s)
    s = s.strip()
    
    if s in {"rent","аренда","long","longterm","долгосрочно","аренда","rent","ქირავდება"}: 
        return "rent"
    if s in {"sale","продажа","buy","sell","продажа","sale","იყიდება"}: 
        return "sale"
    if s in {"daily","посуточно","sutki","сутки","short","shortterm","day","посуточно","daily rent","დღიურად"}: 
        return "daily"
    return ""

def clean_button_text(text: str) -> str:
    """НОВОЕ: Очистка текста кнопки от эмодзи и счётчиков"""
    # Удаляем эмодзи в начале
    text = re.sub(r"^[\U0001F300-\U0001F9FF\s]+", "", text)
    # Удаляем счётчик в конце (15)
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
    """НОВОЕ: Валидация URL фото"""
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
    """ИСПРАВЛЕНО: Сбор и валидация фото"""
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

# ------ Filtering ------
def _filter_rows(rows: List[Dict[str, Any]], q: Dict[str, Any]) -> List[Dict[str, Any]]:
    """ИСПРАВЛЕНО: Корректная фильтрация с нормализацией режима"""
    def ok(r):
        # Нормализуем режим запроса
        if q.get("mode"):
            row_mode = norm_mode(r.get("mode"))
            query_mode = norm_mode(q["mode"])  # ИСПРАВЛЕНО: добавлена нормализация
            logger.debug(f"Mode check: row={row_mode}, query={query_mode}")
            if row_mode != query_mode:
                return False
        
        if q.get("city") and q["city"].strip():
            row_city = norm(r.get("city"))
            query_city = norm(q["city"])
            logger.debug(f"City check: row='{row_city}', query='{query_city}'")
            if row_city != query_city:
                return False
        
        if q.get("district") and q["district"].strip():
            row_district = norm(r.get("district"))
            query_district = norm(q["district"])
            logger.debug(f"District check: row='{row_district}', query='{query_district}'")
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
            except Exception as e:
                logger.debug(f"Rooms parse error: {e}")
                pass
        
        if q.get("price") and q["price"].strip() and q["price"].lower() not in {"пропустить", "skip", "გამოტოვება"}:
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
            except Exception as e:
                logger.error(f"Price filter error: {e}")
                pass
        
        return True
    
    filtered = [r for r in rows if ok(r)]
    
    if len(filtered) == 0 and len(rows) > 0:
        logger.info(f"⚠️ No results for query: {q}")
        logger.info("Sample rows for debugging:")
        for i, r in enumerate(rows[:3]):
            logger.info(f"Row {i}: mode={r.get('mode')}, city={r.get('city')}, district={r.get('district')}")
    
    logger.info(f"✅ Filtered {len(filtered)}/{len(rows)} rows with query: {q}")
    return filtered

def _slice(listing: List[Any], page: int, size: int) -> List[Any]:
    return listing[page*size:(page+1)*size]

# ------ Safe media sending ------
async def send_media_safe(chat_id: int, photos: List[str], text: str, retry_count: int = Config.MEDIA_RETRY_COUNT) -> bool:
    """НОВОЕ: Безопасная отправка медиа с retry"""
    if not photos:
        return False
    
    for attempt in range(retry_count):
        try:
            media = [InputMediaPhoto(media=photos[0], caption=text)]
            for p in photos[1:]:
                media.append(InputMediaPhoto(media=p))
            
            await bot.send_media_group(chat_id, media)
            logger.info(f"✅ Media sent successfully: {len(photos)} photos")
            return True
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Media send attempt {attempt + 1}/{retry_count} failed: {error_msg}")
            
            # Специальная обработка известных ошибок
            if "WEBPAGE_CURL_FAILED" in error_msg:
                logger.error(f"🚫 WEBPAGE_CURL_FAILED for photos: {photos}")
                # Отправляем админу уведомление
                if Config.ADMIN_CHAT_ID:
                    try:
                        await bot.send_message(
                            Config.ADMIN_CHAT_ID,
                            f"⚠️ WEBPAGE_CURL_FAILED\nPhotos:\n{chr(10).join(photos[:3])}"
                        )
                    except Exception:
                        pass
                return False
                
            elif "WEBPAGE_MEDIA_EMPTY" in error_msg:
                logger.error(f"🚫 WEBPAGE_MEDIA_EMPTY for photos: {photos}")
                return False
            
            # Retry с задержкой
            if attempt < retry_count - 1:
                await asyncio.sleep(Config.MEDIA_RETRY_DELAY)
            else:
                logger.error(f"💥 All {retry_count} attempts failed")
                return False
    
    return False

# ------ Commands & Handlers ------
@dp.message(Command("start", "menu"))
async def cmd_start(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid not in USER_LANG:
        code = (message.from_user.language_code or "").strip()
        USER_LANG[uid] = LANG_MAP.get(code, "ru")
    lang = current_lang(uid)
    await state.clear()
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
        f"Refresh interval: {Config.GSHEET_REFRESH_SEC}s"
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

# ------ НОВОЕ: Обработчик кнопки "Назад" ------
@dp.message(F.text.in_([T["btn_back"]["ru"], T["btn_back"]["en"], T["btn_back"]["ka"]]))
async def handle_back(message: types.Message, state: FSMContext):
    """НОВОЕ: Обработка кнопки Назад в FSM"""
    current_state = await state.get_state()
    lang = current_lang(message.from_user.id)
    
    if current_state == Wizard.city.state:
        # Из выбора города -> в выбор режима
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
        # Из выбора района -> в выбор города
        data = await state.get_data()
        mode = data.get("mode", "")
        await state.set_state(Wizard.city)
        
        rows = await rows_async()
        # ИСПРАВЛЕНО: Фильтруем по режиму
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
        # Из выбора комнат -> в выбор района (или города)
        data = await state.get_data()
        city = data.get("city", "")
        
        if city:
            await state.set_state(Wizard.district)
            mode = data.get("mode", "")
            rows = await rows_async()
            # ИСПРАВЛЕНО: Фильтруем по режиму и городу
            filtered_rows = [r for r in rows if norm_mode(r.get("mode")) == mode and norm(r.get("city")) == norm(city)]
            district_counter = Counter([str(r.get("district","")).strip() for r in filtered_rows if r.get("district")])
            
            buttons = [[KeyboardButton(text=f"{d} ({c})")] for d,c in sorted(district_counter.items(), key=lambda x:(-x[1], x[0].lower()))]
            buttons.append([KeyboardButton(text=T["btn_skip"][lang])])
            buttons.append([KeyboardButton(text=T["btn_back"][lang])])
            
            kb = ReplyKeyboardMarkup(keyboard=buttons[:42], resize_keyboard=True)
            await message.answer("⬅️ Выберите район:", reply_markup=kb)
        else:
            # Если города не было, возвращаемся к городу
            await state.set_state(Wizard.city)
            await message.answer("⬅️ Выберите город:")
            
    elif current_state == Wizard.price.state:
        # Из выбора цены -> в выбор комнат
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
        
    else:
        # Если состояния нет или это начало - идём в главное меню
        await state.clear()
        await message.answer("⬅️ Главное меню", reply_markup=main_menu(lang))

# ------ Start search flow (Wizard) ------
@dp.message(F.text.in_([T["btn_search"]["ru"], T["btn_search"]["en"], T["btn_search"]["ka"]]))
@dp.message(Command("search"))
async def start_search(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(Wizard.mode)
    lang = current_lang(message.from_user.id)
    
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
    logger.info(f"User {message.from_user.id} selected mode: {mode}")

    rows = await rows_async()
    # ИСПРАВЛЕНО: Фильтруем строки по режиму перед подсчётом
    filtered_rows = [r for r in rows if norm_mode(r.get("mode")) == mode]
    logger.info(f"Filtered {len(filtered_rows)}/{len(rows)} rows for mode={mode}")
    
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

    # ИСПРАВЛЕНО: Используем новую функцию очистки
    city = clean_button_text(city_text)
    await state.update_data(city=city)
    logger.info(f"User selected city: '{city}' (from button: '{city_text}')")

    data = await state.get_data()
    mode = data.get("mode", "")
    
    rows = await rows_async()
    # ИСПРАВЛЕНО: Фильтруем по режиму И городу
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
        # ИСПРАВЛЕНО: Очистка от счётчика
        district = clean_button_text(text)
        await state.update_data(district=district)
        logger.info(f"User selected district: '{district}' (from button: '{text}')")

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
async def pick_price_prompt(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    text = message.text.strip()
    
    if text.lower() in {t(lang, "btn_skip").lower(), "пропустить", "skip", "весь город", "весь район"}:
        await state.update_data(rooms="")
    else:
        val = text.strip().lower()
        if val=="студия":
            val = "0.5"
        await state.update_data(rooms=val)

    data = await state.get_data()
    mode = data.get("mode","sale")
    ranges = PRICE_RANGES.get(mode, PRICE_RANGES["sale"])
    
    buttons = [[KeyboardButton(text=p)] for p in ranges]
    buttons.append([KeyboardButton(text=T["btn_skip"][lang])])
    buttons.append([KeyboardButton(text=T["btn_back"][lang])])
    
    kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await state.set_state(Wizard.price)
    await message.answer("Выберите ценовой диапазон:", reply_markup=kb)

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
    
    logger.info(f"🔍 User {message.from_user.id} search query: {query}")

    all_rows = await rows_async()
    logger.info(f"📦 Total rows loaded: {len(all_rows)}")
    
    rows = _filter_rows(all_rows, query)
    logger.info(f"✅ Filtered results: {len(rows)}")
    
    USER_RESULTS[message.from_user.id] = {"query": query, "rows": rows, "page": 0}
    USER_CURRENT_INDEX[message.from_user.id] = 0
    
    if not rows:
        msg = "❌ Ничего не найдено по вашим параметрам.\n\n"
        msg += f"Режим: {query['mode']}\n"
        if query['city']:
            msg += f"Город: {query['city']}\n"
        if query['district']:
            msg += f"Район: {query['district']}\n"
        if query['rooms']:
            msg += f"Комнат: {query['rooms']}\n"
        if query['price']:
            msg += f"Цена: {query['price']}\n"
        msg += "\nПопробуйте изменить параметры поиска."
        
        await message.answer(msg, reply_markup=main_menu(lang))
        await state.clear()
        return

    await message.answer(f"✅ Найдено объявлений: {len(rows)}")
    await show_single_ad(message.chat.id, message.from_user.id)
    await state.clear()

# ------ Show single ad with interaction buttons ------
async def show_single_ad(chat_id: int, uid: int):
    """ИСПРАВЛЕНО: Безопасная отправка медиа"""
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
    
    # ИСПРАВЛЕНО: Безопасная отправка медиа
    if photos:
        success = await send_media_safe(chat_id, photos, text)
        if success:
            await bot.send_message(chat_id, "Выберите действие:", reply_markup=kb)
        else:
            # Если медиа не отправилось - отправляем текст
            await bot.send_message(chat_id, f"{text}\n\n⚠️ Фото недоступны", reply_markup=kb)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb)

# ------ Callback handlers ------
@dp.callback_query(F.data.startswith("like:"))
async def cb_like(cb: types.CallbackQuery):
    uid = cb.from_user.id
    index = int(cb.data.split(":")[1])
    
    bundle = USER_RESULTS.get(uid)
    if not bundle or index >= len(bundle["rows"]):
        await cb.answer("Ошибка: объявление не найдено")
        return
    
    row = bundle["rows"][index]
    
    USER_LEAD_DATA[uid] = {
        "ad_index": index,
        "ad_data": row,
        "timestamp": datetime.utcnow().isoformat()
    }
    USER_LEAD_STATE[uid] = "awaiting_name"
    
    await cb.answer("Отлично! 👍")
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
    
    USER_FAVS[uid] = [fav for fav in USER_FAVS[uid] if fav.get("index") != index]
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

# ------ Lead form handlers ------
@dp.message(F.text)
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
        
        # ИСПРАВЛЕНО: Отправляем лид в канал, пользователю короткое сообщение
        await send_lead_to_channel(uid)
        
        del USER_LEAD_STATE[uid]
        lead_data = USER_LEAD_DATA.pop(uid)
        
        # ИСПРАВЛЕНО: Пользователь видит только короткое подтверждение
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
    """ИСПРАВЛЕНО: Отправка лида ТОЛЬКО в канал, не пользователю"""
    if uid not in USER_LEAD_DATA:
        return
    
    lead = USER_LEAD_DATA[uid]
    ad = lead.get("ad_data", {})
    
    # Формируем подробное сообщение для канала/админа
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
    
    # Отправляем в канал с retry
    for attempt in range(3):
        try:
            await bot.send_message(Config.FEEDBACK_CHAT_ID, text)
            logger.info(f"✅ Lead sent to channel for user {uid}")
            return
        except Exception as e:
            logger.error(f"❌ Attempt {attempt + 1}/3 failed to send lead: {e}")
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                # Если канал недоступен - отправляем админу
                if Config.ADMIN_CHAT_ID and Config.ADMIN_CHAT_ID != Config.FEEDBACK_CHAT_ID:
                    try:
                        await bot.send_message(
                            Config.ADMIN_CHAT_ID, 
                            f"⚠️ Ошибка отправки лида в канал:\n\n{text}"
                        )
                    except Exception:
                        logger.error("💥 Failed to send to admin as fallback")

# ------ Generic handlers for language and menu ------
@dp.message(F.text.in_([T["btn_language"]["ru"], T["btn_language"]["en"], T["btn_language"]["ka"]]))
async def choose_language(message: types.Message):
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
    rows = await rows_async()
    if not rows:
        await msg.answer("Нет доступных объявлений.")
        return
    
    sorted_rows = sorted(rows, key=lambda x: str(x.get("published", "")), reverse=True)[:20]
    USER_RESULTS[msg.from_user.id] = {"query": {}, "rows": sorted_rows, "page": 0}
    USER_CURRENT_INDEX[msg.from_user.id] = 0
    
    await msg.answer("🟢 <b>Быстрый подбор</b>\n\nПоказываю лучшие новые объявления:")
    await show_single_ad(msg.chat.id, msg.from_user.id)

@dp.message(F.text.in_([T["btn_favs"]["ru"], T["btn_favs"]["en"], T["btn_favs"]["ka"]]))
async def show_favorites(message: types.Message):
    uid = message.from_user.id
    favs = USER_FAVS.get(uid, [])
    if not favs:
        await message.answer("У вас пока нет избранных объявлений.")
    else:
        USER_RESULTS[uid] = {"query": {}, "rows": [f["data"] for f in favs], "page": 0}
        USER_CURRENT_INDEX[uid] = 0
        await message.answer(f"У вас {len(favs)} избранных объявлений:")
        await show_single_ad(message.chat.id, uid)

@dp.message(F.text.in_([T["btn_latest"]["ru"], T["btn_latest"]["en"], T["btn_latest"]["ka"]]))
async def show_latest(message: types.Message):
    rows = await rows_async()
    if not rows:
        await message.answer("Нет доступных объявлений.")
        return
    
    sorted_rows = sorted(rows, key=lambda x: str(x.get("published", "")), reverse=True)[:20]
    USER_RESULTS[message.from_user.id] = {"query": {}, "rows": sorted_rows, "page": 0}
    USER_CURRENT_INDEX[message.from_user.id] = 0
    await show_single_ad(message.chat.id, message.from_user.id)

@dp.message(F.text.in_([T["btn_about"]["ru"], T["btn_about"]["en"], T["btn_about"]["ka"]]))
async def show_about(message: types.Message):
    lang = current_lang(message.from_user.id)
    await message.answer(t(lang, "about"))

@dp.message(F.text.in_([T["btn_home"]["ru"], T["btn_home"]["en"], T["btn_home"]["ka"]]))
async def show_menu(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    await state.clear()
    await message.answer(T["menu_title"][lang], reply_markup=main_menu(lang))

# catch-all
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

# ------ Analytics ------
ANALYTIC_EVENTS: List[Dict[str, Any]] = []
AGG_BY_DAY = defaultdict(lambda: Counter())

def _today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def log_event(event: str, uid: int):
    day = _today_str()
    ANALYTIC_EVENTS.append({
        "event": event, 
        "uid": uid, 
        "day": day, 
        "ts": datetime.utcnow().isoformat(timespec="seconds")
    })
    AGG_BY_DAY[day][event] += 1

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if message.from_user.id != Config.ADMIN_CHAT_ID:
        return
    d = _today_str()
    c = AGG_BY_DAY[d]
    await message.answer(
        f"📊 <b>Статистика за сегодня</b>\n\n"
        f"🔍 Поисков: {c['search']}\n"
        f"👀 Просмотров: {c['view']}\n"
        f"❤️ Лайков: {c['like']}\n"
        f"📝 Лидов: {c['lead']}"
    )

# ------ НОВОЕ: Автоматическое обновление кэша ------
async def auto_refresh_cache():
    """НОВОЕ: Автоматическое обновление данных из Google Sheets"""
    while True:
        try:
            await asyncio.sleep(Config.GSHEET_REFRESH_SEC)
            logger.info("🔄 Auto-refresh: loading data from Google Sheets...")
            rows = await rows_async(force=True)
            logger.info(f"✅ Auto-refresh complete: {len(rows)} rows in cache")
            
            # Отправляем уведомление админу (опционально, раз в час)
            if Config.ADMIN_CHAT_ID and monotonic() % 3600 < Config.GSHEET_REFRESH_SEC:
                try:
                    await bot.send_message(
                        Config.ADMIN_CHAT_ID,
                        f"🔄 Автообновление: загружено {len(rows)} объявлений"
                    )
                except Exception:
                    pass
                    
        except Exception as e:
            logger.exception(f"❌ Auto-refresh error: {e}")
            # При ошибке пробуем снова через минуту
            await asyncio.sleep(60)

async def heartbeat():
    """ИСПРАВЛЕНО: Мониторинг работы бота"""
    while True:
        try:
            logger.info(f"💓 Heartbeat OK | Cache: {len(_cached_rows)} rows | Age: {int(monotonic() - _cache_ts)}s")
        except Exception:
            logger.exception("❌ Heartbeat error")
        await asyncio.sleep(600)

# ------ Startup / Shutdown ------
async def startup():
    """ИСПРАВЛЕНО: Запуск бота с инициализацией"""
    logger.info("🚀 LivePlace bot starting...")
    
    # Загружаем данные при старте
    await rows_async(force=True)
    
    # Уведомляем админа
    if Config.ADMIN_CHAT_ID:
        try:
            await bot.send_message(
                Config.ADMIN_CHAT_ID, 
                f"✅ <b>LivePlace bot started</b>\n\n"
                f"📊 Loaded: {len(_cached_rows)} ads\n"
                f"🔄 Auto-refresh: every {Config.GSHEET_REFRESH_SEC}s\n"
                f"📢 Feedback channel: {Config.FEEDBACK_CHAT_ID}"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin on startup: {e}")
    
    # Запускаем фоновые задачи
    asyncio.create_task(heartbeat())
    asyncio.create_task(auto_refresh_cache())
    
    logger.info("✅ Bot startup complete")

async def shutdown():
    """Корректное завершение работы"""
    try:
        logger.info("🛑 Bot shutting down...")
        await bot.session.close()
        logger.info("✅ Bot shutdown complete")
    except Exception as e:
        logger.exception(f"Error during shutdown: {e}")

# ------ Main ------
async def main():
    try:
        await startup()
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await shutdown()

if __name__ == "__main__":
    asyncio.run(main())
