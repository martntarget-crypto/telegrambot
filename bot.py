# -*- coding: utf-8 -*-
# LivePlace Telegram Bot — Полная версия, Railway-ready
# Требования окружения:
#   API_TOKEN=...
#   GOOGLE_CREDENTIALS_JSON=...
#   GSHEET_ID=...
#   GSHEET_TAB=Ads
#   SHEETS_ENABLED=1
#   ADMIN_CHAT_ID=...  (опционально)

import os
import re
import json
import time
import random
import asyncio
import logging
from datetime import datetime
from collections import Counter, defaultdict
from typing import List, Dict, Any
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("liveplace")

# .env локально (не обязательно)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# == Config ==
class Config:
    API_TOKEN = os.getenv("API_TOKEN", "").strip()
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or "0")
    SHEETS_ENABLED = os.getenv("SHEETS_ENABLED", "1").strip() not in {"", "0", "false", "False"}
    GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
    GSHEET_TAB = os.getenv("GSHEET_TAB", "Ads").strip()
    GSHEET_REFRESH_MIN = int(os.getenv("GSHEET_REFRESH_MIN", "2") or "2")
    UTM_SOURCE = os.getenv("UTM_SOURCE", "telegram")
    UTM_MEDIUM = os.getenv("UTM_MEDIUM", "bot")
    UTM_CAMPAIGN = os.getenv("UTM_CAMPAIGN", "bot_ads")
    ADS_ENABLED = os.getenv("ADS_ENABLED", "1") not in {"0", "false", "False", ""}
    ADS_PROB = float(os.getenv("ADS_PROB", "0.18"))
    ADS_COOLDOWN_SEC = int(os.getenv("ADS_COOLDOWN_SEC", "180"))

if not Config.API_TOKEN:
    raise RuntimeError("API_TOKEN is not set. Add it to Railway Variables.")

bot = Bot(token=Config.API_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

class SheetsManager:
    def __init__(self):
        if not Config.SHEETS_ENABLED:
            raise RuntimeError("SHEETS_ENABLED must be 1 for SheetsManager")
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            raise RuntimeError("GOOGLE_CREDENTIALS_JSON is missing in Variables")
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
        logger.info(f"Loaded {len(rows)} rows from Sheets [{self.tab_name}]")
        return rows

sheets = SheetsManager()

# Кэш
_cached_rows: List[Dict[str, Any]] = []
_cache_ts: float = 0.0
CACHE_TTL = max(1, Config.GSHEET_REFRESH_MIN) * 60

def load_rows(force: bool = False) -> List[Dict[str, Any]]:
    global _cached_rows, _cache_ts
    if not force and _cached_rows and (time.monotonic() - _cache_ts) < CACHE_TTL:
        return _cached_rows
    try:
        data = sheets.get_rows()
        _cached_rows = data
        _cache_ts = time.monotonic()
        return data
    except Exception as e:
        logger.error(f"Failed to load rows from Sheets: {e}")
        return _cached_rows or []

async def rows_async(force: bool = False) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(load_rows, force)

# Локализация
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
    "btn_prev": {"ru": "« Назад", "en": "« Prev", "ka": "« უკან"},
    "btn_next": {"ru": "Вперёд »", "en": "Next »", "ka": "წინ »"},
    "btn_like": {"ru": "❤️ Нравится", "en": "❤️ Like", "ka": "❤️ მომეწონა"},
    "btn_dislike": {"ru": "👎 Дизлайк", "en": "👎 Dislike", "ka": "👎 არ მომწონს"},
    "btn_fav_add": {"ru": "⭐ В избранное", "en": "⭐ Favorite", "ka": "⭐ რჩეულებში"},
    "btn_fav_del": {"ru": "⭐ Удалить из избранного", "en": "⭐ Remove favorite", "ka": "⭐ წაშლა"},
    "start": {
        "ru": "<b>LivePlace</b>\n👋 Привет! Я помогу подобрать <b>идеальную недвижимость в Грузии</b>.\n\n<b>Как это работает?</b>\n— Задам 3–4 простых вопроса\n— Покажу лучшие варианты с фото и телефоном владельца\n— Просто посмотреть? Жми <b>🟢 Быстрый подбор</b>\n\nДобро пожаловать и удачного поиска! 🏡",
        "en": "<b>LivePlace</b>\n👋 Hi! I'll help you find <b>your ideal home in Georgia</b>.\n\n<b>How it works:</b>\n— 3–4 quick questions\n— Top options with photos & owner phone\n— Just browsing? Tap <b>🟢 Quick picks</b>\n\nWelcome and happy hunting! 🏡",
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

# == Utilities ==
def norm(s: Any) -> str:
    return str(s or "").strip().lower()

def norm_mode(v: Any) -> str:
    s = norm(v)
    if s in {"rent","аренда","long","long-term","долгосрочно","longterm"}: return "rent"
    if s in {"sale","продажа","buy","sell"}: return "sale"
    if s in {"daily","посуточно","sutki","сутки","short","short-term","day"}: return "daily"
    return ""

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

def collect_photos(row: Dict[str, Any]) -> List[str]:
    out = []
    for i in range(1, 11):
        u = str(row.get(f"photo{i}", "")).strip()
        if not u: continue
        u = drive_direct(u)
        if looks_like_image(u): out.append(u)
    return out

def parse_rooms(v: Any) -> float:
    s = str(v or "").strip().lower()
    if s in {"студия","studio","stud","სტუდიო"}: return 0.5
    try: return float(s.replace("+",""))
    except Exception: return -1.0

def build_utm_url(raw: str, ad_id: str, uid: int) -> str:
    if not raw: return "https://liveplace.com.ge/"
    seed = f"{uid}:{datetime.utcnow().strftime('%Y%m%d')}:{ad_id}".encode()
    token = __import__("hashlib").sha256(seed).hexdigest()[:16]
    u = urlparse(raw); q = parse_qs(u.query)
    q["utm_source"]=[Config.UTM_SOURCE]
    q["utm_medium"]=[Config.UTM_MEDIUM]
    q["utm_campaign"]=[Config.UTM_CAMPAIGN]
    q["utm_content"]=[ad_id]
    q["token"]=[token]
    new_q = urlencode({k: v[0] for k,v in q.items()})
    return urlunparse((u.scheme,u.netloc,u.path,u.params,new_q,u.fragment))

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
    if price: lines.append(f"Цена: {price}")
    if pub_txt: lines.append(f"Опубликовано: {pub_txt}")
    if desc: lines.append(desc)
    if phone: lines.append(f"<b>Телефон:</b> {phone}")
    if not desc and not phone: lines.append("—")
    return "\n".join(lines)

# == Пользовательские состояния ==
class Search(StatesGroup):
    mode = State()
    city = State()
    district = State()
    rtype = State()
    rooms = State()
    price = State()

# == Данные пользователя ==
PAGE_SIZE = 8
USER_RESULTS: Dict[int, Dict[str, Any]] = {}
USER_FAVS: Dict[int, List[str]] = {}
LAST_AD_TIME: Dict[int, float] = {}

# == Ads ==
def maybe_send_ad(uid: int) -> bool:
    if not Config.ADS_ENABLED: return False
    now = time.monotonic()
    last = LAST_AD_TIME.get(uid, 0)
    if now - last < Config.ADS_COOLDOWN_SEC: return False
    if random.random() > Config.ADS_PROB: return False
    LAST_AD_TIME[uid] = now
    # Пример: отправляем простое рекламное сообщение
    asyncio.create_task(bot.send_message(uid, "💰 Реклама: попробуйте новые варианты LivePlace!"))
    return True

# == Handlers ==
@dp.message(Command("start"))
async def cmd_start(msg: types.Message, state: FSMContext):
    lang = current_lang(msg.from_user.id)
    await state.clear()
    maybe_send_ad(msg.from_user.id)
    await msg.answer(T["start"][lang], reply_markup=main_menu(lang))

@dp.message(lambda m: m.text in [T["btn_language"].get(current_lang(m.from_user.id))])
async def choose_language(msg: types.Message):
    kb = InlineKeyboardMarkup(row_width=3)
    for l in LANGS:
        kb.add(InlineKeyboardButton(text=l.upper(), callback_data=f"lang:{l}"))
    await msg.answer("Выберите язык / Choose language / ენა", reply_markup=kb)

@dp.callback_query(F.data.startswith("lang:"))
async def set_language(c: types.CallbackQuery):
    uid = c.from_user.id
    lang = c.data.split(":")[1]
    USER_LANG[uid] = lang
    await c.message.edit_text(f"Язык установлен: {lang.upper()}", reply_markup=None)
    await c.answer()
    await c.message.answer("Меню:", reply_markup=main_menu(lang))

# == Polling / Deployment-ready ==
async def main():
    try:
        logger.info("LivePlace bot starting…")
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
