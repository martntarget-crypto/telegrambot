# -*- coding: utf-8 -*-
# LivePlace Telegram Bot — Полная рабочая версия с Google Sheets
# Требования: Aiogram 3.x, gspread, google-auth

import os
import json
import re
import time
import asyncio
import logging
from datetime import datetime
from collections import Counter, defaultdict
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

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

# ========== ЛОГИ ==========

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("liveplace")

# ========== КОНФИГ ==========

class Config:
    API_TOKEN = os.getenv("API_TOKEN", "").strip()
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or "0")
    SHEETS_ENABLED = os.getenv("SHEETS_ENABLED", "1").strip() not in {"", "0", "false", "False"}
    GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
    GSHEET_TAB = os.getenv("GSHEET_TAB", "Ads").strip()
    GSHEET_REFRESH_MIN = int(os.getenv("GSHEET_REFRESH_MIN", "2") or "2")
    ADS_ENABLED = os.getenv("ADS_ENABLED", "1") not in {"0", "false", "False", ""}
    ADS_PROB = float(os.getenv("ADS_PROB", "0.18"))
    ADS_COOLDOWN_SEC = int(os.getenv("ADS_COOLDOWN_SEC", "180"))
    UTM_SOURCE = os.getenv("UTM_SOURCE", "telegram")
    UTM_MEDIUM = os.getenv("UTM_MEDIUM", "bot")
    UTM_CAMPAIGN = os.getenv("UTM_CAMPAIGN", "bot_ads")

if not Config.API_TOKEN:
    raise RuntimeError("API_TOKEN не установлен в переменных окружения")

# ========== БОТ И FSM ==========

bot = Bot(token=Config.API_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())

# ========== GOOGLE SHEETS ==========

class SheetsManager:
    def __init__(self):
        if not Config.SHEETS_ENABLED:
            raise RuntimeError("SHEETS_ENABLED должен быть 1 для SheetsManager")
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            raise RuntimeError("GOOGLE_CREDENTIALS_JSON отсутствует в переменных окружения")
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

    def get_rows(self):
        ws = self.client.open_by_key(self.sheet_id).worksheet(self.tab_name)
        rows = ws.get_all_records()
        logger.info(f"Загружено {len(rows)} строк из Sheets [{self.tab_name}]")
        return rows

sheets = SheetsManager()
_cached_rows = []
_cache_ts = 0.0
CACHE_TTL = max(1, Config.GSHEET_REFRESH_MIN) * 60

def load_rows(force=False):
    global _cached_rows, _cache_ts
    if not force and _cached_rows and (time.monotonic() - _cache_ts) < CACHE_TTL:
        return _cached_rows
    try:
        data = sheets.get_rows()
        _cached_rows = data
        _cache_ts = time.monotonic()
        return data
    except Exception as e:
        logger.error(f"Не удалось загрузить строки из Sheets: {e}")
        return _cached_rows or []

async def rows_async(force=False):
    return await asyncio.to_thread(load_rows, force)

# ========== ЛОКАЛИЗАЦИЯ ==========

LANGS = ["ru","en","ka"]
USER_LANG = {}
LANG_MAP = {"ru":"ru","ru-RU":"ru","en":"en","en-US":"en","en-GB":"en","ka":"ka","ka-GE":"ka"}

T = {
    "menu_title":{"ru":"Главное меню","en":"Main menu","ka":"მთავარი მენიუ"},
    "btn_search":{"ru":"🔎 Поиск","en":"🔎 Search","ka":"🔎 ძიება"},
    "btn_latest":{"ru":"🆕 Новые","en":"🆕 Latest","ka":"🆕 ახალი"},
    "btn_language":{"ru":"🌐 Язык","en":"🌐 Language","ka":"🌐 ენა"},
    "btn_about":{"ru":"ℹ️ О боте","en":"ℹ️ About","ka":"ℹ️ შესახებ"},
    "btn_fast":{"ru":"🟢 Быстрый подбор","en":"🟢 Quick picks","ka":"🟢 სწრაფი არჩევანი"},
    "btn_favs":{"ru":"❤️ Избранное","en":"❤️ Favorites","ka":"❤️ რჩეულები"},
    "btn_home":{"ru":"🏠 Меню","en":"🏠 Menu","ka":"🏠 მენიუ"},
    "btn_daily":{"ru":"🕓 Посуточно 🆕","en":"🕓 Daily rent 🆕","ka":"🕓 დღიურად 🆕"},
    "btn_rent":{"ru":"🏘 Аренда","en":"🏘 Rent","ka":"🏘 ქირავდება"},
    "btn_sale":{"ru":"🏠 Продажа","en":"🏠 Sale","ka":"🏠 იყიდება"},
    "btn_prev":{"ru":"« Назад","en":"« Prev","ka":"« უკან"},
    "btn_next":{"ru":"Вперёд »","en":"Next »","ka":"წინ »"},
    "btn_like":{"ru":"❤️ Нравится","en":"❤️ Like","ka":"❤️ მომეწონა"},
    "btn_dislike":{"ru":"👎 Дизлайк","en":"👎 Dislike","ka":"👎 არ მომწონს"},
    "btn_fav_add":{"ru":"⭐ В избранное","en":"⭐ Favorite","ka":"⭐ რჩეულებში"},
    "btn_fav_del":{"ru":"⭐ Удалить из избранного","en":"⭐ Remove favorite","ka":"⭐ წაშლა"},
    "start":{
        "ru":"<b>LivePlace</b>\n👋 Привет! Я помогу подобрать <b>идеальную недвижимость в Грузии</b>.\n\n<b>Как это работает?</b>\n— Задам 3–4 простых вопроса\n— Покажу лучшие варианты с фото и телефоном владельца\n— Просто посмотреть? Жми <b>🟢 Быстрый подбор</b>\n\nДобро пожаловать! 🏡",
        "en":"<b>LivePlace</b>\n👋 Hi! I’ll help you find <b>your ideal home in Georgia</b>.\n\n<b>How it works:</b>\n— 3–4 quick questions\n— Top options with photos & phone\n— Just browsing? Tap <b>🟢 Quick picks</b>\n\nWelcome! 🏡",
        "ka":"<b>LivePlace</b>\n👋 გამარჯობა! ერთად ვიპოვოთ <b>იდეალური ბინა საქართველოში</b>.\n\n<b>როგორ მუშაობს:</b>\n— 3–4 მარტივი კითხვა\n— საუკეთესო ვარიანტები ფოტოებითა და მფლობელის ნომრით\n— უბრალოდ გადაათვალიერე? დააჭირე <b>🟢 სწრაფი არჩევანი</b>\n\nკეთილი იყოს თქვენი მობრძანება! 🏡"
    },
    "about":{
        "ru":"LivePlace: быстрый подбор недвижимости в Грузии. Фильтры, 10 фото, телефон владельца, избранное.",
        "en":"LivePlace: fast real-estate search in Georgia. Filters, 10 photos, owner phone, favorites.",
        "ka":"LivePlace: უძრავი ქონების სწრაფი ძიება საქართველოში. ფილტრები, 10 ფოტო, მფლობელის ნომერი, რჩეულები."
    }
}

LANG_FIELDS = {"ru":{"title":"title_ru","desc":"description_ru"},
               "en":{"title":"title_en","desc":"description_en"},
               "ka":{"title":"title_ka","desc":"description_ka"}}

def t(lang, key, **kw):
    lang = lang if lang in LANGS else "ru"
    val = T.get(key, {}).get(lang, T.get(key, {}).get("ru", key))
    try: return val.format(**kw) if kw else val
    except Exception: return val

def current_lang(uid):
    return USER_LANG.get(uid,"ru")

def main_menu(lang):
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(T["btn_fast"][lang])],
            [KeyboardButton(T["btn_search"][lang]), KeyboardButton(T["btn_latest"][lang])],
            [KeyboardButton(T["btn_favs"][lang])],
            [KeyboardButton(T["btn_language"][lang]), KeyboardButton(T["btn_about"][lang])]
        ],
        resize_keyboard=True
    )

# ========== УТИЛИТЫ ==========

def norm(s): return str(s or "").strip().lower()
def norm_mode(v):
    s = norm(v)
    if s in {"rent","аренда","long","long-term","долгосрочно","longterm"}: return "rent"
    if s in {"sale","продажа","buy","sell"}: return "sale"
    if s in {"daily","посуточно","sutki","сутки","short","short-term","day"}: return "daily"
    return ""

def drive_direct(url):
    if not url: return url
    m = re.search(r"/d/([A-Za-z0-9_-]{20,})/", url)
    if m: return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    m = re.search(r"[?&]id=([A-Za-z0-9_-]{20,})", url)
    if m: return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return url

def looks_like_image(url):
    if not url: return False
    u = url.lower()
    return any(u.endswith(ext) for ext in (".jpg",".jpeg",".png",".webp")) or \
           "googleusercontent.com" in u or "google.com/uc?export=download" in u

def collect_photos(row):
    out = []
    for i in range(1, 11):
        k = f"photo{i}"
        if k in row:
            url = drive_direct(row[k])
            if looks_like_image(url): out.append(url)
    return out[:10]

def build_utm(url):
    if not url: return url
    parts = list(urlparse(url))
    q = parse_qs(parts[4])
    q.update({"utm_source": Config.UTM_SOURCE,
              "utm_medium": Config.UTM_MEDIUM,
              "utm_campaign": Config.UTM_CAMPAIGN})
    parts[4] = urlencode(q, doseq=True)
    return urlunparse(parts)

# ========== FSM ==========

class SearchStates(StatesGroup):
    city = State()
    area = State()
    rooms = State()
    price = State()
    confirm = State()

# ========== ХЭНДЛЕРЫ ==========

@dp.message(Command("start"))
async def cmd_start(msg: types.Message, state: FSMContext):
    uid = msg.from_user.id
    USER_LANG[uid] = LANG_MAP.get(msg.from_user.language_code,"ru")
    await state.clear()
    kb = main_menu(USER_LANG[uid])
    await msg.answer(t(USER_LANG[uid], "start"), reply_markup=kb)

@dp.message(Command("about"))
async def cmd_about(msg: types.Message):
    lang = current_lang(msg.from_user.id)
    await msg.answer(t(lang,"about"))

@dp.message()
async def all_messages(msg: types.Message, state: FSMContext):
    text = msg.text.strip() if msg.text else ""
    uid = msg.from_user.id
    lang = current_lang(uid)

    # Меню
    if text in [T["btn_fast"][lang], T["btn_search"][lang], T["btn_latest"][lang]]:
        await msg.answer(f"Функционал поиска: {text}")
        return
    if text in [T["btn_language"][lang]]:
        await msg.answer("Выберите язык (RU/EN/KA)")
        return
    if text in [T["btn_about"][lang]]:
        await cmd_about(msg)
        return
    if text in [T["btn_favs"][lang]]:
        await msg.answer("Показ избранного")
        return
    await msg.answer("Неизвестная команда. Используйте меню")

# ========== АДМИН ==========

@dp.message(Command("health"))
async def cmd_health(msg: types.Message):
    if msg.from_user.id != Config.ADMIN_CHAT_ID: return
    await msg.answer("✅ Bot is alive")

@dp.message(Command("refresh"))
async def cmd_refresh(msg: types.Message):
    if msg.from_user.id != Config.ADMIN_CHAT_ID: return
    load_rows(force=True)
    await msg.answer("✅ Sheets refreshed")

# ========== MAIN ==========

async def main():
    logger.info("LivePlace bot starting…")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__=="__main__":
    asyncio.run(main())
