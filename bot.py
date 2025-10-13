# -*- coding: utf-8 -*-
# LivePlace Telegram Bot — Railway-ready (Sheets ENABLED)
# Полная рабочая версия bot.py с реальным подключением к Google Sheets.
# Требования окружения (Railway → Variables):
#   API_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxx
#   GOOGLE_CREDENTIALS_JSON={...весь JSON сервис-аккаунта...}
#   GSHEET_ID=1yrB5Vy7o18B05nkJBqQe9hE9971jJsTMEKKTsDHGa8w
#   GSHEET_TAB=Ads
#   SHEETS_ENABLED=1
#   ADMIN_CHAT_ID=640007272   (опционально)

import os
import re
import csv
import json
import time
import random
import asyncio
import logging
from time import monotonic
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from collections import Counter, defaultdict
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

# == Aiogram 3.x ==
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
)

# == Logging ==
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("liveplace")

# == .env (локально не требуется, но не мешает) ==
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

    # Рекламные UTM
    UTM_SOURCE = os.getenv("UTM_SOURCE", "telegram")
    UTM_MEDIUM = os.getenv("UTM_MEDIUM", "bot")
    UTM_CAMPAIGN = os.getenv("UTM_CAMPAIGN", "bot_ads")

    # Реклама/частота
    ADS_ENABLED = os.getenv("ADS_ENABLED", "1") not in {"0", "false", "False", ""}
    ADS_PROB = float(os.getenv("ADS_PROB", "0.18"))
    ADS_COOLDOWN_SEC = int(os.getenv("ADS_COOLDOWN_SEC", "180"))

if not Config.API_TOKEN:
    raise RuntimeError("API_TOKEN is not set. Add it to Railway Variables.")

# == Bot ==
bot = Bot(token=Config.API_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())

# == Google Sheets ==
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

# == Кэш объявлений ==
_cached_rows: List[Dict[str, Any]] = []
_cache_ts: float = 0.0
CACHE_TTL = max(1, Config.GSHEET_REFRESH_MIN) * 60

def load_rows(force: bool = False) -> List[Dict[str, Any]]:
    global _cached_rows, _cache_ts
    if not force and _cached_rows and (monotonic() - _cache_ts) < CACHE_TTL:
        return _cached_rows
    try:
        data = sheets.get_rows()
        _cached_rows = data
        _cache_ts = monotonic()
        return data
    except Exception as e:
        logger.error(f"Failed to load rows from Sheets: {e}")
        return _cached_rows or []

async def rows_async(force: bool = False) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(load_rows, force)

# == Локализация и тексты ==
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

# == Утилиты ==
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

# == Данные пользователя/результаты ==
PAGE_SIZE = 8
USER_RESULTS: Dict[int, Dict[str, Any]] = {}
USER_FAVS: Dict[int, List[str]] = {}
LAST_AD_TIME: Dict[int, float] = {}
LAST_AD_ID: Dict[int, str] = {}

# == Реклама ==
ADS = [
    {"id":"lead_form","text_ru":"🔥 Ищете квартиру быстрее? Оставьте заявку — подберём за 24 часа!","url":"https://liveplace.com.ge/lead","photo":""},
    {"id":"mortgage_help","text_ru":"🏦 Поможем с ипотекой для нерезидентов. Узнайте детали.","url":"https://liveplace.com.ge/mortgage","photo":""},
    {"id":"rent_catalog","text_ru":"🏘 Посмотрите новые квартиры в аренду — свежие объявления.","url":"https://liveplace.com.ge/rent","photo":""},
    {"id":"sell_service","text_ru":"💼 Хотите продать квартиру? Разместим и продвинем на LivePlace.","url":"https://liveplace.com.ge/sell","photo":""},
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
    if not should_show_ad(uid): 
        return
    ad = pick_ad(uid)
    url = build_utm_url(ad.get("url",""), ad.get("id","ad"), uid)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👉 Подробнее", url=url)]
    ])
    try:
        await bot.send_message(chat_id, ad.get("text_ru","LivePlace"), reply_markup=kb)
    except Exception:
        pass
    LAST_AD_TIME[uid] = time.time()
    LAST_AD_ID[uid] = ad.get("id")

# == Поиск ==
def _filter_rows(rows: List[Dict[str, Any]], q: Dict[str, Any]) -> List[Dict[str, Any]]:
    def ok(r):
        if q.get("mode") and norm_mode(r.get("mode")) != q["mode"]: return False
        if q.get("city") and norm(r.get("city")) != norm(q["city"]): return False
        if q.get("district") and norm(r.get("district")) != norm(q["district"]): return False
        if q.get("rtype") and norm(r.get("type")) != norm(q["rtype"]): return False
        if q.get("rooms"):
            try:
                need = float(q["rooms"])
                have = parse_rooms(r.get("rooms"))
                if have < 0: return False
                if int(need) != int(have) and not (need==0.5 and have==0.5):
                    return False
            except Exception:
                return False
        if q.get("price"):
            try:
                p = float(re.sub(r"[^\d.]", "", str(r.get("price","")) or "0"))
                return p <= float(q["price"])
            except Exception:
                return False
        return True
    out = [r for r in rows if ok(r)]
    return out

def _slice(listing: List[Any], page: int, size: int) -> List[Any]:
    return listing[page*size:(page+1)*size]

# == Команды ==
@dp.message(Command("start", "menu"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in USER_LANG:
        code = (message.from_user.language_code or "").strip()
        USER_LANG[message.from_user.id] = LANG_MAP.get(code, "ru")
    lang = current_lang(message.from_user.id)
    await state.clear()
    await message.answer(t(lang, "start"), reply_markup=main_menu(lang))

@dp.message(Command("about"))
async def cmd_about(message: types.Message):
    await message.answer(T["about"]["ru"])

@dp.message(Command("health"))
async def cmd_health(message: types.Message):
    await message.answer(
        f"✅ Bot OK\n"
        f"Sheets: ENABLED\n"
        f"Rows cached: {len(_cached_rows)}\n"
        f"TTL: {Config.GSHEET_REFRESH_MIN} min"
    )

@dp.message(Command("gs"))
async def cmd_gs(message: types.Message):
    rows = await rows_async(force=True)
    await message.answer(f"📊 Загружено строк: {len(rows)} из Google Sheets.")

@dp.message(Command("refresh","reload"))
async def cmd_refresh(message: types.Message):
    rows = await rows_async(force=True)
    await message.answer(f"♻️ Перезагружено. В кэше: {len(rows)} строк.")

# == Простейший сценарий поиска (мастер из 3 шагов) ==
class Wizard(StatesGroup):
    mode = State()
    city = State()
    budget = State()

@dp.message(F.text == T["btn_search"]["ru"])
@dp.message(Command("search"))
async def start_search(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(Wizard.mode)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="rent")],
            [KeyboardButton(text="sale")],
            [KeyboardButton(text="daily")]
        ],
        resize_keyboard=True
    )
    await message.answer("Выберите режим: rent / sale / daily", reply_markup=kb)

@dp.message(Wizard.mode)
async def pick_city(message: types.Message, state: FSMContext):
    mode = norm_mode(message.text)
    if not mode:
        return await message.answer("Укажи rent/sale/daily")
    await state.update_data(mode=mode)

    rows = await rows_async()
    cities = sorted({str(r.get("city","")).strip() for r in rows if r.get("city")})
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=c)] for c in cities[:20]],
        resize_keyboard=True
    )
    await state.set_state(Wizard.city)
    await message.answer("Выберите город:", reply_markup=kb)

@dp.message(Wizard.city)
async def pick_budget(message: types.Message, state: FSMContext):
    await state.update_data(city=message.text.strip())
    await state.set_state(Wizard.budget)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="500")],
            [KeyboardButton(text="1000")],
            [KeyboardButton(text="2000")],
            [KeyboardButton(text="5000")]
        ],
        resize_keyboard=True
    )
    await message.answer("Максимальный бюджет (число):", reply_markup=kb)

@dp.message(Wizard.budget)
async def show_results(message: types.Message, state: FSMContext):
    try:
        budget = float(message.text.strip())
    except Exception:
        return await message.answer("Введи число, например 1000")
    data = await state.get_data()
    query = {"mode": data["mode"], "city": data["city"], "price": budget}

    rows = _filter_rows(await rows_async(), query)
    USER_RESULTS[message.from_user.id] = {"query": query, "rows": rows, "page": 0}

    if not rows:
        await message.answer("Ничего не найдено.", reply_markup=main_menu("ru"))
        return

    await send_page(message.chat.id, message.from_user.id, 0)
    await state.clear()

async def send_page(chat_id: int, uid: int, page: int):
    bundle = USER_RESULTS.get(uid)
    if not bundle: return
    rows = bundle["rows"]
    page = max(0, min(page, (len(rows)-1)//PAGE_SIZE))
    bundle["page"] = page

    chunk = _slice(rows, page, PAGE_SIZE)
    if not chunk:
        await bot.send_message(chat_id, "Больше объявлений нет.")
        return

    for r in chunk:
        photos = collect_photos(r)
        text = format_card(r, "ru")
        if photos:
            media = [InputMediaPhoto(media=photos[0], caption=text)]
            for p in photos[1:10]:
                media.append(InputMediaPhoto(media=p))
            try:
                await bot.send_media_group(chat_id, media)
            except Exception:
                await bot.send_message(chat_id, text)
        else:
            await bot.send_message(chat_id, text)
        await asyncio.sleep(0.2)

    nav = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="« Назад", callback_data="nav:prev"),
            InlineKeyboardButton(text=f"{page+1}/{(len(rows)-1)//PAGE_SIZE+1}", callback_data="noop"),
            InlineKeyboardButton(text="Вперёд »", callback_data="nav:next"),
        ]
    ])
    await bot.send_message(chat_id, "Навигация:", reply_markup=nav)

    # возможно показать рекламу
    await maybe_show_ad_by_chat(chat_id, uid)

@dp.callback_query(F.data.startswith("nav:"))
async def cb_nav(cb: types.CallbackQuery):
    uid = cb.from_user.id
    bundle = USER_RESULTS.get(uid)
    if not bundle:
        return await cb.answer("Список пуст.")
    page = bundle["page"]
    if cb.data == "nav:prev":
        page -= 1
    elif cb.data == "nav:next":
        page += 1
    await cb.answer()
    await send_page(cb.message.chat.id, uid, page)

# == Аналитика (упрощённая) ==
ANALYTIC_EVENTS: List[Dict[str, Any]] = []
AGG_BY_DAY = defaultdict(lambda: Counter())

def _today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def log_event(event: str, uid: int):
    day = _today_str()
    ANALYTIC_EVENTS.append({"event": event, "uid": uid, "day": day, "ts": datetime.utcnow().isoformat(timespec="seconds")})
    AGG_BY_DAY[day][event] += 1

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    d = _today_str()
    c = AGG_BY_DAY[d]
    await message.answer(
        f"📊 Сегодня: search={c['search']}, view={c['view']}, leads={c['lead']}"
    )

# == Старт/останов ==
async def heartbeat():
    while True:
        try:
            logger.info("Heartbeat OK")
        except Exception:
            pass
        await asyncio.sleep(600)

async def startup():
    await rows_async(force=True)
    if Config.ADMIN_CHAT_ID:
        try:
            await bot.send_message(Config.ADMIN_CHAT_ID, "🤖 LivePlace bot started (Sheets enabled)")
        except Exception:
            pass
    asyncio.create_task(heartbeat())

async def shutdown():
    try:
        await bot.session.close()
    except Exception:
        pass
    logger.info("Bot shutdown complete")

async def main():
    try:
        await startup()
        logger.info("Starting polling…")
        await dp.start_polling(bot)
    finally:
        await shutdown()

if __name__ == "__main__":
    asyncio.run(main())

