# -*- coding: utf-8 -*-
"""
LivePlace Telegram Bot — финальная версия (aiogram 3.x)
Функции:
 - Google Sheets (через сервис-аккаунт)
 - Кэширование листа
 - Динамические кнопки (город -> район)
 - Иконки городов (публичный маппинг)
 - FSM: режим -> город -> район -> комнаты -> цена
 - Показ карточек с фото (1-10), UTM-ссылки
 - Пагинация, лайк/дизлайк, избранное
 - Реклама (простая)
 - Поддержка 3 языков (ru/en/ka)
 - Админ-команды: /health, /gs, /refresh, /stats
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
from typing import List, Dict, Any
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

# External libs for Sheets
import gspread
from google.oauth2.service_account import Credentials

# ------ Logging ------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("liveplace")

# ------ Load .env optionally ------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ------ Config ------
class Config:
    API_TOKEN = os.getenv("API_TOKEN", "").strip()
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or "0")
    SHEETS_ENABLED = os.getenv("SHEETS_ENABLED", "1").strip() not in {"", "0", "false", "False"}
    GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
    GSHEET_TAB = os.getenv("GSHEET_TAB", "Ads").strip()
    GSHEET_REFRESH_MIN = int(os.getenv("GSHEET_REFRESH_MIN", "2") or "2")
    ADS_ENABLED = os.getenv("ADS_ENABLED", "1").strip() not in {"0", "false", "False", ""}
    ADS_PROB = float(os.getenv("ADS_PROB", "0.18") or 0.18)
    ADS_COOLDOWN_SEC = int(os.getenv("ADS_COOLDOWN_SEC", "180") or 180)
    UTM_SOURCE = os.getenv("UTM_SOURCE", "telegram")
    UTM_MEDIUM = os.getenv("UTM_MEDIUM", "bot")
    UTM_CAMPAIGN = os.getenv("UTM_CAMPAIGN", "bot_ads")

if not Config.API_TOKEN:
    raise RuntimeError("API_TOKEN is not set. Add it to environment variables")

# ------ Bot & Dispatcher ------
bot = Bot(token=Config.API_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())

# ------ Sheets manager ------
class SheetsManager:
    def __init__(self):
        if not Config.SHEETS_ENABLED:
            raise RuntimeError("SHEETS_ENABLED must be 1 for SheetsManager")
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
        logger.info(f"Loaded {len(rows)} rows from Sheets [{self.tab_name}]")
        return rows

sheets = SheetsManager()

# ------ Cache rows ------
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
        logger.exception("Failed to load rows from Sheets: %s", e)
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
    # add more mappings as needed (use lowercase keys)
}
PRICE_RANGES = {
    "sale": ["35000$-", "35000$-50000$", "50000$-75000$", "75000$-100000$", "100000$-150000$", "150000$+"],
    "rent": ["300$-", "300$-500$", "500$-700$", "700$-900$", "900$-1100$", "1100$+"],
    "daily": ["Пропустить"]
}

# ------ Utilities ------
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
        u = str(row.get(f"photo{i}", "") or "").strip()
        if not u: continue
        u = drive_direct(u)
        if looks_like_image(u): out.append(u)
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
    if price: lines.append(f"Цена: {price}")
    if pub_txt: lines.append(f"Опубликовано: {pub_txt}")
    if desc: lines.append(desc)
    if phone: lines.append(f"<b>Телефон:</b> {phone}")
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
USER_FAVS: Dict[int, List[str]] = defaultdict(list)
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
    def ok(r):
        if q.get("mode") and norm_mode(r.get("mode")) != q["mode"]: 
            return False
        if q.get("city") and norm(r.get("city")) != norm(q["city"]): 
            return False
        if q.get("district") and norm(r.get("district")) != norm(q["district"]): 
            return False
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
            # q["price"] could be a range like "35000$-50000$" or "35000$-"
            try:
                pr = str(q["price"])
                if "-" in pr:
                    left, right = pr.split("-",1)
                    left_val = float(re.sub(r"[^\d]", "", left) or "0")
                    right_val = float(re.sub(r"[^\d]", "", right) or "0")
                    p = float(re.sub(r"[^\d.]", "", str(r.get("price","")) or "0") or 0)
                    if right_val==0:
                        # left and above
                        if p < left_val: return False
                    else:
                        if p < left_val or p > right_val: return False
                else:
                    # exact numeric cap
                    cap = float(re.sub(r"[^\d.]", "", pr) or "0")
                    p = float(re.sub(r"[^\d.]", "", str(r.get("price","")) or "0") or 0)
                    if p > cap: return False
            except Exception:
                return False
        return True
    return [r for r in rows if ok(r)]

def _slice(listing: List[Any], page: int, size: int) -> List[Any]:
    return listing[page*size:(page+1)*size]

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
    await message.answer(f"✅ Bot OK\nSheets enabled: {Config.SHEETS_ENABLED}\nCached rows: {len(_cached_rows)}")

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

# ---------- Start search flow (Wizard) ----------
@dp.message(F.text == T["btn_search"]["ru"])
@dp.message(F.text == T["btn_search"]["en"])
@dp.message(F.text == T["btn_search"]["ka"])
@dp.message(Command("search"))
async def start_search(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(Wizard.mode)
    # aiogram 3.x requires keyboard kw when creating ReplyKeyboardMarkup
    kb = ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)
    kb.add(KeyboardButton(T["btn_rent"][current_lang(message.from_user.id)]))
    kb.add(KeyboardButton(T["btn_sale"][current_lang(message.from_user.id)]))
    kb.add(KeyboardButton(T["btn_daily"][current_lang(message.from_user.id)]))
    await message.answer("Выберите режим: rent / sale / daily", reply_markup=kb)

@dp.message(F.state == Wizard.mode)
async def pick_city_mode(message: types.Message, state: FSMContext):
    mode = norm_mode(message.text)
    if not mode:
        return await message.answer("Укажи rent/sale/daily")
    await state.update_data(mode=mode)

    rows = await rows_async()
    city_counter = Counter([str(r.get("city","")).strip() for r in rows if r.get("city")])
    buttons = []
    # sort by name, but we want stable deterministic order
    for city, count in sorted(city_counter.items(), key=lambda x: (-x[1], x[0].lower())):
        icon = CITY_ICONS.get(norm(city), "🏠")
        label = f"{icon} {city} ({count})"
        buttons.append([KeyboardButton(label)])
    if not buttons:
        buttons = [[KeyboardButton("Пропустить")]]
    kb = ReplyKeyboardMarkup(keyboard=buttons[:40] + [[KeyboardButton("Пропустить")]], resize_keyboard=True)
    await state.set_state(Wizard.city)
    await message.answer("Выберите город:", reply_markup=kb)

@dp.message(F.state == Wizard.city)
async def pick_district(message: types.Message, state: FSMContext):
    city_text = message.text.strip()
    if city_text.lower() == "пропустить":
        await state.update_data(city="")
        # move directly to rooms/price selection
        await state.set_state(Wizard.district)
        await pick_price_prompt(message, state)
        return

    # extract name (remove icon and (count))
    city = re.sub(r"^\s*[^\w\dА-Яа-яЁёA-Za-z]+","", city_text)
    city = re.sub(r"\(\d+\)\s*$","", city).strip()
    await state.update_data(city=city)

    rows = await rows_async()
    district_counter = Counter([str(r.get("district","")).strip() for r in rows if norm(r.get("city")) == norm(city) and r.get("district")])
    if not district_counter:
        # no districts -> skip
        await state.update_data(district="")
        await state.set_state(Wizard.district)
        await pick_price_prompt(message, state)
        return

    buttons = [[KeyboardButton(f"{d} ({c})")] for d,c in sorted(district_counter.items(), key=lambda x:(-x[1], x[0].lower()))]
    buttons.append([KeyboardButton("Пропустить")])
    kb = ReplyKeyboardMarkup(keyboard=buttons[:40], resize_keyboard=True)
    await state.set_state(Wizard.district)
    await message.answer("Выберите район (или Пропустить):", reply_markup=kb)

@dp.message(F.state == Wizard.district)
async def pick_rooms_or_price(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text.lower() == "пропустить":
        await state.update_data(district="")
    else:
        district = re.sub(r"\(\d+\)\s*$","", text).strip()
        await state.update_data(district=district)

    # ask rooms
    await state.set_state(Wizard.rooms)
    kb = ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)
    kb.add(KeyboardButton("1"), KeyboardButton("2"), KeyboardButton("3"))
    kb.add(KeyboardButton("4"), KeyboardButton("5+"), KeyboardButton("Пропустить"))
    await message.answer("Выберите количество комнат (1,2,3,4,5+ или Пропустить):", reply_markup=kb)

@dp.message(F.state == Wizard.rooms)
async def pick_price_prompt(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text.lower() in {"пропустить","весь город","весь район"}:
        await state.update_data(rooms="")
    else:
        # normalize studio -> 0.5, 5+ -> 5+
        val = text.strip().lower()
        if val=="студия":
            val = "0.5"
        await state.update_data(rooms=val)

    data = await state.get_data()
    mode = data.get("mode","sale")
    # price selection based on mode
    ranges = PRICE_RANGES.get(mode, PRICE_RANGES["sale"])
    buttons = [[KeyboardButton(p)] for p in ranges]
    buttons.append([KeyboardButton("Пропустить")])
    kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await state.set_state(Wizard.price)
    await message.answer("Выберите ценовой диапазон или Пропустить:", reply_markup=kb)

@dp.message(F.state == Wizard.price)
async def show_results_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text.lower() == "пропустить":
        price = ""
    else:
        price = text

    data = await state.get_data()
    await state.update_data(price=price)
    query = {
        "mode": data.get("mode"),
        "city": data.get("city",""),
        "district": data.get("district",""),
        "rooms": data.get("rooms",""),
        "price": price
    }

    rows = _filter_rows(await rows_async(), query)
    USER_RESULTS[message.from_user.id] = {"query": query, "rows": rows, "page": 0}
    if not rows:
        await message.answer("Ничего не найдено по вашим параметрам.", reply_markup=main_menu(current_lang(message.from_user.id)))
        await state.clear()
        return

    await send_page(message.chat.id, message.from_user.id, 0)
    await state.clear()

# ------ send_page / navigation ------
async def send_page(chat_id: int, uid: int, page: int):
    bundle = USER_RESULTS.get(uid)
    if not bundle:
        await bot.send_message(chat_id, "Список пуст.", reply_markup=main_menu(current_lang(uid)))
        return
    rows = bundle["rows"]
    if not rows:
        await bot.send_message(chat_id, "Нет объявлений.", reply_markup=main_menu(current_lang(uid)))
        return
    page = max(0, min(page, (len(rows)-1)//PAGE_SIZE))
    bundle["page"] = page
    chunk = _slice(rows, page, PAGE_SIZE)

    for r in chunk:
        photos = collect_photos(r)
        text = format_card(r, current_lang(uid))
        if photos:
            # send as media group (Telegram requires caption on first)
            media = [InputMediaPhoto(media=photos[0], caption=text)]
            for p in photos[1:]:
                media.append(InputMediaPhoto(media=p))
            try:
                await bot.send_media_group(chat_id, media)
            except Exception:
                await bot.send_message(chat_id, text)
        else:
            await bot.send_message(chat_id, text)
        await asyncio.sleep(0.12)

    # navigation inline keyboard
    total_pages = (len(rows)-1)//PAGE_SIZE + 1
    nav = InlineKeyboardMarkup(row_width=3)
    nav_buttons = []
    nav_buttons.append(InlineKeyboardButton(text=T["btn_prev"][current_lang(uid)], callback_data="nav:prev"))
    nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    nav_buttons.append(InlineKeyboardButton(text=T["btn_next"][current_lang(uid)], callback_data="nav:next"))
    nav.row(*nav_buttons)
    # favorites / like row
    nav.add(InlineKeyboardButton(text=T["btn_fav_add"][current_lang(uid)], callback_data="fav:add"),
            InlineKeyboardButton(text=T["btn_like"][current_lang(uid)], callback_data="like"),
            InlineKeyboardButton(text=T["btn_dislike"][current_lang(uid)], callback_data="dislike"))
    await bot.send_message(chat_id, "Навигация:", reply_markup=nav)
    # maybe ad
    await maybe_show_ad_by_chat(chat_id, uid)

@dp.callback_query(F.data.startswith("nav:"))
async def cb_nav(cb: types.CallbackQuery):
    uid = cb.from_user.id
    bundle = USER_RESULTS.get(uid)
    if not bundle:
        await cb.answer("Список пуст.")
        return
    page = bundle.get("page", 0)
    if cb.data == "nav:prev":
        page -= 1
    elif cb.data == "nav:next":
        page += 1
    page = max(0, min(page, (len(bundle["rows"])-1)//PAGE_SIZE))
    await cb.answer()
    await send_page(cb.message.chat.id, uid, page)

@dp.callback_query(F.data == "noop")
async def cb_noop(cb: types.CallbackQuery):
    await cb.answer()

# ------ Like / Dislike / Favorites callbacks ------
@dp.callback_query(F.data == "like")
async def cb_like(cb: types.CallbackQuery):
    await cb.answer("Спасибо! 👍")
    # Here you can record analytics

@dp.callback_query(F.data == "dislike")
async def cb_dislike(cb: types.CallbackQuery):
    await cb.answer("Учтём ваш отзыв 👎")

@dp.callback_query(F.data.startswith("fav:"))
async def cb_fav(cb: types.CallbackQuery):
    uid = cb.from_user.id
    payload = cb.data.split(":")[1]
    if payload == "add":
        # Determine current ad id - tricky: we don't have single ad id in nav; for demonstration we'll toggle a placeholder
        # In production store ad_id in message or callback_data; here we add a marker
        USER_FAVS[uid].append("manual_fav_placeholder")
        await cb.answer("Добавлено в избранное")
    elif payload == "del":
        if USER_FAVS[uid]:
            USER_FAVS[uid].pop()
        await cb.answer("Удалено из избранного")
    else:
        await cb.answer()

# ------ Generic handlers for language and menu ------
@dp.message(F.text == T["btn_language"]["ru"])
@dp.message(F.text == T["btn_language"]["en"])
@dp.message(F.text == T["btn_language"]["ka"])
async def choose_language(message: types.Message):
    kb = InlineKeyboardMarkup(row_width=3)
    for l in LANGS:
        kb.add(InlineKeyboardButton(text=l.upper(), callback_data=f"lang:{l}"))
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

@dp.message(F.text == T["btn_fast"]["ru"])
@dp.message(F.text == T["btn_fast"]["en"])
@dp.message(F.text == T["btn_fast"]["ka"])
async def quick_pick_entry(msg: types.Message, state: FSMContext):
    # Quick pick - a simplified variant: ask mode then show top N newest
    await state.clear()
    await state.set_state(Wizard.mode)
    # aiogram 3.x requires keyboard kw when creating ReplyKeyboardMarkup
    kb = ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)
    kb.add(KeyboardButton(T["btn_rent"][current_lang(msg.from_user.id)]))
    kb.add(KeyboardButton(T["btn_sale"][current_lang(msg.from_user.id)]))
    kb.add(KeyboardButton(T["btn_daily"][current_lang(msg.from_user.id)]))
    await msg.answer("Выберите режим для быстрого подбора:", reply_markup=kb)

# catch-all to avoid "not handled"
@dp.message()
async def fallback_all(message: types.Message, state: FSMContext):
    # Provide helpful hint instead of ignoring updates
    text = (message.text or "").strip()
    if not text:
        await message.answer("Я получил сообщение, но оно пустое или не текстовое.")
        return
    # If user typed something like 'Тбилиси (12)' while in Wizard.city, FSM handlers handle; else help:
    await message.answer("Если хотите начать поиск — нажмите '🔎 Поиск' или '🟢 Быстрый подбор' в меню.", reply_markup=main_menu(current_lang(message.from_user.id)))

# ------ Analytics / Heartbeat ------
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
    if message.from_user.id != Config.ADMIN_CHAT_ID:
        return
    d = _today_str()
    c = AGG_BY_DAY[d]
    await message.answer(f"📊 Сегодня: search={c['search']}, view={c['view']}, leads={c['lead']}")

async def heartbeat():
    while True:
        try:
            logger.info("Heartbeat OK")
        except Exception:
            logger.exception("Heartbeat error")
        await asyncio.sleep(600)

# ------ Startup / Shutdown ------
async def startup():
    await rows_async(force=True)
    if Config.ADMIN_CHAT_ID:
        try:
            await bot.send_message(Config.ADMIN_CHAT_ID, "🤖 LivePlace bot started (Sheets enabled)")
        except Exception:
            pass
    asyncio.create_task(heartbeat())
    logger.info("LivePlace bot starting…")

async def shutdown():
    try:
        await bot.session.close()
    except Exception:
        pass
    logger.info("Bot shutdown complete")

# ------ Main ------
async def main():
    try:
        await startup()
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await shutdown()

if __name__ == "__main__":
    asyncio.run(main())
