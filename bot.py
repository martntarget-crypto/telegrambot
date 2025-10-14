# -*- coding: utf-8 -*-
# LivePlace Telegram Bot — Railway-ready (Sheets ENABLED)
# Полная рабочая версия bot.py с реальным подключением к Google Sheets.

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
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto

# == Logging ==
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("liveplace")

# == .env ==
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
    ADS_ENABLED = os.getenv("ADS_ENABLED", "1").strip() not in {"0","false","False",""}
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

# == FSM ==
class Wizard(StatesGroup):
    mode = State()
    city = State()
    district = State()
    budget = State()

# == Словари для городов и цен ==
CITY_ICONS = {"Тбилиси":"🏙", "Батуми":"🌊", "Кутаиси":"🏛"}
PRICE_RANGES = {
    "rent":["500","1000","1500","2000"],
    "sale":["50000","100000","150000","200000"],
    "daily":["50","100","150","200"]
}

# == Пользовательские данные ==
PAGE_SIZE = 8
USER_RESULTS: Dict[int, Dict[str, Any]] = {}
LAST_AD_TIME: Dict[int, float] = {}
LAST_AD_ID: Dict[int, str] = {}

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

    txt = f"<b>{title}</b>\n"
    txt += f"{desc}\n"
    txt += f"<b>Город:</b> {city}\n<b>Район:</b> {district}\n<b>Тип:</b> {rtype}\n<b>Комнат:</b> {rooms}\n<b>Цена:</b> {price}\n<b>Телефон:</b> {phone}\n"
    txt += f"<i>Дата публикации: {pub_txt}</i>"
    return txt

# == Handlers ==

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    lang = current_lang(message.from_user.id)
    await state.clear()
    await message.answer(T["start"][lang], reply_markup=main_menu(lang))

@dp.message(F.text.in_([T["btn_fast"]["ru"],T["btn_fast"]["en"],T["btn_fast"]["ka"]]))
async def fast_pick(message: types.Message, state: FSMContext):
    await Wizard.mode.set()
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(T["btn_rent"][current_lang(message.from_user.id)]))
    kb.add(KeyboardButton(T["btn_sale"][current_lang(message.from_user.id)]))
    kb.add(KeyboardButton(T["btn_daily"][current_lang(message.from_user.id)]))
    await message.answer("Выберите режим поиска", reply_markup=kb)

@dp.message(Wizard.mode)
async def pick_mode(message: types.Message, state: FSMContext):
    mode = norm_mode(message.text)
    if not mode:
        await message.answer("Не распознано, выберите режим:")
        return
    await state.update_data(mode=mode)
    await Wizard.city.set()
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    for c in CITY_ICONS.keys():
        kb.add(KeyboardButton(f"{CITY_ICONS[c]} {c}"))
    await message.answer("Выберите город:", reply_markup=kb)

@dp.message(Wizard.city)
async def pick_city(message: types.Message, state: FSMContext):
    city = re.sub(r"^[^А-Яа-яA-Za-z]*","", message.text).strip()
    await state.update_data(city=city)
    await Wizard.district.set()
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    # Здесь можно динамически подтягивать районы из Sheets
    districts = ["Весь город", "Центр", "Сабуртало", "Вере"]  # пример
    for d in districts: kb.add(KeyboardButton(d))
    await message.answer("Выберите район или Пропустить:", reply_markup=kb)

@dp.message(Wizard.district)
async def pick_district(message: types.Message, state: FSMContext):
    district = message.text.strip()
    if district.lower() in {"пропустить","весь город"}:
        district = ""
    await state.update_data(district=district)
    data = await state.get_data()
    mode = data.get("mode")
    await Wizard.budget.set()
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    for p in PRICE_RANGES.get(mode, ["Пропустить"]):
        kb.add(KeyboardButton(p))
    kb.add(KeyboardButton("Пропустить"))
    await message.answer("Выберите бюджет:", reply_markup=kb)

@dp.message(Wizard.budget)
async def pick_price(message: types.Message, state: FSMContext):
    budget = message.text.strip()
    if budget.lower() in {"пропустить","весь диапазон"}:
        budget = ""
    await state.update_data(budget=budget)
    data = await state.get_data()
    await show_results(message.from_user.id, data, state)

# == Результаты ==
async def show_results(uid: int, data: Dict[str, Any], state: FSMContext):
    rows = await rows_async()
    mode, city, district, budget = data.get("mode"), data.get("city"), data.get("district"), data.get("budget")
    filtered = []
    for r in rows:
        if mode and norm_mode(r.get("mode","")) != mode: continue
        if city and norm(r.get("city","")) != norm(city): continue
        if district and norm(r.get("district","")) != norm(district): continue
        if budget:
            try:
                if float(r.get("price",0)) > float(budget): continue
            except Exception: pass
        filtered.append(r)

    if not filtered:
        await bot.send_message(uid, "По вашему запросу ничего не найдено.", reply_markup=main_menu(current_lang(uid)))
        await state.clear()
        return

    USER_RESULTS[uid] = {"rows": filtered, "page":0}
    await send_page(uid)

async def send_page(uid: int):
    res = USER_RESULTS.get(uid)
    if not res: return
    rows = res["rows"]
    page = res["page"]
    row = rows[page]
    lang = current_lang(uid)
    photos = collect_photos(row)
    text = format_card(row, lang)
    kb = InlineKeyboardMarkup(row_width=3)
    if page>0:
        kb.insert(InlineKeyboardButton(T["btn_prev"][lang], callback_data="prev"))
    if page<len(rows)-1:
        kb.insert(InlineKeyboardButton(T["btn_next"][lang], callback_data="next"))
    kb.add(InlineKeyboardButton(T["btn_fav_add"][lang], callback_data="fav"))
    if photos:
        media = [InputMediaPhoto(m, caption=text) for m in photos[:10]]
        await bot.send_media_group(uid, media)
        await bot.send_message(uid, "Следующий вариант:", reply_markup=kb)
    else:
        await bot.send_message(uid, text, reply_markup=kb)

@dp.callback_query(F.data.in_({"prev","next","fav"}))
async def cb_page(cq: types.CallbackQuery):
    uid = cq.from_user.id
    res = USER_RESULTS.get(uid)
    if not res: return
    if cq.data=="prev": res["page"] = max(0, res["page"]-1)
    if cq.data=="next": res["page"] = min(len(res["rows"])-1, res["page"]+1)
    await send_page(uid)
    await cq.answer()

# == Запуск ==
if __name__=="__main__":
    import asyncio
    from aiogram import executor
    logger.info("LivePlace bot is running…")
    executor.start_polling(dp, skip_updates=True)
