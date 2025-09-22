# LivePlace Telegram Bot — FINAL v4.5.1
# (fixed dependencies + analytics + ads + reliability + media fix + i18n keyboard refresh + city/district localization)
#
# Исправлено:
# - Обновлены версии зависимостей для совместимости
# - Фикс установки aiohttp и других пакетов
# - Сохранена вся функциональность v4.5.0

import os
import re
import csv
import asyncio
import logging
import random
import time
import json
import hashlib
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
from time import monotonic
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from collections import Counter, defaultdict

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
)
os.sistem(pip install -r requirements.txt)
# ---- .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("liveplace")

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
WEEKLY_REPORT_DOW   = int(os.getenv("WEEKLY_REPORT_DOW", "1") or "1")  # 1=Mon..7=Sun
WEEKLY_REPORT_HOUR  = int(os.getenv("WEEKLY_REPORT_HOUR", "9") or "9") # UTC

if not API_TOKEN:
    raise RuntimeError("API_TOKEN is not set")

# --- Admins
ADMINS_RAW = os.getenv("ADMINS", "").strip()
ADMINS_SET = set(int(x) for x in ADMINS_RAW.split(",") if x.strip().isdigit())
if ADMIN_CHAT_ID:
    ADMINS_SET.add(ADMIN_CHAT_ID)

def is_admin(user_id: int) -> bool:
    return user_id in ADMINS_SET

# ---- Bot
bot = Bot(token=API_TOKEN, parse_mode="HTML")
dp  = Dispatcher(bot, storage=MemoryStorage())

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

OPTIONAL_L10N = {"city_en","city_ka","district_en","district_ka"}

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
    "btn_fast": {"ru": "🟢 Быстрый подбор", "en": "🟢 Quick picks", "ka": "🟢 სწრაფი არჩევანი"},
    "btn_favs": {"ru": "❤️ Избранное", "en": "❤️ Favorites", "ka": "❤️ რჩეულები"},
    "btn_home": {"ru": "🏠 Меню", "en": "🏠 Menu", "ka": "🏠 მენიუ"},
    "btn_daily": {"ru": "🕓 Посуточно 🆕", "en": "🕓 Daily rent 🆕", "ka": "🕓 დღიურად 🆕"},

    "start": {
        "ru": (
            "<b>LivePlace</b>\n👋 Привет! Я помогу подобрать <b>идеальную недвижимость в Грузии</b>.\n\n"
            "<b>Как это работает?</b>\n"
            "— Задам 3–4 простых вопроса\n"
            "— Покажу лучшие варианты с фото и телефоном владельца\n"
            "— Просто посмотреть? Жми <b>🟢 Быстрый подбор</b>\n\n"
            "Добро пожаловать и удачного поиска! 🏡"
        ),
        "en": (
            "<b>LivePlace</b>\n👋 Hi! I'll help you find <b>your ideal home in Georgia</b>.\n\n"
            "<b>How it works:</b>\n"
            "— I ask 3–4 quick questions\n"
            "— Show top options with photos and owner phone\n"
            "— Just browsing? Tap <b>🟢 Quick picks</b>\n\n"
            "Welcome and happy hunting! 🏡"
        ),
        "ka": (
            "<b>LivePlace</b>\n👋 გამარჯობა! ერთად ვიპოვოთ <b>იდეალური ბინა საქართველოში</b>.\n\n"
            "<b>როგორ მუშაობს:</b>\n"
            "— 3–4 მარტივი კითხვა\n"
            "— საუკეთესო ვარიანტები ფოტოებითა და მფლობელის ნომრით\n"
            "— უბრალოდ გადაათვალიერე? დააჭირე <b>🟢 სწრაფი არჩევანი</b>\n\n"
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

    "lead_ask": {
        "ru": "Оставьте контакт (телефон или @username), и мы свяжем вас с владельцем:",
        "en": "Leave your contact (phone or @username), we'll connect you with the owner:",
        "ka": "მოგვაწოდეთ კონტაქტი (ტელეფონი ან @username), დაგაკავშირდებით მფლობელთან:"
    },
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
    q["utm_source"]   = [UTM_SOURCE]
    q["utm_medium"]   = [UTM_MEDIUM]
    q["utm_campaign"] = [UTM_CAMPAIGN]
    q["utm_content"]  = [ad_id]
    q["token"]        = [token]
    new_q = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

# ---- Menus
def main_menu(lang: str) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(T["btn_fast"][lang]))
    kb.row(KeyboardButton(T["btn_search"][lang]), KeyboardButton(T["btn_latest"][lang]))
    kb.add(KeyboardButton(T["btn_favs"][lang]))
    kb.add(KeyboardButton(T["btn_language"][lang]), KeyboardButton(T["btn_about"][lang]))
    return kb

# ---- Auto-refresh cache
async def _auto_refresh_loop():
    while True:
        try:
            if _is_cache_stale():
                await rows_async(force=True)
                logger.info("Sheets cache refreshed")
        except Exception as e:
            logger.warning(f"Auto refresh failed: {e}")
        await asyncio.sleep(30)

# ===== Utilities / cards / favorites =====
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

PAGE_SIZE = 8
CHOICE_CACHE: Dict[int, Dict[str, List[Tuple[str, str]]]] = {}
CHOICE_MSG: Dict[int, Dict[str, int]] = {}

LEAD_COOLDOWN = 45
LAST_LEAD_AT: Dict[int, float] = {}

LAST_AD_ID: Dict[int, str] = {}

# ===== Локализация списков городов/районов =====
def _l10n_label(row: Dict[str, Any], field: str, lang: str) -> str:
    """Возвращает надпись для кнопки (локализованную) для поля city/district."""
    base = str(row.get(field, "")).strip()
    if field not in ("city", "district"):
        return base
    if lang == "ru":
        return base or ""
    alt = str(row.get(f"{field}_{lang}", "")).strip()
    return alt or base

def unique_values_l10n(rows: List[Dict[str, Any]], field: str, lang: str,
                       where: Optional[List[Tuple[str, str]]] = None) -> List[Tuple[str, str]]:
    """Собирает уникальные значения field c метками по языку: [(label, base_value)]."""
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

# ====== РЕКЛАМА ======
ADS_ENABLED        = os.getenv("ADS_ENABLED", "1").strip() not in {"0", "false", "False", ""}
ADS_PROB           = float(os.getenv("ADS_PROB", "0.18"))
ADS_COOLDOWN_SEC   = int(os.getenv("ADS_COOLDOWN_SEC", "180"))
LAST_AD_TIME: Dict[int, float] = {}

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
    if not ADS_ENABLED or not ADS:
        return False
    now = time.time()
    last = LAST_AD_TIME.get(uid, 0.0)
    if now - last < ADS_COOLDOWN_SEC:
        return False
    return random.random() < ADS_PROB

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

# ===== Guided choices =====
class Search(StatesGroup):
    mode = State()
    city = State()
    district = State()
    rtype = State()
    rooms = State()
    price = State()

USER_RESULTS: Dict[int, Dict[str, Any]] = {}
USER_FAVS: Dict[int, List[str]] = {}

# =====================  АНАЛИТИКА  =====================
def _today_str():
    return datetime.utcnow().strftime("%Y-%m-%d")

ANALYTIC_EVENTS: List[Dict[str, Any]] = []
AGG_BY_DAY = defaultdict(lambda: Counter())
AGG_BY_MODE = defaultdict(lambda: Counter())
AGG_CITY = defaultdict(lambda: Counter())
AGG_DISTRICT = defaultdict(lambda: Counter())
AGG_FUNNEL = defaultdict(lambda: Counter())
TOP_LISTINGS = defaultdict(lambda: Counter())
TOP_LIKES    = defaultdict(lambda: Counter())
TOP_FAVS     = defaultdict(lambda: Counter())

ANALYTICS_SNAPSHOT = "analytics_snapshot.json"
SNAPSHOT_INTERVAL_SEC = 120

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

# ====== Analytics snapshot (persist) ======
async def _snapshot_loop():
    while True:
        try:
            save_analytics_snapshot()
        except Exception as e:
            logger.warning(f"snapshot save failed: {e}")
        await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)

def save_analytics_snapshot():
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

# ====== Google Sheets для статистики (опционально) ======
def _open_stats_book():
    if not GSHEET_STATS_ID:
        raise RuntimeError("GSHEET_STATS_ID is not set")
    try:
        return gc.open_by_key(GSHEET_STATS_ID)
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

def push_top_to_sheet(day: str, top_n: int = 20):
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

def push_day_all(day: str):
    push_daily_to_sheet(day)
    push_top_to_sheet(day)

# ===================  /АНАЛИТИКА  ======================

async def on_startup(dp):
    try:
        await rows_async(force=True)
    except Exception as e:
        logger.warning(f"Preload failed: {e}")
    load_analytics_snapshot()
    asyncio.create_task(_auto_refresh_loop())
    asyncio.create_task(_midnight_flush_loop())
    asyncio.create_task(_weekly_report_loop())
    asyncio.create_task(_snapshot_loop())
    logger.info(f"Admin IDs loaded: {sorted(ADMINS_SET)}")

# ---- Фоновые задачи
async def _midnight_flush_loop():
    """Каждый день в 00:05 UTC пишет статистику вчерашнего дня в Google Sheets."""
    already = set()
    while True:
        try:
            now = datetime.utcnow()
            mark = now.strftime("%Y-%m-%d %H:%M")
            if now.hour == 0 and now.minute >= 5 and mark not in already:
                day = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                if GSHEET_STATS_ID:
                    try:
                        await asyncio.to_thread(push_day_all, day)
                        logger.info(f"Pushed analytics for {day}")
                    except Exception as e:
                        logger.warning(f"Push analytics failed for {day}: {e}")
                already.add(mark)
        except Exception as e:
            logger.warning(f"_midnight_flush_loop error: {e}")
        await asyncio.sleep(30)

async def _weekly_report_loop():
    """Еженедельный отчёт админу (по умолчанию пн 09:00 UTC)."""
    sent_days = set()
    while True:
        try:
            now = datetime.utcnow()
            dow = (now.isoweekday())  # 1..7
            if dow == WEEKLY_REPORT_DOW and now.hour == WEEKLY_REPORT_HOUR and now.minute < 5:
                key = now.strftime("%Y-%m-%d-%H")
                if key not in sent_days:
                    text = render_week_summary()
                    try:
                        await bot.send_message(ADMIN_CHAT_ID, text)
                    except Exception as e:
                        logger.warning(f"Weekly report send failed: {e}")
                    sent_days.add(key)
        except Exception as e:
            logger.warning(f"_weekly_report_loop error: {e}")
        await asyncio.sleep(30)

# ====== Handlers ======
@dp.message_handler(commands=["start", "menu"])
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in USER_LANG:
        code = (message.from_user.language_code or "").strip()
        USER_LANG[message.from_user.id] = LANG_MAP.get(code, "ru")
    lang = USER_LANG[message.from_user.id]
    await state.finish()
    await message.answer(t(lang, "start"), reply_markup=main_menu(lang))

@dp.message_handler(commands=["home"])
async def cmd_home(message: types.Message, state: FSMContext):
    lang = USER_LANG.get(message.from_user.id, "ru")
    await state.finish()
    await message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))

@dp.message_handler(commands=["lang_ru", "lang_en", "lang_ka"])
async def cmd_lang(message: types.Message):
    code = message.get_command().replace("/lang_", "")
    if code not in LANGS:
        code = "ru"
    USER_LANG[message.from_user.id] = code
    await message.answer(t(code, "menu_title"), reply_markup=main_menu(code))

@dp.message_handler(commands=["whoami"], state="*")
async def cmd_whoami(message: types.Message, state: FSMContext):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@dp.message_handler(commands=["admin_debug"], state="*")
async def cmd_admin_debug(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    await message.answer(
        "Debug:\n"
        f"ADMIN_CHAT_ID: <code>{ADMIN_CHAT_ID}</code>\n"
        f"ADMINS (.env): <code>{os.getenv('ADMINS','')}</code>\n"
        f"ADMINS_SET: <code>{sorted(ADMINS_SET)}</code>\n"
        f"GSHEET_STATS_ID: <code>{GSHEET_STATS_ID or '(not set)'}</code>\n"
        f"Weekly: DOW={WEEKLY_REPORT_DOW}, HOUR={WEEKLY_REPORT_HOUR} (UTC)\n"
        f"ADS: enabled={ADS_ENABLED}, prob={ADS_PROB}, cooldown={ADS_COOLDOWN_SEC}s\n"
        f"Cache rows: {len(_cached_rows)}, TTLmin={GSHEET_REFRESH_MIN}"
    )

@dp.message_handler(commands=["health"])
async def cmd_health(message: types.Message):
    try:
        sh = await asyncio.to_thread(open_spreadsheet)
        tabs = [w.title for w in sh.worksheets()]
        ws = await asyncio.to_thread(get_worksheet)
        header = ws.row_values(1)
        sample = ws.row_values(2)
        stats = f"stats_book={'set' if GSHEET_STATS_ID else 'unset'}"
        await message.answer(
            "✅ Connected\n"
            f"Tab: <b>{GSHEET_TAB}</b>\n"
            f"Tabs: {tabs}\n"
            f"Header: {header}\n"
            f"Row2: {sample}\n"
            f"{stats}\n"
            f"Cache rows: {len(_cached_rows)} (stale={_is_cache_stale()})"
        )
    except Exception as e:
        await message.answer(f"❌ {e}")

@dp.message_handler(commands=["gs"])
async def cmd_gs(message: types.Message):
    try:
        rows = await rows_async(force=True)
        await message.answer(f"GS rows: {len(rows)}")
    except Exception as e:
        await message.answer(f"GS error: {e}")

@dp.message_handler(commands=["reload", "refresh"])
async def cmd_reload(message: types.Message):
    try:
        rows = await rows_async(force=True)
        await message.answer(f"♻️ Reloaded. Rows: {len(rows)}")
    except Exception as e:
        await message.answer(f"Reload error: {e}")

# ----- ANALYTICS COMMANDS -----
@dp.message_handler(commands=["stats", "stats_today"], state="*")
async def cmd_stats(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа к статистике.")
    parts = (message.text or "").split(maxsplit=1)
    day = parts[1].strip() if len(parts) == 2 else None
    await message.answer(render_stats(day) or "Данных пока нет.")

@dp.message_handler(commands=["stats_week"], state="*")
async def cmd_stats_week(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    await message.answer(render_week_summary())

@dp.message_handler(commands=["top_today"], state="*")
async def cmd_top_today(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    day = _today_str()
    txt = [
        f"🏆 ТОП за {day}",
        "Просмотры: " + (", ".join([f"{k}:{n}" for k,n in TOP_LISTINGS[day].most_common(10)]) or "—"),
        "Лайки: " + (", ".join([f"{k}:{n}" for k,n in TOP_LIKES[day].most_common(10)]) or "—"),
        "Избранное: " + (", ".join([f"{k}:{n}" for k,n in TOP_FAVS[day].most_common(10)]) or "—"),
    ]
    await message.answer("\n".join(txt))

@dp.message_handler(commands=["top_week"], state="*")
async def cmd_top_week(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    end_dt = datetime.utcnow()
    counters = {"views": Counter(), "likes": Counter(), "favorites": Counter()}
    for i in range(7):
        d = (end_dt - timedelta(days=i)).strftime("%Y-%m-%d")
        counters["views"]      += TOP_LISTINGS[d]
        counters["likes"]      += TOP_LIKES[d]
        counters["favorites"]  += TOP_FAVS[d]
    txt = [
        "🏆 ТОП за 7 дней:",
        "Просмотры: " + (", ".join([f"{k}:{n}" for k,n in counters["views"].most_common(10)]) or "—"),
        "Лайки: " + (", ".join([f"{k}:{n}" for k,n in counters["likes"].most_common(10)]) or "—"),
        "Избранное: " + (", ".join([f"{k}:{n}" for k,n in counters["favorites"].most_common(10)]) or "—"),
    ]
    await message.answer("\n".join(txt))

@dp.message_handler(commands=["stats_push"], state="*")
async def cmd_stats_push(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    if not GSHEET_STATS_ID:
        return await message.answer("GSHEET_STATS_ID не задан. Добавь в .env и дай сервисному аккаунту права редактора.")
    parts = (message.text or "").split(maxsplit=1)
    day = parts[1].strip() if len(parts) == 2 else _today_str()
    try:
        await asyncio.to_thread(push_day_all, day)
        await message.answer(f"✅ Данные за {day} записаны в таблицу.")
    except Exception as e:
        await message.answer(f"❌ Не удалось записать: {e}")

@dp.message_handler(commands=["export_csv"], state="*")
async def cmd_export(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    path = export_analytics_csv("analytics_export.csv")
    await message.answer_document(types.InputFile(path), caption="Экспорт аналитики (CSV)")

# ====== ЯЗЫК ======
@dp.message_handler(lambda m: m.text in (T["btn_language"]["ru"], T["btn_language"]["en"], T["btn_language"]["ka"]), state="*")
async def on_language(message: types.Message, state: FSMContext):
    current = USER_LANG.get(message.from_user.id, "ru")
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
        InlineKeyboardButton(("🇷🇺 Русский" + (" ✅" if current == "ru" else "")), callback_data="lang:ru"),
        InlineKeyboardButton(("🇬🇧 English" + (" ✅" if current == "en" else "")), callback_data="lang:en"),
        InlineKeyboardButton(("🇬🇪 ქართული" + (" ✅" if current == "ka" else "")), callback_data="lang:ka"),
    )
    kb.row(InlineKeyboardButton(T["btn_home"][current], callback_data="home"))
    await message.answer(t(current, "choose_lang"), reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("lang:"), state="*")
async def cb_set_lang(c: CallbackQuery, state: FSMContext):
    code = c.data.split(":", 1)[1]
    if code not in LANGS:
        return await c.answer("Unknown language", show_alert=False)
    USER_LANG[c.from_user.id] = code
    await state.finish()
    try:
        await c.message.edit_reply_markup()
    except Exception:
        pass
    await c.message.answer(t(code, "menu_title"), reply_markup=main_menu(code))
    await c.answer("OK")

@dp.message_handler(lambda m: m.text in (T["btn_about"]["ru"], T["btn_about"]["en"], T["btn_about"]["ka"]))
async def on_about(message: types.Message):
    lang = USER_LANG.get(message.from_user.id, "ru")
    await message.answer(t(lang, "about"))

@dp.message_handler(lambda m: m.text in (T["btn_fast"]["ru"], T["btn_fast"]["en"], T["btn_fast"]["ka"]))
async def on_fast(message: types.Message):
    lang = USER_LANG.get(message.from_user.id, "ru")
    try:
        rows = await rows_async()
    except Exception as e:
        return await message.answer(f"Sheets error: {e}")
    def key_pub(r):
        try:
            return datetime.fromisoformat(str(r.get("published", "")))
        except Exception:
            return datetime.min
    rows_sorted = sorted(rows, key=key_pub, reverse=True)
    USER_RESULTS[message.from_user.id] = {"rows": rows_sorted[:30], "idx": 0, "context": {}}
    if not rows_sorted:
        return await message.answer(t(lang, "no_results"))
    await message.answer(t(lang, "results_found", n=len(rows_sorted[:30])))
    await show_current_card(message, message.from_user.id)

# ====== Поиск ======
@dp.message_handler(lambda m: m.text in (T["btn_search"]["ru"], T["btn_search"]["en"], T["btn_search"]["ka"]))
async def on_search(message: types.Message, state: FSMContext):
    lang = USER_LANG.get(message.from_user.id, "ru")
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton(T["btn_rent"][lang]), KeyboardButton(T["btn_sale"][lang]), KeyboardButton(T["btn_daily"][lang]))
    kb.add(KeyboardButton(T["btn_latest"][lang]), KeyboardButton(T["btn_fast"][lang]))
    kb.add(KeyboardButton(T["btn_language"][lang]), KeyboardButton(T["btn_home"][lang]))
    await Search.mode.set()
    await message.answer(t(lang, "wiz_intro"), reply_markup=kb)

@dp.message_handler(lambda m: m.text in (T["btn_home"]["ru"], T["btn_home"]["en"], T["btn_home"]["ka"]), state="*")
async def on_home_text(message: types.Message, state: FSMContext):
    lang = USER_LANG.get(message.from_user.id, "ru")
    await state.finish()
    await message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))

@dp.message_handler(state=Search.mode)
async def st_mode(message: types.Message, state: FSMContext):
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

    data_flag = await state.get_data()
    if data_flag.get("_city_shown"):
        return
    await state.update_data(_city_shown=True)

    rows = await rows_async()
    cities = unique_values_l10n(rows, "city", lang)
    await Search.city.set()
    await send_choice(message, lang, "city", cities, 0, t(lang, "ask_city"))

async def send_choice(message, lang: str, field: str, values: List[Tuple[str,str]], page: int, prompt: str, allow_skip=True):
    chat_id = message.chat.id if hasattr(message, "chat") else message.from_user.id
    CHOICE_CACHE.setdefault(chat_id, {})[field] = values

    kb = InlineKeyboardMarkup()
    start = page * PAGE_SIZE
    chunk = values[start:start+PAGE_SIZE]
    for idx, (label, _base) in enumerate(chunk, start=start):
        kb.add(InlineKeyboardButton(label, callback_data=f"pick:{field}:{idx}"))
    controls = []
    if start + PAGE_SIZE < len(values):
        controls.append(InlineKeyboardButton(T["btn_more"][lang], callback_data=f"more:{field}:{page+1}"))
    if allow_skip:
        controls.append(InlineKeyboardButton(T["btn_skip"][lang], callback_data=f"pick:{field}:-1"))
    if controls:
        kb.row(*controls)
    kb.row(InlineKeyboardButton(T["btn_home"][lang], callback_data="home"))
    sent = await message.answer(prompt, reply_markup=kb)
    CHOICE_MSG.setdefault(chat_id, {})[field] = sent.message_id

@dp.callback_query_handler(lambda c: c.data == "home", state="*")
async def cb_home(c: CallbackQuery, state: FSMContext):
    lang = USER_LANG.get(c.from_user.id, "ru")
    await state.finish()
    await c.message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))
    await c.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("more:"))
async def cb_more(c: CallbackQuery, state: FSMContext):
    _, field, page = c.data.split(":", 2)
    page = int(page)
    lang = USER_LANG.get(c.from_user.id, "ru")
    rows = await rows_async()
    where = []
    data = await state.get_data()
    if field == "district" and data.get("city"):
        where.append(("city", data["city"]))
    if field in ("city", "district"):
        values = unique_values_l10n(rows, field, lang, where)
    else:
        raw = []
        seen = set()
        for r in rows:
            ok = True
            if where:
                for f, val in where:
                    if norm(r.get(f)) != norm(val):
                        ok = False; break
            if not ok: 
                continue
            v = str(r.get(field, "")).strip()
            if not v or v in seen: 
                continue
            seen.add(v); raw.append((v, v))
        raw.sort(key=lambda x: x[0])
        values = raw

    kb = InlineKeyboardMarkup()
    start = page * PAGE_SIZE
    chunk = values[start:start+PAGE_SIZE]
    for idx, (label, _base) in enumerate(chunk, start=start):
        kb.add(InlineKeyboardButton(label, callback_data=f"pick:{field}:{idx}"))
    controls = []
    if start + PAGE_SIZE < len(values):
        controls.append(InlineKeyboardButton(T["btn_more"][lang], callback_data=f"more:{field}:{page+1}"))
    controls.append(InlineKeyboardButton(T["btn_skip"][lang], callback_data=f"pick:{field}:-1"))
    if controls:
        kb.row(*controls)
    kb.row(InlineKeyboardButton(T["btn_home"][lang], callback_data="home"))
    try:
        await c.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        await c.message.answer(t(lang, f"ask_{'city' if field=='city' else field}"), reply_markup=kb)
    await c.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("pick:"), state="*")
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
            await Search.district.set()
            await send_choice(c.message, lang, "district", dists, 0, t(lang, "ask_district"))
        elif field == "district":
            city_val = (await state.get_data()).get("city", "")
            filters = []
            if city_val:
                filters.append(("city", city_val))
            if value:
                filters.append(("district", value))
            types = unique_values_l10n(rows, "type", lang, filters if filters else None)
            if not types:
                seen=set(); types=[]
                for r in rows:
                    ok=True
                    for f,v in (filters or []):
                        if norm(r.get(f))!=norm(v): ok=False; break
                    if not ok: continue
                    v=str(r.get("type","")).strip()
                    if v and v not in seen:
                        seen.add(v); types.append((v,v))
                types.sort(key=lambda x:x[0])
            await Search.rtype.set()
            await send_choice(c.message, lang, "type", types, 0, t(lang, "ask_type"))
        elif field == "type":
            kb = InlineKeyboardMarkup()
            for r in ["1", "2", "3", "4", "5+"]:
                kb.add(InlineKeyboardButton(r, callback_data=f"rooms:{r}"))
            kb.row(InlineKeyboardButton(T["btn_skip"][lang], callback_data="rooms:"))
            kb.row(InlineKeyboardButton(T["btn_home"][lang], callback_data="home"))
            await Search.rooms.set()
            await c.message.answer(t(lang, "ask_rooms"), reply_markup=kb)
        elif field == "price":
            await finish_search(c.message, c.from_user.id, await state.get_data())
        await c.answer()
    except Exception as e:
        logger.exception("cb_pick failed")
        try:
            await c.answer("Ошибка, попробуйте ещё раз", show_alert=False)
        except Exception:
            pass

@dp.callback_query_handler(lambda c: c.data.startswith("rooms:"), state=Search.rooms)
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
    def price_ranges(mode: str) -> List[Tuple[str, str]]:
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

    kb = InlineKeyboardMarkup()
    for label, code in rngs:
        kb.add(InlineKeyboardButton(label, callback_data=f"price:{code}"))
    kb.row(InlineKeyboardButton(T["btn_skip"][lang], callback_data="price:"))
    kb.row(InlineKeyboardButton(T["btn_home"][lang], callback_data="home"))
    await Search.price.set()
    await c.message.answer(t(lang, "ask_price"), reply_markup=kb)
    await c.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("price:"), state=Search.price)
async def cb_price(c: CallbackQuery, state: FSMContext):
    rng = c.data.split(":",1)[1]
    if rng:
        a,b = rng.split("-")
        await state.update_data(price_min=int(a), price_max=int(b))
    await finish_search(c.message, c.from_user.id, await state.get_data())
    await state.finish()
    await c.answer()

async def finish_search(message: types.Message, user_id: int, data: Dict[str,Any]):
    lang = USER_LANG.get(user_id, "ru")
    try:
        rows = await rows_async()
    except Exception as e:
        return await message.answer(f"Sheets error: {e}")

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

    try:
        log_event("search", user_id, row=(filtered[0] if filtered else None), extra={
            "found": len(filtered),
            "mode": norm_mode((data or {}).get("mode",""))
        })
    except Exception as e:
        logger.warning(f"analytics search failed: {e}")

    USER_RESULTS[user_id] = {"rows": filtered, "idx": 0, "context": {"mode": data.get("mode","")}}
    if not filtered:
        return await message.answer(t(lang, "no_results"), reply_markup=main_menu(lang))
    await message.answer(t(lang, "results_found", n=len(filtered)))
    await show_current_card(message, user_id)

def card_kb(idx: int, total: int, lang: str, fav: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    row1 = []
    if idx > 0:
        row1.append(InlineKeyboardButton(T["btn_prev"][lang], callback_data=f"pg:{idx-1}"))
    if idx < total-1:
        row1.append(InlineKeyboardButton(T["btn_next"][lang], callback_data=f"pg:{idx+1}"))
    if row1:
        kb.row(*row1)
    kb.row(
        InlineKeyboardButton(T["btn_like"][lang], callback_data="like"),
        InlineKeyboardButton(T["btn_dislike"][lang], callback_data="dislike"),
    )
    kb.row(InlineKeyboardButton(T["btn_fav_del"][lang] if fav else T["btn_fav_add"][lang], callback_data="fav"))
    kb.row(InlineKeyboardButton(T["btn_share"][lang], switch_inline_query=""))
    kb.row(InlineKeyboardButton(T["btn_home"][lang], callback_data="home"))
    return kb

async def show_current_card(message_or_cb, user_id: int):
    lang = USER_LANG.get(user_id, "ru")

    context = USER_RESULTS.get(user_id, {}).get("context", {})
    await maybe_show_ad(message_or_cb, user_id, context)

    data = USER_RESULTS.get(user_id, {})
    rows = data.get("rows", [])
    idx  = data.get("idx", 0)
    total = len(rows)
    if not rows:
        return
    row = rows[idx]

    try:
        log_event("view", user_id, row=row)
    except Exception as e:
        logger.warning(f"analytics view failed: {e}")

    fav_keys = USER_FAVS.get(user_id, [])
    is_fav = make_row_key(row) in fav_keys

    text = format_card(row, lang)
    photos = collect_photos(row)[:10]
    kb = card_kb(idx, total, lang, is_fav)

    async def _send_with_photos(msg_obj, text: str, kb: InlineKeyboardMarkup, photos: List[str]):
        if len(photos) >= 2:
            try:
                media = []
                for i, url in enumerate(photos):
                    if i == 0:
                        if text and text.strip():
                            media.append(InputMediaPhoto(media=url, caption=text, parse_mode="HTML"))
                        else:
                            media.append(InputMediaPhoto(media=url))
                    else:
                        media.append(InputMediaPhoto(media=url))
                await msg_obj.answer_media_group(media)
                await msg_obj.answer("\u2063", reply_markup=kb)
                return
            except Exception as e:
                logger.warning(f"media_group failed: {e}")

        if len(photos) == 1:
            try:
                if text and text.strip():
                    await msg_obj.answer_photo(photos[0], caption=text, parse_mode="HTML")
                else:
                    await msg_obj.answer_photo(photos[0])
                await msg_obj.answer("\u2063", reply_markup=btn)
                return
            except Exception as e:
                logger.warning(f"single photo failed: {e}")

        if text and text.strip():
            await msg_obj.answer(text, reply_markup=kb)
        else:
            await msg_obj.answer("\u2063", reply_markup=kb)

    if isinstance(message_or_cb, CallbackQuery):
        m = message_or_cb.message
        try:
            if photos:
                await _send_with_photos(m, text, kb, photos)
            else:
                if text and text.strip():
                    await m.edit_text(text, reply_markup=kb)
                else:
                    await m.answer("\u2063", reply_markup=kb)
        except Exception:
            if photos:
                await _send_with_photos(m, text, kb, photos)
            else:
                if text and text.strip():
                    await m.answer(text, reply_markup=kb)
                else:
                    await m.answer("\u2063", reply_markup=kb)
    else:
        if photos:
            await _send_with_photos(message_or_cb, text, kb, photos)
        else:
            if text and text.strip():
                await message_or_cb.answer(text, reply_markup=kb)
            else:
                await message_or_cb.answer("\u2063", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("pg:"))
async def cb_page(c: CallbackQuery):
    idx = int(c.data.split(":")[1])
    if c.from_user.id in USER_RESULTS:
        USER_RESULTS[c.from_user.id]["idx"] = idx
    await show_current_card(c, c.from_user.id)
    await c.answer()

@dp.callback_query_handler(lambda c: c.data == "fav")
async def cb_fav(c: CallbackQuery):
    user_id = c.from_user.id
    data = USER_RESULTS.get(user_id, {})
    rows = data.get("rows", [])
    idx  = data.get("idx", 0)
    if not rows:
        return await c.answer("No data")
    row = rows[idx]
    key = make_row_key(row)
    favs = USER_FAVS.setdefault(user_id, [])
    if key in favs:
        favs.remove(key)
        try: log_event("fav_remove", user_id, row=row)
        except Exception: pass
        await c.answer(t(USER_LANG.get(user_id,"ru"), "toast_removed"))
    else:
        favs.append(key)
        try: log_event("fav_add", user_id, row=row)
        except Exception: pass
        await c.answer(t(USER_LANG.get(user_id,"ru"), "toast_saved"))
    await show_current_card(c, user_id)

@dp.callback_query_handler(lambda c: c.data == "like")
async def cb_like(c: CallbackQuery, state: FSMContext):
    user_id = c.from_user.id
    lang = USER_LANG.get(user_id, "ru")
    data = USER_RESULTS.get(user_id, {})
    rows = data.get("rows", [])
    idx  = data.get("idx", 0)
    if not rows:
        return await c.answer("No data")

    row = rows[idx]
    pre_msg = (
        f"❤️ LIKE from {c.from_user.full_name} (@{c.from_user.username or 'no_username'})\n\n" +
        format_card(row, lang)
    )
    try:
        target = FEEDBACK_CHAT_ID or ADMIN_CHAT_ID
        if target:
            await bot.send_message(chat_id=target, text=pre_msg)
    except Exception as e:
        logger.warning(f"Failed to send pre-lead: {e}")

    try: log_event("like", user_id, row=row)
    except Exception: pass

    await state.update_data(want_contact=True)
    await c.message.answer(t(lang, "lead_ask"))
    await c.answer("OK")

@dp.callback_query_handler(lambda c: c.data == "dislike")
async def cb_dislike(c: CallbackQuery):
    user_id = c.from_user.id
    dataset = USER_RESULTS.get(user_id, {})
    rows = dataset.get("rows", [])
    if not rows:
        return await c.answer("No data")
    cur_idx = dataset.get("idx", 0)
    try:
        if 0 <= cur_idx < len(rows):
            log_event("dislike", user_id, row=rows[cur_idx])
    except Exception: pass

    idx = cur_idx + 1
    if idx >= len(rows):
        await c.answer(t(USER_LANG.get(user_id,"ru"), "toast_no_more"))
        return
    USER_RESULTS[user_id]["idx"] = idx
    await show_current_card(c, user_id)
    await c.answer(t(USER_LANG.get(user_id,"ru"), "toast_next"))

@dp.message_handler(lambda m: m.text in (T["btn_favs"]["ru"], T["btn_favs"]["en"], T["btn_favs"]["ka"]), state="*")
async def on_favs(message: types.Message, state: FSMContext):
    await state.finish()
    lang = USER_LANG.get(message.from_user.id, "ru")
    favs = set(USER_FAVS.get(message.from_user.id, []))
    if not favs:
        return await message.answer("Пока нет избранного.", reply_markup=main_menu(lang))
    rows = await rows_async()
    picked = [r for r in rows if make_row_key(r) in favs]
    if not picked:
        return await message.answer("Пока нет избранного.", reply_markup=main_menu(lang))
    USER_RESULTS[message.from_user.id] = {"rows": picked, "idx": 0, "context": {}}
    await message.answer(f"Избранное: {len(picked)}")
    await show_current_card(message, message.from_user.id)

# =====================  АДМИН: РЕКЛАМА =====================
@dp.message_handler(commands=["ads_on"])
async def ads_on(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    global ADS_ENABLED
    ADS_ENABLED = True
    await message.answer("✅ Реклама включена")

@dp.message_handler(commands=["ads_off"])
async def ads_off(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    global ADS_ENABLED
    ADS_ENABLED = False
    await message.answer("⛔ Реклама выключена")

@dp.message_handler(commands=["ads_prob"])
async def ads_prob(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    global ADS_PROB
    try:
        val = float(message.get_args())
        if 0 <= val <= 1:
            ADS_PROB = val
            await message.answer(f"🔄 Вероятность показа рекламы обновлена: {val*100:.0f}%")
        else:
            await message.answer("⚠ Укажи число от 0 до 1 (например, 0.25)")
    except Exception:
        await message.answer("❌ Использование: /ads_prob 0.25")

@dp.message_handler(commands=["ads_cooldown"])
async def ads_cooldown(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    global ADS_COOLDOWN_SEC
    try:
        val = int(message.get_args())
        ADS_COOLDOWN_SEC = val
        await message.answer(f"⏱ Кулдаун показа рекламы обновлён: {val} сек.")
    except Exception:
        await message.answer("❌ Использование: /ads_cooldown 300")

@dp.message_handler(commands=["ads_test"])
async def ads_test(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    ad = random.choice(ADS) if ADS else None
    if not ad:
        return await message.answer("Нет креативов ADS.")
    lang = current_lang_for(message.from_user.id)
    txt = ad.get(f"text_{lang}") or ad.get("text_ru") or "LivePlace"
    url = build_utm_url(ad.get("url"), ad.get("id", "ad"), message.from_user.id)
    btn = InlineKeyboardMarkup().add(InlineKeyboardButton(cta_text(lang), url=url))
    if ad.get("photo"):
        try:
            await message.answer_photo(ad["photo"], caption=txt, reply_markup=btn)
        except Exception:
            await message.answer(txt, reply_markup=btn)
    else:
        await message.answer(txt, reply_markup=btn)
    await message.answer("🧪 Тестовая реклама отправлена.")

@dp.message_handler(commands=["ads_stats"])
async def ads_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    txt = ["📊 Статистика рекламы:"]
    total = 0
    for day, data in sorted(AGG_BY_DAY.items()):
        cnt = data.get("ad_show", 0)
        if cnt:
            txt.append(f"{day}: {cnt}")
            total += cnt
    txt.append(f"ИТОГО: {total}")
    await message.answer("\n".join(txt))

# ----- Общий обработчик НЕ-команд -----
@dp.message_handler(lambda m: not ((m.text or "").startswith("/")) and not m.from_user.is_bot, state="*")
async def any_text(message: types.Message, state: FSMContext):
    data = await state.get_data()

    if data.get("want_contact"):
        contact = (message.text or "").strip()
        user = message.from_user
        lang = USER_LANG.get(user.id, "ru")

        is_phone = re.fullmatch(r"\+?\d[\d\-\s]{7,}", contact or "") is not None
        is_username = (contact or "").startswith("@") and len(contact) >= 5
        now = time.time()
        last = LAST_LEAD_AT.get(user.id, 0.0)
        if not (is_phone or is_username):
            return await message.answer(t(lang, "lead_invalid"))
        if now - last < LEAD_COOLDOWN:
            return await message.answer(t(lang, "lead_too_soon"))

        try:
            dataset = USER_RESULTS.get(user.id, {})
            rows = dataset.get("rows", [])
            idx  = dataset.get("idx", 0)
            row = rows[idx] if rows else None

            lead_msg = (
                f"📩 Lead from {user.full_name} (@{user.username or 'no_username'})\n"
                f"Contact: {contact}\n\n" +
                (format_card(row, lang) if row else "(no current listing)")
            )
            target = FEEDBACK_CHAT_ID or ADMIN_CHAT_ID
            if target:
                await bot.send_message(chat_id=target, text=lead_msg)
            try:
                if row:
                    log_event("lead", user.id, row=row, extra={"contact": contact})
            except Exception:
                pass
            LAST_LEAD_AT[user.id] = now
        except Exception as e:
            logger.warning(f"Lead send failed: {e}")

        await state.update_data(want_contact=False)
        return await message.answer(t(lang, "lead_ok"), reply_markup=main_menu(lang))

    KNOWN = {
        T["btn_fast"]["ru"], T["btn_fast"]["en"], T["btn_fast"]["ka"],
        T["btn_search"]["ru"], T["btn_search"]["en"], T["btn_search"]["ka"],
        T["btn_latest"]["ru"], T["btn_latest"]["en"], T["btn_latest"]["ka"],
        T["btn_favs"]["ru"], T["btn_favs"]["en"], T["btn_favs"]["ka"],
        T["btn_language"]["ru"], T["btn_language"]["en"], T["btn_language"]["ka"],
        T["btn_about"]["ru"], T["btn_about"]["en"], T["btn_about"]["ka"],
        T["btn_home"]["ru"], T["btn_home"]["en"], T["btn_home"]["ka"],
        T["btn_daily"]["ru"], T["btn_daily"]["en"], T["btn_daily"]["ka"],
    }
    if (message.text or "") in KNOWN:
        return

    lang = USER_LANG.get(message.from_user.id, "ru")
    await message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))

# ---- Run
if __name__ == "__main__":
    logger.info("LivePlace bot is running…")
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)

