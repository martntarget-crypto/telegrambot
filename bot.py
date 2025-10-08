#!/usr/bin/env python3
import logging
import os
import sys
import asyncio
import signal
import gspread
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import random
import time
import re
from urllib.parse import urlencode
import subprocess
import socket
import psutil

from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, CallbackQuery
)
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# ===== СИНГЛТОН ПРОВЕРКА =====
def check_singleton():
    """Проверка, что бот не запущен в другом процессе"""
    try:
        # Создаем socket lock
        lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        lock_socket.bind('\0' + 'telegram_bot_martntarget_lock')
        print("✅ Singleton check passed - bot can start")
        return True
    except socket.error:
        print("❌ Another instance of bot is already running!")
        print("💡 If you're sure no other bot is running, try:")
        print("   - Restarting the server")
        print("   - Checking running processes: ps aux | grep python")
        return False

if not check_singleton():
    sys.exit(1)

# ===== ЗАГРУЗКА КОНФИГУРАЦИИ =====
load_dotenv()

# Telegram настройки
API_TOKEN = os.getenv('API_TOKEN')
if not API_TOKEN:
    print("❌ API_TOKEN не установлен в переменных окружения")
    sys.exit(1)

try:
    ADMIN_CHAT_ID = int(os.getenv('ADMIN_CHAT_ID', '0'))
    FEEDBACK_CHAT_ID = int(os.getenv('FEEDBACK_CHAT_ID', '0'))
except (ValueError, TypeError):
    ADMIN_CHAT_ID = 0
    FEEDBACK_CHAT_ID = 0

# Google Sheets настройки
GSHEET_ID = os.getenv('GSHEET_ID')
GSHEET_TAB = os.getenv('GSHEET_TAB', 'Ads')
try:
    GSHEET_REFRESH_MIN = int(os.getenv('GSHEET_REFRESH_MIN', '2'))
except (ValueError, TypeError):
    GSHEET_REFRESH_MIN = 2

# Настройки рекламы
ADS_ENABLED = os.getenv('ADS_ENABLED', '0') == '1'
try:
    ADS_PROB = float(os.getenv('ADS_PROB', '0.18'))
    ADS_COOLDOWN_SEC = int(os.getenv('ADS_COOLDOWN_SEC', '180'))
except (ValueError, TypeError):
    ADS_PROB = 0.18
    ADS_COOLDOWN_SEC = 180

# UTM метки
UTM_SOURCE = os.getenv('UTM_SOURCE', 'telegram')
UTM_MEDIUM = os.getenv('UTM_MEDIUM', 'bot')
UTM_CAMPAIGN = os.getenv('UTM_CAMPAIGN', 'bot_ads')

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# ===== МНОГОЯЗЫЧНЫЕ ТЕКСТЫ =====
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
}

# ===== КЛАССЫ СОСТОЯНИЙ =====
class Search(StatesGroup):
    mode = State()
    city = State()
    district = State()
    rtype = State()
    rooms = State()
    price = State()

class UserStates(StatesGroup):
    waiting_for_feedback = State()
    waiting_for_contact = State()

# ===== УТИЛИТЫ =====
def t(lang: str, key: str, **kwargs) -> str:
    """Получение перевода по ключу"""
    lang = lang if lang in ["ru", "en", "ka"] else "ru"
    val = T.get(key, {}).get(lang, T.get(key, {}).get("ru", key))
    if kwargs:
        try:
            return val.format(**kwargs)
        except Exception:
            return val
    return val

def norm(s: str) -> str:
    """Нормализация строки"""
    return (s or "").strip().lower()

def norm_mode(v: str) -> str:
    """Нормализация режима"""
    s = norm(v)
    if s in {"rent","аренда","long","long-term","долгосрочно","longterm"}:
        return "rent"
    if s in {"sale","продажа","buy","sell"}:
        return "sale"
    if s in {"daily","посуточно","sutki","сутки","short","short-term","shortterm","day","day-to-day"}:
        return "daily"
    return ""

def drive_direct(url: str) -> str:
    """Конвертация Google Drive ссылки в прямую"""
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
    """Проверка, что ссылка ведет на изображение"""
    u = (url or "").strip().lower()
    if not u:
        return False
    if any(u.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
        return True
    if "google.com/uc?export=download" in u or "googleusercontent.com" in u:
        return True
    return False

def collect_photos(row: Dict[str, Any]) -> List[str]:
    """Сбор фотографий из строки"""
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
    """Парсинг количества комнат"""
    s = str(v or "").strip().lower()
    if s in {"студия","studio","stud","სტუდიო"}:
        return 0.5
    try:
        return float(s.replace("+", ""))
    except Exception:
        return -1.0

def format_card(row: Dict[str, Any], lang: str) -> str:
    """Форматирование карточки объявления"""
    city = str(row.get("city", "")).strip()
    district = str(row.get("district", "")).strip()
    rtype = str(row.get("type", "")).strip()
    rooms = str(row.get("rooms", "")).strip()
    price = str(row.get("price", "")).strip()
    published = str(row.get("published", "")).strip()
    phone = str(row.get("phone", "")).strip()
    
    # Многоязычные поля
    title_key = f"title_{lang}"
    desc_key = f"description_{lang}"
    
    title = str(row.get(title_key, "")).strip()
    desc = str(row.get(desc_key, "")).strip()

    # Форматирование даты
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

def build_utm_url(raw: str, ad_id: str, uid: int) -> str:
    """Построение UTM-ссылки"""
    if not raw:
        return "https://liveplace.com.ge/"
    
    # Добавляем UTM параметры
    if "?" in raw:
        url = f"{raw}&utm_source={UTM_SOURCE}&utm_medium={UTM_MEDIUM}&utm_campaign={UTM_CAMPAIGN}&utm_content={ad_id}"
    else:
        url = f"{raw}?utm_source={UTM_SOURCE}&utm_medium={UTM_MEDIUM}&utm_campaign={UTM_CAMPAIGN}&utm_content={ad_id}"
    
    return url

def main_menu(lang: str) -> ReplyKeyboardMarkup:
    """Главное меню"""
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(lang, "btn_fast"))],
            [
                KeyboardButton(text=t(lang, "btn_search")), 
                KeyboardButton(text=t(lang, "btn_latest"))
            ],
            [KeyboardButton(text=t(lang, "btn_favs"))],
            [
                KeyboardButton(text=t(lang, "btn_language")), 
                KeyboardButton(text=t(lang, "btn_about"))
            ]
        ],
        resize_keyboard=True
    )
    return kb

# ===== ОСНОВНОЙ КЛАСС БОТА =====
class TelegramAdsBot:
    """Продвинутый бот для управления недвижимостью с Google Sheets"""
    
    def __init__(self):
        self._setup_logging()
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.router: Optional[Router] = None
        self.storage: Optional[MemoryStorage] = None
        self.gc: Optional[gspread.Client] = None
        self.ads_sheet = None
        self.is_running = False
        self.start_time = None
        
        # Кэши данных
        self.properties_cache = []
        self.ads_cache = []
        self.last_cache_update = None
        
        # Пользовательские данные
        self.user_lang = {}
        self.user_results = {}
        self.user_favs = {}
        self.user_last_ad = {}
        
    def _setup_logging(self):
        """Настройка системы логирования"""
        try:
            log_level = getattr(logging, LOG_LEVEL, logging.INFO)
            
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            logger = logging.getLogger()
            logger.setLevel(log_level)
            
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            
            file_handler = logging.FileHandler('bot.log', encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            self.logger = logging.getLogger(__name__)
            self.logger.info("✅ Логгер инициализирован успешно")
            
        except Exception as e:
            print(f"❌ Критическая ошибка инициализации логгера: {e}")
            sys.exit(1)
    
    def _kill_old_instances(self):
        """Убиваем старые процессы бота"""
        try:
            self.logger.info("🔫 Проверяем старые процессы бота...")
            
            current_pid = os.getpid()
            killed_count = 0
            
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    # Ищем процессы Python с нашим скриптом
                    if (proc.info['pid'] != current_pid and 
                        'python' in proc.info['name'].lower() and 
                        proc.info['cmdline'] and 
                        any('bot.py' in cmd for cmd in proc.info['cmdline'])):
                        
                        self.logger.info(f"🔄 Завершаем процесс {proc.info['pid']}")
                        proc.terminate()
                        killed_count += 1
                        
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
            
            if killed_count > 0:
                self.logger.info(f"✅ Завершено {killed_count} старых процессов")
                time.sleep(2)  # Даем время на завершение
            else:
                self.logger.info("✅ Старых процессов не найдено")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при завершении процессов: {e}")
    
    def _validate_config(self):
        """Проверка конфигурации"""
        if not API_TOKEN:
            self.logger.error("❌ API_TOKEN не установлен")
            return False
            
        if not ADMIN_CHAT_ID:
            self.logger.warning("⚠️ ADMIN_CHAT_ID не установлен, админские функции недоступны")
            
        if not FEEDBACK_CHAT_ID:
            self.logger.warning("⚠️ FEEDBACK_CHAT_ID не установлен, лиды не будут пересылаться")
            
        if not GSHEET_ID:
            self.logger.warning("⚠️ GSHEET_ID не установлен, работа с Google Sheets отключена")
            
        self.logger.info("✅ Конфигурация проверена успешно")
        return True
    
    def _setup_signal_handlers(self):
        """Настройка обработчиков сигналов для graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"📞 Получен сигнал {signum}, завершаем работу...")
            asyncio.create_task(self._safe_shutdown())
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _setup_google_sheets(self):
        """Настройка подключения к Google Sheets"""
        try:
            if not GSHEET_ID:
                self.logger.warning("⚠️ GSHEET_ID не указан, пропускаем настройку Google Sheets")
                return True
                
            # Используем service account из переменных окружения
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            
            # Попробуем получить credentials из env
            creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if creds_json:
                import json
                creds_dict = json.loads(creds_json)
                creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            else:
                # Или из файла
                creds_file = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
                if not os.path.exists(creds_file):
                    self.logger.warning(f"⚠️ Файл {creds_file} не найден, Google Sheets отключен")
                    return False
                creds = Credentials.from_service_account_file(creds_file, scopes=scope)
            
            self.gc = gspread.authorize(creds)
            
            # Открываем основную таблицу
            self.ads_sheet = self.gc.open_by_key(GSHEET_ID).worksheet(GSHEET_TAB)
            self.logger.info(f"✅ Подключение к Google Sheets установлено (лист: {GSHEET_TAB})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка настройки Google Sheets: {e}")
            return False
    
    def _refresh_cache(self):
        """Обновление кэша объявлений"""
        try:
            if not self.ads_sheet:
                self.logger.warning("⚠️ Google Sheets не настроен, пропускаем обновление кэша")
                return
                
            # Проверяем, нужно ли обновлять кэш
            if (self.last_cache_update and 
                datetime.now() - self.last_cache_update < timedelta(minutes=GSHEET_REFRESH_MIN)):
                return
            
            self.logger.info("🔄 Обновляем кэш данных из Google Sheets...")
            records = self.ads_sheet.get_all_records()
            
            # Временная отладка: выводим первые 2 записи
            if records and len(records) > 0:
                self.logger.info(f"📊 Первые 2 записи из таблицы: {records[:2]}")
            
            # Разделяем на свойства и рекламные объявления
            self.properties_cache = []
            self.ads_cache = []
            
            for record in records:
                # Для свойств: активные объявления
                active_status = str(record.get('active', '')).strip().lower()
                if active_status in ['1', 'true', 'yes', 'да', 'active']:
                    self.properties_cache.append(record)
                
                # Для рекламы: активные рекламные объявления  
                ad_active_status = str(record.get('ad_active', '')).strip().lower()
                if ad_active_status in ['1', 'true', 'yes', 'да', 'active']:
                    self.ads_cache.append(record)
            
            self.last_cache_update = datetime.now()
            self.logger.info(f"✅ Кэш обновлен: {len(self.properties_cache)} свойств, {len(self.ads_cache)} рекламных объявлений")
            
            # Если свойств нет, попробуем загрузить все записи для отладки
            if len(self.properties_cache) == 0 and len(records) > 0:
                self.logger.warning("⚠️ Нет активных свойств, загружаем все записи для отладки")
                self.properties_cache = records[:10]  # Берем первые 10 для теста
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления кэша: {e}")
    
    def _get_random_ad(self):
        """Получение случайного рекламного объявления"""
        if not self.ads_cache:
            return None
        return random.choice(self.ads_cache)
    
    async def _send_ad_to_user(self, user_id: int, user_name: str):
        """Отправка рекламного объявления пользователю"""
        try:
            ad = self._get_random_ad()
            if not ad:
                return False
            
            lang = self.user_lang.get(user_id, "ru")
            text = ad.get(f'text_{lang}', '') or ad.get('text_ru', '')
            image_url = ad.get('image', '')
            button_text = ad.get('button_text', '')
            button_url = ad.get('button_url', '')
            
            if button_url:
                button_url = build_utm_url(button_url, ad.get('id', 'ad'), user_id)
            
            keyboard = None
            if button_text and button_url:
                cta_text = "👉 Подробнее" if lang == "ru" else "👉 Learn more" if lang == "en" else "👉 დაწვრილებით"
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=cta_text, url=button_url)]
                ])
            
            if image_url and image_url.strip():
                try:
                    await self.bot.send_photo(
                        user_id,
                        image_url,
                        caption=text,
                        reply_markup=keyboard
                    )
                except Exception as e:
                    self.logger.warning(f"⚠️ Не удалось отправить изображение, отправляем текст: {e}")
                    await self.bot.send_message(
                        user_id,
                        text,
                        reply_markup=keyboard
                    )
            else:
                await self.bot.send_message(
                    user_id,
                    text,
                    reply_markup=keyboard
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки рекламы пользователю {user_id}: {e}")
            return False
    
    def _get_user_lang(self, user_id: int) -> str:
        """Получение языка пользователя"""
        return self.user_lang.get(user_id, "ru")
    
    def _unique_values(self, field: str, where: List[tuple] = None) -> List[str]:
        """Получение уникальных значений поля"""
        out = []
        seen = set()
        for r in self.properties_cache:
            ok = True
            if where:
                for f, val in where:
                    if norm(r.get(f, "")) != norm(val):
                        ok = False
                        break
            if not ok:
                continue
            v = str(r.get(field, "")).strip()
            if not v or v in seen:
                continue
            seen.add(v)
            out.append(v)
        out.sort()
        return out
    
    def _filter_properties(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Фильтрация свойств по критериям"""
        filtered = []
        for r in self.properties_cache:
            # Фильтр по режиму
            if filters.get("mode") and norm_mode(r.get("mode", "")) != norm_mode(filters["mode"]):
                continue
                
            # Фильтр по городу
            if filters.get("city") and norm(r.get("city", "")) != norm(filters["city"]):
                continue
                
            # Фильтр по району
            if filters.get("district") and norm(r.get("district", "")) != norm(filters["district"]):
                continue
                
            # Фильтр по типу
            if filters.get("type") and norm(r.get("type", "")) != norm(filters["type"]):
                continue
                
            # Фильтр по комнатам
            if filters.get("rooms_min") is not None:
                try:
                    rr = parse_rooms(r.get("rooms", ""))
                    if rr < filters["rooms_min"]:
                        continue
                except Exception:
                    continue
                    
            # Фильтр по цене
            if filters.get("price_min") is not None:
                try:
                    price = float(r.get("price", 0) or 0)
                    if price < filters["price_min"] or (filters.get("price_max") and price > filters["price_max"]):
                        continue
                except Exception:
                    continue
                    
            filtered.append(r)
            
        return filtered
    
    async def _create_bot_instance(self) -> bool:
        """Создание экземпляра бота с проверкой доступности"""
        try:
            self.logger.info("🤖 Создаем экземпляр бота...")
            
            self.bot = Bot(
                token=API_TOKEN,
                default=DefaultBotProperties(parse_mode=ParseMode.HTML)
            )
            
            me = await self.bot.get_me()
            self.logger.info(f"✅ Бот @{me.username} успешно создан и доступен")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания бота: {e}")
            return False
    
    def _create_dispatcher(self):
        """Создание диспетчера и хранилища"""
        try:
            self.storage = MemoryStorage()
            self.dp = Dispatcher(storage=self.storage)
            self.router = Router()
            self.dp.include_router(self.router)
            self.logger.info("✅ Диспетчер и роутер созданы")
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания диспетчера: {e}")
            raise
    
    def _setup_handlers(self):
        """Настройка обработчиков команд"""
        try:
            # Команда /start
            @self.router.message(CommandStart())
            async def handle_start(message: Message, state: FSMContext):
                try:
                    user = message.from_user
                    if user.id not in self.user_lang:
                        self.user_lang[user.id] = "ru"
                    
                    lang = self._get_user_lang(user.id)
                    await state.finish()
                    
                    # Обновляем кэш
                    self._refresh_cache()
                    
                    # Проверяем и отправляем рекламу если нужно
                    should_send_ad = False
                    if ADS_ENABLED:
                        now = time.time()
                        last_ad_time = self.user_last_ad.get(user.id, 0)
                        if now - last_ad_time > ADS_COOLDOWN_SEC and random.random() < ADS_PROB:
                            should_send_ad = True
                            self.user_last_ad[user.id] = now
                    
                    await message.answer(t(lang, "start"), reply_markup=main_menu(lang))
                    
                    # Отправляем рекламу после приветствия
                    if should_send_ad:
                        await asyncio.sleep(1)
                        await self._send_ad_to_user(user.id, user.first_name)
                        
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /start: {e}")
            
            # Команда /menu
            @self.router.message(Command("menu"))
            async def handle_menu(message: Message, state: FSMContext):
                lang = self._get_user_lang(message.from_user.id)
                await state.finish()
                await message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))
            
            # Кнопка языка
            @self.router.message(F.text.in_([t("ru", "btn_language"), t("en", "btn_language"), t("ka", "btn_language")]))
            async def handle_language(message: Message):
                user_id = message.from_user.id
                current_lang = self._get_user_lang(user_id)
                
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            "🇷🇺 Русский" + (" ✅" if current_lang == "ru" else ""), 
                            callback_data="lang:ru"
                        ),
                        InlineKeyboardButton(
                            "🇬🇧 English" + (" ✅" if current_lang == "en" else ""), 
                            callback_data="lang:en"
                        ),
                        InlineKeyboardButton(
                            "🇬🇪 ქართული" + (" ✅" if current_lang == "ka" else ""), 
                            callback_data="lang:ka"
                        )
                    ],
                    [InlineKeyboardButton(t(current_lang, "btn_home"), callback_data="home")]
                ])
                
                await message.answer(t(current_lang, "choose_lang"), reply_markup=kb)
            
            # Смена языка
            @self.router.callback_query(F.data.startswith("lang:"))
            async def handle_lang_callback(callback: CallbackQuery):
                lang = callback.data.split(":")[1]
                if lang not in ["ru", "en", "ka"]:
                    lang = "ru"
                
                self.user_lang[callback.from_user.id] = lang
                await callback.message.edit_text(t(lang, "menu_title"))
                await callback.message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))
                await callback.answer()
            
            # Кнопка "Быстрый подбор"
            @self.router.message(F.text.in_([t("ru", "btn_fast"), t("en", "btn_fast"), t("ka", "btn_fast")]))
            async def handle_fast_search(message: Message):
                user_id = message.from_user.id
                lang = self._get_user_lang(user_id)
                
                # Обновляем кэш
                self._refresh_cache()
                
                if not self.properties_cache:
                    await message.answer(t(lang, "no_results"))
                    return
                
                # Сортируем по дате публикации (новые first)
                def get_pub_date(row):
                    try:
                        pub_date = row.get("published", "")
                        if isinstance(pub_date, str):
                            return datetime.fromisoformat(pub_date)
                        return datetime.min
                    except:
                        return datetime.min
                
                sorted_properties = sorted(self.properties_cache, key=get_pub_date, reverse=True)[:30]
                
                self.user_results[user_id] = {
                    "rows": sorted_properties,
                    "idx": 0,
                    "context": {"mode": "fast"}
                }
                
                await message.answer(t(lang, "results_found", n=len(sorted_properties)))
                await self._show_current_card(message, user_id)
            
            # Кнопка "Поиск"
            @self.router.message(F.text.in_([t("ru", "btn_search"), t("en", "btn_search"), t("ka", "btn_search")]))
            async def handle_search(message: Message, state: FSMContext):
                lang = self._get_user_lang(message.from_user.id)
                
                kb = ReplyKeyboardMarkup(
                    keyboard=[
                        [
                            KeyboardButton(text=t(lang, "btn_rent")),
                            KeyboardButton(text=t(lang, "btn_sale")),
                            KeyboardButton(text=t(lang, "btn_daily"))
                        ],
                        [
                            KeyboardButton(text=t(lang, "btn_latest")),
                            KeyboardButton(text=t(lang, "btn_fast"))
                        ],
                        [
                            KeyboardButton(text=t(lang, "btn_language")),
                            KeyboardButton(text=t(lang, "btn_home"))
                        ]
                    ],
                    resize_keyboard=True
                )
                
                await Search.mode.set()
                await message.answer(t(lang, "wiz_intro"), reply_markup=kb)
            
            # Начало поиска - выбор режима
            @self.router.message(Search.mode)
            async def handle_search_mode(message: Message, state: FSMContext):
                lang = self._get_user_lang(message.from_user.id)
                text = message.text or ""
                
                # Определяем выбранный режим
                mode_map = {
                    t(lang, "btn_rent"): "rent",
                    t(lang, "btn_sale"): "sale", 
                    t(lang, "btn_daily"): "daily"
                }
                
                picked_mode = mode_map.get(text, "")
                if not picked_mode:
                    await message.answer("Пожалуйста, выберите режим из кнопок")
                    return
                
                await state.update_data(mode=picked_mode)
                
                # Получаем города для выбранного режима
                cities = self._unique_values("city", [("mode", picked_mode)])
                
                if not cities:
                    await message.answer("Нет доступных городов для выбранного режима")
                    await state.finish()
                    return
                
                await Search.city.set()
                await self._send_choice(message, lang, "city", cities, 0, t(lang, "ask_city"))
            
            # Отправка выбора
            async def _send_choice(message, lang: str, field: str, values: List[str], page: int, prompt: str):
                PAGE_SIZE = 8
                start = page * PAGE_SIZE
                chunk = values[start:start + PAGE_SIZE]
                
                kb = InlineKeyboardMarkup()
                for idx, value in enumerate(chunk):
                    kb.add(InlineKeyboardButton(value, callback_data=f"choice:{field}:{start + idx}"))
                
                buttons = []
                if start + PAGE_SIZE < len(values):
                    buttons.append(InlineKeyboardButton(t(lang, "btn_more"), callback_data=f"more:{field}:{page + 1}"))
                buttons.append(InlineKeyboardButton(t(lang, "btn_skip"), callback_data=f"choice:{field}:-1"))
                
                if buttons:
                    kb.row(*buttons)
                
                kb.row(InlineKeyboardButton(t(lang, "btn_home"), callback_data="home"))
                
                await message.answer(prompt, reply_markup=kb)
            
            # Обработка выбора
            @self.router.callback_query(F.data.startswith("choice:"))
            async def handle_choice(callback: CallbackQuery, state: FSMContext):
                try:
                    _, field, idx_str = callback.data.split(":")
                    idx = int(idx_str)
                    user_id = callback.from_user.id
                    lang = self._get_user_lang(user_id)
                    
                    current_data = await state.get_data()
                    
                    if idx >= 0:
                        # Получаем значения для поля
                        if field == "city":
                            values = self._unique_values("city", [("mode", current_data.get("mode"))])
                        elif field == "district":
                            values = self._unique_values("district", [
                                ("mode", current_data.get("mode")),
                                ("city", current_data.get("city"))
                            ])
                        elif field == "type":
                            filters = [("mode", current_data.get("mode"))]
                            if current_data.get("city"):
                                filters.append(("city", current_data.get("city")))
                            if current_data.get("district"):
                                filters.append(("district", current_data.get("district")))
                            values = self._unique_values("type", filters)
                        else:
                            values = []
                        
                        if 0 <= idx < len(values):
                            await state.update_data(**{field: values[idx]})
                    
                    # Переходим к следующему шагу
                    if field == "city":
                        # Районы для выбранного города
                        districts = self._unique_values("district", [
                            ("mode", current_data.get("mode")),
                            ("city", current_data.get("city"))
                        ])
                        await Search.district.set()
                        await callback.message.edit_text(t(lang, "ask_district"))
                        await self._send_choice(callback.message, lang, "district", districts, 0, t(lang, "ask_district"))
                    
                    elif field == "district":
                        # Типы недвижимости
                        types = self._unique_values("type", [
                            ("mode", current_data.get("mode")),
                            ("city", current_data.get("city")),
                            ("district", current_data.get("district"))
                        ])
                        await Search.rtype.set()
                        await callback.message.edit_text(t(lang, "ask_type"))
                        await self._send_choice(callback.message, lang, "type", types, 0, t(lang, "ask_type"))
                    
                    elif field == "type":
                        # Количество комнат
                        kb = InlineKeyboardMarkup()
                        for rooms in ["1", "2", "3", "4", "5+"]:
                            kb.add(InlineKeyboardButton(rooms, callback_data=f"rooms:{rooms}"))
                        kb.row(InlineKeyboardButton(t(lang, "btn_skip"), callback_data="rooms:skip"))
                        kb.row(InlineKeyboardButton(t(lang, "btn_home"), callback_data="home"))
                        
                        await Search.rooms.set()
                        await callback.message.edit_text(t(lang, "ask_rooms"), reply_markup=kb)
                    
                    await callback.answer()
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки выбора: {e}")
                    await callback.answer("Ошибка, попробуйте снова")
            
            # Обработка выбора комнат
            @self.router.callback_query(F.data.startswith("rooms:"), Search.rooms)
            async def handle_rooms(callback: CallbackQuery, state: FSMContext):
                rooms_val = callback.data.split(":")[1]
                lang = self._get_user_lang(callback.from_user.id)
                
                if rooms_val != "skip":
                    await state.update_data(rooms=rooms_val)
                
                # Переходим к бюджету
                current_data = await state.get_data()
                mode = current_data.get("mode", "rent")
                
                # Определяем диапазоны цен в зависимости от режима
                if mode == "sale":
                    price_ranges = [
                        ("до 40,000$", "0-40000"),
                        ("40,000-50,000$", "40000-50000"),
                        ("50,000-70,000$", "50000-70000"), 
                        ("70,000-90,000$", "70000-90000"),
                        ("100,000-150,000$", "100000-150000"),
                        ("от 150,000$", "150000-99999999")
                    ]
                else:
                    price_ranges = [
                        ("до 500$", "0-500"),
                        ("500-800$", "500-800"),
                        ("800-1200$", "800-1200"),
                        ("1200-2000$", "1200-2000"),
                        ("от 2000$", "2000-999999")
                    ]
                
                kb = InlineKeyboardMarkup()
                for label, range_val in price_ranges:
                    kb.add(InlineKeyboardButton(label, callback_data=f"price:{range_val}"))
                kb.row(InlineKeyboardButton(t(lang, "btn_skip"), callback_data="price:skip"))
                kb.row(InlineKeyboardButton(t(lang, "btn_home"), callback_data="home"))
                
                await Search.price.set()
                await callback.message.edit_text(t(lang, "ask_price"), reply_markup=kb)
                await callback.answer()
            
            # Обработка выбора цены
            @self.router.callback_query(F.data.startswith("price:"), Search.price)
            async def handle_price(callback: CallbackQuery, state: FSMContext):
                price_range = callback.data.split(":")[1]
                user_id = callback.from_user.id
                lang = self._get_user_lang(user_id)
                
                search_data = await state.get_data()
                
                # Применяем фильтры
                filters = {"mode": search_data.get("mode")}
                
                if search_data.get("city"):
                    filters["city"] = search_data.get("city")
                if search_data.get("district"):
                    filters["district"] = search_data.get("district") 
                if search_data.get("type"):
                    filters["type"] = search_data.get("type")
                if search_data.get("rooms") and search_data.get("rooms") != "skip":
                    filters["rooms_min"] = float(search_data.get("rooms").replace("+", ""))
                
                if price_range != "skip":
                    min_price, max_price = map(int, price_range.split("-"))
                    filters["price_min"] = min_price
                    filters["price_max"] = max_price
                
                # Фильтруем свойства
                filtered = self._filter_properties(filters)
                
                if not filtered:
                    await callback.message.edit_text(t(lang, "no_results"))
                    await state.finish()
                    return
                
                self.user_results[user_id] = {
                    "rows": filtered,
                    "idx": 0,
                    "context": {"mode": search_data.get("mode")}
                }
                
                await callback.message.edit_text(t(lang, "results_found", n=len(filtered)))
                await state.finish()
                await self._show_current_card(callback.message, user_id)
                await callback.answer()
            
            # Показать текущую карточку
            async def _show_current_card(message_or_callback, user_id: int):
                lang = self._get_user_lang(user_id)
                user_data = self.user_results.get(user_id, {})
                rows = user_data.get("rows", [])
                idx = user_data.get("idx", 0)
                
                if not rows:
                    if isinstance(message_or_callback, CallbackQuery):
                        await message_or_callback.message.answer(t(lang, "no_results"))
                    else:
                        await message_or_callback.answer(t(lang, "no_results"))
                    return
                
                row = rows[idx]
                total = len(rows)
                
                # Форматируем карточку
                text = format_card(row, lang)
                photos = collect_photos(row)
                
                # Создаем клавиатуру
                kb = InlineKeyboardMarkup()
                
                # Навигация
                nav_buttons = []
                if idx > 0:
                    nav_buttons.append(InlineKeyboardButton(t(lang, "btn_prev"), callback_data=f"nav:prev"))
                if idx < total - 1:
                    nav_buttons.append(InlineKeyboardButton(t(lang, "btn_next"), callback_data=f"nav:next"))
                
                if nav_buttons:
                    kb.row(*nav_buttons)
                
                # Действия
                kb.row(
                    InlineKeyboardButton(t(lang, "btn_like"), callback_data="action:like"),
                    InlineKeyboardButton(t(lang, "btn_dislike"), callback_data="action:dislike")
                )
                
                # Избранное
                fav_key = f"{row.get('city', '')}_{row.get('district', '')}_{row.get('type', '')}_{row.get('price', '')}"
                is_fav = fav_key in self.user_favs.get(user_id, [])
                fav_text = t(lang, "btn_fav_del") if is_fav else t(lang, "btn_fav_add")
                kb.row(InlineKeyboardButton(fav_text, callback_data="action:fav"))
                
                kb.row(InlineKeyboardButton(t(lang, "btn_home"), callback_data="home"))
                
                # Отправляем сообщение
                if isinstance(message_or_callback, CallbackQuery):
                    message = message_or_callback.message
                    try:
                        if photos:
                            # Если есть фото, отправляем медиагруппу
                            media = []
                            for i, photo_url in enumerate(photos[:10]):
                                if i == 0:
                                    media.append(InputMediaPhoto(media=photo_url, caption=text, parse_mode="HTML"))
                                else:
                                    media.append(InputMediaPhoto(media=photo_url))
                            await message.answer_media_group(media)
                            await message.answer("📍", reply_markup=kb)
                        else:
                            await message.edit_text(text, reply_markup=kb, parse_mode="HTML")
                    except Exception as e:
                        self.logger.warning(f"Не удалось отправить фото: {e}")
                        await message.edit_text(text, reply_markup=kb, parse_mode="HTML")
                else:
                    if photos:
                        try:
                            media = []
                            for i, photo_url in enumerate(photos[:10]):
                                if i == 0:
                                    media.append(InputMediaPhoto(media=photo_url, caption=text, parse_mode="HTML"))
                                else:
                                    media.append(InputMediaPhoto(media=photo_url))
                            await message_or_callback.answer_media_group(media)
                            await message_or_callback.answer("📍", reply_markup=kb)
                        except Exception as e:
                            self.logger.warning(f"Не удалось отправить медиагруппу: {e}")
                            await message_or_callback.answer(text, reply_markup=kb, parse_mode="HTML")
                    else:
                        await message_or_callback.answer(text, reply_markup=kb, parse_mode="HTML")
            
            # Навигация по карточкам
            @self.router.callback_query(F.data.startswith("nav:"))
            async def handle_navigation(callback: CallbackQuery):
                user_id = callback.from_user.id
                action = callback.data.split(":")[1]
                
                user_data = self.user_results.get(user_id, {})
                current_idx = user_data.get("idx", 0)
                total = len(user_data.get("rows", []))
                
                if action == "prev" and current_idx > 0:
                    self.user_results[user_id]["idx"] = current_idx - 1
                elif action == "next" and current_idx < total - 1:
                    self.user_results[user_id]["idx"] = current_idx + 1
                
                await self._show_current_card(callback, user_id)
                await callback.answer()
            
            # Действия с карточками
            @self.router.callback_query(F.data.startswith("action:"))
            async def handle_actions(callback: CallbackQuery):
                user_id = callback.from_user.id
                action = callback.data.split(":")[1]
                lang = self._get_user_lang(user_id)
                
                user_data = self.user_results.get(user_id, {})
                rows = user_data.get("rows", [])
                idx = user_data.get("idx", 0)
                
                if not rows:
                    await callback.answer(t(lang, "no_results"))
                    return
                
                row = rows[idx]
                
                if action == "like":
                    # Лайк - запрос контакта
                    await callback.message.answer(t(lang, "lead_ask"))
                    await callback.answer("❤️")
                    
                elif action == "dislike":
                    # Дизлайк - следующая карточка
                    if idx < len(rows) - 1:
                        self.user_results[user_id]["idx"] = idx + 1
                        await self._show_current_card(callback, user_id)
                        await callback.answer(t(lang, "toast_next"))
                    else:
                        await callback.answer(t(lang, "toast_no_more"))
                
                elif action == "fav":
                    # Избранное
                    fav_key = f"{row.get('city', '')}_{row.get('district', '')}_{row.get('type', '')}_{row.get('price', '')}"
                    
                    if user_id not in self.user_favs:
                        self.user_favs[user_id] = []
                    
                    if fav_key in self.user_favs[user_id]:
                        self.user_favs[user_id].remove(fav_key)
                        await callback.answer(t(lang, "toast_removed"))
                    else:
                        self.user_favs[user_id].append(fav_key)
                        await callback.answer(t(lang, "toast_saved"))
                    
                    await self._show_current_card(callback, user_id)
            
            # Кнопка "Избранное"
            @self.router.message(F.text.in_([t("ru", "btn_favs"), t("en", "btn_favs"), t("ka", "btn_favs")]))
            async def handle_favorites(message: Message):
                user_id = message.from_user.id
                lang = self._get_user_lang(user_id)
                
                fav_keys = self.user_favs.get(user_id, [])
                if not fav_keys:
                    await message.answer("В избранном пока ничего нет")
                    return
                
                # Находим свойства по ключам
                fav_properties = []
                for prop in self.properties_cache:
                    prop_key = f"{prop.get('city', '')}_{prop.get('district', '')}_{prop.get('type', '')}_{prop.get('price', '')}"
                    if prop_key in fav_keys:
                        fav_properties.append(prop)
                
                if not fav_properties:
                    await message.answer("В избранном пока ничего нет")
                    return
                
                self.user_results[user_id] = {
                    "rows": fav_properties,
                    "idx": 0,
                    "context": {"mode": "favorites"}
                }
                
                await message.answer(f"Избранное: {len(fav_properties)} объявлений")
                await self._show_current_card(message, user_id)
            
            # Кнопка "Назад"
            @self.router.callback_query(F.data == "home")
            async def handle_home(callback: CallbackQuery, state: FSMContext):
                lang = self._get_user_lang(callback.from_user.id)
                await state.finish()
                await callback.message.edit_text(t(lang, "menu_title"))
                await callback.message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))
                await callback.answer()
            
            # Обработка текстовых сообщений (лиды)
            @self.router.message(F.text)
            async def handle_text_message(message: Message, state: FSMContext):
                user_id = message.from_user.id
                lang = self._get_user_lang(user_id)
                text = message.text or ""
                
                # Проверяем, не является ли это сообщение лидом
                if any(keyword in text.lower() for keyword in ["+995", "@", "телефон", "phone", "контакт"]):
                    # Это контакт - отправляем менеджеру
                    if FEEDBACK_CHAT_ID:
                        user = message.from_user
                        lead_text = f"""
📥 НОВЫЙ ЛИД

👤 Пользователь:
ID: {user.id}
Имя: {user.first_name}
Username: @{user.username or 'N/A'}

💬 Контакт:
{text}

🕒 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                        """
                        try:
                            await self.bot.send_message(FEEDBACK_CHAT_ID, lead_text)
                            await message.answer(t(lang, "lead_ok"))
                        except Exception as e:
                            self.logger.error(f"❌ Ошибка отправки лида: {e}")
                    return
                
                # Если это не лид, показываем главное меню
                await message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))
            
            self.logger.info("✅ Все обработчики настроены")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка настройки обработчиков: {e}")
            raise
    
    async def _schedule_tasks(self):
        """Планирование периодических задач"""
        async def cache_updater():
            while self.is_running:
                try:
                    self._refresh_cache()
                    await asyncio.sleep(GSHEET_REFRESH_MIN * 60)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в планировщике кэша: {e}")
                    await asyncio.sleep(60)  # Ждем минуту перед повторной попыткой
        
        # Запускаем планировщик
        asyncio.create_task(cache_updater())
    
    async def start(self):
        """Запуск бота"""
        try:
            self.logger.info("🚀 Запускаем бота...")
            
            # Убиваем старые процессы перед запуском
            self._kill_old_instances()
            
            if not self._validate_config():
                return False
            
            self._setup_signal_handlers()
            
            if not await self._create_bot_instance():
                return False
            
            # Настраиваем Google Sheets
            self._setup_google_sheets()
            
            self._create_dispatcher()
            self._setup_handlers()
            
            self.start_time = time.time()
            self.is_running = True
            
            # Обновляем кэш при старте
            self._refresh_cache()
            
            # Запускаем планировщики
            await self._schedule_tasks()
            
            self.logger.info("✅ Бот успешно инициализирован, запускаем polling...")
            
            await self.dp.start_polling(
                self.bot,
                allowed_updates=["message", "callback_query"],
                handle_signals=False
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка при запуске: {e}")
            return False
    
    async def _safe_shutdown(self):
        """Безопасное завершение работы"""
        self.logger.info("🛑 Начинаем безопасное завершение работы...")
        self.is_running = False
        
        try:
            if self.dp:
                await self.dp.stop_polling()
                self.logger.info("✅ Polling остановлен")
            
            if self.bot:
                await self.bot.session.close()
                self.logger.info("✅ Сессия бота закрыта")
            
            self.logger.info("✅ Все ресурсы освобождены")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при завершении работы: {e}")
        finally:
            # Отменяем все pending tasks
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                pass
            
            self.logger.info("👋 Бот завершил работу")

async def main():
    """Основная асинхронная функция"""
    bot = TelegramAdsBot()
    
    try:
        success = await bot.start()
        if success:
            bot.logger.info("🎉 Бот успешно запущен и работает!")
        else:
            bot.logger.error("❌ Не удалось запустить бота")
            return 1
            
    except KeyboardInterrupt:
        bot.logger.info("📞 Получен сигнал KeyboardInterrupt")
    except Exception as e:
        bot.logger.error(f"💥 Неожиданная ошибка: {e}")
        return 1
    finally:
        await bot._safe_shutdown()
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n👋 Завершение работы по запросу пользователя...")
        sys.exit(0)
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        sys.exit(1)
