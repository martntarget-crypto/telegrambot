#!/usr/bin/env python3
import logging
import os
import sys
import asyncio
import signal
import gspread
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import random
import time

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

GSHEET_STATS_ID = os.getenv('GSHEET_STATS_ID')

# Настройки отчетов
try:
    WEEKLY_REPORT_DOW = int(os.getenv('WEEKLY_REPORT_DOW', '1'))
    WEEKLY_REPORT_HOUR = int(os.getenv('WEEKLY_REPORT_HOUR', '9'))
except (ValueError, TypeError):
    WEEKLY_REPORT_DOW = 1
    WEEKLY_REPORT_HOUR = 9

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

# ===== КЛАССЫ СОСТОЯНИЙ =====
class UserStates(StatesGroup):
    waiting_for_feedback = State()
    waiting_for_contact = State()

# ===== ОСНОВНОЙ КЛАСС БОТА =====
class TelegramAdsBot:
    """Продвинутый бот для управления рекламой в Telegram с Google Sheets"""
    
    def __init__(self):
        self._setup_logging()
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.router: Optional[Router] = None
        self.storage: Optional[MemoryStorage] = None
        self.gc: Optional[gspread.Client] = None
        self.ads_sheet = None
        self.stats_sheet = None
        self.is_running = False
        self.start_time = None
        self.ads_cache = []
        self.last_cache_update = None
        self.user_last_ad = {}  # {user_id: timestamp}
        
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
            
            # Открываем таблицу статистики если указана
            if GSHEET_STATS_ID:
                self.stats_sheet = self.gc.open_by_key(GSHEET_STATS_ID).sheet1
                self.logger.info("✅ Таблица статистики подключена")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка настройки Google Sheets: {e}")
            return False
    
    def _refresh_ads_cache(self):
        """Обновление кэша рекламных объявлений"""
        try:
            if not self.ads_sheet:
                return
                
            # Проверяем, нужно ли обновлять кэш
            if (self.last_cache_update and 
                datetime.now() - self.last_cache_update < timedelta(minutes=GSHEET_REFRESH_MIN)):
                return
            
            records = self.ads_sheet.get_all_records()
            self.ads_cache = [record for record in records if record.get('active', '') == '1']
            self.last_cache_update = datetime.now()
            self.logger.info(f"✅ Кэш объявлений обновлен: {len(self.ads_cache)} активных объявлений")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления кэша объявлений: {e}")
    
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
            
            text = ad.get('text', '')
            image_url = ad.get('image', '')
            button_text = ad.get('button_text', '')
            button_url = ad.get('button_url', '')
            
            # Добавляем UTM параметры к URL
            if button_url:
                if any(param in button_url for param in ['?', '&']):
                    button_url += f"&utm_source={UTM_SOURCE}&utm_medium={UTM_MEDIUM}&utm_campaign={UTM_CAMPAIGN}"
                else:
                    button_url += f"?utm_source={UTM_SOURCE}&utm_medium={UTM_MEDIUM}&utm_campaign={UTM_CAMPAIGN}"
            
            keyboard = None
            if button_text and button_url:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=button_text, url=button_url)]
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
            
            # Логируем показ
            await self._log_ad_shown(ad, user_id, user_name)
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки рекламы пользователю {user_id}: {e}")
            return False
    
    async def _log_ad_shown(self, ad: Dict, user_id: int, user_name: str):
        """Логирование показа рекламы"""
        try:
            if self.stats_sheet:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                row = [timestamp, str(user_id), user_name, ad.get('id', 'N/A'), ad.get('title', 'N/A')]
                self.stats_sheet.append_row(row)
        except Exception as e:
            self.logger.error(f"❌ Ошибка логирования показа рекламы: {e}")
    
    async def _log_lead(self, user_data: Dict, message: str = ""):
        """Логирование лида в Google Sheets и отправка в чат"""
        try:
            # Логируем в Google Sheets если есть таблица статистики
            if self.stats_sheet:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                row = [
                    timestamp, 
                    str(user_data.get('id', '')), 
                    user_data.get('name', ''),
                    user_data.get('username', ''),
                    'LEAD',
                    message[:100]  # ограничиваем длину сообщения
                ]
                self.stats_sheet.append_row(row)
            
            # Отправляем уведомление в чат для лидов
            if FEEDBACK_CHAT_ID:
                lead_text = f"""
📥 НОВЫЙ ЛИД

👤 Пользователь:
ID: {user_data.get('id', 'N/A')}
Имя: {user_data.get('name', 'N/A')}
Username: @{user_data.get('username', 'N/A')}

💬 Сообщение:
{message if message else 'Контакт запрошен'}

🕒 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                """
                await self.bot.send_message(FEEDBACK_CHAT_ID, lead_text)
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка логирования лида: {e}")
    
    async def _create_bot_instance(self) -> bool:
        """Создание экземпляра бота с проверкой доступности"""
        try:
            self.logger.info("🤖 Создаем экземпляр бота...")
            
            # ИСПРАВЛЕНИЕ: убрана передача session в Bot
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
            async def handle_start(message: Message):
                try:
                    user = message.from_user
                    self.logger.info(f"👋 Команда /start от пользователя {user.id}")
                    
                    # Обновляем кэш объявлений
                    self._refresh_ads_cache()
                    
                    # Проверяем и отправляем рекламу если нужно
                    should_send_ad = False
                    if ADS_ENABLED:
                        now = time.time()
                        last_ad_time = self.user_last_ad.get(user.id, 0)
                        if now - last_ad_time > ADS_COOLDOWN_SEC and random.random() < ADS_PROB:
                            should_send_ad = True
                            self.user_last_ad[user.id] = now
                    
                    markup = ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text="ℹ️ О нас"), KeyboardButton(text="💼 Услуги")],
                            [KeyboardButton(text="📞 Связаться"), KeyboardButton(text="🎁 Спецпредложение")],
                            [KeyboardButton(text="🔄 Статус")]
                        ],
                        resize_keyboard=True
                    )
                    
                    welcome_text = f"""
🤖 <b>Добро пожаловать, {user.first_name}!</b>

Рады приветствовать вас! Я помогу вам узнать о наших услугах и специальных предложениях.

Выберите нужный раздел ниже или используйте команды:
/help - список команд
/info - о боте
"""
                    await message.answer(welcome_text, reply_markup=markup)
                    
                    # Отправляем рекламу после приветствия
                    if should_send_ad:
                        await asyncio.sleep(1)
                        await self._send_ad_to_user(user.id, user.first_name)
                        
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /start: {e}")
            
            # Команда /help
            @self.router.message(Command("help"))
            async def handle_help(message: Message):
                try:
                    help_text = """
<b>📚 Доступные команды:</b>

/start - Начать работу
/help - Список команд
/info - О боте
/status - Статус системы
/feedback - Оставить отзыв
/admin - Админ панель

<b>🔧 Основные функции:</b>
• Информация об услугах
• Связь с менеджером
• Специальные предложения
• Автоматические уведомления
"""
                    await message.answer(help_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /help: {e}")
            
            # Команда /feedback
            @self.router.message(Command("feedback"))
            async def handle_feedback_command(message: Message, state: FSMContext):
                try:
                    await message.answer(
                        "💬 <b>Оставьте ваш отзыв или предложение:</b>\n\n"
                        "Напишите сообщение, и мы его обязательно рассмотрим!"
                    )
                    await state.set_state(UserStates.waiting_for_feedback)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /feedback: {e}")
            
            # Обработка текста фидбека
            @self.router.message(UserStates.waiting_for_feedback)
            async def handle_feedback_text(message: Message, state: FSMContext):
                try:
                    user = message.from_user
                    feedback_text = message.text
                    
                    # Логируем фидбек как лид
                    user_data = {
                        'id': user.id,
                        'name': user.first_name,
                        'username': user.username
                    }
                    await self._log_lead(user_data, f"Фидбек: {feedback_text}")
                    
                    await message.answer(
                        "✅ <b>Спасибо за ваш отзыв!</b>\n\n"
                        "Мы ценим ваше мнение и обязательно его рассмотрим."
                    )
                    await state.clear()
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки фидбека: {e}")
                    await state.clear()
            
            # Кнопка "Связаться"
            @self.router.message(F.text == "📞 Связаться")
            async def handle_contact(message: Message, state: FSMContext):
                try:
                    user = message.from_user
                    
                    markup = ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text="📱 Отправить номер телефона", request_contact=True)],
                            [KeyboardButton(text="↩️ Назад")]
                        ],
                        resize_keyboard=True
                    )
                    
                    await message.answer(
                        "📞 <b>Свяжитесь с нами</b>\n\n"
                        "Нажмите кнопку ниже чтобы отправить номер телефона, "
                        "и наш менеджер свяжется с вами в ближайшее время!",
                        reply_markup=markup
                    )
                    await state.set_state(UserStates.waiting_for_contact)
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки кнопки связи: {e}")
            
            # Обработка контакта
            @self.router.message(UserStates.waiting_for_contact, F.contact)
            async def handle_contact_received(message: Message, state: FSMContext):
                try:
                    user = message.from_user
                    contact = message.contact
                    
                    # Логируем контакт как лид
                    user_data = {
                        'id': user.id,
                        'name': contact.first_name or user.first_name,
                        'username': user.username,
                        'phone': contact.phone_number
                    }
                    await self._log_lead(user_data, "Запрос связи (отправлен номер телефона)")
                    
                    await message.answer(
                        "✅ <b>Спасибо! Ваш номер телефона получен.</b>\n\n"
                        "Наш менеджер свяжется с вами в ближайшее время.",
                        reply_markup=ReplyKeyboardMarkup(
                            keyboard=[
                                [KeyboardButton(text="ℹ️ О нас"), KeyboardButton(text="💼 Услуги")],
                                [KeyboardButton(text="📞 Связаться"), KeyboardButton(text="🎁 Спецпредложение")]
                            ],
                            resize_keyboard=True
                        )
                    )
                    await state.clear()
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки контакта: {e}")
                    await state.clear()
            
            # Кнопка "Назад"
            @self.router.message(F.text == "↩️ Назад")
            async def handle_back(message: Message, state: FSMContext):
                try:
                    await state.clear()
                    markup = ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text="ℹ️ О нас"), KeyboardButton(text="💼 Услуги")],
                            [KeyboardButton(text="📞 Связаться"), KeyboardButton(text="🎁 Спецпредложение")],
                            [KeyboardButton(text="🔄 Статус")]
                        ],
                        resize_keyboard=True
                    )
                    await message.answer("🔙 Возвращаемся в главное меню", reply_markup=markup)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки кнопки назад: {e}")
            
            # Кнопка "О нас"
            @self.router.message(F.text == "ℹ️ О нас")
            async def handle_about(message: Message):
                try:
                    about_text = """
<b>🏢 О нашей компании</b>

Мы профессиональная команда, специализирующаяся на digital-решениях. 

<b>Наши преимущества:</b>
• Опыт работы более 5 лет
• 100+ успешных проектов
• Индивидуальный подход к каждому клиенту
• Современные технологии и методологии

Узнайте больше о наших услугах или свяжитесь с нами для консультации!
"""
                    await message.answer(about_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки кнопки 'О нас': {e}")
            
            # Кнопка "Услуги"
            @self.router.message(F.text == "💼 Услуги")
            async def handle_services(message: Message):
                try:
                    services_text = """
<b>💼 Наши услуги</b>

<b>🔹 Разработка ботов</b>
- Telegram, WhatsApp, VK боты
- Интеграция с CRM и базами данных
- Автоматизация бизнес-процессов

<b>🔹 Веб-разработка</b>
- Сайты и веб-приложения
- Интернет-магазины
- Корпоративные порталы

<b>🔹 Digital-маркетинг</b>
- SEO оптимизация
- Контекстная реклама
- SMM продвижение

<b>🔹 Автоматизация</b>
- Интеграция API
- Скрипты и парсинг данных
- Бизнес-процессы

Нажмите "Связаться" для консультации!
"""
                    await message.answer(services_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки кнопки 'Услуги': {e}")
            
            # Кнопка "Спецпредложение"
            @self.router.message(F.text == "🎁 Спецпредложение")
            async def handle_special_offer(message: Message):
                try:
                    offer_text = """
<b>🎁 Специальное предложение</b>

🔥 <b>Только для новых клиентов!</b>

При заказе любой услуги до конца месяца получите:

✅ <b>Бесплатную консультацию</b> по оптимизации бизнеса
✅ <b>Аудит</b> текущих digital-процессов  
✅ <b>Скидку 15%</b> на первый заказ

Успейте воспользоваться предложением! 🏃💨

Нажмите "Связаться" чтобы узнать подробности!
"""
                    await message.answer(offer_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки кнопки 'Спецпредложение': {e}")
            
            # Команда /status
            @self.router.message(Command("status"))
            @self.router.message(F.text == "🔄 Статус")
            async def handle_status(message: Message):
                try:
                    ping_time = await self._get_ping()
                    status_text = f"""
<b>📊 Статус системы:</b>

🤖 <b>Бот:</b> {'🟢 Активен' if self.is_running else '🔴 Остановлен'}
💾 <b>Память:</b> {self._get_memory_usage()} MB
⏰ <b>Аптайм:</b> {self._get_uptime()}
📶 <b>Ping:</b> {ping_time} ms

<b>📈 Статистика:</b>
Объявления в кэше: {len(self.ads_cache)}
Реклама: {'🟢 Вкл' if ADS_ENABLED else '🔴 Выкл'}
Google Sheets: {'🟢 Подключено' if self.ads_sheet else '🔴 Отключено'}

<b>👥 Пользователи:</b>
Активных сегодня: {self._get_today_users_count()}
"""
                    await message.answer(status_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /status: {e}")
            
            # Команда /info
            @self.router.message(Command("info"))
            async def handle_info(message: Message):
                try:
                    info_text = f"""
<b>ℹ️ Информация о боте</b>

<b>Версия:</b> 2.0.0
<b>Назначение:</b> Автоматизация рекламы и сбора лидов
<b>Архитектура:</b> Асинхронная

<b>🔧 Технологии:</b>
• Aiogram 3.22.0
• Google Sheets API
• aiohttp 3.12.15

<b>⚙️ Настройки:</b>
Реклама: {'Включена' if ADS_ENABLED else 'Выключена'}
Вероятность рекламы: {ADS_PROB * 100}%
Кэш объявлений: {GSHEET_REFRESH_MIN} мин.

<b>📊 Функции:</b>
✅ Управление рекламой
✅ Сбор лидов  
✅ Google Sheets интеграция
✅ Авто-отчеты
✅ UTM метки
"""
                    await message.answer(info_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /info: {e}")
            
            # Команда /admin
            @self.router.message(Command("admin"))
            async def handle_admin(message: Message):
                try:
                    if message.from_user.id != ADMIN_CHAT_ID:
                        await message.answer("❌ <b>Доступ запрещен!</b>\n\nЭта команда доступна только администраторам.")
                        return
                    
                    admin_text = f"""
<b>👨‍💻 Админ панель</b>

<b>📈 Статистика:</b>
Активных объявлений: {len(self.ads_cache)}
Обновление кэша: {self.last_cache_update.strftime('%H:%M:%S') if self.last_cache_update else 'Никогда'}
Логов сегодня: {self._get_today_logs_count()}

<b>⚙️ Настройки:</b>
ADS_ENABLED: {ADS_ENABLED}
ADS_PROB: {ADS_PROB}
ADS_COOLDOWN_SEC: {ADS_COOLDOWN_SEC}
GSHEET_REFRESH_MIN: {GSHEET_REFRESH_MIN}

<b>🔧 Команды админа:</b>
/update_ads - Обновить кэш объявлений
/stats - Подробная статистика
"""
                    await message.answer(admin_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /admin: {e}")
            
            # Команда /update_ads
            @self.router.message(Command("update_ads"))
            async def handle_update_ads(message: Message):
                try:
                    if message.from_user.id != ADMIN_CHAT_ID:
                        return
                    
                    self._refresh_ads_cache()
                    await message.answer(f"✅ Кэш объявлений обновлен: {len(self.ads_cache)} активных объявлений")
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /update_ads: {e}")
                    await message.answer("❌ Ошибка обновления кэша")
            
            # Обработка неизвестных сообщений
            @self.router.message()
            async def handle_unknown(message: Message):
                try:
                    self.logger.info(f"❓ Неизвестное сообщение от {message.from_user.id}: {message.text}")
                    
                    # Проверяем и отправляем рекламу если нужно
                    if ADS_ENABLED:
                        user = message.from_user
                        now = time.time()
                        last_ad_time = self.user_last_ad.get(user.id, 0)
                        if now - last_ad_time > ADS_COOLDOWN_SEC and random.random() < ADS_PROB:
                            await self._send_ad_to_user(user.id, user.first_name)
                            self.user_last_ad[user.id] = now
                            return
                    
                    await message.answer(
                        "🤔 <b>Не понимаю команду</b>\n\n"
                        "Используйте /help для просмотра доступных команд или кнопки ниже для навигации.",
                        reply_markup=ReplyKeyboardMarkup(
                            keyboard=[
                                [KeyboardButton(text="ℹ️ О нас"), KeyboardButton(text="💼 Услуги")],
                                [KeyboardButton(text="📞 Связаться"), KeyboardButton(text="🎁 Спецпредложение")]
                            ],
                            resize_keyboard=True
                        )
                    )
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике неизвестного сообщения: {e}")
            
            self.logger.info("✅ Все обработчики команд настроены")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка настройки обработчиков: {e}")
            raise
    
    def _get_current_time(self):
        """Получение текущего времени"""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _get_memory_usage(self):
        """Получение использования памяти"""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return round(process.memory_info().rss / 1024 / 1024, 2)
        except ImportError:
            return "N/A"
    
    def _get_uptime(self):
        """Получение времени работы"""
        try:
            if not self.start_time:
                return "N/A"
            uptime = time.time() - self.start_time
            hours = int(uptime // 3600)
            minutes = int((uptime % 3600) // 60)
            seconds = int(uptime % 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except:
            return "N/A"
    
    async def _get_ping(self):
        """Получение ping до серверов Telegram"""
        try:
            start_time = time.time()
            await self.bot.get_me()
            ping_time = (time.time() - start_time) * 1000
            return f"{ping_time:.2f}"
        except:
            return "N/A"
    
    def _get_today_logs_count(self):
        """Получение количества логов за сегодня"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            count = 0
            if os.path.exists('bot.log'):
                with open('bot.log', 'r', encoding='utf-8') as f:
                    for line in f:
                        if today in line:
                            count += 1
            return count
        except:
            return "N/A"
    
    def _get_today_users_count(self):
        """Оценочное количество активных пользователей за сегодня"""
        try:
            # В реальном приложении здесь должна быть база данных
            # Сейчас возвращаем примерное число на основе логов
            today = datetime.now().strftime('%Y-%m-%d')
            user_ids = set()
            if os.path.exists('bot.log'):
                with open('bot.log', 'r', encoding='utf-8') as f:
                    for line in f:
                        if today in line and 'от пользователя' in line:
                            # Пытаемся извлечь ID пользователя из лога
                            import re
                            match = re.search(r'от пользователя (\d+)', line)
                            if match:
                                user_ids.add(match.group(1))
            return len(user_ids)
        except:
            return "N/A"
    
    async def _send_weekly_report(self):
        """Отправка еженедельного отчета админу"""
        try:
            if not ADMIN_CHAT_ID:
                return
                
            report_text = f"""
<b>📊 Еженедельный отчет</b>

<b>Статистика за неделю:</b>
Активных пользователей: {self._get_today_users_count()}
Показано рекламы: {len(self.user_last_ad)}
Логов: {self._get_today_logs_count()}

<b>Система:</b>
Аптайм: {self._get_uptime()}
Память: {self._get_memory_usage()} MB
Объявления в кэше: {len(self.ads_cache)}

<b>Дата отчета:</b> {self._get_current_time()}
"""
            await self.bot.send_message(ADMIN_CHAT_ID, report_text)
            self.logger.info("✅ Еженедельный отчет отправлен админу")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки недельного отчета: {e}")
    
    async def _schedule_tasks(self):
        """Планирование периодических задач"""
        async def weekly_report_scheduler():
            while self.is_running:
                now = datetime.utcnow()
                
                # Проверяем, наступило ли время для еженедельного отчета
                if (now.weekday() == (WEEKLY_REPORT_DOW - 1) % 7 and 
                    now.hour == WEEKLY_REPORT_HOUR and 
                    now.minute == 0):
                    
                    await self._send_weekly_report()
                    # Ждем 1 час чтобы не отправить отчет несколько раз в течение часа
                    await asyncio.sleep(3600)
                else:
                    await asyncio.sleep(60)  # Проверяем каждую минуту
        
        async def cache_updater():
            while self.is_running:
                self._refresh_ads_cache()
                await asyncio.sleep(GSHEET_REFRESH_MIN * 60)  # Обновляем кэш каждые N минут
        
        # Запускаем планировщики
        asyncio.create_task(weekly_report_scheduler())
        asyncio.create_task(cache_updater())
    
    async def start(self):
        """Запуск бота"""
        try:
            self.logger.info("🚀 Запускаем бота...")
            
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
            self._refresh_ads_cache()
            
            # Запускаем планировщики
            await self._schedule_tasks()
            
            self.logger.info("✅ Бот успешно инициализирован, запускаем polling...")
            
            await self.dp.start_polling(
                self.bot,
                allowed_updates=["message", "callback_query", "chat_member"],
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
            
            # ИСПРАВЛЕНИЕ: правильное закрытие сессии бота
            if self.bot:
                await self.bot.session.close()
                self.logger.info("✅ Сессия бота закрыта")
            
            self.logger.info("✅ Все ресурсы освобождены")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при завершении работы: {e}")
        finally:
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
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
