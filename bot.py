#!/usr/bin/env python3
import logging
import os
import sys
import asyncio
import signal
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
import aiohttp
from dotenv import load_dotenv

# ===== КОНФИГУРАЦИЯ =====
load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    print("❌ BOT_TOKEN не установлен в переменных окружения")
    sys.exit(1)

try:
    ADMIN_IDS = [int(x.strip()) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip()]
except (ValueError, AttributeError):
    ADMIN_IDS = []

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
# =========================

class AdvancedBotManager:
    """Продвинутый менеджер бота с полной обработкой ошибок"""
    
    def __init__(self):
        self._setup_logging()
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.router: Optional[Router] = None
        self.storage: Optional[MemoryStorage] = None
        self.is_running = False
        self.start_time = None
        self.session: Optional[aiohttp.ClientSession] = None
        
    def _setup_logging(self):
        """Настройка системы логирования - В САМОМ НАЧАЛЕ!"""
        try:
            # Устанавливаем уровень логирования
            log_level = getattr(logging, LOG_LEVEL, logging.INFO)
            
            # Создаем форматтер
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            # Настраиваем корневой логгер
            logger = logging.getLogger()
            logger.setLevel(log_level)
            
            # Очищаем существующие обработчики
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            
            # Добавляем консольный обработчик
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            
            # Добавляем файловый обработчик
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
        if not BOT_TOKEN:
            self.logger.error("❌ BOT_TOKEN не установлен")
            return False
            
        if not ADMIN_IDS:
            self.logger.warning("⚠️ ADMIN_IDS не установлены, админские команды недоступны")
            
        self.logger.info("✅ Конфигурация проверена успешно")
        return True
    
    def _setup_signal_handlers(self):
        """Настройка обработчиков сигналов для graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"📞 Получен сигнал {signum}, завершаем работу...")
            asyncio.create_task(self._safe_shutdown())
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def _create_aiohttp_session(self):
        """Создание aiohttp сессии"""
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
            self.logger.info("✅ aiohttp сессия создана")
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания aiohttp сессии: {e}")
    
    async def _close_aiohttp_session(self):
        """Закрытие aiohttp сессии"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                self.logger.info("✅ aiohttp сессия закрыта")
        except Exception as e:
            self.logger.error(f"❌ Ошибка закрытия aiohttp сессии: {e}")
    
    async def _create_bot_instance(self) -> bool:
        """Создание экземпляра бота с проверкой доступности"""
        try:
            self.logger.info("🤖 Создаем экземпляр бота...")
            
            # Создаем бота с настройками по умолчанию
            self.bot = Bot(
                token=BOT_TOKEN,
                default=DefaultBotProperties(parse_mode=ParseMode.HTML),
                session=self.session
            )
            
            # Проверяем доступность бота
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
                    self.logger.info(f"👋 Команда /start от пользователя {message.from_user.id}")
                    
                    markup = ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text="ℹ️ Помощь"), KeyboardButton(text="🔄 Статус")],
                            [KeyboardButton(text="📊 Информация")]
                        ],
                        resize_keyboard=True
                    )
                    
                    user = message.from_user
                    await message.answer(
                        f"🤖 <b>Добро пожаловать, {user.first_name}!</b>\n\n"
                        f"👤 <b>Ваш профиль:</b>\n"
                        f"   ID: <code>{user.id}</code>\n"
                        f"   Имя: {user.first_name or 'Не указано'}\n"
                        f"   Фамилия: {user.last_name or 'Не указана'}\n"
                        f"   Username: @{user.username or 'Не указан'}\n\n"
                        f"🕒 <b>Время сервера:</b> {self._get_current_time()}\n"
                        f"📡 <b>Статус бота:</b> 🟢 Активен",
                        reply_markup=markup
                    )
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /start: {e}")
            
            # Команда /help
            @self.router.message(Command("help"))
            @self.router.message(F.text == "ℹ️ Помощь")
            async def handle_help(message: Message):
                try:
                    help_text = """
<b>📚 Доступные команды:</b>

/start - Запуск бота и информация о пользователе
/help - Список команд и помощь
/status - Статус системы и бота
/info - Подробная информация о боте
/admin - Админ панель (только для админов)

<b>🔧 Основные функции:</b>
• Мониторинг состояния системы
• Управление настройками
• Логирование действий
• Информация о пользователе

<b>⚡ Быстрые команды через кнопки:</b>
Используйте кнопки ниже для быстрого доступа к функциям!
                    """
                    await message.answer(help_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /help: {e}")
            
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
🖥 <b>Нагрузка:</b> {self._get_system_load()}
📈 <b>Логов сегодня:</b> {self._get_today_logs_count()}

<b>🌐 Системная информация:</b>
ОС: {sys.platform}
Python: {sys.version.split()[0]}
Aiogram: 3.22.0
                    """
                    await message.answer(status_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /status: {e}")
            
            # Команда /info
            @self.router.message(Command("info"))
            @self.router.message(F.text == "📊 Информация")
            async def handle_info(message: Message):
                try:
                    info_text = f"""
<b>ℹ️ Информация о боте:</b>

<b>Разработчик:</b> AIogram 3.22.0 Bot Framework
<b>Версия:</b> 2.0.0
<b>Архитектура:</b> Асинхронная
<b>Хранилище:</b> Memory Storage
<b>Логирование:</b> Файловое + Консольное

<b>📊 Статистика:</b>
Запущен: {self._get_start_time()}
Обработано сообщений: {self._get_estimated_messages()}
Файл логов: bot.log ({self._get_log_file_size()})

<b>🔧 Технологии:</b>
• Aiogram 3.22.0
• aiohttp 3.12.15
• asyncio
• Python 3.11+
                    """
                    await message.answer(info_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /info: {e}")
            
            # Команда /admin
            @self.router.message(Command("admin"))
            async def handle_admin(message: Message):
                try:
                    if message.from_user.id not in ADMIN_IDS:
                        await message.answer("❌ <b>Доступ запрещен!</b>\n\nЭта команда доступна только администраторам.")
                        return
                    
                    # Статистика для админов
                    admin_text = f"""
<b>👨‍💻 Админ панель</b>

<b>📈 Статистика:</b>
Пользователей в чате: 1
Всего логов: {self._get_total_logs_count()}
Логов сегодня: {self._get_today_logs_count()}
Размер лог-файла: {self._get_log_file_size()}

<b>🔧 Система:</b>
ОС: {sys.platform}
Python: {sys.version.split()[0]}
Память: {self._get_memory_usage()} MB
Аптайм: {self._get_uptime()}

<b>🤖 Бот:</b>
ID: {(await self.bot.get_me()).id}
Username: @{(await self.bot.get_me()).username}
Версия Aiogram: 3.22.0

<b>👥 Администраторы:</b>
{len(ADMIN_IDS)} пользователей
                    """
                    await message.answer(admin_text)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в обработчике /admin: {e}")
            
            # Обработка неизвестных сообщений
            @self.router.message()
            async def handle_unknown(message: Message):
                try:
                    self.logger.info(f"❓ Неизвестное сообщение от {message.from_user.id}: {message.text}")
                    await message.answer(
                        "🤔 <b>Не понимаю команду</b>\n\n"
                        "Используйте /help для просмотра доступных команд или кнопки ниже для быстрого доступа.",
                        reply_markup=ReplyKeyboardMarkup(
                            keyboard=[
                                [KeyboardButton(text="ℹ️ Помощь"), KeyboardButton(text="🔄 Статус")]
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
            uptime = asyncio.get_event_loop().time() - self.start_time
            hours = int(uptime // 3600)
            minutes = int((uptime % 3600) // 60)
            seconds = int(uptime % 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except:
            return "N/A"
    
    def _get_start_time(self):
        """Получение времени запуска"""
        if self.start_time:
            return datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')
        return "N/A"
    
    def _get_system_load(self):
        """Получение нагрузки системы"""
        try:
            import psutil
            load = psutil.getloadavg()
            return f"{load[0]:.2f}, {load[1]:.2f}, {load[2]:.2f}"
        except (ImportError, AttributeError):
            return "N/A"
    
    async def _get_ping(self):
        """Получение ping до серверов Telegram"""
        try:
            import time
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
    
    def _get_total_logs_count(self):
        """Получение общего количества логов"""
        try:
            if os.path.exists('bot.log'):
                with open('bot.log', 'r', encoding='utf-8') as f:
                    return sum(1 for _ in f)
            return 0
        except:
            return "N/A"
    
    def _get_log_file_size(self):
        """Получение размера файла логов"""
        try:
            if os.path.exists('bot.log'):
                size = os.path.getsize('bot.log')
                if size < 1024:
                    return f"{size} B"
                elif size < 1024 * 1024:
                    return f"{size/1024:.2f} KB"
                else:
                    return f"{size/(1024*1024):.2f} MB"
            return "0 B"
        except:
            return "N/A"
    
    def _get_estimated_messages(self):
        """Оценочное количество обработанных сообщений"""
        try:
            # Это упрощенная оценка - в реальном боте нужно вести счетчик
            logs_count = self._get_total_logs_count()
            return max(0, logs_count // 3)  # Примерная оценка
        except:
            return "N/A"
    
    async def start(self):
        """Запуск бота"""
        try:
            self.logger.info("🚀 Запускаем бота...")
            
            if not self._validate_config():
                return False
            
            self._setup_signal_handlers()
            await self._create_aiohttp_session()
            
            if not await self._create_bot_instance():
                return False
            
            self._create_dispatcher()
            self._setup_handlers()
            
            self.start_time = asyncio.get_event_loop().time()
            self.is_running = True
            
            self.logger.info("✅ Бот успешно инициализирован, запускаем polling...")
            
            # Запускаем polling
            await self.dp.start_polling(
                self.bot,
                allowed_updates=["message", "callback_query"],
                handle_signals=False  # Мы сами обрабатываем сигналы
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
            # Останавливаем polling
            if self.dp:
                await self.dp.stop_polling()
                self.logger.info("✅ Polling остановлен")
            
            # Закрываем сессию бота
            if self.bot:
                await self.bot.session.close()
                self.logger.info("✅ Сессия бота закрыта")
            
            # Закрываем aiohttp сессию
            await self._close_aiohttp_session()
            
            self.logger.info("✅ Все ресурсы освобождены")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при завершении работы: {e}")
        finally:
            # Завершаем event loop
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
            self.logger.info("👋 Бот завершил работу")
            os._exit(0)

async def main():
    """Основная асинхронная функция"""
    bot_manager = AdvancedBotManager()
    
    try:
        success = await bot_manager.start()
        if success:
            bot_manager.logger.info("🎉 Бот успешно запущен и работает!")
        else:
            bot_manager.logger.error("❌ Не удалось запустить бота")
            return 1
            
    except KeyboardInterrupt:
        bot_manager.logger.info("📞 Получен сигнал KeyboardInterrupt")
    except Exception as e:
        bot_manager.logger.error(f"💥 Неожиданная ошибка: {e}")
        return 1
    finally:
        await bot_manager._safe_shutdown()
    
    return 0

if __name__ == "__main__":
    # Запуск асинхронного приложения
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n👋 Завершение работы по запросу пользователя...")
        sys.exit(0)
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        sys.exit(1)
