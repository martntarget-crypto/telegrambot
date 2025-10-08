#!/bin/bash

echo "🚀 Запуск Telegram Bot..."

# Проверяем, не запущен ли уже бот
if docker ps | grep -q "telegram-bot"; then
    echo "❌ Бот уже запущен!"
    echo "💡 Используйте: docker-compose restart"
    exit 1
fi

# Проверяем наличие .env файла
if [ ! -f .env ]; then
    echo "⚠️  Файл .env не найден!"
    echo "📝 Создайте .env файл из .env.example"
    exit 1
fi

# Запускаем бота
docker-compose up -d

echo "✅ Бот запущен!"
echo "📊 Логи: docker-compose logs -f"
echo "🛑 Остановка: docker-compose down"