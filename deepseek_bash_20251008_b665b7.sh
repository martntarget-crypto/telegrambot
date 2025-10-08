#!/bin/bash

echo "🛑 Останавливаем Telegram Bot..."

docker-compose down

echo "✅ Бот остановлен!"
echo "🚀 Запуск: docker-compose up -d"