#!/bin/bash

echo "🔄 Обновление Telegram Bot..."

# Останавливаем бота
docker-compose down

# Обновляем образ
docker-compose pull

# Пересобираем контейнер
docker-compose build --no-cache

# Запускаем заново
docker-compose up -d

echo "✅ Бот обновлен!"
echo "📊 Статус: docker-compose ps"