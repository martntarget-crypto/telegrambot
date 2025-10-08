#!/bin/bash

echo "🔍 Проверка состояния бота..."

# Проверяем, запущен ли контейнер
if docker ps | grep -q "telegram-bot"; then
    echo "✅ Контейнер бота запущен"
    
    # Проверяем health status
    HEALTH=$(docker inspect --format='{{.State.Health.Status}}' telegram-bot)
    echo "🏥 Health status: $HEALTH"
    
    # Показываем логи
    echo "📊 Последние логи:"
    docker logs --tail=10 telegram-bot
else
    echo "❌ Контейнер бота не запущен"
    echo "💡 Запустите: docker-compose up -d"
fi