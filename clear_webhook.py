import asyncio
from aiogram import Bot

API_TOKEN = '7539402706:AAHyXabwvtC3GqHsbQuv1Xle1oU99dGyKe8'

async def main():
    bot = Bot(token=API_TOKEN)
    await bot.delete_webhook()
    await bot.session.close()
    print("✅ Webhook deleted")

if __name__ == '__main__':
    asyncio.run(main())
