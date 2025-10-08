# patch_catchall_fix.py
"""
Usage:
  1) Put this file next to your existing bot.py
  2) Run:  python patch_catchall_fix.py
  3) It will create a backup bot.py.bak and rewrite the catch-all handler so it
     no longer spams "Главное меню" and doesn't react to our own buttons.
"""

import re
from pathlib import Path

BOT = Path("bot.py")
BACKUP = Path("bot.py.bak")

SRC = BOT.read_text(encoding="utf-8")

# 1) Build the fixed catch-all handler block
FIXED = r'''
# ----- ВАЖНО: общий обработчик ТОЛЬКО для НЕ-команд -----
@dp.message_handler(lambda m: not ((m.text or "").startswith("/")) and not m.from_user.is_bot, state="*")
async def any_text(message: types.Message, state: FSMContext):
    data = await state.get_data()

    # Если ждём контакт для лида — обработать и выйти
    if data.get("want_contact"):
        contact = (message.text or "").strip()
        user = message.from_user
        lang = USER_LANG.get(user.id, "ru")

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
            if row:
                try:
                    log_event("lead", user.id, row=row, extra={"contact": contact})
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Lead send failed: {e}")

        await state.update_data(want_contact=False)
        return await message.answer(t(lang, "lead_ok"), reply_markup=main_menu(lang))

    # Игнорировать наши кнопки, чтобы не дублировать ответы
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
        return  # ничего не отвечаем — конкретные хендлеры уже обработают

    # По умолчанию — вернуть в меню
    lang = USER_LANG.get(message.from_user.id, "ru")
    await message.answer(t(lang, "menu_title"), reply_markup=main_menu(lang))
'''.strip() + "\n"

# 2) Remove any previous catch-all handlers to avoid duplicates
pattern = re.compile(
    r'@dp\.message_handler\([^\)]*not\s*\(\(\s*m\.text[^\)]*\)\)\s*,?\s*state\s*=\s*[\"\']\*[\"\']\)\s*?\n'
    r'async def .*?\n'
    r'(?:.*?\n)*?'
    r'(?=\n@dp\.|if __name__ == [\"\']__main__[\"\']|$)',
    re.DOTALL
)

new_src, n = pattern.subn('', SRC)
if n == 0:
    print("⚠️ Не нашёл старый общий обработчик. Просто добавлю новый в конец файла.")
    new_src = SRC
else:
    print(f"✅ Удалено старых обработчиков: {n}")

# 3) Ensure the new handler is at the very end
if not new_src.endswith("\n"):
    new_src += "\n"
new_src += "\n" + FIXED

# Write backup and new file
BACKUP.write_text(SRC, encoding="utf-8")
BOT.write_text(new_src, encoding="utf-8")
print("✅ Готово! Создал bot.py.bak и обновил bot.py")
