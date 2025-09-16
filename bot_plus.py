
"""
LivePlace Addon Plus (non-destructive)
-------------------------------------
This file **doesn't touch your original bot.py**.
It imports it as a module, registers extra handlers, and starts polling.

Adds:
- /go <querystring> : deep-link search (e.g. /go city=Tbilisi&district=Vake&rooms=2&price=500-1000&mode=rent&type=Apartment)
- /repeat           : last 3 searches per user
- /alert            : subscribe to a filter; background loop sends new matches (based on `published`)
- /alerts_list, /alerts_off : manage subscriptions

Storage: ./data/recent.json, ./data/alerts.json
"""

import asyncio, json, os
from datetime import datetime
from urllib.parse import parse_qs

# --- Import your original bot module (must be in the same folder as this file)
import bot as core

dp = core.dp
bot = core.bot

RECENT_PATH = os.path.join(os.path.dirname(__file__), "data", "recent.json")
ALERTS_PATH = os.path.join(os.path.dirname(__file__), "data", "alerts.json")

def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)

def _push_recent(uid: int, filt: dict, maxlen: int = 3):
    store = _load_json(RECENT_PATH, {})
    arr = store.get(str(uid), [])
    # dedupe identical
    if arr and arr[0] == filt:
        pass
    else:
        arr.insert(0, filt)
        arr = arr[:maxlen]
    store[str(uid)] = arr
    _save_json(RECENT_PATH, store)

def _get_recent(uid: int):
    store = _load_json(RECENT_PATH, {})
    return store.get(str(uid), [])

def _set_alert(uid: int, filt: dict, cooldown_h: int = 3):
    store = _load_json(ALERTS_PATH, {})
    store[str(uid)] = {"filt": filt, "cooldown_h": cooldown_h, "last_iso": ""}
    _save_json(ALERTS_PATH, store)

def _get_alert(uid: int):
    store = _load_json(ALERTS_PATH, {})
    return store.get(str(uid))

def _update_alert_ts(uid: int):
    store = _load_json(ALERTS_PATH, {})
    rec = store.get(str(uid))
    if not rec: return
    rec["last_iso"] = datetime.utcnow().isoformat(timespec="seconds")
    store[str(uid)] = rec
    _save_json(ALERTS_PATH, store)

def _iter_alerts():
    store = _load_json(ALERTS_PATH, {})
    for k, v in store.items():
        yield int(k), v

def _parse_price(code: str):
    try:
        a,b = code.split("-")
        return int(a), int(b)
    except Exception:
        return None, None

def _parse_rooms(s: str):
    s = (s or "").strip().lower().replace("+","")
    if s in {"студия","studio","stud","სტუდიო"}:
        return 0.5, 0.5
    try:
        n = float(s)
        return n, n
    except Exception:
        return None, None

def _parse_query(qs: str):
    """Returns dict for core.finish_search"""
    q = parse_qs((qs or "").strip())
    filt = {}
    if "mode" in q:      filt["mode"] = q["mode"][0]
    if "city" in q:      filt["city"] = q["city"][0]
    if "district" in q:  filt["district"] = q["district"][0]
    if "type" in q:      filt["type"] = q["type"][0]
    if "price" in q:
        a,b = _parse_price(q["price"][0])
        if a is not None: filt["price_min"] = a
        if b is not None: filt["price_max"] = b
    if "rooms" in q:
        a,b = _parse_rooms(q["rooms"][0])
        if a is not None: filt["rooms_min"] = a
        if b is not None: filt["rooms_max"] = b
    return filt

# -------- Handlers ---------
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

@dp.message_handler(commands=["go"])
async def cmd_go(message: types.Message):
    args = message.get_args() or ""
    filt = _parse_query(args)
    if not filt:
        return await message.answer("Пример: /go city=Tbilisi&district=Vake&rooms=2&price=500-1000&mode=rent&type=Apartment")
    # ensure language exists
    lang = core.USER_LANG.get(message.from_user.id, "ru")
    await message.answer("⏳ Ищу варианты...")
    try:
        await core.finish_search(message, message.from_user.id, filt)
        _push_recent(message.from_user.id, filt)
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

@dp.message_handler(commands=["repeat"])
async def cmd_repeat(message: types.Message):
    lang = core.USER_LANG.get(message.from_user.id, "ru")
    recent = _get_recent(message.from_user.id)
    if not recent:
        return await message.answer("Пока нет последних запросов.")
    kb = InlineKeyboardMarkup()
    for i, f in enumerate(recent, 1):
        label = f"{f.get('mode','*')} • {f.get('city','*')}/{f.get('district','*')} • {f.get('type','*')} • rooms {f.get('rooms_min','*')} • ${f.get('price_min','*')}-{f.get('price_max','*')}"
        kb.add(InlineKeyboardButton(f"{i}) {label}", callback_data=f"repeat:{i-1}"))
    kb.add(InlineKeyboardButton(core.T['btn_home'][lang], callback_data="home"))
    await message.answer("Недавние запросы:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("repeat:"))
async def cb_repeat(c: types.CallbackQuery):
    idx = int(c.data.split(":",1)[1])
    arr = _get_recent(c.from_user.id)
    if not (0 <= idx < len(arr)):
        return await c.answer("Нет данных")
    filt = arr[idx]
    try:
        await core.finish_search(c.message, c.from_user.id, filt)
    except Exception as e:
        await c.message.answer(f"Ошибка: {e}")
    await c.answer()

@dp.message_handler(commands=["alert"])
async def cmd_alert(message: types.Message, state: FSMContext):
    # take the latest search from USER_RESULTS
    data = core.USER_RESULTS.get(message.from_user.id, {})
    rows = data.get("rows", [])
    if not rows:
        return await message.answer("Сначала сделайте поиск (через меню или /go), потом повторите /alert.")
    # try to reconstruct filter based on last finish_search inputs stored in state or in results context
    filt = {}
    context = data.get("context") or {}
    # We store nothing in context from core; so ask user to rely on /go + /alert flow or wizard
    # As fallback, subscribe to mode/city/district of current page
    idx = data.get("idx", 0)
    if 0 <= idx < len(rows):
        r = rows[idx]
        filt = {
            "mode": core.norm_mode(r.get("mode","")),
            "city": (r.get("city") or ""),
            "district": (r.get("district") or ""),
        }
    _set_alert(message.from_user.id, filt, cooldown_h=3)
    await message.answer("🔔 Подписка сохранена. Я пришлю новые совпадения (не чаще 1 раза в 3 часа).")

@dp.message_handler(commands=["alerts_list"])
async def cmd_alerts_list(message: types.Message):
    rec = _get_alert(message.from_user.id)
    if not rec:
        return await message.answer("У вас нет активной подписки.")
    await message.answer(f"Текущая подписка: {rec.get('filt')} (cooldown {rec.get('cooldown_h')} ч)")

@dp.message_handler(commands=["alerts_off"])
async def cmd_alerts_off(message: types.Message):
    store = _load_json(ALERTS_PATH, {})
    if str(message.from_user.id) in store:
        del store[str(message.from_user.id)]
        _save_json(ALERTS_PATH, store)
        return await message.answer("Подписка отключена.")
    await message.answer("Подписка не найдена.")

# -------- Background: fire alerts --------
async def _alerts_loop():
    while True:
        try:
            for uid, rec in _iter_alerts():
                try:
                    await _try_fire_alert(uid, rec)
                except Exception as e:
                    # keep going even if one user fails
                    pass
        except Exception:
            pass
        await asyncio.sleep(180)  # every 3 min

async def _try_fire_alert(uid: int, rec: dict):
    lang = core.USER_LANG.get(uid, "ru")
    filt = rec.get("filt", {})
    last_iso = rec.get("last_iso")  # ISO string
    cooldown_h = int(rec.get("cooldown_h", 3))

    # honor cooldown
    if last_iso:
        try:
            from datetime import datetime, timedelta
            last_dt = datetime.fromisoformat(last_iso)
            if datetime.utcnow() - last_dt < timedelta(hours=cooldown_h):
                return
        except Exception:
            pass

    rows = await core.rows_async()
    # Filter rows: match by keys in filt
    def norm(s): return (s or "").strip().lower()
    matched = []
    for r in rows:
        ok = True
        if "mode" in filt and core.norm_mode(r.get("mode")) != core.norm_mode(filt["mode"]):
            ok = False
        if ok and "city" in filt and norm(r.get("city")) != norm(filt["city"]):
            ok = False
        if ok and "district" in filt and norm(r.get("district")) != norm(filt["district"]):
            ok = False
        if not ok: 
            continue
        # "new" means published newer than last_iso
        pub = str(r.get("published","")).strip()
        is_new = True
        if last_iso:
            try:
                is_new = datetime.fromisoformat(pub) > datetime.fromisoformat(last_iso)
            except Exception:
                # if parse fails, treat as not-new
                is_new = False
        if is_new:
            matched.append(r)

    if not matched:
        return

    # send up to 5
    for r in matched[:5]:
        text = core.format_card(r, lang)
        photos = core.collect_photos(r)[:10]
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("👉 Открыть", switch_inline_query=""))
        # send album-or-text similar to core.show_current_card()
        try:
            if len(photos) >= 2:
                media = [types.InputMediaPhoto(media=photos[0], caption=text, parse_mode="HTML")]
                for p in photos[1:]: media.append(types.InputMediaPhoto(media=p))
                await bot.send_media_group(uid, media)
                await bot.send_message(uid, "\u2063", reply_markup=kb)
            elif len(photos) == 1:
                await bot.send_photo(uid, photos[0], caption=text, parse_mode="HTML")
                await bot.send_message(uid, "\u2063", reply_markup=kb)
            else:
                await bot.send_message(uid, text, reply_markup=kb)
        except Exception:
            # fallback
            try:
                await bot.send_message(uid, text or "🔔 Новое объявление", reply_markup=kb)
            except Exception:
                pass

    _update_alert_ts(uid)

# --- Startup
async def on_startup_plus(dp):
    try:
        asyncio.create_task(_alerts_loop())
    except Exception:
        pass

if __name__ == "__main__":
    # Register startup and run
    from aiogram import executor
    executor.start_polling(dp, on_startup=on_startup_plus)
