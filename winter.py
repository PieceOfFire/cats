"""
winter.py

Модуль для временного новогоднего ивента.
- Отдельный лист пользователей: `winter2026`
- Лист с котами ивента: `winter_cats`
- Лист рейтинг/лидерборд: `winter_top`
- Лист с адвент-наградами: `winter_advent`

Изменения:
- Адвент: сетка 4 x 5, кнопки показывают только число + статус.
- Удалён журнал покупок (winter_purchases).
- Покупка в магазине переработана: свежие чтения, корректные проверки и обновления.
"""
from multiprocessing import context
import os
import time
import random
import re
import logging
from datetime import datetime, timedelta, date
import aiohttp
from io import BytesIO

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler
from telegram.error import BadRequest
from telegram import InputFile


import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

# -------------------------- Настройки / Константы --------------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
BONUS_CHANNEL = "@gg_ssr"
SPREADSHEET_KEY = os.environ["SPREADSHEET_KEY"]
CREDENTIALS_FILE = "/etc/secrets/cats-476112-9a44bf3e38e2.json"

# именование листов
WINTER_USERS_SHEET = "winter2026"
WINTER_CATS_SHEET = "winter_cats"
WINTER_LEADER_SHEET = "winter_top"
WINTER_ADVENT_SHEET = "winter_advent"

# cache
_WINTER_CATS_CACHE = {"ts": 0, "data": None}
CATS_TTL = 300

# лимит спинов
MAX_WINTER_SPINS = 999
CASHBACK_PER_SPIN = 10

# редкости
RARITY_WEIGHTS_WINTER = {
    "COM": 55,
    "UCOM": 27,
    "RARE": 12,
    "EPIC": 6,
}

RARITY_STYLES_WINTER = {
    "COM":  "❄️ Обычная находка ⭐",
    "UCOM": "🎁 Редкий подарок ⭐⭐",
    "RARE": "🎄 Волшебный приз ⭐⭐⭐",
    "EPIC": "🎆 Новогоднее чудо ⭐⭐⭐⭐",
}

# админ
ADMIN_ID = 1848758956

# удача
MAX_LUCK = 100
LUCK_PER_COMMON = 2
LUCK_DECREASE_ON_RARE = 10
LUCK_WEIGHT_SCALE = 4
GUARANTEED_EPIC_LUCK = 60

FRAME_DEFAULT = 10
FRAME_MAX = 12

# Адвент: дефолтный период — 22 декабря → 10 января (включительно)
ADVENT_DEFAULT_START_MONTH = 12
ADVENT_DEFAULT_START_DAY = 22
ADVENT_DEFAULT_END_MONTH = 1
ADVENT_DEFAULT_END_DAY = 10

# -------------------------- GSheets helpers --------------------------

def gs_client():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)

def _open_wb():
    client = gs_client()
    return client.open_by_key(SPREADSHEET_KEY)

def sheet_winter_users():
    wb = _open_wb()
    try:
        return wb.worksheet(WINTER_USERS_SHEET)
    except Exception:
        return wb.add_worksheet(title=WINTER_USERS_SHEET, rows=1000, cols=40)

def sheet_winter_cats():
    wb = _open_wb()
    try:
        return wb.worksheet(WINTER_CATS_SHEET)
    except Exception:
        return wb.add_worksheet(title=WINTER_CATS_SHEET, rows=1000, cols=20)

def sheet_winter_leader():
    wb = _open_wb()
    try:
        return wb.worksheet(WINTER_LEADER_SHEET)
    except Exception:
        return wb.add_worksheet(title=WINTER_LEADER_SHEET, rows=1000, cols=20)

def sheet_winter_advent():
    wb = _open_wb()
    try:
        return wb.worksheet(WINTER_ADVENT_SHEET)
    except Exception:
        sh = wb.add_worksheet(title=WINTER_ADVENT_SHEET, rows=64, cols=10)
        sh.append_row(["DAY", "SPINS", "CURRENCY", "LUCK"], value_input_option="USER_ENTERED")
        return sh

def sheet_winter_shop():
    wb = _open_wb()
    try:
        return wb.worksheet("winter_shop")
    except Exception:
        sh = wb.add_worksheet(title="winter_shop", rows=200, cols=30)
        headers = ["ITEM_ID", "NAME", "DESCRIPTION", "TYPE", "PRICE", "SPINS", "LUCK", "CARD_ID", "IMAGE_URL", "RARITY", "QUANTITY"]
        sh.append_row(headers, value_input_option="USER_ENTERED")
        return sh

# -------------------------- Utility for columns --------------------------

def colnum_to_letter(n):
    string = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        string = chr(65 + rem) + string
    return string

def column_letter_by_name(sheet, name):
    headers = sheet.row_values(1)
    for idx, h in enumerate(headers, start=1):
        if str(h).strip().upper() == name.upper():
            return colnum_to_letter(idx)
    next_idx = len(headers) + 1
    sheet.update([[name]], f"{colnum_to_letter(next_idx)}1")
    return colnum_to_letter(next_idx)

# -------------------------- Winter sheet user helpers --------------------------

def find_winter_user_row(sheet, user_id):
    try:
        cell = sheet.find(str(user_id), in_column=1)
        row = cell.row
        headers = sheet.row_values(1)
        row_values = sheet.row_values(row)
        if len(row_values) < len(headers):
            row_values += [""] * (len(headers) - len(row_values))
        record = dict(zip(headers, row_values))
        return row, record
    except Exception:
        return None, None

def create_new_winter_user(sheet, user_id):
    row_values = [user_id, "", "", 3, 0, 0, "", 0, "", "", "", " ", 10]
    sheet.append_row(row_values, value_input_option="USER_ENTERED")
    return 3

# -------------------------- Cats cache --------------------------

def clean_cat_records(records):
    cleaned = []
    for r in records:
        cid = r.get("ID") or r.get("Id") or r.get("id")
        url = (r.get("URL") or r.get("Url") or r.get("url") or "").strip()
        desc = (r.get("DESC") or r.get("Desc") or r.get("description") or "").strip()
        rarity = (r.get("RARITY") or r.get("Rarity") or r.get("rarity") or "COM").upper().strip()
        cleaned.append({"id": cid, "url": url, "desc": desc, "rarity": rarity})
    return cleaned

def get_winter_cats_cached():
    now = time.time()
    if _WINTER_CATS_CACHE["data"] is not None and (now - _WINTER_CATS_CACHE["ts"]) < CATS_TTL:
        return _WINTER_CATS_CACHE["data"]
    s = sheet_winter_cats()
    try:
        records = s.get_all_records()
        cats = clean_cat_records(records)
    except Exception as e:
        logger.exception("Ошибка чтения winter_cats: %s", e)
        cats = []
    _WINTER_CATS_CACHE["data"] = cats
    _WINTER_CATS_CACHE["ts"] = now
    return cats

# -------------------------- Advent calendar helpers --------------------------

def ensure_advent_table(days_count=20):
    s = sheet_winter_advent()
    rows = s.get_all_records()
    if len(rows) >= days_count:
        return
    for d in range(len(rows) + 1, days_count + 1):
        s.append_row([d, 1, 5, 0], value_input_option="USER_ENTERED")

def get_advent_days_count():
    s = sheet_winter_advent()
    rows = s.get_all_records()
    return len(rows)

def get_advent_reward_for_day(day_index):
    s = sheet_winter_advent()
    rows = s.get_all_records()
    if 1 <= day_index <= len(rows):
        r = rows[day_index - 1]
        return int(r.get("SPINS") or 0), int(r.get("CURRENCY") or 0), int(r.get("LUCK") or 0)
    return 0, 0, 0

def _default_advent_start_end():
    """
    Возвращает (start_date, end_date) для адвента.
    Ожидаемый стандарт: старт 22 декабря, конец 10 января (пересекает год).
    Логика:
      - Если заданы WINTER_EVENT_START / WINTER_EVENT_END в env — парсим их.
      - Иначе:
          * если сейчас декабрь  -> берем этот декабрь как старт и январь следующего года как конец
          * если сейчас январь   -> берем декабрь предыдущего года как старт и этот январь как конец
          * иначе                -> берем ближайший декабрь этого года как старт и январь следующего года как конец
    Это корректно обрабатывает переход через новый год.
    """
    start_str = os.environ.get("WINTER_EVENT_START")
    end_str = os.environ.get("WINTER_EVENT_END")
    try:
        if start_str:
            start_date = datetime.fromisoformat(start_str).date()
        else:
            now = datetime.utcnow().date()
            if now.month == 12:
                start_date = date(now.year, ADVENT_DEFAULT_START_MONTH, ADVENT_DEFAULT_START_DAY)
            elif now.month == 1:
                # если январь — старт был в декабре предыдущего года
                start_date = date(now.year - 1, ADVENT_DEFAULT_START_MONTH, ADVENT_DEFAULT_START_DAY)
            else:
                # для остальных месяцев считаем, что ближайший адвент начнётся в декабре этого года
                start_date = date(now.year, ADVENT_DEFAULT_START_MONTH, ADVENT_DEFAULT_START_DAY)

        if end_str:
            end_date = datetime.fromisoformat(end_str).date()
        else:
            # если старт в декабре — конец в январе следующего года
            if start_date.month == 12 and ADVENT_DEFAULT_END_MONTH == 1:
                end_date = date(start_date.year + 1, ADVENT_DEFAULT_END_MONTH, ADVENT_DEFAULT_END_DAY)
            else:
                end_date = date(start_date.year, ADVENT_DEFAULT_END_MONTH, ADVENT_DEFAULT_END_DAY)

        # debug/log — полезно для отладки после деплоя
        try:
            logger.debug("Advent window resolved: %s -> %s", start_date, end_date)
        except Exception:
            pass

        return start_date, end_date
    except Exception:
        # безопасный fallback: если что-то пошло не так — строим логически ожидаемое окно
        now = datetime.utcnow().date()
        if now.month == 1:
            return date(now.year - 1, ADVENT_DEFAULT_START_MONTH, ADVENT_DEFAULT_START_DAY), date(now.year, ADVENT_DEFAULT_END_MONTH, ADVENT_DEFAULT_END_DAY)
        else:
            s = date(now.year, ADVENT_DEFAULT_START_MONTH, ADVENT_DEFAULT_START_DAY)
            if s.month == 12 and ADVENT_DEFAULT_END_MONTH == 1:
                return s, date(s.year + 1, ADVENT_DEFAULT_END_MONTH, ADVENT_DEFAULT_END_DAY)
            return s, date(s.year, ADVENT_DEFAULT_END_MONTH, ADVENT_DEFAULT_END_DAY)


def read_user_advent_state(s_users, row, days_count):
    headers = s_users.row_values(1)
    upper_headers = [h.upper() for h in headers]
    if "ADVENT_STATE" not in upper_headers:
        s_users.update([["ADVENT_STATE"]], f"{colnum_to_letter(len(headers)+1)}1")
        headers = s_users.row_values(1)
        upper_headers = [h.upper() for h in headers]
    try:
        row_vals = s_users.row_values(row)
        idx = upper_headers.index('ADVENT_STATE')
        state = row_vals[idx] if len(row_vals) > idx else ""
    except Exception:
        state = ""
    if not state or len(state) < days_count:
        state = ("w" * days_count)
    return state

def ensure_user_advent_state(s_users, row):
    days = get_advent_days_count()
    if days <= 0:
        ensure_advent_table(days_count=20)
        days = get_advent_days_count()

    state = read_user_advent_state(s_users, row, days)
    start_date, end_date = _default_advent_start_end()
    today = datetime.utcnow().date()

    if today < start_date:
        day_index = 0
    elif today > end_date:
        day_index = days
    else:
        day_index = (today - start_date).days + 1
        if day_index < 0:
            day_index = 0
        if day_index > days:
            day_index = days

    new_state = list(state)
    if len(new_state) < days:
        new_state += ["w"] * (days - len(new_state))
    elif len(new_state) > days:
        new_state = new_state[:days]

    for i in range(days):
        if i < day_index:
            if new_state[i] not in ("1", "0"):
                new_state[i] = "0"
        else:
            new_state[i] = "w"
    new_state_str = "".join(new_state)
    if new_state_str != state:
        try:
            s_users.update([[new_state_str]], f"{column_letter_by_name(s_users, 'ADVENT_STATE')}{row}")
        except Exception:
            logger.exception("Не удалось обновить ADVENT_STATE")
    return new_state_str

def claim_advent_day(s_users, row, day_idx):
    days = get_advent_days_count()
    if day_idx < 1 or day_idx > days:
        return False, "Недопустимый день"
    state = read_user_advent_state(s_users, row, days)
    if len(state) < days:
        state = ensure_user_advent_state(s_users, row)
    ch = state[day_idx - 1]
    if ch == '1':
        return False, "Уже получено"
    if ch == 'w':
        return False, "День ещё не настал"
    spins, cur, luck_gain = get_advent_reward_for_day(day_idx)

    headers = s_users.row_values(1)
    row_vals = s_users.row_values(row)

    def get_header_val(hname):
        upper = [h.upper() for h in headers]
        if hname.upper() in upper:
            idx = upper.index(hname.upper())
            return row_vals[idx] if idx < len(row_vals) else ""
        return ""

    try:
        spins_old = int(get_header_val('WINTER_SPINS') or 0)
    except Exception:
        spins_old = 0
    try:
        cur_old = int(get_header_val('WINTER_CURRENCY') or 0)
    except Exception:
        cur_old = 0
    try:
        luck_old = int(get_header_val('LUCK_HIDDEN') or 0)
    except Exception:
        luck_old = 0

    spins_new = min(spins_old + spins, MAX_WINTER_SPINS)
    cur_new = cur_old + cur
    luck_new = min(MAX_LUCK, luck_old + luck_gain)

    s_users.update([[spins_new]], f"{column_letter_by_name(s_users, 'WINTER_SPINS')}{row}")
    s_users.update([[cur_new]], f"{column_letter_by_name(s_users, 'WINTER_CURRENCY')}{row}")
    s_users.update([[luck_new]], f"{column_letter_by_name(s_users, 'LUCK_HIDDEN')}{row}")

    state_list = list(state)
    state_list[day_idx - 1] = '1'
    new_state = "".join(state_list)
    s_users.update([[new_state]], f"{column_letter_by_name(s_users, 'ADVENT_STATE')}{row}")

    if spins  == 1:
        return True, f"Забрано: +{spins} спин, как-то мало, может хотя бы повезет?)"
    else:
        return True, f"Забрано: +{spins} спин(ов), +{cur} монет"

# -------------------------- UI / Menu --------------------------

def get_winter_menu_text(record=None):
    spins = 0
    currency = 0
    nick_display = "Игрок"
    if record:
        try:
            spins = int(record.get("WINTER_SPINS") or 0)
        except Exception:
            spins = 0
        try:
            currency = int(record.get("WINTER_CURRENCY") or 0)
        except Exception:
            currency = 0
        nick = str(record.get("NICK") or "").strip()
        if nick:
            nick_display = nick
        else:
            uid = str(record.get("USER_ID") or "")
            nick_display = f"#{uid[-6:]}" if uid else "Игрок"
    return f"❄️ Новогодний режим\nПользователь: {nick_display}\n\n🎰 Спины (зимние): {spins}\n✨ Валюта: {currency}\n\nВыберите действие:"

def get_winter_menu_markup(is_admin=False):
    kb = []
    kb.append([InlineKeyboardButton("🎰 Спин (зимний)", callback_data="winter_spin")])
    kb.append([InlineKeyboardButton("🏪 Магазин", callback_data="winter_shop"),
               InlineKeyboardButton("🖼 Рамка", callback_data="frame_open")])
    kb.append([InlineKeyboardButton("🎮 Игры", callback_data="winter_games"),
               InlineKeyboardButton("🗓 Адвент", callback_data="winter_advent")])
    kb.append([InlineKeyboardButton("🏔 Топ", callback_data="winter_top"),
               InlineKeyboardButton("✏️ Ник", callback_data="winter_change_nick")])
    kb.append([InlineKeyboardButton("⬅️ В главное меню", callback_data="winter_back_main")])
    return InlineKeyboardMarkup(kb)

# -------------------------- Core: spin + luck --------------------------

def choose_rarity(weights, luck=0):
    rarities = list(weights.keys())
    w = list(weights.values())
    if luck and luck > 0:
        bonus = luck // LUCK_WEIGHT_SCALE
        if bonus > 0:
            bonus_rare = int(bonus * 0.7)
            bonus_epic = bonus - bonus_rare
            w = w.copy()
            for i, r in enumerate(rarities):
                if r == 'RARE':
                    w[i] = w[i] + bonus_rare
                elif r == 'EPIC':
                    w[i] = w[i] + bonus_epic
    return random.choices(rarities, weights=w, k=1)[0]

def adjust_luck_after_spin(s_users, row, gained_rarity):
    try:
        headers = s_users.row_values(1)
        row_vals = s_users.row_values(row)
        upper = [h.upper() for h in headers]
        if 'LUCK_HIDDEN' in upper:
            idx = upper.index('LUCK_HIDDEN')
            cur = int(row_vals[idx] or 0) if idx < len(row_vals) else 0
        else:
            cur = 0
    except Exception:
        cur = 0
    if gained_rarity in ('COM', 'UCOM'):
        cur = min(MAX_LUCK, cur + LUCK_PER_COMMON)
    elif gained_rarity in ('RARE'):
        cur = cur
    else:
        cur = max(0, cur - LUCK_DECREASE_ON_RARE)
    try:
        s_users.update([[cur]], f"{column_letter_by_name(s_users, 'LUCK_HIDDEN')}{row}")
    except Exception:
        logger.exception("Не удалось обновить LUCK_HIDDEN")
    return cur

async def send_card_message(chat_id: int, card_id, context):
    """
    Надёжно отправляет карточку пользователю:
      - ищет карточку в кэше get_winter_cats_cached() (предпочтительно),
      - берёт url/desc/rarity,
      - сначала пытается отправить photo(url, caption),
      - если не получается — скачивает bytes и отправляет InputFile,
      - если нет картинки — отправляет текст.
    """
    try:
        cats = get_winter_cats_cached() or []
    except Exception:
        cats = []

    card = None
    for c in cats:
        if c is None:
            continue
        if str(c.get("id")) == str(card_id):
            card = c
            break

    # fallback: если кэш пуст или не нашлось — попробуем прочитать сырые записи
    if not card:
        try:
            s_cats = sheet_winter_cats()
            raw = s_cats.get_all_records()
            for r in raw:
                # стандартные имена полей в таблице
                if str(r.get("ID") or r.get("Id") or r.get("id") or r.get("CARD_ID") or r.get("ITEM_ID")) == str(card_id):
                    # normalize to same keys as clean_cat_records
                    card = {
                        "id": r.get("ID") or r.get("Id") or r.get("id") or r.get("CARD_ID") or r.get("ITEM_ID"),
                        "url": (r.get("URL") or r.get("Url") or r.get("url") or r.get("IMAGE_URL") or r.get("IMAGE") or "").strip(),
                        "desc": (r.get("DESC") or r.get("Desc") or r.get("DESCRIPTION") or r.get("description") or "").strip(),
                        "rarity": (r.get("RARITY") or r.get("Rarity") or r.get("rarity") or "COM").upper().strip(),
                        "name": (r.get("NAME") or r.get("TITLE") or "").strip()
                    }
                    break
        except Exception:
            logger.exception("send_card_message: fallback read winter_cats failed")

    if not card:
        # ничего не найдено — отправим простое уведомление
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"Получена карточка: ID {card_id}")
        except Exception:
            pass
        return

    # соберём подпись
    rarity_label = RARITY_STYLES_WINTER.get((card.get("rarity") or "").upper(), card.get("rarity") or "")
    name = card.get("name") or ""
    desc = (card.get("desc") or "").strip()
    url = (card.get("url") or "").strip()

    caption_lines = []
    if name:
        caption_lines.append(f"{name}")
    if rarity_label:
        caption_lines.append(rarity_label)
    if desc:
        caption_lines.append("")
        caption_lines.append(desc)
    caption_lines.append("")
    caption_lines.append(f"🆔 ID: {card_id}")
    caption = "\n".join([ln for ln in caption_lines if ln is not None]).strip()

    # helper: convert google drive links to direct download if needed
    def _convert_drive_link(u: str):
        if not u:
            return u
        lu = u.lower()
        if "drive.google.com" in lu:
            if "/d/" in u:
                fid = u.split("/d/")[1].split("/")[0]
                return f"https://drive.google.com/uc?export=download&id={fid}"
            if "id=" in u:
                fid = u.split("id=")[1].split("&")[0]
                return f"https://drive.google.com/uc?export=download&id={fid}"
        return u

    if url:
        url = _convert_drive_link(url)

    # Отправка: первый попытка — send_photo(url, caption) (работает если Telegram/URL поддерживается)
    try:
        if url:
            # try direct send by url / file_id
            # if url looks like a file_id (no http and short) — try as file_id
            if (not url.lower().startswith("http")) and " " not in url and len(url) < 200:
                try:
                    await context.bot.send_photo(chat_id=chat_id, photo=url, caption=caption)
                    return
                except Exception:
                    logger.exception("send_card_message: send_photo with file_id failed, will try to download or send link")
            else:
                # try send by url first (most efficient)
                try:
                    await context.bot.send_photo(chat_id=chat_id, photo=url, caption=caption)
                    return
                except Exception:
                    logger.warning("send_card_message: send_photo by URL failed, will try download")
                    # try download bytes
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(url, timeout=20) as resp:
                                if resp.status == 200:
                                    content = await resp.read()
                                    if content:
                                        bio = BytesIO(content)
                                        bio.name = f"card_{card_id}.jpg"
                                        bio.seek(0)
                                        await context.bot.send_photo(chat_id=chat_id, photo=InputFile(bio, filename=bio.name), caption=caption)
                                        return
                                else:
                                    logger.warning("send_card_message: download returned status %s for %s", resp.status, url)
                    except Exception:
                        logger.exception("send_card_message: download-bytes attempt failed")
        # если картинка не отправилась — отправим текст с (опциональной) ссылкой
        text = caption or f"Карточка ID {card_id}"
        if url:
            text += f"\n\n{url}"
        await context.bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        logger.exception("send_card_message: final fallback failed")
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"Карточка получена: ID {card_id}")
        except Exception:
            pass


async def safe_edit_msg(query, text, reply_markup=None):
    msg = query.message
    try:
        if msg.text is not None:
            await msg.edit_text(text, reply_markup=reply_markup)
        elif msg.caption is not None:
            await msg.edit_caption(caption=text, reply_markup=reply_markup)
        else:
            await query.bot.send_message(
                chat_id=msg.chat_id,
                text=text,
                reply_markup=reply_markup
            )
    except Exception:
        try:
            await query.bot.send_message(
                chat_id=msg.chat_id,
                text=text,
                reply_markup=reply_markup
            )
        except Exception:
            pass


async def safe_edit_message(msg, text: str = None, reply_markup=None):
    """
    Редактирует text если это текстовое сообщение,
    или caption если это медиа (photo/document/video).
    Если ни text ни caption не доступны — пробуем edit_reply_markup.
    В крайнем случае отправляем новое сообщение (reply_text).
    Используйте: await safe_edit_message(query.message, "текст", reply_markup=kb)
    """
    if msg is None:
        return
    try:
        # если это текстовое сообщение — редактируем text
        if getattr(msg, "text", None) is not None:
            await msg.edit_text(text or "", reply_markup=reply_markup)
            return
        # если это медиа с подписью — редактируем caption
        if getattr(msg, "caption", None) is not None:
            await msg.edit_caption(caption=text or "", reply_markup=reply_markup)
            return
        # если ничего из вышеперечисленного — обновляем только клавиатуру (если есть)
        if reply_markup is not None:
            try:
                await msg.edit_reply_markup(reply_markup=reply_markup)
                return
            except Exception:
                # не фатально — дальше fallback
                logger.exception("edit_reply_markup failed in safe_edit_message")
        # fallback: отправляем новое сообщение в чат, чтобы пользователь увидел результат
        try:
            await msg.reply_text(text or "", reply_markup=reply_markup)
        except Exception:
            logger.exception("fallback reply_text failed in safe_edit_message")
    except BadRequest as br:
        # самый частый случай — "There is no text in the message to edit"
        logger.warning("safe_edit_message BadRequest: %s", br)
        # попытаемся редактировать caption (ещё одна попытка)
        try:
            if getattr(msg, "caption", None) is not None:
                await msg.edit_caption(caption=text or "", reply_markup=reply_markup)
                return
        except Exception:
            logger.exception("safe_edit_message second attempt edit_caption failed")
        try:
            await msg.reply_text(text or "", reply_markup=reply_markup)
        except Exception:
            logger.exception("safe_edit_message final fallback failed")
    except Exception:
        logger.exception("safe_edit_message unexpected error")

async def handle_winter_spin_and_send(chat_id, user_id, context: ContextTypes.DEFAULT_TYPE):
    s_users = sheet_winter_users()
    row, record = find_winter_user_row(s_users, user_id)
    if record is None:
        create_new_winter_user(s_users, user_id)
        row, record = find_winter_user_row(s_users, user_id)

    try:
        spins = int(record.get("WINTER_SPINS") or 0)
    except Exception:
        spins = 0

    if spins <= 0:
        await context.bot.send_message(chat_id=chat_id, text="😿 У тебя нет зимних спинов! Попробуй завтра или в магазине.")
        return

    cats = get_winter_cats_cached()
    if not cats:
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Каталог зимних котят недоступен. Попробуй позже.")
        return

    cats_id_raw = record.get("W_CATS_ID") or ""
    owned_tokens = [t.strip() for t in re.split(r"[|,;\\s]+", str(cats_id_raw)) if t.strip()]
    owned_set = set(owned_tokens)
    all_cat_ids = {str(c.get("id")) for c in cats if c.get("id") is not None}
    not_owned_ids = list(all_cat_ids - owned_set)

    if not not_owned_ids:
        await context.bot.send_message(chat_id=chat_id, text="🎉 У тебя уже все зимние карточки! Спин не потрачен.")
        return

    # read current luck (hidden)
    try:
        luck = int(record.get('LUCK_HIDDEN') or 0)
    except Exception:
        luck = 0

    # аггрегируем boost с адвент-дня (плоская добавка)
    effective_luck = min(MAX_LUCK, luck)

    # GUARANTEED EPIC: если effective_luck >= порог, выдаём EPIC гарантированно
    if effective_luck >= GUARANTEED_EPIC_LUCK:
        available_unowned_epic = [c for c in cats if c["rarity"] == "EPIC" and str(c["id"]) not in owned_set]
        if available_unowned_epic:
            chosen = random.choice(available_unowned_epic)
        else:
            unowned_cats = [c for c in cats if str(c["id"]) not in owned_set]
            if not unowned_cats:
                await context.bot.send_message(chat_id=chat_id, text="🎉 У тебя уже все карточки! Спин не потрачен.")
                return
            chosen = random.choice(unowned_cats)
        try:
            effective_luck -= GUARANTEED_EPIC_LUCK
            s_users.update([[effective_luck]], f"{column_letter_by_name(s_users, 'LUCK_HIDDEN')}{row}")
        except Exception:
            logger.exception("Не удалось сбросить LUCK_HIDDEN после гарантии эпика")
        rarity = chosen.get("rarity")
    else:
        rarity = choose_rarity(RARITY_WEIGHTS_WINTER, luck=effective_luck)
        available_unowned = [c for c in cats if c["rarity"] == rarity and str(c["id"]) not in owned_set]
        if available_unowned:
            chosen = random.choice(available_unowned)
        else:
            unowned_cats = [c for c in cats if str(c["id"]) not in owned_set]
            if not unowned_cats:
                await context.bot.send_message(chat_id=chat_id, text="🎉 У тебя уже все карточки. Спин не потрачен.")
                return
            chosen = random.choice(unowned_cats)


    # debit spin
    new_spins = spins - 1
    try:
        spin_col = column_letter_by_name(s_users, "WINTER_SPINS")
        s_users.update([[new_spins]], f"{spin_col}{row}", value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception("Не удалось списать зимний спин: %s", e)
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Ошибка базы: не удалось списать спин. Попробуй позже.")
        return

    chosen_id_str = str(chosen.get("id"))
    owned_set.add(chosen_id_str)
    try:
        sorted_ids = sorted(owned_set, key=lambda x: (int(x) if x.isdigit() else float('inf'), x))
    except Exception:
        sorted_ids = sorted(owned_set)
    new_cats_id = " | ".join(sorted_ids)
    try:
        col_wcats = column_letter_by_name(s_users, "W_CATS_ID")
        s_users.update([[new_cats_id]], f"{col_wcats}{row}", value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception("Не удалось обновить W_CATS_ID: %s", e)

    points_map = {"COM": 1, "UCOM": 2, "RARE": 5, "EPIC": 12}
    gained = points_map.get(chosen.get("rarity"), 0)
    try:
        sum_col = column_letter_by_name(s_users, "SUM")
    except Exception:
        sum_col = None

    try:
        current_sum_raw = record.get("SUM")
        try:
            current_sum = int(current_sum_raw or 0)
        except Exception:
            current_sum = 0
    except Exception:
        current_sum = 0
    new_sum = current_sum + gained
    if sum_col:
        try:
            s_users.update([[new_sum]], f"{sum_col}{row}")
        except Exception:
            logger.exception("Не удалось обновить SUM в winter sheet")

    try:
        adjust_luck_after_spin(s_users, row, chosen.get("rarity"))
    except Exception:
        logger.exception("Не удалось корректировать LUCK после спина")

    # --- НОВОЕ: кешбек за спин ---
    try:
        # читаем свежую валюту из строки
        headers = s_users.row_values(1)
        row_vals = s_users.row_values(row)
        upper = [h.upper() for h in headers]
        if 'WINTER_CURRENCY' in upper:
            idx = upper.index('WINTER_CURRENCY')
            cur_old = int(row_vals[idx] or 0) if idx < len(row_vals) else 0
        else:
            cur_old = 0
    except Exception:
        cur_old = 0

    cashback = CASHBACK_PER_SPIN
    new_cur = cur_old + cashback
    try:
        s_users.update([[new_cur]], f"{column_letter_by_name(s_users, 'WINTER_CURRENCY')}{row}", value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Не удалось записать кешбек за спин")
    # --- /END кешбек ---

    url = (chosen.get("url") or "").strip()
    if "drive.google.com" in url:
        if "/d/" in url:
            file_id = url.split("/d/")[1].split("/")[0]
            url = f"https://drive.google.com/uc?export=download&id={file_id}"
        elif "id=" in url:
            file_id = url.split("id=")[1].split("&")[0]
            url = f"https://drive.google.com/uc?export=download&id={file_id}"

    rarity_label = RARITY_STYLES_WINTER.get(chosen.get("rarity"), chosen.get("rarity"))
    card_id = chosen.get("id")
    # добавляем информацию о кешбеке в подпись
    caption = (
        f"{rarity_label}\n"
        f"{chosen.get('desc')}\n\n"
        f"🆔 ID карточки: {card_id}\n"
        f"❄️ За эту карточку: +{gained} ❄️\n\n"
    )

    try:
        await context.bot.send_photo(chat_id=chat_id, photo=url, caption=caption)
    except Exception as e:
        logger.warning("winter send_photo failed: %s; trying to download and send bytes", e)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as resp:
                    if resp.status != 200:
                        raise Exception(f"HTTP {resp.status}")
                    content = await resp.read()
            bio = BytesIO(content)
            bio.name = f"winter_cat_{chosen.get('id')}.jpg"
            await context.bot.send_photo(chat_id=chat_id, photo=bio, caption=caption)
        except Exception as e2:
            logger.exception("Не удалось скачать/отправить winter картинку: %s", e2)
            await context.bot.send_message(chat_id=chat_id, text="(Не удалось отправить изображение)\n" + caption)


# -------------------------- Shop / Daily claim --------------------------

def load_shop_items():
    s = sheet_winter_shop()
    rows = s.get_all_records()
    items = []
    for r in rows:
        item = {k: (r.get(k) if r.get(k) is not None else "") for k in r.keys()}
        try:
            item["PRICE"] = int(r.get("PRICE") or 0)
        except Exception:
            item["PRICE"] = 0
        try:
            item["SPINS"] = int(r.get("SPINS") or 0)
        except Exception:
            item["SPINS"] = 0
        try:
            item["LUCK"] = int(r.get("LUCK") or 0)
        except Exception:
            item["LUCK"] = 0
        q = r.get("QUANTITY")
        if q is None or str(q).strip() == "":
            item["QUANTITY"] = None
        else:
            try:
                item["QUANTITY"] = int(q)
            except Exception:
                item["QUANTITY"] = None
        items.append(item)
    return items

# --- Заменить существующую функцию winter_shop_menu на эту ---
async def winter_shop_menu(query, context: ContextTypes.DEFAULT_TYPE):
    user_id = query.from_user.id
    s_users = sheet_winter_users()
    row, record = find_winter_user_row(s_users, user_id)
    if record is None:
        create_new_winter_user(s_users, user_id)
        row, record = find_winter_user_row(s_users, user_id)

    items = load_shop_items()
    kb = []
    for it in items:
        label = f"{it.get('NAME','')} — {it.get('PRICE',0)}✨"
        cb = f"winter_shop_show:{it.get('ITEM_ID')}"
        kb.append([InlineKeyboardButton(label, callback_data=cb)])
    kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")])

    text = "🏪 Магазин — выберите позицию для подробностей:"
    markup = InlineKeyboardMarkup(kb)

    msg = query.message
    try:
        # если текущее сообщение — медиа (фото/док/видео) — удаляем его и отправляем новое текстовое меню
        has_media = getattr(msg, "photo", None) or getattr(msg, "document", None) or getattr(msg, "video", None) or getattr(msg, "audio", None)
        if has_media:
            try:
                await msg.delete()
            except Exception:
                logger.exception("winter_shop_menu: не удалось удалить медиа-сообщение")
            # отправляем новое текстовое сообщение с меню
            try:
                await context.bot.send_message(chat_id=msg.chat_id, text=text, reply_markup=markup)
            except Exception:
                logger.exception("winter_shop_menu: не удалось отправить сообщение после удаления медиа")
        else:
            # обычный безопасный редакт (позволит корректно работать если msg — text или caption)
            await safe_edit_message(msg, text, reply_markup=markup)
    except Exception:
        logger.exception("winter_shop_menu: unexpected error")
        # fallback — отправим новое сообщение
        try:
            await context.bot.send_message(chat_id=msg.chat_id, text=text, reply_markup=markup)
        except Exception:
            pass

# --- Заменить существующую функцию winter_shop_show на эту (улучшенный вариант) ---
async def winter_shop_show(query, context: ContextTypes.DEFAULT_TYPE, item_id=None):
    data = query.data or ""
    if item_id is None:
        try:
            item_id = data.split(":", 1)[1]
        except Exception:
            await query.answer()
            return

    items = load_shop_items()
    item = next((it for it in items if str(it.get("ITEM_ID")) == str(item_id)), None)
    if item is None:
        await query.answer("Товар не найден", show_alert=True)
        return

    text_lines = []
    text_lines.append(f"Описание товара")
    desc = item.get("DESCRIPTION") or ""
    if desc:
        text_lines.append(desc)
    text_lines.append(f"Тип: {item.get('TYPE')}, Редкость: {item.get('RARITY') or '-'}")
    text_lines.append(f"Цена: {item.get('PRICE')} ✨")
    if item.get("SPINS"):
        text_lines.append(f"Даёт спинов: {item.get('SPINS')}")
    if item.get("CARD_ID"):
        text_lines.append(f"Карточка: #{item.get('CARD_ID')}")
    if item.get("QUANTITY") is not None:
        text_lines.append(f"Остаток: {item.get('QUANTITY')}")
    image = item.get("IMAGE_URL") or ""
    full_text = "\n".join(text_lines)

    kb = [
        [InlineKeyboardButton("Купить", callback_data=f"winter_shop_buy:{item_id}")],
        [InlineKeyboardButton("⬅️ В магазин", callback_data="winter_shop"), InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")]
    ]

    try:
        if image:
            # отправляем новое сообщение с фотографией (и клавиатурой). 
            # reply_photo создаст отдельный message — исходное меню удалим.
            try:
                sent = await query.message.reply_photo(photo=image, caption=full_text, reply_markup=InlineKeyboardMarkup(kb))
            except Exception:
                # если reply_photo провалился (часто из-за URL) — попробуем как обычный text fallback
                sent = None
                raise

            # удаляем исходное сообщение (чтобы не осталось "меню" под картинкой)
            try:
                await query.message.delete()
            except Exception:
                logger.exception("winter_shop_show: не удалось удалить исходное сообщение после отправки фото")

            # всё успешно — выходим
            return
    except Exception:
        # если отправка фото не удалась — упадём в текстовый fallback ниже
        logger.exception("winter_shop_show: отправка фото не удалась, переход к текстовому отображению")

    # fallback: показываем как текст (без фото)
    try:
        await safe_edit_message(query.message, full_text, reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        # последний вариант — просто отправим новое сообщение с текстом
        try:
            await context.bot.send_message(chat_id=query.message.chat_id, text=full_text, reply_markup=InlineKeyboardMarkup(kb))
            try:
                await query.message.delete()
            except Exception:
                pass
        except Exception:
            logger.exception("winter_shop_show: не удалось показать товар ни одним способом")


async def winter_shop_show(query, context: ContextTypes.DEFAULT_TYPE, item_id=None):
    data = query.data or ""
    if item_id is None:
        try:
            item_id = data.split(":", 1)[1]
        except Exception:
            await query.answer()
            return

    items = load_shop_items()
    item = next((it for it in items if str(it.get("ITEM_ID")) == str(item_id)), None)
    if item is None:
        await query.answer("Товар не найден", show_alert=True)
        return

    text_lines = []
    text_lines.append(f"Описание товара")
    desc = item.get("DESCRIPTION") or ""
    if desc:
        text_lines.append(desc)
    text_lines.append(f"Тип: {item.get('TYPE')}, Редкость: {item.get('RARITY') or '-'}")
    text_lines.append(f"Цена: {item.get('PRICE')} ✨")
    if item.get("SPINS"):
        text_lines.append(f"Даёт спинов: {item.get('SPINS')}")
    if item.get("CARD_ID"):
        text_lines.append(f"Карточка: #{item.get('CARD_ID')}")
    if item.get("QUANTITY") is not None:
        text_lines.append(f"Остаток: {item.get('QUANTITY')}")
    image = item.get("IMAGE_URL") or ""
    full_text = "\n".join(text_lines)

    kb = [
        [InlineKeyboardButton("Купить", callback_data=f"winter_shop_buy:{item_id}")],
        [InlineKeyboardButton("⬅️ В магазин", callback_data="winter_shop"), InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")]
    ]

    try:
        if image:
            await query.message.reply_photo(photo=image, caption=full_text, reply_markup=InlineKeyboardMarkup(kb))
            try:
                await query.message.delete()
            except Exception:
                pass
            return
    except Exception:
        pass

    await query.message.edit_text(full_text, reply_markup=InlineKeyboardMarkup(kb))

async def winter_shop_buy(query, context: ContextTypes.DEFAULT_TYPE):
    data = query.data or ""
    try:
        item_id = data.split(":", 1)[1]
    except Exception:
        await query.answer()
        return
    items = load_shop_items()
    item = next((it for it in items if str(it.get("ITEM_ID")) == str(item_id)), None)
    if item is None:
        await query.answer("Товар не найден", show_alert=True)
        return

    price = item.get("PRICE", 0)
    text = f"Подтвердите покупку: {item.get('NAME')} — {price} ✨"
    kb = [
        [InlineKeyboardButton("✅ Купить", callback_data=f"winter_shop_confirm:{item_id}")],
        [InlineKeyboardButton("❌ Отмена", callback_data=f"winter_shop_show:{item_id}")]
    ]
    await safe_edit_msg(query, text, InlineKeyboardMarkup(kb))


async def winter_shop_confirm(query, context: ContextTypes.DEFAULT_TYPE):
    """
    Выполняет покупку:
    - читаем свежие значения из строки пользователя,
    - проверяем баланс,
    - выполняем обновления (валюта, спины, удача, карточка),
    - уменьшаем QUANTITY товара (если задано).
    """
    data = query.data or ""
    try:
        item_id = data.split(":", 1)[1]
    except Exception:
        await query.answer()
        return

    user_id = query.from_user.id
    s_users = sheet_winter_users()
    row, _ = find_winter_user_row(s_users, user_id)
    if row is None:
        create_new_winter_user(s_users, user_id)
        row, _ = find_winter_user_row(s_users, user_id)

    # load fresh user row values and headers
    headers = s_users.row_values(1)
    row_vals = s_users.row_values(row)
    upper_headers = [h.upper() for h in headers]

    def _get_user_field(field_name):
        if field_name.upper() in upper_headers:
            idx = upper_headers.index(field_name.upper())
            return row_vals[idx] if idx < len(row_vals) else ""
        return ""

    try:
        cur = int(_get_user_field("WINTER_CURRENCY") or 0)
    except Exception:
        cur = 0
    try:
        old_spins = int(_get_user_field("WINTER_SPINS") or 0)
    except Exception:
        old_spins = 0
    try:
        old_luck = int(_get_user_field("LUCK_HIDDEN") or 0)
    except Exception:
        old_luck = 0
    existing_cards = _get_user_field("W_CATS_ID") or ""

    # items и item уже у тебя ниже — оставляем
    items = load_shop_items()
    item = next((it for it in items if str(it.get("ITEM_ID")) == str(item_id)), None)
    if item is None:
        await query.answer("Товар не найден", show_alert=True)
        return

    price = int(item.get("PRICE", 0) or 0)

        # --- PRE-CHECK: если товар типа "frame", не даём купить, если уже на max уровне ---
    try:
        if str(item.get("TYPE", "")).strip().lower() == "frame":
            cur_frame = FRAME_DEFAULT
            if "FRAME_SET" in upper_headers:
                idx_fs = upper_headers.index("FRAME_SET")
                if idx_fs < len(row_vals):
                    fv = row_vals[idx_fs]
                    if str(fv).strip():
                        try:
                            cur_frame = int(fv)
                        except Exception:
                            cur_frame = FRAME_DEFAULT
            if cur_frame >= FRAME_MAX:
                await query.answer("У тебя уже максимальный фон — улучшать нечего.", show_alert=True)
                # возвращаем пользователя в подробности товара (или в магазин)
                await winter_shop_show(query, context, item_id=item_id)
                return
    except Exception:
        logger.exception("Frame pre-check failed (non-fatal)")

    # check if this item grants a card and if user already has it
    card_id = item.get("CARD_ID")
    if card_id:
        # нормализуем существующие карточки пользователя в set
        owned_tokens = [t.strip() for t in re.split(r"[|,;\\s]+", str(existing_cards)) if t.strip()]
        if str(card_id) in owned_tokens:
            # у пользователя уже есть эта карточка — не даём купить снова
            await query.answer("У тебя уже есть эта карточка — повторная покупка невозможна.", show_alert=True)
            await winter_shop_show(query, context, item_id=item_id)
            return

    # check quantity
    q = item.get("QUANTITY")
    if q is not None and q <= 0:
        await query.answer("К сожалению, товар закончился.", show_alert=True)
        await winter_shop_show(query, context, item_id=item_id)
        return

    if cur < price:
        await query.answer("Недостаточно средств.", show_alert=True)
        await winter_shop_show(query, context, item_id=item_id)
        return

    # compute new values
    new_cur = cur - price
    new_spins = min(old_spins + int(item.get("SPINS") or 0), MAX_WINTER_SPINS)
    new_luck = min(MAX_LUCK, old_luck + int(item.get("LUCK") or 0))

    # append card id if present and not already owned
    if card_id:
        owned_tokens = [t.strip() for t in re.split(r"[|,;\\s]+", str(existing_cards)) if t.strip()]
        if str(card_id) not in owned_tokens:
            owned_tokens.append(str(card_id))
        # sort numeric-like ids nicely
        try:
            sorted_ids = sorted(owned_tokens, key=lambda x: (int(x) if x.isdigit() else float('inf'), x))
        except Exception:
            sorted_ids = sorted(owned_tokens)
        appended = " | ".join(sorted_ids)
    else:
        appended = existing_cards


    # Now write updates (try to write atomically-ish: update each cell)
    try:
        # currency
        s_users.update([[new_cur]], f"{column_letter_by_name(s_users, 'WINTER_CURRENCY')}{row}")
        # spins
        s_users.update([[new_spins]], f"{column_letter_by_name(s_users, 'WINTER_SPINS')}{row}")
        # luck
        s_users.update([[new_luck]], f"{column_letter_by_name(s_users, 'LUCK_HIDDEN')}{row}")
        # cards
        s_users.update([[appended]], f"{column_letter_by_name(s_users, 'W_CATS_ID')}{row}")
    except Exception as e:
        logger.exception("Ошибка при обновлении пользователя в магазине: %s", e)
        await query.answer("Ошибка базы данных. Попробуй позже.", show_alert=True)
        await winter_shop_show(query, context, item_id=item_id)
        return
    
    # Обработка улучшения фона
    try:
        if str(item.get("TYPE")).lower() == "frame":
            # Получаем текущий фон пользователя (или 10 по умолчанию)
            cur_frame = 10
            if "FRAME_SET" in upper_headers:
                idx_fs = upper_headers.index("FRAME_SET")
                if idx_fs < len(row_vals):
                    try:
                        cur_frame = int(row_vals[idx_fs] or 10)
                    except Exception:
                        cur_frame = 10
            # Переходим к следующему фону, но не выше 12
            new_frame = cur_frame + 1 if cur_frame < 12 else cur_frame

            # Записываем новый FRAME_SET в таблицу
            col_fs = column_letter_by_name(s_users, "FRAME_SET")
            s_users.update([[new_frame]], f"{col_fs}{row}", value_input_option="USER_ENTERED")

            # Сбрасываем закэшированный file_id рамки (FRAME_FILE_ID) для пересоздания изображения
            col_fid = column_letter_by_name(s_users, "FRAME_FILE_ID")
            s_users.update([[""]], f"{col_fid}{row}", value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception("Не удалось обновить FRAME_SET после покупки: %s", e)

    # decrement shop quantity if set
    if item.get("QUANTITY") is not None:
        try:
            s_shop = sheet_winter_shop()
            all_rows = s_shop.get_all_records()
            for idx, r in enumerate(all_rows, start=2):
                if str(r.get("ITEM_ID")) == str(item.get("ITEM_ID")):
                    cur_q_raw = r.get("QUANTITY")
                    if cur_q_raw is None or str(cur_q_raw).strip() == "":
                        # nothing to do (infinite)
                        pass
                    else:
                        try:
                            cur_q = int(cur_q_raw)
                            new_q = max(0, cur_q - 1)
                            s_shop.update([[new_q]], f"{column_letter_by_name(s_shop, 'QUANTITY')}{idx}")
                        except Exception:
                            logger.exception("Не удалось уменьшить QUANTITY товара")
                    break
        except Exception:
            logger.exception("Ошибка при уменьшении количества товара")

    # success — формируем текст для подтверждения
    text = f"Покупка успешна: {item.get('NAME')} — списано {price}✨"

    # 1) сначала отсылаем карточку отдельным сообщением (если у товара есть CARD_ID)
    try:
        card_id = item.get("CARD_ID")
        chat_id = query.message.chat_id if hasattr(query.message, "chat_id") else query.message.chat.id
        if card_id:
            # отправим карточку пользователю
            await send_card_message(chat_id, card_id, context)
    except Exception:
        logger.exception("Не удалось отправить карточку после покупки")

    # 2) затем удалим исходное сообщение (чтобы картинка/превью не "таскались" вместе с меню)
    try:
        await query.message.delete()
    except Exception:
        # если не удалось удалить — всё равно отправим новое текстовое сообщение (чтобы не остался прикреплённый файл)
        logger.exception("Не удалось удалить исходное сообщение магазина после покупки")
    # 3) отправим новый подтверждающий текст с кнопками (вместо попытки редактирования сообщения с фото)
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ В магазин", callback_data="winter_shop"),
                InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")]
            ])
        )
    except Exception:
        logger.exception("Не удалось отправить финальное сообщение после покупки")
        # fallback: используем safe_edit_message если хотите (но мы уже удалили исходное)
        try:
            await query.answer("Покупка завершена.", show_alert=True)
        except Exception:
            pass

# -------------------------- Advent calendar UI (4x5 grid) --------------------------

async def winter_advent_menu(query, context: ContextTypes.DEFAULT_TYPE):
    """
    Рисуем адвент в виде сетки 4 строки x 5 столбцов (20 дней).
    На кнопке показывается только число и статус:
      - 🕒 — ещё не наступил (w)
      - 🎁 — доступен (0)
      - ✅ — уже получен (1)
    День 1 соответствует дате 22 декабря (start), день 20 — 10 января (end).
    """
    user_id = query.from_user.id
    s_users = sheet_winter_users()
    row, record = find_winter_user_row(s_users, user_id)
    if record is None:
        create_new_winter_user(s_users, user_id)
        row, record = find_winter_user_row(s_users, user_id)

    ensure_advent_table(days_count=20)
    ensure_user_advent_state(s_users, row)
    days = get_advent_days_count()
    state = read_user_advent_state(s_users, row, days)

    # helper: date for index
    def _advent_date_for_index(idx):
        start_date, _ = _default_advent_start_end()
        return start_date + timedelta(days=idx)

    kb = []
    # make 4 rows, each with 5 columns
    row_buttons = []
    for i in range(days):
        dt = _advent_date_for_index(i)
        label = f"{dt.day}"  # only number
        ch = state[i]
        if ch == '1':
            text = f"{label} ✅"
            cb = "winter_advent_none"
        elif ch == 'w':
            text = f"{label} 🕒"
            cb = "winter_advent_none"
        else:
            text = f"{label} 🎁"
            cb = f"winter_advent_claim:{i+1}"
        btn = InlineKeyboardButton(text, callback_data=cb)
        row_buttons.append(btn)
        if len(row_buttons) >= 5:
            kb.append(row_buttons)
            row_buttons = []
    if row_buttons:
        # pad remaining to 5 to keep grid consistent (optional)
        while len(row_buttons) < 5:
            row_buttons.append(InlineKeyboardButton(" ", callback_data="winter_advent_none"))
        kb.append(row_buttons)

    kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")])

    await query.message.edit_text("🗓 Адвент — открой сегодня своё окно (22 декабря → 10 января):",
                                 reply_markup=InlineKeyboardMarkup(kb))

async def winter_advent_claim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split(":")
    if len(parts) != 2:
        await query.message.edit_text("Неправильный формат запроса")
        return
    try:
        day = int(parts[1])
    except Exception:
        await query.answer("Неправильный номер дня", show_alert=True)
        return
    user_id = query.from_user.id
    s_users = sheet_winter_users()
    row, record = find_winter_user_row(s_users, user_id)
    if record is None:
        await query.answer("Сначала /start, пожалуйста.", show_alert=True)
        return
    success, msg = claim_advent_day(s_users, row, day)
    if success:
        await query.message.edit_text("🎉 " + msg, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="winter_advent")]
        ]))
    else:
        await query.answer(msg, show_alert=True)

# -------------------------- Top, nick etc (unchanged) --------------------------

async def winter_show_top(query, context: ContextTypes.DEFAULT_TYPE):
    try:
        s_top = sheet_winter_leader()
        records = s_top.get_all_records() if s_top else []
    except Exception:
        records = []

    if not records:
        try:
            s_users = sheet_winter_users()
            all_records = s_users.get_all_records()
            sorted_rec = sorted(all_records, key=lambda r: int(r.get("SUM") or 0), reverse=True)
            records = sorted_rec
        except Exception:
            records = []

    if not records:
        await query.message.edit_text("Пока нет данных для топа.", reply_markup=get_winter_menu_markup())
        return

    text = "🏔 Топ игроков (зимний):\n\n"
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    for i, r in enumerate(records[:10], start=1):
        score = int(r.get("SUM") or 0)
        nick = (r.get("NICK") or "").strip()
        uid = str(r.get("USER_ID") or "")
        display = nick if nick else (f"#{uid[-6:]}" if uid else f"Игрок {i}")
        medal = medals[i-1] if i-1 < len(medals) else f"{i}."
        text += f"{medal} {display} — {score} ❄️\n"

    keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")]]
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

# -------------------------- Callback dispatcher --------------------------

async def winter_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "winter_main":
        s_users = sheet_winter_users()
        row, rec = find_winter_user_row(s_users, query.from_user.id)
        if rec is None:
            create_new_winter_user(s_users, query.from_user.id)
            row, rec = find_winter_user_row(s_users, query.from_user.id)
        await safe_edit_message(query.message, get_winter_menu_text(rec), reply_markup=get_winter_menu_markup())

        return

    if data == "winter_back_main":
        try:
            import main as main_mod
            s_users = main_mod.sheet_users()
            row, rec = main_mod.find_user_row_fast(s_users, query.from_user.id)
            if rec is None:
                if hasattr(main_mod, "create_new_user"):
                    main_mod.create_new_user(s_users, query.from_user.id)
                    row, rec = main_mod.find_user_row_fast(s_users, query.from_user.id)
                else:
                    await query.message.edit_text("Возвращаемся в главное меню...", reply_markup=None)
                    return
            try:
                import inspect
                sig = inspect.signature(main_mod.get_main_menu_markup)
                if "is_admin" in sig.parameters:
                    is_admin = query.from_user.id == getattr(main_mod, "ADMIN_ID", None)
                    markup = main_mod.get_main_menu_markup(is_admin=is_admin)
                else:
                    markup = main_mod.get_main_menu_markup()
            except Exception:
                try:
                    is_admin = query.from_user.id == getattr(main_mod, "ADMIN_ID", None)
                    markup = main_mod.get_main_menu_markup(is_admin=is_admin)
                except Exception:
                    markup = main_mod.get_main_menu_markup()
            main_text = main_mod.get_main_menu_text(rec)
            try:
                await query.message.edit_text(main_text, reply_markup=markup)
            except Exception:
                await context.bot.send_message(chat_id=query.message.chat_id, text=main_text, reply_markup=markup)
            return
        except Exception as e:
            logger.exception("Не удалось делегировать возврат в main.py: %s", e)
            try:
                await query.message.edit_text("Возвращаемся в главное меню...", reply_markup=None)
            except Exception:
                pass
            return

    if data == "winter_spin":
        chat_id = query.message.chat_id
        # Удаляем старое меню — чтобы чат не засорялся
        try:
            await query.message.delete()
        except Exception:
            logger.exception("Не удалось удалить сообщение меню перед зимним спином")
        # Выполняем сам спин (отправит картинку/результат)
        await handle_winter_spin_and_send(chat_id, query.from_user.id, context)
        # Отправляем обновлённое меню (новое сообщение)
        try:
            s_users = sheet_winter_users()
            _, rec = find_winter_user_row(s_users, query.from_user.id)
            await context.bot.send_message(chat_id=chat_id, text=get_winter_menu_text(rec), reply_markup=get_winter_menu_markup())
        except Exception:
            logger.exception("Не удалось отправить новое меню после зимнего спина")
        return


    if data == "winter_shop":
        await winter_shop_menu(query, context)
        return

    if data.startswith("winter_shop_show:"):
        await winter_shop_show(query, context)
        return

    if data.startswith("winter_shop_buy:"):
        await winter_shop_buy(query, context)
        return

    if data.startswith("winter_shop_confirm:"):
        await winter_shop_confirm(query, context)
        return

    if data == "winter_games":
        kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")]]
        await query.message.edit_text("🎮 Игры — в разработке. Здесь будут мини-игры за игровую валюту.", reply_markup=InlineKeyboardMarkup(kb))
        return

    if data == "winter_advent":
        await winter_advent_menu(query, context)
        return

    if data.startswith("winter_advent_claim:"):
        await winter_advent_claim_callback(update, context)
        return

    if data == "winter_advent_none":
        await query.answer()
        return

    if data == "winter_top":
        await winter_show_top(query, context)
        return

    if data == "winter_change_nick":
        kb = [
            [InlineKeyboardButton("✨ Использовать @username", callback_data="winter_nick_use_username")],
            #[InlineKeyboardButton("✏️ Ввести вручную", callback_data="winter_change_nick_manual")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")],
        ]
        try:
            await query.message.edit_text("Выбери способ установки ника в новогоднем режиме:", reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            await context.bot.send_message(chat_id=query.message.chat_id, text="Выбери способ установки ника в новогоднем режиме:", reply_markup=InlineKeyboardMarkup(kb))
        return

    if data == "winter_nick_use_username":
        usr = query.from_user
        tg_username = usr.username
        if not tg_username:
            kb = [
                [InlineKeyboardButton("✏️ Ввести вручную", callback_data="winter_change_nick_manual")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="winter_main")],
            ]
            await query.message.edit_text("😿 У тебя нет @username. Введи ник вручную:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            try:
                s_users = sheet_winter_users()
                row, record = find_winter_user_row(s_users, usr.id)
                if record is None:
                    create_new_winter_user(s_users, usr.id)
                    row, record = find_winter_user_row(s_users, usr.id)
                col = column_letter_by_name(s_users, "NICK")
                s_users.update([[f"@{tg_username}"]], f"{col}{row}", value_input_option="USER_ENTERED")
                _, new_record = find_winter_user_row(s_users, usr.id)
                await query.message.edit_text(get_winter_menu_text(new_record), reply_markup=get_winter_menu_markup())
            except Exception as e:
                logger.exception("Не удалось записать зимний ник через @username: %s", e)
                await query.answer("Не удалось установить ник через @username. Попробуй вручную.", show_alert=True)
                context.user_data["awaiting_winter_nick"] = True
                context.user_data["winter_nick_prompt_mid"] = query.message.message_id
                try:
                    await query.message.edit_text("✏️ Введи новый ник для новогоднего режима (без символа @):")
                except Exception:
                    await context.bot.send_message(chat_id=query.message.chat_id, text="✏️ Введи новый ник для новогоднего режима (без символа @):")
        return

    if data == "winter_change_nick_manual":
        context.user_data["awaiting_winter_nick"] = True
        context.user_data["winter_nick_prompt_mid"] = query.message.message_id
        try:
            await query.message.edit_text("✏️ Введи новый ник для новогоднего режима (без символа @):")
        except Exception:
            await context.bot.send_message(chat_id=query.message.chat_id, text="✏️ Введи новый ник для новогоднего режима (без символа @):")
        return

    await query.answer()
    return

# -------------------------- Public registration --------------------------

def register_winter_handlers(app):
    app.add_handler(CallbackQueryHandler(winter_button_callback, pattern="^winter_"))
