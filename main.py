# main_v3.py — стрик + оптимизация
import os
import logging
import random
from datetime import datetime, timedelta
import pytz
import time
import time as _time
import asyncio
import os
import json
import threading
import http.server
import socketserver
import re
import aiohttp

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import gspread
from google.oauth2.service_account import Credentials

# --- Настройки ---
BOT_TOKEN = os.environ["BOT_TOKEN"]
SPREADSHEET_KEY = os.environ["SPREADSHEET_KEY"]
CREDENTIALS_FILE = "/etc/secrets/cats-476112-9a44bf3e38e2.json"
BONUS_CHANNEL = "@gg_ssr"

MAX_SPINS = 999
POINTS_BY_RARITY = {
    "COM": 1,
    "UCOM": 3,
    "RARE": 7,
    "EPIC": 20,
    "LEG": 50
}

RARITY_WEIGHTS = {
    "COM": 60,
    "UCOM": 25,
    "RARE": 10,
    "EPIC": 4,
    "LEG": 1
}

RARITY_STYLES = {
        "COM": "⚪️ Обычный",
        "UCOM": "🟢 Необычный",
        "RARE": "🔵 Редкий",
        "EPIC": "🟣 Эпический",
        "LEG": "🟠 Легендарный"
    }

# --- Кэш для leaderboard ---
LEADERBOARD_CACHE = {
    "ts": 0,         # unix time последнего обновления
    "records": None  # список записей (rows) полученный из sheet_leaderboard().get_all_records()
}
LEADERBOARD_TTL = 10  # время жизни кэша в секундах (настраиваемо)
leaderboard_cache_lock = asyncio.Lock()

# --- Кэш для списка котов ---
CATS_CACHE = {
    "ts": 0,       # время последнего обновления
    "data": None   # список котов после clean_cat_records()
}
CATS_TTL = 300     # 5 минут

# Логирование
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Часовой пояс Новосибирска
NOVOSIBIRSK_TZ = pytz.timezone("Asia/Novosibirsk")

# GSheets scopes + helper
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# --- Helpers for GSheets ---
def gs_client():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def sheet_users():
    client = gs_client()
    return client.open_by_key(SPREADSHEET_KEY).worksheet("users")


def sheet_cats():
    client = gs_client()
    return client.open_by_key(SPREADSHEET_KEY).worksheet("cats")

def sheet_promo():
    client = gs_client()
    return client.open_by_key(SPREADSHEET_KEY).worksheet("promo")

def sheet_leaderboard():
    client = gs_client()
    wb = client.open_by_key(SPREADSHEET_KEY)
    try:
        return wb.worksheet("leaderboard")
    except Exception:
        return None

def make_streak_bar(streak: int) -> str:
    bar = []
    streak = streak % 5
    if streak == 0:
        streak = 5
    for i in range(1, 6):
        if streak >= i:
            if i <= 4:
                bar.append("🟩")
            else:
                bar.append("🎁")
        else:
            bar.append("⬜")
    return "".join(bar)

def make_super_grid(counts_by_reward: dict):
    """
    counts_by_reward: {3: count_of_3s, 2: count_of_2s, 1: count_of_1s}
    Возвращает список длины 9 с распределением и перемешанный.
    """
    grid = []
    for reward, cnt in counts_by_reward.items():
        grid += [int(reward)] * int(cnt)
    # дополним +1, если вдруг менее 9
    while len(grid) < 9:
        grid.append(1)
    # если больше — усечём
    if len(grid) > 9:
        grid = grid[:9]
    random.shuffle(grid)
    return grid

def load_promo_codes():
    """
    Загружает промокоды из листа promo.
    Формат таблицы:
    CODE | BONUS | COLUMN | DESC
    """
    s = sheet_promo()
    records = s.get_all_records()

    promo_dict = {}
    for r in records:
        code = str(r.get("CODE") or "").strip().upper()
        bonus = int(r.get("BONUS") or 0)
        column = str(r.get("COLUMN") or "").strip().upper()
        desc = str(r.get("DESC") or "").strip()

        if code and column:
            promo_dict[code] = {
                "bonus": bonus,
                "column": column,
                "desc": desc
            }

    return promo_dict





# --- Utility functions ---
def get_today_date_iso():
    return datetime.now(NOVOSIBIRSK_TZ).date().isoformat()


def find_user_row_fast(sheet, user_id):
    try:
        # Ищем только в колонке USER_ID (A → column=1)
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

def create_new_user(sheet, user_id):
    """
    Добавляет нового пользователя в users с правильным порядком колонок:
    USER_ID, NICK, CATS_ID, SPINS, LAST_DAILY, SUM, SUB_GG_USED, PROMO_WM, PROMO_HE, PROMO_GAD, PROMO_COAL
    """
    # начальные значения: пустой ник, пустой список котов, 3 спина по умолчанию, пустой LAST_DAILY, SUM=0,
    # SUB_GG_USED=0, промо столбцы=0
    row_values = [
        user_id,   # A USER_ID
        "",        # B NICK
        "",        # C CATS_ID
        3,         # D SPINS
        "",        # E LAST_DAILY
        0,         # F STREAK  ← добавили
        0,         # G SUM
    ]
    sheet.append_row(row_values, value_input_option="USER_ENTERED")
    return 3


def colnum_to_letter(n):
    """1 -> A, 27 -> AA"""
    string = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        string = chr(65 + rem) + string
    return string

def get_header_name_by_letter(sheet, letter):
    """Возвращает текст заголовка (имя колонки) по букве столбца."""
    col_index = 0
    for char in letter.upper():
        col_index = col_index * 26 + (ord(char) - ord('A') + 1)

    headers = sheet.row_values(1)
    if 1 <= col_index <= len(headers):
        return headers[col_index - 1]
    return None

def column_letter_by_name(sheet, name):
    """Находит букву колонки по имени (header)"""
    headers = sheet.row_values(1)
    for idx, h in enumerate(headers, start=1):
        if str(h).strip().upper() == name.upper():
            return colnum_to_letter(idx)
    # если нет — добавляем в конец
    next_idx = len(headers) + 1
    sheet.update([[name]], f"{colnum_to_letter(next_idx)}1")
    return colnum_to_letter(next_idx)


def ensure_sum_column(sheet):
    """
    Убедиться, что у листа есть колонка 'SUM'. Если нет — добавляем её в конец заголовков.
    Возвращает индекс колонки SUM (1-based).
    """
    headers = sheet.row_values(1)
    if not headers:
        headers = []
    for idx, h in enumerate(headers, start=1):
        if str(h).upper() == "SUM":
            return idx
    # добавить в конец
    next_idx = len(headers) + 1
    sheet.update([["SUM"]], f"{colnum_to_letter(next_idx)}1")
    return next_idx

async def get_leaderboard_cached():
    """
    Возвращает список записей leaderboard (get_all_records).
    Если кэш свежий — возвращает кэш. Иначе — обновляет leaderboard (формула)
    и считывает данные, записывает в кэш и возвращает.
    """
    now = time.time()
    # быстрый путь без блокировки
    if LEADERBOARD_CACHE["records"] is not None and (now - LEADERBOARD_CACHE["ts"]) < LEADERBOARD_TTL:
        return LEADERBOARD_CACHE["records"]

    # блокируем обновление кэша, чтобы только один запрос делал heavy work
    async with leaderboard_cache_lock:
        # другой таск мог уже обновить кэш — проверить ещё раз
        now = time.time()
        if LEADERBOARD_CACHE["records"] is not None and (now - LEADERBOARD_CACHE["ts"]) < LEADERBOARD_TTL:
            return LEADERBOARD_CACHE["records"]

        # читаем данные из leaderboard
        try:
            s_lb = sheet_leaderboard()
            if not s_lb:
                # пустой / нет leaderboard
                LEADERBOARD_CACHE["records"] = []
                LEADERBOARD_CACHE["ts"] = time.time()
                return []
            records = s_lb.get_all_records()
        except Exception as e:
            logger.warning("Ошибка при чтении leaderboard: %s", e)
            records = []

        # сохраняем в кэш
        LEADERBOARD_CACHE["records"] = records
        LEADERBOARD_CACHE["ts"] = time.time()
        return records


# --- Menu & cards ---
def get_main_menu_text(record=None):
    spins = 0
    nick_display = None
    if record:
        try:
            spins = int(record.get("SPINS") or 0)
        except Exception:
            spins = 0
        # пытаемся получить ник из поля NICK (если его нет в таблице — будет "")
        nick = str(record.get("NICK") or "").strip()
        if nick:
            nick_display = nick
        else:
            uid = str(record.get("USER_ID") or "")
            nick_display = f"#{uid[-6:]}" if uid else "Игрок"
    else:
        nick_display = "Игрок"
    return f"🏠 Главное меню\n Имя пользователя: {nick_display}\n\n💰 Баланс: {spins} спинов\nВыберите действие:"


def get_main_menu_markup():
    keyboard = [
        [InlineKeyboardButton("🎰 Спин", callback_data="spin")],
        [InlineKeyboardButton("🎁 Награды", callback_data="rewards")],
        [InlineKeyboardButton("✏️ Сменить ник", callback_data="change_nick")],
        [InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_rewards_markup():
    keyboard = [
        [InlineKeyboardButton("🗓 Ежедневная", callback_data="reward_daily")],
        [InlineKeyboardButton("📢 За подписку", callback_data="reward_sub")],
        [InlineKeyboardButton("✏️ Ввести промокод", callback_data="promo_enter")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_main")],
    ]
    return InlineKeyboardMarkup(keyboard)


def clean_cat_records(records):
    cleaned = []
    for r in records:
        cid = r.get("ID") or r.get("Id") or r.get("id")
        url = (r.get("URL") or r.get("Url") or r.get("url") or "").strip()
        desc = (r.get("DESC") or r.get("Desc") or r.get("description") or "").strip()
        rarity = (r.get("RARITY") or r.get("Rarity") or r.get("rarity") or "COM").upper().strip()
        cleaned.append({"id": cid, "url": url, "desc": desc, "rarity": rarity})
    return cleaned

def get_cats_cached():
    """
    Возвращает список котов (список словарей) с кэшированием на 5 минут.
    """
    now = time.time()

    # если кэш свежий — возвращаем его
    if CATS_CACHE["data"] is not None and (now - CATS_CACHE["ts"]) < CATS_TTL:
        return CATS_CACHE["data"]

    # иначе — загружаем из таблицы
    s_cats = sheet_cats()
    try:
        records = s_cats.get_all_records()
        cats = clean_cat_records(records)
    except Exception as e:
        logger.exception("Ошибка при получении списка котов: %s", e)
        # если кэш пуст — вернём пустой список
        return CATS_CACHE["data"] or []

    # сохраняем в кэш
    CATS_CACHE["data"] = cats
    CATS_CACHE["ts"] = now
    return cats


def choose_rarity(weights):
    rarities = list(weights.keys())
    w = list(weights.values())
    return random.choices(rarities, weights=w, k=1)[0]


def points_for_rarity(rarity: str) -> int:
    return int(POINTS_BY_RARITY.get(rarity.upper(), 0))


# --- Bot handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    s_users = sheet_users()
    row, record = find_user_row_fast(s_users, user_id)
    if record is None:
        spins = create_new_user(s_users, user_id)
        record = {"SPINS": spins}
    main_text = get_main_menu_text(record)
    await update.message.reply_text(main_text, reply_markup=get_main_menu_markup())


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # защита: гарантируем что user_data существует
    if context.user_data is None:
        context.user_data = {}

    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id
    user_id = query.from_user.id

    # SPIN: удалить текущее меню, выдать фото, отправить новое главное меню
    if data == "spin":
        try:
            await query.message.delete()
        except Exception:
            pass
        await handle_spin_and_send(chat_id, user_id, context)
        s_users = sheet_users()
        _, record = find_user_row_fast(s_users, user_id)
        await context.bot.send_message(chat_id=chat_id, text=get_main_menu_text(record), reply_markup=get_main_menu_markup())
        return

    # show rewards menu
    if data == "rewards":
        await query.message.edit_text("🎁 Меню наград \n\nВыбери:", reply_markup=get_rewards_markup())
        return

    # back main
    if data == "back_main":
        s_users = sheet_users()
        _, record = find_user_row_fast(s_users, user_id)
        await query.message.edit_text(get_main_menu_text(record), reply_markup=get_main_menu_markup())
        return

    # leaderboard
    if data == "leaderboard":
        # обновление leaderboard только при конкретном запросе
        await show_leaderboard(update, context)
        return
    
    # CHANGE NICK: use @username
    if data == "nick_use_username":
        usr = query.from_user
        tg_username = usr.username  # может быть None

        if not tg_username:
            await query.message.edit_text("😿 У тебя нет @username.\nВведи ник вручную.".replace("@", "@\u200b"), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Ввести вручную", callback_data="nick_manual")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_main")],
            ]))
            return

        s_users = sheet_users()
        row, record = find_user_row_fast(s_users, usr.id)

        # ищем колонку NICK
        headers = s_users.row_values(1)
        nick_idx = None
        for idx, h in enumerate(headers, start=1):
            if str(h).strip().upper() == "NICK":
                nick_idx = idx
                break

        if not nick_idx:
            nick_idx = len(headers) + 1
            s_users.update([["NICK"]], f"{colnum_to_letter(nick_idx)}1")

        col_letter = colnum_to_letter(nick_idx)

        s_users.update([[f"@{tg_username}"]], f"{col_letter}{row}")

        _, new_record = find_user_row_fast(s_users, usr.id)
        await query.message.edit_text(
            get_main_menu_text(new_record) + "\n\n✨ Ник установлен через @username!".replace("@", "@\u200b"),
            reply_markup=get_main_menu_markup()
        )
        return

    if data == "nick_manual":
        context.user_data["awaiting_nick"] = True
        context.user_data["nick_prompt_mid"] = query.message.message_id
        await query.message.edit_text("✏️ Введи новый ник (без символа @):")
        return

    # daily reward
    if data == "reward_daily":
        s_users = sheet_users()
        row, record = find_user_row_fast(s_users, user_id)
        if record is None:
            await query.message.edit_text("😿 Ты ещё не зарегистрирован. Сначала пропиши /start.")
            return

        today = get_today_date_iso()
        last_daily = record.get("LAST_DAILY") or ""
        streak = int(record.get("STREAK") or 0)

        # --- Проверка стрика ---
        if last_daily == today:
            reward = 0
            text = f"🐾 Ты уже получил ежедневную награду сегодня!\n\nТвой стрик: {streak}"
        else:
            # Определяем — был ли вчера
            yesterday = (datetime.now(NOVOSIBIRSK_TZ).date().fromisoformat(today))
            yesterday = (datetime.now(NOVOSIBIRSK_TZ).date() - timedelta(days=1)).isoformat()

            if last_daily == yesterday:
                streak += 1
            else:
                streak = 1

            spin_end = 'спина'
            # Награда по схеме 1 1 1 2 2 2 3
            if streak <= 3:
                reward = 1
                spin_end = 'спин'
            elif streak <= 6:
                reward = 2
            else:
                reward = 3

            # Обновляем SPINS
            spins = int(record.get("SPINS") or 0)
            new_spins = min(spins + reward, MAX_SPINS)

            spin_col = column_letter_by_name(s_users, "SPINS")
            steak_col = column_letter_by_name(s_users, "STREAK")
            day_col = column_letter_by_name(s_users, "LAST_DAILY")

            s_users.update([[new_spins]], f"{spin_col}{row}", value_input_option="USER_ENTERED")
            s_users.update([[streak]], f"{steak_col}{row}", value_input_option="USER_ENTERED")
            s_users.update([[today]], f"{day_col}{row}", value_input_option="USER_ENTERED")

            if streak % 5 == 0:
                # отправляем супер-игру
                await offer_super_game(chat_id, user_id, context, s_users, row, streak, message_obj=query.message)
                return
            dop_words = ""
            if streak == 3:
                dop_words = "\n\nЗавтра будет +2, продолжай в том же духе!"
            elif streak == 6:
                dop_words = "\n\nЗавтра будет +3, не останавливайся!"
            elif (streak%5) == 4:
                dop_words = "\n\n🔔 Завтра будет СУПЕР-ИГРА! Не забудь зайти!"

            streak_bar = make_streak_bar(streak)
            text = (
                f"✨ Ты получил ежедневную награду!\n"
                f"Награда: +{reward} {spin_end}\n"
                f"Твой Стрик: {streak}\n{streak_bar}{dop_words}"
            )

        _, new_record = find_user_row_fast(s_users, user_id)
        await query.message.edit_text(get_main_menu_text(new_record) + "\n\n" + text, reply_markup=get_main_menu_markup())
        return

    # супер-игра — выбор клетки
    if data.startswith("super_pick:"):
        idx = int(data.split(":", 1)[1])
        sg = context.user_data.get("super_game")
        if not sg or sg.get("user_id") != user_id:
            await query.answer("Нет активной супер-игры или она принадлежит другому игроку.", show_alert=True)
            return

        if sg.get("picked"):
            await query.answer("Ты уже сделал выбор.", show_alert=True)
            return

        grid = sg["grid"]
        if idx < 0 or idx >= len(grid):
            await query.answer("Неверный выбор.", show_alert=True)
            return

        # помечаем как выбранное
        sg["picked"] = True
        chosen_reward = int(grid[idx])

        # — Начисляем спины в таблице (без переполнения MAX_SPINS)
        try:
            s_users_local = sheet_users()
            # row уже хранится в sg (если была передана)
            row_for_user = sg.get("row")
            if not row_for_user:
                # fallback: найти строку
                row_for_user, rec = find_user_row_fast(s_users_local, user_id)
            # текущее количество спинов (с безопасным парсингом)
            _, rec = find_user_row_fast(s_users_local, user_id)
            current_spins = int(rec.get("SPINS") or 0)
            new_spins = min(current_spins + chosen_reward, MAX_SPINS)
            spin_col = column_letter_by_name(s_users_local, "SPINS")
            s_users_local.update([[new_spins]], f"{spin_col}{row_for_user}", value_input_option="USER_ENTERED")
        except Exception as e:
            logger.exception("Ошибка при начислении супер-спинов: %s", e)
            await query.answer("Ошибка начисления. Попробуй позже.", show_alert=True)
            # опционально: откат picked = False
            sg["picked"] = False
            return

        # Редактируем сообщение — показываем раскрытое поле и результат

        if chosen_reward == 1:
            spin_word = "спин"
        else:
            spin_word = "спина" 
        try:
            reveal_text = f"Ты получаешь: +{chosen_reward} {spin_word}!\n\nПоле открыто:"
            await query.message.edit_text(reveal_text, reply_markup=build_super_markup(hidden=False, grid=grid, chosen_idx=idx))
        except Exception:
            # возможно, message_id устарел — просто отправим новое сообщение
            await context.bot.send_message(chat_id=query.message.chat_id, text=f"Ты выбрал: +{chosen_reward} спина!")
        # очистим state через небольшую паузу (или сразу)
        context.user_data.pop("super_game", None)
        return

    # subscription reward
    if data == "reward_sub":
        s_users = sheet_users()
        row, record = find_user_row_fast(s_users, user_id)
        if record is None:
            await query.message.edit_text("😿 Ты ещё не зарегистрирован. Сначала пропиши /start.")
            return

        if str(record.get("SUB_GG_USED") or "").strip() == "1":
            text = "🎁 Ты уже получал награду за подписку."
        else:
            try:
                member = await context.bot.get_chat_member(chat_id=BONUS_CHANNEL, user_id=user_id)
                if member.status not in ("member", "administrator", "creator"):
                    text = f"😿 Ты не подписан на {BONUS_CHANNEL}. Подпишись и попробуй снова."
                else:
                    spins = int(record.get("SPINS") or 0)
                    new_spins = min(spins + 3, MAX_SPINS)
                    spin_col = column_letter_by_name(s_users, "SPINS")
                    sub_col = column_letter_by_name(s_users, "SUB_GG_USED")
                    s_users.update([[new_spins]], f"{spin_col}{row}")
                    s_users.update([["1"]], f"{sub_col}{row}")

                    text = f"🎉 Спасибо за подписку! Ты получил +3 спина. Теперь {new_spins}."
            except Exception as e:
                text = f"⚠️ Не удалось проверить подписку: {e}"

        _, new_record = find_user_row_fast(s_users, user_id)
        await query.message.edit_text(get_main_menu_text(new_record) + "\n\n" + text, reply_markup=get_main_menu_markup())
        return

    # enter promo
    if data == "promo_enter":
        context.user_data["awaiting_promo"] = True
        context.user_data["promo_prompt_mid"] = query.message.message_id
        await query.message.edit_text("✏️ Введи промокод (одним сообщением). После ввода бот вернёт в главное меню.")
        return

    # CHANGE NICK (новая логика)
    if data == "change_nick":
        keyboard = [
            [InlineKeyboardButton("✨ Использовать @username", callback_data="nick_use_username")],
            [InlineKeyboardButton("✏️ Ввести вручную", callback_data="nick_manual")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_main")],
        ]
        await query.message.edit_text("Выбери способ смены ника:", reply_markup=InlineKeyboardMarkup(keyboard))
        return


# --- Core: handle spin, update SPINS, CATS_ID, and SUM (points) ---
async def handle_spin_and_send(chat_id, user_id, context: ContextTypes.DEFAULT_TYPE):
    s_users = sheet_users()

    # Найти пользователя (row, record). Если нет — создать.
    row, record = find_user_row_fast(s_users, user_id)
    if record is None:
        create_new_user(s_users, user_id)
        row, record = find_user_row_fast(s_users, user_id)

    # Текущее количество спинов
    try:
        spins = int(record.get("SPINS") or 0)
    except Exception:
        spins = 0

    if spins <= 0:
        await context.bot.send_message(chat_id=chat_id, text="😿 У тебя нет спинов! Получи их в разделе «Награды».")
        return

    # Получаем список всех котов из таблицы
    try:
        cats = get_cats_cached()
    except Exception as e:
        logger.exception("Ошибка при получении списка котов: %s", e)
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Ошибка получения каталога котов. Попробуй позже.")
        return

    # Разбираем, какие ID уже есть у пользователя (поддерживаем разные разделители)
    cats_id_raw = record.get("CATS_ID") or ""
    # split по | , ; пробелам и т.п.
    owned_tokens = [t.strip() for t in re.split(r"[|,;\\s]+", str(cats_id_raw)) if t.strip()]
    owned_set = set(owned_tokens)

    # Собираем все ID в каталоге
    all_cat_ids = {str(c.get("id")) for c in cats if c.get("id") is not None}

    # Неполученные id
    not_owned_ids = list(all_cat_ids - owned_set)

    if not not_owned_ids:
        # Пользователь собрал всех котов — не тратим спин
        await context.bot.send_message(chat_id=chat_id, text="🎉 У тебя уже все карточки! Спин не потрачен.")
        return

    # Выбираем редкость по весам и пытаемся найти неполученного кота в этой редкости
    rarity = choose_rarity(RARITY_WEIGHTS)
    available_unowned = [c for c in cats if c["rarity"] == rarity and str(c["id"]) not in owned_set]

    if available_unowned:
        chosen = random.choice(available_unowned)
    else:
        # если в выбранной редкости нет новых — выбираем случайного неполученного кота среди всех
        unowned_cats = [c for c in cats if str(c["id"]) not in owned_set]
        if not unowned_cats:
            # на всякий случай (добавочная защита)
            await context.bot.send_message(chat_id=chat_id, text="🎉 Похоже, у тебя уже все карточки. Спин не потрачен.")
            return
        chosen = random.choice(unowned_cats)
        rarity = chosen["rarity"]  # скорректируем редкость для начисления очков

    # --- успешно выбран неполученный кот -> теперь тратим спин и записываем изменения ---
    new_spins = spins - 1
    try:
        s_users.update([[new_spins]], f"D{row}", value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception("Не удалось списать спин для пользователя %s: %s", user_id, e)
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Ошибка базы: не удалось списать спин. Попробуй позже.")
        return

    # Обновляем CATS_ID (добавляем без дублей)
    chosen_id_str = str(chosen.get("id"))
    owned_set.add(chosen_id_str)
    # Сортируем: по числу если возможно, иначе по строке
    def _sort_key(x):
        return (int(x) if x.isdigit() else float("inf"), x)
    try:
        sorted_ids = sorted(owned_set, key=_sort_key)
    except Exception:
        sorted_ids = sorted(owned_set)
    new_cats_id = " | ".join(sorted_ids)
    try:
        s_users.update([[new_cats_id]], f"C{row}", value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception("Не удалось обновить CATS_ID для %s: %s", user_id, e)
        # не откатываем спин, просто логируем — можно добавить откат при желании

    # Обновляем SUM (очки)
    try:
        sum_col_letter = column_letter_by_name(s_users, "SUM")
    except Exception as e:
        logger.exception("Ошибка при подготовке колонки SUM: %s", e)
        sum_col_letter = None

    try:
        current_sum_raw = record.get("SUM")
        try:
            current_sum = int(current_sum_raw or 0)
        except Exception:
            current_sum = int(str(current_sum_raw).strip() or 0)
    except Exception:
        current_sum = 0

    gained = points_for_rarity(chosen.get("rarity"))
    new_sum = current_sum + gained
    if sum_col_letter:
        try:
            s_users.update([[new_sum]], f"{sum_col_letter}{row}")
        except Exception as e:
            logger.exception("Не удалось обновить SUM для %s: %s", user_id, e)

    logger.info("User %s получил кот %s (редкость=%s), +%d очков, спины %d->%d",
                user_id, chosen.get("id"), chosen.get("rarity"), gained, spins, new_spins)

    # Обработка ссылки Drive -> direct download
    url = (chosen.get("url") or "").strip()
    if "drive.google.com" in url:
        if "/d/" in url:
            file_id = url.split("/d/")[1].split("/")[0]
            url = f"https://drive.google.com/uc?export=download&id={file_id}"
        elif "id=" in url:
            file_id = url.split("id=")[1].split("&")[0]
            url = f"https://drive.google.com/uc?export=download&id={file_id}"

    # Формируем подпись
    rarity_label = RARITY_STYLES.get(chosen.get("rarity"), chosen.get("rarity"))
    caption = f"{rarity_label}\n{chosen.get('desc')}\n\n⭐ За эту карточку: +{gained} ⭐"

    # Попытка отправить изображение по URL, затем fallback на скачивание + отправку байтов
    try:
        await context.bot.send_photo(chat_id=chat_id, photo=url, caption=caption)
    except Exception as e:
        logger.warning("send_photo по URL не удался: %s; пытаюсь скачать и отправить байты...", e)
        try:
            from io import BytesIO

            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as resp:
                    if resp.status != 200:
                        raise Exception(f"HTTP {resp.status}")

                    content = await resp.read()

            bio = BytesIO(content)
            bio.name = f"cat_{chosen.get('id')}.jpg"

            await context.bot.send_photo(chat_id=chat_id, photo=bio, caption=caption)

        except Exception as e2:
            logger.exception("Не удалось скачать/отправить изображение: %s", e2)
            await context.bot.send_message(chat_id=chat_id, text="(Не удалось отправить изображение)\n" + caption)


# --- Handle promo & nick input text ---
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Защита: гарантируем, что user_data существует
    if context.user_data is None:
        context.user_data = {}

    user_id = update.message.from_user.id
    chat_id = update.message.chat_id
    text = update.message.text.strip()

    # NICK flow (высший приоритет)
    if context.user_data.get("awaiting_nick"):
        new_nick = text.strip()
        context.user_data["awaiting_nick"] = False
        prompt_mid = context.user_data.get("nick_prompt_mid")
        # ❗ Защита: ник не должен начинаться с @ и вообще содержать @
        if "@" in new_nick:
            await update.message.reply_text(
                "🚫 Ник не должен содержать символ '@'. Введи другой ник."
            )
            # Возвращаем пользователя снова в режим ввода ника
            context.user_data["awaiting_nick"] = True
            return

        s_users = sheet_users()
        row, record = find_user_row_fast(s_users, user_id)
        if record is None:
            await update.message.reply_text("😿 Ты ещё не зарегистрирован. Сначала /start.")
            context.user_data["nick_prompt_mid"] = None
            return
        # sanitize nick (max length)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        # determine NICK column: if header exists use it, otherwise append header "NICK"
        headers = s_users.row_values(1)
        nick_col_idx = None
        for idx, h in enumerate(headers, start=1):
            if str(h).strip().upper() == "NICK":
                nick_col_idx = idx
                break
        if not nick_col_idx:
            next_idx = len(headers) + 1
            s_users.update([["NICK"]], f"{colnum_to_letter(next_idx)}1", value_input_option="USER_ENTERED")
            nick_col_idx = next_idx
        nick_col_letter = colnum_to_letter(nick_col_idx)
        try:
            s_users.update([[new_nick]], f"{nick_col_letter}{row}", value_input_option="USER_ENTERED")
        except Exception as e:
            logger.exception("Не удалось записать ник: %s", e)
            await update.message.reply_text("⚠️ Не удалось установить ник. Попробуй позже.")
            context.user_data["nick_prompt_mid"] = None
            return
        # respond: edit old prompt message back to main menu if possible
        _, new_record = find_user_row_fast(s_users, user_id)
        if prompt_mid:
            try:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=prompt_mid, text=get_main_menu_text(new_record), reply_markup=get_main_menu_markup())
            except Exception:
                await context.bot.send_message(chat_id=chat_id, text=get_main_menu_text(new_record), reply_markup=get_main_menu_markup())
        else:
            await context.bot.send_message(chat_id=chat_id, text=get_main_menu_text(new_record), reply_markup=get_main_menu_markup())
        context.user_data["nick_prompt_mid"] = None
        return

    # PROMO flow
    if context.user_data.get("awaiting_promo"):
        promo = text.strip().upper()
        context.user_data["awaiting_promo"] = False
        prompt_mid = context.user_data.get("promo_prompt_mid")
        promo_data = load_promo_codes()
        s_users = sheet_users()
        row, record = find_user_row_fast(s_users, user_id)
        if record is None:
            await update.message.reply_text("😿 Ты ещё не зарегистрирован. Сначала /start.")
            context.user_data["promo_prompt_mid"] = None
            return

        if promo in promo_data:
            meta = promo_data[promo]
            col_letter = meta["column"].strip().upper()
            col_header = get_header_name_by_letter(s_users, col_letter)
            used = str(record.get(col_header) or "").strip()
            if used == "1":
                result_text = "🚫 Ты уже использовал этот промокод."
            else:
                spins = int(record.get("SPINS") or 0)
                new_spins = min(spins + meta["bonus"], MAX_SPINS)
                s_users.update([[new_spins]], f"D{row}")
                s_users.update([["1"]], f"{col_letter}{row}")
                result_text = f"{meta['desc']}\n🎉 +{meta['bonus']} спина! Теперь у тебя {new_spins}."
        else:
            result_text = "❌ Неверный промокод."

        prompt_mid = context.user_data.get("promo_prompt_mid")
        if prompt_mid:
            # edit prompt message into main menu + result
            _, new_record = find_user_row_fast(s_users, user_id)
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=prompt_mid,
                    text=get_main_menu_text(new_record) + "\n\n" + result_text,
                    reply_markup=get_main_menu_markup(),
                )
            except Exception:
                await context.bot.send_message(chat_id=chat_id, text=get_main_menu_text(new_record), reply_markup=get_main_menu_markup())
                await context.bot.send_message(chat_id=chat_id, text=result_text)
        else:
            _, new_record = find_user_row_fast(s_users, user_id)
            await context.bot.send_message(chat_id=chat_id, text=get_main_menu_text(new_record), reply_markup=get_main_menu_markup())
            await context.bot.send_message(chat_id=chat_id, text=result_text)

        context.user_data["promo_prompt_mid"] = None
        return

    # если не промо/ник — игнорируем текст
    return

def build_super_markup(hidden=True, grid=None, chosen_idx=None):
    """
    Возвращает InlineKeyboardMarkup для 3x3.
    - hidden=True: все кнопки выглядят "❓" (callback = super_pick:{i}).
    - hidden=False: показываем значения и добавляем внизу кнопку "⬅️ В главное меню".
    - chosen_idx — выделенная клетка (0..8).
    """
    keyboard = []
    for r in range(3):
        row_buttons = []
        for c in range(3):
            i = r * 3 + c
            if hidden:
                text = "❓"
            else:
                sym = "🟢🔵🟣"
                val = int(grid[i])
                val = sym[val-1]  # заменяем цифру на цветной кружок
                prefix = "👉" if (chosen_idx is not None and i == chosen_idx) else ""
                text = f"{prefix} {val}"
            row_buttons.append(InlineKeyboardButton(text, callback_data=f"super_pick:{i}"))
        keyboard.append(row_buttons)

    # Если поле раскрыто — добавляем внизу кнопку возврата в главное меню
    if not hidden:
        keyboard.append([InlineKeyboardButton("⬅️ В главное меню", callback_data="back_main")])

    return InlineKeyboardMarkup(keyboard)

async def offer_super_game(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE, s_users, row, streak: int, message_obj=None):
    """
    Создаёт поле 3x3, сохраняет state в context.user_data и выводит его.
    Если передан message_obj (например query.message) — редактируем его.
    """
    # распределение (можно менять)
    distribution = {5: 1, 3: 3, 2: 5}
    grid = make_super_grid(distribution)

    # store state
    context.user_data["super_game"] = {
        "grid": grid,
        "created_at": _time.time(),
        "picked": False,
        "row": row,
        "user_id": user_id,
    }

    streak_bar = make_streak_bar(streak)

    prompt = (
        f"🎉 Супер-игра!\n\nТвой стрик: {streak}\n{streak_bar}\n\nВыбери одну из 9 клеток.\n🟢 +1 спин, 🔵 +2 спина, 🟣 +3 спина."
    )

    # Если есть объект сообщения — редактируем его (чтобы не оставлять старое меню),
    # иначе отправляем новое сообщение (на всякий случай).
    try:
        if message_obj is not None:
            await message_obj.edit_text(prompt, reply_markup=build_super_markup(hidden=True, grid=grid))
            # сохраним message_id для дальнейшего редактирования
            context.user_data["super_game"]["message_id"] = message_obj.message_id
            context.user_data["super_game"]["chat_id"] = message_obj.chat_id
        else:
            sent = await context.bot.send_message(chat_id=chat_id, text=prompt, reply_markup=build_super_markup(hidden=True, grid=grid))
            context.user_data["super_game"]["message_id"] = sent.message_id
            context.user_data["super_game"]["chat_id"] = sent.chat_id
    except Exception as e:
        logger.exception("Не удалось показать супер-игру: %s", e)
        # fallback: отправим отдельное сообщение
        sent = await context.bot.send_message(chat_id=chat_id, text=prompt, reply_markup=build_super_markup(hidden=True, grid=grid))
        context.user_data["super_game"]["message_id"] = sent.message_id
        context.user_data["super_game"]["chat_id"] = sent.chat_id


async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    # Получаем данные (из кэша или обновим кэш при просрочке)
    records = await get_leaderboard_cached()

    if not records:
        await query.message.edit_text("Пока нет данных для отображения лидерборда.", reply_markup=get_main_menu_markup())
        return

    # Формируем текст топ-5
    leaderboard_text = "🏆 Топ-5 игроков:\n\n"
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    for i, r in enumerate(records[:5], start=1):
        score = r.get("SUM", 0)
        # анонимизируем: показываем место и первые 6 цифр ID (или '#N')
        uid = str(r.get("USER_ID") or "")
        # используем NICK, если он есть
        nick = (r.get("NICK") or "").strip()
        display = nick if nick else (f"Игрок #{uid[-6:]}" if uid else f"Игрок #{i}")
        medal = medals[i-1] if i-1 < len(medals) else f"{i}."
        leaderboard_text += f"{medal} {display} — {score} ⭐\n"

    # Найдём место текущего пользователя
    user_pos = None
    user_sum = 0
    for i, r in enumerate(records, start=1):
        if str(r.get("USER_ID")) == str(user_id):
            user_pos = i
            user_sum = r.get("SUM", 0)
            break

    if user_pos:
        leaderboard_text += f"\n📍 Твоё место: {user_pos}-е, {user_sum} ⭐"
    else:
        leaderboard_text += "\n😿 Ты пока не в рейтинге. Попробуй сделать спин!"

    keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_main")]]
    await query.message.edit_text(leaderboard_text, reply_markup=InlineKeyboardMarkup(keyboard))


async def reload_leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with leaderboard_cache_lock:
        LEADERBOARD_CACHE["records"] = None
        LEADERBOARD_CACHE["ts"] = 0
    await update.message.reply_text("Кэш лидерборда сброшен.")


# --- Main and handlers registration ---
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reload_lb", reload_leaderboard_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Бот запущен")
    app.run_polling()

def keep_alive():
    port = int(os.environ.get("PORT", 8080))
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        print(f"✅ Keep-alive web server running on port {port}")
        httpd.serve_forever()

threading.Thread(target=keep_alive, daemon=True).start()

if __name__ == "__main__":
    main()

