# winter_frame.py
import logging
from io import BytesIO
from typing import List, Tuple
from telegram import InputFile, InputMediaPhoto
import asyncio
from typing import Optional
import os


import requests
from PIL import Image, ImageOps
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    InputMediaPhoto,
    InputFile,
)
from telegram.ext import (
    ContextTypes,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

import winter  # ваш модуль для работы с winter sheets

logger = logging.getLogger(__name__)
FRAME_DEBUG = False

# ------------------ Настройки рамки (настройте под себя) ------------------
BG_WIDTH = 1280
BG_HEIGHT = 800

# Словарь слотов рамки для каждого фона
FRAME_SLOTS = {
    10: [  # фон 10
        {"x":  83, "y": 102, "w": 237, "h": 268},
        {"x": 405, "y": 102, "w": 238, "h": 268},
        {"x": 718, "y": 102, "w": 239, "h": 268},
        {"x": 236, "y": 458, "w": 235, "h": 268},
        {"x": 554, "y": 458, "w": 235, "h": 268},
    ],
    11: [  # фон 11 (замените на реальные координаты)
        {"x":  83, "y": 102, "w": 237, "h": 268},
        {"x": 405, "y": 102, "w": 238, "h": 268},
        {"x": 718, "y": 102, "w": 239, "h": 268},
        {"x": 236, "y": 458, "w": 235, "h": 268},
        {"x": 554, "y": 458, "w": 235, "h": 268},
    ],
    12: [  # фон 12
        {"x":  95, "y": 102, "w": 236, "h": 268},
        {"x": 416, "y": 102, "w": 238, "h": 268},
        {"x": 728, "y": 102, "w": 238, "h": 268},
        {"x": 245, "y": 457, "w": 238, "h": 269},
        {"x": 564, "y": 457, "w": 235, "h": 269},
    ],
}
FRAME_SEP = " | "

# ------------------ Вспомогательные функции для работы с таблицей ------------------

def _ensure_frame_column(sheet):
    return winter.column_letter_by_name(sheet, "FRAME")

# ------------------ Frame cache helpers (Telegram file_id) ------------------

def _ensure_frame_fileid_column(sheet):
    """
    Убедиться, что колонка FRAME_FILE_ID существует — вернуть её букву.
    """
    return winter.column_letter_by_name(sheet, "FRAME_FILE_ID")


def invalidate_user_frame_cache(s_users, row):
    """
    Сбросить кэшированное telegram file_id для пользователя (FRAME_FILE_ID = "").
    """
    try:
        col = _ensure_frame_fileid_column(s_users)
        s_users.update([[""]], f"{col}{row}", value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("invalidate_user_frame_cache failed")


async def _upload_image_and_cache_file_id(context: ContextTypes.DEFAULT_TYPE, s_users, row: int, user_id: int, img_buf: BytesIO) -> Optional[str]:
    """
    Загружает BytesIO в Telegram (в ADMIN_ID) чтобы получить file_id, записывает file_id в таблицу и возвращает его.
    Удаляет сообщение в admin-чате после загрузки.
    """
    try:
        admin_chat = getattr(winter, "ADMIN_ID", None)
        if not admin_chat:
            logger.warning("ADMIN_ID не настроен в winter модуле; file_id не будет кэшироваться.")
            return None
        # обязательно rewind
        if hasattr(img_buf, "seek"):
            img_buf.seek(0)

        # отправляем в админ-чат (чтобы получить file_id), потом удаляем сообщение
        sent = await context.bot.send_photo(chat_id=admin_chat, photo=InputFile(img_buf, filename="frame.png"), caption=f"cache frame {user_id}")
        # получаем file_id
        fid = None
        try:
            if getattr(sent, "photo", None):
                fid = sent.photo[-1].file_id
            elif getattr(sent, "document", None):
                fid = sent.document.file_id
        except Exception:
            logger.exception("Не удалось прочитать file_id из ответа send_photo")
            fid = None

        # удаляем сообщение-источник (чтобы не мусорить в админ-чате)
        try:
            await context.bot.delete_message(chat_id=admin_chat, message_id=sent.message_id)
        except Exception:
            pass

        # записываем в таблицу
        if fid:
            try:
                col = _ensure_frame_fileid_column(s_users)
                s_users.update([[fid]], f"{col}{row}", value_input_option="USER_ENTERED")
            except Exception:
                logger.exception("Не удалось записать FRAME_FILE_ID в таблицу")
            return fid
    except Exception:
        logger.exception("_upload_image_and_cache_file_id failed")
    return None


async def get_or_create_cached_frame_file_id(context: ContextTypes.DEFAULT_TYPE, s_users, row: int, user_id: int, generate_fn) -> Optional[str]:
    """
    Если в таблице уже есть FRAME_FILE_ID — вернуть его.
    Иначе: сгенерировать изображение через generate_fn(user_id) (может быть тяжёлой операцией;
    вызываем в executor, чтобы не блокировать loop), загрузить в Telegram, сохранить file_id в таблице и вернуть.
    """
    # 1) прочитать текущий file_id (свежо)
    try:
        headers = s_users.row_values(1)
        upper = [h.upper() for h in headers]
        fid = ""
        if "FRAME_FILE_ID" in upper:
            idx = upper.index("FRAME_FILE_ID")
            row_vals = s_users.row_values(row)
            if idx < len(row_vals):
                fid = row_vals[idx] or ""
    except Exception:
        logger.exception("Не удалось прочитать FRAME_FILE_ID из таблицы")
        fid = ""

    if fid:
        return fid

    # 2) сгенерировать изображение (в executor, т.к. generate_fn может быть блокирующей)
    try:
        loop = asyncio.get_running_loop()
        out = await loop.run_in_executor(None, generate_fn, user_id)  # блокирующая генерация в пуле
        if not out:
            logger.exception("generate_fn вернул пустой результат")
            return None
    except Exception:
        logger.exception("Ошибка при генерации рамки в executor, пробуем синхронно")
        try:
            out = generate_fn(user_id)
        except Exception:
            logger.exception("Синхронная генерация тоже упала")
            return None

    # 3) загрузить и записать file_id
    try:
        fid = await _upload_image_and_cache_file_id(context, s_users, row, user_id, out)
        return fid
    except Exception:
        logger.exception("Ошибка при загрузке/кешировании изображения")
        return None


async def send_user_frame_fast(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE, generate_fn):
    """
    Удобная обёртка: пытается отправить кэшированную рамку (по file_id).
    Если нет file_id — создаёт и кэширует, затем отправляет.
    Возвращает True/False.
    """
    try:
        s_users = winter.sheet_winter_users()
        row, _ = winter.find_winter_user_row(s_users, user_id)
        if row is None:
            winter.create_new_winter_user(s_users, user_id)
            row, _ = winter.find_winter_user_row(s_users, user_id)

        # попытка взять file_id из таблицы
        headers = s_users.row_values(1)
        upper = [h.upper() for h in headers]
        fid = ""
        if "FRAME_FILE_ID" in upper:
            idx = upper.index("FRAME_FILE_ID")
            row_vals = s_users.row_values(row)
            if idx < len(row_vals):
                fid = row_vals[idx] or ""

        if fid:
            # отправляем по file_id (быстро)
            try:
                await context.bot.send_photo(chat_id=chat_id, photo=fid, caption="Твоя рамка:")
                return True
            except Exception:
                logger.exception("Отправка по file_id не удалась — попробуем пересоздать")
                # очистим нерабочий fid
                try:
                    col = _ensure_frame_fileid_column(s_users)
                    s_users.update([[""]], f"{col}{row}", value_input_option="USER_ENTERED")
                except Exception:
                    logger.exception("Не удалось очистить нерабочий FRAME_FILE_ID")

        # если fid нет или невалиден — создаём и кэшируем
        fid2 = await get_or_create_cached_frame_file_id(context, s_users, row, user_id, generate_fn)
        if fid2:
            try:
                await context.bot.send_photo(chat_id=chat_id, photo=fid2, caption="Твоя рамка:")
                return True
            except Exception:
                logger.exception("Не удалось отправить уже закэшированную рамку")
        # fallback: отправим напрямую BytesIO с генерацией (если всё остальное упало)
        try:
            loop = asyncio.get_running_loop()
            out = await loop.run_in_executor(None, generate_fn, user_id)
            if hasattr(out, "seek"):
                out.seek(0)
            await context.bot.send_photo(chat_id=chat_id, photo=InputFile(out, filename="frame.png"), caption="Твоя рамка:")
            return True
        except Exception:
            logger.exception("Fallback: отправка напрямую bytes также не удалась")
            return False
    except Exception:
        logger.exception("send_user_frame_fast failed")
        return False


def _read_frame_str_from_record(record) -> str:
    frame_raw = ""
    if record:
        frame_raw = record.get("FRAME") or record.get("Frame") or record.get("frame") or ""
    if not frame_raw or str(frame_raw).strip() == "":
        return FRAME_SEP.join(["0"] * 5)
    return str(frame_raw)

def get_user_frame_list(user_id: int) -> Tuple[int, List[int]]:
    s_users = winter.sheet_winter_users()
    row, record = winter.find_winter_user_row(s_users, user_id)
    if record is None:
        winter.create_new_winter_user(s_users, user_id)
        row, record = winter.find_winter_user_row(s_users, user_id)

    frame_str = _read_frame_str_from_record(record)
    parts = [p.strip() for p in frame_str.split("|")]
    ids = []
    for p in parts:
        try:
            ids.append(int(p))
        except Exception:
            ids.append(0)
    if len(ids) < 5:
        ids += [0] * (5 - len(ids))
    else:
        ids = ids[:5]
    return row, ids

def set_user_frame_slot(user_id: int, slot_index: int, card_id: int) -> bool:
    if slot_index < 0 or slot_index >= 5:
        raise ValueError("slot_index must be 0..4")
    s_users = winter.sheet_winter_users()
    row, frame_ids = get_user_frame_list(user_id)
    if row is None:
        return False
    frame_ids[slot_index] = int(card_id) if card_id else 0
    frame_str = FRAME_SEP.join(str(i) for i in frame_ids)
    try:
        col_letter = _ensure_frame_column(s_users)
        s_users.update([[frame_str]], f"{col_letter}{row}", value_input_option="USER_ENTERED")
        # сразу инвалидируем кэш рамки для этого пользователя
        try:
            invalidate_user_frame_cache(s_users, row)
        except Exception:
            logger.exception("Не удалось инвалидировать кэш после set_user_frame_slot")
        return True
    except Exception as e:
        logger.exception("Не удалось записать FRAME в таблицу: %s", e)
        return False


# ------------------ Генерация итоговой рамки ------------------

def _drive_direct_url(url: str) -> str:
    if not url:
        return url
    url = url.strip()
    if "drive.google.com" in url:
        try:
            if "/d/" in url:
                file_id = url.split("/d/")[1].split("/")[0]
                return f"https://drive.google.com/uc?export=download&id={file_id}"
            if "id=" in url:
                file_id = url.split("id=")[1].split("&")[0]
                return f"https://drive.google.com/uc?export=download&id={file_id}"
        except Exception:
            return url
    return url

def generate_frame_image(user_id: int) -> BytesIO:
    """
    Robust generation of user frame:
    - reads FRAME_SET for user
    - attempts to load <FRAME_SET>.png (or assets/<FRAME_SET>.png)
    - forces background to BG_WIDTH x BG_HEIGHT using ImageOps.fit (guarantees exact size)
    - pastes cards into slots
    - saves a debug image next to module if FRAME_DEBUG is True
    """
    # info_lines собираются только если FRAME_DEBUG включён
    info_lines = [] if FRAME_DEBUG else None

    def log_info(s):
        # Если отладка включена — записываем в info_lines и (по желанию) логируем через logger.info
        if not FRAME_DEBUG:
            return
        try:
            logger.info(s)
        except Exception:
            pass
        info_lines.append(str(s))

    try:
        s_users = winter.sheet_winter_users()
    except Exception as e:
        logger.exception("Не удалось открыть sheet_winter_users: %s", e)
        img_blank = Image.new("RGB", (int(BG_WIDTH), int(BG_HEIGHT)), "white")
        buf_b = BytesIO()
        img_blank.save(buf_b, format="PNG")
        buf_b.seek(0)
        return buf_b

    # user row and frame ids
    row, frame_ids = get_user_frame_list(user_id)

    # read FRAME_SET (default 10)
    frame_set = 10
    try:
        headers = s_users.row_values(1)
        upper = [h.upper() for h in headers]
        if "FRAME_SET" in upper:
            idx = upper.index("FRAME_SET")
            row_vals = s_users.row_values(row)
            if idx < len(row_vals) and str(row_vals[idx]).strip():
                try:
                    frame_set = int(row_vals[idx])
                except Exception:
                    frame_set = 10
    except Exception as e:
        logger.exception("Ошибка чтения FRAME_SET: %s", e)

    log_info(f"User {user_id} FRAME_SET = {frame_set}")

    # pick slots
    slots = FRAME_SLOTS.get(frame_set, FRAME_SLOTS.get(10))
    log_info(f"Using {len(slots)} slots for frame {frame_set}")

    # locate background file
    bg_img = None
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__)) or "."
        candidates = [
            os.path.join(base_dir, f"{frame_set}.png"),
            os.path.join(base_dir, "assets", f"{frame_set}.png"),
            os.path.join(base_dir, "assets", str(frame_set) + ".PNG"),
            os.path.join(base_dir, f"{frame_set}.PNG"),
        ]
        found_path = None
        for p in candidates:
            if p and os.path.isfile(p):
                found_path = p
                break

        if found_path:
            log_info(f"Found background file: {found_path}")
            bg_img = Image.open(found_path)
            log_info(f"Original bg size: {bg_img.size}, mode={bg_img.mode}")
            # convert to RGBA (to preserve alpha) then fit to exact size
            try:
                bg_img = bg_img.convert("RGBA")
            except Exception:
                bg_img = bg_img.convert("RGB").convert("RGBA")
            # force-fit to exact BG_WIDTH x BG_HEIGHT (this crops/pads as needed)
            try:
                bg_img = ImageOps.fit(bg_img, (int(BG_WIDTH), int(BG_HEIGHT)), method=Image.LANCZOS)
                log_info(f"Bg after fit size: {bg_img.size}")
            except Exception as e:
                logger.exception("ImageOps.fit failed: %s", e)
                # fallback to simple resize
                try:
                    bg_img = bg_img.resize((int(BG_WIDTH), int(BG_HEIGHT)), Image.LANCZOS)
                    log_info(f"Bg after resize size: {bg_img.size}")
                except Exception as e2:
                    logger.exception("Fallback resize failed: %s", e2)
                    bg_img = None
        else:
            log_info(f"No background file found among candidates: {candidates}")
    except Exception as e:
        logger.exception("Ошибка при загрузке фона: %s", e)
        bg_img = None

    # prepare base image
    if bg_img is None:
        frame_img = Image.new("RGB", (int(BG_WIDTH), int(BG_HEIGHT)), "white")
        log_info("Using white background (bg_img not found)")
    else:
        # if bg_img is RGBA convert to RGB on copy
        try:
            frame_img = bg_img.convert("RGB").copy()
        except Exception:
            frame_img = Image.new("RGB", (int(BG_WIDTH), int(BG_HEIGHT)), "white")
            log_info("Failed to convert bg_img to RGB; using white fallback")

    # prepare cats map
    cats = winter.get_winter_cats_cached() or []
    cats_map = {str(c.get("id")): c for c in cats if c.get("id") is not None}

    # paste cards
    for idx, slot in enumerate(slots):
        try:
            card_id = int(frame_ids[idx]) if idx < len(frame_ids) else 0
        except Exception:
            card_id = 0
        if not card_id:
            continue

        cat_rec = cats_map.get(str(card_id))
        if not cat_rec:
            logger.warning("No cat record for id %s", card_id)
            continue

        url = (cat_rec.get("url") or "").strip()
        if not url:
            continue
        url = _drive_direct_url(url)

        try:
            resp = requests.get(url, timeout=12)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content))
        except Exception as e:
            logger.warning("Failed to download image %s: %s", url, e)
            continue

        try:
            if img.mode in ("RGBA", "LA"):
                bg = Image.new("RGBA", img.size, (255,255,255,255))
                bg.paste(img, (0,0), img)
                img = bg.convert("RGB")
            else:
                img = img.convert("RGB")
        except Exception:
            try:
                img = img.convert("RGB")
            except Exception:
                logger.exception("Failed to convert card image %s", url)
                continue

        # slot expected as dict with x,y,w,h
        x = int(slot.get("x", 0))
        y = int(slot.get("y", 0))
        w = int(slot.get("w", 0))
        h = int(slot.get("h", 0))
        if w <= 0 or h <= 0:
            logger.warning("Invalid slot size for frame %s slot %s", frame_set, slot)
            continue

        try:
            img_resized = ImageOps.fit(img, (w, h), method=Image.LANCZOS)
        except Exception:
            try:
                img_resized = img.resize((w, h))
            except Exception:
                logger.exception("Failed to resize card img %s", url)
                continue

        try:
            frame_img.paste(img_resized, (x, y))
        except Exception as e:
            logger.exception("Failed to paste card into frame: %s", e)
            continue

    # Save debug image and info next to module for inspection only if FRAME_DEBUG True
    if FRAME_DEBUG:
        try:
            debug_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"debug_frame_{user_id}_{frame_set}.png")
            frame_img.save(debug_name, format="PNG")
            log_info(f"Saved debug framing image: {debug_name} size={frame_img.size}")
            info_txt = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"debug_frame_{user_id}_{frame_set}.txt")
            with open(info_txt, "w", encoding="utf-8") as f:
                f.write("\n".join(info_lines or []))
            log_info(f"Wrote debug info: {info_txt}")
        except Exception:
            logger.exception("Не удалось записать debug файлы")

    # return bytes
    out = BytesIO()
    try:
        frame_img.save(out, format="PNG")
    except Exception:
        try:
            frame_img.convert("RGB").save(out, format="PNG")
        except Exception:
            logger.exception("Не удалось сохранить итоговое изображение")
    out.seek(0)
    return out



# ------------------ Telegram UI: обработчики ------------------

def _frame_menu_keyboard():
    kb = [
        [
            InlineKeyboardButton("1", callback_data="frame_pos:1"),
            InlineKeyboardButton("2", callback_data="frame_pos:2"),
            InlineKeyboardButton("3", callback_data="frame_pos:3"),
            InlineKeyboardButton("4", callback_data="frame_pos:4"),
            InlineKeyboardButton("5", callback_data="frame_pos:5"),
        ],
        [
            InlineKeyboardButton("Показать рамку", callback_data="frame_show"),
            InlineKeyboardButton("Очистить слот", callback_data="frame_clear_choice"),
        ],
        [
            InlineKeyboardButton("Очистить всё", callback_data="frame_clear_all"),
            InlineKeyboardButton("⬅️ Назад", callback_data="winter_main"),
        ],
    ]
    return InlineKeyboardMarkup(kb)

# --- БЕЗОПАСНЫЕ ХЕЛПЕРЫ ДЛЯ РЕДАКТИРОВАНИЯ СООБЩЕНИЙ --------------------

async def _maybe_delete_last_frame_photo(context: ContextTypes.DEFAULT_TYPE):
    """
    Если в context.user_data хранится ключ 'frame_last_photo' как (chat_id, message_id),
    попытаться удалить это сообщение (фото). Безопасно - обёрнуто в try/except.
    """
    key = "frame_last_photo"
    info = context.user_data.get(key)
    if not info:
        return
    try:
        chat_id, message_id = info
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        logger.exception("Не удалось удалить прошлое фото рамки (возможно уже удалено).")
    finally:
        context.user_data.pop(key, None)

async def safe_edit_message_text_or_caption(msg, text: str = None, reply_markup=None):
    """
    Редактирует text если это текстовое сообщение,
    или caption если это медиа (photo/document/video).
    Если ни text ни caption не доступны — пробуем edit_reply_markup.
    Если всё падает — отправляем новое сообщение (fallback).
    """
    if msg is None:
        return
    try:
        if getattr(msg, "text", None) is not None:
            await msg.edit_text(text, reply_markup=reply_markup)
            return
        if getattr(msg, "caption", None) is not None:
            await msg.edit_caption(caption=text, reply_markup=reply_markup)
            return
        # fallback: просто обновим inline-клавиатуру
        await msg.edit_reply_markup(reply_markup=reply_markup)
        return
    except Exception:
        logger.exception("safe_edit failed, trying fallback reply/send")
        try:
            # fallback: отправим новое текстовое сообщение
            await msg.reply_text(text or "", reply_markup=reply_markup)
        except Exception:
            logger.exception("safe_edit fallback also failed")

async def _message_has_media(msg) -> bool:
    if msg is None:
        return False
    # проверяем photo/document/video/sticker и т.п.
    try:
        if getattr(msg, "photo", None):
            return True
        if getattr(msg, "document", None):
            return True
        if getattr(msg, "video", None):
            return True
    except Exception:
        pass
    return False

# ------------------ Обработчики ------------------

async def cmd_frame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.message:
        await update.message.reply_text("🖼 Рамка — выберите слот (1–5) или действие:", reply_markup=_frame_menu_keyboard())
    else:
        # callback flow
        await update.callback_query.message.reply_text("🖼 Рамка — выберите слот (1–5) или действие:", reply_markup=_frame_menu_keyboard())

async def frame_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # Защита: гарантируем, что context.user_data — словарь
    if context.user_data is None:
        context.user_data = {}

    data = query.data or ""
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    msg = query.message

    if data == "frame_open":
        await safe_edit_message_text_or_caption(msg, "🖼 Новогодняя рамка\n\nВыбери слот (1–5), чтобы вставить карточку кота:", reply_markup=_frame_menu_keyboard())
        return

    if data.startswith("frame_pos:"):
        try:
            pos = int(data.split(":", 1)[1])
            if not (1 <= pos <= 5):
                raise ValueError()
        except Exception:
            await query.message.reply_text("Неверный номер слота.")
            return

        context.user_data["awaiting_frame_id"] = True
        context.user_data["awaiting_frame_slot"] = pos - 1

        s_users = winter.sheet_winter_users()
        row, record = winter.find_winter_user_row(s_users, user_id)
        owned_raw = ""
        if record:
            owned_raw = record.get("W_CATS_ID") or record.get("W_CATS") or record.get("W_CATS_ID".upper()) or ""
        owned_tokens = [t.strip() for t in __import__("re").split(r"[|,;\\s]+", str(owned_raw)) if t.strip()]
        owned_preview = ", ".join(owned_tokens[:12]) if owned_tokens else "(у тебя нет зимних карточек)"
        txt = (
            f"Выбраан слот #{pos}. Введи ID карточки (числом) из твоих карточек.\n\n"
            f"Твои карточки (превью): {owned_preview}\n\n"
            "Отправь ID как обычное сообщение. Чтобы отменить — нажми кнопку Отмена."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="frame_cancel_input")]])
        try:
            await safe_edit_message_text_or_caption(msg, txt, reply_markup=kb)
        except Exception:
            await query.message.reply_text(txt, reply_markup=kb)
        return

    if data == "frame_cancel_input":
        context.user_data.pop("awaiting_frame_id", None)
        context.user_data.pop("awaiting_frame_slot", None)
        await safe_edit_message_text_or_caption(msg, "Ввод ID отменён.", reply_markup=_frame_menu_keyboard())
        return

    if data == "frame_show":
        # удаляем старое меню (текущее сообщение с кнопками), чтобы не было мусора
        try:
            await msg.delete()
        except Exception:
            logger.exception("Не удалось удалить старое меню рамки (возможно, уже удалено)")

        # удаляем превью/предыдущее фото рамки, если оно хранится
        try:
            await _maybe_delete_last_frame_photo(context)
        except Exception:
            logger.exception("Не удалось удалить прошлую картинку рамки (non-fatal)")

        # удаляем превью-подтверждение (если есть) — чтобы не осталось лишних сообщений
        try:
            mid = context.user_data.get("frame_confirm_msg_id")
            if mid:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=mid)
                except Exception:
                    # не критично, просто логируем
                    logger.exception("Не удалось удалить message_id превью подтверждения")
                context.user_data.pop("frame_confirm_msg_id", None)
        except Exception:
            logger.exception("Ошибка при попытке удалить frame_confirm_msg_id (non-fatal)")

        # Попытка отправить закэшированную рамку (быстро)
        try:
            ok = await send_user_frame_fast(chat_id, user_id, context, generate_frame_image)
            if not ok:
                await context.bot.send_message(chat_id=chat_id, text="Ошибка при отправке рамки.")
                # продолжаем — отправим меню ниже
        except Exception as e:
            logger.exception("frame_show (send_user_frame_fast) failed: %s", e)
            # fallback: старая логика — генерируем в executor и отправляем
            try:
                loop = asyncio.get_running_loop()
                out = await loop.run_in_executor(None, generate_frame_image, user_id)
                if hasattr(out, "seek"):
                    out.seek(0)
                sent = await context.bot.send_photo(chat_id=chat_id, photo=InputFile(out, filename="frame.png"), caption="Твоя рамка:")
                try:
                    context.user_data["frame_last_photo"] = (chat_id, sent.message_id if hasattr(sent, "message_id") else None)
                except Exception:
                    context.user_data["frame_last_photo"] = (chat_id, sent.message_id if hasattr(sent, "message_id") else None)
            except Exception as e2:
                logger.exception("fallback frame send failed: %s", e2)
                await context.bot.send_message(chat_id=chat_id, text="Ошибка при генерации рамки.")

        # После отправки рамки — показываем новое меню (как новое сообщение)
        try:
            await context.bot.send_message(chat_id=chat_id, text="Меню рамки:", reply_markup=_frame_menu_keyboard())
        except Exception:
            # если и это упало — не фатально
            logger.exception("Не удалось отправить новое меню рамки после показа")
        return



    if data == "frame_clear_all":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Очистить всё", callback_data="frame_confirm_clear_all")],
            [InlineKeyboardButton("❌ Отмена", callback_data="frame_back")],
        ])
        await safe_edit_message_text_or_caption(msg, "Подтвердите очистку всех 5 слотов (данные будут перезаписаны).", reply_markup=kb)
        return

    if data == "frame_confirm_clear_all":
        s_users = winter.sheet_winter_users()
        row, frame_ids = get_user_frame_list(user_id)
        if row is None:
            return False
        frame_str = FRAME_SEP.join(["0"] * 5)
        try:
            col_letter = _ensure_frame_column(s_users)
            s_users.update([[frame_str]], f"{col_letter}{row}", value_input_option="USER_ENTERED")
            # инвалидируем cached file_id
            try:
                invalidate_user_frame_cache(s_users, row)
            except Exception:
                logger.exception("Не удалось инвалидировать кэш после очистки всех слотов")
            success = True
        except Exception as e:
            logger.exception("Не удалось записать FRAME в таблицу: %s", e)
            success = False

        if success:
            await safe_edit_message_text_or_caption(msg, "Все слоты очищены.", reply_markup=_frame_menu_keyboard())
        else:
            await safe_edit_message_text_or_caption(msg, "Ошибка при очистке. Попробуй снова.", reply_markup=_frame_menu_keyboard())
        return

    if data == "frame_clear_choice":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("1", callback_data="frame_clear:1"),
                InlineKeyboardButton("2", callback_data="frame_clear:2"),
                InlineKeyboardButton("3", callback_data="frame_clear:3"),
                InlineKeyboardButton("4", callback_data="frame_clear:4"),
                InlineKeyboardButton("5", callback_data="frame_clear:5"),
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data="frame_back")]
        ])
        await safe_edit_message_text_or_caption(msg, "Выберите слот, который нужно очистить:", reply_markup=kb)
        return

    if data.startswith("frame_clear:"):
        try:
            pos = int(data.split(":", 1)[1])
            if not (1 <= pos <= 5):
                raise ValueError()
        except Exception:
            await query.message.reply_text("Неверный слот.")
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить очистку", callback_data=f"frame_confirm_clear:{pos}")],
            [InlineKeyboardButton("❌ Отмена", callback_data="frame_back")],
        ])
        await safe_edit_message_text_or_caption(msg, f"Подтвердите очистку слота #{pos}:", reply_markup=kb)
        return

    if data.startswith("frame_confirm_clear:"):
        try:
            pos = int(data.split(":", 1)[1])
            ok = set_user_frame_slot(user_id, pos - 1, 0)
            if ok:
                await safe_edit_message_text_or_caption(msg, f"Слот #{pos} очищен.", reply_markup=_frame_menu_keyboard())
            else:
                await safe_edit_message_text_or_caption(msg, "Ошибка при очистке слота.", reply_markup=_frame_menu_keyboard())
        except Exception as e:
            logger.exception("frame_confirm_clear error: %s", e)
            await safe_edit_message_text_or_caption(msg, "Ошибка.", reply_markup=_frame_menu_keyboard())
        return

    if data == "frame_back":
        # 🔥 УДАЛЯЕМ ФОТО РАМКИ

        await safe_edit_message_text_or_caption(
            msg,
            "🖼 Меню рамки:",
            reply_markup=_frame_menu_keyboard()
        )
        return


    if data.startswith("frame_confirm_set:"):
        msg = query.message
        try:
            _, rest = data.split(":", 1)
            slot_str, card_str = rest.split(":", 1)
            slot_idx = int(slot_str)
            card_id = int(card_str)
        except Exception:
            await query.answer("Неверный формат подтверждения.", show_alert=True)
            return

        ok = set_user_frame_slot(user_id, slot_idx, card_id)
        if not ok:
            # Попытка аккуратно отредактировать текущее сообщение / показать ошибку
            try:
                await safe_edit_message_text_or_caption(msg, "Не удалось записать выбор в базу.", reply_markup=_frame_menu_keyboard())
            except Exception:
                try:
                    if getattr(msg, "photo", None):
                        await msg.edit_caption(caption="Не удалось записать выбор в базу.", reply_markup=_frame_menu_keyboard())
                    else:
                        await msg.edit_text("Не удалось записать выбор в базу.", reply_markup=_frame_menu_keyboard())
                except Exception:
                    try:
                        await context.bot.send_message(chat_id=chat_id, text="Не удалось записать выбор в базу.", reply_markup=_frame_menu_keyboard())
                    except Exception:
                        logger.exception("Не удалось уведомить пользователя об ошибке записи в базу")
            return

        # Попытка отправить закэшированную рамку (или создать и закешировать её)
        try:
            sent_ok = await send_user_frame_fast(chat_id, user_id, context, generate_frame_image)
        except Exception:
            logger.exception("send_user_frame_fast failed")
            sent_ok = False
        if not sent_ok:
            # Fallback: сгенерировать и отправить напрямую в executor, чтобы не блокировать loop
            try:
                loop = asyncio.get_running_loop()
                out = await loop.run_in_executor(None, generate_frame_image, user_id)
                if not out:
                    raise RuntimeError("generate_frame_image вернул пустое значение")
                # подготовка BytesIO для отправки
                if isinstance(out, BytesIO):
                    out.seek(0)
                    bio_upload = out
                else:
                    # поддержать разные типы возврата (BytesIO / PIL Image / bytes)
                    try:
                        if hasattr(out, "getvalue"):
                            bio_upload = BytesIO(out.getvalue())
                        else:
                            # если out — bytes
                            bio_upload = BytesIO(out)
                    except Exception:
                        out.seek(0)
                        bio_upload = BytesIO(out.read())

                bio_upload.name = "frame.png"
                bio_upload.seek(0)

                # попытка удалить прошлое фото (если есть)
                try:
                    await _maybe_delete_last_frame_photo(context)
                except Exception:
                    logger.exception("Не удалось удалить прошлую картинку рамки (non-fatal)")

                sent = await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=InputFile(bio_upload, filename="frame.png"),
                    caption="Рамка (обновлённая):"
                )

                # запомним последнее фото (опционально)
                try:
                    context.user_data["frame_last_photo"] = (sent.chat.id if hasattr(sent, "chat") else chat_id, sent.message_id)
                except Exception:
                    context.user_data["frame_last_photo"] = (chat_id, sent.message_id if hasattr(sent, "message_id") else None)

            except Exception as e:
                logger.exception("Не удалось сгенерировать/отправить рамку (fallback): %s", e)
                # уведомляем пользователя о проблеме
                try:
                    await safe_edit_message_text_or_caption(msg, "Ошибка: не удалось сгенерировать или отправить рамку.", reply_markup=_frame_menu_keyboard())
                except Exception:
                    try:
                        if getattr(msg, "photo", None):
                            await msg.edit_caption(caption="Ошибка: не удалось сгенерировать или отправить рамку.", reply_markup=_frame_menu_keyboard())
                        else:
                            await msg.edit_text("Ошибка: не удалось сгенерировать или отправить рамку.", reply_markup=_frame_menu_keyboard())
                    except Exception:
                        logger.exception("Не удалось уведомить пользователя об ошибке генерации рамки")
                # Очистка состояний
                context.user_data.pop("awaiting_frame_id", None)
                context.user_data.pop("awaiting_frame_slot", None)
                context.user_data.pop("frame_candidate_id", None)
                context.user_data.pop("frame_confirm_msg_id", None)
                return

    # После успешной отправки — показываем меню рамки (новым сообщением). Если не получилось — пытаемся редактировать текущее.
    try:
        await context.bot.send_message(chat_id=chat_id, text="Меню рамки:", reply_markup=_frame_menu_keyboard())
    except Exception:
        try:
            await safe_edit_message_text_or_caption(msg, "Готово — слот обновлён.", reply_markup=_frame_menu_keyboard())
        except Exception:
            try:
                if getattr(msg, "photo", None):
                    await msg.edit_caption(caption="Готово — слот обновлён.", reply_markup=_frame_menu_keyboard())
                else:
                    await msg.edit_text("Готово — слот обновлён.", reply_markup=_frame_menu_keyboard())
            except Exception:
                logger.exception("Не удалось отправить меню рамки после отправки фото")

    # Очистка временных состояний
    context.user_data.pop("awaiting_frame_id", None)
    context.user_data.pop("awaiting_frame_slot", None)
    context.user_data.pop("frame_candidate_id", None)
    context.user_data.pop("frame_confirm_msg_id", None)
    return



    await query.answer()

async def text_message_handler_for_frame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Защита: гарантируем, что context.user_data — словарь, а не None
    if context.user_data is None:
        context.user_data = {}

    if not context.user_data.get("awaiting_frame_id"):
        return

    user_id = update.effective_user.id
    text = update.message.text.strip() if update.message and update.message.text else ""
    chat_id = update.message.chat.id

    try:
        card_id = int(text)
    except Exception:
        await update.message.reply_text("Пожалуйста, укажи числовой ID карточки (например: 123). Попробуй ещё раз или нажми Отмена.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="frame_cancel_input")]]))
        return

    s_users = winter.sheet_winter_users()
    row, record = winter.find_winter_user_row(s_users, user_id)
    if record is None:
        await update.message.reply_text("Ты не зарегистрирован в таблице. Выполни /start.", reply_markup=_frame_menu_keyboard())
        context.user_data.pop("awaiting_frame_id", None)
        context.user_data.pop("awaiting_frame_slot", None)
        return

    owned_raw = record.get("W_CATS_ID") or record.get("W_CATS") or record.get("w_cats_id") or ""
    owned_tokens = [t.strip() for t in __import__("re").split(r"[|,;\\s]+", str(owned_raw)) if t.strip()]
    if str(card_id) not in owned_tokens:
        await update.message.reply_text("У тебя нет такой карточки. Убедись, что ID правильный и карточка есть у тебя. Ввод отменён.", reply_markup=_frame_menu_keyboard())
        context.user_data.pop("awaiting_frame_id", None)
        context.user_data.pop("awaiting_frame_slot", None)
        return

    slot_idx = context.user_data.get("awaiting_frame_slot")
    if slot_idx is None:
        await update.message.reply_text("Произошла ошибка: слот не выбран. Попробуй снова.", reply_markup=_frame_menu_keyboard())
        context.user_data.pop("awaiting_frame_id", None)
        return

    context.user_data["frame_candidate_id"] = card_id

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"frame_confirm_set:{slot_idx}:{card_id}")],
        [InlineKeyboardButton("❌ Отмена", callback_data="frame_cancel_input")],
    ])

    try:
        cats = winter.get_winter_cats_cached() or []
        cats_map = {str(c.get("id")): c for c in cats if c.get("id") is not None}
        rec = cats_map.get(str(card_id))
        url = _drive_direct_url(rec.get("url", "")) if rec else ""
        if url:
            sent = await update.message.reply_photo(
                photo=url,
                caption=f"Подтвердите установку карточки #{card_id} в слот #{slot_idx+1}:",
                reply_markup=kb
            )
            context.user_data["frame_confirm_msg_id"] = sent.message_id
            # запомним превью-фото, чтобы можно было удалить при выходе
            try:
                context.user_data["frame_last_photo"] = (sent.chat.id if hasattr(sent, "chat") else chat_id, sent.message_id)
            except Exception:
                context.user_data["frame_last_photo"] = (chat_id, sent.message_id)

        else:
            await update.message.reply_text(f"Подтвердите установку карточки #{card_id} в слот #{slot_idx+1}:", reply_markup=kb)
    except Exception:
        await update.message.reply_text(f"Подтвердите установку карточки #{card_id} в слот #{slot_idx+1}:", reply_markup=kb)

def register_frame_handlers(application):
    application.add_handler(CommandHandler("frame", cmd_frame))
    application.add_handler(CallbackQueryHandler(frame_callback_handler, pattern=r"^frame_"))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler_for_frame),
        group=10
    )
