# -*- coding: utf-8 -*-
"""
Legend HD Telegram Bot
Python 3.11+
aiogram 3 + Flask + Gunicorn + Render Webhook
Persistent SQLite FSM + Durable Webhook Queue
"""

# ==========================================================
# 1) IMPORTS
# ==========================================================

import os
import re
import io
import json
import html
import hmac
import hashlib
import time
import uuid
import copy
import shutil
import sqlite3
import asyncio
import logging
import threading
import difflib
import unicodedata
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import parse_qsl, quote
from functools import wraps

import requests

from flask import Flask, request, jsonify, abort, Response

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.types import (
    Message,
    CallbackQuery,
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InputMediaVideo,
    InputMediaDocument,
    WebAppInfo,
    MenuButtonWebApp,
)

# ==========================================================
# 2) CONFIGURATION
# ==========================================================

BOT_NAME = "Movies Films Zone"
BOT_USERNAME = "@Movies_Films_Zone_Bot"

ADMIN_ID = 7432609021

BOT_TOKEN = os.getenv("8884956189:AAEMB_8W2KGeGZm2nCzFIs8hPkS8y2YNYqk", "")

SUBSCRIPTION_CHANNELS = [
    {
        "chat_id": "@Legend_HD11",
        "url": "https://t.me/Legend_HD11",
        "name": "Legend HD 1",
    },
    {
        "chat_id": "@Legend_HD2",
        "url": "https://t.me/Legend_HD2",
        "name": "Legend HD 2",
    },
]

REQUIRE_SUBSCRIPTION = True

STORAGE_CHANNEL_NAME = "تخزين مسلسلات"
STORAGE_CHANNEL_URL = "https://t.me/+_ETOgizzmmBkMWRk"

_storage_id = os.getenv("-1006032545154", "").strip()
STORAGE_CHANNEL_ID = int(_storage_id) if _storage_id else None

RENDER_URL = os.getenv(
    "RENDER_EXTERNAL_URL",
    "https://legend-movies-bot.onrender.com"
).rstrip("/")

WEBHOOK_PATH = "/telegram/webhook"
WEBHOOK_URL = RENDER_URL + WEBHOOK_PATH
MINI_APP_PATH = "/miniapp"
MINI_APP_URL = RENDER_URL + MINI_APP_PATH

PORT = int(os.getenv("PORT", "10000"))

WEBHOOK_SECRET_TOKEN = os.getenv(
    "WEBHOOK_SECRET_TOKEN",
    ""
)

if not WEBHOOK_SECRET_TOKEN:
    WEBHOOK_SECRET_TOKEN = uuid.uuid4().hex + uuid.uuid4().hex

# ==========================================================
# 3) ADSGRAM MINI APP SDK
# ==========================================================

# ضع Block ID الجديد الخاص بإعلانات Interstitial في Render Environment.
# يمكن استخدام معرّف واحد أو معرّفين للتناوب بينهما.
ADSGRAM_INTERSTITIAL_BLOCK_ID_1 = os.getenv(
    "int-62282", ""
).strip()
ADSGRAM_INTERSTITIAL_BLOCK_ID_2 = os.getenv(
    "ADSGRAM_INTERSTITIAL_BLOCK_ID_2", ""
).strip()
ADSGRAM_INTERSTITIAL_BLOCK_IDS = [
    block_id for block_id in (
        ADSGRAM_INTERSTITIAL_BLOCK_ID_1,
        ADSGRAM_INTERSTITIAL_BLOCK_ID_2,
    ) if block_id
]
# Rewarded Ads - separate from Interstitial
ADSGRAM_REWARDED_BLOCK_ID = os.getenv(
    "reward-62283", ""
).strip()

ADSGRAM_REWARD_WEBHOOK_SECRET = os.getenv(
    "ADSGRAM_REWARD_WEBHOOK_SECRET", ""
).strip()

MINI_APP_INTERSTITIAL_INTERVAL = max(
    60, int(os.getenv("MINI_APP_INTERSTITIAL_INTERVAL", "180"))
)

ADS_ENABLED_DEFAULT = True

# ==========================================================
# 4) STORAGE
# ==========================================================

DATA_DIR = "data"
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
SQLITE_PATH = os.path.join(DATA_DIR, "legend.sqlite3")

PAGE_SIZE = 10
FUZZY_CUTOFF = 0.72
MAX_SEARCH_RESULTS = 10

DEFAULT_LANGUAGE = "ar"
LANGUAGES = ("ar", "en")

MAX_TITLE_LENGTH = 150
MAX_STORY_LENGTH = 2500
MAX_ALIAS_COUNT = 30

BACKUP_KEEP_COUNT = 15

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# ==========================================================
# 5) LOGGING
# ==========================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("legend-hd")

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN in Render Environment Variables.")

# ==========================================================
# 6) SQLITE DATABASE
# ==========================================================

DB_LOCK = threading.RLock()


def db_connect():
    connection = sqlite3.connect(
        SQLITE_PATH,
        timeout=30
    )
    connection.row_factory = sqlite3.Row
    return connection


def init_sqlite():
    with DB_LOCK:
        connection = db_connect()

        connection.execute("""
            CREATE TABLE IF NOT EXISTS fsm (
                key TEXT PRIMARY KEY,
                state TEXT,
                data TEXT NOT NULL DEFAULT '{}'
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS webhook_queue (
                update_id INTEGER PRIMARY KEY,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL
            )
        """)

        connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_webhook_status
            ON webhook_queue(status, created_at)
        """)
        connection.execute("""
            CREATE TABLE IF NOT EXISTS miniapp_user_access (
                user_id INTEGER PRIMARY KEY,
                free_used INTEGER NOT NULL DEFAULT 0
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS miniapp_reward_requests (
                request_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                item_id TEXT NOT NULL,
                number INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                verified_at REAL,
                used_at REAL
            )
        """)

        connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_miniapp_reward_user_status
            ON miniapp_reward_requests(user_id, status, created_at)
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS miniapp_reward_callback_guard (
                user_id INTEGER PRIMARY KEY,
                last_callback_at REAL NOT NULL
            )
        """)

        connection.commit()
        connection.close()


init_sqlite()

# ==========================================================
# 7) PERSISTENT FSM STORAGE
# ==========================================================

class SQLiteFSMStorage(BaseStorage):

    def _key(self, key: StorageKey):
        return ":".join([
            str(key.bot_id),
            str(key.chat_id),
            str(key.user_id),
            str(key.thread_id or 0),
            str(key.business_connection_id or ""),
            str(key.destiny or "default"),
        ])

    async def get_state(self, key: StorageKey):
        return await asyncio.to_thread(
            self._get_state,
            self._key(key)
        )

    def _get_state(self, key):
        with DB_LOCK:
            connection = db_connect()
            row = connection.execute(
                "SELECT state FROM fsm WHERE key=?",
                (key,)
            ).fetchone()
            connection.close()

        return row["state"] if row else None

    async def set_state(self, key: StorageKey, state=None):
        value = state.state if hasattr(state, "state") else state

        await asyncio.to_thread(
            self._set_state,
            self._key(key),
            value
        )

    def _set_state(self, key, value):
        with DB_LOCK:
            connection = db_connect()

            connection.execute("""
                INSERT INTO fsm(key, state, data)
                VALUES (?, ?, '{}')
                ON CONFLICT(key)
                DO UPDATE SET state=excluded.state
            """, (key, value))

            connection.commit()
            connection.close()

    async def get_data(self, key: StorageKey):
        return await asyncio.to_thread(
            self._get_data,
            self._key(key)
        )

    def _get_data(self, key):
        with DB_LOCK:
            connection = db_connect()

            row = connection.execute(
                "SELECT data FROM fsm WHERE key=?",
                (key,)
            ).fetchone()

            connection.close()

        if not row:
            return {}

        try:
            return json.loads(row["data"])
        except Exception:
            return {}

    async def set_data(self, key: StorageKey, data):
        await asyncio.to_thread(
            self._set_data,
            self._key(key),
            data
        )

    def _set_data(self, key, data):
        with DB_LOCK:
            connection = db_connect()

            connection.execute("""
                INSERT INTO fsm(key, state, data)
                VALUES (?, NULL, ?)
                ON CONFLICT(key)
                DO UPDATE SET data=excluded.data
            """, (
                key,
                json.dumps(data, ensure_ascii=False)
            ))

            connection.commit()
            connection.close()

    async def close(self):
        return None


# ==========================================================
# 8) TRANSLATIONS
# ==========================================================

TEXTS = {
    "ar": {
        "choose_lang": "اختر اللغة:",
        "welcome": "أهلاً بك في <b>Legend HD</b> 🎬\nابحث وتصفح مكتبة الأفلام والمسلسلات.",
        "home": "القائمة الرئيسية",
        "search": "🔎 البحث",
        "movies": "🎬 الأفلام",
        "series": "📺 المسلسلات",
        "categories": "📚 التصنيفات",
        "language": "🌐 تغيير اللغة",
        "about": "ℹ️ حول البوت",
        "sponsored": "📢 إعلان",
        "back": "⬅️ رجوع",
        "next": "التالي ➡️",
        "prev": "⬅️ السابق",
        "empty": "لا توجد نتائج حالياً.",
        "ask_search": "أرسل اسم الفيلم أو المسلسل للبحث.",
        "no_match": "لم أجد نتيجة مطابقة.",
        "join": "للاستخدام، اشترك أولاً في القنوات التالية ثم اضغط تحقق:",
        "verify": "✅ تحقق من الاشتراك",
        "not_joined": "لم يكتمل اشتراكك في القنوات التالية.",
        "joined": "تم التحقق من الاشتراك بنجاح.",
        "lang_saved": "تم تغيير اللغة.",
        "about_text": "<b>Legend HD</b>\nبوت لتصفح مكتبة الأفلام والمسلسلات.",
        "requesting": "جارٍ إرسال المحتوى...",
        "not_found": "المحتوى غير موجود أو لم يعد متاحاً.",
        "admin_only": "هذا الأمر مخصص للأدمن فقط.",
        "add_type": "اختر نوع المحتوى:",
        "movie": "فيلم",
        "series_type": "مسلسل",
        "title": "أرسل اسم المحتوى:",
        "category": "اختر التصنيف:",
        "video": "أرسل فيديو الجزء الأول أو الحلقة الأولى.",
        "part": "أرسل رقم الجزء:",
        "poster": "أرسل صورة البوستر أو اكتب تخطي.",
        "story": "أرسل القصة أو اكتب تخطي.",
        "aliases": "أرسل الأسماء البديلة مفصولة بعلامة | أو اكتب تخطي.",
        "confirm": "تأكيد",
        "cancel": "إلغاء",
        "skip": "تخطي",
        "saved": "تم حفظ المحتوى بنجاح.",
        "cancelled": "تم إلغاء العملية.",
        "bad_number": "أرسل رقماً صحيحاً أكبر من صفر.",
        "duplicate": "هذا الرقم موجود بالفعل.",
        "no_categories": "لا توجد تصنيفات.",
        "season": "الموسم",
        "episode": "الحلقة",
        "part_label": "الجزء",
        "choose_season": "اختر الموسم:",
        "send_failed": "تعذر إرسال المحتوى.",
        "category_added": "تمت إضافة التصنيف.",
        "category_exists": "هذا التصنيف موجود بالفعل.",
        "category_deleted": "تم حذف التصنيف.",
        "category_delete_used": "لا يمكن حذف تصنيف مرتبط بمحتوى.",
        "storage_origin": "يجب تحويل الفيديو من قناة التخزين المحددة.",
        "ad_unavailable": "لا يوجد إعلان متاح حالياً.",
        "ad_error": "تعذر عرض الإعلان حالياً.",
        "ad_button": "فتح الإعلان",
        "reward_button": "الحصول على المكافأة",
        "admin_panel": "🛠 لوحة الإدارة",
        "manage_movies": "🎬 إدارة الأفلام",
        "manage_series": "📺 إدارة المسلسلات",
        "manage_categories": "📚 إدارة التصنيفات",
        "settings": "⚙️ الإعدادات",
        "statistics": "📊 الإحصائيات",
        "backup": "💾 نسخة احتياطية",
        "restore": "📥 استعادة نسخة",
        "users": "👥 المستخدمون",
        "ads_on": "الإعلانات: مفعلة",
        "ads_off": "الإعلانات: متوقفة",
        "sub_on": "الاشتراك: مفعل",
        "sub_off": "الاشتراك: متوقف",
        "delete": "🗑 حذف",
        "edit": "✏️ تعديل",
        "choose_action": "اختر العملية:",
        "send_new_value": "أرسل القيمة الجديدة:",
        "deleted": "تم الحذف.",
        "updated": "تم التعديل.",
        "stats": "📊 إحصائيات Legend HD",
        "blocked": "هذا الحساب غير متاح حالياً.",
        "unblock": "إلغاء الحظر",
        "block": "حظر المستخدم",
        "backup_done": "تم إنشاء النسخة الاحتياطية.",
        "restore_done": "تمت استعادة النسخة الاحتياطية.",
        "restore_help": "أرسل ملف ZIP الناتج من النسخ الاحتياطي.",
        "search_admin": "أرسل جزءاً من اسم المحتوى للبحث عنه.",
        "choose_season": "اختر الموسم:",
        "no_episodes": "لا توجد حلقات في هذا الموسم.",
        "episode_added": "تمت إضافة الحلقة {number}.",
        "done_episodes": "تم إنهاء إضافة الحلقات.",
        "admin_help": "استخدم /manage لفتح لوحة الإدارة.",
    },

    "en": {
        "choose_lang": "Choose your language:",
        "welcome": "Welcome to <b>Legend HD</b> 🎬\nBrowse and search movies and series.",
        "home": "Main menu",
        "search": "🔎 Search",
        "movies": "🎬 Movies",
        "series": "📺 Series",
        "categories": "📚 Categories",
        "language": "🌐 Change language",
        "about": "ℹ️ About",
        "sponsored": "📢 Advertisement",
        "back": "⬅️ Back",
        "next": "Next ➡️",
        "prev": "⬅️ Previous",
        "empty": "No content available.",
        "ask_search": "Send a movie or series title.",
        "no_match": "No matching title found.",
        "join": "Please join the following channels, then verify:",
        "verify": "✅ Verify subscription",
        "not_joined": "You have not joined all required channels.",
        "joined": "Subscription verified.",
        "lang_saved": "Language changed.",
        "about_text": "<b>Legend HD</b>\nA movie and series library bot.",
        "requesting": "Sending content...",
        "not_found": "Content not found.",
        "admin_only": "Administrator only.",
        "add_type": "Choose content type:",
        "movie": "Movie",
        "series_type": "Series",
        "title": "Send title:",
        "category": "Choose category:",
        "video": "Send the first video.",
        "part": "Send part number:",
        "poster": "Send poster or Skip.",
        "story": "Send synopsis or Skip.",
        "aliases": "Send aliases separated by | or Skip.",
        "confirm": "Confirm",
        "cancel": "Cancel",
        "skip": "Skip",
        "saved": "Content saved.",
        "cancelled": "Cancelled.",
        "bad_number": "Send a positive number.",
        "duplicate": "This number already exists.",
        "no_categories": "No categories.",
        "season": "Season",
        "episode": "Episode",
        "part_label": "Part",
        "choose_season": "Choose season:",
        "send_failed": "Could not send content.",
        "category_added": "Category added.",
        "category_exists": "Category already exists.",
        "category_deleted": "Category deleted.",
        "category_delete_used": "Category is in use.",
        "storage_origin": "Forward the video from the configured storage channel.",
        "ad_unavailable": "No advertisement available.",
        "ad_error": "Could not display advertisement.",
        "ad_button": "Open advertisement",
        "reward_button": "Claim reward",
        "admin_panel": "Admin panel",
        "manage_movies": "Manage movies",
        "manage_series": "Manage series",
        "manage_categories": "Manage categories",
        "settings": "Settings",
        "statistics": "Statistics",
        "backup": "Backup",
        "restore": "Restore",
        "users": "Users",
        "ads_on": "Ads: ON",
        "ads_off": "Ads: OFF",
        "sub_on": "Subscription: ON",
        "sub_off": "Subscription: OFF",
        "delete": "Delete",
        "edit": "Edit",
        "choose_action": "Choose action:",
        "send_new_value": "Send the new value:",
        "deleted": "Deleted.",
        "updated": "Updated.",
        "stats": "Legend HD statistics",
        "blocked": "This account is unavailable.",
        "unblock": "Unblock user",
        "block": "Block user",
        "backup_done": "Backup created.",
        "restore_done": "Backup restored.",
        "restore_help": "Send the ZIP backup file.",
        "search_admin": "Send part of the content title.",
        "no_episodes": "No episodes in this season.",
        "episode_added": "Episode {number} added.",
        "done_episodes": "Episode adding finished.",
        "admin_help": "Use /manage to open the admin panel.",
    }
}


def tr(lang, key, **kwargs):
    lang = lang if lang in TEXTS else DEFAULT_LANGUAGE
    value = TEXTS[lang].get(
        key,
        TEXTS["ar"].get(key, key)
    )
    return value.format(**kwargs)


# ==========================================================
# 9) JSON DATABASE
# ==========================================================

STORE_LOCK = threading.RLock()

FILES = {
    "movies": os.path.join(DATA_DIR, "movies.json"),
    "series": os.path.join(DATA_DIR, "series.json"),
    "categories": os.path.join(DATA_DIR, "categories.json"),
    "users": os.path.join(DATA_DIR, "users.json"),
    "state": os.path.join(DATA_DIR, "state.json"),
}

DEFAULT_CATEGORIES = [
    {"id": "action", "name": {"ar": "أكشن", "en": "Action"}},
    {"id": "adventure", "name": {"ar": "مغامرة", "en": "Adventure"}},
    {"id": "comedy", "name": {"ar": "كوميديا", "en": "Comedy"}},
    {"id": "drama", "name": {"ar": "دراما", "en": "Drama"}},
    {"id": "horror", "name": {"ar": "رعب", "en": "Horror"}},
    {"id": "scifi", "name": {"ar": "خيال علمي", "en": "Sci-Fi"}},
    {"id": "thriller", "name": {"ar": "إثارة", "en": "Thriller"}},
    {"id": "animation", "name": {"ar": "أنيميشن", "en": "Animation"}},
    {"id": "romance", "name": {"ar": "رومانسية", "en": "Romance"}},
]


def cleanup_backups(filename):
    backups = sorted(
        Path(BACKUP_DIR).glob(filename + ".*.bak"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

    for old in backups[BACKUP_KEEP_COUNT:]:
        try:
            old.unlink()
        except OSError:
            log.warning("Could not delete old backup: %s", old)


def atomic_json(path, data):
    temp_path = path + ".tmp"

    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2
        )
        file.flush()
        os.fsync(file.fileno())

    if os.path.exists(path):
        stamp = datetime.now(timezone.utc).strftime(
            "%Y%m%d%H%M%S%f"
        )

        backup_name = (
            os.path.basename(path)
            + "."
            + stamp
            + ".bak"
        )

        backup_path = os.path.join(
            BACKUP_DIR,
            backup_name
        )

        try:
            shutil.copy2(path, backup_path)
            cleanup_backups(os.path.basename(path))
        except OSError:
            log.exception("Backup creation failed.")

    os.replace(temp_path, path)


def load_json(path, default):
    if not os.path.exists(path):
        atomic_json(path, default)
        return copy.deepcopy(default)

    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)

        if not isinstance(data, type(default)):
            raise ValueError("Invalid JSON structure")

        return data

    except Exception:
        log.exception("Could not read database: %s", path)

        corrupted = path + ".corrupted"

        try:
            shutil.copy2(path, corrupted)
        except OSError:
            pass

        return copy.deepcopy(default)


with STORE_LOCK:
    MOVIES = load_json(FILES["movies"], {})
    SERIES = load_json(FILES["series"], {})
    CATEGORIES = load_json(FILES["categories"], DEFAULT_CATEGORIES)
    USERS = load_json(FILES["users"], {})

    STATE = load_json(
        FILES["state"],
        {
            "search_wait": {},
            "episode_targets": {},
            "settings": {
                "ads_enabled": ADS_ENABLED_DEFAULT,
                "require_subscription": True,
            },
        }
    )


STATE.setdefault("search_wait", {})
STATE.setdefault("episode_targets", {})
STATE.setdefault("settings", {})
STATE["settings"].setdefault("ads_enabled", ADS_ENABLED_DEFAULT)
STATE["settings"].setdefault("require_subscription", True)


def save(name):
    with STORE_LOCK:
        data = {
            "movies": MOVIES,
            "series": SERIES,
            "categories": CATEGORIES,
            "users": USERS,
            "state": STATE,
        }[name]

        atomic_json(FILES[name], data)


def user_record(user_id):
    key = str(user_id)

    with STORE_LOCK:
        if key not in USERS:
            USERS[key] = {
                "language": DEFAULT_LANGUAGE,
                "language_selected": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "blocked": False,
            }

            save("users")

        USERS[key].setdefault("blocked", False)
        USERS[key].setdefault("language", DEFAULT_LANGUAGE)

        return USERS[key]


def category_name(category_id, lang):
    for category in CATEGORIES:
        if category.get("id") == category_id:
            names = category.get("name", {})
            return (
                names.get(lang)
                or names.get("ar")
                or category_id
            )

    return category_id


def settings():
    return STATE["settings"]


# ==========================================================
# 10) SEARCH
# ==========================================================

ARABIC_MAP = str.maketrans({
    "أ": "ا",
    "إ": "ا",
    "آ": "ا",
    "ى": "ي",
    "ة": "ه",
    "ؤ": "و",
    "ئ": "ي",
})


def normalize(value):
    value = unicodedata.normalize(
        "NFKC",
        str(value or "")
    ).translate(ARABIC_MAP).casefold()

    value = re.sub(r"[\u064b-\u065f\u0670]", "", value)

    return re.sub(
        r"[\W_]+",
        "",
        value,
        flags=re.UNICODE
    )


def title_score(query, title, aliases):
    q = normalize(query)
    best = 0

    for candidate in [title] + list(aliases or []):
        c = normalize(candidate)

        if not q or not c:
            continue

        score = difflib.SequenceMatcher(
            None,
            q,
            c
        ).ratio()

        if q in c or c in q:
            score = max(score, 0.85)

        best = max(best, score)

    return best


def search_content(query):
    results = []

    for kind, collection in (
        ("movie", MOVIES),
        ("series", SERIES),
    ):
        for item_id, item in collection.items():
            score = title_score(
                query,
                item.get("title", ""),
                item.get("aliases", [])
            )

            if score >= FUZZY_CUTOFF:
                results.append((
                    score,
                    kind,
                    item_id,
                    item
                ))

    results.sort(key=lambda x: x[0], reverse=True)

    return results[:MAX_SEARCH_RESULTS]


# ==========================================================
# 11) BOT AND FSM
# ==========================================================

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(
        parse_mode=ParseMode.HTML
    )
)

dp = Dispatcher(storage=SQLiteFSMStorage())
router = Router()
dp.include_router(router)


class AddFlow(StatesGroup):
    kind = State()
    title = State()
    category = State()
    video = State()
    number = State()
    poster = State()
    story = State()
    aliases = State()
    review = State()


class EditFlow(StatesGroup):
    value = State()


class CategoryFlow(StatesGroup):
    name = State()


# ==========================================================
# 12) KEYBOARDS
# ==========================================================

def main_keyboard(lang):
    app_button_text = "🎬 افتح تطبيق Legend HD" if lang == "ar" else "🎬 Open Legend HD"
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=app_button_text)
            ],
            [
                KeyboardButton(text=tr(lang, "search")),
                KeyboardButton(text=tr(lang, "movies")),
            ],
            [
                KeyboardButton(text=tr(lang, "series")),
                KeyboardButton(text=tr(lang, "categories")),
            ],
            [
                KeyboardButton(text=tr(lang, "language")),
                KeyboardButton(text=tr(lang, "about")),
            ],
        ],
        resize_keyboard=True
    )

def lang_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="العربية 🇸🇩",
                    callback_data="lang:ar"
                ),
                InlineKeyboardButton(
                    text="English 🇬🇧",
                    callback_data="lang:en"
                ),
            ]
        ]
    )


def join_keyboard(lang, missing):
    rows = []

    for channel in missing:
        rows.append([
            InlineKeyboardButton(
                text=channel["name"],
                url=channel["url"]
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text=tr(lang, "verify"),
            callback_data="verify_join"
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


def categories_keyboard(lang, prefix="browsecat"):
    rows = []

    for category in CATEGORIES:
        rows.append([
            InlineKeyboardButton(
                text=category_name(category["id"], lang),
                callback_data=f"{prefix}:{category['id']}"
            )
        ])

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


# ==========================================================
# 13) ADMIN CHECK
# ==========================================================

def admin_only(func):
    @wraps(func)
    async def wrapped(message: Message, *args, **kwargs):
        if not message.from_user or message.from_user.id != ADMIN_ID:
            await message.answer(tr("ar", "admin_only"))
            return

        return await func(message, *args, **kwargs)

    return wrapped


def is_admin(user_id):
    return int(user_id) == ADMIN_ID


# ==========================================================
# 14) SUBSCRIPTION
# ==========================================================

async def missing_channels(user_id):
    missing = []

    for channel in SUBSCRIPTION_CHANNELS:
        try:
            member = await bot.get_chat_member(
                channel["chat_id"],
                user_id
            )

            if member.status in (
                ChatMemberStatus.LEFT,
                ChatMemberStatus.KICKED
            ):
                missing.append(channel)

            elif member.status == ChatMemberStatus.RESTRICTED:
                if not getattr(member, "is_member", False):
                    missing.append(channel)

        except TelegramAPIError:
            log.exception(
                "Subscription check failed for %s",
                channel["chat_id"]
            )
            missing.append(channel)

    return missing


async def gate(message):
    if not settings().get("require_subscription", True):
        return True

    if message.from_user and is_admin(message.from_user.id):
        return True

    if not message.from_user:
        return False

    record = user_record(message.from_user.id)

    if record.get("blocked"):
        await message.answer(tr(record.get("language", "ar"), "blocked"))
        return False

    lang = record.get("language", DEFAULT_LANGUAGE)

    missing = await missing_channels(message.from_user.id)

    if missing:
        await message.answer(
            tr(lang, "join"),
            reply_markup=join_keyboard(lang, missing)
        )
        return False

    return True


async def gate_callback(call):
    if not settings().get("require_subscription", True):
        return True

    if is_admin(call.from_user.id):
        return True

    record = user_record(call.from_user.id)
    lang = record.get("language", DEFAULT_LANGUAGE)

    if record.get("blocked"):
        await call.answer(
            tr(lang, "blocked"),
            show_alert=True
        )
        return False

    missing = await missing_channels(call.from_user.id)

    if missing:
        await call.answer(
            tr(lang, "not_joined"),
            show_alert=True
        )
        return False

    return True


# ==========================================================
# 15) CONTENT LISTS
# ==========================================================

def content_list(kind, page, lang, category="all"):
    collection = MOVIES if kind == "movie" else SERIES

    items = [
        (item_id, item)
        for item_id, item in collection.items()
        if category == "all" or item.get("category") == category
    ]

    items.sort(
        key=lambda x: x[1].get("title", "").casefold()
    )

    total_pages = max(
        1,
        (len(items) + PAGE_SIZE - 1) // PAGE_SIZE
    )

    page = max(0, min(page, total_pages - 1))

    chunk = items[
        page * PAGE_SIZE:
        (page + 1) * PAGE_SIZE
    ]

    rows = []

    for item_id, item in chunk:
        rows.append([
            InlineKeyboardButton(
                text=item.get("title", "Untitled")[:60],
                callback_data=f"view:{kind}:{item_id}"
            )
        ])

    navigation = []

    if page > 0:
        navigation.append(
            InlineKeyboardButton(
                text=tr(lang, "prev"),
                callback_data=f"page:{kind}:{page-1}:{category}"
            )
        )

    if page + 1 < total_pages:
        navigation.append(
            InlineKeyboardButton(
                text=tr(lang, "next"),
                callback_data=f"page:{kind}:{page+1}:{category}"
            )
        )

    if navigation:
        rows.append(navigation)

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


# ==========================================================
# 16) VIDEO HELPERS
# ==========================================================

def get_video_id(message):
    if message.video:
        video = message.video

        return (
            video.file_id,
            "video",
            {
                "duration": video.duration,
                "file_size": video.file_size,
                "mime_type": video.mime_type,
            }
        )

    if (
        message.document
        and (message.document.mime_type or "").startswith("video/")
    ):
        document = message.document

        return (
            document.file_id,
            "document",
            {
                "file_size": document.file_size,
                "mime_type": document.mime_type,
            }
        )

    return None, None, None


async def send_stored_file(chat_id, file_id, file_type, caption):
    if file_type == "document":
        return await bot.send_document(
            chat_id,
            file_id,
            caption=caption[:1024],
            protect_content=True
        )

    return await bot.send_video(
        chat_id,
        file_id,
        caption=caption[:1024],
        protect_content=True
    )


# ==========================================================
# 17) ADSGRAM MINI APP ENTRY
# ==========================================================

async def show_sponsored_ad(message: Message):
    """AdsGram Interstitial ads are shown inside the Mini App, not in chat."""
    if not message.from_user:
        return

    lang = user_record(message.from_user.id).get("language", DEFAULT_LANGUAGE)
    text = (
        "الإعلانات تظهر داخل تطبيق Legend HD. افتح التطبيق من الزر أدناه."
        if lang == "ar"
        else "Ads are displayed inside the Legend HD Mini App. Open the app below."
    )
    await message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="🎬 فتح التطبيق" if lang == "ar" else "🎬 Open app",
                web_app=WebAppInfo(url=MINI_APP_URL)
            )
        ]])
    )


@router.message(Command("sponsored"))
async def sponsored_command(message: Message):
    await show_sponsored_ad(message)


# ==========================================================
# 18) START AND LANGUAGE
# ==========================================================

@router.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()

    if not message.from_user:
        return

    record = user_record(message.from_user.id)

    if record.get("blocked"):
        await message.answer(tr(record.get("language", "ar"), "blocked"))
        return

    if not record.get("language_selected"):
        await message.answer(
            tr(DEFAULT_LANGUAGE, "choose_lang"),
            reply_markup=lang_keyboard()
        )
        return

    if not await gate(message):
        return

    lang = record.get("language", DEFAULT_LANGUAGE)

    await message.answer(
        tr(lang, "welcome"),
        reply_markup=main_keyboard(lang)
    )


@router.callback_query(F.data.startswith("lang:"))
async def set_language(call: CallbackQuery):
    lang = call.data.split(":", 1)[1]

    if lang not in LANGUAGES:
        await call.answer()
        return

    record = user_record(call.from_user.id)

    record["language"] = lang
    record["language_selected"] = True

    save("users")

    await call.answer(tr(lang, "lang_saved"))

    await call.message.answer(
        tr(lang, "welcome"),
        reply_markup=main_keyboard(lang)
    )


@router.callback_query(F.data == "verify_join")
async def verify_join(call: CallbackQuery):
    missing = await missing_channels(call.from_user.id)

    lang = user_record(call.from_user.id).get(
        "language",
        DEFAULT_LANGUAGE
    )

    if missing:
        await call.answer(
            tr(lang, "not_joined"),
            show_alert=True
        )

        try:
            await call.message.edit_reply_markup(
                reply_markup=join_keyboard(lang, missing)
            )
        except TelegramAPIError:
            pass

    else:
        await call.answer(
            tr(lang, "joined"),
            show_alert=True
        )

        await call.message.answer(
            tr(lang, "welcome"),
            reply_markup=main_keyboard(lang)
        )


# ==========================================================
# 19) CATEGORY BROWSING
# ==========================================================

@router.callback_query(F.data.startswith("browsecat:"))
async def browse_category(call: CallbackQuery):
    if not await gate_callback(call):
        return

    category_id = call.data.split(":", 1)[1]

    lang = user_record(call.from_user.id).get(
        "language",
        DEFAULT_LANGUAGE
    )

    rows = [
        [
            InlineKeyboardButton(
                text=tr(lang, "movies"),
                callback_data=f"page:movie:0:{category_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text=tr(lang, "series"),
                callback_data=f"page:series:0:{category_id}"
            )
        ],
    ]

    await call.message.edit_text(
        category_name(category_id, lang),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=rows
        )
    )

    await call.answer()


@router.callback_query(F.data.startswith("page:"))
async def page_callback(call: CallbackQuery):
    if not await gate_callback(call):
        return

    _, kind, page, category = call.data.split(":")

    if kind not in ("movie", "series"):
        await call.answer()
        return

    lang = user_record(call.from_user.id).get(
        "language",
        DEFAULT_LANGUAGE
    )

    keyboard = content_list(
        kind,
        int(page),
        lang,
        category
    )

    try:
        await call.message.edit_reply_markup(
            reply_markup=keyboard
        )
    except TelegramAPIError:
        await call.message.answer(
            tr(lang, "empty"),
            reply_markup=keyboard
        )

    await call.answer()


# ==========================================================
# 20) VIEW CONTENT
# ==========================================================

@router.callback_query(F.data.startswith("view:"))
async def view_content(call: CallbackQuery):
    if not await gate_callback(call):
        return

    _, kind, item_id = call.data.split(":")

    if kind not in ("movie", "series"):
        await call.answer()
        return

    collection = MOVIES if kind == "movie" else SERIES
    item = collection.get(item_id)

    lang = user_record(call.from_user.id).get(
        "language",
        DEFAULT_LANGUAGE
    )

    if not item:
        await call.answer(
            tr(lang, "not_found"),
            show_alert=True
        )
        return

    title = html.escape(item.get("title", ""))
    category = html.escape(
        category_name(item.get("category", ""), lang)
    )
    story = html.escape(item.get("story", ""))

    caption = f"<b>{title}</b>\n{category}\n\n{story}"

    rows = []

    if kind == "movie":
        for part in sorted(
            item.get("parts", []),
            key=lambda x: int(x.get("number", 0))
        ):
            rows.append([
                InlineKeyboardButton(
                    text=f"{tr(lang, 'part_label')} {part['number']}",
                    callback_data=f"getpart:{item_id}:{part['number']}"
                )
            ])

    else:
        for season in sorted(
            item.get("seasons", []),
            key=lambda x: int(x.get("number", 0))
        ):
            rows.append([
                InlineKeyboardButton(
                    text=f"{tr(lang, 'season')} {season['number']}",
                    callback_data=f"season:{item_id}:{season['number']}"
                )
            ])

    keyboard = (
        InlineKeyboardMarkup(inline_keyboard=rows)
        if rows else None
    )

    try:
        # Send poster separately so Telegram caption limits never split HTML tags.
        if item.get("poster"):
            try:
                await call.message.answer_photo(
                    item["poster"],
                    protect_content=True
                )
            except TelegramAPIError:
                log.warning("Poster could not be sent for item %s", item_id)

        await call.message.answer(
            caption[:4096],
            reply_markup=keyboard,
            protect_content=True
        )

    except TelegramAPIError:
        log.exception("Could not display content.")
        await call.message.answer(
            html.escape(item.get("title", ""))[:4096],
            reply_markup=keyboard
        )

    await call.answer()


# ==========================================================
# 21) MOVIE DELIVERY
# ==========================================================

async def send_movie_part(chat_id, user_id, item, part_number):
    lang = user_record(user_id).get(
        "language",
        DEFAULT_LANGUAGE
    )

    part = next(
        (
            p for p in item.get("parts", [])
            if int(p.get("number", 0)) == part_number
        ),
        None
    )

    if not part:
        await bot.send_message(
            chat_id,
            tr(lang, "not_found")
        )
        return

    await bot.send_message(
        chat_id,
        tr(lang, "requesting")
    )

    try:
        await send_stored_file(
            chat_id,
            part["file_id"],
            part.get("file_type", "video"),
            (
                f"{item.get('title', '')} — "
                f"{tr(lang, 'part_label')} {part_number}"
            )
        )

    except (TelegramAPIError, KeyError):
        log.exception("Movie delivery failed.")
        await bot.send_message(
            chat_id,
            tr(lang, "send_failed")
        )


@router.callback_query(F.data.startswith("getpart:"))
async def get_part(call: CallbackQuery):
    if not await gate_callback(call):
        return

    _, item_id, number = call.data.split(":")

    item = MOVIES.get(item_id)

    if not item:
        await call.answer()
        return

    await call.answer()

    await send_movie_part(
        call.message.chat.id,
        call.from_user.id,
        item,
        int(number)
    )


# ==========================================================
# 22) SERIES DELIVERY - MEDIA GROUPS
# ==========================================================

BACKGROUND_TASKS = set()


def keep_task(task):
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)


async def send_season(chat_id, user_id, item, season_number):
    lang = user_record(user_id).get(
        "language",
        DEFAULT_LANGUAGE
    )

    season = next(
        (
            s for s in item.get("seasons", [])
            if int(s.get("number", 0)) == season_number
        ),
        None
    )

    if not season:
        await bot.send_message(
            chat_id,
            tr(lang, "not_found")
        )
        return

    episodes = sorted(
        season.get("episodes", []),
        key=lambda x: int(x.get("number", 0))
    )

    if not episodes:
        await bot.send_message(
            chat_id,
            tr(lang, "no_episodes")
        )
        return

    await bot.send_message(
        chat_id,
        tr(lang, "requesting")
    )

    # Telegram يسمح بحد أقصى 10 عناصر في Media Group.
    for start in range(0, len(episodes), 10):
        batch = episodes[start:start + 10]
        media = []

        for episode in batch:
            number = int(episode.get("number", 0))

            caption = (
                f"{item.get('title', '')} — "
                f"S{season_number:02d}E{number:02d}"
            )

            if episode.get("file_type") == "document":
                media.append(
                    InputMediaDocument(
                        media=episode["file_id"],
                        caption=caption[:1024]
                    )
                )
            else:
                media.append(
                    InputMediaVideo(
                        media=episode["file_id"],
                        caption=caption[:1024]
                    )
                )

        try:
            if len(media) == 1:
                episode = batch[0]

                await send_stored_file(
                    chat_id,
                    episode["file_id"],
                    episode.get("file_type", "video"),
                    (
                        f"{item.get('title', '')} — "
                        f"S{season_number:02d}"
                        f"E{int(episode['number']):02d}"
                    )
                )
            else:
                await bot.send_media_group(
                    chat_id,
                    media=media,
                    protect_content=True
                )

        except TelegramAPIError:
            log.exception(
                "Media group failed for season %s",
                season_number
            )

            # محاولة إرسال العناصر بشكل منفرد إذا فشلت المجموعة.
            for episode in batch:
                try:
                    await send_stored_file(
                        chat_id,
                        episode["file_id"],
                        episode.get("file_type", "video"),
                        (
                            f"{item.get('title', '')} — "
                            f"S{season_number:02d}"
                            f"E{int(episode['number']):02d}"
                        )
                    )
                except TelegramAPIError:
                    log.exception("Episode failed.")

        if start + 10 < len(episodes):
            await asyncio.sleep(0.7)


@router.callback_query(F.data.startswith("season:"))
async def get_season(call: CallbackQuery):
    if not await gate_callback(call):
        return

    _, item_id, number = call.data.split(":")

    item = SERIES.get(item_id)

    if not item:
        await call.answer()
        return

    await call.answer()

    task = asyncio.create_task(
        send_season(
            call.message.chat.id,
            call.from_user.id,
            copy.deepcopy(item),
            int(number)
        )
    )

    keep_task(task)


# ==========================================================
# 23) ADMIN ADD FLOW
# ==========================================================

def admin_kind_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=tr("ar", "movie"),
                    callback_data="addkind:movie"
                ),
                InlineKeyboardButton(
                    text=tr("ar", "series_type"),
                    callback_data="addkind:series"
                ),
            ],
            [
                InlineKeyboardButton(
                    text=tr("ar", "cancel"),
                    callback_data="addcancel"
                )
            ],
        ]
    )


@router.message(Command("add"))
@admin_only
async def add_command(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AddFlow.kind)

    await message.answer(
        tr("ar", "add_type"),
        reply_markup=admin_kind_keyboard()
    )


@router.callback_query(F.data.startswith("addkind:"))
async def add_kind(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True)
        return

    kind = call.data.split(":", 1)[1]

    if kind not in ("movie", "series"):
        await call.answer()
        return

    await state.update_data(
        kind=kind,
        aliases=[],
        poster=None,
        story=""
    )

    await state.set_state(AddFlow.title)

    await call.message.answer(tr("ar", "title"))
    await call.answer()


@router.message(AddFlow.title)
async def add_title(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    title = (message.text or "").strip()

    if not title or len(title) > MAX_TITLE_LENGTH:
        await message.answer("اسم غير صالح أو طويل جداً.")
        return

    await state.update_data(title=title)

    await state.set_state(AddFlow.category)

    await message.answer(
        tr("ar", "category"),
        reply_markup=categories_keyboard("ar", "addcat")
    )


@router.callback_query(F.data.startswith("addcat:"))
async def add_category_choice(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True)
        return

    category_id = call.data.split(":", 1)[1]

    if not any(c["id"] == category_id for c in CATEGORIES):
        await call.answer("Invalid category", show_alert=True)
        return

    await state.update_data(category=category_id)
    await state.set_state(AddFlow.video)

    await call.message.answer(tr("ar", "video"))
    await call.answer()


@router.message(AddFlow.video)
async def add_video(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    file_id, file_type, meta = get_video_id(message)

    if not file_id:
        await message.answer(
            "لم أتعرف على الفيديو. أرسل فيديو صالحاً."
        )
        return

    origin = message.forward_origin

    if STORAGE_CHANNEL_ID is not None:
        if not origin or not getattr(origin, "chat", None):
            await message.answer(tr("ar", "storage_origin"))
            return

        if origin.chat.id != int(STORAGE_CHANNEL_ID):
            await message.answer(tr("ar", "storage_origin"))
            return

    await state.update_data(
        file_id=file_id,
        file_type=file_type,
        file_meta=meta
    )

    data = await state.get_data()

    if data["kind"] == "movie":
        await state.set_state(AddFlow.number)
        await message.answer(tr("ar", "part"))
    else:
        await state.update_data(number=1)
        await state.set_state(AddFlow.poster)
        await message.answer(tr("ar", "poster"))


@router.message(AddFlow.number)
async def add_movie_number(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    try:
        number = int((message.text or "").strip())

        if number <= 0:
            raise ValueError

    except ValueError:
        await message.answer(tr("ar", "bad_number"))
        return

    await state.update_data(number=number)
    await state.set_state(AddFlow.poster)

    await message.answer(tr("ar", "poster"))


@router.message(AddFlow.poster)
async def add_poster(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    poster = message.photo[-1].file_id if message.photo else None

    if not poster:
        text = (message.text or "").strip().lower()

        if text not in ("تخطي", "skip"):
            await message.answer(tr("ar", "poster"))
            return

    await state.update_data(poster=poster)
    await state.set_state(AddFlow.story)

    await message.answer(tr("ar", "story"))


@router.message(AddFlow.story)
async def add_story(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    story = (message.text or "").strip()

    if story.lower() in ("تخطي", "skip"):
        story = ""

    if len(story) > MAX_STORY_LENGTH:
        await message.answer("القصة طويلة جداً.")
        return

    await state.update_data(story=story)
    await state.set_state(AddFlow.aliases)

    await message.answer(tr("ar", "aliases"))


@router.message(AddFlow.aliases)
async def add_aliases(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    raw = (message.text or "").strip()

    aliases = (
        []
        if raw.lower() in ("تخطي", "skip")
        else [
            x.strip()
            for x in raw.split("|")
            if x.strip()
        ]
    )

    if len(aliases) > MAX_ALIAS_COUNT:
        await message.answer("عدد الأسماء البديلة كبير جداً.")
        return

    await state.update_data(aliases=aliases)

    data = await state.get_data()

    review = (
        "<b>مراجعة الإضافة</b>\n"
        f"الاسم: {html.escape(data['title'])}\n"
        f"النوع: {html.escape(data['kind'])}\n"
        f"التصنيف: {html.escape(category_name(data['category'], 'ar'))}\n"
        f"الرقم: {data.get('number', 1)}\n"
        f"الأسماء البديلة: {html.escape(', '.join(aliases) or '-')}\n\n"
        "هل تؤكد الإضافة؟"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=tr("ar", "confirm"),
                    callback_data="addconfirm"
                ),
                InlineKeyboardButton(
                    text=tr("ar", "cancel"),
                    callback_data="addcancel"
                ),
            ]
        ]
    )

    await state.set_state(AddFlow.review)

    await message.answer(
        review,
        reply_markup=keyboard
    )


@router.callback_query(F.data == "addconfirm")
async def add_confirm(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True)
        return

    data = await state.get_data()

    if not data.get("file_id") or not data.get("title"):
        await call.answer("Missing data", show_alert=True)
        return

    kind = data["kind"]
    title = data["title"]

    collection = MOVIES if kind == "movie" else SERIES

    with STORE_LOCK:
        existing = next(
            (
                (item_id, item)
                for item_id, item in collection.items()
                if normalize(item.get("title", "")) == normalize(title)
            ),
            None
        )

        if existing:
            item_id, item = existing
        else:
            item_id = uuid.uuid4().hex[:12]

            item = {
                "id": item_id,
                "title": title,
                "category": data["category"],
                "poster": data.get("poster"),
                "story": data.get("story", ""),
                "aliases": data.get("aliases", []),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            if kind == "movie":
                item["parts"] = []
            else:
                item["seasons"] = []

            collection[item_id] = item

        if kind == "movie":
            number = int(data.get("number", 1))

            if any(
                int(p.get("number", 0)) == number
                for p in item["parts"]
            ):
                await call.answer(
                    tr("ar", "duplicate"),
                    show_alert=True
                )
                return

            item["parts"].append({
                "number": number,
                "file_id": data["file_id"],
                "file_type": data["file_type"],
                "meta": data.get("file_meta", {}),
            })

            item["parts"].sort(
                key=lambda x: int(x["number"])
            )

        else:
            # أول فيديو عند إضافة مسلسل يصبح الحلقة الأولى من الموسم الأول.
            season = next(
                (
                    s for s in item["seasons"]
                    if int(s["number"]) == 1
                ),
                None
            )

            if not season:
                season = {
                    "number": 1,
                    "episodes": [],
                }
                item["seasons"].append(season)

            episode_number = max(
                [
                    int(e["number"])
                    for e in season["episodes"]
                ],
                default=0
            ) + 1

            season["episodes"].append({
                "number": episode_number,
                "file_id": data["file_id"],
                "file_type": data["file_type"],
                "meta": data.get("file_meta", {}),
            })

        save("movies" if kind == "movie" else "series")

    await state.clear()

    await call.message.answer(tr("ar", "saved"))
    await call.answer()


@router.callback_query(F.data == "addcancel")
async def add_cancel(call: CallbackQuery, state: FSMContext):
    if is_admin(call.from_user.id):
        await state.clear()
        await call.message.answer(tr("ar", "cancelled"))

    await call.answer()


# ==========================================================
# 24) ADD SERIES EPISODES
# ==========================================================

@router.message(Command("episode"))
@admin_only
async def episode_command(message: Message):
    raw = (message.text or "").split(maxsplit=1)

    if len(raw) < 2:
        await message.answer(
            "الاستخدام:\n/episode اسم المسلسل رقم الموسم\n\n"
            "مثال:\n/episode Breaking Bad 2"
        )
        return

    arguments = raw[1].strip()

    # آخر كلمة هي رقم الموسم، وما قبلها اسم المسلسل كاملاً.
    parts = arguments.rsplit(maxsplit=1)

    if len(parts) != 2:
        await message.answer("يجب كتابة اسم المسلسل ثم رقم الموسم.")
        return

    title = parts[0].replace("_", " ")

    try:
        season_number = int(parts[1])

        if season_number <= 0:
            raise ValueError

    except ValueError:
        await message.answer(tr("ar", "bad_number"))
        return

    found = next(
        (
            (item_id, item)
            for item_id, item in SERIES.items()
            if normalize(item.get("title", "")) == normalize(title)
        ),
        None
    )

    if not found:
        await message.answer(tr("ar", "not_found"))
        return

    item_id, item = found

    season = next(
        (
            s for s in item["seasons"]
            if int(s["number"]) == season_number
        ),
        None
    )

    if not season:
        season = {
            "number": season_number,
            "episodes": [],
        }

        item["seasons"].append(season)
        save("series")

    STATE.setdefault("episode_targets", {})[
        str(ADMIN_ID)
    ] = {
        "series_id": item_id,
        "season": season_number,
    }

    save("state")

    await message.answer(
        f"أرسل حلقات الموسم {season_number} الآن.\n"
        "يمكنك إرسالها تباعاً، ثم استخدم /done_episodes."
    )


@router.message(Command("done_episodes"))
@admin_only
async def done_episodes(message: Message):
    STATE.get("episode_targets", {}).pop(
        str(ADMIN_ID),
        None
    )

    save("state")

    await message.answer(tr("ar", "done_episodes"))


@router.message(F.video | F.document)
async def receive_episode(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    target = STATE.get("episode_targets", {}).get(
        str(ADMIN_ID)
    )

    if not target:
        return

    file_id, file_type, meta = get_video_id(message)

    if not file_id:
        await message.answer("الملف ليس فيديو.")
        return

    item = SERIES.get(target["series_id"])

    if not item:
        await message.answer(tr("ar", "not_found"))
        return

    season = next(
        (
            s for s in item["seasons"]
            if int(s["number"]) == int(target["season"])
        ),
        None
    )

    if not season:
        return

    episode_number = max(
        [
            int(e["number"])
            for e in season["episodes"]
        ],
        default=0
    ) + 1

    season["episodes"].append({
        "number": episode_number,
        "file_id": file_id,
        "file_type": file_type,
        "meta": meta,
    })

    save("series")

    await message.answer(
        tr("ar", "episode_added", number=episode_number)
    )


# ==========================================================
# 25) ADMIN PANEL
# ==========================================================

def admin_main_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ إضافة محتوى",
                    callback_data="adm:add"
                ),
                InlineKeyboardButton(
                    text="🎬 الأفلام",
                    callback_data="adm:movies"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📺 المسلسلات",
                    callback_data="adm:series"
                ),
                InlineKeyboardButton(
                    text="📚 التصنيفات",
                    callback_data="adm:categories"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⚙️ الإعدادات",
                    callback_data="adm:settings"
                ),
                InlineKeyboardButton(
                    text="📊 الإحصائيات",
                    callback_data="adm:stats"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="💾 نسخة احتياطية",
                    callback_data="adm:backup"
                ),
                InlineKeyboardButton(
                    text="📥 استعادة",
                    callback_data="adm:restore"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="👥 المستخدمون",
                    callback_data="adm:users"
                ),
            ],
        ]
    )


@router.message(Command("manage"))
@admin_only
async def manage_command(message: Message):
    await message.answer(
        "<b>🛠 لوحة إدارة Legend HD</b>\nاختر القسم الذي تريد إدارته:",
        reply_markup=admin_main_keyboard()
    )


@router.callback_query(F.data.startswith("adm:"))
async def admin_panel_callback(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True)
        return

    section = call.data.split(":", 1)[1]

    if section == "add":
        await state.clear()
        await state.set_state(AddFlow.kind)

        await call.message.answer(
            tr("ar", "add_type"),
            reply_markup=admin_kind_keyboard()
        )

    elif section in ("movies", "series"):
        kind = "movie" if section == "movies" else "series"

        await state.update_data(
            admin_list_kind=kind
        )

        await send_admin_content_list(
            call.message,
            kind,
            0
        )

    elif section == "categories":
        await show_admin_categories(call.message)

    elif section == "settings":
        await show_admin_settings(call.message)

    elif section == "stats":
        await show_statistics(call.message)

    elif section == "backup":
        await create_and_send_backup(call.message)

    elif section == "restore":
        await state.set_state(EditFlow.value)
        await state.update_data(edit_action="restore")
        await call.message.answer(tr("ar", "restore_help"))

    elif section == "users":
        await show_users(call.message)

    await call.answer()


async def send_admin_content_list(message, kind, page):
    collection = MOVIES if kind == "movie" else SERIES

    items = sorted(
        collection.items(),
        key=lambda x: x[1].get("title", "").casefold()
    )

    total_pages = max(
        1,
        (len(items) + PAGE_SIZE - 1) // PAGE_SIZE
    )

    page = max(0, min(page, total_pages - 1))

    chunk = items[
        page * PAGE_SIZE:
        (page + 1) * PAGE_SIZE
    ]

    rows = []

    for item_id, item in chunk:
        rows.append([
            InlineKeyboardButton(
                text=item.get("title", "")[:55],
                callback_data=f"admitem:{kind}:{item_id}"
            )
        ])

    nav = []

    if page > 0:
        nav.append(
            InlineKeyboardButton(
                text="⬅️",
                callback_data=f"admpage:{kind}:{page-1}"
            )
        )

    if page + 1 < total_pages:
        nav.append(
            InlineKeyboardButton(
                text="➡️",
                callback_data=f"admpage:{kind}:{page+1}"
            )
        )

    if nav:
        rows.append(nav)

    rows.append([
        InlineKeyboardButton(
            text="🔎 البحث عن محتوى",
            callback_data=f"admsearch:{kind}"
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ لوحة الإدارة",
            callback_data="adm:home"
        )
    ])

    await message.answer(
        f"إدارة {'الأفلام' if kind == 'movie' else 'المسلسلات'}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=rows
        )
    )


@router.callback_query(F.data == "adm:home")
async def admin_home_callback(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    await call.message.answer(
        "<b>لوحة إدارة Legend HD</b>",
        reply_markup=admin_main_keyboard()
    )

    await call.answer()


@router.callback_query(F.data.startswith("admpage:"))
async def admin_page_callback(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, kind, page = call.data.split(":")

    await send_admin_content_list(
        call.message,
        kind,
        int(page)
    )

    await call.answer()


@router.callback_query(F.data.startswith("admitem:"))
async def admin_item_callback(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, kind, item_id = call.data.split(":")

    collection = MOVIES if kind == "movie" else SERIES
    item = collection.get(item_id)

    if not item:
        await call.answer("غير موجود", show_alert=True)
        return

    title = html.escape(item.get("title", ""))

    rows = [
        [
            InlineKeyboardButton(
                text="✏️ تعديل الاسم",
                callback_data=f"admfield:title:{kind}:{item_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="📝 تعديل القصة",
                callback_data=f"admfield:story:{kind}:{item_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="📚 تغيير التصنيف",
                callback_data=f"admfield:category:{kind}:{item_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="🏷 تعديل الأسماء البديلة",
                callback_data=f"admfield:aliases:{kind}:{item_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="🗑 حذف المحتوى",
                callback_data=f"admdelete:{kind}:{item_id}"
            )
        ],
    ]

    if kind == "movie":
        for part in item.get("parts", []):
            rows.append([
                InlineKeyboardButton(
                    text=f"حذف الجزء {part['number']}",
                    callback_data=f"admdeletepart:{item_id}:{part['number']}"
                )
            ])

    else:
        for season in item.get("seasons", []):
            rows.append([
                InlineKeyboardButton(
                    text=f"إضافة حلقات للموسم {season['number']}",
                    callback_data=f"admseasonadd:{item_id}:{season['number']}"
                )
            ])

            rows.append([
                InlineKeyboardButton(
                    text=f"حذف الموسم {season['number']}",
                    callback_data=f"admdeleteseason:{item_id}:{season['number']}"
                )
            ])

            for episode in season.get("episodes", []):
                rows.append([
                    InlineKeyboardButton(
                        text=(
                            f"حذف الحلقة {episode['number']} "
                            f"من الموسم {season['number']}"
                        ),
                        callback_data=(
                            f"admdeleteepisode:{item_id}:"
                            f"{season['number']}:{episode['number']}"
                        )
                    )
                ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ رجوع",
            callback_data=f"adm:{'movies' if kind == 'movie' else 'series'}"
        )
    ])

    await call.message.answer(
        f"<b>{title}</b>\nاختر العملية:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=rows
        )
    )

    await call.answer()


# ==========================================================
# 26) ADMIN EDITING
# ==========================================================

@router.callback_query(F.data.startswith("admfield:"))
async def admin_edit_field(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, field, kind, item_id = call.data.split(":")

    if field == "category":
        rows = []

        for category in CATEGORIES:
            rows.append([
                InlineKeyboardButton(
                    text=category_name(category["id"], "ar"),
                    callback_data=(
                        f"admcat:{kind}:{item_id}:{category['id']}"
                    )
                )
            ])

        await call.message.answer(
            "اختر التصنيف الجديد:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=rows
            )
        )

        await call.answer()
        return

    await state.clear()
    await state.set_state(EditFlow.value)

    await state.update_data(
        edit_action="content",
        edit_field=field,
        edit_kind=kind,
        edit_id=item_id
    )

    await call.message.answer(
        tr("ar", "send_new_value")
    )

    await call.answer()


@router.message(EditFlow.value)
async def admin_edit_value(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    data = await state.get_data()
    action = data.get("edit_action")

    if action == "restore":
        await restore_backup_from_message(message, state)
        return

    if action == "category_add":
        name = (message.text or "").strip()

        if not name:
            await message.answer("الاسم غير صالح.")
            return

        if any(
            normalize(c["name"].get("ar", "")) == normalize(name)
            for c in CATEGORIES
        ):
            await message.answer(tr("ar", "category_exists"))
            return

        CATEGORIES.append({
            "id": uuid.uuid4().hex[:10],
            "name": {
                "ar": name,
                "en": name,
            }
        })

        save("categories")
        await state.clear()

        await message.answer(tr("ar", "category_added"))
        return

    if action != "content":
        await state.clear()
        return

    field = data.get("edit_field")
    kind = data.get("edit_kind")
    item_id = data.get("edit_id")

    collection = MOVIES if kind == "movie" else SERIES
    item = collection.get(item_id)

    if not item:
        await state.clear()
        await message.answer(tr("ar", "not_found"))
        return

    value = (message.text or "").strip()

    if field == "title":
        if not value or len(value) > MAX_TITLE_LENGTH:
            await message.answer("الاسم غير صالح.")
            return

        item["title"] = value

    elif field == "story":
        if len(value) > MAX_STORY_LENGTH:
            await message.answer("القصة طويلة جداً.")
            return

        item["story"] = value

    elif field == "aliases":
        aliases = [
            x.strip()
            for x in value.split("|")
            if x.strip()
        ]

        if len(aliases) > MAX_ALIAS_COUNT:
            await message.answer("عدد الأسماء كبير جداً.")
            return

        item["aliases"] = aliases

    save("movies" if kind == "movie" else "series")

    await state.clear()
    await message.answer(tr("ar", "updated"))


@router.callback_query(F.data.startswith("admcat:"))
async def admin_change_category(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, kind, item_id, category_id = call.data.split(":")

    collection = MOVIES if kind == "movie" else SERIES
    item = collection.get(item_id)

    if not item:
        await call.answer(tr("ar", "not_found"), show_alert=True)
        return

    if not any(c["id"] == category_id for c in CATEGORIES):
        await call.answer("تصنيف غير صالح", show_alert=True)
        return

    item["category"] = category_id

    save("movies" if kind == "movie" else "series")

    await call.answer(tr("ar", "updated"), show_alert=True)


# ==========================================================
# 27) ADMIN DELETE CONTENT
# ==========================================================

@router.callback_query(F.data.startswith("admdelete:"))
async def admin_delete_content(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, kind, item_id = call.data.split(":")

    collection = MOVIES if kind == "movie" else SERIES

    if item_id not in collection:
        await call.answer(tr("ar", "not_found"), show_alert=True)
        return

    del collection[item_id]

    save("movies" if kind == "movie" else "series")

    await call.message.answer(tr("ar", "deleted"))
    await call.answer()


@router.callback_query(F.data.startswith("admdeletepart:"))
async def admin_delete_part(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, item_id, number = call.data.split(":")

    item = MOVIES.get(item_id)

    if not item:
        await call.answer()
        return

    item["parts"] = [
        p for p in item.get("parts", [])
        if int(p.get("number", 0)) != int(number)
    ]

    save("movies")

    await call.answer(tr("ar", "deleted"), show_alert=True)


@router.callback_query(F.data.startswith("admdeleteseason:"))
async def admin_delete_season(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, item_id, number = call.data.split(":")

    item = SERIES.get(item_id)

    if not item:
        await call.answer()
        return

    item["seasons"] = [
        s for s in item.get("seasons", [])
        if int(s.get("number", 0)) != int(number)
    ]

    save("series")

    await call.answer(tr("ar", "deleted"), show_alert=True)


@router.callback_query(F.data.startswith("admdeleteepisode:"))
async def admin_delete_episode(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, item_id, season_number, episode_number = call.data.split(":")

    item = SERIES.get(item_id)

    if not item:
        await call.answer()
        return

    season = next(
        (
            s for s in item["seasons"]
            if int(s["number"]) == int(season_number)
        ),
        None
    )

    if not season:
        await call.answer()
        return

    season["episodes"] = [
        e for e in season["episodes"]
        if int(e["number"]) != int(episode_number)
    ]

    save("series")
    await call.answer(tr("ar", "deleted"), show_alert=True)


@router.callback_query(F.data.startswith("admseasonadd:"))
async def admin_add_season_episodes(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, item_id, season_number = call.data.split(":")

    item = SERIES.get(item_id)

    if not item:
        await call.answer()
        return

    STATE.setdefault("episode_targets", {})[
        str(ADMIN_ID)
    ] = {
        "series_id": item_id,
        "season": int(season_number),
    }

    save("state")

    await call.message.answer(
        f"أرسل حلقات الموسم {season_number} الآن.\n"
        "استخدم /done_episodes عند الانتهاء."
    )

    await call.answer()


# ==========================================================
# 28) CATEGORY MANAGEMENT
# ==========================================================

async def show_admin_categories(message):
    rows = []

    for category in CATEGORIES:
        rows.append([
            InlineKeyboardButton(
                text=category_name(category["id"], "ar"),
                callback_data=f"admcatitem:{category['id']}"
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text="➕ إضافة تصنيف",
            callback_data="admcatadd"
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ لوحة الإدارة",
            callback_data="adm:home"
        )
    ])

    await message.answer(
        "إدارة التصنيفات:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=rows
        )
    )


@router.callback_query(F.data.startswith("admcatitem:"))
async def admin_category_item(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    category_id = call.data.split(":", 1)[1]

    category = next(
        (
            c for c in CATEGORIES
            if c["id"] == category_id
        ),
        None
    )

    if not category:
        await call.answer()
        return

    rows = [
        [
            InlineKeyboardButton(
                text="✏️ تعديل الاسم",
                callback_data=f"admcatrename:{category_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="🗑 حذف التصنيف",
                callback_data=f"admcatdelete:{category_id}"
            )
        ],
    ]

    await call.message.answer(
        category_name(category_id, "ar"),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=rows
        )
    )

    await call.answer()


@router.callback_query(F.data == "admcatadd")
async def admin_category_add(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    await state.clear()
    await state.set_state(EditFlow.value)
    await state.update_data(edit_action="category_add")

    await call.message.answer("أرسل اسم التصنيف الجديد.")
    await call.answer()


@router.callback_query(F.data.startswith("admcatrename:"))
async def admin_category_rename(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    category_id = call.data.split(":", 1)[1]

    await state.clear()
    await state.set_state(EditFlow.value)

    await state.update_data(
        edit_action="category_rename",
        category_id=category_id
    )

    await call.message.answer("أرسل الاسم الجديد للتصنيف.")
    await call.answer()


@router.callback_query(F.data.startswith("admcatdelete:"))
async def admin_category_delete(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    category_id = call.data.split(":", 1)[1]

    used = any(
        item.get("category") == category_id
        for collection in (MOVIES, SERIES)
        for item in collection.values()
    )

    if used:
        await call.answer(
            tr("ar", "category_delete_used"),
            show_alert=True
        )
        return

    CATEGORIES[:] = [
        c for c in CATEGORIES
        if c["id"] != category_id
    ]

    save("categories")

    await call.answer(tr("ar", "category_deleted"), show_alert=True)


# ==========================================================
# 29) CATEGORY RENAME STATE
# ==========================================================

@router.message(EditFlow.value)
async def admin_edit_value_secondary(message: Message, state: FSMContext):
    # هذا المعالج احتياطي؛ المعالج السابق يتولى الحالة.
    return
 # ==========================================================
# 30) SETTINGS
# ==========================================================

async def show_admin_settings(message):
    ads_enabled = settings().get("ads_enabled", True)
    sub_enabled = settings().get("require_subscription", True)

    ads_text = (
        tr("ar", "ads_on")
        if ads_enabled
        else tr("ar", "ads_off")
    )

    sub_text = (
        tr("ar", "sub_on")
        if sub_enabled
        else tr("ar", "sub_off")
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=ads_text,
                    callback_data="toggle:ads"
                )
            ],
            [
                InlineKeyboardButton(
                    text=sub_text,
                    callback_data="toggle:subscription"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ لوحة الإدارة",
                    callback_data="adm:home"
                )
            ],
        ]
    )

    await message.answer(
        "إعدادات البوت:",
        reply_markup=keyboard
    )


@router.callback_query(F.data.startswith("toggle:"))
async def toggle_setting(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    setting = call.data.split(":", 1)[1]

    if setting == "ads":
        settings()["ads_enabled"] = not settings().get(
            "ads_enabled",
            True
        )

    elif setting == "subscription":
        settings()["require_subscription"] = not settings().get(
            "require_subscription",
            True
        )

    save("state")

    await call.answer("تم تحديث الإعداد.")
    await show_admin_settings(call.message)


# ==========================================================
# 31) STATISTICS
# ==========================================================

async def show_statistics(message):
    movie_count = len(MOVIES)
    series_count = len(SERIES)

    parts_count = sum(
        len(item.get("parts", []))
        for item in MOVIES.values()
    )

    episode_count = sum(
        len(season.get("episodes", []))
        for item in SERIES.values()
        for season in item.get("seasons", [])
    )

    user_count = len(USERS)

    blocked_count = sum(
        1 for user in USERS.values()
        if user.get("blocked")
    )

    text = (
        "<b>📊 إحصائيات Legend HD</b>\n\n"
        f"👥 المستخدمون: {user_count}\n"
        f"🚫 المحظورون: {blocked_count}\n"
        f"🎬 الأفلام: {movie_count}\n"
        f"📦 أجزاء الأفلام: {parts_count}\n"
        f"📺 المسلسلات: {series_count}\n"
        f"🎞 الحلقات: {episode_count}\n"
        f"📚 التصنيفات: {len(CATEGORIES)}"
    )

    await message.answer(text)


# ==========================================================
# 32) USERS
# ==========================================================

async def show_users(message):
    rows = [
        [
            InlineKeyboardButton(
                text="🔎 البحث عن مستخدم",
                callback_data="admusersearch"
            )
        ],
        [
            InlineKeyboardButton(
                text="⬅️ لوحة الإدارة",
                callback_data="adm:home"
            )
        ]
    ]

    await message.answer(
        f"عدد المستخدمين المسجلين: {len(USERS)}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=rows
        )
    )


@router.callback_query(F.data == "admusersearch")
async def admin_user_search(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    await state.clear()
    await state.set_state(EditFlow.value)
    await state.update_data(edit_action="user_search")

    await call.message.answer("أرسل Telegram User ID.")
    await call.answer()


@router.callback_query(F.data.startswith("admuser:"))
async def admin_user_action(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    _, action, user_id = call.data.split(":")

    record = USERS.get(user_id)

    if not record:
        await call.answer("المستخدم غير موجود.", show_alert=True)
        return

    if action == "block":
        record["blocked"] = True
    elif action == "unblock":
        record["blocked"] = False

    save("users")

    await call.answer("تم تحديث حالة المستخدم.", show_alert=True)


# ==========================================================
# 33) BACKUP AND RESTORE
# ==========================================================

async def create_and_send_backup(message):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    backup_path = os.path.join(
        BACKUP_DIR,
        f"Legend_HD_backup_{stamp}.zip"
    )

    with zipfile.ZipFile(
        backup_path,
        "w",
        zipfile.ZIP_DEFLATED
    ) as archive:
        for name, path in FILES.items():
            if os.path.exists(path):
                archive.write(
                    path,
                    arcname=os.path.basename(path)
                )

    try:
        with open(backup_path, "rb") as file:
            await message.answer_document(
                file,
                caption=tr("ar", "backup_done")
            )
    finally:
        try:
            os.remove(backup_path)
        except OSError:
            pass
async def restore_backup_from_message(message, state):
    if not message.document:
        await message.answer("أرسل ملف ZIP.")
        return

    filename = (message.document.file_name or "").lower()

    if not filename.endswith(".zip"):
        await message.answer("يجب أن يكون الملف بصيغة ZIP.")
        return

    buffer = io.BytesIO()

    try:
        await bot.download(
            message.document.file_id,
            destination=buffer
        )

        buffer.seek(0)

        with zipfile.ZipFile(buffer) as archive:
            allowed = {
                "movies.json",
                "series.json",
                "categories.json",
                "users.json",
                "state.json",
            }

            contents = {}

            for info in archive.infolist():
                name = os.path.basename(info.filename)

                if name not in allowed:
                    continue

                raw = archive.read(info.filename)
                contents[name] = json.loads(
                    raw.decode("utf-8")
                )

            expected = {
                "movies.json": dict,
                "series.json": dict,
                "categories.json": list,
                "users.json": dict,
                "state.json": dict,
            }

            for name, expected_type in expected.items():
                if name in contents:
                    if not isinstance(contents[name], expected_type):
                        raise ValueError("Invalid backup structure")

            with STORE_LOCK:
                for name, data in contents.items():
                    path = os.path.join(DATA_DIR, name)
                    atomic_json(path, data)

                MOVIES.clear()
                MOVIES.update(contents.get("movies.json", MOVIES))

                SERIES.clear()
                SERIES.update(contents.get("series.json", SERIES))

                CATEGORIES[:] = contents.get(
                    "categories.json",
                    CATEGORIES
                )

                USERS.clear()
                USERS.update(contents.get("users.json", USERS))

                STATE.clear()
                STATE.update(contents.get("state.json", STATE))

            await state.clear()

            await message.answer(tr("ar", "restore_done"))

    except Exception:
        log.exception("Backup restore failed.")
        await message.answer(
            "تعذرت استعادة النسخة. تأكد من سلامة ملف ZIP."
        )


# ==========================================================
# 34) CATEGORY COMMANDS
# ==========================================================

@router.message(Command("add_category"))
@admin_only
async def add_category_command(message: Message):
    parts = (message.text or "").split(maxsplit=1)

    if len(parts) < 2:
        await message.answer("الاستخدام: /add_category اسم التصنيف")
        return

    name = parts[1].strip()

    exists = any(
        normalize(c["name"].get("ar", "")) == normalize(name)
        or normalize(c["name"].get("en", "")) == normalize(name)
        for c in CATEGORIES
    )

    if exists:
        await message.answer(tr("ar", "category_exists"))
        return

    CATEGORIES.append({
        "id": uuid.uuid4().hex[:10],
        "name": {
            "ar": name,
            "en": name,
        }
    })

    save("categories")

    await message.answer(tr("ar", "category_added"))


@router.message(Command("edit_category"))
@admin_only
async def edit_category_command(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or "|" not in parts[1]:
        await message.answer(
            "الاستخدام:\n/edit_category الاسم القديم | الاسم الجديد"
        )
        return

    old_name, new_name = [
        x.strip()
        for x in parts[1].split("|", 1)
    ]

    category = next(
        (
            c for c in CATEGORIES
            if normalize(c["name"].get("ar", "")) == normalize(old_name)
            or normalize(c["name"].get("en", "")) == normalize(old_name)
        ),
        None
    )

    if not category:
        await message.answer(tr("ar", "not_found"))
        return

    category["name"] = {
        "ar": new_name,
        "en": new_name,
    }

    save("categories")

    await message.answer(tr("ar", "updated"))


@router.message(Command("delete_category"))
@admin_only
async def delete_category_command(message: Message):
    await show_admin_categories(message)


# ==========================================================
# 35) ADMIN SEARCH
# ==========================================================

@router.callback_query(F.data.startswith("admsearch:"))
async def admin_search_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer()
        return

    kind = call.data.split(":", 1)[1]

    await state.clear()
    await state.set_state(EditFlow.value)

    await state.update_data(
        edit_action="admin_search",
        search_kind=kind
    )

    await call.message.answer(tr("ar", "search_admin"))
    await call.answer()


# ==========================================================
# 36) ADMIN VALUE ROUTER
# ==========================================================

@router.message(EditFlow.value)
async def admin_state_router(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    data = await state.get_data()
    action = data.get("edit_action")

    if action == "user_search":
        try:
            target_id = str(int((message.text or "").strip()))
        except ValueError:
            await message.answer("أرسل رقماً صحيحاً.")
            return

        record = USERS.get(target_id)

        if not record:
            await message.answer("المستخدم غير مسجل.")
            await state.clear()
            return

        blocked = record.get("blocked", False)

        button = (
            InlineKeyboardButton(
                text=tr("ar", "unblock"),
                callback_data=f"admuser:unblock:{target_id}"
            )
            if blocked
            else InlineKeyboardButton(
                text=tr("ar", "block"),
                callback_data=f"admuser:block:{target_id}"
            )
        )

        await message.answer(
            f"User ID: <code>{target_id}</code>\n"
            f"Language: {record.get('language', 'ar')}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[button]]
            )
        )

        await state.clear()
        return

    if action == "admin_search":
        query = (message.text or "").strip()
        kind = data.get("search_kind")

        collection = MOVIES if kind == "movie" else SERIES

        results = []

        for item_id, item in collection.items():
            score = title_score(
                query,
                item.get("title", ""),
                item.get("aliases", [])
            )

            if score >= FUZZY_CUTOFF:
                results.append((score, item_id, item))

        results.sort(key=lambda x: x[0], reverse=True)

        rows = []

        for _, item_id, item in results[:20]:
            rows.append([
                InlineKeyboardButton(
                    text=item.get("title", "")[:55],
                    callback_data=f"admitem:{kind}:{item_id}"
                )
            ])

        await message.answer(
            "نتائج البحث:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=rows
            ) if rows else None
        )

        await state.clear()
        return

    if action == "category_rename":
        category_id = data.get("category_id")
        new_name = (message.text or "").strip()

        category = next(
            (
                c for c in CATEGORIES
                if c["id"] == category_id
            ),
            None
        )

        if not category or not new_name:
            await message.answer("تعذر تعديل التصنيف.")
            await state.clear()
            return

        category["name"] = {
            "ar": new_name,
            "en": new_name,
        }

        save("categories")
        await state.clear()

        await message.answer(tr("ar", "updated"))
        return


# ==========================================================
# 37) MAIN MENU AND SEARCH
# ==========================================================

@router.message(Command("cancel"))
async def cancel_command(message: Message, state: FSMContext):
    if message.from_user and is_admin(message.from_user.id):
        await state.clear()

        STATE.get("episode_targets", {}).pop(
            str(ADMIN_ID),
            None
        )

        save("state")

        await message.answer(tr("ar", "cancelled"))


@router.message(F.text)
async def menu_text(message: Message, state: FSMContext):
    if not message.from_user:
        return

    if await state.get_state():
        return

    if not await gate(message):
        return

    record = user_record(message.from_user.id)

    lang = record.get("language", DEFAULT_LANGUAGE)
    text = (message.text or "").strip()

    mapping = {
        tr(lang, "search"): "search",
        tr(lang, "movies"): "movie",
        tr(lang, "series"): "series",
        tr(lang, "categories"): "categories",
        tr(lang, "language"): "language",
        tr(lang, "about"): "about",
        tr(lang, "sponsored"): "sponsored",
        ("🎬 افتح تطبيق Legend HD" if lang == "ar" else "🎬 Open Legend HD"): "open_app",
    }

    action = mapping.get(text)

    if action == "open_app":
        await message.answer(
            "اضغط لفتح تطبيق Legend HD:" if lang == "ar" else "Tap to open Legend HD:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="🎬 فتح التطبيق" if lang == "ar" else "🎬 Open app",
                    web_app=WebAppInfo(url=MINI_APP_URL)
                )
            ]])
        )
        return

    if action == "language":
        await message.answer(
            tr(lang, "choose_lang"),
            reply_markup=lang_keyboard()
        )
        return

    if action == "about":
        await message.answer(tr(lang, "about_text"))
        return

    if action == "sponsored":
        await show_sponsored_ad(message)
        return

    if action == "search":
        STATE.setdefault("search_wait", {})[
            str(message.from_user.id)
        ] = True

        save("state")

        await message.answer(tr(lang, "ask_search"))
        return

    if action in ("movie", "series"):
        STATE.get("search_wait", {}).pop(
            str(message.from_user.id),
            None
        )
        save("state")

        keyboard = content_list(
            action,
            0,
            lang
        )

        await message.answer(
            tr(lang, "movies" if action == "movie" else "series"),
            reply_markup=keyboard
        )
        return

    if action == "categories":
        STATE.get("search_wait", {}).pop(
            str(message.from_user.id),
            None
        )
        save("state")

        await message.answer(
            tr(lang, "categories"),
            reply_markup=categories_keyboard(lang)
        )
        return

    search_wait = STATE.setdefault("search_wait", {})

    if str(message.from_user.id) in search_wait:
        search_wait.pop(str(message.from_user.id), None)
        save("state")

        results = search_content(text)

        if not results:
            await message.answer(tr(lang, "no_match"))
            return

        rows = []

        for score, kind, item_id, item in results:
            rows.append([
                InlineKeyboardButton(
                    text=item.get("title", "")[:60],
                    callback_data=f"view:{kind}:{item_id}"
                )
            ])

        await message.answer(
            "🔎 نتائج البحث:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=rows
            )
        )
        return

    await message.answer(
        tr(lang, "welcome"),
        reply_markup=main_keyboard(lang)
    )


# ==========================================================
# 38) WEBHOOK QUEUE
# ==========================================================

app = Flask(__name__)

BOT_LOOP = asyncio.new_event_loop()


def loop_worker():
    asyncio.set_event_loop(BOT_LOOP)
    BOT_LOOP.run_forever()


loop_thread = threading.Thread(
    target=loop_worker,
    name="legend-hd-loop",
    daemon=True
)

loop_thread.start()


def enqueue_update(update_id, payload):
    encoded = json.dumps(
        payload,
        ensure_ascii=False
    )

    with DB_LOCK:
        connection = db_connect()

        connection.execute("""
            INSERT OR IGNORE INTO webhook_queue(
                update_id,
                payload,
                status,
                attempts,
                created_at
            )
            VALUES (?, ?, 'pending', 0, ?)
        """, (
            update_id,
            encoded,
            time.time()
        ))

        connection.commit()
        connection.close()


def fetch_next_update():
    with DB_LOCK:
        connection = db_connect()

        connection.execute("BEGIN IMMEDIATE")

        row = connection.execute("""
            SELECT update_id, payload, attempts
            FROM webhook_queue
            WHERE status='pending'
            ORDER BY created_at ASC
            LIMIT 1
        """).fetchone()

        if not row:
            connection.commit()
            connection.close()
            return None

        connection.execute("""
            UPDATE webhook_queue
            SET status='processing'
            WHERE update_id=?
        """, (row["update_id"],))

        connection.commit()
        connection.close()

    return dict(row)


def complete_update(update_id):
    with DB_LOCK:
        connection = db_connect()

        connection.execute(
            "DELETE FROM webhook_queue WHERE update_id=?",
            (update_id,)
        )

        connection.commit()
        connection.close()


def retry_update(update_id, attempts):
    with DB_LOCK:
        connection = db_connect()

        if attempts >= 5:
            connection.execute("""
                UPDATE webhook_queue
                SET status='failed', attempts=?
                WHERE update_id=?
            """, (attempts + 1, update_id))
        else:
            connection.execute("""
                UPDATE webhook_queue
                SET status='pending', attempts=?
                WHERE update_id=?
            """, (attempts + 1, update_id))

        connection.commit()
        connection.close()


def reset_processing_updates():
    with DB_LOCK:
        connection = db_connect()

        connection.execute("""
            UPDATE webhook_queue
            SET status='pending'
            WHERE status='processing'
        """)

        connection.commit()
        connection.close()


async def update_worker():
    log.info("Webhook queue worker started.")

    while True:
        try:
            row = await asyncio.to_thread(fetch_next_update)

            if not row:
                await asyncio.sleep(0.4)
                continue

            update_id = row["update_id"]

            try:
                payload = json.loads(row["payload"])

                update = Update.model_validate(
                    payload,
                    context={"bot": bot}
                )

                await dp.feed_update(bot, update)

                await asyncio.to_thread(
                    complete_update,
                    update_id
                )

            except Exception:
                log.exception(
                    "Update processing failed: %s",
                    update_id
                )

                await asyncio.to_thread(
                    retry_update,
                    update_id,
                    row["attempts"]
                )

                await asyncio.sleep(1)

        except Exception:
            log.exception("Webhook queue worker error.")
            await asyncio.sleep(2)


# ==========================================================
# 39) LEGEND HD TELEGRAM MINI APP + AUTHENTICATED API
# ==========================================================

MINI_APP_HTML = r"""<!doctype html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#10131c">
  <title>Legend HD</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <script src="https://sad.adsgram.ai/js/sad.min.js"></script>
  <style>
    :root{color-scheme:dark;--bg:#10131c;--panel:#191e2b;--panel2:#222a3b;--text:#f5f6fa;--muted:#aab2c4;--accent:#e6b84b;--line:#30394c;--danger:#ff7474}
        *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI",sans-serif;padding:env(safe-area-inset-top) 14px calc(18px + env(safe-area-inset-bottom));}
    button,input{font:inherit}button{cursor:pointer;border:0} .top{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:14px 2px 18px}.brand{font-size:22px;font-weight:900;letter-spacing:.3px}.brand span{color:var(--accent)}.smallbtn{background:var(--panel2);color:var(--text);padding:9px 12px;border-radius:12px}
    .search{width:100%;padding:14px 15px;border-radius:14px;border:1px solid var(--line);background:var(--panel);color:var(--text);outline:none;margin-bottom:14px}
    .tabs{display:flex;gap:8px;margin-bottom:14px}.tab{flex:1;background:var(--panel);color:var(--muted);padding:12px;border-radius:12px;font-weight:700}.tab.active{background:var(--accent);color:#171717}
    .chips{display:flex;gap:8px;overflow-x:auto;padding:2px 0 14px;scrollbar-width:none}.chip{white-space:nowrap;background:var(--panel2);color:var(--text);padding:8px 13px;border-radius:999px;border:1px solid transparent}.chip.active{border-color:var(--accent);color:var(--accent)}
    .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.card{background:var(--panel);border:1px solid var(--line);border-radius:15px;overflow:hidden;min-width:0}.poster{width:100%;aspect-ratio:2/3;object-fit:cover;background:var(--panel2);display:block}.placeholder{aspect-ratio:2/3;display:flex;align-items:center;justify-content:center;background:var(--panel2);font-size:42px;color:var(--accent)}.cardtitle{padding:10px 10px 12px;font-weight:750;font-size:14px;line-height:1.4;min-height:58px}
    .muted{color:var(--muted)}.empty{text-align:center;padding:36px 10px;color:var(--muted)}.sectiontitle{font-size:17px;font-weight:800;margin:10px 0 14px}
    .detail{background:var(--panel);border:1px solid var(--line);border-radius:16px;overflow:hidden}.detailhead{display:flex;gap:13px;padding:13px}.detailposter{width:112px;height:158px;object-fit:cover;border-radius:10px;background:var(--panel2);flex-shrink:0}.detailname{font-size:18px;font-weight:850;margin:2px 0 8px}.story{padding:0 14px 14px;color:#d7dbe5;white-space:pre-wrap;line-height:1.7}.group{padding:0 14px 14px}.group h3{font-size:14px;color:var(--accent);margin:8px 0}.actions{display:flex;flex-wrap:wrap;gap:8px}.action{background:var(--panel2);border:1px solid var(--line);color:var(--text);padding:10px 13px;border-radius:11px}.primary{background:var(--accent);color:#171717;font-weight:800}.back{margin:0 0 12px;background:transparent;color:var(--accent);padding:8px 0;font-weight:800}
    .notice{background:var(--panel);border:1px solid var(--line);padding:16px;border-radius:14px;line-height:1.7;margin:20px 0}.joinlink{display:block;margin:9px 0;padding:11px;background:var(--panel2);border-radius:10px;color:var(--accent);text-decoration:none}.footer{text-align:center;color:var(--muted);font-size:11px;padding:24px 0 8px}.hidden{display:none!important}
  </style>
</head>
<body>
  <header class="top"><div class="brand">LEGEND <span>HD</span></div><button id="langBtn" class="smallbtn">EN</button></header>
  <main id="app">
    <input id="search" class="search" type="search" placeholder="ابحث عن فيلم أو مسلسل..." autocomplete="off">
    <div class="tabs"><button id="moviesTab" class="tab active">🎬 الأفلام</button><button id="seriesTab" class="tab">📺 المسلسلات</button></div>
    <div id="categories" class="chips"></div>
    <div id="content" class="grid"></div>
    <div id="footer" class="footer">Legend HD</div>
  </main>
  <script>
  (()=>{
    const tg=window.Telegram?.WebApp;
    const app=document.getElementById('app'), content=document.getElementById('content'), categories=document.getElementById('categories');
    const state={lang:'ar',kind:'movie',category:'all',items:[],bootstrap:null,detail:null,controllers:[],rewardController:null,adIndex:0,searchTimer:null,lastInterstitialAt:0,interstitialTimer:null,rewardInProgress:false};
    const initData=tg?.initData||'';
    if(tg){tg.ready();tg.expand();try{tg.setHeaderColor('#10131c');tg.setBackgroundColor('#10131c');}catch(e){}}
    const $=id=>document.getElementById(id);
    const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const tr=(ar,en)=>state.lang==='ar'?ar:en;
    async function api(path,options={}){
      const headers={'X-Telegram-Init-Data':initData,...(options.headers||{})};
      if(options.body)headers['Content-Type']='application/json';
      const response=await fetch(path,{...options,headers});
      const data=await response.json().catch(()=>({error:'bad_response'}));
      if(!response.ok)throw Object.assign(new Error(data.message||data.error||'Request failed'),{data,status:response.status});
      return data;
    }
    function notice(message,links=[]){
      app.innerHTML='<div class="notice">'+esc(message)+links.map(x=>'<a class="joinlink" href="'+esc(x.url)+'" target="_blank">'+esc(x.name)+'</a>').join('')+'</div>';
    }
function initAds(){
  state.controllers=[];
  state.rewardController=null;

  if(!state.bootstrap?.ads?.enabled||!window.Adsgram)return;

  // Interstitial controllers - unchanged block IDs
  const ids=state.bootstrap?.ads?.interstitial_block_ids||[];

  ids.forEach(id=>{
    try{
      state.controllers.push(window.Adsgram.init({blockId:id}));
    }catch(e){
      console.warn('AdsGram Interstitial init failed',e);
    }
  });

  // Separate Rewarded controller
  const rewardId=state.bootstrap?.ads?.rewarded_block_id||'';

  if(rewardId){
    try{
      state.rewardController=window.Adsgram.init({blockId:rewardId});
    }catch(e){
      console.warn('AdsGram Rewarded init failed',e);
    }
  }

  state.lastInterstitialAt=Date.now();

  const interval=Math.max(
    60,
    Number(state.bootstrap?.ads?.interstitial_interval||180)
  )*1000;

  if(state.interstitialTimer)clearInterval(state.interstitialTimer);

  state.interstitialTimer=setInterval(
    ()=>showInterstitial(false),
    interval
  );
}

async function showInterstitial(force=true){

  if(!state.controllers.length||state.rewardInProgress)return;

  const interval=Math.max(
    60,
    Number(state.bootstrap?.ads?.interstitial_interval||180)
  )*1000;

  const now=Date.now();

  if(!force&&(
    document.visibilityState!=='visible'||
    now-state.lastInterstitialAt<interval
  ))return;

  const controller=state.controllers[
    state.adIndex%state.controllers.length
  ];

  state.adIndex++;
  state.lastInterstitialAt=now;

  try{
    await controller.show();
  }catch(e){
    console.warn('AdsGram Interstitial unavailable',e);
  }
}

async function showRewarded(requestId){

  if(!state.rewardController){
    throw new Error(
      tr(
        'إعلان المكافأة غير مهيأ حالياً.',
        'Rewarded ad is not configured.'
      )
    );
  }

  state.rewardInProgress=true;

  try{

    const result=await state.rewardController.show();

    if(!result||result.done!==true){
      throw new Error(
        tr(
          'لم تكتمل مشاهدة الإعلان.',
          'The ad was not completed.'
        )
      );
    }

    // Wait for server-side reward confirmation
    for(let attempt=0;attempt<30;attempt++){

      const status=await api(
        '/api/miniapp/reward/status?request_id='+
        encodeURIComponent(requestId)
      );

      if(status.status==='eligible')return true;

      if(
        status.status==='expired'||
        status.status==='cancelled'||
        status.status==='used'
      )break;

      await new Promise(resolve=>setTimeout(resolve,1000));
    }

    throw new Error(
      tr(
        'لم يصل تأكيد المكافأة من خادم الإعلانات. حاول مرة أخرى.',
        'Reward confirmation was not received. Please try again.'
      )
    );

  }finally{
    state.rewardInProgress=false;
  }
}
      if(!state.controllers.length)return;
      const controller=state.controllers[state.adIndex%state.controllers.length];state.adIndex++;
      try{await controller.show();}catch(e){console.warn('AdsGram ad unavailable',e);}
    }
    function setDirection(){document.documentElement.lang=state.lang;document.documentElement.dir=state.lang==='ar'?'rtl':'ltr';$('langBtn').textContent=state.lang==='ar'?'EN':'عربي';$('search').placeholder=tr('ابحث عن فيلم أو مسلسل...','Search movies and series...');$('moviesTab').textContent=tr('🎬 الأفلام','🎬 Movies');$('seriesTab').textContent=tr('📺 المسلسلات','📺 Series');}
    function renderCategories(){
      const cats=state.bootstrap?.categories||[];
      categories.innerHTML=[{id:'all',name:tr('الكل','All')},...cats.map(c=>({id:c.id,name:c.name}))].map(c=>'<button class="chip '+(state.category===c.id?'active':'')+'" data-cat="'+esc(c.id)+'">'+esc(c.name)+'</button>').join('');
      categories.querySelectorAll('[data-cat]').forEach(b=>b.addEventListener('click',()=>{state.category=b.dataset.cat;renderCategories();loadCatalog();}));
    }
    function renderCatalog(){
      if(!state.items.length){content.className='';content.innerHTML='<div class="empty">'+tr('لا توجد نتائج حالياً.','No results found.')+'</div>';return;}
      content.className='grid';content.innerHTML=state.items.map(item=>'<article class="card" data-id="'+esc(item.id)+'">'+(item.poster?'<img class="poster" src="'+esc(item.poster)+'" loading="lazy" onerror="this.outerHTML=\'<div class=placeholder>🎬</div>\'">':'<div class="placeholder">🎬</div>')+'<div class="cardtitle">'+esc(item.title)+'</div></article>').join('');
      content.querySelectorAll('[data-id]').forEach(card=>card.addEventListener('click',()=>openItem(card.dataset.id)));
    }
    async function loadCatalog(){
      content.className='';content.innerHTML='<div class="empty">'+tr('جارٍ التحميل...','Loading...')+'</div>';
      try{const q=$('search').value.trim();const d=await api('/api/miniapp/catalog?kind='+state.kind+'&category='+encodeURIComponent(state.category)+'&q='+encodeURIComponent(q));state.items=d.items||[];renderCatalog();}
      catch(e){if(e.status===403&&e.data?.needs_subscription){showJoin(e.data);return;}content.innerHTML='<div class="empty">'+esc(e.message)+'</div>';}
    }
    function showJoin(data){
      const links=data.channels||[];app.innerHTML='<div class="notice">'+tr('للاستخدام، اشترك في القنوات التالية ثم اضغط تحقق.','Join the required channels, then verify.')+links.map(c=>'<a class="joinlink" href="'+esc(c.url)+'" target="_blank">'+esc(c.name)+'</a>').join('')+'<button id="verify" class="action primary">'+tr('تحقق من الاشتراك','Verify subscription')+'</button></div>';
      $('verify').onclick=async()=>{try{const r=await api('/api/miniapp/verify',{method:'POST',body:'{}'});if(r.ok)location.reload();else showJoin(r);}catch(e){showJoin(e.data||{});}};
    }
    async function openItem(id){
      try{const d=await api('/api/miniapp/item?kind='+state.kind+'&id='+encodeURIComponent(id));state.detail=d.item;renderDetail();}
      catch(e){alert(e.message);}
    }
    function renderDetail(){
      const x=state.detail;if(!x)return;
      const poster=x.poster?'<img class="detailposter" src="'+esc(x.poster)+'">':'<div class="detailposter placeholder">🎬</div>';
      let groups='';
      if(state.kind==='movie')groups='<div class="group"><h3>'+tr('الأجزاء','Parts')+'</h3><div class="actions">'+(x.parts||[]).map(p=>'<button class="action primary" data-number="'+p.number+'">'+tr('الجزء ','Part ')+p.number+'</button>').join('')+'</div></div>';
      else groups=(x.seasons||[]).map(s=>'<div class="group"><h3>'+tr('الموسم ','Season ')+s.number+' · '+s.episodes_count+' '+tr('حلقة','episodes')+'</h3><div class="actions"><button class="action primary" data-number="'+s.number+'">'+tr('إرسال الموسم','Send season')+'</button></div></div>').join('');
      content.className='';content.innerHTML='<button id="back" class="back">'+tr('← رجوع','← Back')+'</button><section class="detail"><div class="detailhead">'+poster+'<div><div class="detailname">'+esc(x.title)+'</div><div class="muted">'+esc(x.category_name||'')+'</div></div></div><div class="story">'+esc(x.story||tr('لا توجد قصة متاحة.','No synopsis available.'))+'</div>'+groups+'</section>';
      $('back').onclick=()=>{state.detail=null;loadCatalog();};
      content.querySelectorAll('[data-number]').forEach(b=>b.addEventListener('click',()=>deliver(Number(b.dataset.number))));
    }
async function deliver(number){

  const payload={
    kind:state.kind,
    item_id:state.detail.id,
    number
  };

  try{

    let d=await api(
      '/api/miniapp/deliver',
      {
        method:'POST',
        body:JSON.stringify(payload)
      }
    );

    if(d.requires_reward){

      const accepted=confirm(
        tr(
          'لقد استخدمت طلبك المجاني الأول. شاهد إعلاناً كاملاً للحصول على هذا المحتوى؟',
          'Your first free request has been used. Watch a complete ad to get this content?'
        )
      );

      if(!accepted)return;

      await showRewarded(d.request_id);

      d=await api(
        '/api/miniapp/deliver',
        {
          method:'POST',
          body:JSON.stringify({
            ...payload,
            request_id:d.request_id
          })
        }
      );
    }

    alert(
      tr(
        'تم طلب إرسال المحتوى إلى محادثتك مع البوت.',
        'The content is being sent to your bot chat.'
      )
    );

    if(d.ad_after_delivery){
      await showInterstitial(true);
    }

  }catch(e){

    if(e.status===403&&e.data?.needs_subscription){
      showJoin(e.data);
    }else{
      alert(e.message);
    }
  }
}
    $('moviesTab').onclick=()=>{state.kind='movie';state.detail=null;$('moviesTab').classList.add('active');$('seriesTab').classList.remove('active');loadCatalog();};
    $('seriesTab').onclick=()=>{state.kind='series';state.detail=null;$('seriesTab').classList.add('active');$('moviesTab').classList.remove('active');loadCatalog();};
    $('langBtn').onclick=async()=>{state.lang=state.lang==='ar'?'en':'ar';setDirection();try{await api('/api/miniapp/language',{method:'POST',body:JSON.stringify({language:state.lang})});const d=await api('/api/miniapp/bootstrap');state.bootstrap=d;renderCategories();loadCatalog();}catch(e){console.warn(e);}};
    $('search').addEventListener('input',()=>{clearTimeout(state.searchTimer);state.searchTimer=setTimeout(loadCatalog,300);});
    async function start(){
      if(!initData){notice('افتح هذه الصفحة من داخل Telegram عبر زر تطبيق Legend HD.');return;}
      try{state.bootstrap=await api('/api/miniapp/bootstrap');state.lang=state.bootstrap.language||'ar';setDirection();initAds();renderCategories();loadCatalog();if(state.bootstrap.is_admin)$('footer').textContent=tr('إدارة المحتوى متاحة للأدمن عبر الأمر /manage في محادثة البوت.','Admin content management remains available with /manage in the bot chat.');}
      catch(e){if(e.status===403&&e.data?.needs_subscription)showJoin(e.data);else notice(e.message||'تعذر تشغيل التطبيق.');}
    }
    start();
  })();
  </script>
</body>
</html>"""


def validate_telegram_webapp_init_data(init_data, max_age_seconds=86400):
    """Validate Telegram Mini App initData using the bot token (never trust initDataUnsafe)."""
    if not init_data or not BOT_TOKEN:
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
        received_hash = pairs.pop("hash", None)
        if not received_hash:
            return None
        data_check_string = "\n".join(
            f"{key}={value}" for key, value in sorted(pairs.items())
        )
        secret_key = hmac.new(
            b"WebAppData",
            BOT_TOKEN.encode("utf-8"),
            hashlib.sha256
        ).digest()
        calculated_hash = hmac.new(
            secret_key,
            data_check_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(calculated_hash, received_hash):
            return None
        auth_date = int(pairs.get("auth_date", "0"))
        now = int(time.time())
        if auth_date <= 0 or auth_date > now + 60 or now - auth_date > max_age_seconds:
            return None
        user = json.loads(pairs.get("user", "{}"))
        if not isinstance(user, dict) or not user.get("id"):
            return None
        return user
    except Exception:
        return None


def miniapp_authenticated_user():
    raw = request.headers.get("X-Telegram-Init-Data", "")
    return validate_telegram_webapp_init_data(raw)


def miniapp_access_result(user_id):
    record = user_record(user_id)
    if record.get("blocked"):
        return {"ok": False, "blocked": True, "message": "هذا الحساب محظور من استخدام البوت."}
    if settings().get("require_subscription", True) and not is_admin(user_id):
        missing = run_bot_coroutine(missing_channels(user_id), timeout=25)
        if missing:
            return {
                "ok": False,
                "needs_subscription": True,
                "message": "اشترك في القنوات المطلوبة أولاً.",
                "channels": [
                    {"url": c.get("url", ""), "name": c.get("name", "قناة الاشتراك")}
                    for c in missing
                ]
            }
    return {"ok": True}


def miniapp_poster_url(file_id, user_id):
    if not file_id:
        return ""
    value = str(file_id)
    if value.startswith("https://"):
        return value
    expires = int(time.time()) + 3600
    message = f"{int(user_id)}:{value}:{expires}"
    signature = hmac.new(
        BOT_TOKEN.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return (
        "/api/miniapp/poster?file_id=" + quote(value, safe="")
        + "&user_id=" + str(int(user_id))
        + "&expires=" + str(expires)
        + "&sig=" + signature
    )


@app.get("/api/miniapp/poster")
def miniapp_poster():
    file_id = request.args.get("file_id", "")
    user_id = request.args.get("user_id", "")
    expires = request.args.get("expires", "")
    signature = request.args.get("sig", "")
    try:
        expires_int = int(expires)
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        abort(403)
    if not file_id or expires_int < int(time.time()) or expires_int > int(time.time()) + 3700:
        abort(403)
    message = f"{user_id_int}:{file_id}:{expires_int}"
    expected = hmac.new(
        BOT_TOKEN.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, signature):
        abort(403)
    try:
        meta_response = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
            params={"file_id": file_id},
            timeout=15
        )
        meta_response.raise_for_status()
        meta = meta_response.json()
        if not meta.get("ok") or not meta.get("result", {}).get("file_path"):
            abort(404)
        file_path = meta["result"]["file_path"]
        image_response = requests.get(
            f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}",
            timeout=25
        )
        image_response.raise_for_status()
        content_type = image_response.headers.get("Content-Type", "image/jpeg")
        if not content_type.startswith("image/"):
            abort(404)
        return Response(
            image_response.content,
            mimetype=content_type,
            headers={"Cache-Control": "private, max-age=1800", "X-Content-Type-Options": "nosniff"}
        )
    except Exception:
        log.exception("Could not proxy Mini App poster")
        abort(404)


def miniapp_item_summary(item_id, item, kind, lang, user_id):
    if kind == "movie":
        parts = sorted(item.get("parts", []), key=lambda p: int(p.get("number", 0)))
        return {
            "id": item_id,
            "title": item.get("title", ""),
            "category": item.get("category", ""),
            "category_name": category_name(item.get("category", ""), lang),
            "poster": miniapp_poster_url(item.get("poster"), user_id) if item.get("poster") else "",
            "story": item.get("story", ""),
            "parts": [{"number": int(p.get("number", 0))} for p in parts],
        }
    seasons = sorted(item.get("seasons", []), key=lambda s: int(s.get("number", 0)))
    return {
        "id": item_id,
        "title": item.get("title", ""),
        "category": item.get("category", ""),
        "category_name": category_name(item.get("category", ""), lang),
        "poster": miniapp_poster_url(item.get("poster"), user_id) if item.get("poster") else "",
        "story": item.get("story", ""),
        "seasons": [
            {
                "number": int(s.get("number", 0)),
                "episodes_count": len(s.get("episodes", [])),
            }
            for s in seasons
        ],
    }


@app.get(MINI_APP_PATH)
def miniapp_page():
    return MINI_APP_HTML, 200, {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store"}


@app.get("/api/miniapp/bootstrap")
def miniapp_bootstrap():
    tg_user = miniapp_authenticated_user()
    if not tg_user:
        return jsonify({"error": "invalid_telegram_init_data", "message": "افتح التطبيق من داخل Telegram."}), 401
    user_id = int(tg_user["id"])
    access = miniapp_access_result(user_id)
    if not access["ok"]:
        return jsonify(access), 403
    record = user_record(user_id)
    lang = record.get("language", DEFAULT_LANGUAGE)
    categories = [
        {"id": c.get("id", ""), "name": c.get("name", {}).get(lang) or c.get("name", {}).get("ar") or c.get("id", "")}
        for c in CATEGORIES
    ]
    return jsonify({
        "ok": True,
        "language": lang,
        "is_admin": is_admin(user_id),
        "categories": categories,
        "ads": {
            "enabled": bool(settings().get("ads_enabled", ADS_ENABLED_DEFAULT)),
            "interstitial_block_ids": ADSGRAM_INTERSTITIAL_BLOCK_IDS,
            "rewarded_block_id": ADSGRAM_REWARDED_BLOCK_ID,
            "interstitial_interval": MINI_APP_INTERSTITIAL_INTERVAL,
        }
    })
@app.get("/api/miniapp/catalog")
def miniapp_catalog():
    tg_user = miniapp_authenticated_user()
    if not tg_user:
        return jsonify({"error": "invalid_telegram_init_data"}), 401
    user_id = int(tg_user["id"])
    access = miniapp_access_result(user_id)
    if not access["ok"]:
        return jsonify(access), 403
    lang = user_record(user_id).get("language", DEFAULT_LANGUAGE)
    kind = request.args.get("kind", "movie")
    category = request.args.get("category", "all")
    query = request.args.get("q", "").strip()
    if kind not in ("movie", "series"):
        return jsonify({"error": "invalid_kind"}), 400
    collection = MOVIES if kind == "movie" else SERIES
    entries = list(collection.items())
    if category != "all":
        entries = [(i, x) for i, x in entries if x.get("category") == category]
    if query:
        scored = [(title_score(query, x.get("title", ""), x.get("aliases", [])), i, x) for i, x in entries]
        entries = [(i, x) for score, i, x in scored if score >= FUZZY_CUTOFF]
        entries.sort(key=lambda pair: title_score(query, pair[1].get("title", ""), pair[1].get("aliases", [])), reverse=True)
    else:
        entries.sort(key=lambda pair: pair[1].get("title", "").casefold())
    return jsonify({
        "ok": True,
        "items": [
            {
                "id": item_id,
                "title": item.get("title", ""),
                "poster": miniapp_poster_url(item.get("poster"), user_id) if item.get("poster") else "",
                "category_name": category_name(item.get("category", ""), lang),
            }
            for item_id, item in entries[:100]
        ]
    })


@app.get("/api/miniapp/item")
def miniapp_item():
    tg_user = miniapp_authenticated_user()
    if not tg_user:
        return jsonify({"error": "invalid_telegram_init_data"}), 401
    access = miniapp_access_result(int(tg_user["id"]))
    if not access["ok"]:
        return jsonify(access), 403
    kind = request.args.get("kind", "movie")
    item_id = request.args.get("id", "")
    if kind not in ("movie", "series"):
        return jsonify({"error": "invalid_kind"}), 400
    collection = MOVIES if kind == "movie" else SERIES
    item = collection.get(item_id)
    if not item:
        return jsonify({"error": "not_found", "message": "المحتوى غير موجود."}), 404
    lang = user_record(int(tg_user["id"])).get("language", DEFAULT_LANGUAGE)
    result = miniapp_item_summary(item_id, item, kind, lang, int(tg_user["id"]))
    if kind == "movie":
        result["parts"] = sorted(
            [{"number": int(p.get("number", 0))} for p in item.get("parts", [])],
            key=lambda p: p["number"]
        )
    else:
        result["seasons"] = sorted(result["seasons"], key=lambda s: s["number"])
    return jsonify({"ok": True, "item": result})


@app.post("/api/miniapp/verify")
def miniapp_verify():
    tg_user = miniapp_authenticated_user()
    if not tg_user:
        return jsonify({"error": "invalid_telegram_init_data"}), 401
    access = miniapp_access_result(int(tg_user["id"]))
    return jsonify(access), (200 if access["ok"] else 403)


@app.post("/api/miniapp/language")
def miniapp_language():
    tg_user = miniapp_authenticated_user()
    if not tg_user:
        return jsonify({"error": "invalid_telegram_init_data"}), 401
    data = request.get_json(silent=True) or {}
    lang = data.get("language")
    if lang not in LANGUAGES:
        return jsonify({"error": "invalid_language"}), 400
    record = user_record(int(tg_user["id"]))
    record["language"] = lang
    record["language_selected"] = True
    save("users")
    return jsonify({"ok": True, "language": lang})


@app.post("/api/miniapp/deliver")
def miniapp_deliver():

    tg_user = miniapp_authenticated_user()

    if not tg_user:
        return jsonify({"error": "invalid_telegram_init_data"}), 401

    user_id = int(tg_user["id"])

    access = miniapp_access_result(user_id)

    if not access["ok"]:
        return jsonify(access), 403

    data = request.get_json(silent=True) or {}

    kind = data.get("kind")
    item_id = str(data.get("item_id", ""))
    request_id = str(data.get("request_id", "")).strip()

    try:
        number = int(data.get("number", 0))
    except (TypeError, ValueError):
        number = 0

    if number <= 0 or kind not in ("movie", "series"):
        return jsonify({"error": "invalid_request"}), 400

    collection = MOVIES if kind == "movie" else SERIES
    item = collection.get(item_id)

    if not item:
        return jsonify({"error": "not_found"}), 404

    snapshot = copy.deepcopy(item)

    if kind == "movie":
        valid_number = any(
            int(p.get("number", 0)) == number
            for p in snapshot.get("parts", [])
        )
    else:
        valid_number = any(
            int(s.get("number", 0)) == number
            for s in snapshot.get("seasons", [])
        )

    if not valid_number:
        return jsonify({"error": "content_unit_not_found"}), 404

    ads_enabled = bool(
        settings().get("ads_enabled", ADS_ENABLED_DEFAULT)
    )

    now = time.time()

    # Claim an already verified Rewarded request
    if request_id:

        with DB_LOCK:

            connection = db_connect()
            connection.execute("BEGIN IMMEDIATE")

            row = connection.execute(
                """
                SELECT * FROM miniapp_reward_requests
                WHERE request_id=? AND user_id=?
                """,
                (request_id, user_id)
            ).fetchone()

            if (
                not row
                or row["status"] != "eligible"
                or row["expires_at"] < now
                or row["kind"] != kind
                or row["item_id"] != item_id
                or int(row["number"]) != number
            ):

                connection.rollback()
                connection.close()

                return jsonify({
                    "error": "reward_not_verified",
                    "message": "لم يتم تأكيد مكافأة الإعلان لهذا الطلب."
                }), 403

            updated = connection.execute(
                """
                UPDATE miniapp_reward_requests
                SET status='used', used_at=?
                WHERE request_id=? AND user_id=? AND status='eligible'
                """,
                (now, request_id, user_id)
            ).rowcount

            if updated != 1:
                connection.rollback()
                connection.close()

                return jsonify({
                    "error": "reward_already_used",
                    "message": "تم استخدام هذه المكافأة من قبل."
                }), 409

            connection.commit()
            connection.close()

    # First free request, then Rewarded is required
    elif ads_enabled:

        with DB_LOCK:

            connection = db_connect()
            connection.execute("BEGIN IMMEDIATE")

            connection.execute(
                """
                INSERT OR IGNORE INTO miniapp_user_access
                (user_id, free_used)
                VALUES (?, 0)
                """,
                (user_id,)
            )

            access_row = connection.execute(
                """
                SELECT free_used FROM miniapp_user_access
                WHERE user_id=?
                """,
                (user_id,)
            ).fetchone()

            if not access_row["free_used"]:

                connection.execute(
                    """
                    UPDATE miniapp_user_access
                    SET free_used=1
                    WHERE user_id=? AND free_used=0
                    """,
                    (user_id,)
                )

                connection.commit()
                connection.close()

            else:

                if (
                    not ADSGRAM_REWARDED_BLOCK_ID
                    or not ADSGRAM_REWARD_WEBHOOK_SECRET
                ):

                    connection.rollback()
                    connection.close()

                    return jsonify({
                        "error": "rewarded_not_configured",
                        "message": "إعلانات المكافأة غير مهيأة. يرجى إبلاغ الإدارة."
                    }), 503

                connection.execute(
                    """
                    UPDATE miniapp_reward_requests
                    SET status='expired'
                    WHERE user_id=?
                    AND status='pending'
                    AND expires_at<?
                    """,
                    (user_id, now)
                )

                pending = connection.execute(
                    """
                    SELECT request_id, kind, item_id, number
                    FROM miniapp_reward_requests
                    WHERE user_id=?
                    AND status='pending'
                    AND expires_at>=?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (user_id, now)
                ).fetchone()

                if pending:

                    connection.commit()
                    connection.close()

                    if (
                        pending["kind"] == kind
                        and pending["item_id"] == item_id
                        and int(pending["number"]) == number
                    ):

                        return jsonify({
                            "ok": True,
                            "requires_reward": True,
                            "request_id": pending["request_id"]
                        })

                    return jsonify({
                        "error": "reward_request_in_progress",
                        "message": "لديك طلب إعلان مكافأة قيد التنفيذ. أكمله أولاً أو انتظر انتهاء صلاحيته."
                    }), 409

                reward_request_id = uuid.uuid4().hex

                connection.execute(
                    """
                    INSERT INTO miniapp_reward_requests
                    (
                        request_id, user_id, kind, item_id,
                        number, status, created_at, expires_at
                    )
                    VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (
                        reward_request_id,
                        user_id,
                        kind,
                        item_id,
                        number,
                        now,
                        now + 300
                    )
                )

                connection.commit()
                connection.close()

                return jsonify({
                    "ok": True,
                    "requires_reward": True,
                    "request_id": reward_request_id
                })

    # Ads disabled by admin: no Rewarded restriction
    if kind == "movie":

        run_bot_coroutine(
            send_movie_part(user_id, user_id, snapshot, number),
            timeout=75
        )

    else:

        async def schedule_season_delivery():

            task = asyncio.create_task(
                send_season(user_id, user_id, snapshot, number)
            )

            keep_task(task)
            return True

        run_bot_coroutine(
            schedule_season_delivery(),
            timeout=10
        )

    return jsonify({
        "ok": True,
        "message": "delivery_started",
        "ad_after_delivery": (
            ads_enabled and bool(ADSGRAM_INTERSTITIAL_BLOCK_IDS)
        )
    })


@app.get("/adsgram/reward")
def adsgram_reward_callback():

    supplied_secret = request.args.get("key", "")

    if (
        not ADSGRAM_REWARD_WEBHOOK_SECRET
        or not hmac.compare_digest(
            supplied_secret,
            ADSGRAM_REWARD_WEBHOOK_SECRET
        )
    ):
        return jsonify({"ok": False}), 403

    raw_user_id = (
        request.args.get("userid")
        or request.args.get("userId")
        or request.args.get("telegramId")
        or ""
    )

    try:
        user_id = int(raw_user_id)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "invalid_user_id"}), 400

    now = time.time()

    with DB_LOCK:

        connection = db_connect()
        connection.execute("BEGIN IMMEDIATE")

        guard = connection.execute(
            """
            SELECT last_callback_at
            FROM miniapp_reward_callback_guard
            WHERE user_id=?
            """,
            (user_id,)
        ).fetchone()

        if guard and now - guard["last_callback_at"] < 30:

            connection.commit()
            connection.close()

            return jsonify({
                "ok": True,
                "duplicate_ignored": True
            })

        row = connection.execute(
            """
            SELECT request_id
            FROM miniapp_reward_requests
            WHERE user_id=?
            AND status='pending'
            AND expires_at>=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_id, now)
        ).fetchone()

        if row:

            connection.execute(
                """
                UPDATE miniapp_reward_requests
                SET status='eligible', verified_at=?
                WHERE request_id=? AND status='pending'
                """,
                (now, row["request_id"])
            )

            connection.execute(
                """
                INSERT INTO miniapp_reward_callback_guard
                (user_id, last_callback_at)
                VALUES (?, ?)
                ON CONFLICT(user_id)
                DO UPDATE SET last_callback_at=excluded.last_callback_at
                """,
                (user_id, now)
            )

        connection.commit()
        connection.close()

    return jsonify({
        "ok": True,
        "reward_confirmed": bool(row)
    })


@app.get("/api/miniapp/reward/status")
def miniapp_reward_status():

    tg_user = miniapp_authenticated_user()

    if not tg_user:
        return jsonify({"error": "invalid_telegram_init_data"}), 401

    user_id = int(tg_user["id"])

    request_id = request.args.get("request_id", "").strip()

    if not request_id:
        return jsonify({"error": "missing_request_id"}), 400

    now = time.time()

    with DB_LOCK:

        connection = db_connect()

        row = connection.execute(
            """
            SELECT status, expires_at
            FROM miniapp_reward_requests
            WHERE request_id=? AND user_id=?
            """,
            (request_id, user_id)
        ).fetchone()

        if row and row["status"] == "pending" and row["expires_at"] < now:

            connection.execute(
                """
                UPDATE miniapp_reward_requests
                SET status='expired'
                WHERE request_id=? AND status='pending'
                """,
                (request_id,)
            )

            connection.commit()
            status = "expired"

        else:

            status = row["status"] if row else "not_found"

        connection.close()

    return jsonify({
        "ok": True,
        "status": status
    })


# ==========================================================
# 40) FLASK WEBHOOK ROUTES
# ==========================================================

@app.get("/")
def health():
    return jsonify({
        "ok": True,
        "service": BOT_NAME,
        "status": "running"
    })


@app.get("/health")
def health_check():
    return jsonify({
        "ok": True,
        "queue": "active"
    })


@app.post(WEBHOOK_PATH)
def telegram_webhook():
    if (
        WEBHOOK_SECRET_TOKEN
        and request.headers.get(
            "X-Telegram-Bot-Api-Secret-Token"
        ) != WEBHOOK_SECRET_TOKEN
    ):
        abort(403)

    try:
        payload = request.get_json(force=True)

        if not isinstance(payload, dict):
            return jsonify({"ok": False}), 400

        update_id = int(payload["update_id"])

        enqueue_update(
            update_id,
            payload
        )

        # نؤكد الاستلام بسرعة، والتنفيذ يحدث في الخلفية.
        return jsonify({
            "ok": True,
            "queued": True
        }), 200

    except Exception:
        log.exception("Could not enqueue Telegram update.")

        return jsonify({
            "ok": False,
            "error": "queue_failed"
        }), 500


# ==========================================================
# 41) WEBHOOK STARTUP
# ==========================================================

def run_bot_coroutine(coro, timeout=40):
    future = asyncio.run_coroutine_threadsafe(
        coro,
        BOT_LOOP
    )

    return future.result(timeout=timeout)


def initialize_webhook():
    if not RENDER_URL.startswith("https://"):
        log.error("Invalid Render HTTPS URL.")
        return

    async def setup():
        await bot.set_webhook(
            url=WEBHOOK_URL,
            secret_token=WEBHOOK_SECRET_TOKEN,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=False,
            max_connections=20
        )
        try:
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="Legend HD",
                    web_app=WebAppInfo(url=MINI_APP_URL)
                )
            )
        except TelegramAPIError:
            log.exception("Could not configure Telegram Mini App menu button.")

    try:
        run_bot_coroutine(setup(), timeout=40)
        log.info("Telegram webhook configured.")

    except Exception:
        log.exception("Webhook setup failed.")


# ==========================================================
# 42) START BACKGROUND WORKERS
# ==========================================================

reset_processing_updates()

asyncio.run_coroutine_threadsafe(
    update_worker(),
    BOT_LOOP
)

initialize_webhook()


# ==========================================================
# 43) CONFIGURATION WARNINGS
# ==========================================================

if not ADSGRAM_INTERSTITIAL_BLOCK_IDS:
    log.warning(
        "No AdsGram Interstitial Block IDs configured. Mini App ads will remain disabled."
    )

if STORAGE_CHANNEL_ID is None:
    log.warning(
        "STORAGE_CHANNEL_ID is not configured. "
        "Strict movie storage-origin verification is disabled."
    )

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=PORT
    )
