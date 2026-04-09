#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import html
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

import psycopg
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
from urllib3.util.retry import Retry


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# =========================
# ENV
# =========================

PORT = int(os.getenv("PORT", "10000"))
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or "0")
ENABLE_LINKEDIN = os.getenv("ENABLE_LINKEDIN", "0") == "1"
MAX_RESULTS_TO_SHOW = int(os.getenv("MAX_RESULTS_TO_SHOW", "1000"))
SHOW_COMPANY = os.getenv("SHOW_COMPANY", "0") == "1"

LANG_MENU, MAIN_MENU, OLD_JOBS_MENU = range(3)


# =========================
# CONSTANTS
# =========================

SITE_LABELS = {
    "jobsearch": "JobSearch",
    "busy": "Busy.az",
    "glorri": "Glorri",
    "azvak": "AzVak",
    "hellojob": "HelloJob",
    "linkedin": "LinkedIn",
}

KEYWORDS = [
    "юрист",
    "юрисконсульт",
    "hüquqşünas",
    "huquqsunas",
    "aparıcı hüquqşünas",
    "baş hüquqşünas",
    "kiçik hüquqşünas",
    "korporativ hüquqşünas",
    "korporativ müqavilələr üzrə hüquqşünas",
    "hüquq üzrə mütəxəssis",
    "hüquq məsləhətçisi",
    "hüquq departamenti",
    "hüquqi təhlil",
    "legal counsel",
    "legal specialist",
    "senior legal specialist",
    "lawyer",
    "corporate lawyer",
    "banking lawyer",
    "finance lawyer",
    "banking & finance lawyer",
    "compliance specialist",
    "legal officer",
    "legal associate",
    "junior lawyer",
    "senior lawyer",
    "attorney",
    "paralegal",
    "vəkil",
    "legal",
    "compliance",
    "hüquq",
    "huquq",
]
SORTED_KEYWORDS = sorted(KEYWORDS, key=len, reverse=True)

SITE_URLS = {
    "jobsearch": "https://classic.jobsearch.az/vacancies?category=1375",
    "busy_professions": [
        "https://cdn.busy.az/professions/huquqsunas",
        "https://cdn.busy.az/professions/huquq-meslehetcisi",
        "https://cdn.busy.az/professions/huquq-sobesinin-mutexessisi",
        "https://cdn.busy.az/professions/lawyer",
        "https://cdn.busy.az/professions/bas-huquqsunas",
        "https://cdn.busy.az/professions/huquqsunas-komekcisi",
        "https://cdn.busy.az/professions/legal-specialist",
        "https://cdn.busy.az/professions/legal-counsel",
        "https://cdn.busy.az/professions/compliance-specialist",
        "https://cdn.busy.az/professions/corporate-lawyer",
    ],
    "glorri": [
        "https://jobs.glorri.com/?jobFunctions=legal-services",
        "https://jobs.glorri.com/en?jobFunctions=legal-services",
        "https://jobs.glorri.az/?jobFunctions=legal-services",
    ],
    "azvak": [
        "https://azvak.az/vezifeler/huquqsunas/134",
        "https://azvak.az/",
    ],
    "hellojob": "https://www.hellojob.az/is-elanlari/huquq",
    "linkedin": [
        "https://az.linkedin.com/jobs/legal-jobs?countryRedirected=1",
        "https://az.linkedin.com/jobs/law-jobs?countryRedirected=1",
        "https://az.linkedin.com/jobs/legal-compliance-jobs?countryRedirected=1",
        "https://az.linkedin.com/jobs/legal-manager-jobs?countryRedirected=1",
        "https://az.linkedin.com/jobs/legal-executive-jobs?countryRedirected=1",
        "https://az.linkedin.com/jobs/law-firm-jobs?countryRedirected=1",
    ],
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "az,en-US;q=0.9,en;q=0.8,ru;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

TEXTS = {
    "ru": {
        "choose_language": "Choose a language:",
        "welcome": (
            "Добро пожаловать в бот для поиска вакансий юриста.\n\n"
            "База обновляется автоматически по расписанию.\n"
            "Если бот спал на бесплатном Render, первый ответ после долгой паузы может занять до минуты.\n\n"
            "В архиве показываются вакансии за последние 30 дней."
        ),
        "help": (
            "Помощь:\n\n"
            "• Кнопка поиска просто показывает уже собранную базу — без долгого парсинга на твоём сообщении.\n"
            "• Архив хранит вакансии за последние 30 дней.\n"
            "• Если бот спал на бесплатном Render, первое пробуждение может занять до минуты."
        ),
        "bot_awake": "Бот проснулся. Нажмите нужную кнопку ниже.",
        "archive_header": "Показываю архив вакансий за последние 30 дней.",
        "last_refresh": "Последнее обновление базы: {value}",
        "last_refresh_unknown": "Последнее обновление базы: пока неизвестно",
        "old_jobs_prompt": "Выбери сайт, чтобы открыть архив вакансий за последние 30 дней.",
        "empty_recent": "За последние 30 дней вакансий пока не найдено.",
        "empty_site": "По сайту {site} вакансий за последние 30 дней пока нет.",
        "main_menu": "Главное меню:",
        "cancelled": "Действие отменено. Ты в главном меню.",
        "press_button": "Нажми нужную кнопку в меню.",
        "pick_site_button": "Выбери сайт кнопкой.",
        "site_label": "Сайт",
        "date_label": "Дата",
        "company_label": "Компания",
        "start_btn": "Start",
        "search_btn": "Искать вакансии",
        "change_lang_btn": "Сменить язык",
        "old_btn": "Старые вакансии",
        "help_btn": "Помощь",
        "cancel_btn": "Отмена",
        "back_btn": "Назад",
        "lang_btn_ru": "🇷🇺 Русский",
        "lang_btn_az": "🇦🇿 Azərbaycan",
        "lang_btn_en": "🇬🇧 English",
        "stats_text": (
            "Статистика:\n"
            "Пользователей: {users}\n"
            "Вакансий за 30 дней: {vacancies}\n"
            "Последнее обновление: {last_refresh}"
        ),
        "not_admin": "Эта команда доступна только администратору.",
    },
    "az": {
        "choose_language": "Choose a language:",
        "welcome": (
            "Hüquqşünas vakansiyalarını axtaran bota xoş gəlmisiniz.\n\n"
            "Baza avtomatik cədvəl üzrə yenilənir.\n"
            "Bot pulsuz Render-də yatmış olarsa, uzun fasilədən sonrakı ilk cavab 1 dəqiqəyə qədər çəkə bilər.\n\n"
            "Arxivdə son 30 günün vakansiyaları göstərilir."
        ),
        "help": (
            "Kömək:\n\n"
            "• Axtarış düyməsi hazır bazanı göstərir — sizin mesajda ağır parsinq işə düşmür.\n"
            "• Arxiv son 30 günün vakansiyalarını saxlayır.\n"
            "• Bot pulsuz Render-də yatıbsa, ilk oyanış 1 dəqiqəyə qədər çəkə bilər."
        ),
        "bot_awake": "Bot oyandı. Aşağıdakı uyğun düyməni seçin.",
        "archive_header": "Son 30 günün vakansiya arxivi göstərilir.",
        "last_refresh": "Bazanın son yenilənməsi: {value}",
        "last_refresh_unknown": "Bazanın son yenilənməsi: hələ məlum deyil",
        "old_jobs_prompt": "Son 30 gün üzrə vakansiya arxivini açmaq üçün saytı seçin.",
        "empty_recent": "Son 30 gün üzrə vakansiya tapılmadı.",
        "empty_site": "{site} saytı üzrə son 30 gündə vakansiya yoxdur.",
        "main_menu": "Əsas menyu:",
        "cancelled": "Əməliyyat ləğv edildi. Siz əsas menyudasınız.",
        "press_button": "Menyudan uyğun düyməni seçin.",
        "pick_site_button": "Saytı düymə ilə seçin.",
        "site_label": "Sayt",
        "date_label": "Tarix",
        "company_label": "Şirkət",
        "start_btn": "Start",
        "search_btn": "Vakansiyaları axtar",
        "change_lang_btn": "Dili dəyiş",
        "old_btn": "Köhnə vakansiyalar",
        "help_btn": "Kömək",
        "cancel_btn": "Ləğv et",
        "back_btn": "Geri",
        "lang_btn_ru": "🇷🇺 Русский",
        "lang_btn_az": "🇦🇿 Azərbaycan",
        "lang_btn_en": "🇬🇧 English",
        "stats_text": (
            "Statistika:\n"
            "İstifadəçi sayı: {users}\n"
            "30 gün üzrə vakansiya sayı: {vacancies}\n"
            "Son yenilənmə: {last_refresh}"
        ),
        "not_admin": "Bu komanda yalnız administrator üçün açıqdır.",
    },
    "en": {
        "choose_language": "Choose a language:",
        "welcome": (
            "Welcome to the lawyer vacancy search bot.\n\n"
            "The database is updated automatically on a schedule.\n"
            "If the bot was sleeping on free Render, the first reply after a long pause can take up to a minute.\n\n"
            "The archive shows vacancies from the last 30 days."
        ),
        "help": (
            "Help:\n\n"
            "• The search button shows the ready-made database — there is no heavy parsing on the user's message.\n"
            "• The archive stores vacancies from the last 30 days.\n"
            "• If the bot was sleeping on free Render, the first wake-up can take up to a minute."
        ),
        "bot_awake": "The bot is awake. Press the needed button below.",
        "archive_header": "Showing the vacancy archive for the last 30 days.",
        "last_refresh": "Last database update: {value}",
        "last_refresh_unknown": "Last database update: unknown yet",
        "old_jobs_prompt": "Choose a website to open the vacancy archive for the last 30 days.",
        "empty_recent": "No vacancies found for the last 30 days.",
        "empty_site": "No vacancies found for {site} in the last 30 days.",
        "main_menu": "Main menu:",
        "cancelled": "Action cancelled. You are in the main menu.",
        "press_button": "Press the needed button in the menu.",
        "pick_site_button": "Choose a website using the button.",
        "site_label": "Site",
        "date_label": "Date",
        "company_label": "Company",
        "start_btn": "Start",
        "search_btn": "Search vacancies",
        "change_lang_btn": "Change language",
        "old_btn": "Old vacancies",
        "help_btn": "Help",
        "cancel_btn": "Cancel",
        "back_btn": "Back",
        "lang_btn_ru": "🇷🇺 Русский",
        "lang_btn_az": "🇦🇿 Azərbaycan",
        "lang_btn_en": "🇬🇧 English",
        "stats_text": (
            "Stats:\n"
            "Users: {users}\n"
            "Vacancies for the last 30 days: {vacancies}\n"
            "Last refresh: {last_refresh}"
        ),
        "not_admin": "This command is available only to the admin.",
    },
}


# =========================
# HTTP session
# =========================

retry = Retry(
    total=2,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
SESSION = requests.Session()
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)
SESSION.headers.update(HEADERS)


# =========================
# MODELS
# =========================

@dataclass
class Vacancy:
    site: str
    title: str
    url: str
    published_date: Optional[date]
    company: Optional[str] = None

    @property
    def unique_hash(self) -> str:
        raw = build_vacancy_storage_key(
            site=self.site,
            title=self.title,
            url=self.url,
            company=self.company,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# =========================
# DB
# =========================

def require_database() -> None:
    if not DATABASE_URL:
        raise ValueError("Не найдена переменная DATABASE_URL")


def require_bot_env() -> None:
    if not TOKEN:
        raise ValueError("Не найдена переменная BOT_TOKEN")
    if not RENDER_EXTERNAL_URL:
        raise ValueError("Не найдена переменная RENDER_EXTERNAL_URL")


def get_connection():
    require_database()
    return psycopg.connect(DATABASE_URL)


def init_db() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    site TEXT NOT NULL,
                    title TEXT NOT NULL,
                    company TEXT,
                    url TEXT NOT NULL,
                    published_date DATE,
                    found_date DATE NOT NULL,
                    unique_hash TEXT NOT NULL UNIQUE
                )
                """
            )
            cur.execute("ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS company TEXT")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_vacancies_recent
                ON vacancies (site, found_date DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_vacancies_published
                ON vacancies (published_date DESC)
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_users (
                    user_id BIGINT PRIMARY KEY,
                    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                "ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS first_seen TIMESTAMPTZ DEFAULT NOW()"
            )
            cur.execute(
                """
                UPDATE bot_users
                SET first_seen = COALESCE(first_seen, NOW())
                WHERE first_seen IS NULL
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        conn.commit()


def save_user(user_id: Optional[int]) -> None:
    if not user_id:
        return
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bot_users (user_id, first_seen)
                    VALUES (%s, NOW())
                    ON CONFLICT (user_id) DO NOTHING
                    """,
                    (user_id,),
                )
            conn.commit()
    except Exception as exc:
        logger.warning("save_user skipped: %s", exc)


def count_users() -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bot_users")
            row = cur.fetchone()
            return int(row[0] or 0)


def set_meta(key: str, value: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_meta (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """,
                (key, value),
            )
        conn.commit()


def get_meta(key: str) -> Optional[str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM bot_meta WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None


def cleanup_old_vacancies() -> None:
    border = date.today() - timedelta(days=30)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM vacancies WHERE COALESCE(published_date, found_date) < %s",
                (border,),
            )
        conn.commit()


def cleanup_duplicate_vacancies() -> None:
    border = date.today() - timedelta(days=30)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, site, title, company, url, published_date, found_date
                FROM vacancies
                WHERE COALESCE(published_date, found_date) >= %s
                ORDER BY id DESC
                """,
                (border,),
            )
            rows = cur.fetchall()

            seen = set()
            ids_to_delete = []

            for row_id, site, title, company, url, published_date, found_date in rows:
                key = build_vacancy_storage_key(site, title, url, company)
                if key in seen:
                    ids_to_delete.append(row_id)
                    continue
                seen.add(key)

            if ids_to_delete:
                cur.execute("DELETE FROM vacancies WHERE id = ANY(%s)", (ids_to_delete,))
        conn.commit()


def save_vacancies(vacancies: List[Vacancy]) -> int:
    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for v in vacancies:
                cur.execute(
                    """
                    INSERT INTO vacancies (site, title, company, url, published_date, found_date, unique_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (unique_hash) DO NOTHING
                    """,
                    (
                        v.site,
                        v.title,
                        v.company,
                        v.url,
                        v.published_date,
                        date.today(),
                        v.unique_hash,
                    ),
                )
                if cur.rowcount > 0:
                    inserted += 1
        conn.commit()
    return inserted


def get_recent_vacancies(limit: int = 1000) -> List[Dict]:
    border = date.today() - timedelta(days=30)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT site, title, company, url, published_date, found_date
                FROM vacancies
                WHERE COALESCE(published_date, found_date) >= %s
                ORDER BY COALESCE(published_date, found_date) DESC, id DESC
                LIMIT %s
                """,
                (border, limit),
            )
            rows = cur.fetchall()
    return deduplicate_vacancy_rows(rows)


def get_recent_vacancies_by_site(site: str, limit: int = 1000) -> List[Dict]:
    border = date.today() - timedelta(days=30)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT site, title, company, url, published_date, found_date
                FROM vacancies
                WHERE site = %s
                  AND COALESCE(published_date, found_date) >= %s
                ORDER BY COALESCE(published_date, found_date) DESC, id DESC
                LIMIT %s
                """,
                (site, border, limit),
            )
            rows = cur.fetchall()
    return deduplicate_vacancy_rows(rows)


def count_recent_vacancies() -> int:
    border = date.today() - timedelta(days=30)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM vacancies
                WHERE COALESCE(published_date, found_date) >= %s
                """,
                (border,),
            )
            row = cur.fetchone()
            return int(row[0] or 0)


# =========================
# TEXT / DATE HELPERS
# =========================

def normalize_text(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def clean_title(text: Optional[str]) -> str:
    text = (text or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def is_legal_vacancy(title: str) -> bool:
    t = normalize_text(title)
    return any(keyword in t for keyword in KEYWORDS)


def looks_like_noise(title: str) -> bool:
    t = normalize_text(title)
    if not t:
        return True

    noise = {
        "haqqımızda",
        "xidmətlər",
        "əlaqə",
        "ana səhifə",
        "vakansiyalar",
        "şirkətlər",
        "vəzifələr",
        "hamısı",
        "hüquq",
        "müraciət et",
        "elan yerləşdir",
        "axtar",
        "sıfırla",
        "help",
        "latest vacancies",
        "kateqoriyalar",
        "sənaye",
        "seçilmiş elanlar",
        "uyğun iş elanları",
        "işə aid seçimlər",
        "vakansiya axtarışı",
        "seç",
        "sil",
        "tam iş günü",
        "razılaşma yolu ilə",
        "full time",
        "part time",
        "internship",
        "remote",
        "hybrid",
        "all vacancies",
        "all jobs",
        "iş elanları",
        "jobs",
        "job",
        "vacancy",
        "vakansiya",
    }
    return t in noise or len(t) < 3


def month_name_to_number(month_name: str) -> Optional[int]:
    month_name = month_name.lower().strip()
    months = {
        "yan": 1, "yanvar": 1,
        "fev": 2, "fevral": 2,
        "mar": 3, "mart": 3,
        "apr": 4, "aprel": 4,
        "may": 5,
        "iyn": 6, "iyun": 6,
        "iyl": 7, "iyul": 7,
        "avq": 8, "avqust": 8,
        "sen": 9, "sentyabr": 9,
        "okt": 10, "oktyabr": 10,
        "noy": 11, "noyabr": 11,
        "dek": 12, "dekabr": 12,
        "jan": 1, "feb": 2, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10,
        "nov": 11, "dec": 12,
    }
    return months.get(month_name)


def parse_relative_days(raw: str) -> Optional[date]:
    raw = raw.lower()
    today = date.today()

    patterns = [
        (r"(\d+)\s*gün", 1),
        (r"(\d+)\s*day", 1),
        (r"(\d+)\s*days", 1),
        (r"(\d+)\s*дн", 1),
        (r"(\d+)\s*week", 7),
        (r"(\d+)\s*weeks", 7),
        (r"(\d+)\s*month", 30),
        (r"(\d+)\s*months", 30),
        (r"(\d+)\s*həftə", 7),
        (r"(\d+)\s*ay", 30),
        (r"(\d+)\s*нед", 7),
        (r"(\d+)\s*мес", 30),
    ]

    for pattern, factor in patterns:
        m = re.search(pattern, raw)
        if m:
            return today - timedelta(days=int(m.group(1)) * factor)

    if "today" in raw or raw == "today" or "bu gün" in raw or "bugün" in raw or "сегодня" in raw:
        return today
    if "yesterday" in raw or raw == "yesterday" or "dünən" in raw or "вчера" in raw:
        return today - timedelta(days=1)

    return None


def parse_date_loose(text: Optional[str]) -> Optional[date]:
    if not text:
        return None

    raw = clean_title(text).lower()
    today = date.today()

    rel = parse_relative_days(raw)
    if rel:
        return rel

    m = re.search(r"(\d{2})[-./](\d{2})[-./](\d{4})", raw)
    if m:
        day_num, month_num, year_num = map(int, m.groups())
        try:
            return date(year_num, month_num, day_num)
        except ValueError:
            return None

    m = re.search(r"(\d{4})[-./](\d{2})[-./](\d{2})", raw)
    if m:
        year_num, month_num, day_num = map(int, m.groups())
        try:
            return date(year_num, month_num, day_num)
        except ValueError:
            return None

    m = re.search(r"([a-z]{3})\s+(\d{1,2}),\s*(\d{4})", raw)
    if m:
        month_num = month_name_to_number(m.group(1))
        day_num = int(m.group(2))
        year_num = int(m.group(3))
        if month_num:
            try:
                return date(year_num, month_num, day_num)
            except ValueError:
                return None

    m = re.search(r"(\d{1,2})\s+([a-zəğıöşçü]+)\s+(\d{4})", raw)
    if m:
        day_num = int(m.group(1))
        month_num = month_name_to_number(m.group(2))
        year_num = int(m.group(3))
        if month_num:
            try:
                return date(year_num, month_num, day_num)
            except ValueError:
                return None

    m = re.search(r"(\d{1,2})\s+([a-zəğıöşçü]+)", raw)
    if m:
        day_num = int(m.group(1))
        month_num = month_name_to_number(m.group(2))
        if month_num:
            try:
                guessed = date(today.year, month_num, day_num)
                if guessed > today:
                    guessed = date(today.year - 1, month_num, day_num)
                return guessed
            except ValueError:
                return None

    return None


def extract_dates_from_text(text: Optional[str]) -> Optional[date]:
    return parse_date_loose(clean_title(text))


def is_fresh_enough(vacancy_date: Optional[date]) -> bool:
    if vacancy_date is None:
        return True
    return vacancy_date >= date.today() - timedelta(days=30)


def remove_known_meta(text: str) -> str:
    text = clean_title(text)

    junk_patterns = [
        r"\bMüraciət et\b",
        r"\bApply\b",
        r"\bПодать заявку\b",
        r"\bActively Hiring\b",
        r"\bBe an early applicant\b",
    ]
    for pattern in junk_patterns:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)

    age_patterns = [
        r"\d+\s*gün(?:\s*əvvəl)?",
        r"\d+\s*days?\s*ago",
        r"\d+\s*weeks?\s*ago",
        r"\d+\s*months?\s*ago",
        r"\d+\s*həftə",
        r"\d+\s*ay",
        r"\d+\s*нед",
        r"\d+\s*мес",
        r"\bBu gün\b",
        r"\bBugün\b",
        r"\bDünən\b",
        r"\bToday\b",
        r"\bYesterday\b",
        r"\bСегодня\b",
        r"\bВчера\b",
        r"\d{2}[./-]\d{2}[./-]\d{4}",
        r"\d{4}[./-]\d{2}[./-]\d{2}",
        r"\d{1,2}\s+[A-Za-zƏĞIİÖŞÇÜa-zəğıiöşçü]+\s+\d{4}",
    ]
    for pattern in age_patterns:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)

    text = re.sub(r"\b\d+(?:\.\d+)?K\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d+\b", " ", text)
    return clean_title(text)


def extract_title_from_leading_keywords(text: str) -> str:
    source = clean_title(text)
    lowered = normalize_text(source)

    for keyword in SORTED_KEYWORDS:
        kw_norm = normalize_text(keyword)
        if lowered.startswith(kw_norm):
            pattern = re.compile(
                rf"^{re.escape(keyword)}(?:\s*\([^)]{{1,160}}\))?",
                re.IGNORECASE,
            )
            m = pattern.search(source)
            if m:
                return clean_title(m.group(0))
            return clean_title(source[: len(keyword)])
    return source


def extract_company_after_title(text: str, title: str) -> Optional[str]:
    cleaned = clean_title(text)
    if not cleaned or not title:
        return None

    if "●" in cleaned:
        cleaned = clean_title(cleaned.split("●", 1)[0])

    if cleaned.lower().startswith(title.lower()):
        rest = clean_title(cleaned[len(title):])
        rest = rest.strip("-–—:/|•")
        rest = clean_title(rest)
        if rest and rest.lower() != title.lower():
            return rest
    return None


def absolute_url(base: str, url: str) -> str:
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return base.rstrip("/") + url
    return base.rstrip("/") + "/" + url.lstrip("/")


def extract_trailing_numeric_id(url: str) -> Optional[str]:
    cleaned = (url or "").strip().rstrip("/")
    patterns = [
        r"-(\d+)$",
        r"/(\d+)$",
        r"id=(\d+)",
        r"currentJobId=(\d+)",
        r"jobId=(\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, cleaned, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def canonicalize_job_url(site: str, url: str) -> str:
    cleaned = ((url or "").strip().split("#", 1)[0]).rstrip("/")
    if not cleaned:
        return ""

    cleaned_lower = cleaned.lower()
    vacancy_id = extract_trailing_numeric_id(cleaned)

    if site == "hellojob" and vacancy_id:
        return f"hellojob:{vacancy_id}"
    if site == "azvak" and vacancy_id and "/vakansiyalar/" in cleaned_lower:
        return f"azvak:{vacancy_id}"
    if site == "jobsearch" and vacancy_id and "/vacancies/" in cleaned_lower:
        return f"jobsearch:{vacancy_id}"
    if site == "busy" and vacancy_id and "/vacancy/" in cleaned_lower:
        return f"busy:{vacancy_id}"
    if site == "linkedin" and vacancy_id and "/jobs/view/" in cleaned_lower:
        return f"linkedin:{vacancy_id}"

    cleaned = cleaned.split("?", 1)[0]
    return cleaned_lower


def build_vacancy_storage_key(
    site: str,
    title: str,
    url: str,
    company: Optional[str] = None,
) -> str:
    canonical_url = canonicalize_job_url(site, url)
    normalized_title = normalize_text(title)
    normalized_company = normalize_text(company or "")

    if canonical_url:
        return f"{site}|{canonical_url}"

    return f"{site}|{normalized_company}|{normalized_title}|{(url or '').strip().rstrip('/').lower()}"


def deduplicate_vacancy_rows(rows) -> List[Dict]:
    result = []
    seen = set()

    for site, title, company, url, published_date, found_date in rows:
        canonical_url = canonicalize_job_url(site, url)
        normalized_title = normalize_text(title)
        normalized_company = normalize_text(company or "")
        key = canonical_url or f"{site}|{normalized_company}|{normalized_title}|{published_date or found_date}"

        if key in seen:
            continue

        seen.add(key)
        result.append(
            {
                "site": site,
                "title": title,
                "company": company,
                "url": url,
                "published_date": published_date,
                "found_date": found_date,
            }
        )
    return result


def deduplicate_vacancies(vacancies: List[Vacancy]) -> List[Vacancy]:
    seen = set()
    result = []

    for v in vacancies:
        cleaned_url = v.url.rstrip("/")
        key = build_vacancy_storage_key(v.site, v.title, cleaned_url, v.company)

        if key in seen:
            continue
        if looks_like_noise(v.title):
            continue
        if not is_legal_vacancy(v.title):
            continue
        if not cleaned_url:
            continue
        if not is_fresh_enough(v.published_date):
            continue

        seen.add(key)
        result.append(v)

    return result


# =========================
# FETCH
# =========================

def fetch_html(url: str, timeout: int = 35) -> Optional[str]:
    try:
        response = SESSION.get(url, timeout=timeout)
        if response.status_code in {403, 429, 999}:
            logger.warning("Source blocked request %s with status %s", url, response.status_code)
            return None
        response.raise_for_status()
        return response.text
    except Exception as exc:
        logger.error("Ошибка при запросе %s: %s", url, exc)
        return None


# =========================
# PARSERS
# =========================

def parse_jobsearch() -> List[Vacancy]:
    html_text = fetch_html(SITE_URLS["jobsearch"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []
    seen = set()

    for a in soup.select('a[href*="/vacancies/"]'):
        href = (a.get("href") or "").strip()
        title = clean_title(a.get_text(" ", strip=True))
        if not href or not title or href == "/vacancies":
            continue
        if looks_like_noise(title) or not is_legal_vacancy(title):
            continue

        url = absolute_url("https://classic.jobsearch.az", href)
        published_date = None
        company = None

        containers = []
        node = a
        for _ in range(4):
            node = node.parent
            if not node:
                break
            containers.append(node)

        for container in containers:
            context = clean_title(container.get_text(" ", strip=True))
            if published_date is None:
                published_date = extract_dates_from_text(context)
            if company is None:
                maybe = extract_company_after_title(context, title)
                if maybe and maybe.lower() != title.lower():
                    company = maybe

        key = (normalize_text(title), url.rstrip("/"))
        if key in seen:
            continue
        seen.add(key)
        vacancies.append(Vacancy("jobsearch", title, url, published_date, company))

    logger.info("JOBSEARCH FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def parse_busy_page(url: str) -> List[Vacancy]:
    html_text = fetch_html(url)
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []
    seen = set()

    for a in soup.select('a[href*="/vacancy/"]'):
        href = (a.get("href") or "").strip()
        raw_text = clean_title(a.get_text(" ", strip=True))
        if not href or not raw_text:
            continue

        full_url = absolute_url("https://busy.az", href)
        published_date = extract_dates_from_text(raw_text)
        compact = remove_known_meta(raw_text)
        title = extract_title_from_leading_keywords(compact)

        if not title or looks_like_noise(title) or not is_legal_vacancy(title):
            continue

        company = extract_company_after_title(compact, title)
        key = (canonicalize_job_url("busy", full_url), normalize_text(title))
        if key in seen:
            continue

        seen.add(key)
        vacancies.append(Vacancy("busy", title, full_url, published_date, company))

    logger.info("BUSY PAGE %s FOUND %s", url, len(vacancies))
    return deduplicate_vacancies(vacancies)


def parse_busy() -> List[Vacancy]:
    items: List[Vacancy] = []
    for url in SITE_URLS["busy_professions"]:
        try:
            items.extend(parse_busy_page(url))
            time.sleep(0.3)
        except Exception as exc:
            logger.error("BUSY parse error for %s: %s", url, exc)
    logger.info("BUSY TOTAL FOUND %s", len(items))
    return deduplicate_vacancies(items)


def parse_glorri() -> List[Vacancy]:
    vacancies: List[Vacancy] = []
    seen = set()

    for url in SITE_URLS["glorri"]:
        html_text = fetch_html(url)
        if not html_text:
            continue

        soup = BeautifulSoup(html_text, "html.parser")
        local_found = 0

        for a in soup.select('a[href*="/vacancies/"], a[href*="/jobs/"]'):
            href = (a.get("href") or "").strip()
            raw = clean_title(a.get_text(" ", strip=True))
            if not href or not raw:
                continue

            published_date = extract_dates_from_text(raw)
            compact = remove_known_meta(raw)
            left_part = clean_title(compact.split("●", 1)[0])
            title = extract_title_from_leading_keywords(left_part)

            if not title or looks_like_noise(title) or not is_legal_vacancy(title):
                continue

            company = extract_company_after_title(left_part, title)
            full_url = absolute_url("https://jobs.glorri.com", href)
            key = (canonicalize_job_url("glorri", full_url), normalize_text(title))
            if key in seen:
                continue

            seen.add(key)
            local_found += 1
            vacancies.append(Vacancy("glorri", title, full_url, published_date, company))

        if local_found:
            logger.info("GLORRI PAGE %s FOUND %s", url, local_found)

    logger.info("GLORRI TOTAL FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def parse_azvak() -> List[Vacancy]:
    html_text = fetch_html(SITE_URLS["azvak"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")

    date_re = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
    stop_markers = {
        "AzVak-ın (azvak.az) Məxfilik siyasəti və Xidmət şərtləri",
        "Vəzifələr",
        "Şöbələr",
        "Şirkətlər",
        "Rayonlar",
    }
    meta_lines = {"PREMİUM", "YENİ", "VIP", "Company Logo", "banner"}

    def business_key(title: str, company: Optional[str], published_date: Optional[date]):
        return (
            normalize_text(title),
            normalize_text(company or ""),
            published_date.isoformat() if published_date else "",
        )

    vacancies: List[Vacancy] = []
    seen = set()

    # 1) Пробуем прямые ссылки на вакансии
    links = soup.select('a[href*="/vakansiyalar/"]')

    for a in links:
        href = (a.get("href") or "").strip()
        title = clean_title(a.get_text(" ", strip=True))

        if not href or not title:
            continue
        if "/vakansiyalar/" not in href:
            continue
        if looks_like_noise(title) or not is_legal_vacancy(title):
            continue

        full_url = absolute_url("https://azvak.az", href)

        m = re.search(r"/vakansiyalar/[^/]+/(\d+)", full_url)
        if not m:
            continue

        try:
            vacancy_id = int(m.group(1))
        except ValueError:
            continue

        # category-like ссылки режем
        if vacancy_id < 1000:
            continue

        canonical_url = canonicalize_job_url("azvak", full_url)
        if not canonical_url or canonical_url in seen:
            continue

        published_date = None
        company = None

        container = a
        for _ in range(6):
            if not container:
                break

            context_lines = [
                clean_title(x)
                for x in container.get_text("\n", strip=True).splitlines()
                if clean_title(x)
            ]

            if published_date is None:
                for ln in context_lines:
                    if date_re.match(ln):
                        published_date = parse_date_loose(ln)
                        break

            if company is None:
                for ln in context_lines:
                    if (
                        not ln
                        or ln == title
                        or date_re.match(ln)
                        or ln.isdigit()
                        or ln.upper() in meta_lines
                    ):
                        continue
                    company = ln
                    break

            container = container.parent

        key = business_key(title, company, published_date)
        if key in seen:
            continue

        seen.add(key)
        vacancies.append(Vacancy("azvak", title, full_url, published_date))

    # 2) Fallback по тексту страницы
    text = soup.get_text("\n", strip=True)
    lines = [clean_title(x) for x in text.splitlines() if clean_title(x)]

    start_idx = 0
    for idx, line in enumerate(lines):
        if line.startswith("Sıralama"):
            start_idx = idx + 1
            break

    lines = lines[start_idx:]

    i = 0
    while i < len(lines):
        line = lines[i]

        if line in stop_markers:
            break

        title = line

        if looks_like_noise(title) or not is_legal_vacancy(title):
            i += 1
            continue

        company = None
        published_date = None

        j = i + 1
        steps = 0
        while j < len(lines) and steps < 6:
            cur = lines[j]

            if cur in stop_markers:
                break

            if cur.upper() in meta_lines:
                j += 1
                steps += 1
                continue

            if cur.isdigit():
                j += 1
                steps += 1
                continue

            if date_re.match(cur):
                published_date = parse_date_loose(cur)
                break

            if company is None:
                company = cur

            j += 1
            steps += 1

        if published_date:
            key = business_key(title, company, published_date)
            if key not in seen:
                seen.add(key)
                vacancies.append(Vacancy("azvak", title, SITE_URLS["azvak"], published_date))
            i = j + 1
        else:
            i += 1

    logger.info("AZVAK FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def parse_hellojob() -> List[Vacancy]:
    html_text = fetch_html(SITE_URLS["hellojob"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []
    seen = set()

    for a in soup.select('a[href^="/vakansiya/"], a[href*="hellojob.az/vakansiya/"]'):
        href = (a.get("href") or "").strip()
        if not re.search(r"/vakansiya/[a-z0-9\-_%]+-\d+/?$", href, re.IGNORECASE):
            continue

        url = absolute_url("https://www.hellojob.az", href)
        vacancy_id = extract_trailing_numeric_id(url) or url.rstrip("/").lower()

        title = clean_title(a.get_text(" ", strip=True))
        published_date = None
        company = None

        container = a
        for _ in range(6):
            if not container:
                break

            context = clean_title(container.get_text(" ", strip=True))

            if (not title) or len(title) < 6:
                parsed_title = None
                m = re.search(
                    r"([A-ZƏĞIİÖŞÇÜa-zəğıiöşçü0-9][^\n]{8,180})\s+(?:Razılaşma ilə|\d{1,2}\s+[A-Za-zƏĞIİÖŞÇÜa-zəğıiöşçü]+\s+\d{4})",
                    context,
                )
                if m:
                    parsed_title = clean_title(m.group(1))
                if parsed_title:
                    title = parsed_title

            if published_date is None:
                published_date = extract_dates_from_text(context)

            if company is None and title:
                maybe_company = extract_company_after_title(context, title)
                if maybe_company:
                    company = maybe_company

            container = container.parent

        if not title:
            continue
        if looks_like_noise(title) or not is_legal_vacancy(title):
            continue

        key = (vacancy_id, normalize_text(title))
        if key in seen:
            continue

        seen.add(key)
        vacancies.append(Vacancy("hellojob", title, url, published_date, company))

    logger.info("HELLOJOB FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def parse_linkedin() -> List[Vacancy]:
    if not ENABLE_LINKEDIN:
        return []

    vacancies: List[Vacancy] = []
    seen = set()

    for url in SITE_URLS["linkedin"]:
        html_text = fetch_html(url, timeout=40)
        if not html_text:
            continue

        soup = BeautifulSoup(html_text, "html.parser")
        local_found = 0

        for a in soup.select('a[href*="/jobs/view/"]'):
            href = (a.get("href") or "").strip()
            title = clean_title(a.get_text(" ", strip=True))
            if not href or not title:
                continue
            if looks_like_noise(title) or not is_legal_vacancy(title):
                continue

            full_url = absolute_url("https://az.linkedin.com", href)
            published_date = None
            company = None

            containers = []
            node = a
            for _ in range(5):
                node = node.parent
                if not node:
                    break
                containers.append(node)

            for container in containers:
                context = clean_title(container.get_text(" ", strip=True))
                if published_date is None:
                    published_date = extract_dates_from_text(context)

                if company is None:
                    maybe_company = extract_company_after_title(context, title)
                    if maybe_company:
                        company = maybe_company

            key = (canonicalize_job_url("linkedin", full_url), normalize_text(title))
            if key in seen:
                continue

            seen.add(key)
            local_found += 1
            vacancies.append(Vacancy("linkedin", title, full_url, published_date, company))

        if local_found:
            logger.info("LINKEDIN PAGE %s FOUND %s", url, local_found)
        time.sleep(0.5)

    logger.info("LINKEDIN TOTAL FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def collect_all_vacancies() -> Dict[str, List[Vacancy]]:
    result = {
        "jobsearch": parse_jobsearch(),
        "busy": parse_busy(),
        "glorri": parse_glorri(),
        "azvak": parse_azvak(),
        "hellojob": parse_hellojob(),
    }
    if ENABLE_LINKEDIN:
        result["linkedin"] = parse_linkedin()

    for site, items in result.items():
        logger.info("SITE %s FOUND %s", site, len(items))
        for item in items[:10]:
            logger.info("SITE %s ITEM %s | %s", site, item.title, item.url)

    return result


def refresh_database_once() -> Dict[str, int]:
    init_db()
    cleanup_old_vacancies()

    collected = collect_all_vacancies()
    all_vacancies: List[Vacancy] = []
    for items in collected.values():
        all_vacancies.extend(items)

    inserted = save_vacancies(all_vacancies)
    cleanup_old_vacancies()
    cleanup_duplicate_vacancies()

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    set_meta("last_refresh_at", now_utc.isoformat())
    set_meta("last_refresh_found", str(len(all_vacancies)))
    set_meta("last_refresh_inserted", str(inserted))

    for site, items in collected.items():
        set_meta(f"site_found_{site}", str(len(items)))

    return {
        "found": len(all_vacancies),
        "inserted": inserted,
    }


# =========================
# TELEGRAM UI
# =========================

def get_lang(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get("lang", "ru")


def t(context: ContextTypes.DEFAULT_TYPE, key: str) -> str:
    lang = get_lang(context)
    return TEXTS.get(lang, TEXTS["ru"]).get(key, key)


def remember_user_once(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.get("_user_saved"):
        return
    user_id = update.effective_user.id if update.effective_user else None
    save_user(user_id)
    context.user_data["_user_saved"] = True


def get_language_keyboard():
    keyboard = [
        [TEXTS["ru"]["lang_btn_az"], TEXTS["ru"]["lang_btn_ru"], TEXTS["ru"]["lang_btn_en"]],
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=False,
    )


def get_main_menu_keyboard(context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [t(context, "search_btn"), t(context, "change_lang_btn")],
        [t(context, "old_btn")],
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=False,
    )


def get_old_jobs_keyboard(context: ContextTypes.DEFAULT_TYPE):
    third_row = ["HelloJob"]
    if ENABLE_LINKEDIN:
        third_row.append("LinkedIn")

    keyboard = [
        ["JobSearch", "Busy.az"],
        ["Glorri", "AzVak"],
        third_row,
        [t(context, "back_btn")],
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=False,
    )


def format_vacancy_lines_html(
    vacancies: List[Dict],
    empty_text: str,
    context: ContextTypes.DEFAULT_TYPE,
) -> str:
    if not vacancies:
        return html.escape(empty_text)

    lines = []
    for idx, item in enumerate(vacancies, start=1):
        display_date = item["published_date"] or item["found_date"]
        date_str = display_date.strftime("%Y-%m-%d") if display_date else "-"

        safe_title = html.escape(item["title"])
        safe_site = html.escape(SITE_LABELS.get(item["site"], item["site"]))
        safe_url = html.escape(item["url"])
        safe_company = html.escape(item.get("company") or "")

        block = (
            f'{idx}. <a href="{safe_url}">{safe_title}</a>\n'
            f'{html.escape(t(context, "site_label"))}: {safe_site}\n'
            f'{html.escape(t(context, "date_label"))}: {date_str}'
        )
        if SHOW_COMPANY and safe_company:
            block += f'\n{html.escape(t(context, "company_label"))}: {safe_company}'
        lines.append(block)

    return "\n\n".join(lines)


def split_long_message(text: str, limit: int = 3500) -> List[str]:
    if len(text) <= limit:
        return [text]

    parts: List[str] = []
    current = ""

    for block in text.split("\n\n"):
        candidate = block if not current else current + "\n\n" + block
        if len(candidate) <= limit:
            current = candidate
        else:
            if current:
                parts.append(current)
            current = block

    if current:
        parts.append(current)
    return parts


def normalize_button(text: str) -> str:
    return clean_title(text)


def resolve_language_choice(text: str) -> Optional[str]:
    value = normalize_button(text)
    if value == normalize_button(TEXTS["ru"]["lang_btn_ru"]):
        return "ru"
    if value == normalize_button(TEXTS["ru"]["lang_btn_az"]):
        return "az"
    if value == normalize_button(TEXTS["ru"]["lang_btn_en"]):
        return "en"
    return None


def resolve_button_lang(text: str, key: str) -> Optional[str]:
    value = normalize_button(text)
    for lang in ("ru", "az", "en"):
        if value == normalize_button(TEXTS[lang][key]):
            return lang
    return None


SITE_BUTTON_TO_KEY = {
    "JobSearch": "jobsearch",
    "Busy.az": "busy",
    "Glorri": "glorri",
    "AzVak": "azvak",
    "HelloJob": "hellojob",
    "LinkedIn": "linkedin",
}


def format_last_refresh_for_lang(context: ContextTypes.DEFAULT_TYPE) -> str:
    raw = get_meta("last_refresh_at")
    if not raw:
        return t(context, "last_refresh_unknown")

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        dt = dt.astimezone(timezone(timedelta(hours=4)))
        value = dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        value = raw

    return t(context, "last_refresh").format(value=value)


async def send_recent_archive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_recent_vacancies(limit=MAX_RESULTS_TO_SHOW)
    header = t(context, "archive_header") + "\n" + format_last_refresh_for_lang(context) + "\n\n"
    body = format_vacancy_lines_html(rows, t(context, "empty_recent"), context)

    for chunk in split_long_message(header + body):
        await update.message.reply_text(
            chunk,
            reply_markup=get_main_menu_keyboard(context),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


async def send_site_archive(update: Update, context: ContextTypes.DEFAULT_TYPE, site_button: str):
    site = SITE_BUTTON_TO_KEY[site_button]
    rows = get_recent_vacancies_by_site(site, limit=MAX_RESULTS_TO_SHOW)
    body = format_vacancy_lines_html(
        rows,
        t(context, "empty_site").format(site=site_button),
        context,
    )

    header = format_last_refresh_for_lang(context) + "\n\n"
    for chunk in split_long_message(header + body):
        await update.message.reply_text(
            chunk,
            reply_markup=get_old_jobs_keyboard(context),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    return OLD_JOBS_MENU


async def open_language_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        TEXTS["en"]["choose_language"],
        reply_markup=get_language_keyboard(),
    )
    return LANG_MENU


async def wake_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user_once(update, context)

    if "lang" not in context.user_data:
        await update.message.reply_text(
            TEXTS["en"]["choose_language"],
            reply_markup=get_language_keyboard(),
        )
        return LANG_MENU

    await update.message.reply_text(
        t(context, "bot_awake"),
        reply_markup=get_main_menu_keyboard(context),
    )
    return MAIN_MENU


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user_once(update, context)

    if "lang" not in context.user_data:
        await update.message.reply_text(
            TEXTS["en"]["choose_language"],
            reply_markup=get_language_keyboard(),
        )
        return LANG_MENU

    await update.message.reply_text(
        t(context, "welcome"),
        reply_markup=get_main_menu_keyboard(context),
    )
    return MAIN_MENU


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "lang" not in context.user_data:
        await update.message.reply_text(
            TEXTS["en"]["choose_language"],
            reply_markup=get_language_keyboard(),
        )
        return LANG_MENU

    await update.message.reply_text(
        t(context, "help"),
        reply_markup=get_main_menu_keyboard(context),
    )
    return MAIN_MENU


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user_once(update, context)

    if ADMIN_USER_ID and (not update.effective_user or update.effective_user.id != ADMIN_USER_ID):
        await update.message.reply_text(
            t(context, "not_admin") if "lang" in context.user_data else TEXTS["ru"]["not_admin"],
        )
        return MAIN_MENU

    users = count_users()
    vacancies = count_recent_vacancies()
    last_refresh = get_meta("last_refresh_at") or "unknown"

    if "lang" not in context.user_data:
        context.user_data["lang"] = "ru"

    try:
        dt = datetime.fromisoformat(last_refresh.replace("Z", "+00:00"))
        dt = dt.astimezone(timezone(timedelta(hours=4)))
        last_refresh = dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass

    await update.message.reply_text(
        t(context, "stats_text").format(
            users=users,
            vacancies=vacancies,
            last_refresh=last_refresh,
        ),
        reply_markup=get_main_menu_keyboard(context),
    )
    return MAIN_MENU


async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_recent_archive(update, context)
    return MAIN_MENU


async def open_old_jobs_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        t(context, "old_jobs_prompt"),
        reply_markup=get_old_jobs_keyboard(context),
    )
    return OLD_JOBS_MENU


async def universal_button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user_once(update, context)

    raw_text = update.message.text or ""
    text = normalize_button(raw_text)

    selected_lang = resolve_language_choice(raw_text)
    if selected_lang:
        context.user_data["lang"] = selected_lang
        await update.message.reply_text(
            t(context, "welcome"),
            reply_markup=get_main_menu_keyboard(context),
        )
        return MAIN_MENU

    if "lang" not in context.user_data:
        inferred_lang = (
            resolve_button_lang(raw_text, "search_btn")
            or resolve_button_lang(raw_text, "change_lang_btn")
            or resolve_button_lang(raw_text, "old_btn")
            or resolve_button_lang(raw_text, "back_btn")
        )
        if inferred_lang:
            context.user_data["lang"] = inferred_lang

    if text == normalize_button("start"):
        return await wake_to_main_menu(update, context)

    if any(text == normalize_button(TEXTS[lang]["search_btn"]) for lang in ("ru", "az", "en")):
        return await handle_search(update, context)

    if any(text == normalize_button(TEXTS[lang]["change_lang_btn"]) for lang in ("ru", "az", "en")):
        return await open_language_menu(update, context)

    if any(text == normalize_button(TEXTS[lang]["old_btn"]) for lang in ("ru", "az", "en")):
        return await open_old_jobs_menu(update, context)

    if any(text == normalize_button(TEXTS[lang]["help_btn"]) for lang in ("ru", "az", "en")):
        return await help_command(update, context)

    if any(text == normalize_button(TEXTS[lang]["back_btn"]) for lang in ("ru", "az", "en")):
        await update.message.reply_text(
            t(context, "main_menu"),
            reply_markup=get_main_menu_keyboard(context),
        )
        return MAIN_MENU

    if raw_text in SITE_BUTTON_TO_KEY:
        if "lang" not in context.user_data:
            context.user_data["lang"] = "ru"
        return await send_site_archive(update, context, raw_text)

    if "lang" not in context.user_data:
        await update.message.reply_text(
            TEXTS["en"]["choose_language"],
            reply_markup=get_language_keyboard(),
        )
        return LANG_MENU

    await update.message.reply_text(
        t(context, "press_button"),
        reply_markup=get_main_menu_keyboard(context),
    )
    return MAIN_MENU


# =========================
# APP BOOT
# =========================

def build_app():
    app = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router),
        ],
        states={
            LANG_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router)],
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router)],
            OLD_JOBS_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router)],
        },
        fallbacks=[
            CommandHandler("start", start),
            MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("language", open_language_menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats_command))
    return app


def run_bot() -> None:
    require_database()
    require_bot_env()
    init_db()
    cleanup_old_vacancies()

    app = build_app()

    webhook_path = os.getenv("WEBHOOK_PATH") or hashlib.sha256(TOKEN.encode("utf-8")).hexdigest()[:32]
    webhook_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/{webhook_path}"

    logger.info("Webhook configured for Render")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=webhook_path,
        webhook_url=webhook_url,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--refresh-once",
        action="store_true",
        help="Run parser once, save vacancies to Neon, and exit.",
    )
    args = parser.parse_args()

    if args.refresh_once:
        stats = refresh_database_once()
        print(
            f"Refresh complete. Found={stats['found']} Inserted={stats['inserted']} "
            f"LinkedIn={'on' if ENABLE_LINKEDIN else 'off'}"
        )
        return

    run_bot()


if __name__ == "__main__":
    main()
