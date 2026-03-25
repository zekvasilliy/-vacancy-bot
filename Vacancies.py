import os
import re
import logging
import hashlib
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional

import psycopg
import requests
from bs4 import BeautifulSoup
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("8602454193:AAEy3AInd2S9igNrCU896x61w7xlFgml-qU")
PORT = int(os.getenv("PORT", "10000"))
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

if not TOKEN:
    raise ValueError("Не найдена переменная BOT_TOKEN")
if not DATABASE_URL:
    raise ValueError("Не найдена переменная DATABASE_URL")
if not RENDER_EXTERNAL_URL:
    raise ValueError("Не найдена переменная RENDER_EXTERNAL_URL")

MAIN_MENU, OLD_JOBS_MENU = range(2)

SITE_LABELS = {
    "jobsearch": "JobSearch",
    "busy": "Busy.az",
    "glorri": "Glorri",
    "smartjob": "SmartJob",
    "azvak": "Azvak",
    "hellojob": "HelloJob",
}

KEYWORDS = [
    "юрист",
    "юрисконсульт",
    "hüquqşünas",
    "huquqsunas",
    "korporativ hüquqşünas",
    "legal",
    "lawyer",
    "legal assistant",
    "compliance",
    "contract",
    "hüquq üzrə mütəxəssis",
    "hüquq məsləhətçisi",
]

SITE_URLS = {
    "jobsearch": "https://jobsearch.az/vacancies",
    "busy": "https://busy.az",
    "glorri": "https://jobs.glorri.com",
    "smartjob": "https://smartjob.az",
    "azvak": "https://azvak.az",
    "hellojob": "https://www.hellojob.az",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


class Vacancy:
    def __init__(self, site: str, title: str, url: str, published_date: Optional[date]):
        self.site = site
        self.title = title.strip()
        self.url = url.strip()
        self.published_date = published_date

    @property
    def unique_hash(self) -> str:
        raw = f"{self.site}|{self.title}|{self.url}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_connection():
    return psycopg.connect(DATABASE_URL)


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    site TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    published_date DATE,
                    found_date DATE NOT NULL,
                    unique_hash TEXT NOT NULL UNIQUE
                )
                """
            )
        conn.commit()


def cleanup_old_vacancies():
    border = date.today() - timedelta(days=60)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM vacancies WHERE COALESCE(published_date, found_date) < %s",
                (border,),
            )
        conn.commit()


def save_vacancies(vacancies: List[Vacancy]) -> int:
    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for v in vacancies:
                cur.execute(
                    """
                    INSERT INTO vacancies (site, title, url, published_date, found_date, unique_hash)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (unique_hash) DO NOTHING
                    """,
                    (v.site, v.title, v.url, v.published_date, date.today(), v.unique_hash),
                )
                if cur.rowcount > 0:
                    inserted += 1
        conn.commit()
    return inserted


def get_recent_vacancies(limit: int = 100) -> List[Dict]:
    border = date.today() - timedelta(days=60)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT site, title, url, published_date, found_date
                FROM vacancies
                WHERE COALESCE(published_date, found_date) >= %s
                ORDER BY COALESCE(published_date, found_date) DESC, id DESC
                LIMIT %s
                """,
                (border, limit),
            )
            rows = cur.fetchall()

    result = []
    for site, title, url, published_date, found_date in rows:
        result.append(
            {
                "site": site,
                "title": title,
                "url": url,
                "published_date": published_date,
                "found_date": found_date,
            }
        )
    return result


def get_recent_vacancies_by_site(site: str, limit: int = 50) -> List[Dict]:
    border = date.today() - timedelta(days=60)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT site, title, url, published_date, found_date
                FROM vacancies
                WHERE site = %s AND COALESCE(published_date, found_date) >= %s
                ORDER BY COALESCE(published_date, found_date) DESC, id DESC
                LIMIT %s
                """,
                (site, border, limit),
            )
            rows = cur.fetchall()

    result = []
    for site_name, title, url, published_date, found_date in rows:
        result.append(
            {
                "site": site_name,
                "title": title,
                "url": url,
                "published_date": published_date,
                "found_date": found_date,
            }
        )
    return result


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def is_legal_vacancy(title: str) -> bool:
    t = normalize_text(title)
    return any(keyword in t for keyword in KEYWORDS)


def parse_date_loose(text: str) -> Optional[date]:
    if not text:
        return None

    raw = text.strip().lower()
    today = date.today()

    if raw in ["bu gün", "bugün", "today"]:
        return today
    if raw in ["dünən", "dunen", "yesterday"]:
        return today - timedelta(days=1)

    match = re.search(r"(\d{2})[-./](\d{2})[-./](\d{4})", raw)
    if match:
        day, month, year = map(int, match.groups())
        try:
            return date(year, month, day)
        except ValueError:
            return None

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
    }

    match2 = re.search(r"(\d{1,2})\s+([a-zəğıöşçü]+)", raw)
    if match2:
        day_num = int(match2.group(1))
        month_name = match2.group(2)
        month_num = months.get(month_name)
        if month_num:
            try:
                guessed = date(today.year, month_num, day_num)
                if guessed > today:
                    guessed = date(today.year - 1, month_num, day_num)
                return guessed
            except ValueError:
                return None

    return None


def is_fresh_enough(vacancy_date: Optional[date]) -> bool:
    if vacancy_date is None:
        return True
    return vacancy_date >= date.today() - timedelta(days=60)


def absolute_url(base: str, url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return base.rstrip("/") + url
    return base.rstrip("/") + "/" + url.lstrip("/")


def fetch_html(url: str) -> Optional[str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=25)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.exception("Ошибка при запросе %s: %s", url, e)
        return None


# ===== PARSERS =====
# Эти парсеры сделаны как стартовый каркас. После первого теста вживую
# HTML-селекторы, скорее всего, придется точечно подправить под каждый сайт.

def parse_jobsearch() -> List[Vacancy]:
    html = fetch_html(SITE_URLS["jobsearch"])
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select("a"):
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not title or not href:
            continue
        if not is_legal_vacancy(title):
            continue
        url = absolute_url("https://jobsearch.az", href)
        vacancies.append(Vacancy("jobsearch", title, url, None))

    return deduplicate_vacancies(vacancies)


def parse_busy() -> List[Vacancy]:
    html = fetch_html(SITE_URLS["busy"])
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select("a"):
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not title or not href:
            continue
        if not is_legal_vacancy(title):
            continue
        url = absolute_url("https://busy.az", href)
        vacancies.append(Vacancy("busy", title, url, None))

    return deduplicate_vacancies(vacancies)


def parse_glorri() -> List[Vacancy]:
    html = fetch_html(SITE_URLS["glorri"])
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select("a"):
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not title or not href:
            continue
        if not is_legal_vacancy(title):
            continue
        url = absolute_url("https://jobs.glorri.com", href)
        vacancies.append(Vacancy("glorri", title, url, None))

    return deduplicate_vacancies(vacancies)


def parse_smartjob() -> List[Vacancy]:
    html = fetch_html(SITE_URLS["smartjob"])
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select("a"):
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not title or not href:
            continue
        if not is_legal_vacancy(title):
            continue
        url = absolute_url("https://smartjob.az", href)
        vacancies.append(Vacancy("smartjob", title, url, None))

    return deduplicate_vacancies(vacancies)


def parse_azvak() -> List[Vacancy]:
    html = fetch_html(SITE_URLS["azvak"])
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select("a"):
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not title or not href:
            continue
        if not is_legal_vacancy(title):
            continue
        url = absolute_url("https://azvak.az", href)
        vacancies.append(Vacancy("azvak", title, url, None))

    return deduplicate_vacancies(vacancies)


def parse_hellojob() -> List[Vacancy]:
    html = fetch_html(SITE_URLS["hellojob"])
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select("a"):
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not title or not href:
            continue
        if not is_legal_vacancy(title):
            continue
        url = absolute_url("https://www.hellojob.az", href)
        vacancies.append(Vacancy("hellojob", title, url, None))

    return deduplicate_vacancies(vacancies)


def deduplicate_vacancies(vacancies: List[Vacancy]) -> List[Vacancy]:
    seen = set()
    result = []
    for v in vacancies:
        key = (v.site, v.title.lower(), v.url)
        if key in seen:
            continue
        seen.add(key)
        if not is_fresh_enough(v.published_date):
            continue
        result.append(v)
    return result


def collect_all_vacancies() -> Dict[str, List[Vacancy]]:
    return {
        "jobsearch": parse_jobsearch(),
        "busy": parse_busy(),
        "glorri": parse_glorri(),
        "smartjob": parse_smartjob(),
        "azvak": parse_azvak(),
        "hellojob": parse_hellojob(),
    }


# ===== KEYBOARDS =====

def get_main_menu_keyboard():
    keyboard = [
        ["Искать вакансии", "Обновить вакансии"],
        ["Старые вакансии", "Помощь"],
        ["Start", "Отмена"],
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=True,
    )


def get_old_jobs_keyboard():
    keyboard = [
        ["JobSearch", "Busy.az"],
        ["Glorri", "SmartJob"],
        ["Azvak", "HelloJob"],
        ["Start", "Назад", "Отмена"],
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=True,
    )


WELCOME_TEXT = (
    "Добро пожаловать в бот для поиска вакансий юриста.\n\n"
    "Важно:\n"
    "На бесплатном хостинге бот может засыпать. Поэтому кнопка Start вынесена в каждое меню.\n\n"
    "Что умеет бот:\n"
    "1. Искать новые вакансии\n"
    "2. Обновлять вакансии\n"
    "3. Показывать архив по сайтам за последние 2 месяца"
)

HELP_TEXT = (
    "Как пользоваться ботом:\n\n"
    "1. Нажми Start, если бот уснул\n"
    "2. Нажми 'Искать вакансии' для быстрого поиска\n"
    "3. Нажми 'Обновить вакансии', чтобы заново собрать вакансии и сохранить новые\n"
    "4. Нажми 'Старые вакансии', чтобы открыть архив по сайтам\n\n"
    "Показываются вакансии только за последние 2 месяца.\n"
    "В сообщениях указывается дата без часов и секунд."
)


def format_vacancy_lines(vacancies: List[Dict], empty_text: str) -> str:
    if not vacancies:
        return empty_text

    lines = []
    for idx, item in enumerate(vacancies, start=1):
        display_date = item["published_date"] or item["found_date"]
        date_str = display_date.strftime("%Y-%m-%d") if display_date else "дата не указана"
        site_label = SITE_LABELS.get(item["site"], item["site"])
        lines.append(
            f"{idx}. {item['title']}\n"
            f"Сайт: {site_label}\n"
            f"Дата: {date_str}\n"
            f"Ссылка: {item['url']}"
        )
    return "\n\n".join(lines)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(WELCOME_TEXT, reply_markup=get_main_menu_keyboard())
    return MAIN_MENU


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, reply_markup=get_main_menu_keyboard())
    return MAIN_MENU


async def wake_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Бот проснулся. Ты в главном меню.",
        reply_markup=get_main_menu_keyboard(),
    )
    return MAIN_MENU


async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Ищу вакансии по всем сайтам. Подожди немного.")

    collected = collect_all_vacancies()
    all_vacancies: List[Vacancy] = []
    for _, items in collected.items():
        all_vacancies.extend(items)

    inserted = save_vacancies(all_vacancies)
    cleanup_old_vacancies()

    recent = get_recent_vacancies(limit=50)
    text = format_vacancy_lines(recent, "Свежих вакансий пока не найдено.")

    header = (
        f"Поиск завершен. Найдено: {len(all_vacancies)}\n"
        f"Новых сохранено: {inserted}\n\n"
    )

    chunks = split_long_message(header + text)
    for chunk in chunks:
        await update.message.reply_text(chunk, reply_markup=get_main_menu_keyboard())

    return MAIN_MENU


async def handle_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Обновляю вакансии и архив. Подожди немного.")

    collected = collect_all_vacancies()
    all_vacancies: List[Vacancy] = []
    summary_lines = []

    for site, items in collected.items():
        all_vacancies.extend(items)
        summary_lines.append(f"{SITE_LABELS[site]}: {len(items)}")

    inserted = save_vacancies(all_vacancies)
    cleanup_old_vacancies()

    message = (
        "Обновление завершено.\n\n"
        + "\n".join(summary_lines)
        + f"\n\nНовых вакансий сохранено: {inserted}"
    )
    await update.message.reply_text(message, reply_markup=get_main_menu_keyboard())
    return MAIN_MENU


async def open_old_jobs_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Выбери сайт, чтобы открыть архив вакансий за последние 2 месяца.",
        reply_markup=get_old_jobs_keyboard(),
    )
    return OLD_JOBS_MENU


async def old_jobs_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "Start":
        return await wake_to_main_menu(update, context)

    if text == "Назад":
        await update.message.reply_text("Главное меню:", reply_markup=get_main_menu_keyboard())
        return MAIN_MENU

    if text == "Отмена":
        await update.message.reply_text("Действие отменено. Ты в главном меню.", reply_markup=get_main_menu_keyboard())
        return MAIN_MENU

    map_text_to_site = {
        "JobSearch": "jobsearch",
        "Busy.az": "busy",
        "Glorri": "glorri",
        "SmartJob": "smartjob",
        "Azvak": "azvak",
        "HelloJob": "hellojob",
    }

    if text not in map_text_to_site:
        await update.message.reply_text("Выбери сайт кнопкой.", reply_markup=get_old_jobs_keyboard())
        return OLD_JOBS_MENU

    site = map_text_to_site[text]
    rows = get_recent_vacancies_by_site(site)
    body = format_vacancy_lines(rows, f"По сайту {text} вакансий за последние 2 месяца пока нет.")

    chunks = split_long_message(body)
    for chunk in chunks:
        await update.message.reply_text(chunk, reply_markup=get_old_jobs_keyboard())

    return OLD_JOBS_MENU


async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "Start":
        return await wake_to_main_menu(update, context)

    if text == "Искать вакансии":
        return await handle_search(update, context)

    if text == "Обновить вакансии":
        return await handle_refresh(update, context)

    if text == "Старые вакансии":
        return await open_old_jobs_menu(update, context)

    if text == "Помощь":
        return await help_command(update, context)

    if text == "Отмена":
        await update.message.reply_text("Действие отменено. Ты в главном меню.", reply_markup=get_main_menu_keyboard())
        return MAIN_MENU

    await update.message.reply_text(
        "Нажми нужную кнопку в меню.",
        reply_markup=get_main_menu_keyboard(),
    )
    return MAIN_MENU


def split_long_message(text: str, limit: int = 3500) -> List[str]:
    if len(text) <= limit:
        return [text]

    parts = []
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


def main():
    init_db()
    cleanup_old_vacancies()

    app = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex("^Start$"), wake_to_main_menu),
        ],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu_handler)],
            OLD_JOBS_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, old_jobs_menu_handler)],
        },
        fallbacks=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex("^Start$"), wake_to_main_menu),
            MessageHandler(filters.Regex("^Отмена$"), main_menu_handler),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("help", help_command))

    webhook_path = TOKEN
    webhook_url = f"{RENDER_EXTERNAL_URL}/{TOKEN}"

    logger.info("Webhook URL: %s", webhook_url)

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=webhook_path,
        webhook_url=webhook_url,
    )


if __name__ == "__main__":
    main()
