import os
import re
import html
import logging
import hashlib
from datetime import date, timedelta
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

TOKEN = os.getenv("BOT_TOKEN")
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
    "aparıcı hüquqşünas",
    "baş hüquqşünas",
    "kiçik hüquqşünas",
    "korporativ hüquqşünas",
    "korporativ müqavilələr üzrə hüquqşünas",
    "hüquq üzrə mütəxəssis",
    "hüquq məsləhətçisi",
    "hüquq departamenti",
    "hüquqi təhlil",
    "legal",
    "legal counsel",
    "legal specialist",
    "senior legal specialist",
    "lawyer",
    "corporate lawyer",
    "banking & finance lawyer",
    "license and permit specialist",
    "compliance",
    "contract management",
    "contract manager",
    "vəkil",
    "hüquq",
    "huquq",
]

SITE_URLS = {
    "jobsearch": "https://classic.jobsearch.az/vacancies?category=1375",
    "busy_category": "https://busy.az/category/huquq",
    "busy_profession": "https://busy.az/dp/huquqsunas-vakansiyalar",
    "glorri": "https://jobs.glorri.com/?jobFunctions=legal-services",
    "smartjob": "https://smartjob.az/index.php/vacancies?job_category_id%5B%5D=127",
    "azvak": "https://azvak.az/vezifeler/huquqsunas/134",
    "hellojob": "https://www.hellojob.az/is-elanlari/huquq",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "az,en-US;q=0.9,en;q=0.8,ru;q=0.7",
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


def get_recent_vacancies(limit: int = 1000) -> List[Dict]:
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

    return [
        {
            "site": site,
            "title": title,
            "url": url,
            "published_date": published_date,
            "found_date": found_date,
        }
        for site, title, url, published_date, found_date in rows
    ]


def get_recent_vacancies_by_site(site: str, limit: int = 1000) -> List[Dict]:
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

    return [
        {
            "site": site_name,
            "title": title,
            "url": url,
            "published_date": published_date,
            "found_date": found_date,
        }
        for site_name, title, url, published_date, found_date in rows
    ]


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def clean_title(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def is_legal_vacancy(title: str) -> bool:
    t = normalize_text(title)
    return any(keyword in t for keyword in KEYWORDS)


def month_name_to_number(month_name: str) -> Optional[int]:
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
    m = re.search(r"(\d+)\s*gün", raw)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))
    return None


def parse_date_loose(text: str) -> Optional[date]:
    if not text:
        return None

    raw = text.strip().lower()
    today = date.today()

    if "bu gün" in raw or "bugün" in raw or raw == "today":
        return today
    if "dünən" in raw or "dunen" in raw or raw == "yesterday":
        return today - timedelta(days=1)

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


def extract_dates_from_text(text: str) -> Optional[date]:
    return parse_date_loose(clean_title(text))


def is_fresh_enough(vacancy_date: Optional[date]) -> bool:
    if vacancy_date is None:
        return True
    return vacancy_date >= date.today() - timedelta(days=60)


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


def fetch_html(url: str) -> Optional[str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 403:
            logger.warning("Сайт вернул 403: %s", url)
            return None
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error("Ошибка при запросе %s: %s", url, e)
        return None


def looks_like_noise(title: str) -> bool:
    t = normalize_text(title)
    if not t:
        return True
    noise = {
        "haqqımızda", "xidmətlər", "əlaqə", "ana səhifə", "vakansiyalar",
        "şirkətlər", "vəzifələr", "hamısı", "hüquq", "müraciət et",
        "elan yerləşdir", "axtar", "sıfırla", "help", "latest vacancies",
        "kateqoriyalar", "sənaye", "seçilmiş elanlar", "uyğun iş elanları",
        "işə aid seçimlər", "vakansiya axtarışı", "seç", "sil",
        "tam iş günü", "razılaşma yolu ilə"
    }
    return t in noise or len(t) < 3


def deduplicate_vacancies(vacancies: List[Vacancy]) -> List[Vacancy]:
    seen = set()
    result = []
    for v in vacancies:
        key = (v.site, normalize_text(v.title), v.url)
        if key in seen:
            continue
        seen.add(key)
        if not is_fresh_enough(v.published_date):
            continue
        result.append(v)
    return result


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

        containers = []
        if a.parent:
            containers.append(a.parent)
        if a.parent and a.parent.parent:
            containers.append(a.parent.parent)
        if a.parent and a.parent.parent and a.parent.parent.parent:
            containers.append(a.parent.parent.parent)

        for container in containers:
            context = clean_title(container.get_text(" ", strip=True))
            parsed = extract_dates_from_text(context)
            if parsed:
                published_date = parsed
                break

        key = (normalize_text(title), url)
        if key in seen:
            continue

        vacancies.append(Vacancy("jobsearch", title, url, published_date))
        seen.add(key)

    logger.info("JOBSEARCH FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def parse_busy_page(url: str) -> List[Vacancy]:
    html_text = fetch_html(url)
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        title = clean_title(a.get_text(" ", strip=True))

        if not href or not title:
            continue

        if not (
            "/vacancies/" in href
            or "/jobs/" in href
            or "/dp/" in href
            or "/profession" in href
            or "/company" in href
        ):
            if not is_legal_vacancy(title):
                continue

        if looks_like_noise(title) or not is_legal_vacancy(title):
            continue

        full_url = absolute_url("https://busy.az", href)
        context = ""
        if a.parent:
            context = clean_title(a.parent.get_text(" ", strip=True))
            if a.parent.parent:
                context += " " + clean_title(a.parent.parent.get_text(" ", strip=True))

        published_date = extract_dates_from_text(context)
        vacancies.append(Vacancy("busy", title, full_url, published_date))

    return deduplicate_vacancies(vacancies)


def parse_busy() -> List[Vacancy]:
    items = []
    items.extend(parse_busy_page(SITE_URLS["busy_category"]))
    items.extend(parse_busy_page(SITE_URLS["busy_profession"]))
    return deduplicate_vacancies(items)


def parse_glorri() -> List[Vacancy]:
    html_text = fetch_html(SITE_URLS["glorri"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        title = clean_title(a.get_text(" ", strip=True))

        if not href or not title:
            continue
        if looks_like_noise(title) or not is_legal_vacancy(title):
            continue

        url = absolute_url("https://jobs.glorri.com", href)
        published_date = None

        if a.parent:
            published_date = extract_dates_from_text(a.parent.get_text(" ", strip=True))
            if not published_date and a.parent.parent:
                published_date = extract_dates_from_text(a.parent.parent.get_text(" ", strip=True))

        vacancies.append(Vacancy("glorri", title, url, published_date))

    return deduplicate_vacancies(vacancies)


def parse_smartjob() -> List[Vacancy]:
    html_text = fetch_html(SITE_URLS["smartjob"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []
    seen = set()

    def extract_smartjob_date(anchor) -> Optional[date]:
        containers = []
        node = anchor
        for _ in range(6):
            node = getattr(node, "parent", None)
            if not node:
                break
            containers.append(node)

        for container in containers:
            context = clean_title(container.get_text(" ", strip=True))

            m = re.search(r"Yerləşdirilib\s*(\d{2}\.\d{2}\.\d{4})", context, re.IGNORECASE)
            if m:
                parsed = parse_date_loose(m.group(1))
                if parsed:
                    return parsed

            m = re.search(r"(\d{2}\.\d{2}\.\d{4})", context)
            if m:
                parsed = parse_date_loose(m.group(1))
                if parsed:
                    return parsed

        return None

    for a in soup.select('a[href*="/index.php/vacancy/"]'):
        href = (a.get("href") or "").strip()
        title = clean_title(a.get_text(" ", strip=True))

        if not href or not title:
            continue
        if looks_like_noise(title) or not is_legal_vacancy(title):
            continue

        url = absolute_url("https://smartjob.az", href)
        published_date = extract_smartjob_date(a)

        key = (normalize_text(title), url)
        if key in seen:
            continue

        vacancies.append(Vacancy("smartjob", title, url, published_date))
        seen.add(key)

    logger.info("SMARTJOB FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def parse_azvak() -> List[Vacancy]:
    html_text = fetch_html(SITE_URLS["azvak"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []
    lines = []

    for raw_line in soup.get_text("\n", strip=True).splitlines():
        line = clean_title(raw_line)
        if line:
            lines.append(line)

    link_candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        text = clean_title(a.get_text(" ", strip=True))
        if href and text:
            link_candidates.append((normalize_text(text), absolute_url("https://azvak.az", href)))

    seen_titles = set()

    for i, line in enumerate(lines):
        title = clean_title(line)

        if looks_like_noise(title) or not is_legal_vacancy(title):
            continue
        if title in seen_titles:
            continue

        published_date = None
        for j in range(i + 1, min(i + 7, len(lines))):
            possible_date = parse_date_loose(lines[j])
            if possible_date:
                published_date = possible_date
                break

        matched_url = None
        normalized_title = normalize_text(title)

        for link_text, link_url in link_candidates:
            if link_text == normalized_title:
                matched_url = link_url
                break

        if not matched_url:
            for link_text, link_url in link_candidates:
                if normalized_title in link_text or link_text in normalized_title:
                    matched_url = link_url
                    break

        if not matched_url:
            matched_url = SITE_URLS["azvak"]

        vacancies.append(Vacancy("azvak", title, matched_url, published_date))
        seen_titles.add(title)

    logger.info("AZVAK FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def parse_hellojob() -> List[Vacancy]:
    html_text = fetch_html(SITE_URLS["hellojob"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []

    for a in soup.select('a[href*="/is-elanlari/"]'):
        href = a.get("href") or ""
        title = clean_title(a.get_text(" ", strip=True))

        if not href or not title:
            continue
        if "/is-elanlari/huquq" in href:
            continue
        if looks_like_noise(title):
            continue

        url = absolute_url("https://www.hellojob.az", href)
        context = clean_title(a.parent.get_text(" ", strip=True)) if a.parent else title
        if a.parent and a.parent.parent:
            context += " " + clean_title(a.parent.parent.get_text(" ", strip=True))
        published_date = extract_dates_from_text(context)

        vacancies.append(Vacancy("hellojob", title, url, published_date))

    return deduplicate_vacancies(vacancies)


def collect_all_vacancies() -> Dict[str, List[Vacancy]]:
    result = {
        "jobsearch": parse_jobsearch(),
        "busy": parse_busy(),
        "glorri": parse_glorri(),
        "smartjob": parse_smartjob(),
        "azvak": parse_azvak(),
        "hellojob": parse_hellojob(),
    }

    for site, items in result.items():
        logger.info("SITE %s FOUND %s", site, len(items))
        for item in items[:10]:
            logger.info("SITE %s ITEM %s | %s", site, item.title, item.url)

    return result


def get_main_menu_keyboard():
    keyboard = [
        ["Искать вакансии", "Обновить вакансии"],
        ["Старые вакансии", "Помощь"],
        ["Start", "Отмена"],
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=True,
        is_persistent=False,
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
        one_time_keyboard=True,
        is_persistent=False,
    )


WELCOME_TEXT = (
    "Добро пожаловать в бот для поиска вакансий юриста.\n\n"
    "Что умеет бот:\n"
    "1. Искать новые вакансии\n"
    "2. Обновлять вакансии\n"
    "3. Показывать архив по сайтам за последние 2 месяца\n\n"
    "Кнопка Start оставлена на случай, если бесплатный хостинг уснет."
)

HELP_TEXT = (
    "Как пользоваться ботом:\n\n"
    "1. Нажми Start, если бот уснул\n"
    "2. Нажми 'Искать вакансии' для быстрого поиска\n"
    "3. Нажми 'Обновить вакансии', чтобы заново собрать вакансии и сохранить новые\n"
    "4. Нажми 'Старые вакансии', затем выбери сайт\n\n"
    "В архиве название вакансии — синяя кликабельная ссылка.\n"
    "Показываются вакансии только за последние 2 месяца."
)


def format_vacancy_lines_html(vacancies: List[Dict], empty_text: str) -> str:
    if not vacancies:
        return html.escape(empty_text)

    lines = []
    for idx, item in enumerate(vacancies, start=1):
        display_date = item["published_date"] or item["found_date"]
        date_str = display_date.strftime("%Y-%m-%d") if display_date else "дата не указана"

        safe_title = html.escape(item["title"])
        safe_site = html.escape(SITE_LABELS.get(item["site"], item["site"]))
        safe_url = html.escape(item["url"])

        lines.append(
            f"{idx}. <a href=\"{safe_url}\">{safe_title}</a>\n"
            f"Сайт: {safe_site}\n"
            f"Дата: {date_str}"
        )
    return "\n\n".join(lines)


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

    recent = get_recent_vacancies(limit=1000)
    text = format_vacancy_lines_html(recent, "Свежих вакансий пока не найдено.")

    header = (
        f"Поиск завершен. Найдено: {len(all_vacancies)}\n"
        f"Новых сохранено: {inserted}\n\n"
    )

    for chunk in split_long_message(header + text):
        await update.message.reply_text(
            chunk,
            reply_markup=get_main_menu_keyboard(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

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
        await update.message.reply_text(
            "Действие отменено. Ты в главном меню.",
            reply_markup=get_main_menu_keyboard(),
        )
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
    rows = get_recent_vacancies_by_site(site, limit=1000)
    body = format_vacancy_lines_html(
        rows,
        f"По сайту {text} вакансий за последние 2 месяца пока нет."
    )

    for chunk in split_long_message(body):
        await update.message.reply_text(
            chunk,
            reply_markup=get_old_jobs_keyboard(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

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
        await update.message.reply_text(
            "Действие отменено. Ты в главном меню.",
            reply_markup=get_main_menu_keyboard(),
        )
        return MAIN_MENU

    await update.message.reply_text(
        "Нажми нужную кнопку в меню.",
        reply_markup=get_main_menu_keyboard(),
    )
    return MAIN_MENU


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
