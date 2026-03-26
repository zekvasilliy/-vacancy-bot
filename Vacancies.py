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

LANG_MENU, MAIN_MENU, OLD_JOBS_MENU = range(3)

SITE_LABELS = {
    "jobsearch": "JobSearch",
    "busy": "Busy.az",
    "glorri": "Glorri",
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
    "banking lawyer",
    "finance lawyer",
    "banking & finance lawyer",
    "compliance",
    "compliance specialist",
    "contract manager",
    "contract management",
    "legal officer",
    "legal associate",
    "junior lawyer",
    "senior lawyer",
    "attorney",
    "paralegal",
    "vəkil",
    "hüquq",
    "huquq",
]

SITE_URLS = {
    "jobsearch": "https://classic.jobsearch.az/vacancies?category=1375",
    "busy_professions": [
        "https://busy.az/professions/huquqsunas",
        "https://busy.az/professions/huquq-meslehetcisi",
        "https://busy.az/professions/huquq-sobesinin-mutexessisi",
        "https://busy.az/professions/lawyer",
        "https://busy.az/professions/bas-huquqsunas",
        "https://busy.az/professions/huquqsunas-komekcisi",
        "https://busy.az/professions/legal-specialist",
        "https://busy.az/professions/legal-counsel",
        "https://busy.az/professions/compliance-specialist",
        "https://busy.az/professions/corporate-lawyer",
    ],
    "glorri": "https://jobs.glorri.com/?jobFunctions=legal-services",
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
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

TEXTS = {
    "ru": {
        "choose_language": "Choose a language:",
        "welcome": (
            "Добро пожаловать в бот для поиска вакансий юриста.\n\n"
            "Этот бот работает по кнопке Start.\n"
            "После нажатия Start нужно подождать примерно 1 минуту, пока бот проснется.\n"
            "При каждом новом использовании тоже нужно нажимать Start, чтобы бот проснулся.\n\n"
            "Что умеет бот:\n"
            "1. Искать вакансии\n"
            "2. Менять язык\n"
            "3. Показывать архив вакансий по сайтам за последние 2 месяца"
        ),
        "help": (
            "Помощь:\n\n"
            "Этот бот работает по кнопке Start.\n"
            "После нажатия Start нужно подождать примерно 1 минуту, пока бот проснется.\n"
            "При каждом новом использовании нужно снова нажимать Start, чтобы бот проснулся.\n\n"
            "Как пользоваться:\n"
            "1. Нажми Start\n"
            "2. Нажми 'Искать вакансии' для быстрого поиска\n"
            "3. Нажми 'Сменить язык', чтобы переключить язык бота\n"
            "4. Нажми 'Старые вакансии', затем выбери сайт\n\n"
            "В архиве название вакансии — синяя кликабельная ссылка.\n"
            "Показываются вакансии только за последние 2 месяца."
        ),
        "bot_awake": "Бот проснулся. Ты в главном меню.",
        "searching": "Ищу вакансии по всем сайтам. Подожди немного.",
        "search_done": "Поиск завершен. Найдено: {found}\nНовых сохранено: {inserted}\n\n",
        "old_jobs_prompt": "Выбери сайт, чтобы открыть архив вакансий за последние 2 месяца.",
        "empty_recent": "Свежих вакансий пока не найдено.",
        "empty_site": "По сайту {site} вакансий за последние 2 месяца пока нет.",
        "main_menu": "Главное меню:",
        "cancelled": "Действие отменено. Ты в главном меню.",
        "press_button": "Нажми нужную кнопку в меню.",
        "pick_site_button": "Выбери сайт кнопкой.",
        "site_label": "Сайт",
        "date_label": "Дата",
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
    },
    "az": {
        "choose_language": "Choose a language:",
        "welcome": (
            "Hüquqşünas vakansiyalarını axtaran bota xoş gəlmisiniz.\n\n"
            "Bu bot Start düyməsi ilə işləyir.\n"
            "Start düyməsinə basdıqdan sonra botun oyanması üçün təxminən 1 dəqiqə gözləmək lazımdır.\n"
            "Hər yeni istifadədə də botun oyanması üçün yenidən Start düyməsinə basmaq lazımdır.\n\n"
            "Bot bunları edə bilir:\n"
            "1. Vakansiyaları axtarmaq\n"
            "2. Dili dəyişmək\n"
            "3. Son 2 ay üzrə saytlar üzrə vakansiya arxivini göstərmək"
        ),
        "help": (
            "Kömək:\n\n"
            "Bu bot Start düyməsi ilə işləyir.\n"
            "Start düyməsinə basdıqdan sonra botun oyanması üçün təxminən 1 dəqiqə gözləmək lazımdır.\n"
            "Hər yeni istifadədə botun oyanması üçün yenidən Start düyməsinə basmaq lazımdır.\n\n"
            "İstifadə qaydası:\n"
            "1. Start düyməsinə basın\n"
            "2. Sürətli axtarış üçün 'Vakansiyaları axtar' düyməsinə basın\n"
            "3. Botun dilini dəyişmək üçün 'Dili dəyiş' düyməsinə basın\n"
            "4. 'Köhnə vakansiyalar' düyməsinə basın, sonra saytı seçin\n\n"
            "Arxivdə vakansiyanın adı mavi kliklənə bilən keçiddir.\n"
            "Yalnız son 2 ayın vakansiyaları göstərilir."
        ),
        "bot_awake": "Bot oyandı. Siz əsas menyudasınız.",
        "searching": "Bütün saytlar üzrə vakansiyalar axtarılır. Bir az gözləyin.",
        "search_done": "Axtarış tamamlandı. Tapıldı: {found}\nYeni saxlanıldı: {inserted}\n\n",
        "old_jobs_prompt": "Son 2 ay üzrə vakansiya arxivini açmaq üçün saytı seçin.",
        "empty_recent": "Hələlik yeni vakansiya tapılmadı.",
        "empty_site": "{site} saytı üzrə son 2 ayda vakansiya yoxdur.",
        "main_menu": "Əsas menyu:",
        "cancelled": "Əməliyyat ləğv edildi. Siz əsas menyudasınız.",
        "press_button": "Menyudan uyğun düyməni seçin.",
        "pick_site_button": "Saytı düymə ilə seçin.",
        "site_label": "Sayt",
        "date_label": "Tarix",
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
    },
    "en": {
        "choose_language": "Choose a language:",
        "welcome": (
            "Welcome to the lawyer vacancy search bot.\n\n"
            "This bot works with the Start button.\n"
            "After pressing Start, you need to wait about 1 minute for the bot to wake up.\n"
            "Each time you use the bot again, you should press Start so that the bot wakes up.\n\n"
            "What this bot can do:\n"
            "1. Search vacancies\n"
            "2. Change language\n"
            "3. Show vacancy archive by website for the last 2 months"
        ),
        "help": (
            "Help:\n\n"
            "This bot works with the Start button.\n"
            "After pressing Start, you need to wait about 1 minute for the bot to wake up.\n"
            "Each time you use the bot again, you should press Start so that the bot wakes up.\n\n"
            "How to use:\n"
            "1. Press Start\n"
            "2. Press 'Search vacancies' for a quick search\n"
            "3. Press 'Change language' to switch the bot language\n"
            "4. Press 'Old vacancies', then choose a website\n\n"
            "In the archive, the vacancy title is a blue clickable link.\n"
            "Only vacancies from the last 2 months are shown."
        ),
        "bot_awake": "The bot is awake. You are in the main menu.",
        "searching": "Searching vacancies across all websites. Please wait.",
        "search_done": "Search completed. Found: {found}\nNew saved: {inserted}\n\n",
        "old_jobs_prompt": "Choose a website to open the vacancy archive for the last 2 months.",
        "empty_recent": "No fresh vacancies found yet.",
        "empty_site": "No vacancies found for {site} in the last 2 months.",
        "main_menu": "Main menu:",
        "cancelled": "Action cancelled. You are in the main menu.",
        "press_button": "Press the needed button in the menu.",
        "pick_site_button": "Choose a website using the button.",
        "site_label": "Site",
        "date_label": "Date",
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
    },
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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_users (
                    telegram_id BIGINT PRIMARY KEY,
                    first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_seen TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
        conn.commit()


def register_user(update: Update):
    user = update.effective_user
    if not user:
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_users (telegram_id, first_seen, last_seen)
                VALUES (%s, NOW(), NOW())
                ON CONFLICT (telegram_id)
                DO UPDATE SET last_seen = NOW()
                """,
                (user.id,),
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

    m = re.search(r"(\d+)\s*gün", raw)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))

    m = re.search(r"(\d+)\s*day", raw)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))

    m = re.search(r"(\d+)\s*days", raw)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))

    m = re.search(r"(\d+)\s*дн", raw)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))

    return None


def parse_date_loose(text: str) -> Optional[date]:
    if not text:
        return None

    raw = text.strip().lower()
    today = date.today()

    if "bu gün" in raw or "bugün" in raw or raw == "today" or "today" in raw or "сегодня" in raw:
        return today
    if "dünən" in raw or "dunen" in raw or raw == "yesterday" or "yesterday" in raw or "вчера" in raw:
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
        response = requests.get(url, headers=HEADERS, timeout=35)
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
        "tam iş günü", "razılaşma yolu ilə", "full time", "part time",
        "internship", "remote", "hybrid", "all vacancies", "all jobs",
        "iş elanları", "jobs", "job", "vacancy", "vakansiya"
    }
    return t in noise or len(t) < 3


def deduplicate_vacancies(vacancies: List[Vacancy]) -> List[Vacancy]:
    seen = set()
    result = []

    for v in vacancies:
        normalized_title = normalize_text(v.title)
        cleaned_url = v.url.rstrip("/")
        key = (v.site, normalized_title, cleaned_url)

        if key in seen:
            continue
        seen.add(key)

        if looks_like_noise(v.title):
            continue
        if not is_legal_vacancy(v.title):
            continue
        if not cleaned_url:
            continue
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

        key = (normalize_text(title), url.rstrip("/"))
        if key in seen:
            continue

        vacancies.append(Vacancy("jobsearch", title, url, published_date))
        seen.add(key)

    return deduplicate_vacancies(vacancies)


def extract_busy_date_from_context(text: str) -> Optional[date]:
    raw = clean_title(text)

    patterns = [
        r"(\d{2}[./-]\d{2}[./-]\d{4})",
        r"(\d{4}[./-]\d{2}[./-]\d{2})",
        r"(bugün|bu gün|dünən|\d+\s+gün əvvəl)",
        r"(today|yesterday|\d+\s+days?\s+ago)",
    ]

    for pattern in patterns:
        m = re.search(pattern, raw, re.IGNORECASE)
        if m:
            parsed = parse_date_loose(m.group(1))
            if parsed:
                return parsed

    return extract_dates_from_text(raw)


def parse_busy_page(url: str) -> List[Vacancy]:
    html_text = fetch_html(url)
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []
    seen = set()

    selectors = [
        'a[href*="/vacancy/"]',
        'a[href*="/jobs/"]',
        'a[href^="/vacancy/"]',
    ]

    links = []
    for selector in selectors:
        links.extend(soup.select(selector))

    for a in links:
        href = (a.get("href") or "").strip()
        title = clean_title(a.get_text(" ", strip=True))

        if not href or not title:
            continue
        if looks_like_noise(title):
            continue
        if not is_legal_vacancy(title):
            continue

        full_url = absolute_url("https://busy.az", href)
        published_date = None

        containers = []
        if a.parent:
            containers.append(a.parent)
        if a.parent and a.parent.parent:
            containers.append(a.parent.parent)
        if a.parent and a.parent.parent and a.parent.parent.parent:
            containers.append(a.parent.parent.parent)
        if a.parent and a.parent.parent and a.parent.parent.parent and a.parent.parent.parent.parent:
            containers.append(a.parent.parent.parent.parent)

        for container in containers:
            context = clean_title(container.get_text(" ", strip=True))
            parsed = extract_busy_date_from_context(context)
            if parsed:
                published_date = parsed
                break

        key = (normalize_text(title), full_url.rstrip("/"))
        if key in seen:
            continue

        vacancies.append(Vacancy("busy", title, full_url, published_date))
        seen.add(key)

    return deduplicate_vacancies(vacancies)


def parse_busy() -> List[Vacancy]:
    items = []
    for url in SITE_URLS["busy_professions"]:
        try:
            items.extend(parse_busy_page(url))
        except Exception as e:
            logger.error("BUSY parse error for %s: %s", url, e)

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
        for j in range(i + 1, min(i + 8, len(lines))):
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

    return deduplicate_vacancies(vacancies)


def parse_hellojob() -> List[Vacancy]:
    html_text = fetch_html(SITE_URLS["hellojob"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []
    seen = set()

    selectors = [
        'a[href*="/is-elanlari/"]',
        'a[href^="/is-elanlari/"]',
        'a[href*="vakans"]',
    ]

    links = []
    for selector in selectors:
        links.extend(soup.select(selector))

    for a in links:
        href = (a.get("href") or "").strip()
        title = clean_title(a.get_text(" ", strip=True))

        if not href or not title:
            continue
        if href.rstrip("/") == "/is-elanlari/huquq":
            continue
        if looks_like_noise(title):
            continue
        if not is_legal_vacancy(title):
            continue

        url = absolute_url("https://www.hellojob.az", href)
        published_date = None

        containers = []
        if a.parent:
            containers.append(a.parent)
        if a.parent and a.parent.parent:
            containers.append(a.parent.parent)
        if a.parent and a.parent.parent and a.parent.parent.parent:
            containers.append(a.parent.parent.parent)
        if a.parent and a.parent.parent and a.parent.parent.parent and a.parent.parent.parent.parent:
            containers.append(a.parent.parent.parent.parent)

        for container in containers:
            context = clean_title(container.get_text(" ", strip=True))
            parsed = extract_dates_from_text(context)
            if parsed:
                published_date = parsed
                break

        key = (normalize_text(title), url.rstrip("/"))
        if key in seen:
            continue

        vacancies.append(Vacancy("hellojob", title, url, published_date))
        seen.add(key)

    return deduplicate_vacancies(vacancies)


def collect_all_vacancies() -> Dict[str, List[Vacancy]]:
    return {
        "jobsearch": parse_jobsearch(),
        "busy": parse_busy(),
        "glorri": parse_glorri(),
        "azvak": parse_azvak(),
        "hellojob": parse_hellojob(),
    }


def get_lang(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get("lang", "ru")


def t(context: ContextTypes.DEFAULT_TYPE, key: str) -> str:
    lang = get_lang(context)
    return TEXTS.get(lang, TEXTS["ru"]).get(key, key)


def get_language_keyboard():
    keyboard = [
        [TEXTS["ru"]["lang_btn_ru"]],
        [TEXTS["ru"]["lang_btn_az"]],
        [TEXTS["ru"]["lang_btn_en"]],
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
        [t(context, "old_btn"), t(context, "help_btn")],
        [t(context, "start_btn"), t(context, "cancel_btn")],
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=False,
    )


def get_old_jobs_keyboard(context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        ["JobSearch", "Busy.az"],
        ["Glorri", "Azvak"],
        ["HelloJob"],
        [t(context, "start_btn"), t(context, "back_btn"), t(context, "cancel_btn")],
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

        lines.append(
            f"{idx}. <a href=\"{safe_url}\">{safe_title}</a>\n"
            f"{html.escape(t(context, 'site_label'))}: {safe_site}\n"
            f"{html.escape(t(context, 'date_label'))}: {date_str}"
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


async def open_language_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)
    await update.message.reply_text(
        TEXTS["en"]["choose_language"] if "lang" not in context.user_data else t(context, "choose_language"),
        reply_markup=get_language_keyboard(),
    )
    return LANG_MENU


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)

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


async def choose_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)
    selected = resolve_language_choice(update.message.text)

    if not selected:
        await update.message.reply_text(
            TEXTS["en"]["choose_language"],
            reply_markup=get_language_keyboard(),
        )
        return LANG_MENU

    context.user_data["lang"] = selected

    await update.message.reply_text(
        t(context, "welcome"),
        reply_markup=get_main_menu_keyboard(context),
    )
    return MAIN_MENU


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)

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


async def wake_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)

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


async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)

    await update.message.reply_text(
        t(context, "searching"),
        reply_markup=get_main_menu_keyboard(context),
    )

    collected = collect_all_vacancies()
    all_vacancies: List[Vacancy] = []
    for items in collected.values():
        all_vacancies.extend(items)

    inserted = save_vacancies(all_vacancies)
    cleanup_old_vacancies()

    recent = get_recent_vacancies(limit=1000)
    text = format_vacancy_lines_html(recent, t(context, "empty_recent"), context)
    header = t(context, "search_done").format(found=len(all_vacancies), inserted=inserted)

    for chunk in split_long_message(header + text):
        await update.message.reply_text(
            chunk,
            reply_markup=get_main_menu_keyboard(context),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    return MAIN_MENU


async def open_old_jobs_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)
    await update.message.reply_text(
        t(context, "old_jobs_prompt"),
        reply_markup=get_old_jobs_keyboard(context),
    )
    return OLD_JOBS_MENU


async def old_jobs_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)
    text = normalize_button(update.message.text)

    if text == normalize_button(t(context, "start_btn")):
        return await wake_to_main_menu(update, context)

    if text == normalize_button(t(context, "back_btn")):
        await update.message.reply_text(
            t(context, "main_menu"),
            reply_markup=get_main_menu_keyboard(context),
        )
        return MAIN_MENU

    if text == normalize_button(t(context, "cancel_btn")):
        await update.message.reply_text(
            t(context, "cancelled"),
            reply_markup=get_main_menu_keyboard(context),
        )
        return MAIN_MENU

    map_text_to_site = {
        "JobSearch": "jobsearch",
        "Busy.az": "busy",
        "Glorri": "glorri",
        "Azvak": "azvak",
        "HelloJob": "hellojob",
    }

    if update.message.text not in map_text_to_site:
        await update.message.reply_text(
            t(context, "pick_site_button"),
            reply_markup=get_old_jobs_keyboard(context),
        )
        return OLD_JOBS_MENU

    site = map_text_to_site[update.message.text]
    rows = get_recent_vacancies_by_site(site, limit=1000)
    body = format_vacancy_lines_html(
        rows,
        t(context, "empty_site").format(site=update.message.text),
        context,
    )

    for chunk in split_long_message(body):
        await update.message.reply_text(
            chunk,
            reply_markup=get_old_jobs_keyboard(context),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    return OLD_JOBS_MENU


async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update)
    text = normalize_button(update.message.text)

    if text == normalize_button(t(context, "start_btn")):
        return await wake_to_main_menu(update, context)

    if text == normalize_button(t(context, "search_btn")):
        return await handle_search(update, context)

    if text == normalize_button(t(context, "change_lang_btn")):
        return await open_language_menu(update, context)

    if text == normalize_button(t(context, "old_btn")):
        return await open_old_jobs_menu(update, context)

    if text == normalize_button(t(context, "help_btn")):
        return await help_command(update, context)

    if text == normalize_button(t(context, "cancel_btn")):
        await update.message.reply_text(
            t(context, "cancelled"),
            reply_markup=get_main_menu_keyboard(context),
        )
        return MAIN_MENU

    await update.message.reply_text(
        t(context, "press_button"),
        reply_markup=get_main_menu_keyboard(context),
    )
    return MAIN_MENU


def main():
    init_db()
    cleanup_old_vacancies()

    app = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex(r"(?i)^start$"), wake_to_main_menu),
        ],
        states={
            LANG_MENU: [
                MessageHandler(filters.Regex(r"(?i)^start$"), wake_to_main_menu),
                MessageHandler(filters.TEXT & ~filters.COMMAND, choose_language),
            ],
            MAIN_MENU: [
                MessageHandler(filters.Regex(r"(?i)^start$"), wake_to_main_menu),
                MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu_handler),
            ],
            OLD_JOBS_MENU: [
                MessageHandler(filters.Regex(r"(?i)^start$"), wake_to_main_menu),
                MessageHandler(filters.TEXT & ~filters.COMMAND, old_jobs_menu_handler),
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex(r"(?i)^start$"), wake_to_main_menu),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("language", open_language_menu))

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
