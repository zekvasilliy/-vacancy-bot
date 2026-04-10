import os

import re

import html

import time

import logging

import hashlib

import asyncio

from datetime import date, timedelta, datetime

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

    "hüquq üzrə mütəxəxəssis",

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

            "Нажмите нужную кнопку ниже и подождите около 1 минуты, пока бот проснется.\n\n"

            "Бот ищет юридические вакансии и хранит архив вакансий по сайтам за последние 30 дней."

        ),

        "help": (

            "Помощь:\n\n"

            "Нажмите нужную кнопку ниже и подождите около 1 минуты, пока бот проснется.\n\n"

            "Бот ищет юридические вакансии и хранит архив вакансий по сайтам за последние 30 дней.\n"

            "В архиве название вакансии — синяя кликабельная ссылка."

        ),

        "bot_awake": "Бот проснулся. Нажмите нужную кнопку ниже.",

        "searching": "Ищу вакансии по всем сайтам. Подождите немного.",

        "search_done": "Поиск завершен. Найдено: {found}\nНовых сохранено: {inserted}\n\n",

        "old_jobs_prompt": "Выбери сайт, чтобы открыть архив вакансий за последние 30 дней.",

        "empty_recent": "Свежих вакансий пока не найдено.",

        "empty_site": "По сайту {site} вакансий за последние 30 дней пока нет.",

        "main_menu": "Главное меню:",

        "cancelled": "Действие отменено. Ты в главном меню.",

        "press_button": "Нажми нужную кнопку в меню.",

        "pick_site_button": "Выбери сайт кнопкой.",

        "site_label": "Сайт",

        "date_label": "Дата",

        "search_busy": "⏳ Другой пользователь уже обновляет вакансии. Попробуй снова через 1–2 минуты.",

        "using_cache": "📋 Показываю свежий кэш вакансий. Обновление было меньше 1 часа назад.\n\n",

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

            "Aşağıdakı uyğun düyməni seçin və botun oyanması üçün təxminən 1 dəqiqə gözləyin.\n\n"

            "Bot hüquq üzrə vakansiyaları axtarır və saytlar üzrə son 30 günün vakansiya arxivini saxlayır."

        ),

        "help": (

            "Kömək:\n\n"

            "Aşağıdakı uyğun düyməni seçin və botun oyanması üçün təxminən 1 dəqiqə gözləyin.\n\n"

            "Bot hüquq üzrə vakansiyaları axtarır və saytlar üzrə son 30 günün vakansiya arxivini saxlayır.\n"

            "Arxivdə vakansiyanın adı mavi kliklənə bilən keçiddir."

        ),

        "bot_awake": "Bot oyandı. Aşağıdakı uyğun düyməni seçin.",

        "searching": "Bütün saytlar üzrə vakansiyalar axtarılır. Bir az gözləyin.",

        "search_done": "Axtarış tamamlandı. Tapıldı: {found}\nYeni saxlanıldı: {inserted}\n\n",

        "old_jobs_prompt": "Son 30 gün üzrə vakansiya arxivini açmaq üçün saytı seçin.",

        "empty_recent": "Hələlik yeni vakansiya tapılmadı.",

        "empty_site": "{site} saytı üzrə son 30 gündə vakansiya yoxdur.",

        "main_menu": "Əsas menyu:",

        "cancelled": "Əməliyyat ləğv edildi. Siz əsas menyudasınız.",

        "press_button": "Menyudan uyğun düyməni seçin.",

        "pick_site_button": "Saytı düymə ilə seçin.",

        "site_label": "Sayt",

        "date_label": "Tarix",

        "search_busy": "⏳ Başqa istifadəçi artıq vakansiyaları yeniləyir. 1–2 dəqiqədən sonra yenidən cəhd edin.",

        "using_cache": "📋 Sizə vakansiyaların təzə keş versiyası göstərilir. Yenilənmə 1 saatdan az əvvəl olub.\n\n",

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

            "Press the needed button below and wait about 1 minute for the bot to wake up.\n\n"

            "The bot searches legal vacancies and keeps a website-based archive for the last 30 days."

        ),

        "help": (

            "Help:\n\n"

            "Press the needed button below and wait about 1 minute for the bot to wake up.\n\n"

            "The bot searches legal vacancies and keeps a website-based archive for the last 30 days.\n"

            "In the archive, the vacancy title is a blue clickable link."

        ),

        "bot_awake": "The bot is awake. Press the needed button below.",

        "searching": "Searching vacancies across all websites. Please wait.",

        "search_done": "Search completed. Found: {found}\nNew saved: {inserted}\n\n",

        "old_jobs_prompt": "Choose a website to open the vacancy archive for the last 30 days.",

        "empty_recent": "No fresh vacancies found yet.",

        "empty_site": "No vacancies found for {site} in the last 30 days.",

        "main_menu": "Main menu:",

        "cancelled": "Action cancelled. You are in the main menu.",

        "press_button": "Press the needed button in the menu.",

        "pick_site_button": "Choose a website using the button.",

        "site_label": "Site",

        "date_label": "Date",

        "search_busy": "⏳ Another user is already updating vacancies. Please try again in 1–2 minutes.",

        "using_cache": "📋 Showing fresh cached vacancies. The last update was less than 1 hour ago.\n\n",

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



CACHE_TTL = timedelta(hours=1)
SEARCH_BUTTON_REGEX = r"^(?:Искать вакансии|Vakansiyaları axtar|Search vacancies)$"
parsing_lock = asyncio.Lock()
cache_payload: Optional[Dict] = None
cache_time: Optional[datetime] = None



class Vacancy:

    def __init__(self, site: str, title: str, url: str, published_date: Optional[date]):

        self.site = site

        self.title = title.strip()

        self.url = url.strip()

        self.published_date = published_date



    @property

    def unique_hash(self) -> str:

        raw = build_vacancy_storage_key(self.site, self.title, self.url)

        return hashlib.sha256(raw.encode("utf-8")).hexdigest()





def get_connection(max_attempts: int = 5, base_delay: int = 2):
    last_exc = None

    for attempt in range(1, max_attempts + 1):
        try:
            return psycopg.connect(
                DATABASE_URL,
                connect_timeout=10,
            )
        except psycopg.OperationalError as exc:
            last_exc = exc
            msg = str(exc).lower()

            retriable = (
                "temporary failure in name resolution" in msg
                or "could not translate host name" in msg
                or "connection refused" in msg
                or "timeout expired" in msg
                or "server closed the connection unexpectedly" in msg
            )

            if not retriable or attempt == max_attempts:
                logger.error(
                    "DB connection failed finally on attempt %s/%s: %s",
                    attempt,
                    max_attempts,
                    exc,
                )
                raise

            wait_seconds = min(base_delay * attempt, 10)
            logger.warning(
                "DB connection failed on attempt %s/%s: %s. Retry in %s sec",
                attempt,
                max_attempts,
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)

    raise last_exc





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

                    user_id BIGINT PRIMARY KEY,

                    first_seen TIMESTAMPTZ,

                    last_seen TIMESTAMPTZ

                )

                """

            )

            cur.execute("ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS first_seen TIMESTAMPTZ")

            cur.execute("ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ")

            cur.execute(

                """

                UPDATE bot_users

                SET first_seen = COALESCE(first_seen, NOW()),

                    last_seen = COALESCE(last_seen, NOW())

                WHERE first_seen IS NULL OR last_seen IS NULL

                """

            )

            cur.execute("ALTER TABLE bot_users ALTER COLUMN first_seen SET DEFAULT NOW()")

            cur.execute("ALTER TABLE bot_users ALTER COLUMN last_seen SET DEFAULT NOW()")

        conn.commit()



def save_user(user_id: Optional[int]):

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



def cleanup_old_vacancies():

    border = date.today() - timedelta(days=30)

    with get_connection() as conn:

        with conn.cursor() as cur:

            cur.execute(

                "DELETE FROM vacancies WHERE COALESCE(published_date, found_date) < %s",

                (border,),

            )

        conn.commit()





def cleanup_duplicate_vacancies():

    border = date.today() - timedelta(days=30)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, site, title, url, published_date, found_date
                FROM vacancies
                WHERE COALESCE(published_date, found_date) >= %s
                ORDER BY id DESC
                """,
                (border,),
            )
            rows = cur.fetchall()

            seen = set()
            ids_to_delete = []

            for row_id, site, title, url, published_date, found_date in rows:
                key = build_vacancy_storage_key(site, title, url)
                if key in seen:
                    ids_to_delete.append(row_id)
                    continue
                seen.add(key)

            if ids_to_delete:
                cur.execute(
                    "DELETE FROM vacancies WHERE id = ANY(%s)",
                    (ids_to_delete,),
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

    border = date.today() - timedelta(days=30)

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



    return deduplicate_vacancy_rows(rows)



def get_recent_vacancies_by_site(site: str, limit: int = 1000) -> List[Dict]:

    border = date.today() - timedelta(days=30)

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



    return deduplicate_vacancy_rows(rows)



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

    return vacancy_date >= date.today() - timedelta(days=30)





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
    ]

    for pattern in patterns:
        m = re.search(pattern, cleaned, re.IGNORECASE)
        if m:
            return m.group(1)

    return None



def canonicalize_job_url(site: str, url: str) -> str:

    cleaned = ((url or "").strip().split("#", 1)[0].split("?", 1)[0]).rstrip("/")
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

    return cleaned_lower



def build_vacancy_storage_key(site: str, title: str, url: str) -> str:

    canonical_url = canonicalize_job_url(site, url)
    normalized_title = normalize_text(title)

    if canonical_url:
        return f"{site}|{canonical_url}"

    return f"{site}|{normalized_title}|{(url or '').strip().rstrip('/').lower()}"



def deduplicate_vacancy_rows(rows) -> List[Dict]:

    result = []
    seen = set()

    for site, title, url, published_date, found_date in rows:
        canonical_url = canonicalize_job_url(site, url)
        normalized_title = normalize_text(title)
        key = canonical_url or f"{site}|{normalized_title}|{published_date or found_date}"

        if key in seen:
            continue

        seen.add(key)
        result.append({
            "site": site,
            "title": title,
            "url": url,
            "published_date": published_date,
            "found_date": found_date,
        })

    return result



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
        canonical_url = canonicalize_job_url(v.site, cleaned_url)
        key = canonical_url or f"{v.site}|{normalized_title}|{v.published_date}"

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



    logger.info("JOBSEARCH FOUND %s", len(vacancies))

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



    logger.info("BUSY PAGE %s FOUND %s", url, len(vacancies))

    return deduplicate_vacancies(vacancies)





def parse_busy() -> List[Vacancy]:

    items = []



    for url in SITE_URLS["busy_professions"]:

        try:

            parsed = parse_busy_page(url)

            items.extend(parsed)

            time.sleep(0.4)

        except Exception as e:

            logger.error("BUSY parse error for %s: %s", url, e)



    logger.info("BUSY TOTAL FOUND %s", len(items))

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



    logger.info("GLORRI FOUND %s", len(vacancies))

    return deduplicate_vacancies(vacancies)





def parse_azvak() -> List[Vacancy]:

    html_text = fetch_html(SITE_URLS["azvak"])
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    vacancies: List[Vacancy] = []
    seen = set()

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
        canonical_url = canonicalize_job_url("azvak", full_url)
        if not canonical_url:
            continue
        if canonical_url in seen:
            continue

        published_date = None
        container = a
        for _ in range(6):
            if not container:
                break
            context = clean_title(container.get_text(" ", strip=True))
            parsed = extract_dates_from_text(context)
            if parsed:
                published_date = parsed
                break
            container = container.parent

        seen.add(canonical_url)
        vacancies.append(Vacancy("azvak", title, full_url, published_date))

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

            container = container.parent

        if not title:
            continue
        if looks_like_noise(title) or not is_legal_vacancy(title):
            continue

        key = (vacancy_id, normalize_text(title))
        if key in seen:
            continue

        seen.add(key)
        vacancies.append(Vacancy("hellojob", title, url, published_date))

    logger.info("HELLOJOB FOUND %s", len(vacancies))
    return deduplicate_vacancies(vacancies)


def collect_all_vacancies() -> Dict[str, List[Vacancy]]:

    result = {

        "jobsearch": parse_jobsearch(),

        "busy": parse_busy(),

        "glorri": parse_glorri(),

        "azvak": parse_azvak(),

        "hellojob": parse_hellojob(),

    }



    for site, items in result.items():

        logger.info("SITE %s FOUND %s", site, len(items))

        for item in items[:10]:

            logger.info("SITE %s ITEM %s | %s", site, item.title, item.url)



    return result





def get_lang(context: ContextTypes.DEFAULT_TYPE) -> str:

    return context.user_data.get("lang", "ru")





def t(context: ContextTypes.DEFAULT_TYPE, key: str) -> str:

    lang = get_lang(context)

    return TEXTS.get(lang, TEXTS["ru"]).get(key, key)





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

    keyboard = [

        ["JobSearch", "Busy.az"],

        ["Glorri", "Azvak"],

        ["HelloJob"],

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



def resolve_button_lang(text: str, key: str) -> Optional[str]:

    value = normalize_button(text)



    for lang in ("ru", "az", "en"):

        if value == normalize_button(TEXTS[lang][key]):

            return lang



    return None



def resolve_search_button_lang(text: str) -> Optional[str]:

    return resolve_button_lang(text, "search_btn")



def is_cache_fresh() -> bool:
    return cache_payload is not None and cache_time is not None and datetime.utcnow() - cache_time < CACHE_TTL


async def send_cached_result(update: Update, context: ContextTypes.DEFAULT_TYPE):
    header = t(context, "using_cache") + cache_payload["header"]
    text = format_vacancy_lines_html(cache_payload["recent"], t(context, "empty_recent"), context)

    for chunk in split_long_message(header + text):
        await update.message.reply_text(
            chunk,
            reply_markup=get_main_menu_keyboard(context),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )





SITE_BUTTON_TO_KEY = {
    "JobSearch": "jobsearch",
    "Busy.az": "busy",
    "Glorri": "glorri",
    "Azvak": "azvak",
    "HelloJob": "hellojob",
}


async def send_site_archive(update: Update, context: ContextTypes.DEFAULT_TYPE, site_button: str):
    site = SITE_BUTTON_TO_KEY[site_button]
    rows = get_recent_vacancies_by_site(site, limit=1000)
    body = format_vacancy_lines_html(
        rows,
        t(context, "empty_site").format(site=site_button),
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


async def universal_button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user.id if update.effective_user else None)
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


async def open_language_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):

    await update.message.reply_text(

        TEXTS["en"]["choose_language"],

        reply_markup=get_language_keyboard(),

    )

    return LANG_MENU





async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    save_user(update.effective_user.id if update.effective_user else None)

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

    save_user(update.effective_user.id if update.effective_user else None)

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

    save_user(update.effective_user.id if update.effective_user else None)

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





async def search_entrypoint(update: Update, context: ContextTypes.DEFAULT_TYPE):

    save_user(update.effective_user.id if update.effective_user else None)

    selected_lang = resolve_search_button_lang(update.message.text)

    if "lang" not in context.user_data and selected_lang:

        context.user_data["lang"] = selected_lang

    return await handle_search(update, context)



async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global cache_payload, cache_time

    if is_cache_fresh():
        await send_cached_result(update, context)
        return MAIN_MENU

    if parsing_lock.locked():
        await update.message.reply_text(
            t(context, "search_busy"),
            reply_markup=get_main_menu_keyboard(context),
        )
        return MAIN_MENU

    async with parsing_lock:
        if is_cache_fresh():
            await send_cached_result(update, context)
            return MAIN_MENU

        await update.message.reply_text(

            t(context, "searching"),

            reply_markup=get_main_menu_keyboard(context),

        )



        collected = collect_all_vacancies()

        all_vacancies: List[Vacancy] = []

        for _, items in collected.items():

            all_vacancies.extend(items)



        inserted = save_vacancies(all_vacancies)

        cleanup_old_vacancies()
        cleanup_duplicate_vacancies()

        recent = get_recent_vacancies(limit=1000)

        text = format_vacancy_lines_html(recent, t(context, "empty_recent"), context)



        header = t(context, "search_done").format(found=len(all_vacancies), inserted=inserted)

        cache_payload = {
            "header": header,
            "recent": recent,
        }
        cache_time = datetime.utcnow()

        for chunk in split_long_message(header + text):

            await update.message.reply_text(

                chunk,

                reply_markup=get_main_menu_keyboard(context),

                parse_mode="HTML",

                disable_web_page_preview=True,

            )



    return MAIN_MENU





async def open_old_jobs_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):

    await update.message.reply_text(

        t(context, "old_jobs_prompt"),

        reply_markup=get_old_jobs_keyboard(context),

    )

    return OLD_JOBS_MENU





async def old_jobs_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

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

            MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router),

        ],

        states={

            LANG_MENU: [

                MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router),

            ],

            MAIN_MENU: [

                MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router),

            ],

            OLD_JOBS_MENU: [

                MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router),

            ],

        },

        fallbacks=[

            CommandHandler("start", start),

            MessageHandler(filters.TEXT & ~filters.COMMAND, universal_button_router),

        ],

        allow_reentry=True,

    )



    app.add_handler(conv_handler)


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
