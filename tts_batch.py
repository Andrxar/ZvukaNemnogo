# tts_batch.py
# Скрипт для пакетной генерации TTS аудио из текста
# Поддерживает:
# - разбиение текста на фрагменты
# - генерацию аудио через API
# - конвертацию WAV в MP3
# - проверку размеров файлов
# - сборку ZIP архива
# - загрузку на Backblaze B2
# - автоматическое создание отдельного лог-файла для каждой книги
# - запись также в общий tts_batch.log для совместимости с workflow
# - финальная выгрузка остатка (если остались файлы после основного цикла)

import os
import sys
import datetime
import glob
import requests
import re
import zipfile
import hashlib
import json
import time
import base64
from tqdm import tqdm
from bs4 import BeautifulSoup

# ================== НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ ==================

# Название исходного текстового файла (может быть .txt или .fb2)
TEXT_FILE_NAME = "Nadejdin_Jizn_Naoborot.txt"

# Папка для готовых mp3 файлов
OUTPUT_MP3_DIR = "output_mp3"

# Временная папка для промежуточных wav файлов
TMP_AUDIO_DIR = "tmp_audio"

# Минимальный и максимальный размер mp3 файла в КБ
MIN_SIZE_KB = 15
MAX_SIZE_KB = 5000

# Лимит общей папки mp3 перед сборкой zip (в МБ)
AUDIO_SIZE_LIMIT_MB = 450

# Частота дискретизации аудио при конвертации (Гц)
SAMPLE_RATE_HZ = 24000

# Битрейт MP3 при конвертации из WAV (например: "64k", "96k", "128k")
MP3_BITRATE = "128k"

# Голоса и языки (можно расширять)
VOICES_DATA = {"voices": []}
LANGS_DATA = {"langs": []}

# Выбранный голос и язык (можно менять на свой)
VOICE_NAME = "Виталий"
LANG_NAME = "Русский"

# Параметры повторов (можно переопределить через окружение)
DEFAULT_RETRY_ATTEMPTS = int(os.environ.get("RETRY_ATTEMPTS", "20"))
DEFAULT_RETRY_DELAY = int(os.environ.get("RETRY_DELAY_SEC", "10"))

def env_value(name, default=None):
    value = os.environ.get(name)
    if value is None:
        return default
    value = value.strip()
    if value == "":
        return default
    return value

FREETTS_BASE_URL = "https://freetts.ru"
FREETTS_SYNTHESIS_URL = "https://freetts.ru/api/synthesis"
FREETTS_AUDIO_EXT = env_value("FREETTS_AUDIO_EXT", "mp3")
FREETTS_POLL_ATTEMPTS = int(env_value("FREETTS_POLL_ATTEMPTS", "30"))
FREETTS_POLL_DELAY = int(env_value("FREETTS_POLL_DELAY_SEC", "2"))
FREETTS_REQUEST_DELAY = int(env_value("FREETTS_REQUEST_DELAY_SEC", "3"))
FREETTS_TOKEN = env_value("FREETTS_TOKEN")
FREETTS_VOICE_ID = env_value("FREETTS_VOICE_ID")
FREETTS_LANG_CODE = env_value("FREETTS_LANG_CODE")
FREETTS_FALLBACK_VOICE_ID = env_value("FREETTS_FALLBACK_VOICE_ID", "VbNqRtKmLpOz")
FREETTS_FALLBACK_LANG_CODE = env_value("FREETTS_FALLBACK_LANG_CODE", "ru")
FREETTS_COOKIE = env_value("FREETTS_COOKIE")

# ----------------- ЛОГ-ФАЙЛЫ -----------------
BOOK_BASENAME = os.path.splitext(os.path.basename(TEXT_FILE_NAME))[0]
LOG_FILE = BOOK_BASENAME + ".log"
AUDIO_URLS_LOG = BOOK_BASENAME + "_audio_urls.jsonl"

def resolve_global_log_file(book_basename):
    """
    Возвращает наиболее подходящий глобальный лог для текущей книги.
    1) Если существует точный файл "tts_batch(book).log" — возвращаем его.
    2) Иначе ищем все файлы, начинающиеся с "tts_batch(book" и выбираем самый новый по времени изменения.
    3) Если ничего не найдено — возвращаем стандартное имя (оно будет создано при записи).
    """
    exact = f"tts_batch({book_basename}).log"
    if os.path.exists(exact):
        return exact
    # Ищем похожие (например могли быть суффиксы/вариации). Выбираем самый свежий.
    candidates = glob.glob(f"tts_batch({book_basename}*.log")
    if candidates:
        candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return candidates[0]
    # fallback — тот же формат, даже если файла ещё нет (будет создан).
    return exact

GLOBAL_LOG_FILE = resolve_global_log_file(BOOK_BASENAME)

# Файл-маркер для успешной заливки на B2
B2_MARKER_FILE = ".b2_upload_ok.json"

# Имя zip архива с результатами (временное имя, удаляется после upload)
ZIP_FILE_NAME = "mp3_results.zip"

# ================== ФУНКЦИИ ==================

def log_to_file(message):
    """
    Записывает message с меткой времени в:
     - персональный лог (LOG_FILE)
     - общий лог (GLOBAL_LOG_FILE) — теперь уникальный для книги
    """
    ts = f"{datetime.datetime.now()} {message}\n"
    try:
        with open(LOG_FILE, "a", encoding='utf-8') as f:
            f.write(ts)
    except Exception:
        pass
    try:
        with open(GLOBAL_LOG_FILE, "a", encoding='utf-8') as f:
            f.write(ts)
    except Exception:
        pass

def write_audio_url_log(part_name, voice_id, voice_name, lang_code, lang_name, url):
    entry = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "part": part_name,
        "voice_id": voice_id,
        "voice_name": voice_name,
        "lang_code": lang_code,
        "lang_name": lang_name,
        "url": url
    }
    try:
        with open(AUDIO_URLS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass

# ------------------- Текстовые утилиты -------------------
def clean_text_from_fb2(file_path):
    print(f"Очистка текста из файла FB2: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    soup = BeautifulSoup(content, 'xml')
    text = ' '.join([p.get_text() for p in soup.find_all('p')])
    unwanted_chars = set("{[*+=<>#@\\$&'\"~`/|\\()]}") 
    cleaned_text = ''.join(c for c in text if c not in unwanted_chars)
    print("Очистка текста из FB2 завершена.")
    return cleaned_text

def split_text_fragments(text, max_length=980):
    print("Разбивка текста на фрагменты...")
    delimiters = {'.', '!', '?', '...'}
    fragments, start = [], 0
    while start < len(text):
        end = start + max_length
        if end >= len(text):
            fragments.append(text[start:])
            break
        while end > start and text[end-1] not in delimiters:
            end -= 1
        if end == start:
            end = start + max_length
        fragments.append(text[start:end].strip())
        start = end
    print(f"Текст разбит на {len(fragments)} фрагментов.")
    return fragments

# ------------------- API TTS (низкоуровневый запрос) -------------------
def make_freetts_session():
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Origin": FREETTS_BASE_URL,
        "Referer": FREETTS_BASE_URL + "/",
    }
    if FREETTS_TOKEN:
        headers["token"] = FREETTS_TOKEN
    if FREETTS_COOKIE:
        headers["Cookie"] = FREETTS_COOKIE
    session.headers.update(headers)
    return session

def fetch_freetts_voices(session):
    resp = session.get(FREETTS_BASE_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    voices = []
    seen = set()
    for el in soup.select('[data-type="voice"][data-id][data-name]'):
        voice_id = el.get("data-id")
        voice_name = el.get("data-name")
        if not voice_id or not voice_name:
            continue
        key = (voice_id, voice_name)
        if key in seen:
            continue
        seen.add(key)
        voices.append({"id": voice_id, "name": voice_name})
    return voices

def fetch_freetts_langs(session):
    resp = session.get(FREETTS_BASE_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    langs = []
    seen = set()
    for el in soup.select('[data-type="lang"][data-code][data-name]'):
        lang_code = el.get("data-code")
        lang_name = el.get("data-name")
        if not lang_code or not lang_name:
            continue
        key = (lang_code, lang_name)
        if key in seen:
            continue
        seen.add(key)
        langs.append({"code": lang_code, "name": lang_name})
    return langs

def choose_voice_id(voices, preferred_name, preferred_id):
    if preferred_id:
        for v in voices:
            if v["id"] == preferred_id:
                return v["id"], v["name"]
        return preferred_id, preferred_name
    if preferred_name:
        for v in voices:
            if v["name"].lower() == preferred_name.lower():
                return v["id"], v["name"]
    if voices:
        return voices[0]["id"], voices[0]["name"]
    if FREETTS_FALLBACK_VOICE_ID:
        return FREETTS_FALLBACK_VOICE_ID, preferred_name
    return None, preferred_name

def choose_lang_code(langs, preferred_name, preferred_code):
    if preferred_code:
        for l in langs:
            if l["code"] == preferred_code:
                return l["code"], l["name"]
        return preferred_code, preferred_name
    if preferred_name:
        for l in langs:
            if l["name"].lower() == preferred_name.lower():
                return l["code"], l["name"]
    if langs:
        return langs[0]["code"], langs[0]["name"]
    if FREETTS_FALLBACK_LANG_CODE:
        return FREETTS_FALLBACK_LANG_CODE, preferred_name
    return None, preferred_name

def extract_audio_from_data(data):
    if isinstance(data, (bytes, bytearray)):
        return data, "audio/mpeg"
    if isinstance(data, str):
        if data.startswith("data:audio"):
            header, b64 = data.split(",", 1)
            content_type = header.split(";")[0].replace("data:", "")
            return base64.b64decode(b64), content_type
    return None, None

def extract_status_message(obj):
    if isinstance(obj, dict):
        status = obj.get("status")
        message = obj.get("message")
        if status or message:
            return status, message
    return None, None

def normalize_audio_url(url_value):
    if not isinstance(url_value, str):
        return None
    cleaned = url_value.strip().strip("`").strip().strip("^")
    if not cleaned:
        return None
    return cleaned

def find_audio_url_in_json(obj):
    if obj is None:
        return None
    if isinstance(obj, str):
        m = re.search(r'(https?://[^\s"\'<>]+?\.(mp3|wav))', obj, re.I)
        if m:
            return normalize_audio_url(m.group(1))
        m = re.search(r'(/[^"\s<>]+?\.(mp3|wav))', obj, re.I)
        if m:
            return normalize_audio_url(FREETTS_BASE_URL + m.group(1))
        return None
    if isinstance(obj, dict):
        for v in obj.values():
            found = find_audio_url_in_json(v)
            if found:
                return found
    if isinstance(obj, list):
        for v in obj:
            found = find_audio_url_in_json(v)
            if found:
                return found
    return None

def send_request(session, text, voice_id, voice_name, lang_code, lang_name, part_name, timeout=90):
    payload = {
        "ext": FREETTS_AUDIO_EXT,
        "text": text,
        "voiceid": voice_id,
        "lang": lang_code
    }
    start_json = None
    try:
        resp = session.post(FREETTS_SYNTHESIS_URL, json=payload, timeout=timeout)
        resp.raise_for_status()
        if "audio" in (resp.headers.get("Content-Type", "") or "").lower():
            return resp.content, resp.headers.get("Content-Type", "")
        try:
            start_json = resp.json()
        except Exception:
            start_json = None
    except Exception:
        start_json = None

    if start_json is None:
        try:
            resp = session.get(FREETTS_SYNTHESIS_URL, params=payload, timeout=timeout)
            resp.raise_for_status()
            if "audio" in (resp.headers.get("Content-Type", "") or "").lower():
                return resp.content, resp.headers.get("Content-Type", "")
            try:
                start_json = resp.json()
            except Exception:
                return None, None
        except Exception:
            return None, None

    status, message = extract_status_message(start_json)
    if status or message:
        log_to_file(f"[FREETTS] {part_name} status={status} message={message}")

    audio_bytes, content_type = extract_audio_from_data(start_json)
    if audio_bytes:
        return audio_bytes, content_type

    audio_url = find_audio_url_in_json(start_json)
    if audio_url:
        log_to_file(f"[FREETTS] {part_name} audio_url={audio_url}")
        write_audio_url_log(part_name, voice_id, voice_name, lang_code, lang_name, audio_url)
        audio_resp = session.get(audio_url, timeout=timeout)
        audio_resp.raise_for_status()
        return audio_resp.content, audio_resp.headers.get("Content-Type", "")

    for _ in range(FREETTS_POLL_ATTEMPTS):
        time.sleep(FREETTS_POLL_DELAY)
        poll_resp = session.get(FREETTS_SYNTHESIS_URL, timeout=timeout)
        poll_resp.raise_for_status()
        if "audio" in (poll_resp.headers.get("Content-Type", "") or "").lower():
            return poll_resp.content, poll_resp.headers.get("Content-Type", "")
        try:
            poll_json = poll_resp.json()
        except Exception:
            continue
        status, message = extract_status_message(poll_json)
        if status or message:
            log_to_file(f"[FREETTS] {part_name} status={status} message={message}")

        audio_bytes, content_type = extract_audio_from_data(poll_json)
        if audio_bytes:
            return audio_bytes, content_type
        audio_url = find_audio_url_in_json(poll_json)
        if audio_url:
            log_to_file(f"[FREETTS] {part_name} audio_url={audio_url}")
            write_audio_url_log(part_name, voice_id, voice_name, lang_code, lang_name, audio_url)
            audio_resp = session.get(audio_url, timeout=timeout)
            audio_resp.raise_for_status()
            return audio_resp.content, audio_resp.headers.get("Content-Type", "")
    return None, None

# ------------------- Обёртка с повторами -------------------
def generate_audio_with_retries(session, text, voice_id, voice_name, lang_code, lang_name, part_name, max_attempts=DEFAULT_RETRY_ATTEMPTS, delay=DEFAULT_RETRY_DELAY):
    """
    Попытки выполнить send_request до max_attempts c паузой delay (сек) между попытками.
    Если по завершении попыток не получилось — возвращает (None, None) и сохраняет текст фрагмента в OUTPUT_MP3_DIR как .txt.
    """
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            log_to_file(f"[RETRY] Попытка {attempt}/{max_attempts} генерации аудио...")
            audio_bytes, content_type = send_request(session, text, voice_id, voice_name, lang_code, lang_name, part_name)
            # Проверяем content_type — только аудио принимаем как успех
            if content_type and ("audio" in content_type.lower()):
                log_to_file(f"[RETRY] Успех на попытке {attempt} (content_type={content_type}).")
                return audio_bytes, content_type
            else:
                last_err = f"Неверный Content-Type: {content_type}"
                log_to_file(f"[RETRY] Попытка {attempt} вернула некорректный Content-Type: {content_type}")
        except Exception as e:
            last_err = str(e)
            log_to_file(f"[RETRY] Попытка {attempt} — ошибка: {e}")
        # если не последний — ждем и повторяем
        if attempt < max_attempts:
            log_to_file(f"[RETRY] Ждём {delay} секунд перед очередной попыткой...")
            time.sleep(delay)
    # если дошли сюда — всё не удалось
    log_to_file(f"[RETRY] Все {max_attempts} попыток завершились неудачей. Ошибка: {last_err}")
    return None, None

# ------------------- Размеры и индексы -------------------
def get_total_size_mb(directory):
    total = sum(os.path.getsize(f) for f in glob.glob(os.path.join(directory, "*.mp3")))
    return total / (1024 * 1024)

def get_last_processed_index_from_log(log_file_path):
    if not os.path.exists(log_file_path):
        return 0
    last_successful_index = 0
    target_string = "в пределах нормы"
    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            for line in f:
                if target_string in line:
                    match = re.search(r"part_(\d+)\.mp3", line)
                    if match:
                        idx = int(match.group(1))
                        if idx > last_successful_index:
                            last_successful_index = idx
        return last_successful_index
    except Exception:
        return 0

def get_highest_part_index_on_disk():
    parts = glob.glob(os.path.join(OUTPUT_MP3_DIR, "part_*.mp3"))
    max_idx = 0
    for p in parts:
        m = re.search(r"part_(\d+)\.mp3", os.path.basename(p))
        if m:
            max_idx = max(max_idx, int(m.group(1)))
    return max_idx

# ------------------- Хеш/zip для B2 -------------------
def compute_sha1_of_file(path):
    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            sha1.update(chunk)
    return sha1.hexdigest()

def zip_output_mp3(zip_name=ZIP_FILE_NAME):
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(OUTPUT_MP3_DIR):
            for f in files:
                zf.write(
                    os.path.join(root, f),
                    arcname=os.path.join(os.path.relpath(root, OUTPUT_MP3_DIR), f)
                )
    size = os.path.getsize(zip_name)
    return zip_name, size

# ------------------- B2 functions -------------------
def b2_authorize(key_id, app_key):
    resp = requests.get(
        "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
        auth=(key_id, app_key), timeout=30
    )
    resp.raise_for_status()
    return resp.json()

def b2_get_upload_url(api_url, auth_token, bucket_id):
    url = api_url.rstrip("/") + "/b2api/v2/b2_get_upload_url"
    headers = {"Authorization": auth_token}
    payload = {"bucketId": bucket_id}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

def b2_upload_file_to_bucket(upload_url, upload_auth_token, local_file_path, remote_file_name):
    size = os.path.getsize(local_file_path)
    sha1 = compute_sha1_of_file(local_file_path)
    headers = {
        "Authorization": upload_auth_token,
        "X-Bz-File-Name": remote_file_name,
        "Content-Type": "application/zip",
        "Content-Length": str(size),
        "X-Bz-Content-Sha1": sha1
    }
    with open(local_file_path, "rb") as f:
        resp = requests.post(upload_url, headers=headers, data=f, timeout=300)
    resp.raise_for_status()
    return resp.json()

def upload_zip_to_b2_and_verify(zip_path, bucket_id, bucket_name, key_id, app_key):
    auth = b2_authorize(key_id, app_key)
    upload_info = b2_get_upload_url(auth["apiUrl"], auth["authorizationToken"], bucket_id)
    remote_name = f"{BOOK_BASENAME}/{os.path.basename(zip_path)}"
    result = b2_upload_file_to_bucket(
        upload_info["uploadUrl"],
        upload_info["authorizationToken"],
        zip_path,
        remote_name
    )
    remote_size = int(result.get("contentLength", 0))
    local_size = os.path.getsize(zip_path)
    if local_size != remote_size:
        raise RuntimeError(f"B2 verification failed: local {local_size} != remote {remote_size}")
    return {
        "fileId": result.get("fileId"),
        "remote_name": remote_name,
        "local_size": local_size,
        "remote_size": remote_size
    }

# ================== ГЛАВНАЯ ФУНКЦИЯ ==================
def main():
    # Проверка наличия исходного файла
    if not os.path.isfile(TEXT_FILE_NAME):
        print(f"Файл {TEXT_FILE_NAME} не найден!")
        sys.exit(1)

    # Создаем необходимые папки
    os.makedirs(OUTPUT_MP3_DIR, exist_ok=True)
    os.makedirs(TMP_AUDIO_DIR, exist_ok=True)

    # Чтение текста
    if TEXT_FILE_NAME.lower().endswith(".fb2"):
        text = clean_text_from_fb2(TEXT_FILE_NAME)
    else:
        with open(TEXT_FILE_NAME, "r", encoding="utf-8") as f:
            text = f.read()

    session = make_freetts_session()
    try:
        voices = fetch_freetts_voices(session)
        VOICES_DATA["voices"] = [v["name"] for v in voices]
        if voices:
            voice_list_text = ", ".join([f"{v['name']}({v['id']})" for v in voices])
            print(f"Доступные голоса: {voice_list_text}")
            log_to_file(f"Доступные голоса: {voice_list_text}")
        else:
            print("Список голосов пуст.")
            log_to_file("Список голосов пуст.")
            log_to_file(f"[FREETTS] env voice_id={FREETTS_VOICE_ID} fallback_voice_id={FREETTS_FALLBACK_VOICE_ID} lang_code={FREETTS_LANG_CODE} fallback_lang_code={FREETTS_FALLBACK_LANG_CODE} cookie_set={bool(FREETTS_COOKIE)} token_set={bool(FREETTS_TOKEN)}")
    except Exception as e:
        voices = []
        print(f"Не удалось получить список голосов: {e}")
        log_to_file(f"Не удалось получить список голосов: {e}")
        log_to_file(f"[FREETTS] env voice_id={FREETTS_VOICE_ID} fallback_voice_id={FREETTS_FALLBACK_VOICE_ID} lang_code={FREETTS_LANG_CODE} fallback_lang_code={FREETTS_FALLBACK_LANG_CODE} cookie_set={bool(FREETTS_COOKIE)} token_set={bool(FREETTS_TOKEN)}")

    try:
        langs = fetch_freetts_langs(session)
        LANGS_DATA["langs"] = [l["name"] for l in langs]
        if langs:
            lang_list_text = ", ".join([f"{l['name']}({l['code']})" for l in langs])
            print(f"Доступные языки: {lang_list_text}")
            log_to_file(f"Доступные языки: {lang_list_text}")
        else:
            print("Список языков пуст.")
            log_to_file("Список языков пуст.")
            log_to_file(f"[FREETTS] env voice_id={FREETTS_VOICE_ID} fallback_voice_id={FREETTS_FALLBACK_VOICE_ID} lang_code={FREETTS_LANG_CODE} fallback_lang_code={FREETTS_FALLBACK_LANG_CODE} cookie_set={bool(FREETTS_COOKIE)} token_set={bool(FREETTS_TOKEN)}")
    except Exception as e:
        langs = []
        print(f"Не удалось получить список языков: {e}")
        log_to_file(f"Не удалось получить список языков: {e}")
        log_to_file(f"[FREETTS] env voice_id={FREETTS_VOICE_ID} fallback_voice_id={FREETTS_FALLBACK_VOICE_ID} lang_code={FREETTS_LANG_CODE} fallback_lang_code={FREETTS_FALLBACK_LANG_CODE} cookie_set={bool(FREETTS_COOKIE)} token_set={bool(FREETTS_TOKEN)}")

    voice_id, voice_name = choose_voice_id(voices, VOICE_NAME, FREETTS_VOICE_ID)
    if not voice_id:
        print("Не выбран voice_id для freetts.ru")
        log_to_file("Не выбран voice_id для freetts.ru")
        sys.exit(1)

    lang_code, lang_name = choose_lang_code(langs, LANG_NAME, FREETTS_LANG_CODE)
    if not lang_code:
        print("Не выбран язык для freetts.ru")
        log_to_file("Не выбран язык для freetts.ru")
        sys.exit(1)

    print(f"Выбран голос: {voice_name} ({voice_id})")
    log_to_file(f"Выбран голос: {voice_name} ({voice_id})")
    print(f"Выбран язык: {lang_name} ({lang_code})")
    log_to_file(f"Выбран язык: {lang_name} ({lang_code})")

    # Разбиваем текст на фрагменты
    all_chunks = split_text_fragments(text, max_length=980)

    # Определяем последний обработанный фрагмент:
    last_idx_book = get_last_processed_index_from_log(LOG_FILE)
    last_idx_global = get_last_processed_index_from_log(GLOBAL_LOG_FILE)
    last_idx = max(last_idx_book, last_idx_global)
    if last_idx > 0:
        print(f"Возобновляем с фрагмента: {last_idx+1} (найдено в логах)")
        log_to_file(f"Возобновление с фрагмента {last_idx+1} (book_log={last_idx_book}, global_log={last_idx_global})")
    else:
        print("Начинаем с самого начала (логов нет или нет записей).")
        log_to_file("Начало новой генерации (логов не найдено или нет успешных записей).")

    retry_attempts = int(os.environ.get("RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS))
    retry_delay = int(os.environ.get("RETRY_DELAY_SEC", DEFAULT_RETRY_DELAY))

    # Основной цикл генерации аудио
    for idx in range(last_idx, len(all_chunks)):
        chunk = all_chunks[idx]
        base_name = f"part_{idx+1:04}"
        tmp_wav = os.path.join(TMP_AUDIO_DIR, f"{base_name}.wav")
        out_mp3 = os.path.join(OUTPUT_MP3_DIR, f"{base_name}.mp3")
        out_txt = os.path.join(OUTPUT_MP3_DIR, f"{base_name}.txt")

        print(f"Генерация {base_name}: {len(chunk)} символов.")
        if idx > last_idx and FREETTS_REQUEST_DELAY > 0:
            log_to_file(f"[DELAY] {FREETTS_REQUEST_DELAY} секунд перед запросом")
            time.sleep(FREETTS_REQUEST_DELAY)
        audio_content, content_type = generate_audio_with_retries(session, chunk, voice_id, voice_name, lang_code, lang_name, base_name, max_attempts=retry_attempts, delay=retry_delay)

        if audio_content is None:
            # ничего не получилось — сохраняем текст фрагмента в OUTPUT_MP3_DIR с именем part_XXXX.txt
            try:
                with open(out_txt, "w", encoding="utf-8") as tf:
                    tf.write(chunk)
                log_to_file(f"Фрагмент {idx+1} не озвучен — сохранён как текст {out_txt}. Продолжаем.")
            except Exception as e:
                log_to_file(f"Не удалось сохранить текстовый файл для фрагмента {idx+1}: {e}")
            continue

        # Сохранение и конвертация в зависимости от Content-Type
        try:
            ctype = content_type.lower() if content_type else ""
            if "wav" in ctype:
                with open(tmp_wav, "wb") as f:
                    f.write(audio_content)
                # Конвертация WAV -> MP3
                try:
                    from pydub import AudioSegment
                    audio = AudioSegment.from_wav(tmp_wav)
                    if SAMPLE_RATE_HZ:
                        audio = audio.set_frame_rate(SAMPLE_RATE_HZ)
                    audio.export(out_mp3, format="mp3", bitrate=MP3_BITRATE)
                except Exception as e:
                    log_to_file(f"Ошибка конвертации wav->mp3 для {base_name}: {e}")
                    if os.path.exists(tmp_wav):
                        os.remove(tmp_wav)
                    continue
            elif "mpeg" in ctype or "mp3" in ctype or "audio/mpeg" in ctype:
                # API вернул mp3 — сохраняем сразу
                with open(out_mp3, "wb") as f:
                    f.write(audio_content)
            else:
                # Неподдерживаемый тип — лог и пропуск (хотя generate_audio_with_retries должен был это отфильтровать)
                log_to_file(f"[LOG] Неподдерживаемый Content-Type для {base_name}: {content_type}. Пропуск.")
                continue
        except Exception as e:
            log_to_file(f"Ошибка сохранения/конвертации для {base_name}: {e}")
            continue

        # Проверка размера mp3 файла
        try:
            size_kb = os.path.getsize(out_mp3) // 1024
        except Exception as e:
            log_to_file(f"Не могу получить размер {out_mp3}: {e}")
            continue

        if not (MIN_SIZE_KB < size_kb < MAX_SIZE_KB):
            log_to_file(f"Файл {out_mp3} не прошёл по размеру: {size_kb} КБ. Удалён.")
            try:
                os.remove(out_mp3)
            except Exception:
                pass
            continue

        # Успешная генерация фрагмента
        log_to_file(f"Размер файла {out_mp3} {size_kb} КБ в пределах нормы.")

        # ===== ПРОВЕРКА ОБЩЕГО ЛИМИТА =====
        total_mb = get_total_size_mb(OUTPUT_MP3_DIR)
        print(f"Текущий суммарный размер папки {OUTPUT_MP3_DIR}: {total_mb:.2f} МБ (лимит {AUDIO_SIZE_LIMIT_MB} МБ).")
        if total_mb >= AUDIO_SIZE_LIMIT_MB:
            # --- 1) создаём zip ---
            zip_path, zip_size = zip_output_mp3()
            log_to_file(f"Создан архив {zip_path}, размер {zip_size} байт (сумма mp3: {total_mb:.2f} МБ).")
            # --- вычисляем highest part, чтобы записать в маркер ---
            highest_part = get_highest_part_index_on_disk()

            # --- 2) пытаемся залить на Backblaze B2 ---
            key_id = os.environ.get("B2_KEY_ID")
            app_key = os.environ.get("B2_APP_KEY")
            bucket_id = os.environ.get("B2_BUCKET_ID")
            bucket_name = os.environ.get("B2_BUCKET_NAME", "tts-archive")

            try:
                if not all([key_id, app_key, bucket_id]):
                    raise RuntimeError("B2 credentials or bucket id not set in environment variables.")
                upload_result = upload_zip_to_b2_and_verify(zip_path, bucket_id, bucket_name, key_id, app_key)

                # --- 3) если успешно — создаём маркер для workflow с детальной информацией ---
                marker = {
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                    "zip": os.path.basename(zip_path),
                    "local_size": upload_result["local_size"],
                    "remote_size": upload_result["remote_size"],
                    "remote_name": upload_result["remote_name"],
                    "fileId": upload_result["fileId"],
                    "last_part": highest_part
                }
                with open(B2_MARKER_FILE, "w", encoding="utf-8") as mf:
                    json.dump(marker, mf)
                log_to_file(f"B2: Успешно загружено {marker['zip']} (last_part={highest_part}). Маркер {B2_MARKER_FILE} создан.")

                # --- 4) Удаляем локальный zip чтобы runner не отправил его в артефакт по ошибке ---
                try:
                    os.remove(zip_path)
                    log_to_file("Локальный zip удалён после успешной загрузки на B2.")
                except Exception:
                    pass

                # --- 5) Удаляем все mp3-файлы из OUTPUT_MP3_DIR, т.к. они уже в B2 ---
                try:
                    deleted_count = 0
                    for fpath in glob.glob(os.path.join(OUTPUT_MP3_DIR, "*.mp3")):
                        try:
                            os.remove(fpath)
                            deleted_count += 1
                        except Exception:
                            pass
                    log_to_file(f"Удалено {deleted_count} mp3-файлов из {OUTPUT_MP3_DIR} после успешной загрузки.")
                except Exception as e:
                    log_to_file(f"Ошибка при удалении локальных mp3 после загрузки: {e}")

            except Exception as e:
                log_to_file(f"Ошибка при заливке на B2: {e}")
                # в случае ошибки — оставляем mp3 и zip (zip если остался) чтобы workflow мог отправить их в артефакт
                # (не удаляем local mp3)
            # конец блока обработки загрузки

        # Удаляем временный WAV файл
        if os.path.exists(tmp_wav):
            try:
                os.remove(tmp_wav)
            except Exception:
                pass

    # ---------- ФИНАЛ: залить остаток (если остался) ----------
    remaining = glob.glob(os.path.join(OUTPUT_MP3_DIR, "*.mp3"))
    if remaining:
        log_to_file(f"По завершении цикла обнаружено {len(remaining)} mp3-файлов. Попытка финальной упаковки и загрузки в B2.")
        zip_path, zip_size = zip_output_mp3()
        log_to_file(f"Создан финальный архив {zip_path}, размер {zip_size} байт.")
        highest_part = get_highest_part_index_on_disk()

        key_id = os.environ.get("B2_KEY_ID")
        app_key = os.environ.get("B2_APP_KEY")
        bucket_id = os.environ.get("B2_BUCKET_ID")
        bucket_name = os.environ.get("B2_BUCKET_NAME", "tts-archive")

        try:
            if not all([key_id, app_key, bucket_id]):
                raise RuntimeError("B2 credentials or bucket id not set in environment variables.")
            upload_result = upload_zip_to_b2_and_verify(zip_path, bucket_id, bucket_name, key_id, app_key)

            marker = {
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "zip": os.path.basename(zip_path),
                "local_size": upload_result["local_size"],
                "remote_size": upload_result["remote_size"],
                "remote_name": upload_result["remote_name"],
                "fileId": upload_result["fileId"],
                "last_part": highest_part
            }
            with open(B2_MARKER_FILE, "w", encoding="utf-8") as mf:
                json.dump(marker, mf)
            log_to_file(f"B2: Финальная загрузка успешна {marker['zip']} (last_part={highest_part}). Маркер {B2_MARKER_FILE} создан.")

            # удаляем локальный zip и mp3
            try:
                os.remove(zip_path)
            except Exception:
                pass
            deleted_count = 0
            for fpath in glob.glob(os.path.join(OUTPUT_MP3_DIR, "*.mp3")):
                try:
                    os.remove(fpath)
                    deleted_count += 1
                except Exception:
                    pass
            log_to_file(f"Удалено {deleted_count} mp3-файлов из {OUTPUT_MP3_DIR} после финальной загрузки.")
        except Exception as e:
            log_to_file(f"Ошибка при финальной заливке на B2: {e}")
            log_to_file("Оставляю финальный zip/mp3 в каталоге, чтобы workflow мог экспортировать их в артефакт.")

    print("Все фрагменты обработаны.")
    log_to_file("Все фрагменты обработаны.")

# ================== ЗАПУСК СКРИПТА ==================
if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {exc}")
        log_to_file(f"КРИТИЧЕСКАЯ ОШИБКА: {exc}")
        sys.exit(1)
