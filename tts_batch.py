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
from tqdm import tqdm
from bs4 import BeautifulSoup

# ================== НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ ==================

# Название исходного текстового файла (может быть .txt или .fb2)
TEXT_FILE_NAME = "111Royzman_Delo-Volk-Vor-nevidimka.txt"

# Папка для готовых mp3 файлов
OUTPUT_MP3_DIR = "output_mp3"

# Временная папка для промежуточных wav файлов
TMP_AUDIO_DIR = "tmp_audio"

# Минимальный и максимальный размер mp3 файла в КБ
MIN_SIZE_KB = 150
MAX_SIZE_KB = 5000

# Лимит общей папки mp3 перед сборкой zip (в МБ)
AUDIO_SIZE_LIMIT_MB = 5

# Частота дискретизации аудио при конвертации (Гц)
SAMPLE_RATE_HZ = 22000

# Битрейт MP3 при конвертации из WAV (например: "64k", "96k", "128k")
MP3_BITRATE = "128k"

# Голоса и характер озвучки (можно расширять)
VOICES_DATA = { "voices": [ "Alloy", "Ash", "Ballad", "Coral", "Echo", "Fable", "Onyx", "Nova", "Sage", "Shimmer", "Verse" ] }
VIBES_DATA = {
    "Calm (Спокойный)": ["Emotion: Искреннее сочувствие, уверенность.", "Emphasis: Выделите ключевые мысли."],
    "Energetic (Энергичный)": ["Emotion: Яркий, энергичный тон.", "Emphasis: Выделите эмоциональные слова."],
}

# Выбранный голос и характер (можно менять на свой)
VOICE_NAME = "Sage"
VIBE_NAME = "Energetic (Энергичный)"

# ----------------- ЛОГ-ФАЙЛЫ -----------------
# Персональный лог по имени книги (например Royzman_Delo-Volk-Vor-nevidimka.log)
# (оставляем как в исходном коде)
BOOK_BASENAME = os.path.splitext(os.path.basename(TEXT_FILE_NAME))[0]
LOG_FILE = BOOK_BASENAME + ".log"

# Глобальный лог теперь формируется с именем, указывающим книгу:
# tts_batch(<bookname>).log — это убирает пересечения между разными книгами.
# Кроме того, чтобы НЕ подхватить старый лог по ошибке, реализуем проверку:
def resolve_global_log_file(book_basename):
    """
    Возвращает наиболее подходящий глобальный лог для текущей книги.
    1) Если существует точный файл "tts_batch(book).log" — возвращаем его.
    2) Иначе ищем все файлы, начинающиеся с "tts_batch(book" и выбираем самый новый по времени изменения.
    3) Если ничего не найдено — возвращаем стандартное имя (оно будет создано при записи).
    Это обеспечивает, что при обращении к "глобальному" логу мы возьмём файл, относящийся к текущей книге
    и предпочтём самый свежий вариант.
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

# Получаем глобальный лог, подходящий для текущей книги
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
        # При каждой записи используем текущий resolved GLOBAL_LOG_FILE
        with open(GLOBAL_LOG_FILE, "a", encoding='utf-8') as f:
            f.write(ts)
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

def format_vibe_prompt(vibe_name, vibes_data):
    print(f"Форматирование промпта для характера: {vibe_name}")
    vibe_content = vibes_data.get(vibe_name)
    if vibe_content:
        return "\n\n".join(vibe_content)
    print("Характер не найден, используется стандартный промпт.")
    return "Voice Affect: Calm, composed, and reassuring."

# ------------------- API TTS -------------------
def send_request(text, voice, vibe_prompt):
    print(f"Отправка запроса к API для генерации аудио (голос: {voice}).")
    url = "https://www.openai.fm/api/generate"
    boundary = "----WebKitFormBoundarya027BOtfh6crFn7A"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }
    data = [
        f"--{boundary}", f'Content-Disposition: form-data; name="input"\r\n\r\n{text}',
        f"--{boundary}", f'Content-Disposition: form-data; name="prompt"\r\n\r\n{vibe_prompt}',
        f"--{boundary}", f'Content-Disposition: form-data; name="voice"\r\n\r\n{voice.lower()}',
        f"--{boundary}", f'Content-Disposition: form-data; name="vibe"\r\n\r\nnull',
        f"--{boundary}--"
    ]
    body = "\r\n".join(data).encode('utf-8')
    try:
        response = requests.post(url, headers=headers, data=body, timeout=90)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "audio/wav" in content_type or "audio/mpeg" in content_type:
            return response.content, content_type
        log_to_file(f"[LOG] API вернул неверный тип контента: {content_type}")
        return None, None
    except Exception as e:
        log_to_file(f"[LOG] Сетевая ошибка при запросе к API: {e}")
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
    remote_name = f"{bucket_name}-{os.path.basename(zip_path)}"
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

    # Разбиваем текст на фрагменты
    all_chunks = split_text_fragments(text, max_length=980)

    # Определяем последний обработанный фрагмент:
    # Обязательно проверяем персональный лог по книге и только глобальный лог,
    # относящийся к текущей книге (GLOBAL_LOG_FILE), чтобы не подхватить чужой/старый лог.
    last_idx_book = get_last_processed_index_from_log(LOG_FILE)
    last_idx_global = get_last_processed_index_from_log(GLOBAL_LOG_FILE)
    last_idx = max(last_idx_book, last_idx_global)
    if last_idx > 0:
        print(f"Возобновляем с фрагмента: {last_idx+1} (найдено в логах)")
        log_to_file(f"Возобновление с фрагмента {last_idx+1} (book_log={last_idx_book}, global_log={last_idx_global})")
    else:
        print("Начинаем с самого начала (логов нет или нет записей).")
        log_to_file("Начало новой генерации (логов не найдено или нет успешных записей).")

    vibe_prompt = format_vibe_prompt(VIBE_NAME, VIBES_DATA)

    # Основной цикл генерации аудио
    for idx in range(last_idx, len(all_chunks)):
        chunk = all_chunks[idx]
        base_name = f"part_{idx+1:04}"
        tmp_wav = os.path.join(TMP_AUDIO_DIR, f"{base_name}.wav")
        out_mp3 = os.path.join(OUTPUT_MP3_DIR, f"{base_name}.mp3")

        print(f"Генерация {base_name}: {len(chunk)} символов.")
        audio_content, content_type = send_request(chunk, VOICE_NAME, vibe_prompt)
        if audio_content is None:
            log_to_file(f"Ошибка генерации аудио для фрагмента {idx+1}. Пропуск.")
            continue

        # Сохраняем WAV
        with open(tmp_wav, "wb") as f:
            f.write(audio_content)

        # Конвертация WAV в MP3
        if "wav" in content_type:
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
        else:
            # если API сразу вернул mp3
            os.rename(tmp_wav, out_mp3)

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

                # --- 4) удаляем локальный zip чтобы runner не отправил его в артефакт по ошибке ---
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
