# tts_batch.py
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

# ================== НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ (как у вас) ==================
TEXT_FILE_NAME = "Royzman_Delo-306-Volk-Vor-nevidimka.txt"
OUTPUT_MP3_DIR = "output_mp3"
TMP_AUDIO_DIR = "tmp_audio"
MIN_SIZE_KB = 150
MAX_SIZE_KB = 5000
AUDIO_SIZE_LIMIT_MB = 5  # <-- порог в МБ (как у вас)
SAMPLE_RATE_HZ = 22000
VOICES_DATA = { "voices": [ "Alloy", "Ash", "Ballad", "Coral", "Echo", "Fable", "Onyx", "Nova", "Sage", "Shimmer", "Verse" ] }
VIBES_DATA = { "Calm (Спокойный)": ["Emotion: Искреннее сочувствие, уверенность.", "Emphasis: Выделите ключевые мысли."], "Energetic (Энергичный)": ["Emotion: Яркий, энергичный тон.", "Emphasis: Выделите эмоциональные слова."], }
VOICE_NAME = "Ballad"
VIBE_NAME = "Energetic (Энергичный)"
LOG_FILE = "tts_batch.log"
# ======================================================================

# --- ФАЙЛ-МАРКЕР ДЛЯ WORKFLOW, ЧТО B2 ЗАЛИТИЕ УСПЕШНО ---
B2_MARKER_FILE = ".b2_upload_ok.json"
ZIP_FILE_NAME = "mp3_results.zip"

def log_to_file(message):
    try:
        with open(LOG_FILE, "a", encoding='utf-8') as log_file:
            log_file.write(f"{datetime.datetime.now()} {message}\n")
    except Exception:
        pass

# ---------- ВАШИ СТАРЫЕ ФУНКЦИИ (clean_text_from_fb2, split_text_fragments, ...) ----------
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
            fragments.append(text[start:]); break
        while end > start and text[end-1] not in delimiters: end -= 1
        if end == start: end = start + max_length
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

def send_request(text, voice, vibe_prompt):
    print(f"Отправка запроса к API для генерации аудио (голос: {voice}).")
    url = "https://www.openai.fm/api/generate"
    boundary = "----WebKitFormBoundarya027BOtfh6crFn7A"
    headers = { "User-Agent": "Mozilla/5.0", "Content-Type": f"multipart/form-data; boundary={boundary}", }
    data = [ f"--{boundary}", f'Content-Disposition: form-data; name="input"\r\n\r\n{text}', f"--{boundary}", f'Content-Disposition: form-data; name="prompt"\r\n\r\n{vibe_prompt}', f"--{boundary}", f'Content-Disposition: form-data; name="voice"\r\n\r\n{voice.lower()}', f"--{boundary}", f'Content-Disposition: form-data; name="vibe"\r\n\r\nnull', f"--{boundary}--" ]
    body = "\r\n".join(data).encode('utf-8')
    try:
        response = requests.post(url, headers=headers, data=body, timeout=90)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "audio/wav" in content_type or "audio/mpeg" in content_type:
            print("Аудио контент успешно получен.")
            return response.content, content_type
        print(f"[LOG] API вернул неверный тип контента: {content_type}")
        return None, None
    except Exception as e:
        print(f"[LOG] Сетевая ошибка при запросе к API: {e}")
        return None, None

def get_total_size_mb(directory):
    total = sum(os.path.getsize(f) for f in glob.glob(os.path.join(directory, "*.mp3")))
    total_mb = total / (1024 * 1024)
    print(f"Общий размер: {total_mb:.2f} МБ.")
    return total_mb

def get_last_processed_index_from_log(log_file_path):
    print(f"Поиск точки возобновления в лог-файле: {log_file_path}")
    if not os.path.exists(log_file_path):
        print("Лог-файл не найден. Работа начнется с самого начала.")
        return 0
    last_successful_index = 0
    target_string = "в пределах нормы"
    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for line in lines:
            if target_string in line:
                match = re.search(r"part_(\d+)\.mp3", line)
                if match:
                    last_successful_index = int(match.group(1))
        if last_successful_index > 0:
            print(f"Последний успешный фрагмент: {last_successful_index}. Возобновление со следующего.")
            return last_successful_index
        else:
            print("Успешных записей не найдено. Работа начнется с самого начала.")
            return 0
    except Exception as e:
        print(f"Критическая ошибка при чтении лог-файла: {e}. Работа начнется с начала.")
        return 0

# ---------- НОВЫЕ УТИЛИТЫ ДЛЯ B2 ----------
def compute_sha1_of_file(path):
    """Возвращает hex SHA1 файла (строку) — требуется Backblaze."""
    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            sha1.update(chunk)
    return sha1.hexdigest()

def zip_output_mp3(zip_name=ZIP_FILE_NAME):
    """Упаковать output_mp3 в zip_name. Возвращает путь и размер файла в байтах."""
    print(f"Упаковываем каталог {OUTPUT_MP3_DIR} → {zip_name}")
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(OUTPUT_MP3_DIR):
            for f in files:
                zf.write(os.path.join(root, f), arcname=os.path.join(os.path.relpath(root, OUTPUT_MP3_DIR), f))
    size = os.path.getsize(zip_name)
    print(f"Создан {zip_name}, размер {size} байт.")
    return zip_name, size

def b2_authorize(key_id, app_key):
    """
    Авторизация в Backblaze B2.
    Возвращает dict с apiUrl, authorizationToken, accountId.
    """
    print("B2: Авторизация...")
    resp = requests.get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account", auth=(key_id, app_key), timeout=30)
    resp.raise_for_status()
    return resp.json()

def b2_get_upload_url(api_url, auth_token, bucket_id):
    """
    Получить uploadUrl и uploadAuthToken для указанного bucketId.
    """
    url = api_url.rstrip("/") + "/b2api/v2/b2_get_upload_url"
    headers = {"Authorization": auth_token}
    payload = {"bucketId": bucket_id}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

def b2_upload_file_to_bucket(upload_url, upload_auth_token, local_file_path, remote_file_name):
    """
    Загружает файл на предоставленный upload_url.
    Возвращает JSON ответа b2_upload_file (включая fileId, contentLength и т.д.)
    """
    print(f"B2: Upload {local_file_path} -> {remote_file_name}")
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
    """
    Полный flow: authorize -> get_upload_url -> upload -> verify по contentLength -> return dict or raise.
    """
    auth = b2_authorize(key_id, app_key)
    api_url = auth["apiUrl"]
    auth_token = auth["authorizationToken"]
    upload_info = b2_get_upload_url(api_url, auth_token, bucket_id)
    upload_url = upload_info["uploadUrl"]
    upload_auth_token = upload_info["authorizationToken"]

    remote_name = f"{bucket_name}-{os.path.basename(zip_path)}"
    result = b2_upload_file_to_bucket(upload_url, upload_auth_token, zip_path, remote_name)

    # contentLength в ответе — хранит размер загруженного объекта
    remote_size = int(result.get("contentLength", 0))
    local_size = os.path.getsize(zip_path)
    fileId = result.get("fileId")
    if local_size != remote_size:
        raise RuntimeError(f"B2 verification failed: local {local_size} != remote {remote_size}")
    print("B2: verification OK (size matches).")
    return {"fileId": fileId, "remote_name": remote_name, "local_size": local_size, "remote_size": remote_size}

# ---------- ПРИВЯЗКА К ОСНОВНОМУ ЦИКЛУ ----------
def main():
    print("Начало работы скрипта.")
    log_to_file("Начало работы скрипта.")

    if not os.path.isfile(TEXT_FILE_NAME):
        print(f"Файл {TEXT_FILE_NAME} не найден!")
        log_to_file(f"Файл {TEXT_FILE_NAME} не найден!")
        sys.exit(1)

    os.makedirs(OUTPUT_MP3_DIR, exist_ok=True)
    os.makedirs(TMP_AUDIO_DIR, exist_ok=True)

    print(f"Чтение текста из файла: {TEXT_FILE_NAME}")
    if TEXT_FILE_NAME.lower().endswith(".fb2"):
        text = clean_text_from_fb2(TEXT_FILE_NAME)
    else:
        with open(TEXT_FILE_NAME, "r", encoding="utf-8") as f:
            text = f.read()
    print("Чтение и очистка текста завершены.")

    all_chunks = split_text_fragments(text, max_length=980)

    last_idx = get_last_processed_index_from_log(LOG_FILE)
    vibe_prompt = format_vibe_prompt(VIBE_NAME, VIBES_DATA)

    print("Начало основного цикла обработки фрагментов.")
    for idx in range(last_idx, len(all_chunks)):
        chunk = all_chunks[idx]
        base_name = f"part_{idx+1:04}"
        tmp_wav = os.path.join(TMP_AUDIO_DIR, f"{base_name}.wav")
        out_mp3 = os.path.join(OUTPUT_MP3_DIR, f"{base_name}.mp3")

        print(f"Генерация {base_name}: {len(chunk)} символов.")
        audio_content, content_type = send_request(chunk, VOICE_NAME, vibe_prompt)
        if audio_content is None:
            print(f"Ошибка генерации аудио для фрагмента {idx+1}. Пропуск.")
            continue

        with open(tmp_wav, "wb") as f:
            f.write(audio_content)

        if "wav" in content_type:
            print(f"Конвертация {tmp_wav} в {out_mp3}...")
            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_wav(tmp_wav)
                if SAMPLE_RATE_HZ:
                    audio = audio.set_frame_rate(SAMPLE_RATE_HZ)
                audio.export(out_mp3, format="mp3", bitrate="128k")
            except Exception as e:
                print(f"Ошибка конвертации wav->mp3: {e}")
                continue
        else:
            os.rename(tmp_wav, out_mp3)

        size_kb = os.path.getsize(out_mp3) // 1024
        if not (MIN_SIZE_KB < size_kb < MAX_SIZE_KB):
            print(f"Файл {out_mp3} не прошёл по размеру: {size_kb} КБ. Удалён.")
            os.remove(out_mp3)
            continue

        log_to_file(f"Размер файла {out_mp3} {size_kb} КБ в пределах нормы.")

        # ===== ПРОВЕРКА ОБЩЕГО ЛИМИТА =====
        total_mb = get_total_size_mb(OUTPUT_MP3_DIR)
        if total_mb >= AUDIO_SIZE_LIMIT_MB:
            print(f"Достигнут лимит размера {AUDIO_SIZE_LIMIT_MB} МБ. Работа будет остановлена и произведена выгрузка архива на B2.")
            # --- 1) создаём zip ---
            zip_path, zip_size = zip_output_mp3()

            # --- 2) пытаемся залить на Backblaze B2 ---
            # читаем ключи из переменных среды (они должны приходить в runner из Secrets)
            key_id = os.environ.get("B2_KEY_ID")
            app_key = os.environ.get("B2_APP_KEY")
            bucket_id = os.environ.get("B2_BUCKET_ID")
            bucket_name = os.environ.get("B2_BUCKET_NAME", "tts-archive")

            try:
                if not all([key_id, app_key, bucket_id]):
                    raise RuntimeError("B2 credentials or bucket id not set in environment variables.")

                upload_result = upload_zip_to_b2_and_verify(zip_path, bucket_id, bucket_name, key_id, app_key)

                # --- 3) если успешно — создаём маркер для workflow ---
                marker = {
                    "timestamp": datetime.datetime.utcnow().isoformat()+"Z",
                    "zip": os.path.basename(zip_path),
                    "local_size": upload_result["local_size"],
                    "remote_size": upload_result["remote_size"],
                    "remote_name": upload_result["remote_name"],
                    "fileId": upload_result["fileId"]
                }
                with open(B2_MARKER_FILE, "w", encoding="utf-8") as mf:
                    json.dump(marker, mf)
                print(f"B2: Успешно загружено и проверено. Создан маркер {B2_MARKER_FILE}")

                # --- 4) (опционально) удаляем локальный zip чтобы runner не отправил его в артефакт по ошибке ---
                try:
                    os.remove(zip_path)
                    print("Локальный zip удалён после успешной загрузки на B2.")
                except Exception:
                    pass

            except Exception as e:
                print(f"Ошибка при заливке на B2: {e}")
                log_to_file(f"Ошибка при заливке на B2: {e}")
                # В случае провала — оставляем zip, чтобы workflow мог отправить его в артефакты
            break  # останавливаем основной цикл, как и раньше

        if os.path.exists(tmp_wav):
            os.remove(tmp_wav)

    print("Работа завершена!")
    log_to_file("Работа завершена!")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {exc}")
        log_to_file(f"КРИТИЧЕСКАЯ ОШИБКА: {exc}")
        sys.exit(1)
