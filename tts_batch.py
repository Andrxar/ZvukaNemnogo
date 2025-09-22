# tts_batch.py
# Скрипт для пакетной генерации TTS аудио из текста
# Поддерживает разбиение текста на фрагменты, генерацию аудио, конвертацию в mp3,
# сборку zip архива и загрузку на Backblaze B2.

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
TEXT_FILE_NAME = "Royzman_Delo-306-Volk-Vor-nevidimka.txt"

# Папка для готовых mp3 файлов
OUTPUT_MP3_DIR = "output_mp3"

# Временная папка для промежуточных wav файлов
TMP_AUDIO_DIR = "tmp_audio"

# Минимальный и максимальный размер mp3 файла в КБ
MIN_SIZE_KB = 150
MAX_SIZE_KB = 5000

# Лимит общей папки mp3 перед сборкой zip (в МБ)
AUDIO_SIZE_LIMIT_MB = 5

# Частота дискретизации аудио при конвертации
SAMPLE_RATE_HZ = 22000

# Голоса и характер озвучки (можно расширять)
VOICES_DATA = { "voices": [ "Alloy", "Ash", "Ballad", "Coral", "Echo", "Fable", "Onyx", "Nova", "Sage", "Shimmer", "Verse" ] }
VIBES_DATA = {
    "Calm (Спокойный)": ["Emotion: Искреннее сочувствие, уверенность.", "Emphasis: Выделите ключевые мысли."],
    "Energetic (Энергичный)": ["Emotion: Яркий, энергичный тон.", "Emphasis: Выделите эмоциональные слова."],
}

# Выбранный голос и характер (можно менять на свой)
VOICE_NAME = "Ballad"
VIBE_NAME = "Energetic (Энергичный)"

# Файл логов, куда пишутся успешные операции и ошибки
LOG_FILE = "tts_batch.log"

# Файл-маркер для успешной заливки на B2
B2_MARKER_FILE = ".b2_upload_ok.json"

# Имя zip архива с результатами
ZIP_FILE_NAME = "mp3_results.zip"
# ======================================================================

# ================== ФУНКЦИИ ==================

# Функция для записи сообщений в лог-файл
def log_to_file(message):
    try:
        with open(LOG_FILE, "a", encoding='utf-8') as log_file:
            log_file.write(f"{datetime.datetime.now()} {message}\n")
    except Exception:
        pass

# Очистка текста из FB2 файла, удаление спецсимволов
def clean_text_from_fb2(file_path):
    print(f"Очистка текста из файла FB2: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    soup = BeautifulSoup(content, 'xml')
    text = ' '.join([p.get_text() for p in soup.find_all('p')])
    # Символы, которые мы хотим убрать
    unwanted_chars = set("{[*+=<>#@\\$&'\"~`/|\\()]}")
    cleaned_text = ''.join(c for c in text if c not in unwanted_chars)
    print("Очистка текста из FB2 завершена.")
    return cleaned_text

# Разбивка текста на фрагменты по 980 символов максимум (можно менять max_length)
def split_text_fragments(text, max_length=980):
    print("Разбивка текста на фрагменты...")
    delimiters = {'.', '!', '?', '...'}  # Где можно разрезать текст
    fragments, start = [], 0
    while start < len(text):
        end = start + max_length
        if end >= len(text):
            fragments.append(text[start:]); break
        # ищем ближайший конец предложения
        while end > start and text[end-1] not in delimiters: end -= 1
        if end == start: end = start + max_length
        fragments.append(text[start:end].strip())
        start = end
    print(f"Текст разбит на {len(fragments)} фрагментов.")
    return fragments

# Формирование "характера" озвучки
def format_vibe_prompt(vibe_name, vibes_data):
    print(f"Форматирование промпта для характера: {vibe_name}")
    vibe_content = vibes_data.get(vibe_name)
    if vibe_content:
        return "\n\n".join(vibe_content)
    print("Характер не найден, используется стандартный промпт.")
    return "Voice Affect: Calm, composed, and reassuring."

# Отправка запроса на генерацию аудио в API
def send_request(text, voice, vibe_prompt):
    print(f"Отправка запроса к API для генерации аудио (голос: {voice}).")
    url = "https://www.openai.fm/api/generate"
    boundary = "----WebKitFormBoundarya027BOtfh6crFn7A"
    headers = { "User-Agent": "Mozilla/5.0", "Content-Type": f"multipart/form-data; boundary={boundary}", }
    data = [ f"--{boundary}", f'Content-Disposition: form-data; name="input"\r\n\r\n{text}',
             f"--{boundary}", f'Content-Disposition: form-data; name="prompt"\r\n\r\n{vibe_prompt}',
             f"--{boundary}", f'Content-Disposition: form-data; name="voice"\r\n\r\n{voice.lower()}',
             f"--{boundary}", f'Content-Disposition: form-data; name="vibe"\r\n\r\nnull',
             f"--{boundary}--" ]
    body = "\r\n".join(data).encode('utf-8')
    try:
        response = requests.post(url, headers=headers, data=body, timeout=90)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "audio/wav" in content_type or "audio/mpeg" in content_type:
            return response.content, content_type
        print(f"[LOG] API вернул неверный тип контента: {content_type}")
        return None, None
    except Exception as e:
        print(f"[LOG] Сетевая ошибка при запросе к API: {e}")
        return None, None

# Получение общего размера папки mp3 в МБ
def get_total_size_mb(directory):
    total = sum(os.path.getsize(f) for f in glob.glob(os.path.join(directory, "*.mp3")))
    return total / (1024 * 1024)

# Определение последнего обработанного фрагмента по логам
def get_last_processed_index_from_log(log_file_path):
    if not os.path.exists(log_file_path):
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
        return last_successful_index
    except Exception:
        return 0

# Вычисление SHA1 для проверки целостности файла
def compute_sha1_of_file(path):
    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            sha1.update(chunk)
    return sha1.hexdigest()

# Создание zip архива из всех mp3
def zip_output_mp3(zip_name=ZIP_FILE_NAME):
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(OUTPUT_MP3_DIR):
            for f in files:
                zf.write(os.path.join(root, f), arcname=os.path.join(os.path.relpath(root, OUTPUT_MP3_DIR), f))
    size = os.path.getsize(zip_name)
    return zip_name, size

# Авторизация в B2
def b2_authorize(key_id, app_key):
    resp = requests.get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account", auth=(key_id, app_key), timeout=30)
    resp.raise_for_status()
    return resp.json()

# Получение URL для загрузки файла на B2
def b2_get_upload_url(api_url, auth_token, bucket_id):
    url = api_url.rstrip("/") + "/b2api/v2/b2_get_upload_url"
    headers = {"Authorization": auth_token}
    payload = {"bucketId": bucket_id}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

# Загрузка файла на B2
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

# Полная загрузка zip на B2 и проверка
def upload_zip_to_b2_and_verify(zip_path, bucket_id, bucket_name, key_id, app_key):
    auth = b2_authorize(key_id, app_key)
    upload_info = b2_get_upload_url(auth["apiUrl"], auth["authorizationToken"], bucket_id)
    remote_name = f"{bucket_name}-{os.path.basename(zip_path)}"
    result = b2_upload_file_to_bucket(upload_info["uploadUrl"], upload_info["authorizationToken"], zip_path, remote_name)
    remote_size = int(result.get("contentLength", 0))
    local_size = os.path.getsize(zip_path)
    if local_size != remote_size:
        raise RuntimeError(f"B2 verification failed: local {local_size} != remote {remote_size}")
    return {"fileId": result.get("fileId"), "remote_name": remote_name, "local_size": local_size, "remote_size": remote_size}

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
    # Определяем последний обработанный фрагмент, чтобы можно было продолжить
    last_idx = get_last_processed_index_from_log(LOG_FILE)
    # Формируем промпт для характера озвучки
    vibe_prompt = format_vibe_prompt(VIBE_NAME, VIBES_DATA)

    # Основной цикл генерации аудио
    for idx in range(last_idx, len(all_chunks)):
        chunk = all_chunks[idx]
        base_name = f"part_{idx+1:04}"
        tmp_wav = os.path.join(TMP_AUDIO_DIR, f"{base_name}.wav")
        out_mp3 = os.path.join(OUTPUT_MP3_DIR, f"{base_name}.mp3")

        # Отправка запроса на генерацию аудио
        audio_content, content_type = send_request(chunk, VOICE_NAME, vibe_prompt)
        if audio_content is None:
            continue

        # Сохраняем wav
        with open(tmp_wav, "wb") as f:
            f.write(audio_content)

        # Конвертация wav в mp3 при необходимости
        if "wav" in content_type:
            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_wav(tmp_wav)
                audio = audio.set_frame_rate(SAMPLE_RATE_HZ)
                audio.export(out_mp3, format="mp3", bitrate="128k")
            except Exception:
                continue
        else:
            os.rename(tmp_wav, out_mp3)

        # Проверка размера mp3 файла
        size_kb = os.path.getsize(out_mp3) // 1024
        if not (MIN_SIZE_KB < size_kb < MAX_SIZE_KB):
            os.remove(out_mp3)
            continue

        log_to_file(f"Размер файла {out_mp3} {size_kb} КБ в пределах нормы.")

        # Если общий размер превышен, делаем zip и загружаем на B2
        total_mb = get_total_size_mb(OUTPUT_MP3_DIR)
        if total_mb >= AUDIO_SIZE_LIMIT_MB:
            zip_path, _ = zip_output_mp3()
            key_id = os.environ.get("B2_KEY_ID")
            app_key = os.environ.get("B2_APP_KEY")
            bucket_id = os.environ.get("B2_BUCKET_ID")
            bucket_name = os.environ.get("B2_BUCKET_NAME", "tts-archive")
            try:
                if key_id and app_key and bucket_id:
                    upload_result = upload_zip_to_b2_and_verify(zip_path, bucket_id, bucket_name, key_id, app_key)
                    with open(B2_MARKER_FILE, "w", encoding="utf-8") as mf:
                        json.dump(upload_result, mf)
                    os.remove(zip_path)
            except Exception as e:
                log_to_file(f"Ошибка при заливке на B2: {e}")

        # Удаляем временный wav файл
        if os.path.exists(tmp_wav):
            os.remove(tmp_wav)

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
