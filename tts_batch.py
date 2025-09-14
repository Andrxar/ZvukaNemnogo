import os
import sys
import datetime
import glob
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup

# ========== НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ ==========

# Входной текстовый файл (txt или fb2)
TEXT_FILE_NAME = "Royzman_Delo-306-Volk-Vor-nevidimka.txt"  # или .fb2

# Выходные папки (упрощённо)
OUTPUT_MP3_DIR = "output_mp3"
TMP_AUDIO_DIR = "tmp_audio"

# Проверка размеров mp3
MIN_SIZE_KB = 150
MAX_SIZE_KB = 5000
AUDIO_SIZE_LIMIT_MB = 450  # максимальный общий размер всех mp3 для одного запуска

# Голос и характер озвучки
VOICES_DATA = {
    "voices": [
        "Alloy", "Ash", "Ballad", "Coral", "Echo", "Fable", "Onyx", "Nova", "Sage", "Shimmer", "Verse"
    ]
}
VIBES_DATA = {
    "Calm (Спокойный)": ["Emotion: Искреннее сочувствие, уверенность.", "Emphasis: Выделите ключевые мысли."],
    "Energetic (Энергичный)": ["Emotion: Яркий, энергичный тон.", "Emphasis: Выделите эмоциональные слова."],
    # ... (сокращено, но можно вставить весь справочник)
}

VOICE_NAME = "Nova"
VIBE_NAME = "Calm (Спокойный)"  # или None

# ========== КОНЕЦ НАСТРОЕК ПОЛЬЗОВАТЕЛЯ ==========

LOG_FILE = "tts_batch.log"

def log_operation(message):
    try:
        with open(LOG_FILE, "a", encoding='utf-8') as log_file:
            log_file.write(f"{datetime.datetime.now()} {message}\n")
    except Exception:
        pass

def clean_text_from_fb2(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    soup = BeautifulSoup(content, 'xml')
    text = ' '.join([p.get_text() for p in soup.find_all('p')])
    unwanted_chars = set("{[*+=<>#@\\$&'\"~`/|\\()]}")
    return ''.join(c for c in text if c not in unwanted_chars)

def split_text_fragments(text, max_length=980):
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
    return fragments

def format_vibe_prompt(vibe_name, vibes_data):
    vibe_content = vibes_data.get(vibe_name)
    if vibe_content:
        return "\n\n".join(vibe_content)
    return "Voice Affect: Calm, composed, and reassuring."

def send_request(text, voice, vibe_prompt):
    url = "https://www.openai.fm/api/generate"
    boundary = "----WebKitFormBoundarya027BOtfh6crFn7A"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }
    data = [
        f"--{boundary}",
        f'Content-Disposition: form-data; name="input"\r\n\r\n{text}',
        f"--{boundary}",
        f'Content-Disposition: form-data; name="prompt"\r\n\r\n{vibe_prompt}',
        f"--{boundary}",
        f'Content-Disposition: form-data; name="voice"\r\n\r\n{voice.lower()}',
        f"--{boundary}",
        f'Content-Disposition: form-data; name="vibe"\r\n\r\nnull',
        f"--{boundary}--"
    ]
    body = "\r\n".join(data).encode('utf-8')
    try:
        response = requests.post(url, headers=headers, data=body, timeout=90)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "audio/wav" in content_type or "audio/mpeg" in content_type:
            return response.content, content_type
        log_operation(f"[LOG] API вернул неверный тип контента: {content_type}")
        return None, None
    except Exception as e:
        log_operation(f"[LOG] Сетевая ошибка при запросе к API: {e}")
        return None, None

def get_total_size_mb(directory):
    total = 0
    for f in glob.glob(os.path.join(directory, "*.mp3")):
        total += os.path.getsize(f)
    return total / (1024 * 1024)

def main():
    if not os.path.isfile(TEXT_FILE_NAME):
        log_operation(f"Файл {TEXT_FILE_NAME} не найден!")
        print(f"Файл {TEXT_FILE_NAME} не найден!")
        sys.exit(1)

    os.makedirs(OUTPUT_MP3_DIR, exist_ok=True)
    os.makedirs(TMP_AUDIO_DIR, exist_ok=True)

    # Чтение и очистка текста
    if TEXT_FILE_NAME.lower().endswith(".fb2"):
        text = clean_text_from_fb2(TEXT_FILE_NAME)
    else:
        with open(TEXT_FILE_NAME, "r", encoding="utf-8") as f:
            text = f.read()

    # Фрагментация текста
    all_chunks = split_text_fragments(text, max_length=980)
    log_operation(f"Текст разбит на {len(all_chunks)} фрагментов.")

    # Определяем с какого места продолжать
    existing_mp3 = sorted(glob.glob(os.path.join(OUTPUT_MP3_DIR, "part_*.mp3")))
    last_idx = 0
    if existing_mp3:
        last_file = os.path.basename(existing_mp3[-1])
        try:
            last_idx = int(last_file.split("_")[1].split(".")[0])
        except Exception:
            last_idx = len(existing_mp3)

    vibe_prompt = format_vibe_prompt(VIBE_NAME, VIBES_DATA)

    # Основной цикл обработки
    for idx in range(last_idx, len(all_chunks)):
        chunk = all_chunks[idx]
        base_name = f"part_{idx+1:04}"
        tmp_wav = os.path.join(TMP_AUDIO_DIR, f"{base_name}.wav")
        out_mp3 = os.path.join(OUTPUT_MP3_DIR, f"{base_name}.mp3")

        log_operation(f"Генерация {base_name}: {len(chunk)} символов.")
        tqdm.write(f"Генерация {base_name}: {len(chunk)} символов.")

        # Запрос к TTS endpoint
        audio_content, content_type = send_request(chunk, VOICE_NAME, vibe_prompt)
        if audio_content is None:
            log_operation(f"Ошибка генерации аудио для фрагмента {idx+1}. Пропуск.")
            continue

        # Сохраняем временный аудиофайл (wav/mp3)
        with open(tmp_wav, "wb") as f:
            f.write(audio_content)

        # Конвертируем в mp3 если нужно (только если пришел wav)
        if "wav" in content_type:
            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_wav(tmp_wav)
                audio.export(out_mp3, format="mp3", bitrate="192k")
            except Exception as e:
                log_operation(f"Ошибка конвертации wav->mp3: {e}")
                continue
        else:
            os.rename(tmp_wav, out_mp3)

        # Проверяем размер итогового mp3
        size_kb = os.path.getsize(out_mp3) // 1024
        if size_kb < MIN_SIZE_KB or size_kb > MAX_SIZE_KB:
            log_operation(f"Файл {out_mp3} не прошёл по размеру: {size_kb} КБ. Удалён.")
            os.remove(out_mp3)
            continue

        # Проверяем общий размер всех mp3
        total_mb = get_total_size_mb(OUTPUT_MP3_DIR)
        tqdm.write(f"Текущий общий размер mp3: {total_mb:.2f} МБ")
        if total_mb >= AUDIO_SIZE_LIMIT_MB:
            log_operation("Достигнут лимит размера. Остановка для ручной выгрузки файлов!")
            print(f"Достигнут лимит {AUDIO_SIZE_LIMIT_MB} МБ. Скачайте артефакт, удалите все mp3 кроме последнего и перезапустите!")
            break

        # Удаляем временный wav
        if os.path.exists(tmp_wav):
            os.remove(tmp_wav)

    log_operation("Работа завершена!")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log_operation(f"КРИТИЧЕСКАЯ ОШИБКА: {exc}")
        sys.exit(1)
