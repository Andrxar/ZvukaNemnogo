import os
import sys
import datetime
import glob
import requests
import re
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
AUDIO_SIZE_LIMIT_MB = 5  # максимальный общий размер всех mp3 для одного запуска

# Частота дискретизации (18, 20, 22, 24, 26, 28 kHz)
SAMPLE_RATE_HZ = 22000 # Значение должно быть одним из: 18000, 20000, 22000, 24000, 26000, 28000

# Голос и характер озвучки
VOICES_DATA = { "voices": [ "Alloy", "Ash", "Ballad", "Coral", "Echo", "Fable", "Onyx", "Nova", "Sage", "Shimmer", "Verse" ] }
VIBES_DATA = { "Calm (Спокойный)": ["Emotion: Искреннее сочувствие, уверенность.", "Emphasis: Выделите ключевые мысли."], "Energetic (Энергичный)": ["Emotion: Яркий, энергичный тон.", "Emphasis: Выделите эмоциональные слова."], }

VOICE_NAME = "Ballad"
VIBE_NAME = "Energetic (Энергичный)"  # или None

# ========== КОНЕЦ НАСТРОЕК ПОЛЬЗОВАТЕЛЯ ==========

LOG_FILE = "tts_batch.log"

def log_operation(message):
    print(message)
    try:
        with open(LOG_FILE, "a", encoding='utf-8') as log_file:
            log_file.write(f"{datetime.datetime.now()} {message}\n")
    except Exception:
        pass

def clean_text_from_fb2(file_path):
    log_operation(f"Очистка текста из файла FB2: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    soup = BeautifulSoup(content, 'xml')
    text = ' '.join([p.get_text() for p in soup.find_all('p')])
    unwanted_chars = set("{[*+=<>#@\\$&'\"~`/|\\()]}")
    cleaned_text = ''.join(c for c in text if c not in unwanted_chars)
    log_operation("Очистка текста из FB2 завершена.")
    return cleaned_text

def split_text_fragments(text, max_length=980):
    log_operation("Разбивка текста на фрагменты...")
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
    log_operation(f"Текст разбит на {len(fragments)} фрагментов.")
    return fragments

def format_vibe_prompt(vibe_name, vibes_data):
    log_operation(f"Форматирование промпта для характера: {vibe_name}")
    vibe_content = vibes_data.get(vibe_name)
    if vibe_content:
        return "\n\n".join(vibe_content)
    log_operation("Характер не найден, используется стандартный промпт.")
    return "Voice Affect: Calm, composed, and reassuring."

def send_request(text, voice, vibe_prompt):
    log_operation(f"Отправка запроса к API для генерации аудио (голос: {voice}).")
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
            log_operation("Аудио контент успешно получен.")
            return response.content, content_type
        log_operation(f"[LOG] API вернул неверный тип контента: {content_type}")
        return None, None
    except Exception as e:
        log_operation(f"[LOG] Сетевая ошибка при запросе к API: {e}")
        return None, None

def get_total_size_mb(directory):
    log_operation(f"Подсчет общего размера файлов в папке: {directory}")
    total = sum(os.path.getsize(f) for f in glob.glob(os.path.join(directory, "*.mp3")))
    total_mb = total / (1024 * 1024)
    log_operation(f"Общий размер: {total_mb:.2f} МБ.")
    return total_mb

# =============================================================================
# ИЗМЕНЕННАЯ И БОЛЕЕ НАДЕЖНАЯ ФУНКЦИЯ ДЛЯ ОПРЕДЕЛЕНИЯ ТОЧКИ ВОЗОБНОВЛЕНИЯ
# =============================================================================
def get_last_processed_index_from_log(log_file_path):
    log_operation(f"Поиск точки возобновления в лог-файле: {log_file_path}")
    
    if not os.path.exists(log_file_path):
        log_operation("Лог-файл не найден. Работа начнется с самого начала.")
        return 0

    last_successful_index = 0
    target_string = "в пределах нормы"
    lines_read = 0
    found_matches = 0

    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        lines_read = len(lines)

        # Ищем последнее совпадение, проходя по файлу с начала до конца
        for line in lines:
            if target_string in line:
                match = re.search(r"part_(\d+)\.mp3", line)
                if match:
                    # Мы нашли совпадение, запоминаем его номер и продолжаем.
                    # Это гарантирует, что мы найдем самый последний номер в файле.
                    last_successful_index = int(match.group(1))
                    found_matches += 1
        
        # После цикла, у нас будет самый последний найденный номер
        if found_matches > 0:
            log_operation(f"Диагностика: Прочитано {lines_read} строк. Найдено {found_matches} совпадений.")
            log_operation(f"Последний успешный фрагмент: {last_successful_index}. Возобновление со следующего.")
            return last_successful_index
        else:
            log_operation(f"Диагностика: Прочитано {lines_read} строк, но совпадений с '{target_string}' не найдено.")
            log_operation("Работа начнется с самого начала.")
            return 0

    except Exception as e:
        log_operation(f"Критическая ошибка при чтении лог-файла: {e}. Работа начнется с начала.")
        return 0
# =============================================================================

def main():
    log_operation("Начало работы скрипта.")
    if not os.path.isfile(TEXT_FILE_NAME):
        log_operation(f"Файл {TEXT_FILE_NAME} не найден!")
        print(f"Файл {TEXT_FILE_NAME} не найден!")
        sys.exit(1)

    log_operation(f"Создание папок {OUTPUT_MP3_DIR} и {TMP_AUDIO_DIR}, если они не существуют.")
    os.makedirs(OUTPUT_MP3_DIR, exist_ok=True)
    os.makedirs(TMP_AUDIO_DIR, exist_ok=True)

    # Чтение и очистка текста
    log_operation(f"Чтение текста из файла: {TEXT_FILE_NAME}")
    if TEXT_FILE_NAME.lower().endswith(".fb2"):
        text = clean_text_from_fb2(TEXT_FILE_NAME)
    else:
        with open(TEXT_FILE_NAME, "r", encoding="utf-8") as f:
            text = f.read()
    log_operation("Чтение и очистка текста завершены.")

    # Фрагментация текста
    all_chunks = split_text_fragments(text, max_length=980)

    # =============================================================================
    # ИЗМЕНЕННЫЙ БЛОК: ОПРЕДЕЛЕНИЕ ТОЧКИ ВОЗОБНОВЛЕНИЯ ИЗ ЛОГА
    # Старый код, искавший .mp3 файлы, удален.
    # =============================================================================
    last_idx = get_last_processed_index_from_log(LOG_FILE)
    # =============================================================================

    vibe_prompt = format_vibe_prompt(VIBE_NAME, VIBES_DATA)

    # Основной цикл обработки
    log_operation("Начало основного цикла обработки фрагментов.")
    for idx in range(last_idx, len(all_chunks)):
        chunk = all_chunks[idx]
        base_name = f"part_{idx+1:04}"
        tmp_wav = os.path.join(TMP_AUDIO_DIR, f"{base_name}.wav")
        out_mp3 = os.path.join(OUTPUT_MP3_DIR, f"{base_name}.mp3")

        log_operation(f"Генерация {base_name}: {len(chunk)} символов.")
        
        audio_content, content_type = send_request(chunk, VOICE_NAME, vibe_prompt)
        if audio_content is None:
            log_operation(f"Ошибка генерации аудио для фрагмента {idx+1}. Пропуск.")
            continue

        with open(tmp_wav, "wb") as f:
            f.write(audio_content)

        if "wav" in content_type:
            log_operation(f"Конвертация {tmp_wav} в {out_mp3}...")
            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_wav(tmp_wav)
                if SAMPLE_RATE_HZ and SAMPLE_RATE_HZ in [18000, 20000, 22000, 24000, 26000, 28000]:
                    log_operation(f"Установка частоты дискретизации: {SAMPLE_RATE_HZ} Hz")
                    audio = audio.set_frame_rate(SAMPLE_RATE_HZ)
                audio.export(out_mp3, format="mp3", bitrate="128k")
                log_operation("Конвертация в mp3 завершена.")
            except Exception as e:
                log_operation(f"Ошибка конвертации wav->mp3: {e}")
                continue
        else:
            log_operation(f"Переименование {tmp_wav} в {out_mp3} (файл уже в mp3).")
            os.rename(tmp_wav, out_mp3)

        log_operation(f"Проверка размера файла: {out_mp3}")
        size_kb = os.path.getsize(out_mp3) // 1024
        if size_kb < MIN_SIZE_KB or size_kb > MAX_SIZE_KB:
            log_operation(f"Файл {out_mp3} не прошёл по размеру: {size_kb} КБ. Удалён.")
            os.remove(out_mp3)
            continue
        log_operation(f"Размер файла {size_kb} КБ в пределах нормы.")

        # =============================================================================
        # ИЗМЕНЕННЫЙ БЛОК: УДАЛЕНА НЕКОРРЕКТНАЯ ИНСТРУКЦИЯ ДЛЯ ПОЛЬЗОВАТЕЛЯ
        # =============================================================================
        # Проверяем общий размер всех mp3
        total_mb = get_total_size_mb(OUTPUT_MP3_DIR)
        if total_mb >= AUDIO_SIZE_LIMIT_MB:
            log_operation(f"Достигнут лимит размера {AUDIO_SIZE_LIMIT_MB} МБ. Работа будет остановлена.")
            print(f"Достигнут лимит размера {AUDIO_SIZE_LIMIT_MB} МБ. Запустите workflow заново для продолжения.")
            break
        # =============================================================================
        
        if os.path.exists(tmp_wav):
            log_operation(f"Удаление временного файла: {tmp_wav}")
            os.remove(tmp_wav)

    log_operation("Работа завершена!")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log_operation(f"КРИТИЧЕСКАЯ ОШИБКА: {exc}")
        sys.exit(1)
