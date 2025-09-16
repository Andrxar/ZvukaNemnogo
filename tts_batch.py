# =============================================================================
# ИМПОРТ НЕОБХОДИМЫХ БИБЛИОТЕК
# =============================================================================
import os                   # Для работы с файловой системой (создание папок, проверка файлов)
import sys                  # Для работы с системными функциями (например, выход из программы)
import datetime             # Для добавления временных меток в лог-файл
import glob                 # Для поиска файлов по шаблону (например, всех .mp3 файлов в папке)
import requests             # Для отправки HTTP-запросов к TTS API
import re                   # Для работы с регулярными выражениями (поиск номера в логах)
from tqdm import tqdm       # Библиотека для создания красивых индикаторов прогресса (в данном коде не используется, но была в оригинале)
from bs4 import BeautifulSoup # Библиотека для парсинга XML/HTML, используется для очистки .fb2 файлов

# =============================================================================
# ========== НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ ==========
# В этом блоке собраны все параметры, которые можно изменять для настройки
# поведения скрипта под конкретную задачу.
# =============================================================================

# Укажите здесь имя вашего текстового файла. Поддерживаются форматы .txt и .fb2.
# Файл должен лежать в той же папке, что и сам скрипт.
TEXT_FILE_NAME = "Royzman_Delo-306-Volk-Vor-nevidimka.txt"  # или, например, "my_book.fb2"

# --- Настройки папок ---
OUTPUT_MP3_DIR = "output_mp3" # Папка, куда будут сохраняться готовые MP3 файлы. Создается автоматически.
TMP_AUDIO_DIR = "tmp_audio"   # Временная папка для промежуточных файлов (например, .wav). Создается автоматически.

# --- Настройки контроля размера файлов ---
MIN_SIZE_KB = 150
MAX_SIZE_KB = 5000
AUDIO_SIZE_LIMIT_MB = 5  # максимальный общий размер всех mp3 для одного запуска

# --- Настройки качества аудио ---
SAMPLE_RATE_HZ = 22000

# --- Настройки голоса и интонации ---
VOICES_DATA = { "voices": [ "Alloy", "Ash", "Ballad", "Coral", "Echo", "Fable", "Onyx", "Nova", "Sage", "Shimmer", "Verse" ] }
VIBES_DATA = { "Calm (Спокойный)": ["Emotion: Искреннее сочувствие, уверенность.", "Emphasis: Выделите ключевые мысли."], "Energetic (Энергичный)": ["Emotion: Яркий, энергичный тон.", "Emphasis: Выделите эмоциональные слова."], }
VOICE_NAME = "Ballad"
VIBE_NAME = "Energetic (Энергичный)"

# =============================================================================
# ========== КОНЕЦ НАСТРОЕК ПОЛЬЗОВАТЕЛЯ ==========
# =============================================================================

# =============================================================================
# НОВОЕ: ДИНАМИЧЕСКОЕ ИМЯ ЛОГ-ФАЙЛА
# Имя файла теперь зависит от имени книги, чтобы избежать путаницы при смене книги.
# =============================================================================
BOOK_NAME_CLEAN = os.path.splitext(TEXT_FILE_NAME)[0]
LOG_FILE = f"{BOOK_NAME_CLEAN}.log"

def log_to_file(message):
    """
    Эта функция ТОЛЬКО пишет сообщение в лог-файл, без вывода в консоль.
    """
    try:
        with open(LOG_FILE, "a", encoding='utf-8') as log_file:
            log_file.write(f"{datetime.datetime.now()} {message}\n")
    except Exception:
        pass

def clean_text_from_fb2(file_path):
    """
    Очищает текст из файла формата FB2.
    """
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
    """
    Разбивает один большой текст на небольшие фрагменты (чанки).
    """
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
    """
    Формирует текстовый промпт для API на основе выбранной интонации.
    """
    print(f"Форматирование промпта для характера: {vibe_name}")
    vibe_content = vibes_data.get(vibe_name)
    if vibe_content:
        return "\n\n".join(vibe_content)
    print("Характер не найден, используется стандартный промпт.")
    return "Voice Affect: Calm, composed, and reassuring."

def send_request(text, voice, vibe_prompt):
    """
    Отправляет запрос к TTS API.
    """
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
    """
    Подсчитывает общий размер всех .mp3 файлов в указанной папке.
    """
    total = sum(os.path.getsize(f) for f in glob.glob(os.path.join(directory, "*.mp3")))
    total_mb = total / (1024 * 1024)
    print(f"Общий размер: {total_mb:.2f} МБ.")
    return total_mb

def get_last_processed_index_from_log(log_file_path):
    """
    Ключевая функция для возобновления работы.
    """
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

def main():
    """
    Главная управляющая функция скрипта.
    """
    # =============================================================================
    # НОВЫЙ БЛОК: Передача переменных из Python в YML-файл
    # =============================================================================
    github_output_file = os.getenv('GITHUB_OUTPUT')
    if github_output_file:
        with open(github_output_file, 'a') as f:
            print(f'book_name={BOOK_NAME_CLEAN}', file=f)
            print(f'log_filename={LOG_FILE}', file=f)

    # --- 1. ПОДГОТОВКА ---
    print("Начало работы скрипта.")
    log_to_file("Начало работы скрипта.")

    if not os.path.isfile(TEXT_FILE_NAME):
        print(f"Файл {TEXT_FILE_NAME} не найден!")
        log_to_file(f"Файл {TEXT_FILE_NAME} не найден!")
        sys.exit(1)

    print(f"Создание папок {OUTPUT_MP3_DIR} и {TMP_AUDIO_DIR}, если они не существуют.")
    os.makedirs(OUTPUT_MP3_DIR, exist_ok=True)
    os.makedirs(TMP_AUDIO_DIR, exist_ok=True)

    # --- 2. ЧТЕНИЕ И ОБРАБОТКА ТЕКСТА ---
    print(f"Чтение текста из файла: {TEXT_FILE_NAME}")
    if TEXT_FILE_NAME.lower().endswith(".fb2"):
        text = clean_text_from_fb2(TEXT_FILE_NAME)
    else:
        with open(TEXT_FILE_NAME, "r", encoding="utf-8") as f:
            text = f.read()
    print("Чтение и очистка текста завершены.")

    all_chunks = split_text_fragments(text, max_length=980)

    # --- 3. ОПРЕДЕЛЕНИЕ ТОЧКИ ВОЗОБНОВЛЕНИЯ ---
    last_idx = get_last_processed_index_from_log(LOG_FILE)
    
    vibe_prompt = format_vibe_prompt(VIBE_NAME, VIBES_DATA)

    # --- 4. ОСНОВНОЙ ЦИКЛ ОБРАБОТКИ ФРАГМЕНТОВ ---
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

        total_mb = get_total_size_mb(OUTPUT_MP3_DIR)
        if total_mb >= AUDIO_SIZE_LIMIT_MB:
            print(f"Достигнут лимит размера {AUDIO_SIZE_LIMIT_MB} МБ. Работа будет остановлена.")
            break
        
        if os.path.exists(tmp_wav):
            os.remove(tmp_wav)

    print("Работа завершена!")
    log_to_file("Работа завершена!")

# =============================================================================
# ТОЧКА ВХОДА В ПРОГРАММУ
# =============================================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {exc}")
        log_to_file(f"КРИТИЧЕСКАЯ ОШИБКА: {exc}")
        sys.exit(1)
