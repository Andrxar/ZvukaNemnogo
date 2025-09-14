# --- 1. Установка всех необходимых библиотек ---
print("Шаг 1: Установка зависимостей...")
!pip install -q git+https://github.com/openai/whisper.git
!pip install -q requests beautifulsoup4 numpy pydub
print("Установка завершена.")

# --- 2. Импорты и подготовка ---
import os
import datetime
import json
import time
import re
from requests import Session
from bs4 import BeautifulSoup
import wave
import contextlib
import numpy as np
import shutil
from google.colab import drive
from tqdm.notebook import tqdm
import whisper
from pydub import AudioSegment
from pydub.silence import split_on_silence
import torch

# --- Встраиваем ваши файлы voices.json и vibes.json прямо в код ---
VOICES_DATA = {
    "voices": ["Alloy", "Ash", "Ballad", "Coral", "Echo", "Fable", "Onyx", "Nova", "Sage", "Shimmer", "Verse"]
}
VIBES_DATA = {
    "Angry (Сердитый)": ["Emotion(Эмоция): Страстная...", "Emphasis(Подчеркните): Выделите слова..."],
    "Anxious (Тревожный)": ["Emotion(Эмоция): Нервная...", "Emphasis(Подчеркните): Выделите слова..."],
    "Blissful (Блаженный)": ["Emotion(Эмоция): Чистая радость...", "Emphasis(Подчеркните): Выделите слова..."],
    "Calm (Спокойный)": ["Emotion(Эмоция): Искреннее сочувствие...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Compassionate (Сострадательный)": ["Emotion(Эмоция): Заботливый...", "Emphasis(Подчеркните): Выделите слова..."],
    "Confident (Уверенный)": ["Emotion(Эмоция): Уверенный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Contemplative (Созерцательный)": ["Emotion(Эмоция): Задумчивый...", "Emphasis(Подчеркните): Выделите слова..."],
    "Curious (Любознательный)": ["Emotion(Эмоция): Любознательный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Delighted (Восхищенный)": ["Emotion(Эмоция): Радостный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Dreamy (Мечтательный)": ["Emotion(Эмоция): Причудливый...", "Emphasis(Подчеркните): Выделите слова..."],
    "Energetic (Энергичный)": ["Emotion(Эмоция): Яркий...", "Emphasis(Подчеркните): Выделите слова..."],
    "Grateful (Благодарный)": ["Emotion(Эмоция): Благодарный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Hopeful (Надежный)": ["Emotion(Эмоция): Оптимистичный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Melancholic (Меланхоличный)": ["Emotion(Эмоция): Задумчивый...", "Emphasis(Подчеркните): Выделите слова..."],
    "Nostalgic (Ностальгический)": ["Emotion(Эмоция): Воспоминательный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Peaceful (Мирный)": ["Emotion(Эмоция): Безмятежный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Playful (Игривый)": ["Emotion(Эмоция): Веселый...", "Emphasis(Подчеркните): Выделите слова..."],
    "Reflective (Размышляющий)": ["Emotion(Эмоция): Медитативный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Romantic (Романтичный)": ["Emotion(Эмоция): Нежный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Serene (Безмятежный)": ["Emotion(Эмоция): Теплота...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Soulful (Душевный)": ["Emotion(Эмоция): Глубокий...", "Emphasis(Подчеркните): Выделите слова..."],
    "Whimsical (Причудливый)": ["Emotion(Эмоция): Фантастический...", "Emphasis(Подчеркните): Выделите слова..."],
    "Auctioneer (Аукционист)": ["Emotion(Эмоция): Энергичный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Cheerleader (Болельщик)": ["Emotion(Эмоция): Яркий...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Corporate_Executive (Корпоративный руководитель)": ["Emotion(Эмоция): Уверенный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Detective (Детектив)": ["Emotion(Эмоция): Заинтригованный...", "Emphasis(Подчеркните): Выделите улики..."],
    "Doctor (Доктор)": ["Emotion(Эмоция): Сочувствующий...", "Emphasis(Подчеркните): Выделите важную..."],
    "Dramatic (Драматичный)": ["Emotion(Эмоция): Сдержанный...", "Emphasis(Подчеркните): Выделите слова..."],
    "Fitness_Instructor (Инструктор по фитнесу)": ["Emotion(Эмоция): Воодушевляющий...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Food_Critic (Критик еды)": ["Emotion(Эмоция): Выразительный...", "Emphasis(Подчеркните): Выделите сенсорные..."],
    "Game_Show_Host (Ведущий игрового шоу)": ["Emotion(Эмоция): Восторженный...", "Emphasis(Подчеркните): Выделите игровые..."],
    "Librarian (Библиотекарь)": ["Emotion(Эмоция): Спокойный...", "Emphasis(Подчеркните): Выделите ключевую..."],
    "Life_Coach (Лайф-коуч)": ["Emotion(Эмоция): Оптимистичный...", "Emphasis(Подчеркните): Выделите мотивационные..."],
    "Medieval_Knight (Средневековый рыцарь)": ["Emotion(Эмоция): Восторг...", "Emphasis(Подчеркните): Паузы после..."],
    "Movie_Trailer (Трейлер фильма)": ["Emotion(Эмоция): Волнительный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "News_Anchor (Ведущий новостей)": ["Emotion(Эмоция): Беспристрастный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Patient_Teacher (Терпеливый учитель)": ["Emotion(Эмоция): Искреннее сочувствие...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Poetry_Reader (Чтец поэзии)": ["Emotion(Эмоция): Эмоциональный...", "Emphasis(Подчеркните): Выделите поэтические..."],
    "Radio_Host (Радиоведущий)": ["Emotion(Эмоция): Энтузиастичный...", "Emphasis(Подчеркните): Выделите увлекательные..."],
    "Scientific_Narrator (Научный рассказчик)": ["Emotion(Эмоция): Беспристрастный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Smooth_Jazz_DJ (Диджей смуз-джаза)": ["Emotion(Эмоция): Расслабленный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Sincere (Искренний)": ["Emotion(Эмоция): Спокойное уверение...", "Pacing(Темп): Медленный во время..."],
    "Sports_Commentator (Спортивный комментатор)": ["Emotion(Эмоция): Восторженный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Stand_Up_Comic (Стендап-комик)": ["Emotion(Эмоция): Веселый...", "Emphasis(Подчеркните): Выделите кульминационные..."],
    "Storyteller (Рассказчик)": ["Emotion(Эмоция): Эмоциональный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Surfer (Серфер)": ["Emotion(Эмоция): Беззаботный...", "Emphasis(Подчеркните): Выделите позитивные..."],
    "Tour_Guide (Гид)": ["Emotion(Эмоция): Восторженный...", "Emphasis(Подчеркните): Выделите ключевые..."],
    "Wise_Elder (Мудрый старейшина)": ["Emotion(Эмоция): Спокойный...", "Emphasis(Подчеркните): Выделите мудрые..."]
}

session = Session()
all_good_pages_dir = ""

# --- 3. Конфигурация ---
print("\nШаг 2: Настройка параметров...")

TEXT_FILE_PATH = '/content/drive/MyDrive/my_text.txt'
SELECTED_VOICE_NAME = 'Alloy'
SELECTED_VIBE_NAME = 'Calm (Спокойный)'
MIN_SIZE_KB = 150
MAX_SIZE_KB = 5000

print("Конфигурация завершена.")


# --- 4. Функции ---

def log_operation(message):
    try:
        with open(os.path.join(all_good_pages_dir, "order_logs.txt"), "a", encoding='utf-8') as log_file:
            log_file.write(f"{datetime.datetime.now()} {message}\n")
    except Exception:
        pass

def format_vibe_prompt(vibe_name, vibes_data):
    vibe_content = vibes_data.get(vibe_name)
    if vibe_content: return "\\n\\n".join(vibe_content)
    return "Voice Affect: Calm, composed, and reassuring."

def send_request(text, voice, vibe_prompt):
    url = "https://www.openai.fm/api/generate"
    boundary = "----WebKitFormBoundarya027BOtfh6crFn7A"
    headers = {"User-Agent": "Mozilla/5.0...", "Content-Type": f"multipart/form-data; boundary={boundary}"}
    data = [f"--{boundary}", f'Content-Disposition: form-data; name="input"\r\n\r\n{text}',
            f"--{boundary}", f'Content-Disposition: form-data; name="prompt"\r\n\r\n{vibe_prompt}',
            f"--{boundary}", f'Content-Disposition: form-data; name="voice"\r\n\r\n{voice.lower()}',
            f"--{boundary}", f'Content-Disposition: form-data; name="vibe"\r\n\r\nnull', f"--{boundary}--"]
    body = "\r\n".join(data).encode('utf-8')
    try:
        response = session.post(url, headers=headers, data=body, timeout=90)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "audio/wav" in content_type or "audio/mpeg" in content_type:
            return response.content, content_type
        tqdm.write(f"  [ЛОГ] API вернул неверный тип контента: {content_type}")
        return None, None
    except Exception as e:
        tqdm.write(f"  [ЛОГ] Сетевая ошибка при запросе к API: {e}")
        return None, None

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

def merge_audio_files(file_list, output_path):
    combined = AudioSegment.empty()
    for file_path in file_list:
        try: combined += AudioSegment.from_wav(file_path)
        except Exception as e: tqdm.write(f"Не удалось добавить файл {os.path.basename(file_path)} в сборку: {e}")
    combined.export(output_path, format="wav")

whisper_model = None
def run_whisper_check(file_path):
    global whisper_model
    try:
        if whisper_model is None:
            tqdm.write("  [ЛОГ] Загрузка модели Whisper (только один раз)...")
            whisper_model = whisper.load_model("tiny")
            tqdm.write("  [ЛОГ] Модель Whisper загружена.")
        result = whisper_model.transcribe(file_path, fp16=torch.cuda.is_available())

        # !!! КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: ПРИНИМАЕМ РУССКИЙ И УКРАИНСКИЙ ЯЗЫКИ !!!
        is_valid_language = result.get('language') in ['ru', 'uk'] and result.get('text', '').strip()

        return is_valid_language, result.get('language', 'N/A'), result.get('text', '')
    except Exception as e:
        tqdm.write(f"  [ЛОГ] Критическая ошибка Whisper: {e}")
        return False, "error", ""

def edit_silence(file_path, output_path):
    try:
        audio = AudioSegment.from_file(file_path)
        chunks = split_on_silence(audio, min_silence_len=4000, silence_thresh=audio.dBFS-14)
        processed_audio = AudioSegment.silent(duration=0)
        for i, chunk in enumerate(chunks):
            processed_audio += chunk
            if i < len(chunks) - 1:
                processed_audio += AudioSegment.silent(duration=2000)
        processed_audio.export(output_path, format="wav")
        return True
    except Exception as e:
        tqdm.write(f"  [ЛОГ] Ошибка при обработке тишины: {e}")
        return False

# --- 5. Основная логика ---
def main():
    global all_good_pages_dir
    drive.mount('/content/drive')

    if not os.path.exists(TEXT_FILE_PATH):
        raise FileNotFoundError(f"Файл НЕ НАЙДЕН: {TEXT_FILE_PATH}.")

    base_filename = os.path.splitext(os.path.basename(TEXT_FILE_PATH))[0]
    base_output_path = f'/content/drive/MyDrive/{base_filename}'

    output_dir = base_output_path
    counter = 1
    if os.path.exists(output_dir):
        while os.path.exists(f"{base_output_path}_{counter}"):
            counter += 1
        output_dir = f"{base_output_path}_{counter - 1}" if counter > 1 else base_output_path
        print(f"Найдена существующая папка, продолжаем работу в: {output_dir}")
    else:
        os.makedirs(output_dir)
        print(f"Создана новая папка для аудиофайлов: {output_dir}")

    temp_dir = os.path.join(output_dir, "TEMP_audio")
    os.makedirs(temp_dir, exist_ok=True)
    all_good_pages_dir = os.path.join(output_dir, "all_good_pages")
    os.makedirs(all_good_pages_dir, exist_ok=True)

    log_operation(f"Программа запущена. Выходная папка: {output_dir}")

    if TEXT_FILE_PATH.endswith('.fb2'):
        text = clean_text_from_fb2(TEXT_FILE_PATH)
    else:
        with open(TEXT_FILE_PATH, 'r', encoding='utf-8') as file:
            text = file.read()

    fragments = split_text_fragments(text)
    print(f"Текст разбит на {len(fragments)} фрагментов.")

    voices = VOICES_DATA["voices"]
    vibes_data = VIBES_DATA
    vibe_choices = list(vibes_data.keys())

    selected_voice = SELECTED_VOICE_NAME
    selected_vibe_name = SELECTED_VIBE_NAME
    original_voice = selected_voice
    original_vibe_name = selected_vibe_name

    # !!! КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: СПАСАТЕЛЬНАЯ ОПЕРАЦИЯ TEMP ИНТЕГРИРОВАНА И ВЫПОЛНЯЕТСЯ ПЕРЕД СКАНИРОВАНИЕМ all_good_pages !!!
    print("\n--- Проверка TEMP на наличие необработанных файлов... ---")
    if os.path.exists(temp_dir):
        temp_files_to_process = [f for f in os.listdir(temp_dir) if re.match(r'^\d{5}-tmp\.(wav|mp3)$', f)]
        if temp_files_to_process:
            print(f"Найдено {len(temp_files_to_process)} 'осиротевших' файлов. Попытка восстановить...")
            for temp_filename in tqdm(temp_files_to_process, desc="Восстановление файлов"):
                index_str = re.match(r'^(\d{5})', temp_filename)
                if not index_str: continue
                index = int(index_str.group(1))
                if index > len(fragments): continue # Проверка на случай, если файл от старого текста
                fragment = fragments[index - 1]

                temp_filepath = os.path.join(temp_dir, temp_filename)
                temp_wav_path = os.path.join(temp_dir, f"{index:05d}-tmp.wav")

                if not temp_filename.endswith('.wav'):
                     try:
                         AudioSegment.from_file(temp_filepath).export(temp_wav_path, format="wav")
                         os.remove(temp_filepath)
                     except Exception as e:
                         tqdm.write(f"  [ЛОГ] Не удалось конвертировать TEMP файл {temp_filename}: {e}")
                         continue
                else:
                    os.rename(temp_filepath, temp_wav_path)


                is_valid, lang, _ = run_whisper_check(temp_wav_path)
                if is_valid:
                    edited_filepath = os.path.join(temp_dir, f"{index:05d}-edited.wav")
                    if edit_silence(temp_wav_path, edited_filepath):
                        file_size_kb = os.path.getsize(edited_filepath) / 1024
                        final_filepath = os.path.join(all_good_pages_dir, f"{index:05d}.wav")
                        # Проверяем, не существует ли уже файл с таким индексом в all_good_pages
                        if not os.path.exists(final_filepath) and not os.path.exists(os.path.join(all_good_pages_dir, f"{index:05d}.txt")):
                            shutil.move(edited_filepath, final_filepath)

                            if MIN_SIZE_KB <= file_size_kb <= MAX_SIZE_KB:
                                tqdm.write(f"  [ЛОГ] Восстановлен файл {index}: OK ({lang}, {file_size_kb:.1f} КБ).")
                            else:
                                txt_filepath = os.path.join(all_good_pages_dir, f"{index:05d}.txt")
                                with open(txt_filepath, 'w', encoding='utf-8') as f: f.write(fragment)
                                # Удаляем подозрительный wav файл, оставляем только txt
                                if os.path.exists(final_filepath): os.remove(final_filepath)
                                tqdm.write(f"  [ЛОГ] Восстановлен файл {index}: ПОДОЗРИТЕЛЬНЫЙ ({lang}, {file_size_kb:.1f} КБ). Сохранен .txt")
                        else:
                            tqdm.write(f"  [ЛОГ] Файл {index}.wav/.txt уже существует в all_good_pages. Пропускаем восстановление.")
                            if os.path.exists(edited_filepath): os.remove(edited_filepath) # Удаляем дубликат
                if os.path.exists(temp_wav_path): os.remove(temp_wav_path)
            print("Восстановление завершено.")


    # !!! КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: ЗАГРУЗКА СУЩЕСТВУЮЩИХ ФАЙЛОВ ДЛЯ БАТЧЕЙ И ОПРЕДЕЛЕНИЕ НАИБОЛЬШЕГО ИНДЕКСА !!!
    primary_files_batch = []
    highest_index = 0
    if os.path.exists(all_good_pages_dir):
        existing_files = sorted([f for f in os.listdir(all_good_pages_dir) if f.endswith('.wav') or f.endswith('.txt')])
        for filename in existing_files:
             match_individual = re.match(r'^(\d{5})\.(wav|txt)$', filename)
             match_merged = re.match(r'^(\d{5})(?: \(([^,]+), ([^)]+)\))?\.wav$', filename)

             current_index = -1
             if match_individual:
                 current_index = int(match_individual.group(1))
                 if filename.endswith('.wav'):
                     # Добавляем индивидуальные wav файлы в батч только если они не являются частью объединенных файлов
                     # (Это будет не совсем точно, но лучше, чем ничего, если объединенные файлы были удалены вручную)
                     # Более надежный способ - проверять, есть ли этот файл в списке файлов, из которых собраны объединенные
                     # Но для простоты пока оставим так, полагаясь на то, что объединенные файлы не удаляются
                     primary_files_batch.append((os.path.join(all_good_pages_dir, filename), "EXISTING", "EXISTING"))
             elif match_merged:
                 current_index = int(match_merged.group(1)) + 9 # Объединенный файл содержит 10 фрагментов, берем индекс последнего

             if current_index > highest_index:
                 highest_index = current_index

    print(f"Найдено {len([f for f in os.listdir(all_good_pages_dir) if f.endswith('.wav') or f.endswith('.txt')])} существующих файлов в {all_good_pages_dir}.")
    if highest_index > 0:
        print(f"Наибольший найденный индекс фрагмента (включая объединенные файлы): {highest_index}.")
        print(f"Продолжаем озвучку с фрагмента номер {highest_index + 1}.")
    else:
        print("Существующих файлов не найдено. Начинаем озвучку с фрагмента номер 1.")


    # !!! КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: ОБРАБОТКА ПЕРВОНАЧАЛЬНОГО БАТЧА, ВКЛЮЧАЮЩЕГО СУЩЕСТВУЮЩИЕ ФАЙЛЫ !!!
    # Первоначальное объединение батча из существующих файлов
    # Оставляем эту логику, но она будет выполняться только если highest_index не слишком велик
    # и в primary_files_batch достаточно файлов, которые еще не были объединены
    # (т.е. это индивидуальные файлы, оставшиеся от предыдущих прерванных партий)
    # Проверяем, нужно ли объединить начальную партию из *собранных* primary_files_batch
    # Фильтруем primary_files_batch, оставляя только те файлы, индексы которых меньше или равны highest_index
    # и которые еще не были объединены (это сложная проверка, пока пропустим ее для простоты)
    # Вместо этого, давайте просто объединим те файлы из primary_files_batch, которые собрались в начале,
    # при условии, что их количество >= 10 и первый файл в батче имеет индекс <= highest_index - 9
    # Эта логика тоже не совсем идеальна при ручном удалении файлов, но покроет большинство случаев

    # Давайте упростим: primary_files_batch теперь содержит только индивидуальные .wav файлы
    # Мы их будем объединять по ходу выполнения основного цикла, как и новые
    # Удалим старую логику первоначального объединения батча


    for index, fragment in tqdm(enumerate(fragments, start=1), total=len(fragments), desc="Озвучка фрагментов"):

        # !!! КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: ПРОПУСКАЕМ ФРАГМЕНТЫ ДО highest_index !!!
        if index <= highest_index:
            continue

        # !!! КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: ПРОВЕРКА РАЗМЕРА БАТЧА И ОБЪЕДИНЕНИЕ ВО ВРЕМЯ ОСНОВНОГО ЦИКЛА !!!
        # Теперь primary_files_batch будет накапливать как новые, так и существующие индивидуальные файлы
        # Логика объединения остается прежней, но она будет срабатывать, когда в батче накопится 10 файлов
        # (будь то новые, старые или их комбинация), при условии, что первый файл в батче имеет индекс
        # >= highest_index + 1 (что уже гарантируется условием цикла) и батч не содержит .txt
        if len(primary_files_batch) >= 10 and not any(f[0].endswith('.txt') for f in primary_files_batch[:10]):
            batch_to_merge = primary_files_batch[:10]
            first_file_info = batch_to_merge[0]
            # Извлекаем индекс из имени файла, учитывая формат '00000.wav' или '00000 (...).wav'
            match_index = re.match(r'^(\d{5})', os.path.basename(first_file_info[0]))
            if match_index:
                first_index = int(match_index.group(1))
                # Проверяем, что первый файл в батче действительно из диапазона, который нужно объединять
                # (индекс должен быть больше highest_index, чтобы не пытаться объединить уже объединенное)
                # Хотя условие цикла уже это гарантирует, добавим проверку на всякий случай.
                # Главное - убедиться, что имя файла для объединенного файла формируется корректно
                # из ПЕРВОГО файла в текущем батче primary_files_batch
                # voice_info и vibe_info должны браться из первого файла батча
                voice_info_for_merge = first_file_info[1] if first_file_info[1] != "EXISTING" else "MERGED" # Указываем, что это объединенный из существующих
                vibe_info_for_merge = first_file_info[2] if first_file_info[2] != "EXISTING" else "MERGED"
                secondary_filename = f"{first_index:05d} ({voice_info_for_merge}, {vibe_info_for_merge}).wav"
                secondary_filepath = os.path.join(all_good_pages_dir, secondary_filename)
                tqdm.write(f"Объединение {len(batch_to_merge)} файлов в {secondary_filename} (начинается с индекса {first_index})...")
                merge_audio_files([f[0] for f in batch_to_merge], secondary_filepath)
                for f_path, _, _ in batch_to_merge:
                    if os.path.exists(f_path): os.remove(f_path)
                primary_files_batch = primary_files_batch[10:] # Удаляем объединенные файлы из списка
            else:
                tqdm.write(f"  [ЛОГ] Не удалось извлечь индекс из имени файла {os.path.basename(first_file_info[0])} для объединения.")

        elif len(primary_files_batch) >= 10:
             tqdm.write("В текущей партии есть .txt файлы, объединение пропущено.")


        fragment_processed = False
        voice_attempts = 0
        while voice_attempts < len(voices) and not fragment_processed:
            vibe_attempts = 0
            while vibe_attempts < len(vibe_choices) and not fragment_processed:
                vibe_prompt = format_vibe_prompt(selected_vibe_name, vibes_data)
                audio_content, content_type = send_request(fragment, selected_voice, vibe_prompt)

                if not audio_content:
                    tqdm.write(f"  [ЛОГ] Фрагмент {index}: API не вернул аудиоконтент.")
                else:
                    file_ext = ".mp3" if "mpeg" in content_type else ".wav"
                    temp_filepath = os.path.join(temp_dir, f"{index:05d}-tmp{file_ext}")
                    with open(temp_filepath, "wb") as f: f.write(audio_content)

                    temp_wav_path = os.path.join(temp_dir, f"{index:05d}-tmp.wav")
                    if file_ext == ".mp3":
                        try:
                            AudioSegment.from_mp3(temp_filepath).export(temp_wav_path, format="wav")
                            os.remove(temp_filepath)
                        except Exception as e:
                            tqdm.write(f"  [ЛОГ] Не удалось конвертировать временный MP3 файл {temp_filepath}: {e}")
                            if os.path.exists(temp_filepath): os.remove(temp_filepath)
                            continue
                    else:
                         if os.path.exists(temp_wav_path): os.remove(temp_wav_path) # Удаляем старый файл, если есть
                         os.rename(temp_filepath, temp_wav_path)


                    tqdm.write(f"  [ЛОГ] Фрагмент {index}: Получен файл. Запуск Whisper...")
                    is_valid, lang, text_from_audio = run_whisper_check(temp_wav_path)
                    if not is_valid:
                        tqdm.write(f"  [ЛОГ] Фрагмент {index}: ПРОВАЛ. Whisper определил язык: {lang}. Текст: '{text_from_audio[:50]}...'")
                        if os.path.exists(temp_wav_path): os.remove(temp_wav_path)
                    else:
                        tqdm.write(f"  [ЛОГ] Фрагмент {index}: УСПЕХ Whisper. Язык: {lang}. Запуск обработки тишины...")
                        edited_filepath = os.path.join(temp_dir, f"{index:05d}-edited.wav")
                        if edit_silence(temp_wav_path, edited_filepath):
                            file_size_kb = os.path.getsize(edited_filepath) / 1024
                            final_filepath = os.path.join(all_good_pages_dir, f"{index:05d}.wav")
                            # Проверяем, не существует ли уже файл с таким индексом в all_good_pages
                            if not os.path.exists(final_filepath) and not os.path.exists(os.path.join(all_good_pages_dir, f"{index:05d}.txt")):
                                shutil.move(edited_filepath, final_filepath)

                                if MIN_SIZE_KB <= file_size_kb <= MAX_SIZE_KB:
                                    primary_files_batch.append((final_filepath, selected_voice, selected_vibe_name))
                                    tqdm.write(f"  [ЛОГ] Фрагмент {index}: OK ({lang}, {file_size_kb:.1f} КБ). Сохранен.")
                                else:
                                    txt_filepath = os.path.join(all_good_pages_dir, f"{index:05d}.txt")
                                    with open(txt_filepath, 'w', encoding='utf-8') as f: f.write(fragment)
                                    # Удаляем подозрительный wav файл, оставляем только txt
                                    if os.path.exists(final_filepath): os.remove(final_filepath)
                                    tqdm.write(f"  [ЛОГ] Фрагмент {index}: ПОДОЗРИТЕЛЬНЫЙ ({lang}, {file_size_kb:.1f} КБ). Сохранен .txt")
                                fragment_processed = True
                            else:
                                tqdm.write(f"  [ЛОГ] Файл {index}.wav/.txt уже существует в all_good_pages. Пропускаем обработку.")
                                fragment_processed = True # Считаем обработанным, т.к. файл уже есть
                                if os.path.exists(edited_filepath): os.remove(edited_filepath) # Удаляем дубликат


                        if os.path.exists(temp_wav_path): os.remove(temp_wav_path)

                if not fragment_processed:
                    vibe_attempts += 1
                    current_vibe_index = vibe_choices.index(selected_vibe_name)
                    selected_vibe_name = vibe_choices[(current_vibe_index + 1) % len(vibe_choices)]

            if not fragment_processed:
                voice_attempts += 1
                current_voice_index = voices.index(selected_voice)
                selected_voice = voices[(current_voice_index + 1) % len(voices)]
                selected_vibe_name = original_vibe_name
                tqdm.write(f"--- Фрагмент {index}: ВСЕ характеры перепробованы. Меняем голос на '{selected_voice}' ---")

        if not fragment_processed:
            txt_filepath = os.path.join(all_good_pages_dir, f"{index:05d}.txt")
            # Проверяем, не существует ли уже файл с таким индексом в all_good_pages (как txt или wav)
            if not os.path.exists(txt_filepath) and not os.path.exists(os.path.join(all_good_pages_dir, f"{index:05d}.wav")):
                 with open(txt_filepath, 'w', encoding='utf-8') as f: f.write(fragment)
                 tqdm.write(f"--- Фрагмент {index}: НЕ УДАЛОСЬ озвучить всеми голосами. Сохранен как .txt ---")
            else:
                 tqdm.write(f"--- Фрагмент {index}: НЕ УДАЛОСЬ озвучить всеми голосами, но файл уже существует в all_good_pages. Пропускаем сохранение .txt ---")



    # !!! КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: ОБРАБОТКА ПОСЛЕДНЕГО БАТЧА !!!
    # Эта логика осталась, но теперь она обработает только те файлы, которые были созданы
    # в текущем сеансе и остались в primary_files_batch
    if primary_files_batch:
        # Проверяем, есть ли в оставшихся файлах в батче .txt файлы
        batch_has_txt = any(f[0].endswith('.txt') for f in primary_files_batch)
        # Проверяем, что все файлы в батче имеют индекс > highest_index,
        # чтобы не пытаться объединить уже объединенное (хотя условие цикла уже это гарантирует)
        batch_indices_valid = all(int(re.match(r'^(\d{5})', os.path.basename(f[0])).group(1)) > highest_index for f in primary_files_batch if re.match(r'^(\d{5})', os.path.basename(f[0])))

        if not batch_has_txt and batch_indices_valid: # Если нет .txt файлов и все индексы валидны для объединения
            first_file_info = primary_files_batch[0]
            match_index = re.match(r'^(\d{5})', os.path.basename(first_file_info[0]))
            if match_index:
                first_index = int(match_index.group(1))
                voice_info_for_merge = first_file_info[1] if first_file_info[1] != "EXISTING" else "MERGED_LAST" # Указываем, что это объединенный из существующих
                vibe_info_for_merge = first_file_info[2] if first_file_info[2] != "EXISTING" else "MERGED_LAST"
                secondary_filename = f"{first_index:05d} ({voice_info_for_merge}, {vibe_info_for_merge}).wav"
                secondary_filepath = os.path.join(all_good_pages_dir, secondary_filename)
                tqdm.write(f"Объединение последнего батча из {len(primary_files_batch)} файлов в {secondary_filename} (начинается с индекса {first_index})...")
                merge_audio_files([f[0] for f in primary_files_batch], secondary_filepath)
                for f_path, _, _ in primary_files_batch:
                    if os.path.exists(f_path): os.remove(f_path)
                primary_files_batch = [] # Очищаем список после объединения
            else:
                tqdm.write(f"  [ЛОГ] Не удалось извлечь индекс из имени файла {os.path.basename(first_file_info[0])} для объединения последнего батча.")
        else:
             tqdm.write("В последнем батче есть .txt файлы или индексы некорректны для объединения, объединение пропущено.")


    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    print("\n\n--- ПРОЦЕСС ОЗВУЧКИ ЗАВЕРШЕН! ---")

# --- 7. Запуск ---
if __name__ == "__main__":
    main()
