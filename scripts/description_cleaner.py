import pandas as pd
import re
import csv

def clean_skills_data(value):
    """Очистка данных в столбцах с навыками"""
    if pd.isna(value) or value == '[]' or value == '':
        return ''
    
    value = str(value)
    # Убираем квадратные скобки и кавычки
    value = value.replace('[', '').replace(']', '').replace("'", "").replace('"', '')
    # Убираем лишние пробелы
    value = re.sub(r'\s+', ' ', value).strip()
    return value

def clean_html_tags(text):
    """Очистка текста от HTML тегов"""
    if pd.isna(text):
        return ''
    
    text = str(text)
    # Удаляем HTML теги
    clean_text = re.sub(r'<[^>]+>', '', text)
    # Заменяем множественные пробелы на одинарные
    clean_text = re.sub(r'\s+', ' ', clean_text)
    # Убираем лишние пробелы в начале и конце
    return clean_text.strip()

def process_csv_file(file_path):
    """Основная функция обработки CSV файла"""
    try:
        df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
        
        print(f"Успешно прочитано строк: {len(df)}")
        
        # Список столбцов с навыками для обработки
        skills_columns = [
            'key_skills', 
            'key_skills_from_key_skills_field', 
            'hard_skills_from_description', 
            'soft_skills_from_description'
        ]
        
        # Обрабатываем столбцы с навыками
        for column in skills_columns:
            if column in df.columns:
                print(f"Обрабатываю столбец: {column}")
                df[column] = df[column].apply(clean_skills_data)
            else:
                print(f"Столбец {column} не найден в файле")
        
        # Обрабатываем столбец description
        if 'description' in df.columns:
            print("Обрабатываю столбец: description")
            df['description'] = df['description'].apply(clean_html_tags)
        else:
            print("Столбец description не найден в файле")
        
        # Сохраняем обработанный файл
        output_path = file_path.replace('.csv', '_cleaned.csv')
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"Обработка завершена! Файл сохранен как: {output_path}")
        print(f"Обработано строк: {len(df)}")
        
        # Показываем структуру данных
        print("\nСтруктура данных:")
        print(df.info())
        
        return df
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        # Пробуем альтернативный способ чтения
        return read_csv_alternative(file_path)

def read_csv_alternative(file_path):
    """Альтернативный способ чтения CSV с обработкой ошибок"""
    try:
        print("Пробую альтернативный способ чтения CSV...")
        
        # Читаем с указанием engine и обработкой ошибок
        df = pd.read_csv(file_path, engine='python', encoding='utf-8', on_bad_lines='warn')
        print(f"Альтернативное чтение: прочитано {len(df)} строк")
        return df
        
    except Exception as e:
        print(f"Альтернативное чтение также failed: {str(e)}")
        return read_csv_with_csv_module(file_path)

def read_csv_with_csv_module(file_path):
    """Чтение CSV с использованием стандартного модуля csv"""
    try:
        print("Использую стандартный модуль csv для чтения...")
        
        data = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Читаем заголовки
            print(f"Заголовки: {headers}")
            
            for i, row in enumerate(reader):
                if len(row) == len(headers):
                    data.append(row)
                else:
                    print(f"Пропускаю строку {i+2}: несоответствие количества полей")
        
        df = pd.DataFrame(data, columns=headers)
        print(f"Прочитано строк через csv модуль: {len(df)}")
        return df
        
    except Exception as e:
        print(f"Ошибка при чтении через csv модуль: {str(e)}")
        return None

# Основной код
if __name__ == "__main__":
    file_path = r"path"
    
    # Запускаем обработку
    processed_df = process_csv_file(file_path)
    
    if processed_df is not None:
        # Показываем пример обработанных данных
        print("\nПример обработанных данных:")
        
        # Проверяем наличие столбцов перед выводом
        if 'key_skills' in processed_df.columns:
            print("key_skills (первые 3 строки):")
            for i, val in enumerate(processed_df['key_skills'].head(3)):
                print(f"Строка {i+1}: {val}")
        
        if 'description' in processed_df.columns:
            print("\ndescription (первые 200 символов из первой строки):")
            desc = processed_df['description'].iloc[0] if len(processed_df) > 0 else ""
            print(f"{str(desc)[:200]}...")
    else:
        print("Не удалось прочитать файл. Проверьте путь и формат файла.")