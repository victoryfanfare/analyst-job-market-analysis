import requests
import pandas as pd
import time
from datetime import datetime
import os
import random

class HHParser:
    def __init__(self):
        self.base_url = "https://api.hh.ru/vacancies"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        })
        self.request_count = 0
        self.last_request_time = time.time()
        
    def make_request(self, url, params=None):
        """Безопасный метод для выполнения запросов с задержками"""
        # Задержка для избежания 403 ошибки (минимум 0.5 сек между запросами)
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < 0.5:
            time.sleep(0.5 - time_since_last)
        
        try:
            response = self.session.get(url, params=params)
            self.request_count += 1
            self.last_request_time = time.time()
            
            # Если получаем 403, делаем большую паузу
            if response.status_code == 403:
                print("Получен 403, делаю паузу 10 секунд...")
                time.sleep(10)
                return self.make_request(url, params)  # Рекурсивно повторяем запрос
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Статус код: {e.response.status_code}")
                if e.response.status_code == 403:
                    print("403 ошибка, делаю паузу 30 секунд...")
                    time.sleep(30)
                    return self.make_request(url, params)
            return None
        
    def get_vacancies(self, search_text: str) -> list:
        """Получение вакансий по поисковому запросу"""
        all_vacancies = []
        page = 0
        
        while True:
            params = {
                "text": search_text,
                "page": page,
                "per_page": 100,
                "locale": "RU",
                "search_field": ["name", "company_name"]  # Поиск только в названии вакансии
            }
                
            response = self.make_request(self.base_url, params=params)
            if response is None:
                break
                
            try:
                data = response.json()
                
                if "items" not in data or not data["items"]:
                    print(f"Нет больше вакансий для '{search_text}'")
                    break
                    
                all_vacancies.extend(data["items"])
                print(f"Страница {page + 1}: собрано {len(data['items'])} вакансий для '{search_text}'")
                
                pages = data.get("pages", 0)
                if page >= pages - 1:
                    print(f"Все страницы обработаны для '{search_text}'")
                    break
                    
                page += 1
                # Случайная пауза между запросами для имитации человеческого поведения
                time.sleep(random.uniform(0.3, 1.0))
                
            except Exception as e:
                print(f"Ошибка при обработке данных для '{search_text}': {e}")
                break
                
        return all_vacancies
    
    def get_vacancy_details(self, vacancy_id: str) -> dict:
        """Получение детальной информации о вакансии"""
        url = f"{self.base_url}/{vacancy_id}"
        response = self.make_request(url)
        
        if response is None:
            return {}
            
        try:
            return response.json()
        except Exception as e:
            print(f"Ошибка при парсинге деталей вакансии {vacancy_id}: {e}")
            return {}
    
    def extract_skills_from_description(self, description: str) -> dict:
        """Извлечение hard и soft skills из описания"""
        if not description:
            return {"hard_skills_from_description": [], "soft_skills_from_description": []}
        
        hard_skills_keywords = [
            'sql', 'python', 'excel', 'tableau', 'power bi', 'bi', 'etl', 'olap',
            'базы данных', 'mysql', 'postgresql', 'oracle', 'ms sql', 'clickhouse',
            'airflow', 'dbt', 'superset', 'redash', 'metabase', 'hadoop', 'spark',
            'statistics', 'математика', 'анализ данных', 'machine learning', 'ml',
            'r language', 'r studio', 'pandas', 'numpy', 'scikit', 'tensorflow',
            'powerpoint', 'word', 'access', 'vba', 'git', 'docker', 'kubernetes',
            'api', 'json', 'xml', 'html', 'css', 'javascript', 'nosql', 'mongodb',
            'redis', 'kafka', 'aws', 'azure', 'google cloud', 'gcp'
        ]
        
        soft_skills_keywords = [
            'коммуникация', 'общение', 'переговоры', 'презентация', 'лидерство',
            'управление', 'менеджмент', 'команда', 'team', 'аналитическое мышление',
            'решение проблем', 'problem solving', 'критическое мышление',
            'тайм-менеджмент', 'time management', 'организация', 'планирование',
            'адаптивность', 'гибкость', 'креативность', 'творчество', 'стрессоустойчивость',
            'ответственность', 'инициативность', 'мотивация', 'обучаемость',
            'внимание к деталям', 'multitasking', 'многозадачность'
        ]
        
        text_lower = description.lower()
        
        hard_skills = []
        soft_skills = []
        
        for skill in hard_skills_keywords:
            if skill in text_lower:
                hard_skills.append(skill)
        
        for skill in soft_skills_keywords:
            if skill in text_lower:
                soft_skills.append(skill)
                
        return {
            "hard_skills_from_description": list(set(hard_skills)),
            "soft_skills_from_description": list(set(soft_skills))
        }
    
    def determine_grade(self, name: str, experience: dict, description: str) -> str:
        """Определение грейда"""
        if not experience:
            return "Unknown"
            
        text = (name + " " + (description or "")).lower()
        exp_id = experience.get("id", "").lower()
        
        if exp_id == "noexperience":
            return "Junior"
        elif exp_id == "between1and3":
            return "Junior+"
        elif exp_id == "between3and6":
            return "Middle"
        elif exp_id == "morethan6":
            return "Senior"
        
        junior_keywords = ['junior', 'начальный', 'стажер', 'trainee', 'без опыта']
        middle_keywords = ['middle', 'опытный', 'с опытом']
        senior_keywords = ['senior', 'ведущий', 'руковод', 'team lead', 'главный']
        
        for keyword in senior_keywords:
            if keyword in text:
                return "Senior"
                
        for keyword in middle_keywords:
            if keyword in text:
                return "Middle"
                
        for keyword in junior_keywords:
            if keyword in text:
                return "Junior"
                
        return "Unknown"
    
    def salary_to_bin(self, salary_data: dict) -> str:
        """Преобразование зарплаты в диапазон"""
        if not salary_data:
            return "Не указана"
            
        salary_from = salary_data.get("from")
        salary_to = salary_data.get("to")
        currency = salary_data.get("currency", "")
        
        if currency != "RUR":
            return "Не указана"
            
        if salary_from is not None and salary_to is not None:
            avg = (salary_from + salary_to) / 2
        elif salary_from is not None:
            avg = salary_from
        elif salary_to is not None:
            avg = salary_to
        else:
            return "Не указана"
            
        if avg < 50000:
            return "до 50k"
        elif avg < 100000:
            return "50k-100k"
        elif avg < 150000:
            return "100k-150k"
        elif avg < 200000:
            return "150k-200k"
        elif avg < 300000:
            return "200k-300k"
        else:
            return "300k+"
    
    def safe_get(self, data, key, default=None):
        """Безопасное получение значения из словаря"""
        if not data:
            return default
        return data.get(key, default)
    
    def parse_vacancies(self, search_queries: list) -> pd.DataFrame:
        """Основной метод парсинга вакансий"""
        all_data = []
        
        for query in search_queries:
            print(f"\n=== Собираю вакансии для: '{query}' ===")
            
            vacancies = self.get_vacancies(query)
            print(f"Всего найдено {len(vacancies)} вакансий для '{query}'")
            
            for i, vacancy in enumerate(vacancies):
                # Выводим информацию о каждой обрабатываемой вакансии
                vacancy_name = self.safe_get(vacancy, "name", "Неизвестно")
                employer = self.safe_get(vacancy.get("employer", {}), "name", "Неизвестно")
                print(f"Обрабатывается вакансия {i+1}/{len(vacancies)}: '{vacancy_name}' в '{employer}'")
                
                details = self.get_vacancy_details(vacancy["id"])
                if not details:
                    print(f"  ❌ Не удалось получить детали вакансии {vacancy['id']}")
                    continue
                
                skills = self.extract_skills_from_description(self.safe_get(details, "description"))
                
                grade = self.determine_grade(
                    self.safe_get(details, "name", ""),
                    self.safe_get(details, "experience", {}),
                    self.safe_get(details, "description", "")
                )
                
                salary_data = self.safe_get(details, "salary", {})
                salary_bin = self.salary_to_bin(salary_data)
                
                record = {
                    "id": self.safe_get(details, "id"),
                    "name": self.safe_get(details, "name"),
                    "published_at": self.safe_get(details, "published_at"),
                    "alternate_url": self.safe_get(details, "alternate_url"),
                    "type": self.safe_get(self.safe_get(details, "type", {}), "name"),
                    "employer": self.safe_get(self.safe_get(details, "employer", {}), "name"),
                    "department": self.safe_get(self.safe_get(details, "department", {}), "name"),
                    "area": self.safe_get(self.safe_get(details, "area", {}), "name"),
                    "experience": self.safe_get(self.safe_get(details, "experience", {}), "name"),
                    "key_skills": [skill.get("name") for skill in self.safe_get(details, "key_skills", [])],
                    "schedule": self.safe_get(self.safe_get(details, "schedule", {}), "name"),
                    "employment": self.safe_get(self.safe_get(details, "employment", {}), "name"),
                    "description": self.safe_get(details, "description"),
                    "salary_from": self.safe_get(salary_data, "from"),
                    "salary_to": self.safe_get(salary_data, "to"),
                    "salary_currency": self.safe_get(salary_data, "currency"),
                    "salary_bin": salary_bin,
                    "key_skills_from_key_skills_field": [skill.get("name") for skill in self.safe_get(details, "key_skills", [])],
                    "hard_skills_from_description": skills["hard_skills_from_description"],
                    "soft_skills_from_description": skills["soft_skills_from_description"],
                    "grade": grade,
                    "search_query": query
                }
                
                all_data.append(record)
                print(f"  ✅ Вакансия '{vacancy_name}' успешно обработана")
        
        return pd.DataFrame(all_data)

def main():
    project_path = r"path"
    os.chdir(project_path)
    
    data_folder = os.path.join(project_path, "data")
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    
    parser = HHParser()
    
    # 4 поисковых запроса для разных типов аналитиков
    search_queries = [
        "Аналитик данных", 
        "Системный аналитик", 
        "Бизнес-аналитик", 
        "Продуктовый аналитик"
    ]
    
    print("Начинаем сбор данных с HH.ru...")
    df = parser.parse_vacancies(search_queries)
    
    if df.empty:
        print("Не удалось собрать данные.")
        return
    
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Создаем отдельные датасеты для каждого типа аналитика
    data_analyst_df = df[df['search_query'] == 'Аналитик данных']
    system_analyst_df = df[df['search_query'] == 'Системный аналитик']
    business_analyst_df = df[df['search_query'] == 'Бизнес-аналитик']
    product_analyst_df = df[df['search_query'] == 'Продуктовый аналитик']
    
    # Сохраняем каждый датасет в отдельный CSV файл
    data_analyst_df.to_csv(os.path.join(data_folder, f"data_analyst_{current_date}.csv"), 
                          index=False, encoding='utf-8-sig')
    system_analyst_df.to_csv(os.path.join(data_folder, f"system_analyst_{current_date}.csv"), 
                           index=False, encoding='utf-8-sig')
    business_analyst_df.to_csv(os.path.join(data_folder, f"business_analyst_{current_date}.csv"), 
                             index=False, encoding='utf-8-sig')
    product_analyst_df.to_csv(os.path.join(data_folder, f"product_analyst_{current_date}.csv"), 
                            index=False, encoding='utf-8-sig')
    
    # Также сохраняем полный датасет
    df.to_csv(os.path.join(data_folder, f"all_vacancies_{current_date}.csv"), 
             index=False, encoding='utf-8-sig')
    
    print(f"\nДанные успешно сохранены в папке: {data_folder}")
    print(f"Всего собрано вакансий: {len(df)}")
    print(f"Аналитик данных: {len(data_analyst_df)}")
    print(f"Системный аналитик: {len(system_analyst_df)}")
    print(f"Бизнес-аналитик: {len(business_analyst_df)}")
    print(f"Продуктовый аналитик: {len(product_analyst_df)}")

if __name__ == "__main__":
    main()