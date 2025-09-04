# 📊 Анализ рынка труда для аналитиков на основе данных HH.ru

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange)](https://jupyter.org/)
[![Pandas](https://img.shields.io/badge/Pandas-Data%20Analysis-green)](https://pandas.pydata.org/)

Комплексное исследование вакансий для четырех ключевых аналитических специализаций с выявлением рыночных трендов, требований к навыкам и зарплатных ожиданий.

## 🎯 Цель проекта

Провести всестороннее исследование вакансий для аналитических специализаций:
- **Data Analyst** (Аналитик данных)
- **Business Analyst** (Бизнес-аналитик)
- **System Analyst** (Системный аналитик) 
- **Product Analyst** (Продуктовый аналитик)

### Ключевые задачи:
- Выявить различия в требованиях и условиях труда
- Определить ключевые навыки для разных уровней грейдов
- Проанализировать динамику спроса и рыночные тренды
- Сформулировать практические рекомендации

## 📁 Структура проекта
- analyst-job-market-analysis/
- ├── notebooks/ # Jupyter notebooks с анализом
- ├── scripts/ # Python скрипты
- ├── results/ # Результаты и визуализации


## 🛠️ Технологический стек

- **Python 3.8+**
- **Pandas** - обработка и анализ данных
- **NumPy** - математические операции
- **Matplotlib/Seaborn** - статичная визуализация
- **Plotly** - интерактивная визуализация
- **Jupyter Notebook** - интерактивная среда разработки

## 📊 Данные

### Источники данных:
- **HH.ru API** - вакансии аналитиков
- **4 специализации**: Data Analyst, Business Analyst, System Analyst, Product Analyst
- **3786 вакансий** с полной информацией (за август 2025 года)

### Основные поля данных:
- Информация о вакансиях и работодателях
- Зарплатные данные (вилки, валюта)
- Требуемые навыки (hard и soft skills)
- Грейды (Junior, Junior+, Middle, Senior)
- Условия работы и требования

## 🚀 Быстрый старт

```bash
git clone https://github.com/your-username/analyst-job-market-analysis.git
cd analyst-job-market-analysis
