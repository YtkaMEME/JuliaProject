# Telegram Channel Analyzer

Проект для анализа данных из Telegram каналов и сохранения их в PostgreSQL и Google Sheets.

## Функциональность

- Сбор данных из Telegram каналов
- Анализ метрик каналов (подписчики, просмотры, репосты)
- Сохранение данных в PostgreSQL
- Резервное копирование в Google Sheets

## Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/your-username/telegram-channel-analyzer.git
cd telegram-channel-analyzer
```

2. Создайте виртуальное окружение и активируйте его:
```bash
python -m venv venv
source venv/bin/activate  # для Linux/Mac
# или
venv\Scripts\activate  # для Windows
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

## Конфигурация

1. Создайте файл `.env` в корневой директории проекта:
```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_PORT=5432
```

2. Настройте файл конфигурации в `config/config.py`

## Использование

Запустите основной скрипт:
```bash
python main_bot.py
```

## Структура проекта

```
telegram-channel-analyzer/
├── config/
│   ├── config.py
│   └── key_google.json
├── Telegram_channel/
│   └── *.json
├── main_bot.py
├── requirements.txt
└── README.md
```

## Лицензия

MIT 