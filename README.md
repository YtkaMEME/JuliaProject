# Telegram Channel Analyzer

Проект для анализа данных из Telegram каналов и сохранения их в PostgreSQL и Google Sheets.

## Функциональность

- Сбор данных из Telegram каналов
- Анализ метрик каналов (подписчики, просмотры, репосты)
- Сохранение данных в PostgreSQL
- Резервное копирование в Google Sheets
- Два режима работы: полный парсинг и ежедневное обновление

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

### Режимы работы:

1. **Полный парсинг** - сбор данных за последние 100 дней:
```bash
python main.py --mode full
```
Или с указанием количества дней:
```bash
python main.py --mode full --days 50
```

2. **Ежедневное обновление** - запускается в режиме daemon и обновляет базу каждый день в 00:00. При этом удаляются записи за самый старый день и добавляются посты за текущий день:
```bash
python main.py --mode daily
```

## Структура проекта

```
telegram-channel-analyzer/
├── config/
│   ├── config.py
│   └── key_google.json
├── Telegram_channel/
│   └── *.json
├── VK_pars/
│   └── *.json
├── Data_base/
│   └── Data_base.py
├── AI/
│   └── sentiment_analysis.py
├── main.py
├── help_defs.py
├── requirements.txt
└── README.md
```

## Лицензия

MIT 