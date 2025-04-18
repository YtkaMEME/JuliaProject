#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройки Telegram API для клиента
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
ADMIN_USERNAME = "miroslav_muratov"

# Путь к сессии Telegram
SESSION_PATH = "mira.session"

# Конфигурация PostgreSQL
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432')
}