import sys
import os
import json
import asyncio
import logging
import argparse
from gettext import textdomain
from typing import List
import pandas as pd
from datetime import datetime, timedelta, time
import time as time_module
from functools import wraps
from markdown_it.common.html_re import comment

from AI.sentiment_analysis import predict_sentiment
from VK_pars.vk_pars import get_vk_group_posts_last_month
from Telegram_channel.tg_pars import parse_telegram_channel
from Data_base.Data_base import DataBase
from config.config import DB_CONFIG
from help_defs import save_df_to_excel

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def measure_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = await func(*args, **kwargs)
        end_time = datetime.now()
        execution_time = end_time - start_time
        logger.info(f"Время выполнения {func.__name__}: {execution_time}")
        return result
    return wrapper

def extract_links_from_json(json_path: str) -> List[str]:
    """Извлекает ссылки из JSON файла"""
    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return [item.get('ссылка', '') for item in data if item.get('ссылка')]
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {json_path}: {str(e)}")
        return []

async def concat_vk_tg_dfs(df_telegram: pd.DataFrame, df_vk: pd.DataFrame):
    column_map = {
        "type": {"vk": "type", "tg": "type"},
        "channel_url": {"vk": "group_link", "tg": "channel_url"},
        "post_id": {"vk": "post_id", "tg": "post_id"},
        "date": {"vk": "date", "tg": "date"},
        "text": {"vk": "text", "tg": "text"},
        "views": {"vk": "views", "tg": "views"},
        "forwards": {"vk": "reposts", "tg": "forwards"},
        "reactions": {"vk": "likes", "tg": "reactions"},
        "comments": {"vk": "all_comments", "tg": "all_comments"},
        "sentiment_score": {"vk": "sentiment_score", "tg": "sentiment_score"},
        "table_name": {"vk": "table_name", "tg": "table_name"},
        "count_comments": {"vk": "count_comments", "tg": "count_comments"},
    }

    def normalize_df(df, source_type):
        # 1. Переименование колонок
        rename_dict = {
            v[source_type]: k
            for k, v in column_map.items()
            if v.get(source_type) in df.columns
        }
        df = df.rename(columns=rename_dict)

        # 2. Удалим дубликаты колонок (если есть)
        df = df.loc[:, ~df.columns.duplicated()]

        # 3. Добавим недостающие колонки
        for unified_col in column_map.keys():
            if unified_col not in df.columns:
                df[unified_col] = None

        # 4. Только нужные колонки в нужном порядке
        df = df[[col for col in column_map.keys()]]

        # 5. Сбросим индекс
        return df.reset_index(drop=True)

    df_telegram_norm = normalize_df(df_telegram, "tg") if df_telegram is not None else pd.DataFrame()
    df_vk_norm = normalize_df(df_vk, "vk") if df_vk is not None else pd.DataFrame()
    combined_df = pd.concat([df_telegram_norm, df_vk_norm], ignore_index=True)
    return combined_df

async def process_social_media_data(links: List[str], token: str, table_name: str, source: str, days_back=100):
    """Обрабатывает данные из социальных сетей"""
    try:
        all_data = []
        
        for link in links:
            try:
                if source.lower() == "vk":
                    df = get_vk_group_posts_last_month(link, token, table_name, days_back)
                else:  # telegram
                    df = await parse_telegram_channel(link, table_name, days_back)

                if df is not None and not df.empty:
                    df['sentiment_score'] = df['all_comments'].apply(predict_sentiment)
                    all_data.append(df)
                    logger.info(f"Успешно обработаны данные из {link}")
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке {link}: {str(e)}")
                continue
        
        if all_data:
            final_df = pd.concat(all_data)
            return final_df
            
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {str(e)}")

@measure_time
async def process_data(days_back=100):
    try:
        # Telegram
        telegram_json_paths = [
            'Telegram_channel/channels/Gaming_and_eSports.json',
            'Telegram_channel/channels/Self-development_and_career.json',
            'Telegram_channel/channels/Personal_Finance_and_Investments.json',
            'Telegram_channel/channels/Social_networks_and_trends.json',
            'Telegram_channel/channels/Psychology_and_Mental_Health.json',
            'Telegram_channel/channels/Politics_and_civic_position.json',
            'Telegram_channel/channels/Education_and_Science.json',
            'Telegram_channel/channels/Lifestyle_and_fashion.json',
            'Telegram_channel/channels/Cooking_and_Healthy_Lifestyle.json',
            'Telegram_channel/channels/Technologies_and_neural_networks.json'
        ]
        
        # VK
        vk_json_paths = [
            'VK_pars/channels/Gaming_and_eSports.json',
            'VK_pars/channels/Self-development_and_career.json',
            'VK_pars/channels/Personal_Finance_and_Investments.json',
            'VK_pars/channels/Social_networks_and_trends.json',
            'VK_pars/channels/Psychology_and_Mental_Health.json',
            'VK_pars/channels/Politics_and_civic_position.json',
            'VK_pars/channels/Education_and_Science.json',
            'VK_pars/channels/Lifestyle_and_fashion.json',
            'VK_pars/channels/Cooking_and_Healthy_Lifestyle.json',
            'VK_pars/channels/Technologies_and_neural_networks.json'
        ]
        
        token = "a1556495a1556495a155649544a24da9fdaa155a1556495c72d436ceb13045566cdd7a9"
        
        for index in range(len(telegram_json_paths)):
            try:
                json_path_telegram = telegram_json_paths[index]
                links = extract_links_from_json(json_path_telegram)
                if not links:
                    logger.warning(f"Не найдены ссылки в файле {json_path_telegram}")
                    continue

                table_name = json_path_telegram.split('/')[2].split('.')[0].lower().replace('-', '_')
                dfs_telegram = await process_social_media_data(links, None, table_name, 'telegram', days_back)

                json_path_vk = vk_json_paths[index]
                links = extract_links_from_json(json_path_vk)
                dfs_vk = await process_social_media_data(links, token, table_name, 'vk', days_back)
                final_df = await concat_vk_tg_dfs(dfs_telegram, dfs_vk)
                db = DataBase(DB_CONFIG)
                await db.save_social_media_posts_to_postgresql(final_df, table_name)
                logger.info(f"Данные успешно сохранены в таблицу {table_name}")

            except Exception as e:
                logger.error(f"Ошибка при обработке файла {json_path_telegram}: {str(e)}")
                continue
    except Exception as e:
        logger.error(f"Критическая ошибка в main: {str(e)}")

async def daily_update():
    """Обновляет данные ежедневно, удаляя самые старые записи и добавляя новые"""
    db = DataBase(DB_CONFIG)
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    # Список таблиц для обновления
    tables = [
        'gaming_and_esports',
        'self_development_and_career',
        'personal_finance_and_investments',
        'social_networks_and_trends',
        'psychology_and_mental_health',
        'politics_and_civic_position',
        'education_and_science',
        'lifestyle_and_fashion',
        'cooking_and_healthy_lifestyle',
        'technologies_and_neural_networks'
    ]
    
    for table in tables:
        # Удаляем самые старые записи (посты самого старого дня)
        await db.delete_oldest_day_posts(table)
        logger.info(f"Удалены самые старые посты из таблицы {table}")
    
    # Запускаем парсинг данных за вчерашний день
    await process_data(days_back=1)
    logger.info(f"Добавлены новые посты за {yesterday}")

async def wait_until_midnight():
    """Ожидает до полуночи следующего дня"""
    now = datetime.now()
    midnight = datetime.combine(now.date() + timedelta(days=1), time(0, 0))
    seconds_until_midnight = (midnight - now).total_seconds()
    
    logger.info(f"Ожидание до полуночи: {seconds_until_midnight} секунд")
    await asyncio.sleep(seconds_until_midnight)

async def scheduled_updates():
    """Запускает ежедневные обновления в полночь"""
    while True:
        await wait_until_midnight()
        logger.info("Запуск ежедневного обновления данных")
        await daily_update()

@measure_time
async def main():
    # Настройка аргументов командной строки
    parser = argparse.ArgumentParser(description='Система парсинга данных из социальных сетей')
    parser.add_argument('--mode', choices=['full', 'daily'], default='full',
                      help='Режим работы: full - парсинг за 100 дней, daily - ежедневное обновление')
    parser.add_argument('--days', type=int, default=3,
                      help='Количество дней для парсинга (только для режима full)')
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'full':
            logger.info(f"Запуск полного парсинга за последние {args.days} дней")
            await process_data(days_back=args.days)
        else:  # daily mode
            logger.info("Запуск в режиме ежедневного обновления")
            # Сначала запустим обновление сразу
            await daily_update()
            # Затем настроим ежедневное обновление в полночь
            await scheduled_updates()
    except Exception as e:
        logger.error(f"Критическая ошибка в main: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())

