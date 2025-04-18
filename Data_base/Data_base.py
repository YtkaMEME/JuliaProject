from psycopg2 import connect
from psycopg2.extras import execute_values
import pandas as pd
import json
from help_defs import datetime_handler
from datetime import datetime, timedelta

class DataBase:
    def __init__(self, db_config: dict):
        self.db_config = db_config

    def connect_to_db(self):
        """
        Подключение к базе данных PostgreSQL.

        :return: Объект подключения к базе данных, курсор
        """
        conn = connect(**self.db_config)
        cursor = conn.cursor()

        return conn, cursor

    def drop_table_by_name(self, table_name: str):
        """
        Удаляет таблицу из базы данных PostgreSQL по её имени.

        :param table_name: Название таблицы для удаления
        """
        conn, cursor = self.connect_to_db()
        try:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
            conn.commit()
            print(f"✅ Таблица '{table_name}' успешно удалена.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Ошибка при удалении таблицы '{table_name}': {e}")
        finally:
            cursor.close()
            conn.close()

    async def save_social_media_posts_to_postgresql(self, df, table_name_maim: str):
        if df.empty:
            print(f"⚠️ DataFrame для {table_name_maim} пуст — пропуск сохранения.")
            return

        conn, cursor = self.connect_to_db()

        # Автоматическое создание таблицы
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name_maim} (
            type TEXT,
            channel_url TEXT,
            post_id TEXT,
            date TIMESTAMP,
            text TEXT,
            views BIGINT,
            forwards BIGINT,
            reactions BIGINT,
            comments TEXT,
            sentiment_score TEXT,
            table_name TEXT,
            count_comments BIGINT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Приведение типов и переименование (если нужно)
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["post_id"] = df["post_id"].astype(str)  # если могут быть длинные
        df["comments"] = df["comments"].fillna("")  # null → пустая строка

        # Подготовка к batch insert
        insert_query = f"""
        INSERT INTO {table_name_maim} 
        (type, channel_url, post_id, date, text, views, forwards, reactions, comments, sentiment_score, table_name,count_comments)
        VALUES %s
        """
        values = df[[
            "type", "channel_url", "post_id", "date", "text",
            "views", "forwards", "reactions", "comments", "sentiment_score", "table_name", "count_comments"
        ]].values.tolist()

        try:
            execute_values(cursor, insert_query, values)
            conn.commit()
            print(f"✅ Успешно сохранено {len(values)} записей в таблицу '{table_name_maim}'")
        except Exception as e:
            conn.rollback()
            print(f"❌ Ошибка при вставке в базу данных: {e}")
        finally:
            cursor.close()
            conn.close()