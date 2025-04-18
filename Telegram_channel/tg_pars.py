from telethon.sync import TelegramClient
from telethon.tl.types import Message
from telethon.errors import FloodWaitError
from datetime import datetime, timedelta, timezone
from telethon.sessions import MemorySession
import pandas as pd
import asyncio
import logging
import sqlite3

from config.config import API_ID, API_HASH, SESSION_PATH

logging.basicConfig(level=logging.INFO)


async def parse_telegram_channel(channel_url, table_name, days_back=30, use_memory_session=False):
    session = MemorySession() if use_memory_session else SESSION_PATH
    client = TelegramClient(session, API_ID, API_HASH)

    try:
        await client.start()
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
        posts_data = []
        channel_username = channel_url.rstrip("/").split("/")[-1]

        try:
            entity = await client.get_entity(channel_username)
        except ValueError:
            logging.error(f"❌ Канал не найден или приватный: {channel_url}")
            return pd.DataFrame()

        async for message in client.iter_messages(entity):
            if message.date < cutoff_date:
                break
            if not isinstance(message, Message) or not message.message:
                continue

            comments_text = ''
            count_comments = 0
            try:
                if message.replies and message.replies.replies:
                    comments = []
                    async for comment in client.iter_messages(entity, reply_to=message.id):
                        if comment.message:
                            comments.append(comment.message.strip())

                    count_comments = len(comments)
                    comments_text = '----\n'.join(comments)
                    await asyncio.sleep(0.5)  # пауза между запросами комментариев
            except FloodWaitError as e:
                logging.warning(f"⚠️ Flood wait {e.seconds}s on comments. Пропускаем комменты к посту {message.id}")
            except Exception as e:
                logging.error(f"Ошибка при загрузке комментариев: {e}")

            post_info = {
                'post_id': message.id,
                'date': message.date,
                'text': message.message,
                'views': message.views,
                'forwards': message.forwards,
                'reactions': sum(r.count for r in message.reactions.results) if message.reactions else 0,
                'channel_url': channel_url,
                'all_comments': comments_text,
                'type': 'tg',
                "table_name": table_name,
                "count_comments": count_comments,
            }

            posts_data.append(post_info)
            break

        return pd.DataFrame(posts_data)

    except sqlite3.OperationalError as db_err:
        logging.error(f"💥 SQLite ошибка: {db_err}")
        return pd.DataFrame()

    except Exception as e:
        logging.error(f"🔥 Ошибка при обработке канала {channel_url}: {e}")
        return pd.DataFrame()

    finally:
        await client.disconnect()
