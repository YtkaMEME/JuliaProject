import vk_api
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse
import pandas as pd
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def replace_mentions_with_links(text):
    """Заменяет [id123|Имя] и [club123|Группа] на гиперссылки"""
    try:
        def replacer(match):
            raw = match.group(1)
            label = match.group(2)
            if raw.startswith('id'):
                return f'https://vk.com/{raw}'
            elif raw.startswith('club') or raw.startswith('public'):
                return f'https://vk.com/{raw}'
            return label  # fallback

        return re.sub(r'\[([^\|\]]+)\|([^\]]+)\]', replacer, text)
    except Exception as e:
        logger.error(f"Ошибка при замене упоминаний: {str(e)}")
        return text

def extract_group_id_from_url(group_url):
    """Получает короткое имя группы из ссылки"""
    try:
        parsed = urlparse(group_url)
        path = parsed.path.strip('/')
        if path.startswith('club') or path.startswith('public'):
            return '-' + path.lstrip('clubpublic')  # минус — признак сообщества
        return path
    except Exception as e:
        logger.error(f"Ошибка при извлечении ID группы из URL {group_url}: {str(e)}")
        return None

def get_group_id(api, screen_name):
    """Получает ID группы по короткому имени"""
    try:
        response = api.utils.resolveScreenName(screen_name=screen_name)
        if not response:
            logger.error(f"Пустой ответ от API для {screen_name}")
            return None
            
        if isinstance(response, dict):
            if response.get('type') == 'group':
                return -response.get('object_id')  # группы идут с минусом
            elif response.get('type') == 'page':
                return response.get('object_id')
            else:
                logger.error(f"Неизвестный тип объекта: {response.get('type')} для {screen_name}")
                return None
        else:
            logger.error(f"Неверный формат ответа от API для {screen_name}: {response}")
            return None
    except Exception as e:
        logger.error(f"Ошибка при получении ID группы {screen_name}: {str(e)}")
        return None

def get_post_comments(api, owner_id, post_id, max_comments=100):
    """Возвращает список всех комментариев к посту"""
    try:
        comments = []
        offset = 0
        while True:
            try:
                response = api.wall.getComments(
                    owner_id=owner_id,
                    post_id=post_id,
                    count=100,
                    offset=offset,
                    extended=0,
                )
                if not response or 'items' not in response:
                    break
                    
                items = response['items']
                if not items:
                    break
                    
                comments += [c.get('text', '') for c in items if c.get('text')]
                offset += 100
                if len(comments) >= max_comments:
                    break
                    
                time.sleep(0.34)  # Задержка для избежания rate limit
            except Exception as e:
                logger.error(f"Ошибка при получении комментариев (offset={offset}): {str(e)}")
                break
                
        return "\n\n---\n\n".join(comments)
    except Exception as e:
        logger.error(f"Ошибка при получении комментариев к посту {post_id}: {str(e)}")
        return ""

def extract_attachment_links(attachments):
    try:
        links = []
        if not attachments:
            return ""
            
        for att in attachments:
            try:
                if not isinstance(att, dict) or 'type' not in att:
                    continue
                    
                if att['type'] == 'photo':
                    if 'photo' not in att or 'sizes' not in att['photo']:
                        continue
                    sizes = att['photo']['sizes']
                    max_photo = max(sizes, key=lambda x: x.get('width', 0) * x.get('height', 0))
                    links.append(max_photo.get('url', ''))
                elif att['type'] == 'video':
                    if 'video' not in att:
                        continue
                    owner_id = att['video'].get('owner_id')
                    video_id = att['video'].get('id')
                    if owner_id and video_id:
                        links.append(f'https://vk.com/video{owner_id}_{video_id}')
                elif att['type'] == 'doc':
                    if 'doc' in att and 'url' in att['doc']:
                        links.append(att['doc']['url'])
                elif att['type'] == 'link':
                    if 'link' in att and 'url' in att['link']:
                        links.append(att['link']['url'])
                elif att['type'] == 'audio':
                    artist = att.get('audio', {}).get('artist', '')
                    title = att.get('audio', {}).get('title', '')
                    if artist or title:
                        links.append(f'{artist} - {title}')
                else:
                    links.append(f"[{att['type']}]")
            except Exception as e:
                logger.error(f"Ошибка при обработке вложения: {str(e)}")
                continue
                
        return '; '.join(filter(None, links))
    except Exception as e:
        logger.error(f"Ошибка при извлечении ссылок из вложений: {str(e)}")
        return ""


def fetch_comments_wrapper(api, owner_id, post_id):
    """Обёртка для многопоточной загрузки комментариев"""
    try:
        comments = get_post_comments(api, owner_id, post_id)
        return post_id, comments
    except Exception as e:
        logger.error(f"Ошибка при загрузке комментариев для поста {post_id}: {e}")
        return post_id, ""

def get_comments_batch(api, owner_id, post_ids):
    """Получает комментарии к списку постов через execute"""
    comments_map = {}
    batch_size = 25  # максимум для execute

    for i in range(0, len(post_ids), batch_size):
        batch = post_ids[i:i + batch_size]

        # Генерируем VKScript
        code = "return [\n"
        for post_id in batch:
            code += f"API.wall.getComments({{'owner_id': {owner_id}, 'post_id': {post_id}, 'count': 100}}),\n"
        code += "];"
        try:
            result = api.execute(code=code)
            for i, post_id in enumerate(batch):
                comments = result[i]['items'] if result[i] and 'items' in result[i] else []
                texts = [c['text'] for c in comments if c.get('text')]
                count_comments = len(texts)
                comments_map[post_id] = {"comments_text": "\n\n---\n\n".join(texts), "count_comments": count_comments}
        except Exception as e:
            logger.error(f"Ошибка при batch-запросе комментариев для постов {batch}: {e}")
            # При ошибке лучше заполнять пустыми значениями, чтобы не терять остальные посты
            for post_id in batch:
                comments_map[post_id] = ""

    return comments_map

def posts_to_dataframe(posts, api=None, owner_id=None, group_link=None, table_name="name"):
    try:
        if not posts:
            logger.warning("Нет постов для обработки")
            return pd.DataFrame()

        post_ids = [p.get('id') for p in posts if isinstance(p, dict) and p.get('id')]

        # Получаем комментарии пачками через execute
        comments_map = {}
        if api and owner_id:
            comments_map = get_comments_batch(api, owner_id, post_ids)

        data = []
        for post in posts:
            try:
                if not isinstance(post, dict):
                    continue

                post_id = post.get('id')


                dt_str = datetime.utcfromtimestamp(post.get('date', 0)).strftime('%Y-%m-%dT%H:%M:%S')
                post_url = f'{group_link}?w=wall{owner_id}_{post_id}'

                # Обработка счётчиков
                likes = post.get('likes', {})
                comments = post.get('comments', {})
                reposts = post.get('reposts', {})
                views = post.get('views', {})

                if isinstance(likes, int): likes = {'count': likes}
                # if isinstance(comments, int): comments = {'count': comments}
                if isinstance(reposts, int): reposts = {'count': reposts}
                if isinstance(views, int): views = {'count': views}

                data.append({
                    'post_id': post_id,
                    'date': dt_str,
                    'text': replace_mentions_with_links(post.get('text', '')),
                    'likes': likes.get('count', 0),
                    'reposts': reposts.get('count', 0),
                    'views': views.get('count', 0),
                    'is_pinned': post.get('is_pinned', 0),
                    'post_type': post.get('post_type', ''),
                    'signer_id': post.get('signer_id'),
                    'group_link': group_link,
                    'post_link': post_url,
                    "all_comments": comments_map[post_id]["comments_text"],
                    "type": "vk",
                    "table_name": table_name,
                    "count_comments": comments_map[post_id]["count_comments"],
                })

            except Exception as e:
                logger.error(f"Ошибка при обработке поста {post.get('id', 'unknown')}: {str(e)}")
                continue

        return pd.DataFrame(data)

    except Exception as e:
        logger.error(f"Ошибка при создании DataFrame: {str(e)}")
        return pd.DataFrame()

def get_vk_group_posts_last_month(group_url, token, table_name, days_back=100):
    """Получает все посты из группы за указанное количество дней, обходя лимит в 100 постов"""
    try:
        vk_session = vk_api.VkApi(token=token)
        api = vk_session.get_api()

        screen_name = extract_group_id_from_url(group_url)
        if not screen_name:
            logger.error(f"Не удалось извлечь screen_name из URL: {group_url}")
            return pd.DataFrame()

        group_id = get_group_id(api, screen_name)
        if not group_id:
            logger.error(f"Не удалось получить ID группы для {screen_name}")
            return pd.DataFrame()

        # Рассчитываем дату для ограничения выборки
        cutoff_date = datetime.now() - timedelta(days=days_back)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        all_posts = []
        offset = 0
        batch_size = 100
        while True:
            try:
                posts_data = api.wall.get(owner_id=group_id, count=batch_size, offset=offset)
                if not posts_data or 'items' not in posts_data:
                    logger.error(f"Неверный формат ответа от API для группы {group_id}")
                    break
                items = posts_data['items']
                if not items:
                    break

                # Проверяем, достигли ли мы указанной даты
                oldest_post_date = items[-1].get('date', 0)
                if oldest_post_date < cutoff_timestamp:
                    # Добавляем только посты после cutoff_date
                    for item in items:
                        if item.get('date', 0) >= cutoff_timestamp:
                            post = {
                                'id': item.get('id'),
                                'date': item.get('date'),
                                'text': item.get('text', ''),
                                'likes': item.get('likes', {}).get('count', 0),
                                'reposts': item.get('reposts', {}).get('count', 0),
                                'comments': item.get('comments', {}).get('count', 0),
                                'views': item.get('views', {}).get('count', 0),
                                'attachments': item.get('attachments', []),
                                'is_pinned': item.get('is_pinned', 0),
                                'post_type': item.get('post_type', ''),
                                'signer_id': item.get('signer_id', None),
                                "table_name": table_name
                            }
                            all_posts.append(post)
                    break
                else:
                    # Добавляем все посты из пакета
                    for item in items:
                        post = {
                            'id': item.get('id'),
                            'date': item.get('date'),
                            'text': item.get('text', ''),
                            'likes': item.get('likes', {}).get('count', 0),
                            'reposts': item.get('reposts', {}).get('count', 0),
                            'comments': item.get('comments', {}).get('count', 0),
                            'views': item.get('views', {}).get('count', 0),
                            'attachments': item.get('attachments', []),
                            'is_pinned': item.get('is_pinned', 0),
                            'post_type': item.get('post_type', ''),
                            'signer_id': item.get('signer_id', None),
                            "table_name": table_name
                        }
                        all_posts.append(post)

                offset += batch_size
                time.sleep(0.34)  # Задержка чтобы избежать rate limiting

            except Exception as e:
                logger.error(f"Ошибка при получении постов (offset={offset}): {str(e)}")
                break

        all_posts_df = posts_to_dataframe(all_posts, api, group_id, group_url, table_name)
        return all_posts_df
    except Exception as e:
        logger.error(f"Критическая ошибка при обработке группы {group_url}: {str(e)}")
        return pd.DataFrame()
