import pandas as pd
import json
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import time
from functools import wraps
from datetime import datetime



def save_df_to_excel(df, output_path):
    # Сохранение в Excel
    df.to_excel(output_path, index=False)
    print(f"Файл успешно сохранён в: {output_path}")

def save_df_to_google_sheet(df: pd.DataFrame, sheet_name: str, worksheet_name: str, creds_json_path: str):
    """
    Сохраняет DataFrame во вкладку Google Sheets с обработкой больших ячеек.
    
    :param df: DataFrame для сохранения
    :param sheet_name: Название таблицы Google Sheets
    :param worksheet_name: Название вкладки (листа) для записи
    :param creds_json_path: Путь к .json файлу с Google Service Account credentials
    """
    # Авторизация через сервисный аккаунт
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, scope)
    client = gspread.authorize(creds)

    # Открываем Google Sheet
    spreadsheet = client.open(sheet_name)

    # Открываем или создаём лист
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="20")

    # Обрабатываем большие ячейки
    processed_df = truncate_large_cells(df)
    
    # Сохраняем DataFrame в лист
    set_with_dataframe(worksheet, processed_df)
    print(f"✅ DataFrame сохранён в Google Sheets: {sheet_name} → {worksheet_name}")

def truncate_large_cells(df: pd.DataFrame, max_cell_size: int = 25000) -> pd.DataFrame:
    """
    Обрезает большие ячейки до максимального размера, сохраняя начало текста.
    
    :param df: DataFrame для обработки
    :param max_cell_size: максимальный размер ячейки (меньше лимита Google Sheets)
    :return: обработанный DataFrame
    """
    df = df.copy()
    
    def truncate_string(text, max_size):
        if not isinstance(text, str):
            return text
        if len(text) <= max_size:
            return text
        return text[:max_size] + "... [текст обрезан]"
    
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(lambda x: truncate_string(x, max_cell_size))
    
    return df

def measure_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"Функция '{func.__name__}' выполнилась за {end - start:.4f} секунд.")
        return result
    return wrapper

def merge_df(dataframes):
    """
    Объединяет список DataFrame'ов, выравнивает столбцы, переводит даты и время в строки, сохраняет в .xlsx.
    
    :param dataframes: список pandas.DataFrame
    :param output_path: путь к выходному .xlsx файлу
    """
    # Объединение всех DataFrame с приведением к общему набору столбцов
    merged_df = pd.concat(dataframes, ignore_index=True, sort=False)

    # Преобразуем все datetime колонки (с таймзоной и без) в строки
    for col in merged_df.columns:
        if pd.api.types.is_datetime64_any_dtype(merged_df[col]):
            merged_df[col] = merged_df[col].astype(str)
    
    return merged_df

def extract_links_from_json(json_path):
    """
    Загружает JSON-файл и возвращает список ссылок на телеграм-каналы.

    :param json_path: путь к файлу JSON
    :return: список ссылок (строки)
    """
    with open(json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    links = [item.get("ссылка") for item in data if "ссылка" in item]
    return links

def datetime_handler(obj):
    """
    Обработчик для сериализации datetime объектов в JSON.
    """
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, dict):
        return {k: datetime_handler(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [datetime_handler(item) for item in obj]
    return obj
