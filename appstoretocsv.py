# Streamlit App: Сбор отзывов App Store (RU)
# Полностью готовый к запуску в Colab/Streamlit

# Установка необходимых пакетов (раскомментируйте при запуске в Colab)
# !pip install streamlit requests beautifulsoup4 lxml pandas tqdm

import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import time
import random
import logging
from datetime import datetime

# ----------------------------
# Логирование
# ----------------------------
def setup_logger(app_id):
    log_filename = f"app_{app_id}_reviews_error.log"
    logger = logging.getLogger(app_id)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(handler)
    return logger, log_filename

# ----------------------------
# Функция для сохранения CSV
# ----------------------------
def save_csv(df, filename):
    df.to_csv(filename, index=False, encoding='utf-8-sig')

# ----------------------------
# Нормализация отдельного отзыва
# ----------------------------
def normalize_review(raw, app_id, app_name):
    review = {
        "review_id": raw.get("id") or raw.get("reviewId") or None,
        "app_id": app_id,
        "app_name": app_name,
        "country": "ru",
        "user_name": raw.get("userName") or raw.get("author", {}).get("name") or None,
        "rating": raw.get("rating") or raw.get("averageUserRating") or None,
        "title": raw.get("title") or raw.get("summary") or "",
        "text": raw.get("content") or raw.get("review") or "",
        "version": raw.get("version") or raw.get("appVersion") or None,
        "date": raw.get("date") or raw.get("updated") or None,
        "helpful_count": raw.get("voteCount") or 0,
        "raw_source_url": raw.get("link") or None
    }
    # Приведение даты к ISO 8601
    if review["date"]:
        try:
            review["date"] = pd.to_datetime(review["date"]).isoformat()
        except:
            review["date"] = None
    return review

# ----------------------------
# Получение отзывов через API/JSON
# ----------------------------
def fetch_reviews_api(app_id, limit=1000, logger=None):
    reviews = []
    page = 1
    per_page = 50  # Apple RSS/JSON обычно возвращает 50 за раз
    app_name = None

    while len(reviews) < limit:
        url = f"https://itunes.apple.com/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"
        headers = {"User-Agent": "Mozilla/5.0"}
        success = False
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    entries = data.get("feed", {}).get("entry", [])
                    if page == 1 and entries:
                        app_name = entries[0].get("im:name", {}).get("label")
                        entries = entries[1:]  # первый элемент — информация о приложении
                    if not entries:
                        return reviews
                    for e in entries:
                        reviews.append(normalize_review(e, app_id, app_name))
                    success = True
                    break
                else:
                    time.sleep(random.uniform(0.5, 1.5))
            except Exception as ex:
                if logger:
                    logger.error(f"API fetch attempt {attempt+1} failed: {ex}")
                time.sleep(random.uniform(0.5, 1.5))
        if not success:
            if logger:
                logger.error("API fetch failed, остановка сборa.")
            break
        page += 1
    return reviews[:limit]

# ----------------------------
# Получение отзывов через веб-скрейпинг
# ----------------------------
def fetch_reviews_scrape(app_id, limit=1000, logger=None):
    reviews = []
    page = 1
    app_name = None
    seen_ids = set()

    while len(reviews) < limit:
        url = f"https://apps.apple.com/ru/app/id{app_id}?see-all=reviews&page={page}"
        headers = {"User-Agent": "Mozilla/5.0"}
        success = False
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "lxml")
                    if page == 1:
                        title_tag = soup.find("h1", {"class": "app-header__title"})
                        app_name = title_tag.text.strip() if title_tag else None
                    review_blocks = soup.find_all("div", {"class": "we-customer-review"})
                    if not review_blocks:
                        return reviews
                    for block in review_blocks:
                        review = {}
                        review["reviewId"] = block.get("id")
                        review["userName"] = block.find("span", {"class": "we-truncate we-truncate--single-line"}).text.strip() if block.find("span", {"class": "we-truncate we-truncate--single-line"}) else None
                        review["title"] = block.find("h3").text.strip() if block.find("h3") else ""
                        review["content"] = block.find("blockquote").text.strip() if block.find("blockquote") else ""
                        review["rating"] = int(block.find("span", {"class": "we-star-rating"})["aria-label"].split()[0]) if block.find("span", {"class": "we-star-rating"}) else None
                        review["version"] = block.find("span", {"class": "we-customer-review__version"}).text.strip() if block.find("span", {"class": "we-customer-review__version"}) else None
                        review["date"] = block.find("time")["datetime"] if block.find("time") else None
                        review["voteCount"] = int(block.find("span", {"class": "we-customer-review__helpful-count"}).text.strip().split()[0]) if block.find("span", {"class": "we-customer-review__helpful-count"}) else 0
                        review["link"] = url
                        normalized = normalize_review(review, app_id, app_name)
                        # Дедупликация
                        unique_key = normalized["review_id"] or (normalized["user_name"], normalized["date"], normalized["text"][:120])
                        if unique_key in seen_ids:
                            continue
                        seen_ids.add(unique_key)
                        reviews.append(normalized)
                    success = True
                    break
                elif resp.status_code == 429:
                    if logger:
                        logger.error("Обнаружена блокировка/CAPTCHA, остановка.")
                    return reviews
                else:
                    time.sleep(random.uniform(0.5, 1.5))
            except Exception as ex:
                if logger:
                    logger.error(f"Scrape attempt {attempt+1} failed: {ex}")
                time.sleep(random.uniform(0.5, 1.5))
        if not success:
            if logger:
                logger.error("Scrape failed, остановка.")
            break
        page += 1
    return reviews[:limit]

# ----------------------------
# Streamlit интерфейс
# ----------------------------
st.set_page_config(page_title="App Store Reviews Collector", layout="wide")
st.title("Сбор отзывов App Store (RU)")

st.markdown("""
**Инструкция:**
1. Вставьте App ID приложения.
2. Нажмите кнопку «Собрать отзывы».
3. После завершения сборa можно скачать CSV.
""")

app_id_input = st.text_input("Введите App ID (например, 1234567890)")

if st.button("Собрать отзывы") and app_id_input:
    logger, log_file = setup_logger(app_id_input)
    st.info("Начинаем сбор отзывов...")
    progress_text = st.empty()
    progress_bar = st.progress(0)

    reviews = []

    # Сначала пробуем API
    try:
        api_reviews = fetch_reviews_api(app_id_input, limit=1000, logger=logger)
        reviews.extend(api_reviews)
        progress_text.text(f"Собрано через API: {len(reviews)} отзывов")
        progress_bar.progress(min(len(reviews)/1000, 1.0))
    except Exception as e:
        logger.error(f"Ошибка API: {e}")

    # Если мало отзывов, дополняем скрейпингом
    if len(reviews) < 1000:
        try:
            scrape_reviews = fetch_reviews_scrape(app_id_input, limit=1000 - len(reviews), logger=logger)
            reviews.extend(scrape_reviews)
            progress_text.text(f"Всего собрано: {len(reviews)} отзывов")
            progress_bar.progress(min(len(reviews)/1000, 1.0))
        except Exception as e:
            logger.error(f"Ошибка скрейпинга: {e}")

    if reviews:
        df = pd.DataFrame(reviews)
        csv_filename = f"app_{app_id_input}_reviews.csv"
        save_csv(df, csv_filename)
        st.success(f"Сбор завершен! Всего отзывов: {len(df)}")
        st.dataframe(df.head(10))
        st.download_button(
            label="Скачать CSV",
            data=df.to_csv(index=False, encoding='utf-8-sig'),
            file_name=csv_filename,
            mime="text/csv"
        )
    else:
        st.warning("Отзывы не были собраны. Проверьте App ID или наличие блокировок.")
        st.write(f"Смотрите лог: {log_file}")
