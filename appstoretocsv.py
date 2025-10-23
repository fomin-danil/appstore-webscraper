# app.py
import streamlit as st
import requests
import pandas as pd
import random
import time
from dateutil import parser as dateparser

st.title("App Store Reviews Collector")

# Ввод параметров
APP_ID = st.text_input("Введите App Store ID (например 1234567890):")
MAX_REVIEWS = st.number_input("Максимум отзывов", min_value=10, max_value=1000, value=1000)
START_BTN = st.button("Собрать отзывы")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/118.0",
]

BASE_URL = "https://itunes.apple.com/ru/rss/customerreviews/id={}/json?page={}"

# Функция для безопасного запроса
def fetch_page(app_id, page):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    time.sleep(random.uniform(0.5, 1.5))
    try:
        resp = requests.get(BASE_URL.format(app_id, page), headers=headers, timeout=15)
        if resp.status_code != 200 or "captcha" in resp.text.lower() or "<html" in resp.text.lower():
            return None, True
        data = resp.json()
        return data, False
    except:
        return None, False

# Нормализация отзыва
def normalize_review(raw):
    rid = raw.get("id", {}).get("label")
    author = raw.get("author", {}).get("name", {}).get("label","")
    rating = raw.get("im:rating", {}).get("label","")
    title = raw.get("title", {}).get("label","")
    text = raw.get("content", {}).get("label","")
    version = raw.get("im:version", {}).get("label","")
    date_raw = raw.get("updated", {}).get("label","")
    date_iso = ""
    try:
        if date_raw:
            date_iso = dateparser.parse(date_raw).isoformat()
    except:
        date_iso = ""
    url = raw.get("id", {}).get("label","")
    return {
        "review_id": rid,
        "app_id": APP_ID,
        "app_name": "",
        "country": "ru",
        "user_name": author,
        "rating": rating,
        "title": title,
        "text": text,
        "version": version,
        "date": date_iso,
        "helpful_count": None,
        "raw_source_url": url
    }

# Основная функция сбора
def collect_reviews(app_id, max_reviews):
    collected = []
    page = 1
    blocked = False
    while len(collected) < max_reviews:
        data, block = fetch_page(app_id, page)
        if block:
            blocked = True
            break
        if not data:
            break
        entries = data.get("feed", {}).get("entry", [])
        if len(entries) <= 1:
            break
        for e in entries[1:]:  # первый элемент — мета
            rid = e.get("id", {}).get("label")
            if any(r["review_id"]==rid for r in collected):
                continue
            collected.append(normalize_review(e))
            if len(collected) >= max_reviews:
                break
        page += 1
    return collected, blocked

if START_BTN:
    if not APP_ID:
        st.warning("Введите App ID!")
    else:
        st.info("Сбор отзывов…")
        reviews, blocked = collect_reviews(APP_ID, MAX_REVIEWS)
        df = pd.DataFrame(reviews)
        df.to_csv(f"app_{APP_ID}_reviews.csv", index=False, encoding="utf-8-sig")
        st.success(f"Готово! Собрано {len(df)} отзывов. CSV сохранен как app_{APP_ID}_reviews.csv")
        if blocked:
            st.warning("Сбор остановлен из-за CAPTCHA/блокировки.")
        st.dataframe(df.head(10))
        st.download_button(
            label="Скачать CSV",
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"app_{APP_ID}_reviews.csv",
            mime="text/csv"
        )
