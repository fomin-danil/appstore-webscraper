import streamlit as st
import requests
import csv
import time
import datetime
import pandas as pd

# ----------------------------
# Настройки приложения
# ----------------------------
st.title("Google Play Reviews Scraper")
st.write("Сбор отзывов по ID приложения из Google Play без API")

app_id = st.text_input("Введите applicationId (например: com.whatsapp):")
country = st.text_input("Страна (код, например ru, us, de):", "ru")
max_pages = st.number_input("Макс. страниц", min_value=1, max_value=100, value=10)
delay = st.number_input("Пауза между запросами (сек)", min_value=0.5, max_value=10.0, value=3.0)

start_btn = st.button("🚀 Начать сбор отзывов")

progress = st.empty()
log_area = st.empty()

def fetch_reviews(app_id, country, max_pages, delay):
    collected = []
    base_url = "https://play.google.com/store/getreviews"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0 Safari/537.36"
    }

    for page in range(max_pages):
        progress.progress((page + 1) / max_pages)

        data = {
            "reviewType": 0,
            "pageNum": page,
            "id": app_id,
            "reviewSortOrder": 0,
            "xhr": 1,
            "hl": country
        }

        resp = requests.post(base_url, data=data, headers=headers)

        # Блок / CAPTCHA → остановка
        if resp.status_code != 200 or "<HTML>" in resp.text.upper():
            log_area.error("❌ Обнаружен блок или CAPTCHA, остановка")
            break

        try:
            # Ответ содержит JSON в странном формате → вырезаем
            json_text = resp.text.split("\n")[-1]
            reviews = eval(json_text)[0][2]
        except:
            log_area.warning("⚠️ Не удалось разобрать JSON → остановка")
            break

        if not reviews:
            log_area.info("✅ Отзывов больше нет, остановка")
            break

        for item in reviews:
            collected.append({
                "user": item[1],
                "rating": item[2],
                "date": item[5],
                "comment": item[4],
                "app_id": app_id,
                "country": country
            })

        log_area.info(f"✅ Страница {page+1}: получено {len(reviews)} отзывов")

        time.sleep(delay)

    return collected


if start_btn:
    if not app_id:
        st.error("Введите приложение!")
    else:
        log_area.info("⏳ Начинаю сбор...")
        reviews = fetch_reviews(app_id, country, max_pages, delay)

        if reviews:
            df = pd.DataFrame(reviews)
            csv = df.to_csv(index=False).encode("utf-8")
            st.success(f"🎉 Сбор завершён! Всего отзывов: {len(df)}")

            st.download_button(
                "⬇️ Скачать CSV",
                csv,
                f"{app_id}_reviews.csv",
                "text/csv"
            )

            st.dataframe(df.head(20))
        else:
            st.error("❌ Не удалось собрать отзывы")
