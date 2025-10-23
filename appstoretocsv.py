# appstoretocsv.py

import streamlit as st
import requests, time, random, re, os, logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as dateutil_parser

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_reviews_scraper")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
]

def _get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/html, */*;q=0.1",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    }

def _random_sleep():
    time.sleep(random.uniform(0.5, 1.5))

def _is_captcha_or_block(resp):
    if resp is None:
        return False
    if resp.status_code in (403, 429):
        return True
    text = getattr(resp, "text", "").lower()
    triggers = ["captcha", "заблокирован", "access denied", "not allowed", "отказано", "verify you are human"]
    return any(t in text for t in triggers)

def _safe_get(url, params=None, headers=None, allow_redirects=True, timeout=15):
    headers = headers or _get_headers()
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout, allow_redirects=allow_redirects)
        return resp
    except requests.RequestException as e:
        logger.warning("Network error for %s: %s", url, e)
        return None

def fetch_reviews_api(app_id, limit=200):
    collected = []
    per_page = 200
    page = 1
    base_url = f"https://itunes.apple.com/rss/customerreviews/id={app_id}/json"

    while len(collected) < limit:
        params = {"cc":"ru", "l":"ru","sortBy":"mostRecent","limit":per_page,"page":page}
        resp = _safe_get(base_url, params=params)
        if resp is None or _is_captcha_or_block(resp):
            raise RuntimeError("CAPTCHA_OR_BLOCK")
        try:
            data = resp.json()
        except:
            break
        feed = data.get("feed", {})
        entries = feed.get("entry", [])
        new_count = 0
        for ent in entries:
            if "im:rating" not in ent:
                continue
            collected.append(ent)
            new_count += 1
            if len(collected) >= limit:
                break
        if new_count == 0:
            break
        page += 1
        _random_sleep()
    return collected[:limit]

def fetch_reviews_scrape(app_id, limit=200):
    collected = []
    base_url = f"https://apps.apple.com/ru/app/id{app_id}"
    resp = _safe_get(base_url)
    if resp is None or _is_captcha_or_block(resp):
        raise RuntimeError("CAPTCHA_OR_BLOCK")
    soup = BeautifulSoup(resp.text, "lxml")
    review_nodes = soup.find_all(lambda tag: (tag.name=="div" and tag.get("role")=="article") or (tag.name=="div" and tag.get("class") and any("we-customer-review" in c for c in tag.get("class"))))
    for node in review_nodes:
        if len(collected) >= limit:
            break
        try:
            raw = {}
            review_id = node.get("data-review-id") or (node.find(attrs={"data-review-id": True}) or {}).get("data-review-id")
            title = (node.find(["h3","h4"]) or {}).get_text(strip=True) if node.find(["h3","h4"]) else ""
            body_tag = node.find("blockquote") or node.find("p")
            body = body_tag.get_text(" ",strip=True) if body_tag else node.get_text(" ",strip=True)
            rating_tag = node.find(lambda tag: tag.name in ("span","i") and tag.get("aria-label") and re.search(r"\d", tag.get("aria-label")))
            rating = int(re.search(r"(\d)", rating_tag.get("aria-label")).group(1)) if rating_tag else None
            user_tag = node.find(lambda tag: tag.name in ("span","h4","div") and tag.get("class") and any("user" in c for c in " ".join(tag.get("class"))))
            user_name = user_tag.get_text(strip=True) if user_tag else ""
            date_tag = node.find("time")
            date = date_tag["datetime"] if date_tag and date_tag.has_attr("datetime") else None
            raw.update({"id":{"label":review_id},"title":{"label":title},"content":{"label":body},"author":{"name":{"label":user_name}},"rating":{"label":rating},"updated":{"label":date}})
            collected.append(raw)
        except:
            continue
    return collected[:limit]

def _parse_date_to_iso(date_raw):
    if not date_raw: return None
    try: return dateutil_parser.parse(str(date_raw), fuzzy=True).isoformat()
    except: return None

def normalize_review(raw, app_id=None):
    try:
        review_id = raw.get("id",{}).get("label")
        user_name = raw.get("author",{}).get("name",{}).get("label") or ""
        rating = int(raw.get("rating",{}).get("label") or 0)
        title = raw.get("title",{}).get("label") or ""
        text = raw.get("content",{}).get("label") or ""
        version = raw.get("im:version",{}).get("label") if raw.get("im:version") else ""
        date_iso = _parse_date_to_iso(raw.get("updated",{}).get("label"))
        return {
            "review_id": str(review_id),
            "app_id": str(app_id),
            "app_name": "",
            "country": "ru",
            "user_name": user_name,
            "rating": rating,
            "title": title,
            "text": text,
            "version": version,
            "date": date_iso,
            "helpful_count": None,
            "raw_source_url": review_id or ""
        }
    except:
        return None

def save_csv(df, filename):
    df.to_csv(filename,index=False,encoding="utf-8-sig")

def collect_reviews(app_id, max_reviews=1000):
    collected_raw=[]
    error_flag=False
    try:
        collected_raw.extend(fetch_reviews_api(app_id, max_reviews))
    except RuntimeError:
        error_flag=True
    except:
        pass
    if not error_flag and len(collected_raw)<max_reviews:
        try:
            need = max_reviews - len(collected_raw)
            collected_raw.extend(fetch_reviews_scrape(app_id, need))
        except RuntimeError:
            error_flag=True
    normalized=[]
    seen_ids=set()
    for raw in collected_raw:
        norm = normalize_review(raw, app_id)
        if not norm: continue
        rid = norm.get("review_id")
        if rid in seen_ids: continue
        seen_ids.add(rid)
        normalized.append(norm)
        if len(normalized)>=max_reviews: break
    df=pd.DataFrame(normalized,columns=["review_id","app_id","app_name","country","user_name","rating","title","text","version","date","helpful_count","raw_source_url"])
    csv_file=f"app_{app_id}_reviews.csv"
    log_file=f"app_{app_id}_reviews_error.log"
    save_csv(df,csv_file)
    if error_flag:
        with open(log_file,"w",encoding="utf-8") as f:
            f.write(f"[{datetime.utcnow().isoformat()}] CAPTCHA/block detected. Collected: {len(df)} reviews\n")
    return df,error_flag,csv_file,log_file

# --- Streamlit UI ---
st.title("Сбор отзывов из российского App Store")
st.write("Введите идентификатор приложения (APP_ID) из App Store и нажмите кнопку 'Собрать отзывы'.")

app_id_input = st.text_input("APP_ID", "")
max_reviews = st.slider("Максимум отзывов", min_value=100, max_value=1000, value=1000, step=50)

if st.button("Собрать отзывы") and app_id_input.strip():
    with st.spinner("Сбор отзывов..."):
        try:
            df, blocked, csv_file, log_file = collect_reviews(app_id_input.strip(), max_reviews=max_reviews)
            st.success(f"Сбор завершён. Всего уникальных отзывов: {len(df)}")
            st.dataframe(df.head(10))
            st.download_button("Скачать CSV", df.to_csv(index=False).encode("utf-8-sig"), file_name=csv_file)
            if blocked:
                st.warning(f"Сбор остановлен из-за CAPTCHA/блокировки. Сохранён лог: {log_file}")
        except Exception as e:
            st.error(f"Произошла ошибка: {e}")
