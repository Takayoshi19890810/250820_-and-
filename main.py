# -*- coding: utf-8 -*-
"""
まとめシートのみ出力 / 今日の YYMMDD に、昨日15:00〜今日14:59 の記事を集約
- Google/MSN は参照コードのセレクタ・ロジックを優先採用（安定）
- Yahoo は /articles/ or /pickup/ の記事のみ対象、記事ページから投稿日を補完
- 並び順: MSN → Google → Yahoo（各ソース内は投稿日降順）
- A列=ソース（MSN/Google/Yahoo）
"""

import os
import re
import json
import time
import requests
from datetime import datetime, timedelta, time as dtime
from email.utils import parsedate_to_datetime

import gspread
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ====== 設定 ======
KEYWORD = os.getenv("NEWS_KEYWORD", "日産")
SPREADSHEET_ID = os.getenv(
    "SPREADSHEET_ID",
    "1Vs4Cx8QPN4H2NOgtwaviOCe8zBTpUNDgJjqkHr51IZE"  # 正しい出力先
)

# ====== 共通ユーティリティ ======
def jst_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=9)

def fmt(dt: datetime) -> str:
    return dt.strftime("%Y/%m/%d %H:%M")

def try_parse_jst(dt_str: str):
    if not dt_str or dt_str == "取得不可":
        return None
    patterns = [
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]
    for p in patterns:
        try:
            dt = datetime.strptime(dt_str, p)
            if p.endswith("Z"):
                dt = dt + timedelta(hours=9)
            elif "%z" in p:
                dt = dt.astimezone(tz=None)
                dt = dt + timedelta(hours=9)
            return dt
        except Exception:
            pass
    return None

def parse_relative_time(label: str, base: datetime) -> str:
    s = (label or "").strip().lower()
    try:
        m = re.search(r"(\d+)\s*分前|(\d+)\s*minute", s)
        if m: return fmt(base - timedelta(minutes=int(m.group(1) or m.group(2) or 0)))
        h = re.search(r"(\d+)\s*時間前|(\d+)\s*hour", s)
        if h: return fmt(base - timedelta(hours=int(h.group(1) or h.group(2) or 0)))
        d = re.search(r"(\d+)\s*日前|(\d+)\s*day", s)
        if d: return fmt(base - timedelta(days=int(d.group(1) or d.group(2) or 0)))
        if re.match(r"\d{1,2}月\d{1,2}日$", s):
            dt = datetime.strptime(f"{base.year}年{s}", "%Y年%m月%d日")
            return fmt(dt)
        if re.match(r"\d{4}/\d{1,2}/\d{1,2}$", s):
            dt = datetime.strptime(s, "%Y/%m/%d")
            return fmt(dt)
        if re.match(r"\d{1,2}:\d{2}$", s):
            t = datetime.strptime(s, "%H:%M").time()
            dt = datetime.combine(base.date(), t)
            if dt > base:
                dt -= timedelta(days=1)
            return fmt(dt)
    except Exception:
        pass
    return "取得不可"

def get_last_modified_datetime(url: str) -> str:
    try:
        r = requests.head(url, timeout=6, allow_redirects=True)
        if "Last-Modified" in r.headers:
            dt = parsedate_to_datetime(r.headers["Last-Modified"])
            if dt.tzinfo:
                dt = dt.astimezone(tz=None)
                dt = dt + timedelta(hours=9)
            else:
                dt = dt + timedelta(hours=9)
            return fmt(dt)
    except Exception:
        pass
    return "取得不可"

def fetch_html(url: str, timeout: int = 10):
    try:
        hdrs = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=hdrs, timeout=timeout)
        if r.ok:
            return r.text
    except Exception:
        pass
    return ""

def extract_datetime_from_article(html: str) -> str:
    """JSON-LD / <time datetime> / OGメタから日時を拾ってJST文字列に"""
    if not html: return "取得不可"
    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if not isinstance(obj, dict): continue
                for key in ["datePublished", "dateModified", "uploadDate"]:
                    if obj.get(key):
                        dt = try_parse_jst(str(obj[key]).strip())
                        if dt: return fmt(dt)
        except Exception:
            continue

    # <time datetime>
    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        dt = try_parse_jst(t["datetime"].strip())
        if dt: return fmt(dt)

    # OG系
    for prop in ["article:published_time", "article:modified_time", "og:updated_time"]:
        m = soup.find("meta", attrs={"property": prop, "content": True})
        if m and m.get("content"):
            dt = try_parse_jst(m["content"].strip())
            if dt: return fmt(dt)

    return "取得不可"

def chrome_driver():
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--disable-gpu")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1920,1080")
    opt.add_argument("--lang=ja-JP")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opt)

# ====== 取得：Google ======
def get_google_news(keyword: str):
    """
    まず参照コードのセレクタで取得（a.JtKRv / time.hvbAAd / div.vr1PYe）
    取れない場合に備えてフォールバックセレクタも併用
    """
    driver = chrome_driver()
    url = f"https://news.google.com/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    driver.get(url)
    time.sleep(5)
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.6)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data, with_time = [], 0

    # 参照コード準拠の取り方
    for art in soup.find_all("article"):
        try:
            a_tag   = art.select_one("a.JtKRv") or art.select_one("a.WwrzSb") or art.select_one("a.DY5T1d") or art.select_one("h3 a")
            time_el = art.select_one("time.hvbAAd") or art.select_one("time[datetime]") or art.find("time")
            source_el = art.select_one("div.vr1PYe") or art.select_one("div.SVJrMe")
            if not a_tag: 
                continue
            title = a_tag.get_text(strip=True)
            href  = a_tag.get("href")
            if not title or not href:
                continue
            url = "https://news.google.com" + href[1:] if href.startswith("./") else href
            if not url.startswith("http"):
                continue

            pub = "取得不可"
            if time_el and time_el.get("datetime"):
                dt = try_parse_jst(time_el.get("datetime").strip())
                if dt: 
                    pub = fmt(dt); with_time += 1
            if pub == "取得不可":
                html = fetch_html(url)
                pub = extract_datetime_from_article(html)

            source = (source_el.get_text(strip=True) if source_el else "Google") or "Google"
            data.append({"タイトル": title, "URL": url, "投稿日": pub, "引用元": source, "ソース": "Google"})
        except Exception:
            continue

    print(f"✅ Googleニュース: {len(data)} 件（投稿日取得 {with_time} 件）")
    return data

# ====== 取得：Yahoo ======
def get_yahoo_news(keyword: str):
    driver = chrome_driver()
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    def is_article(u: str) -> bool:
        return u and u.startswith("http") and ("news.yahoo.co.jp/articles/" in u or "news.yahoo.co.jp/pickup/" in u)

    raw_links = [a.get("href") for a in soup.find_all("a", href=True)]
    article_links = [u for u in raw_links if is_article(u)]

    data, with_time = [], 0
    seen = set()
    for href in article_links:
        if href in seen: continue
        seen.add(href)
        try:
            html = fetch_html(href)
            if not html:
                continue
            soup2 = BeautifulSoup(html, "html.parser")

            # タイトル
            title = ""
            h1 = soup2.find("h1")
            if h1: title = h1.get_text(strip=True)
            if not title:
                ogt = soup2.find("meta", attrs={"property": "og:title", "content": True})
                if ogt: title = ogt["content"].strip()
            if not title or len(title) < 6:
                continue

            # 日付
            pub = extract_datetime_from_article(html)
            if pub != "取得不可": with_time += 1

            # 出典
            source = "Yahoo"
            src_meta = soup2.find("meta", attrs={"name": "source", "content": True})
            if src_meta and src_meta.get("content"):
                source = src_meta["content"].strip() or "Yahoo"

            data.append({"タイトル": title, "URL": href, "投稿日": pub, "引用元": source, "ソース": "Yahoo"})
        except Exception:
            continue

    print(f"✅ Yahoo!ニュース（記事のみ）: {len(data)} 件（投稿日取得 {with_time} 件）")
    return data

# ====== 取得：MSN（Bing News） ======
def get_msn_news(keyword: str):
    """
    参照コードの data-* 属性付き 'div.news-card' を優先
    無い場合は見出しリンクからフォールバック
    """
    base = jst_now()
    driver = chrome_driver()
    url = f"https://www.bing.com/news/search?q={keyword}&qft=sortbydate%3d'1'&form=YFNR"
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data, with_time = [], 0

    # 優先：参照コードのカード
    cards = soup.select("div.news-card") or []
    for card in cards:
        try:
            title  = (card.get("data-title") or "").strip()
            link   = (card.get("data-url") or "").strip()
            source = (card.get("data-author") or "").strip() or "MSN"
            if not title or not link or not link.startswith("http"):
                continue

            pub_label = ""
            pub_tag = card.find("span", attrs={"aria-label": True})
            if pub_tag and pub_tag.has_attr("aria-label"):
                pub_label = pub_tag["aria-label"].strip().lower()

            pub = parse_relative_time(pub_label, base)
            if pub != "取得不可":
                with_time += 1
            else:
                html = fetch_html(link)
                pub = extract_datetime_from_article(html)
                if pub == "取得不可":
                    pub = get_last_modified_datetime(link)

            data.append({"タイトル": title, "URL": link, "投稿日": pub, "引用元": source, "ソース": "MSN"})
        except Exception:
            continue

    # フォールバック：一般的なリンク構造
    if not data:
        for a in soup.select("a.title, h2 a, h3 a"):
            try:
                href = a.get("href")
                title = a.get_text(strip=True)
                if not href or not href.startswith("http") or not title:
                    continue
                # 近傍の相対時間
                container = a.find_parent(["div","li","article"]) or soup
                lab = ""
                tspan = container.find("span", attrs={"aria-label": True})
                if tspan and tspan.has_attr("aria-label"):
                    lab = tspan["aria-label"].strip()
                pub = parse_relative_time(lab, base)
                if pub == "取得不可":
                    html = fetch_html(href)
                    pub = extract_datetime_from_article(html)
                    if pub == "取得不可":
                        pub = get_last_modified_datetime(href)
                else:
                    with_time += 1
                data.append({"タイトル": title, "URL": href, "投稿日": pub, "引用元": "MSN", "ソース": "MSN"})
            except Exception:
                continue

    print(f"✅ MSNニュース: {len(data)} 件（投稿日取得/推定 {with_time} 件）")
    return data

# ====== スプレッドシート ======
def get_gspread_client():
    key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    if key:
        try:
            creds = json.loads(key)
            return gspread.service_account_from_dict(creds)
        except Exception as e:
            raise RuntimeError(f"GCP_SERVICE_ACCOUNT_KEY のJSONが不正です: {e}")
    return gspread.service_account(filename="credentials.json")

def compute_window(now_jst: datetime):
    """
    いつ実行しても「昨日15:00〜今日14:59」が対象
    シート名は「今日のYYMMDD」に固定
    """
    today = now_jst.date()
    today_1500 = datetime.combine(today, dtime(hour=15, minute=0))
    start = today_1500 - timedelta(days=1)        # 昨日 15:00
    end   = today_1500 - timedelta(seconds=1)     # 今日 14:59:59
    label = today.strftime("%y%m%d")              # 今日 → YYMMDD
    return start, end, label

def build_daily_sheet(sh, rows_all: list):
    now = jst_now()
    start, end, label = compute_window(now)
    print(f"🕒 集約期間: {fmt(start)} 〜 {fmt(end)} → シート名: {label}")

    filtered = {"MSN": [], "Google": [], "Yahoo": []}
    no_date = 0
    for r in rows_all:
        dt = try_parse_jst(r.get("投稿日", ""))
        if not dt:
            no_date += 1
            continue
        if start <= dt <= end:
            src = r.get("ソース","")
            if src in filtered:
                filtered[src].append(r)

    print(f"📊 フィルタ結果: MSN={len(filtered['MSN'])}, Google={len(filtered['Google'])}, Yahoo={len(filtered['Yahoo'])}, 日付無しスキップ={no_date}")

    def dedup_sort(lst):
        seen = set()
        uniq = []
        for d in lst:
            if d["URL"] not in seen:
                seen.add(d["URL"]); uniq.append(d)
        uniq.sort(key=lambda x: try_parse_jst(x["投稿日"]) or datetime(1970,1,1), reverse=True)
        return uniq

    ordered = []
    for src in ["MSN", "Google", "Yahoo"]:
        ordered.extend(dedup_sort(filtered[src]))

    headers = ["ソース", "URL", "タイトル", "投稿日", "引用元"]
    try:
        ws = sh.worksheet(label)
        ws.clear()
        ws.append_row(headers)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=label, rows=str(max(2, len(ordered)+5)), cols="5")
        ws.append_row(headers)

    if ordered:
        rows = [[d["ソース"], d["URL"], d["タイトル"], d["投稿日"], d["引用元"]] for d in ordered]
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"✅ 集約シート {label}: {len(rows)} 件")
    else:
        print(f"⚠️ 集約シート {label}: 対象記事なし")

# ====== メイン ======
def main():
    print(f"🔎 キーワード: {KEYWORD}")
    print(f"📄 SPREADSHEET_ID: {SPREADSHEET_ID}")

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    print(f"📘 Opened spreadsheet title: {sh.title}")

    print("\n--- 取得 ---")
    google_items = get_google_news(KEYWORD)
    yahoo_items  = get_yahoo_news(KEYWORD)
    msn_items    = get_msn_news(KEYWORD)

    # まとめだけ出力
    all_items = []
    all_items.extend(msn_items)
    all_items.extend(google_items)
    all_items.extend(yahoo_items)

    print("\n--- 集約（まとめシートのみ / A列=ソース / 順=MSN→Google→Yahoo） ---")
    build_daily_sheet(sh, all_items)

if __name__ == "__main__":
    main()
