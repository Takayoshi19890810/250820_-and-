# -*- coding: utf-8 -*-
"""
ニュース集約スクリプト（まとめシートのみ出力）
- MSN / Google / Yahoo からキーワード検索でニュース取得（Selenium）
- Yahooはノイズ除外（/articles/ または /pickup/ のみ）し、記事ページから投稿日も補完
- 日付ウィンドウ（前日15:00〜当日14:59 JST）で「YYMMDD」シートに集約
- 集約シートは A列=ソース名、並びは MSN → Google → Yahoo（各ソース内は投稿日降順）

環境変数:
- GCP_SERVICE_ACCOUNT_KEY: サービスアカウントJSON文字列（GitHub Secrets 推奨）
  （ローカルは credentials.json でも可）
- NEWS_KEYWORD: 検索キーワード（デフォ: "日産"）
- SPREADSHEET_ID: 出力先スプレッドシートID（デフォはご指定ID）
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


# ========= 設定 =========
KEYWORD = os.getenv("NEWS_KEYWORD", "日産")
SPREADSHEET_ID = os.getenv(
    "SPREADSHEET_ID",
    "1Vs4Cx8QPN4H2NOgtwaviOCe8zBTpUNDgJjqkHr51IZE"
)


# ========= ユーティリティ =========
def jst_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=9)


def fmt(dt: datetime) -> str:
    return dt.strftime("%Y/%m/%d %H:%M")


def try_parse_jst(dt_str: str):
    """代表的な日時文字列→datetime（JST）。失敗時は None"""
    if not dt_str or dt_str == "取得不可":
        return None
    patterns = [
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%SZ",  # UTC → JST
        "%Y-%m-%dT%H:%M:%S%z", # tz aware
    ]
    for p in patterns:
        try:
            dt = datetime.strptime(dt_str, p)
            if p.endswith("Z"):
                dt = dt + timedelta(hours=9)
            elif "%z" in p:
                # tz-aware → JSTへ
                dt = dt.astimezone(tz=None)  # UTCローカル
                dt = dt + timedelta(hours=9)
            return dt
        except Exception:
            pass
    return None


def parse_relative_time(label: str, base: datetime) -> str:
    """
    「◯分前 / ◯時間前 / ◯日前」「MM月DD日」「HH:MM」などを JST 絶対時刻に。
    失敗時は "取得不可"
    """
    s = (label or "").strip()
    try:
        m = re.search(r"(\d+)\s*分前", s)
        if m:
            return fmt(base - timedelta(minutes=int(m.group(1))))
        h = re.search(r"(\d+)\s*時間前", s)
        if h:
            return fmt(base - timedelta(hours=int(h.group(1))))
        d = re.search(r"(\d+)\s*日前", s)
        if d:
            return fmt(base - timedelta(days=int(d.group(1))))
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
    """HTTPヘッダ Last-Modified → JST（最終手段）"""
    try:
        r = requests.head(url, timeout=6, allow_redirects=True)
        if "Last-Modified" in r.headers:
            dt = parsedate_to_datetime(r.headers["Last-Modified"])
            if dt.tzinfo:
                dt = dt.astimezone(tz=None)  # UTCローカル
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


def extract_yahoo_article_datetime(html: str) -> str:
    """Yahoo記事ページから投稿日を推定（複数候補を順にチェック）"""
    if not html:
        return "取得不可"
    soup = BeautifulSoup(html, "html.parser")

    # 1) <time datetime="...">
    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        cand = t["datetime"].strip()
        # 例: 2025-08-20T09:00:00+09:00 / 2025-08-20T00:00:00Z
        dt = try_parse_jst(cand)
        if dt:
            return fmt(dt)

    # 2) meta[property=article:published_time] / article:modified_time / og:updated_time
    for prop in ["article:published_time", "article:modified_time", "og:updated_time"]:
        m = soup.find("meta", attrs={"property": prop, "content": True})
        if m and m.get("content"):
            dt = try_parse_jst(m["content"].strip())
            if dt:
                return fmt(dt)

    return "取得不可"


def chrome_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


# ========= スクレイパ =========
def get_google_news(keyword: str):
    driver = chrome_driver()
    url = f"https://news.google.com/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    driver.get(url)
    time.sleep(4)
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.4)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    for art in soup.find_all("article"):
        try:
            a = art.select_one("a.JtKRv, a.WwrzSb")
            t = art.select_one("time[datetime]")
            src = art.select_one("div.vr1PYe, div.SVJrMe")
            if not a or not t:
                continue
            title = a.get_text(strip=True)
            href = a.get("href")
            url = "https://news.google.com" + href[1:] if href and href.startswith("./") else href
            dt = datetime.strptime(t.get("datetime"), "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=9)
            pub = fmt(dt)
            source = src.get_text(strip=True) if src else "Google"
            if title and url and url.startswith("http"):
                data.append({"タイトル": title, "URL": url, "投稿日": pub, "引用元": source, "ソース": "Google"})
        except Exception:
            continue
    print(f"✅ Googleニュース: {len(data)} 件")
    return data


def get_yahoo_news(keyword: str):
    driver = chrome_driver()
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(url)
    time.sleep(4)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    # ノイズ除外のため、記事URLのみ採用
    def is_article(u: str) -> bool:
        if not u or not u.startswith("http"):
            return False
        return ("news.yahoo.co.jp/articles/" in u) or ("news.yahoo.co.jp/pickup/" in u)

    # 広めに a[href] を拾い、記事URLのみ残す
    for a in soup.find_all("a", href=True):
        try:
            href = a["href"]
            if not is_article(href):
                continue
            title = a.get_text(strip=True)
            if not title or len(title) < 6:
                # サムネ/カテゴリ等の短いテキストを弾く
                continue

            # 可能なら記事ページから日時補完
            html = fetch_html(href)
            pub = extract_yahoo_article_datetime(html)
            # 出典（media名）も拾えたら載せる（無ければ "Yahoo"）
            source = "Yahoo"
            if html:
                soup2 = BeautifulSoup(html, "html.parser")
                m = soup2.find("meta", attrs={"name": "source", "content": True})
                if m and m.get("content"):
                    source = m["content"].strip() or "Yahoo"
                # 代替で、記事ヘッダ付近の媒体名っぽい要素を拾う簡易ロジック
                if source == "Yahoo":
                    cand = soup2.find(["span","div"], string=re.compile(r".+"))
                    if cand:
                        txt = cand.get_text(strip=True)
                        if 2 <= len(txt) <= 20 and not txt.isdigit():
                            source = txt

            data.append({"タイトル": title, "URL": href, "投稿日": pub, "引用元": source, "ソース": "Yahoo"})
        except Exception:
            continue

    # 重複URL除去
    uniq = []
    seen = set()
    for d in data:
        if d["URL"] not in seen:
            seen.add(d["URL"])
            uniq.append(d)

    print(f"✅ Yahoo!ニュース（記事のみ）: {len(uniq)} 件")
    return uniq


def get_msn_news(keyword: str):
    base = jst_now()
    driver = chrome_driver()
    url = f"https://www.bing.com/news/search?q={keyword}&qft=sortbydate%3d'1'&form=YFNR"
    driver.get(url)
    time.sleep(4)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    cards = soup.select("div.news-card, news-card")
    for c in cards:
        try:
            title = (c.get("data-title") or "").strip()
            link = (c.get("data-url") or "").strip()
            author = (c.get("data-author") or "").strip()
            if not title or not link or not link.startswith("http"):
                continue

            pub_label = ""
            span = c.find("span", attrs={"aria-label": True})
            if span and span.has_attr("aria-label"):
                pub_label = span["aria-label"].strip()

            pub = parse_relative_time(pub_label, base)
            if pub == "取得不可":
                pub = get_last_modified_datetime(link)

            data.append({"タイトル": title, "URL": link, "投稿日": pub, "引用元": author or "MSN", "ソース": "MSN"})
        except Exception:
            continue

    print(f"✅ MSNニュース: {len(data)} 件")
    return data


# ========= スプレッドシート =========
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
    実行が 15:00 以降:   昨日15:00〜今日14:59:59 / シート名=今日のYYMMDD
    実行が 15:00 より前: 一昨日15:00〜昨日14:59:59 / シート名=昨日のYYMMDD
    """
    today = now_jst.date()
    fifteen = datetime.combine(today, dtime(hour=15, minute=0))
    if now_jst >= fifteen:
        start = fifteen - timedelta(days=1)
        end = fifteen - timedelta(seconds=1)
        label = today.strftime("%y%m%d")
    else:
        start = fifteen - timedelta(days=2)
        end = fifteen - timedelta(days=1, seconds=1)
        label = (today - timedelta(days=1)).strftime("%y%m%d")
    return start, end, label


def build_daily_sheet(sh, rows_all: list):
    """
    rows_all: [ {"ソース","URL","タイトル","投稿日","引用元"} ... ]
    並び: MSN → Google → Yahoo（各ソース内は投稿日降順）
    """
    now = jst_now()
    start, end, label = compute_window(now)
    print(f"🕒 集約期間: {fmt(start)} 〜 {fmt(end)} → シート名: {label}")

    # ウィンドウ内のみに絞る
    filtered_by_src = {"MSN": [], "Google": [], "Yahoo": []}
    for r in rows_all:
        dt = try_parse_jst(r.get("投稿日", ""))
        if dt and (start <= dt <= end):
            src = r.get("ソース", "")
            if src in filtered_by_src:
                filtered_by_src[src].append(r)

    # ソース内URL去重 & 投稿日降順
    def dedup_sort(lst):
        seen = set()
        uniq = []
        for d in lst:
            if d["URL"] not in seen:
                seen.add(d["URL"])
                uniq.append(d)
        uniq.sort(key=lambda x: try_parse_jst(x["投稿日"]) or datetime(1970,1,1), reverse=True)
        return uniq

    ordered = []
    for src in ["MSN", "Google", "Yahoo"]:
        ordered.extend(dedup_sort(filtered_by_src[src]))

    # 出力
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


# ========= メイン =========
def main():
    print(f"🔎 キーワード: {KEYWORD}")
    print(f"📄 SPREADSHEET_ID(env優先): {SPREADSHEET_ID}")

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    print(f"📘 Opened spreadsheet title: {sh.title}")

    print("\n--- 取得 ---")
    google_items = get_google_news(KEYWORD)
    yahoo_items  = get_yahoo_news(KEYWORD)
    msn_items    = get_msn_news(KEYWORD)

    # まとめシート用に結合（個別シートへの書き込みはしない）
    all_items = []
    all_items.extend(msn_items)
    all_items.extend(google_items)
    all_items.extend(yahoo_items)

    print("\n--- 集約（まとめシートのみ出力: MSN→Google→Yahoo / A列=ソース） ---")
    build_daily_sheet(sh, all_items)


if __name__ == "__main__":
    main()
