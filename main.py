# -*- coding: utf-8 -*-
"""
ニュース集約スクリプト
- MSN / Google / Yahoo からキーワード検索でニュースを取得（Selenium）
- 取得結果を各ソース専用シート（Google / Yahoo / MSN）へURL去重で追記
- 日付ウィンドウ（前日15:00〜当日14:59 JST）で「YYMMDD」シートに集約
- 集約シートは A列にソース名、並び順は MSN → Google → Yahoo（各ソース内は投稿日降順）

環境変数:
- GCP_SERVICE_ACCOUNT_KEY: サービスアカウントJSONの文字列（GitHub Secrets 推奨）
  * ローカル実行は同ディレクトリに credentials.json でも可
- NEWS_KEYWORD: 検索キーワード（デフォルト: "日産"）
- SPREADSHEET_ID: 書き込み先スプレッドシートID
  * デフォルト: 1Vs4Cx8QPN4H2NOgtwaviOCe8zBTpUNDgJjqkHr51IZE
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
    "1Vs4Cx8QPN4H2NOgtwaviOCe8zBTpUNDgJjqkHr51IZE"  # ご指定のIDをデフォルトに
)


# ========= ユーティリティ =========
def jst_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=9)


def fmt(dt: datetime) -> str:
    return dt.strftime("%Y/%m/%d %H:%M")


def try_parse_jst(dt_str: str):
    """ 'YYYY/MM/DD HH:MM' などを datetime(JST) に。失敗は None """
    if not dt_str or dt_str == "取得不可":
        return None
    patterns = [
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%SZ",  # ZはUTC想定→JSTへ
    ]
    for p in patterns:
        try:
            dt = datetime.strptime(dt_str, p)
            if p.endswith("Z"):
                dt = dt + timedelta(hours=9)
            return dt
        except Exception:
            pass
    return None


def parse_relative_time(label: str, base: datetime) -> str:
    """
    「◯分前 / ◯時間前 / ◯日前」「MM月DD日」「HH:MM」等を JST 絶対時刻へ
    失敗時は "取得不可"
    """
    s = (label or "").strip()
    try:
        # ◯分前
        m = re.search(r"(\d+)\s*分前", s)
        if m:
            return fmt(base - timedelta(minutes=int(m.group(1))))
        # ◯時間前
        h = re.search(r"(\d+)\s*時間前", s)
        if h:
            return fmt(base - timedelta(hours=int(h.group(1))))
        # ◯日前
        d = re.search(r"(\d+)\s*日前", s)
        if d:
            return fmt(base - timedelta(days=int(d.group(1))))
        # 例) 8月20日 / 08月20日
        if re.match(r"\d{1,2}月\d{1,2}日", s):
            dt = datetime.strptime(f"{base.year}年{s}", "%Y年%m月%d日")
            return fmt(dt)
        # 例) 2025/08/20
        if re.match(r"\d{4}/\d{1,2}/\d{1,2}$", s):
            dt = datetime.strptime(s, "%Y/%m/%d")
            return fmt(dt)
        # 例) 12:34（当日か前日）
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
    """ HEADのLast-ModifiedからJSTを推定（なければ取得不可） """
    try:
        r = requests.head(url, timeout=5, allow_redirects=True)
        if "Last-Modified" in r.headers:
            dt = parsedate_to_datetime(r.headers["Last-Modified"])
            # tz-aware の場合はUTC基準、naiveは一応UTCとして+9h
            if dt.tzinfo:
                dt = dt.astimezone(tz=None)  # ローカルtz（GitHub ActionsはUTC）
                dt = dt + timedelta(hours=9)  # JST
            else:
                dt = dt + timedelta(hours=9)
            return fmt(dt)
    except Exception:
        pass
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
    """
    Googleニュース検索 (news.google.com) をSelenium+BS4で取得
    """
    driver = chrome_driver()
    url = f"https://news.google.com/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    driver.get(url)
    time.sleep(4)

    # スクロール数回
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    # 記事カードはarticleタグ。クラスは揺れるため、セレクタは保守的に
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
            if title and url:
                data.append({"タイトル": title, "URL": url, "投稿日": pub, "引用元": source})
        except Exception:
            continue

    print(f"✅ Googleニュース: {len(data)} 件")
    return data


def get_yahoo_news(keyword: str):
    """
    Yahoo!ニュース検索をSelenium+BS4で取得
    """
    driver = chrome_driver()
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(url)
    time.sleep(4)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    # リスト要素はクラスが頻繁に変わるため、ゆるめに抽出
    articles = soup.find_all("li")
    for li in articles:
        try:
            a = li.find("a", href=True)
            if not a:
                continue
            title = a.get_text(strip=True)
            url = a["href"]

            # 投稿日表示（timeタグ等）
            time_tag = li.find("time")
            date_str = time_tag.get_text(strip=True) if time_tag else ""
            # (火) 等の曜日カッコ消し
            date_str = re.sub(r"\([月火水木金土日]\)", "", date_str).strip()
            pub = "取得不可"
            # 代表的な "YYYY/MM/DD HH:MM" に対応
            if re.match(r"\d{4}/\d{1,2}/\d{1,2}", date_str):
                try:
                    dt = try_parse_jst(date_str)
                    if dt:
                        pub = fmt(dt)
                except Exception:
                    pass

            # 出典（短いテキスト）を推測
            source = ""
            for tag in li.find_all(["span", "div"], string=True):
                text = tag.get_text(strip=True)
                if 2 <= len(text) <= 20 and not text.isdigit() and re.search(r"[ぁ-んァ-ン一-龥A-Za-z]", text):
                    source = text
                    break
            if not source:
                source = "Yahoo"

            if title and url:
                data.append({"タイトル": title, "URL": url, "投稿日": pub, "引用元": source})
        except Exception:
            continue

    print(f"✅ Yahoo!ニュース: {len(data)} 件")
    return data


def get_msn_news(keyword: str):
    """
    MSN（Bingニュース検索）のカードをSelenium+BS4で取得
    """
    base = jst_now()
    driver = chrome_driver()
    # 新しい順指定（sortbydate=1）
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

            # 相対時間（aria-label等）
            pub_label = ""
            span = c.find("span", attrs={"aria-label": True})
            if span and span.has_attr("aria-label"):
                pub_label = span["aria-label"].strip()

            pub = parse_relative_time(pub_label, base)
            if pub == "取得不可" and link:
                # 最終手段：HEAD の Last-Modified
                pub = get_last_modified_datetime(link)

            if title and link:
                data.append({"タイトル": title, "URL": link, "投稿日": pub, "引用元": author or "MSN"})
        except Exception:
            continue

    print(f"✅ MSNニュース: {len(data)} 件")
    return data


# ========= スプレッドシート I/O =========
def get_gspread_client():
    """
    - 環境変数 GCP_SERVICE_ACCOUNT_KEY があれば dict から認証
    - なければ credentials.json を使用
    """
    key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    if key:
        try:
            creds = json.loads(key)
            return gspread.service_account_from_dict(creds)
        except Exception as e:
            raise RuntimeError(f"GCP_SERVICE_ACCOUNT_KEY のJSONが不正です: {e}")
    # ローカル等
    return gspread.service_account(filename="credentials.json")


def append_to_source_sheet(sh, sheet_name: str, articles: list):
    """
    各ソースシートへURL去重で追記
    カラム: タイトル / URL / 投稿日 / 引用元
    """
    if not articles:
        print(f"⚠️ {sheet_name}: 新規0件")
        return

    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows="1", cols="4")
        ws.append_row(["タイトル", "URL", "投稿日", "引用元"])

    values = ws.get_all_values()
    existing_urls = set(row[1] for row in values[1:] if len(row) > 1)

    new_rows = []
    for a in articles:
        url = a.get("URL")
        if url and url not in existing_urls:
            new_rows.append([a.get("タイトル", ""), url, a.get("投稿日", ""), a.get("引用元", sheet_name)])

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        print(f"✅ {sheet_name}: {len(new_rows)} 件 追記")
    else:
        print(f"⚠️ {sheet_name}: 追記対象なし（全て既存URL）")


def compute_window(now_jst: datetime):
    """
    直近完了ウィンドウを返す:
    - 実行が 15:00 以降:   昨日15:00 〜 今日14:59:59 / シート名=今日のYYMMDD
    - 実行が 15:00 より前: 一昨日15:00 〜 昨日14:59:59 / シート名=昨日のYYMMDD
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


def build_daily_sheet(sh):
    """
    各ソースシートからウィンドウ内の記事を集め、
    シート名 YYMMDD で一覧化
    並び: MSN → Google → Yahoo（各ソース内は投稿日降順）
    カラム: ソース / URL / タイトル / 投稿日 / 引用元
    """
    now = jst_now()
    start, end, label = compute_window(now)
    print(f"🕒 集約期間: {fmt(start)} 〜 {fmt(end)} → シート名: {label}")

    rows_by_source = {"MSN": [], "Google": [], "Yahoo": []}

    for src in ["MSN", "Google", "Yahoo"]:
        try:
            ws = sh.worksheet(src)
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️ {src} シートが存在しないためスキップ")
            continue

        for d in ws.get_all_records():
            url = d.get("URL") or ""
            title = d.get("タイトル") or ""
            posted = try_parse_jst(d.get("投稿日") or "")
            origin = d.get("引用元") or src
            if not url or not title or not posted:
                continue
            if start <= posted <= end:
                rows_by_source[src].append([src, url, title, fmt(posted), origin])

    # 出力順: MSN → Google → Yahoo
    ordered_rows = []
    for src in ["MSN", "Google", "Yahoo"]:
        # ソース内URL去重
        seen = set()
        uniq = []
        for r in rows_by_source[src]:
            if r[1] not in seen:
                seen.add(r[1])
                uniq.append(r)
        # 投稿日降順
        uniq.sort(key=lambda x: try_parse_jst(x[3]) or datetime(1970, 1, 1), reverse=True)
        ordered_rows.extend(uniq)

    headers = ["ソース", "URL", "タイトル", "投稿日", "引用元"]
    try:
        out_ws = sh.worksheet(label)
        out_ws.clear()
        out_ws.append_row(headers)
    except gspread.exceptions.WorksheetNotFound:
        out_ws = sh.add_worksheet(title=label, rows=str(max(2, len(ordered_rows) + 5)), cols="5")
        out_ws.append_row(headers)

    if ordered_rows:
        out_ws.append_rows(ordered_rows, value_input_option="USER_ENTERED")
        print(f"✅ 集約シート {label}: {len(ordered_rows)} 件 出力")
    else:
        print(f"⚠️ 集約シート {label}: 対象記事なし")


# ========= メイン =========
def main():
    print(f"🔎 キーワード: {KEYWORD}")
    print(f"📄 スプレッドシート: {SPREADSHEET_ID}")

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    print("\n--- Google News ---")
    google_items = get_google_news(KEYWORD)
    append_to_source_sheet(sh, "Google", google_items)

    print("\n--- Yahoo! News ---")
    yahoo_items = get_yahoo_news(KEYWORD)
    append_to_source_sheet(sh, "Yahoo", yahoo_items)

    print("\n--- MSN News ---")
    msn_items = get_msn_news(KEYWORD)
    append_to_source_sheet(sh, "MSN", msn_items)

    print("\n--- 日次集約（MSN→Google→Yahoo / A列=ソース） ---")
    build_daily_sheet(sh)


if __name__ == "__main__":
    main()
