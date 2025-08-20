import os
import json
import time
import re
import random
import requests
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import gspread
import gspread.exceptions

# ========= 設定 =========
KEYWORD = "日産"  # 必要に応じて変更
SPREADSHEET_ID = "1Vs4Cx8QPN4H2NOgtwaviOCe8zBTpUNDgJjqkHr51IZE"  # 指定の出力先
JST = timezone(timedelta(hours=9))

# 出力列の並び
OUTPUT_HEADERS = ["タイトル", "URL", "投稿日", "引用元"]

# ========= ユーティリティ =========
def format_datetime(dt_obj: datetime) -> str:
    return dt_obj.astimezone(JST).strftime("%Y/%m/%d %H:%M")

def try_parse_jst_datetime(s: str):
    """
    "YYYY/MM/DD HH:MM" 想定の文字列をJST datetimeへ。失敗で None
    """
    s = (s or "").strip()
    for fmt in ["%Y/%m/%d %H:%M", "%Y/%m/%d", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=JST)
            return dt.astimezone(JST)
        except Exception:
            continue
    return None

def parse_relative_time(pub_label: str, base_time: datetime) -> str:
    """
    MSNの相対表記などをJSTの絶対時間へ。戻り値は "YYYY/MM/DD HH:MM" または "取得不可"
    """
    label = (pub_label or "").strip()
    try:
        # 日本語相対 / 英語相対の両方にそこそこ耐性を持たせる
        if "分前" in label or "minute" in label:
            m = re.search(r"(\d+)", label)
            if m:
                dt = base_time - timedelta(minutes=int(m.group(1)))
                return format_datetime(dt)
        if "時間前" in label or "hour" in label:
            m = re.search(r"(\d+)", label)
            if m:
                dt = base_time - timedelta(hours=int(m.group(1)))
                return format_datetime(dt)
        if "日前" in label or "day" in label:
            m = re.search(r"(\d+)", label)
            if m:
                dt = base_time - timedelta(days=int(m.group(1)))
                return format_datetime(dt)
        # "8/20" のような表記が来た場合の緩い対応
        m = re.match(r"(\d{1,2})/(\d{1,2})", label)
        if m:
            month, day = int(m.group(1)), int(m.group(2))
            dt = datetime(year=base_time.year, month=month, day=day, tzinfo=JST)
            return format_datetime(dt)
    except Exception:
        pass
    return "取得不可"

def get_last_modified_datetime(url: str) -> str:
    try:
        res = requests.head(url, timeout=5)
        if "Last-Modified" in res.headers:
            dt = parsedate_to_datetime(res.headers["Last-Modified"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return format_datetime(dt.astimezone(JST))
    except Exception:
        pass
    return "取得不可"

def setup_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    # UA固定は必要に応じて
    # options.add_argument("--user-agent=Mozilla/5.0 ...")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

# ========= スクレイパ =========
def get_google_news(keyword: str) -> list[dict]:
    driver = setup_driver()
    url = f"https://news.google.com/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    driver.get(url)
    time.sleep(5)
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    for art in soup.find_all("article"):
        try:
            a_tag = art.select_one("a.JtKRv")
            time_tag = art.select_one("time.hvbAAd")
            source_tag = art.select_one("div.vr1PYe")

            if not a_tag or not time_tag:
                continue

            title = a_tag.get_text(strip=True)
            href = a_tag.get("href", "")
            url = "https://news.google.com" + href[1:] if href.startswith("./") else href

            # GoogleはUTCのISO表記。JSTに変換
            iso = time_tag.get("datetime")
            dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).astimezone(JST)
            pub = format_datetime(dt)

            source = source_tag.get_text(strip=True) if source_tag else "Google"
            data.append({"タイトル": title, "URL": url, "投稿日": pub, "引用元": source})
        except Exception:
            continue
    print(f"✅ Googleニュース件数: {len(data)} 件")
    return data

def get_yahoo_news(keyword: str) -> list[dict]:
    driver = setup_driver()
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    items = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    for art in items:
        try:
            title_tag = art.find("div", class_=re.compile("sc-3ls169-0"))
            link_tag = art.find("a", href=True)
            time_tag = art.find("time")

            if not title_tag or not link_tag:
                continue

            title = title_tag.get_text(strip=True)
            url = link_tag["href"]
            date_str = time_tag.get_text(strip=True) if time_tag else ""
            # 曜日"(月)"などを除去
            date_str = re.sub(r"\([月火水木金土日]\)", "", date_str).strip()

            # 期待形式: "YYYY/MM/DD HH:MM"
            pub = date_str if date_str else "取得不可"

            # ソース推定（失敗時は "Yahoo"）
            source = "Yahoo"
            # 画面構造の変化に強めのフォールバック
            spans = art.find_all(["span", "div"], string=True)
            for s in spans:
                text = s.get_text(strip=True)
                if 2 <= len(text) <= 20 and not text.isdigit() and re.search(r"[ぁ-んァ-ン一-龥A-Za-z]", text):
                    source = text
                    break

            data.append({"タイトル": title, "URL": url, "投稿日": pub, "引用元": source})
        except Exception:
            continue
    print(f"✅ Yahoo!ニュース件数: {len(data)} 件")
    return data

def get_msn_news(keyword: str) -> list[dict]:
    now = datetime.now(JST)
    driver = setup_driver()
    url = f"https://www.bing.com/news/search?q={keyword}&qft=sortbydate%3d'1'&form=YFNR"
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    cards = soup.select("div.news-card")
    for card in cards:
        try:
            title = (card.get("data-title") or "").strip()
            url = (card.get("data-url") or "").strip()
            source = (card.get("data-author") or "").strip() or "MSN"

            pub_label = ""
            pub_tag = card.find("span", attrs={"aria-label": True})
            if pub_tag and pub_tag.has_attr("aria-label"):
                pub_label = pub_tag["aria-label"].strip()

            pub = parse_relative_time(pub_label, now)
            if pub == "取得不可" and url:
                pub = get_last_modified_datetime(url)

            if title and url:
                data.append({"タイトル": title, "URL": url, "投稿日": pub, "引用元": source})
        except Exception:
            continue
    print(f"✅ MSNニュース件数: {len(data)} 件")
    return data

# ========= 集計 & 書き込み =========
def compute_window(now_jst: datetime):
    """
    「前日15:00〜当日14:59」の集計窓と、シート名(YYMMDD)を返す。
    - 15:00以降に実行 → 窓は「当日15:00までの前日15:00から」、シート名は今日
    - 15:00より前に実行 → 窓は「今日14:59までの前日15:00から」、シート名は今日
    """
    today = now_jst.date()
    # 当日14:59:59
    end = datetime(today.year, today.month, today.day, 14, 59, 59, tzinfo=JST)
    start = end - timedelta(days=1) + timedelta(seconds=1)  # 前日15:00:00
    # シート名: YYMMDD（endの日付を使用）
    sheet_name = end.strftime("%y%m%d")
    return start, end, sheet_name

def in_window(dt_str: str, start: datetime, end: datetime) -> bool:
    dt = try_parse_jst_datetime(dt_str)
    if dt is None:
        return False
    return start <= dt <= end

def service_account():
    # 環境変数 GCP_SERVICE_ACCOUNT_KEY があればそれを使用、なければ credentials.json を読む
    env_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY", "")
    if env_str:
        try:
            creds = json.loads(env_str)
            return gspread.service_account_from_dict(creds)
        except Exception as e:
            raise RuntimeError(f"サービスアカウントJSONの読み込みに失敗: {e}")
    else:
        return gspread.service_account(filename="credentials.json")

def write_unified_sheet(articles: list[dict], spreadsheet_id: str, sheet_name: str):
    gc = service_account()

    # 5回までリトライ（API 429対策）
    for attempt in range(5):
        try:
            sh = gc.open_by_key(spreadsheet_id)
            try:
                ws = sh.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=sheet_name, rows="100", cols=str(len(OUTPUT_HEADERS)))
                ws.append_row(OUTPUT_HEADERS, value_input_option="USER_ENTERED")

            # 既存URLの重複回避
            existing = ws.get_all_values()
            existing_urls = set()
            if existing and len(existing) > 1:
                for row in existing[1:]:
                    if len(row) >= 2 and row[1]:
                        existing_urls.add(row[1])

            new_rows = []
            for a in articles:
                url = a.get("URL", "")
                if not url or url in existing_urls:
                    continue
                new_rows.append([a.get("タイトル", ""), url, a.get("投稿日", ""), a.get("引用元", "")])

            if new_rows:
                ws.append_rows(new_rows, value_input_option="USER_ENTERED")
                print(f"✅ {len(new_rows)} 件を '{sheet_name}' に追記しました。")
            else:
                print("⚠️ 追記対象なし（重複 or 該当期間なし）")

            return
        except gspread.exceptions.APIError as e:
            print(f"⚠️ Google API Error (attempt {attempt+1}/5): {e}")
            time.sleep(5 + random.random() * 5)

    raise RuntimeError("❌ スプレッドシート書き込みに失敗（5回試行）")

def main():
    now_jst = datetime.now(JST)
    start, end, sheet_name = compute_window(now_jst)
    print(f"📅 収集ウィンドウ: {start.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end.strftime('%Y/%m/%d %H:%M:%S')} (JST)")
    print(f"🗂 出力シート名: {sheet_name}")

    all_articles = []
    # 取得
    g = get_google_news(KEYWORD)
    y = get_yahoo_news(KEYWORD)
    m = get_msn_news(KEYWORD)

    # 期間フィルタ + URL重複排除（優先度: 早く取れた順）
    seen = set()
    for src_list in [g, y, m]:
        for a in src_list:
            if not a.get("URL"):
                continue
            if a["URL"] in seen:
                continue
            if a.get("投稿日") and in_window(a["投稿日"], start, end):
                all_articles.append(a)
                seen.add(a["URL"])

    print(f"🧮 期間該当件数: {len(all_articles)}")

    # 書き込み
    if all_articles:
        write_unified_sheet(all_articles, SPREADSHEET_ID, sheet_name)
    else:
        print("⚠️ 該当データがありませんでした。")

if __name__ == "__main__":
    main()
