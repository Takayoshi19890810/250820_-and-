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

# === NEW: Gemini ===
import google.generativeai as genai

# ========= 設定 =========
KEYWORD = "日産"  # 必要に応じて変更
SPREADSHEET_ID = "1Vs4Cx8QPN4H2NOgtwaviOCe8zBTpUNDgJjqkHr51IZE"  # 指定の出力先
JST = timezone(timedelta(hours=9))

# 出力列の並び（A〜G）
OUTPUT_HEADERS = ["ソース", "タイトル", "URL", "投稿日", "引用元", "ポジネガ", "カテゴリ"]

# Gemini モデル（速さ重視なら 1.5-flash、精度重視なら 1.5-pro）
GEMINI_MODEL_NAME = os.environ.get("GEMINI_MODEL_NAME", "gemini-1.5-flash")


# ========= ユーティリティ =========
def format_datetime(dt_obj: datetime) -> str:
    return dt_obj.astimezone(JST).strftime("%Y/%m/%d %H:%M")

def try_parse_jst_datetime(s: str):
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
    label = (pub_label or "").strip()
    try:
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

            iso = time_tag.get("datetime")
            dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).astimezone(JST)
            pub = format_datetime(dt)

            source = source_tag.get_text(strip=True) if source_tag else "Google"
            data.append({"ソース": "Google", "タイトル": title, "URL": url, "投稿日": pub, "引用元": source})
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
            date_str = re.sub(r"\([月火水木金土日]\)", "", date_str).strip()
            pub = date_str if date_str else "取得不可"

            source = "Yahoo"
            spans = art.find_all(["span", "div"], string=True)
            for s in spans:
                text = s.get_text(strip=True)
                if 2 <= len(text) <= 20 and not text.isdigit() and re.search(r"[ぁ-んァ-ン一-龥A-Za-z]", text):
                    source = text
                    break

            data.append({"ソース": "Yahoo", "タイトル": title, "URL": url, "投稿日": pub, "引用元": source})
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
                data.append({"ソース": "MSN", "タイトル": title, "URL": url, "投稿日": pub, "引用元": source})
        except Exception:
            continue
    print(f"✅ MSNニュース件数: {len(data)} 件")
    return data


# ========= 集計 & 書き込み =========
def compute_window(now_jst: datetime):
    """
    「前日15:00〜当日14:59」の集計窓と、シート名(YYMMDD)を返す。
    """
    today = now_jst.date()
    end = datetime(today.year, today.month, today.day, 14, 59, 59, tzinfo=JST)
    start = end - timedelta(days=1) + timedelta(seconds=1)  # 前日15:00:00
    sheet_name = end.strftime("%y%m%d")
    return start, end, sheet_name

def in_window(dt_str: str, start: datetime, end: datetime) -> bool:
    dt = try_parse_jst_datetime(dt_str)
    if dt is None:
        return False
    return start <= dt <= end

def service_account():
    env_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY", "")
    if env_str:
        try:
            creds = json.loads(env_str)
            return gspread.service_account_from_dict(creds)
        except Exception as e:
            raise RuntimeError(f"サービスアカウントJSONの読み込みに失敗: {e}")
    else:
        return gspread.service_account(filename="credentials.json")


# ========= NEW: タイトル分類（Gemini） =========
GEMINI_PROMPT = """
あなたは敏腕雑誌記者です。以下のニュースタイトルごとに、次の二つを判定してください。
1) ポジネガ判定: 「ポジティブ」「ネガティブ」「ニュートラル」から一つ
2) カテゴリー: 次から必ず一つだけ選んでください（並記禁止）
   - 会社（ニッサン、トヨタ、ホンダ、スバル、マツダ、スズキ、ミツビシ、ダイハツの場合は (企業名) を付記）
   - 車（新型/現行/旧型 + 名称を () で記載。日産以外の車なら「車（競合）」とし、()に名称）
   - 技術（EV）
   - 技術（e-POWER）
   - 技術（e-4ORCE）
   - 技術（AD/ADAS）
   - 技術（その他）
   - モータースポーツ
   - 株式
   - 政治・経済
   - スポーツ
   - その他

制約:
- 出力は必ず JSON 配列。各要素は {"title": <入力タイトル>, "sentiment": "ポジティブ|ネガティブ|ニュートラル", "category": "<上記のいずれか>"} の形。
- 入力タイトルは一切変更せず、そのまま "title" に入れてください。
- 必ずタイトル数と同じ件数を返してください。
"""

def init_gemini():
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が未設定です。")
    genai.configure(api_key=api_key)
    return genai.GenerativeModel(GEMINI_MODEL_NAME)

def classify_titles_gemini(titles: list[str]) -> dict:
    """
    titles の各タイトルに対し {"sentiment":..., "category":...} を返す dict を作る
    失敗時は デフォルト: ニュートラル / その他
    """
    model = init_gemini()
    default = {"sentiment": "ニュートラル", "category": "その他"}
    if not titles:
        return {}

    # バッチで投げる（件数が多い場合に備えて 50 件ずつ）
    result_map = {}
    BATCH = 50
    for i in range(0, len(titles), BATCH):
        chunk = titles[i:i+BATCH]
        payload = {
            "titles": chunk
        }
        prompt = GEMINI_PROMPT + "\n入力タイトル一覧(JSON)：\n" + json.dumps(payload, ensure_ascii=False)
        try:
            resp = model.generate_content(prompt)
            text = resp.text.strip()
            # JSON 部分抽出（コードブロックが付くケースに対応）
            m = re.search(r"\[.*\]", text, flags=re.DOTALL)
            json_str = m.group(0) if m else text
            data = json.loads(json_str)
            if isinstance(data, list):
                for item in data:
                    t = item.get("title", "")
                    sent = item.get("sentiment", "").strip() or default["sentiment"]
                    cat = item.get("category", "").strip() or default["category"]
                    result_map[t] = {"sentiment": sent, "category": cat}
            else:
                # 想定外形式は全てデフォルト
                for t in chunk:
                    result_map[t] = default
        except Exception:
            for t in chunk:
                result_map[t] = default

        # 軽いクールダウン（レート対策）
        time.sleep(0.5)

    return result_map


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
                    if len(row) >= 3 and row[2]:
                        existing_urls.add(row[2])

            # === NEW: タイトルを先に分類（Gemini） ===
            titles = [a["タイトル"] for a in articles if a.get("タイトル")]
            title_to_cls = classify_titles_gemini(titles)

            new_rows = []
            for a in articles:
                url = a.get("URL", "")
                if not url or url in existing_urls:
                    continue
                title = a.get("タイトル", "")
                cls = title_to_cls.get(title, {"sentiment": "ニュートラル", "category": "その他"})
                new_rows.append([
                    a.get("ソース", ""),           # A: ソース
                    title,                         # B: タイトル
                    url,                           # C: URL
                    a.get("投稿日", ""),            # D: 投稿日
                    a.get("引用元", ""),            # E: 引用元
                    cls["sentiment"],              # F: ポジネガ
                    cls["category"],               # G: カテゴリ
                ])

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

    # 取得
    g = get_google_news(KEYWORD)
    y = get_yahoo_news(KEYWORD)
    m = get_msn_news(KEYWORD)

    # 期間フィルタ + URL重複排除
    all_articles = []
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

    if all_articles:
        write_unified_sheet(all_articles, SPREADSHEET_ID, sheet_name)
    else:
        print("⚠️ 該当データがありませんでした。")


if __name__ == "__main__":
    main()
