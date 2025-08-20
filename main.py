# -*- coding: utf-8 -*-
import os
import re
import json
import time
import random
import requests
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from bs4 import BeautifulSoup
import gspread
import gspread.exceptions

# Selenium / Chrome
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# === Gemini ===
import google.generativeai as genai

# =======================
# 設定
# =======================
KEYWORD = os.environ.get("NEWS_KEYWORD", "日産")  # 必要に応じて Actions の env で上書き可
SPREADSHEET_ID = "1Vs4Cx8QPN4H2NOgtwaviOCe8zBTpUNDgJjqkHr51IZE"
JST = timezone(timedelta(hours=9))

# 出力列（A〜G）
OUTPUT_HEADERS = ["ソース", "タイトル", "URL", "投稿日", "引用元", "ポジネガ", "カテゴリ"]

# Gemini モデル名（速さ重視: 1.5-flash / 精度重視: 1.5-pro）
GEMINI_MODEL_NAME = os.environ.get("GEMINI_MODEL_NAME", "gemini-1.5-flash")

# =======================
# ユーティリティ
# =======================
def format_datetime(dt_obj: datetime) -> str:
    return dt_obj.astimezone(JST).strftime("%Y/%m/%d %H:%M")

def try_parse_jst_datetime(s: str):
    """ "YYYY/MM/DD HH:MM" などをJST datetimeに。失敗なら None """
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
    """ MSNなど相対表記を絶対(JST)へ。"""
    label = (pub_label or "").strip()
    try:
        m = re.search(r"(\d+)", label)
        n = int(m.group(1)) if m else None

        # 日本語/英語どちらもゆるく対応
        if ("分前" in label or "minute" in label) and n is not None:
            return format_datetime(base_time - timedelta(minutes=n))
        if ("時間前" in label or "hour" in label) and n is not None:
            return format_datetime(base_time - timedelta(hours=n))
        if ("日前" in label or "day" in label) and n is not None:
            return format_datetime(base_time - timedelta(days=n))

        # "8/20" のような表記
        m2 = re.match(r"(\d{1,2})/(\d{1,2})", label)
        if m2:
            month, day = int(m2.group(1)), int(m2.group(2))
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
    # 安定用：画像読み込みオフなどを入れたい場合はここに追加
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

# =======================
# スクレイパ
# =======================
def get_google_news(keyword: str) -> list[dict]:
    driver = setup_driver()
    url = f"https://news.google.com/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    driver.get(url)
    time.sleep(5)
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.2)
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

            # GoogleはUTCのISO表記
            iso = time_tag.get("datetime")
            dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).astimezone(JST)
            pub = format_datetime(dt)

            source_name = source_tag.get_text(strip=True) if source_tag else "Google"
            data.append({"ソース": "MSN" if False else "Google",  # 保険: 変な置換回避
                         "タイトル": title,
                         "URL": url,
                         "投稿日": pub,
                         "引用元": source_name})
        except Exception:
            continue
    # ソース名修正（上の変な保険を無効化）
    for d in data:
        d["ソース"] = "Google"
    print(f"✅ Googleニュース件数: {len(data)} 件")
    return data

def get_yahoo_news(keyword: str) -> list[dict]:
    """ Yahoo側のDOM変化に強い取り方：記事URLパターンで拾う """
    driver = setup_driver()
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data = []
    # Yahoo記事リンクは "https://news.yahoo.co.jp/articles/xxxxx" が基本
    links = soup.select("a[href^='https://news.yahoo.co.jp/articles/']")
    seen_urls = set()
    for a in links:
        try:
            title = a.get_text(strip=True)
            url = a.get("href")
            if not title or not url or url in seen_urls:
                continue
            seen_urls.add(url)

            parent_li = a.find_parent("li")
            # 投稿日
            date_str = "取得不可"
            if parent_li:
                time_tag = parent_li.find("time")
                if time_tag:
                    date_str = time_tag.get_text(strip=True)
                    date_str = re.sub(r"\([月火水木金土日]\)", "", date_str).strip()

            # 引用元（媒体名）
            source_name = "Yahoo"
            if parent_li:
                # 見出し周辺の短文テキストを拾う（媒体名候補）
                for s in parent_li.select("span, div"):
                    t = s.get_text(strip=True)
                    if t and 2 <= len(t) <= 20 and re.search(r"[ぁ-んァ-ン一-龥A-Za-z]", t) and "記事" not in t:
                        source_name = t
                        break

            data.append({"ソース": "Yahoo",
                         "タイトル": title,
                         "URL": url,
                         "投稿日": date_str or "取得不可",
                         "引用元": source_name})
        except Exception:
            continue

    print(f"✅ Yahoo!ニュース件数: {len(data)} 件")
    return data

def get_msn_news(keyword: str) -> list[dict]:
    now = datetime.now(JST)
    driver = setup_driver()
    # Bing News（新しい順）
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
            source_name = (card.get("data-author") or "").strip() or "MSN"

            pub_label = ""
            pub_tag = card.find("span", attrs={"aria-label": True})
            if pub_tag and pub_tag.has_attr("aria-label"):
                pub_label = pub_tag["aria-label"].strip()

            pub = parse_relative_time(pub_label, now)
            if pub == "取得不可" and url:
                pub = get_last_modified_datetime(url)

            if title and url:
                data.append({"ソース": "MSN",
                             "タイトル": title,
                             "URL": url,
                             "投稿日": pub,
                             "引用元": source_name})
        except Exception:
            continue
    print(f"✅ MSNニュース件数: {len(data)} 件")
    return data

# =======================
# 時間窓・シート名
# =======================
def compute_window(now_jst: datetime):
    """
    「前日15:00〜当日14:59」の集計窓とシート名(YYMMDD)を返す。
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

# =======================
# Google Sheets
# =======================
def service_account():
    """
    環境変数 GCP_SERVICE_ACCOUNT_KEY (JSON文字列) を優先。
    無ければ credentials.json を読む。
    """
    env_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY", "")
    if env_str:
        try:
            creds = json.loads(env_str)
            return gspread.service_account_from_dict(creds)
        except Exception as e:
            raise RuntimeError(f"サービスアカウントJSONの読み込みに失敗: {e}")
    else:
        return gspread.service_account(filename="credentials.json")

# =======================
# Gemini（タイトル分類）
# =======================
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
    titles の各タイトルに対し {"sentiment":..., "category":...} を返す dict を作る。
    失敗時は ニュートラル / その他。
    """
    model = init_gemini()
    default = {"sentiment": "ニュートラル", "category": "その他"}
    if not titles:
        return {}

    result_map = {}
    BATCH = 50
    for i in range(0, len(titles), BATCH):
        chunk = titles[i:i+BATCH]
        payload = {"titles": chunk}
        prompt = GEMINI_PROMPT + "\n入力タイトル一覧(JSON)：\n" + json.dumps(payload, ensure_ascii=False)
        try:
            resp = model.generate_content(prompt)
            text = (resp.text or "").strip()
            # JSON検出（コードブロック対策）
            m = re.search(r"\[.*\]", text, flags=re.DOTALL)
            json_str = m.group(0) if m else text
            data = json.loads(json_str)
            if isinstance(data, list):
                for item in data:
                    t = item.get("title", "")
                    sent = (item.get("sentiment", "") or "").strip() or default["sentiment"]
                    cat = (item.get("category", "") or "").strip() or default["category"]
                    result_map[t] = {"sentiment": sent, "category": cat}
            else:
                for t in chunk:
                    result_map[t] = default
        except Exception:
            for t in chunk:
                result_map[t] = default
        time.sleep(0.5)  # rate 対策

    return result_map

# =======================
# 書き込み
# =======================
def write_unified_sheet(articles: list[dict], spreadsheet_id: str, sheet_name: str):
    gc = service_account()

    # 5回までリトライ（429対策）
    for attempt in range(5):
        try:
            sh = gc.open_by_key(spreadsheet_id)
            try:
                ws = sh.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=sheet_name, rows="200", cols=str(len(OUTPUT_HEADERS)))
                ws.append_row(OUTPUT_HEADERS, value_input_option="USER_ENTERED")

            # 既存URLの重複回避
            existing = ws.get_all_values()
            existing_urls = set()
            if existing and len(existing) > 1:
                for row in existing[1:]:
                    if len(row) >= 3 and row[2]:
                        existing_urls.add(row[2])

            # === タイトル分類（Gemini） ===
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
                    a.get("ソース", ""),           # A: ソース (MSN/Google/Yahoo)
                    title,                         # B: タイトル
                    url,                           # C: URL
                    a.get("投稿日", ""),            # D: 投稿日 (JST)
                    a.get("引用元", ""),            # E: 引用元（媒体名）
                    cls["sentiment"],              # F: ポジネガ
                    cls["category"],               # G: カテゴリ
                ])

            if new_rows:
                ws.append_rows(new_rows, value_input_option="USER_ENTERED")
                print(f"✅ {len(new_rows)} 件を '{sheet_name}' に追記しました。")
            else:
                print("⚠️ 追記対象なし（重複 or 該当期間外）")

            return
        except gspread.exceptions.APIError as e:
            print(f"⚠️ Google API Error (attempt {attempt+1}/5): {e}")
            time.sleep(5 + random.random() * 5)

    raise RuntimeError("❌ スプレッドシート書き込みに失敗（5回試行）")

# =======================
# メイン
# =======================
def main():
    now_jst = datetime.now(JST)
    start, end, sheet_name = compute_window(now_jst)
    print(f"🔎 キーワード: {KEYWORD}")
    print(f"📅 収集ウィンドウ: {start.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end.strftime('%Y/%m/%d %H:%M:%S')} (JST)")
    print(f"🗂 出力シート名: {sheet_name}")

    # 取得（MSN→Google→Yahoo の順で後段の出力順も担保）
    m_list = get_msn_news(KEYWORD)
    g_list = get_google_news(KEYWORD)
    y_list = get_yahoo_news(KEYWORD)

    # 期間フィルタ + URL重複排除（順番は MSN → Google → Yahoo）
    all_articles = []
    seen = set()
    for src_list in [m_list, g_list, y_list]:  # 出力順固定
        for a in src_list:
            url = a.get("URL")
            if not url or url in seen:
                continue
            if a.get("投稿日") and in_window(a["投稿日"], start, end):
                all_articles.append(a)
                seen.add(url)

    print(f"🧮 期間該当件数: {len(all_articles)}")

    if all_articles:
        write_unified_sheet(all_articles, SPREADSHEET_ID, sheet_name)
    else:
        print("⚠️ 該当データがありませんでした。")

if __name__ == "__main__":
    main()
