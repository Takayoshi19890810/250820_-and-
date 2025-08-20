import os
import re
import json
import unicodedata
from datetime import datetime, timedelta, time as dtime
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import google.generativeai as genai

# ========= 環境変数 =========
NEWS_KEYWORD = os.getenv("NEWS_KEYWORD", "日産")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GCP_KEY = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# ========= Google Sheets 認証 =========
def get_spreadsheet(spreadsheet_id: str):
    creds = None
    if GCP_KEY:
        key_data = json.loads(GCP_KEY)
        creds = Credentials.from_service_account_info(
            key_data,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    else:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_KEY が未設定です")

    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id)

# ========= Google News =========
def fetch_google_news(keyword):
    url = f"https://news.google.com/rss/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    try:
        soup = BeautifulSoup(res.text, "lxml-xml")
    except Exception:
        soup = BeautifulSoup(res.text, "xml")

    items = []
    for item in soup.find_all("item"):
        title = (item.title.text or "").strip()
        link = (item.link.text or "").strip()
        pubdate_raw = item.pubDate.text.strip() if item.pubDate else ""
        source = item.source.text.strip() if item.source else "Googleニュース"
        items.append(("Google", link, title, pubdate_raw, source))
    return items

# ========= MSN News =========
def fetch_msn_news(keyword):
    url = f"https://www.bing.com/news/search?q={keyword}&format=RSS&cc=JP"
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    try:
        soup = BeautifulSoup(res.text, "lxml-xml")
    except Exception:
        soup = BeautifulSoup(res.text, "xml")

    items = []
    for item in soup.find_all("item"):
        title = (item.title.text or "").strip()
        link = (item.link.text or "").strip()
        pubdate_raw = item.pubDate.text.strip() if item.pubDate else ""
        source = item.source.text.strip() if item.source else "MSNニュース"
        items.append(("MSN", link, title, pubdate_raw, source))
    return items

# ========= Yahoo News =========
def fetch_yahoo_news(keyword):
    url = f"https://news.yahoo.co.jp/rss/search?p={keyword}"
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    try:
        soup = BeautifulSoup(res.text, "lxml-xml")
    except Exception:
        soup = BeautifulSoup(res.text, "xml")

    items = []
    for item in soup.find_all("item"):
        title = (item.title.text or "").strip()
        link = (item.link.text or "").strip()
        pubdate_raw = item.pubDate.text.strip() if item.pubDate else ""
        source = "Yahoo!ニュース"
        items.append(("Yahoo", link, title, pubdate_raw, source))
    return items

# ========= 日付フィルタ =========
def compute_window(now_jst: datetime):
    today = now_jst.date()
    today_1500 = datetime.combine(today, dtime(hour=15, minute=0))
    start = today_1500 - timedelta(days=1)
    end   = today_1500 - timedelta(seconds=1)
    label = today.strftime("%y%m%d")
    return start, end, label

def parse_pubdate(text: str):
    if not text:
        return None
    for fmt in [
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None

# ========= Gemini API =========
def analyze_titles_gemini(titles):
    if not GEMINI_API_KEY or not titles:
        return {}
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")

    prompt = """あなたは敏腕雑誌記者です。 以下のWebニュースのタイトルについて、
①ポジティブ/ネガティブ/ニュートラルを判別
②記事のカテゴリーを判別。ルール:
会社：企業の施策や生産、販売台数など。ニッサン/トヨタ/ホンダ/スバル/マツダ/スズキ/ミツビシ/ダイハツなら (会社名) を付記。他は その他。
車：クルマ名称が含まれる場合。新型/現行/旧型+名称を()付で。日産以外は 車（競合）。
技術（EV/e-POWER/e-4ORCE/AD/ADAS/その他）
モータースポーツ、株式、政治・経済、スポーツ、その他。
出力は JSON 配列 [{"title":"...","sentiment":"...","category":"..."}] の形式で。
タイトル一覧:
""" + "\n".join([f"- {t}" for t in titles])

    try:
        resp = model.generate_content(prompt)
        text = resp.text.strip()
        data = json.loads(text)
        return {d["title"]: (d["sentiment"], d["category"]) for d in data}
    except Exception as e:
        print(f"Gemini解析失敗: {e}")
        return {}

# ========= カテゴリー判定強化ロジック =========
def _norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")

BRAND_PATTERNS = [
    (re.compile(r"(日産|ニッサン|NISSAN)", re.IGNORECASE), "ニッサン"),
    (re.compile(r"(トヨタ|TOYOTA)", re.IGNORECASE), "トヨタ"),
    (re.compile(r"(ホンダ|HONDA)", re.IGNORECASE), "ホンダ"),
    (re.compile(r"(スバル|SUBARU)", re.IGNORECASE), "スバル"),
    (re.compile(r"(マツダ|MAZDA)", re.IGNORECASE), "マツダ"),
    (re.compile(r"(スズキ|SUZUKI)", re.IGNORECASE), "スズキ"),
    (re.compile(r"(三菱|ミツビシ|MITSUBISHI)", re.IGNORECASE), "ミツビシ"),
    (re.compile(r"(ダイハツ|DAIHATSU)", re.IGNORECASE), "ダイハツ"),
]
NISSAN_MODELS = ["リーフ","セレナ","スカイライン","フェアレディZ","ノート","オーラ","アリア","キックス","エクストレイル","ジューク","デイズ","ルークス","マーチ","ティアナ","シルビア","GT-R"]
GEN_PREFIXES = [("新型","新型"),("現行","現行"),("旧型","旧型"),("先代","旧型")]

def detect_brand_name(title: str):
    for pat, name in BRAND_PATTERNS:
        if pat.search(title):
            return name
    return None

def detect_nissan_model(title: str):
    t = _norm(title)
    for m in NISSAN_MODELS:
        if _norm(m) in t:
            return m
    return None

def build_car_category(title: str):
    model = detect_nissan_model(title)
    if model:
        prefix = ""
        for key, norm in GEN_PREFIXES:
            if key in title:
                prefix = norm
                break
        label = f"{prefix}{model}" if prefix else model
        return f"車（{label}）"
    return None

def build_company_category(title: str):
    brand = detect_brand_name(title)
    return f"会社（{brand if brand else 'その他'}）"

def normalize_category(title: str, gemini_cat: str):
    t = _norm(title)
    base = (gemini_cat or "").strip()
    if detect_nissan_model(title):
        return build_car_category(title)
    if detect_brand_name(title):
        return build_company_category(title)
    if "EV" in t or "電気自動車" in t:
        return "技術（EV）"
    if "e-POWER" in t or "イーパワー" in t:
        return "技術（e-POWER）"
    if "e-4ORCE" in t or "AWD" in t:
        return "技術（e-4ORCE）"
    if "自動運転" in t or "ADAS" in t:
        return "技術（AD/ADAS）"
    return base if base else "その他"

# ========= 集約 & 出力 =========
def build_daily_sheet(sh, all_items):
    now_jst = datetime.utcnow() + timedelta(hours=9)
    start, end, sheet_label = compute_window(now_jst)

    filtered = {"MSN": [], "Google": [], "Yahoo": []}
    no_date = 0
    for src, link, title, pubdate_raw, site in all_items:
        dt = parse_pubdate(pubdate_raw)
        if not dt:
            no_date += 1
            continue
        dt_jst = dt + timedelta(hours=9)
        if not (start <= dt_jst <= end):
            continue
        filtered[src].append((src, link, title, dt_jst.strftime("%Y/%m/%d %H:%M"), site))

    all_rows = filtered["MSN"] + filtered["Google"] + filtered["Yahoo"]

    print(f"🕒 集約期間: {start} 〜 {end} → シート名: {sheet_label}")
    print(f"📊 フィルタ結果: MSN={len(filtered['MSN'])}, Google={len(filtered['Google'])}, Yahoo={len(filtered['Yahoo'])}, 日付無しスキップ={no_date}")

    ws = None
    try:
        ws = sh.worksheet(sheet_label)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=sheet_label, rows="1000", cols="20")

    ws.update("A1", [["ソース","URL","タイトル","投稿日","引用元","コメント数","ポジネガ","カテゴリ"]])

    titles = [r[2] for r in all_rows]
    gemini_map = analyze_titles_gemini(titles)

    rows = []
    for src, link, title, date_str, site in all_rows:
        sentiment, category = gemini_map.get(title, ("", ""))
        category = normalize_category(title, category)
        rows.append([src, link, title, date_str, site, "", sentiment, category])

    if rows:
        ws.update(f"A2", rows)

    print(f"✅ 集約シート {sheet_label}: {len(rows)} 件")
    return sheet_label

# ========= メイン =========
def main():
    print(f"🔎 キーワード: {NEWS_KEYWORD}")
    print(f"📄 SPREADSHEET_ID: {SPREADSHEET_ID}")
    sh = get_spreadsheet(SPREADSHEET_ID)
    print(f"📘 Opened spreadsheet title: {sh.title}")

    google_items = fetch_google_news(NEWS_KEYWORD)
    yahoo_items = fetch_yahoo_news(NEWS_KEYWORD)
    msn_items = fetch_msn_news(NEWS_KEYWORD)

    print(f"--- 取得 ---")
    print(f"✅ Googleニュース: {len(google_items)} 件")
    print(f"✅ Yahoo!ニュース: {len(yahoo_items)} 件")
    print(f"✅ MSNニュース: {len(msn_items)} 件")

    all_items = google_items + yahoo_items + msn_items
    build_daily_sheet(sh, all_items)

if __name__ == "__main__":
    main()
