# -*- coding: utf-8 -*-
import os
import re
import json
import time
import unicodedata
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import gspread
from google.oauth2.service_account import Credentials
import google.generativeai as genai

# ========= 環境変数 =========
NEWS_KEYWORD = os.getenv("NEWS_KEYWORD", "日産")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GCP_KEY = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# ========= 共通 =========
JST = timezone(timedelta(hours=9))
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

def now_jst():
    return datetime.now(JST)

def fmt_jst(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y/%m/%d %H:%M")

def fetch_html(url: str, timeout: int = 20) -> str:
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.ok:
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
    except Exception:
        pass
    return ""

# ========= Google Sheets 認証 =========
def get_spreadsheet(spreadsheet_id: str):
    if not GCP_KEY:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_KEY が未設定です。")
    key_data = json.loads(GCP_KEY)
    creds = Credentials.from_service_account_info(
        key_data, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id)

# ========= 日付ウィンドウ（昨日15:00〜今日14:59 / シート名=今日のYYMMDD） =========
def compute_window():
    now = now_jst()
    today_1500 = now.replace(hour=15, minute=0, second=0, microsecond=0)
    start = today_1500 - timedelta(days=1)          # 昨日 15:00
    end = today_1500                                 # 今日 15:00（未満判定）
    sheet_name = now.strftime("%y%m%d")
    return start, end, sheet_name

def in_window_str(pub_str: str, start: datetime, end: datetime) -> bool:
    # pub_str は "YYYY/MM/DD HH:MM"
    try:
        dt = datetime.strptime(pub_str, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
        return start <= dt < end
    except Exception:
        return False

# ========= 記事ページから日時/タイトル/引用元抽出 =========
def try_parse_jst(dt_str: str):
    if not dt_str:
        return None
    patterns = [
        "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
        "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]
    for p in patterns:
        try:
            d = datetime.strptime(dt_str, p)
            if p.endswith("Z"):
                d = d.replace(tzinfo=timezone.utc).astimezone(JST)
            elif "%z" in p:
                d = d.astimezone(JST)
            else:
                d = d.replace(tzinfo=JST)
            return d
        except Exception:
            pass
    return None

def extract_datetime_from_article(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # JSON-LD
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if not isinstance(obj, dict):
                    continue
                for key in ("datePublished", "dateModified", "uploadDate"):
                    if obj.get(key):
                        dt = try_parse_jst(str(obj[key]).strip())
                        if dt:
                            return fmt_jst(dt)
        except Exception:
            continue
    # <time datetime="">
    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        dt = try_parse_jst(t["datetime"].strip())
        if dt:
            return fmt_jst(dt)
    # OGP/Meta
    for prop in ("article:published_time", "article:modified_time", "og:updated_time"):
        m = soup.find("meta", attrs={"property": prop, "content": True})
        if m and m.get("content"):
            dt = try_parse_jst(m["content"].strip())
            if dt:
                return fmt_jst(dt)
    return ""

def extract_yahoo_title_source(html: str) -> tuple[str, str]:
    title, source = "", "Yahoo!ニュース"
    if not html:
        return title, source
    soup = BeautifulSoup(html, "html.parser")
    # JSON-LD
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if isinstance(obj, dict):
                    if not title and obj.get("headline"):
                        title = str(obj["headline"]).strip()
                    pub = obj.get("publisher")
                    if pub and isinstance(pub, dict) and pub.get("name"):
                        source = str(pub["name"]).strip() or source
        except Exception:
            continue
    # Fallback
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
    if not title:
        og = soup.find("meta", attrs={"property": "og:title", "content": True})
        if og and og.get("content"):
            t = og["content"].strip()
            if t and t != "Yahoo!ニュース":
                title = t
    return title, source

def resolve_yahoo_article_url(html: str, fallback_url: str) -> str:
    if not html:
        return fallback_url
    soup = BeautifulSoup(html, "html.parser")
    can = soup.find("link", rel="canonical")
    if can and can.get("href"):
        href = can["href"]
        if "news.yahoo.co.jp/articles/" in href:
            return href
    a = soup.select_one('a[href*="news.yahoo.co.jp/articles/"]')
    if a and a.get("href"):
        return a["href"]
    return fallback_url

# ========= ニュース取得 =========
def fetch_google_news(keyword: str):
    url = f"https://news.google.com/rss/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    r = requests.get(url, headers=UA, timeout=20)
    r.raise_for_status()
    # XMLパーサ優先
    soup = None
    for parser in ("lxml-xml", "xml", "html.parser"):
        try:
            soup = BeautifulSoup(r.text, parser)
            if soup:
                break
        except Exception:
            soup = None
    if soup is None:
        return []
    items = []
    for it in soup.find_all("item"):
        title = (it.title.text if it.title else "").strip()
        link = (it.link.text if it.link else "").strip()
        source = (it.source.text if it.source else "Googleニュース").strip()
        pub = ""
        if it.pubDate and it.pubDate.text:
            try:
                dt = parsedate_to_datetime(it.pubDate.text.strip()).astimezone(JST)
                pub = fmt_jst(dt)
            except Exception:
                pub = ""
        if title and link:
            items.append(("Google", link, title, pub, source))
    return items

def fetch_msn_news(keyword: str):
    # Bing News RSS
    url = f"https://www.bing.com/news/search?q={keyword}&format=RSS&cc=JP"
    r = requests.get(url, headers=UA, timeout=20)
    r.raise_for_status()
    soup = None
    for parser in ("lxml-xml", "xml", "html.parser"):
        try:
            soup = BeautifulSoup(r.text, parser)
            if soup:
                break
        except Exception:
            soup = None
    if soup is None:
        return []
    items = []
    for it in soup.find_all("item"):
        title = (it.title.text if it.title else "").strip()
        link = (it.link.text if it.link else "").strip()
        source = (it.source.text if it.source else "MSNニュース").strip()
        pub = ""
        if it.pubDate and it.pubDate.text:
            try:
                dt = parsedate_to_datetime(it.pubDate.text.strip()).astimezone(JST)
                pub = fmt_jst(dt)
            except Exception:
                pub = fmt_jst(now_jst())
        if title and link:
            items.append(("MSN", link, title, pub, source))
    return items

def fetch_yahoo_news(keyword: str):
    # ★ RSSではなく検索HTMLから記事URLを抽出 → 記事ページで日時/タイトル/引用元を取得
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&ts=0&st=n&sr=1&sk=all"
    html = fetch_html(search_url)
    soup = BeautifulSoup(html, "html.parser")
    cand_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "news.yahoo.co.jp/articles/" in href or "news.yahoo.co.jp/pickup/" in href:
            if href.startswith("//"):
                href = "https:" + href
            if href.startswith("http"):
                cand_urls.append(href)
    # 重複除去
    seen, targets = set(), []
    for u in cand_urls:
        if u not in seen:
            seen.add(u)
            targets.append(u)

    items = []
    for u in targets:
        try:
            html0 = fetch_html(u)
            art_url = resolve_yahoo_article_url(html0, u)
            # pickup で記事URLが解決できなければスキップ
            if "news.yahoo.co.jp/pickup/" in art_url and art_url == u:
                continue
            html1 = html0 if art_url == u else fetch_html(art_url)
            if not html1:
                continue

            title, source = extract_yahoo_title_source(html1)
            if not title or title == "Yahoo!ニュース":
                og = BeautifulSoup(html1, "html.parser").find("meta", attrs={"property": "og:title", "content": True})
                if og and og.get("content"):
                    t = og["content"].strip()
                    if t and t != "Yahoo!ニュース":
                        title = t

            pub = extract_datetime_from_article(html1) or fmt_jst(now_jst())

            items.append(("Yahoo", art_url, title, pub, source))
            time.sleep(0.2)
        except Exception:
            continue
    return items

# ========= カテゴリー判定（強化版） =========
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
NISSAN_MODELS = [
    "リーフ","セレナ","スカイライン","フェアレディZ","ノート","オーラ","アリア","キックス",
    "エクストレイル","ジューク","デイズ","ルークス","マーチ","ティアナ","シルビア","GT-R","サクラ","キャラバン","パトロール","フロンティア"
]
RIVAL_BRANDS = {
    "トヨタ": ["クラウン","プリウス","カローラ","ヤリス","アクア","アルファード","ヴェルファイア","ハリアー","RAV4","GR86","スープラ","ランドクルーザー"],
    "ホンダ": ["シビック","フィット","ヴェゼル","N-BOX","ステップワゴン","アコード","ZR-V","NSX","インテグラ"],
    "スバル": ["レヴォーグ","フォレスター","アウトバック","インプレッサ","BRZ","ソルテラ"],
    "マツダ": ["ロードスター","CX-5","CX-3","CX-30","MAZDA3","アテンザ","デミオ","RX-7","RX-8"],
    "スズキ": ["スイフト","ソリオ","ハスラー","ジムニー","アルト","ワゴンR","スペーシア"],
    "ミツビシ": ["アウトランダー","デリカ","エクリプスクロス","RVR","パジェロ"],
    "ダイハツ": ["タント","ムーヴ","ミライース","タフト","ロッキー","コペン"]
}
GEN_PREFIXES = [("新型","新型"), ("現行","現行"), ("旧型","旧型"), ("先代","旧型")]

TECH_KEYS = {
    "ev": ["EV","電気自動車","BEV","バッテリー","急速充電","充電網","充電スタンド","充電器","航続距離","LFP","NCM"],
    "epower": ["e-POWER","ePOWER","イーパワー"],
    "e4orce": ["e-4ORCE","e4ORCE","4ORCE","4WD","AWD","2WD","四輪駆動"],
    "adas": ["自動運転","レベル2","レベル3","ADAS","先進運転支援","プロパイロット","ACC","レーンキープ","自動駐車"],
}
MOTORSPORT_KEYS = ["F1","フォーミュラE","Formula E","WRC","ラリー","SUPER GT","スーパーGT","ル・マン","ルマン","耐久レース"]
COMPANY_KEYS = ["販売台数","販売", "生産", "工場", "生産停止", "停止", "出荷", "雇用", "人員", "リコール", "提携", "統合", "出資", "投資", "再建", "撤退", "サプライヤー", "受注", "能力増強"]
STOCK_KEYS = ["株","株価","上場","IPO","自社株買い","決算","通期","四半期","増収","減益","上方修正","下方修正","業績","見通し"]
POLICY_KEYS = ["首相","内閣","大臣","選挙","税","予算","規制","補助金","関税","日銀","景気","経済対策","為替","インフレ","GDP","財政"]
SPORTS_KEYS = ["野球","サッカー","Jリーグ","MLB","W杯","ワールドカップ","バレーボール","バスケット","NBA","高校野球"]

def contains_any(t: str, keys: list[str]) -> bool:
    T = _norm(t)
    return any(k in T for k in keys)

def detect_brand_name(title: str) -> str|None:
    for pat, name in BRAND_PATTERNS:
        if pat.search(title):
            return name
    return None

def detect_nissan_model(title: str) -> str|None:
    T = _norm(title)
    for m in NISSAN_MODELS:
        if _norm(m) in T:
            return m
    return None

def detect_rival_model(title: str) -> bool:
    T = _norm(title)
    for brand, models in RIVAL_BRANDS.items():
        if brand in T and any(_norm(m) in T for m in models):
            return True
    modelish = any(k in T for k in ["新型","モデル","グレード","発表","発売","SUV","セダン","ハッチバック","クーペ","ミニバン"])
    return modelish and not ("日産" in T or "ニッサン" in T or "NISSAN" in T)

def build_car_category(title: str) -> str|None:
    model = detect_nissan_model(title)
    if model:
        prefix = ""
        for key, norm in GEN_PREFIXES:
            if key in title:
                prefix = norm
                break
        label = f"{prefix}{model}" if prefix else model
        return f"車（{label}）"
    if detect_rival_model(title):
        return "車（競合）"
    return None

def build_company_category(title: str) -> str:
    brand = detect_brand_name(title)
    return f"会社（{brand if brand else 'その他'}）"

def normalize_category(title: str, gemini_cat: str) -> str:
    t = _norm(title)
    base = (gemini_cat or "").strip()

    # 技術細目を最優先
    if contains_any(t, TECH_KEYS["epower"]):
        return "技術（e-POWER）"
    if contains_any(t, TECH_KEYS["e4orce"]):
        return "技術（e-4ORCE）"
    if contains_any(t, TECH_KEYS["adas"]):
        return "技術（AD/ADAS）"
    if contains_any(t, TECH_KEYS["ev"]):
        if contains_any(t, COMPANY_KEYS):
            return build_company_category(title)
        return "技術（EV）"

    if contains_any(t, MOTORSPORT_KEYS):
        return "モータースポーツ"
    if contains_any(t, STOCK_KEYS):
        return "株式"
    if contains_any(t, POLICY_KEYS):
        return "政治・経済"
    if contains_any(t, SPORTS_KEYS):
        return "スポーツ"

    car_cat = build_car_category(title)
    if car_cat:
        return car_cat

    if contains_any(t, COMPANY_KEYS) or detect_brand_name(title):
        return build_company_category(title)

    if base.startswith("車"):
        car_cat = build_car_category(title)
        if car_cat:
            return car_cat
        return "車（競合）"
    if base in ["会社","企業"]:
        return build_company_category(title)

    return "その他"

# ========= ポジネガ（Gemini＋フォールバック） =========
def _heuristic_sentiment(title: str) -> str:
    neg_kw = ["停止","終了","撤退","不祥事","下落","否定","炎上","事故","問題","破談","人員削減","雇用不安","閉鎖","減産"]
    pos_kw = ["発表","受賞","好調","上昇","登場","公開","新型","強化","受注","発売","ラインナップ","増加"]
    if any(k in title for k in neg_kw):
        return "ネガティブ"
    if any(k in title for k in pos_kw):
        return "ポジティブ"
    return "ニュートラル"

def classify_titles_gemini_batched(titles: list[str], batch_size: int = 80) -> list[tuple[str, str]]:
    if not titles:
        return []
    if not GEMINI_API_KEY:
        out = []
        for t in titles:
            s = _heuristic_sentiment(t)
            c = normalize_category(t, "その他")
            out.append((s, c))
        return out

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        "gemini-1.5-flash",
        generation_config={"response_mime_type": "application/json"}
    )

    out = [("", "")] * len(titles)
    for start_idx in range(0, len(titles), batch_size):
        batch = titles[start_idx:start_idx+batch_size]
        payload = [{"row": start_idx+i, "title": t} for i, t in enumerate(batch)]
        sys_prompt = (
            "あなたは敏腕雑誌記者です。各タイトルについて以下を判定し、"
            "JSON配列のみで返してください。各要素は "
            '{"row": 数値, "sentiment": "ポジティブ|ネガティブ|ニュートラル", '
            '"category": "会社|車|車（競合）|技術（EV）|技術（e-POWER）|技術（e-4ORCE）|'
            '技術（AD/ADAS）|技術|モータースポーツ|株式|政治・経済|スポーツ|その他"}。'
            "タイトルは改変しない。カテゴリは単一。"
        )
        try:
            resp = model.generate_content([
                sys_prompt,
                {"mime_type": "application/json", "text": json.dumps(payload, ensure_ascii=False)}
            ])
            text = (getattr(resp, "text", "") or "").strip()
            arr = None
            try:
                if text:
                    arr = json.loads(text)
            except Exception:
                # ガード：本文に前後説明が混ざった時
                s = text.find("[")
                e = text.rfind("]")
                if s != -1 and e != -1 and e > s:
                    try:
                        arr = json.loads(text[s:e+1])
                    except Exception:
                        arr = None
            if isinstance(arr, dict):
                arr = [arr]
            if isinstance(arr, list):
                for obj in arr:
                    try:
                        idx = int(obj.get("row"))
                        s = str(obj.get("sentiment", "")).strip()
                        c = str(obj.get("category", "")).strip()
                        if 0 <= idx < len(out):
                            out[idx] = (s, c)
                    except Exception:
                        continue
            # 正規化
            for i in range(start_idx, start_idx+len(batch)):
                s, c = out[i]
                s = s or _heuristic_sentiment(titles[i])
                c = normalize_category(titles[i], c or "その他")
                out[i] = (s, c)
        except Exception as e:
            print(f"Geminiバッチ失敗: {e}")
            for i in range(start_idx, start_idx+len(batch)):
                s = _heuristic_sentiment(titles[i])
                c = normalize_category(titles[i], "その他")
                out[i] = (s, c)
        time.sleep(0.2)
    return out

# ========= 集約 & 出力 =========
def build_daily_sheet(sh, msn_items, google_items, yahoo_items):
    start, end, sheet_name = compute_window()

    msn_f    = [x for x in msn_items    if x[3] and in_window_str(x[3], start, end)]
    google_f = [x for x in google_items if x[3] and in_window_str(x[3], start, end)]
    yahoo_f  = [x for x in yahoo_items  if x[3] and in_window_str(x[3], start, end)]

    print(f"📊 フィルタ結果: MSN={len(msn_f)}, Google={len(google_f)}, Yahoo={len(yahoo_f)}")
    ordered = msn_f + google_f + yahoo_f  # 並び：MSN→Google→Yahoo

    # タイトル一括分類
    titles = [row[2] for row in ordered]
    senti_cate = classify_titles_gemini_batched(titles)

    # シート
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows="5000", cols="10")

    headers = ["ソース", "URL", "タイトル", "投稿日", "引用元", "コメント数", "ポジネガ", "カテゴリ"]
    ws.update("A1:H1", [headers])

    rows = []
    for i, row in enumerate(ordered):
        src, url, title, pub, origin = row
        sentiment, category = senti_cate[i] if i < len(senti_cate) else ("", "")
        rows.append([src, url, title, pub, origin, "", sentiment, category])

    if rows:
        ws.update(f"A2:H{len(rows)+1}", rows)

    print(f"🕒 集約期間: {start.strftime('%Y/%m/%d %H:%M')} 〜 {(end - timedelta(minutes=1)).strftime('%Y/%m/%d %H:%M')} → シート名: {sheet_name}")
    print(f"✅ 集約シート {sheet_name}: {len(rows)} 件")
    return sheet_name

# ========= メイン =========
def main():
    print(f"🔎 キーワード: {NEWS_KEYWORD}")
    print(f"📄 SPREADSHEET_ID: {SPREADSHEET_ID}")
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID が未設定です。")
    sh = get_spreadsheet(SPREADSHEET_ID)
    print(f"📘 Opened spreadsheet title: {sh.title}")

    print("\n--- 取得 ---")
    google_items = fetch_google_news(NEWS_KEYWORD)
    yahoo_items  = fetch_yahoo_news(NEWS_KEYWORD)   # ← 404対策版（HTML→記事解析）
    msn_items    = fetch_msn_news(NEWS_KEYWORD)

    print(f"✅ Googleニュース: {len(google_items)} 件（投稿日取得 {sum(1 for i in google_items if i[3])} 件）")
    print(f"✅ Yahoo!ニュース: {len(yahoo_items)} 件（投稿日取得 {sum(1 for i in yahoo_items if i[3])} 件）")
    print(f"✅ MSNニュース: {len(msn_items)} 件（投稿日取得/推定 {sum(1 for i in msn_items if i[3])} 件）")

    print("\n--- 集約（まとめシートのみ / A列=ソース / 順=MSN→Google→Yahoo） ---")
    build_daily_sheet(sh, msn_items, google_items, yahoo_items)

if __name__ == "__main__":
    main()
