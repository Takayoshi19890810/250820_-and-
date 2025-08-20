import os
import re
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import gspread
from google.oauth2.service_account import Credentials
import google.generativeai as genai

# ----------------------------
# 環境変数
# ----------------------------
NEWS_KEYWORD = os.environ.get("NEWS_KEYWORD", "日産")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_SERVICE_ACCOUNT_KEY = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

JST = timezone(timedelta(hours=9))

def now_jst():
    return datetime.now(JST)

def fmt_jst(dt: datetime) -> str:
    """JSTの日時を 'YYYY/MM/DD HH:MM' に整形"""
    return dt.astimezone(JST).strftime("%Y/%m/%d %H:%M")

# ----------------------------
# Google Sheets 認証
# ----------------------------
def get_gspread_client():
    creds_json = json.loads(GCP_SERVICE_ACCOUNT_KEY)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(credentials)

# ----------------------------
# Googleニュース取得 (RSS)
# ----------------------------
def fetch_google_news(keyword):
    # lxml を使うため parser="xml" を指定
    url = f"https://news.google.com/rss/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "xml")  # ← lxml が必要
    items = []
    for item in soup.find_all("item"):
        title = (item.title.text or "").strip()
        link = (item.link.text or "").strip()
        pubdate_raw = item.pubDate.text.strip() if item.pubDate else ""
        source = item.source.text.strip() if item.source else "Google"

        # pubDate を JST に変換
        pub_jst = ""
        if pubdate_raw:
            try:
                dt_aware = parsedate_to_datetime(pubdate_raw)  # 例: Tue, 20 Aug 2025 09:40:00 GMT
                pub_jst = fmt_jst(dt_aware)
            except Exception:
                pub_jst = ""

        items.append(("Google", link, title, pub_jst, source))
    return items

# ----------------------------
# MSNニュース取得（簡易スクレイプ）
# ----------------------------
def fetch_msn_news(keyword):
    url = f"https://www.bing.com/news/search?q={keyword}&cc=jp"
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    items = []
    # タイトルリンク（見出し）を拾う
    for a in soup.select("a.title, h2 a, h3 a"):
        link = a.get("href") or ""
        title = a.get_text(strip=True)
        if not title or not link:
            continue
        pub_jst = fmt_jst(now_jst())  # 取得時刻（MSN側は時刻の安定抽出が難しいため）
        source = "MSN"
        items.append(("MSN", link, title, pub_jst, source))
    return items

# ----------------------------
# Yahooニュース取得＋コメント数
# ----------------------------
YAHOO_COMMENT_COUNT_RE = re.compile(r"コメント[（(]\s*([0-9,]+)\s*[)）]")

def fetch_yahoo_comment_count(url):
    """記事ページから 'コメント（N）' を抽出。見つからない場合は 0。"""
    if "news.yahoo.co.jp" not in url:
        return 0
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        text = BeautifulSoup(res.text, "html.parser").get_text(" ", strip=True)
        m = YAHOO_COMMENT_COUNT_RE.search(text)
        if m:
            return int(m.group(1).replace(",", ""))
    except Exception:
        pass
    return 0

def fetch_yahoo_news(keyword):
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8"
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    items = []
    # 検索結果のカード
    for a in soup.select("a.newsFeed_item_link"):
        link = a.get("href") or ""
        title = a.get("title") or a.get_text(strip=True)
        if not title or not link:
            continue
        pub_jst = fmt_jst(now_jst())  # 検索面からは時刻が取りにくいので暫定で取得時刻
        source = "Yahoo!ニュース"
        comment_count = fetch_yahoo_comment_count(link)
        items.append(("Yahoo", link, title, pub_jst, source, comment_count))
    return items

# ----------------------------
# Gemini 分析（タイトル → ポジネガ & カテゴリ）
# ----------------------------
def analyze_titles_gemini(titles):
    if not GEMINI_API_KEY or not titles:
        return {}
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")

    prompt = """あなたは敏腕雑誌記者です。 以下のWebニュースのタイトルについて、
①ポジティブ/ネガティブ/ニュートラルを判別
②記事のカテゴリーを判別（会社、車、車（競合）、技術（EV）、技術（e-POWER）、技術（e-4ORCE）、技術（AD/ADAS）、技術、モータースポーツ、株式、政治・経済、スポーツ、その他）
出力は JSON 配列で、各要素が {"title": "...", "sentiment": "ポジティブ|ネガティブ|ニュートラル", "category": "..."} という形式でお願いします。
タイトル一覧:
""" + "\n".join([f"- {t}" for t in titles])

    try:
        resp = model.generate_content(prompt)
        text = (resp.text or "").strip()
        data = json.loads(text)
        return {d["title"]: (d.get("sentiment",""), d.get("category","")) for d in data if isinstance(d, dict) and "title" in d}
    except Exception as e:
        print(f"Gemini解析失敗: {e}")
        return {}

# ----------------------------
# 集約シート作成（昨日15:00〜今日14:59, シート名=今日のYYMMDD）
# ----------------------------
def build_daily_sheet(sh, all_items):
    now = now_jst()
    today_1500 = now.replace(hour=15, minute=0, second=0, microsecond=0)
    start = today_1500 - timedelta(days=1)    # 昨日15:00
    end = today_1500                          # 今日14:59:59まで（< end で判定）
    sheet_name = now.strftime("%y%m%d")

    filtered = {"MSN": [], "Google": [], "Yahoo": []}
    no_date = 0

    for item in all_items:
        source = item[0]
        pub_str = item[3]  # "YYYY/MM/DD HH:MM" 期待
        try:
            dt = datetime.strptime(pub_str, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
            if start <= dt < end:
                filtered[source].append(item)
        except Exception:
            no_date += 1

    # 既存シートはクリア、なければ作成
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows="2000", cols="10")

    # ヘッダー
    ws.update("A1:H1", [["ソース", "URL", "タイトル", "投稿日", "引用元", "コメント数", "ポジネガ", "カテゴリ"]])

    # 並び順：MSN → Google → Yahoo
    ordered = filtered["MSN"] + filtered["Google"] + filtered["Yahoo"]

    # タイトルをまとめて Gemini へ
    titles = [row[2] for row in ordered]
    gemini_map = analyze_titles_gemini(titles)

    rows = []
    for row in ordered:
        source, link, title, pubdate, origin = row[:5]
        comment = row[5] if len(row) > 5 else ""
        senti, cate = gemini_map.get(title, ("", ""))
        rows.append([source, link, title, pubdate, origin, comment, senti, cate])

    if rows:
        ws.update(f"A2:H{len(rows)+1}", rows)

    print(f"🕒 集約期間: {start.strftime('%Y/%m/%d %H:%M')} 〜 {(end - timedelta(minutes=1)).strftime('%Y/%m/%d %H:%M')} → シート名: {sheet_name}")
    print(f"📊 フィルタ結果: MSN={len(filtered['MSN'])}, Google={len(filtered['Google'])}, Yahoo={len(filtered['Yahoo'])}, 日付無しスキップ={no_date}")
    print(f"✅ 集約シート {sheet_name}: {len(rows)} 件")
    return sheet_name

# ----------------------------
# Main
# ----------------------------
def main():
    print(f"🔎 キーワード: {NEWS_KEYWORD}")
    print(f"📄 SPREADSHEET_ID: {SPREADSHEET_ID}")

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    print(f"📘 Opened spreadsheet title: {sh.title}")

    print("\n--- 取得 ---")
    google_items = fetch_google_news(NEWS_KEYWORD)
    yahoo_items = fetch_yahoo_news(NEWS_KEYWORD)
    msn_items = fetch_msn_news(NEWS_KEYWORD)

    print(f"✅ Googleニュース: {len(google_items)} 件（投稿日取得 {sum(1 for i in google_items if i[3])} 件）")
    print(f"✅ Yahoo!ニュース: {len(yahoo_items)} 件（投稿日取得 {sum(1 for i in yahoo_items if i[3])} 件）")
    print(f"✅ MSNニュース: {len(msn_items)} 件（投稿日取得/推定 {sum(1 for i in msn_items if i[3])} 件）")

    all_items = google_items + yahoo_items + msn_items

    print("\n--- 集約（まとめシートのみ / A列=ソース / 順=MSN→Google→Yahoo） ---")
    build_daily_sheet(sh, all_items)

if __name__ == "__main__":
    main()
