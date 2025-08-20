import os
import re
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
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

# ----------------------------
# Google Sheets 認証
# ----------------------------
def get_gspread_client():
    creds_json = json.loads(GCP_SERVICE_ACCOUNT_KEY)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(credentials)

# ----------------------------
# Googleニュース取得
# ----------------------------
def fetch_google_news(keyword):
    url = f"https://news.google.com/rss/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "xml")
    items = []
    for item in soup.find_all("item"):
        title = item.title.text
        link = item.link.text
        pubdate = item.pubDate.text if item.pubDate else ""
        source = item.source.text if item.source else ""
        items.append(("Google", link, title, pubdate, source))
    return items

# ----------------------------
# MSNニュース取得
# ----------------------------
def fetch_msn_news(keyword):
    url = f"https://www.bing.com/news/search?q={keyword}&cc=jp"
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser")
    items = []
    for a in soup.select("a.title"):
        link = a.get("href")
        title = a.get_text().strip()
        pubdate = datetime.now().strftime("%Y/%m/%d %H:%M")
        source = "MSN"
        items.append(("MSN", link, title, pubdate, source))
    return items

# ----------------------------
# Yahooニュース取得＋コメント数
# ----------------------------
def fetch_yahoo_news(keyword):
    url = f"https://news.yahoo.co.jp/search?p={keyword}"
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser")
    items = []
    for a in soup.select("a.newsFeed_item_link"):
        link = a.get("href")
        title = a.get_text().strip()
        pubdate = datetime.now().strftime("%Y/%m/%d %H:%M")
        source = "Yahoo!ニュース"
        comment_count = fetch_yahoo_comment_count(link)
        items.append(("Yahoo", link, title, pubdate, source, comment_count))
    return items

# Yahooコメント数取得
def fetch_yahoo_comment_count(url):
    try:
        if "news.yahoo.co.jp" not in url:
            return 0
        res = requests.get(url)
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")
        span = soup.select_one("span.yjLnb_comment_count")
        if span:
            return int(span.get_text().replace(",", ""))
    except Exception:
        return 0
    return 0

# ----------------------------
# Gemini 分析
# ----------------------------
def analyze_titles_gemini(titles):
    if not GEMINI_API_KEY:
        return {}
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")

    prompt = """あなたは敏腕雑誌記者です。 以下のWebニュースのタイトルについて、
①ポジティブ/ネガティブ/ニュートラルを判別
②記事のカテゴリーを判別（会社・車・技術（EV/e-POWER/e-4ORCE/AD/ADAS/その他）・モータースポーツ・株式・政治・経済・スポーツ・その他）
出力は JSON 配列で、各要素が {"title": "...", "sentiment": "...", "category": "..."} という形式でお願いします。
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

# ----------------------------
# 集約シート作成
# ----------------------------
def build_daily_sheet(sh, all_items):
    JST = timezone(timedelta(hours=9))
    now = datetime.now(JST)
    start = datetime(now.year, now.month, now.day, 15, 0, tzinfo=JST) - timedelta(days=1)
    end = start + timedelta(days=1)
    sheet_name = now.strftime("%y%m%d")

    filtered = {"MSN": [], "Google": [], "Yahoo": []}
    no_date = 0

    for item in all_items:
        source = item[0]
        pubdate = item[3]
        try:
            dt = datetime.strptime(pubdate[:16], "%Y/%m/%d %H:%M")
        except Exception:
            no_date += 1
            continue
        dt = JST.localize(dt)
        if start <= dt < end:
            filtered[source].append(item)

    ws = None
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except:
        ws = sh.add_worksheet(title=sheet_name, rows="1000", cols="20")

    ws.update("A1:H1", [["ソース", "URL", "タイトル", "投稿日", "引用元", "コメント数", "ポジネガ", "カテゴリ"]])

    ordered = filtered["MSN"] + filtered["Google"] + filtered["Yahoo"]

    titles = [row[2] for row in ordered]
    gemini_results = analyze_titles_gemini(titles)

    rows = []
    for row in ordered:
        source, link, title, pubdate, origin = row[:5]
        comment = row[5] if len(row) > 5 else ""
        sentiment, category = ("", "")
        if title in gemini_results:
            sentiment, category = gemini_results[title]
        rows.append([source, link, title, pubdate, origin, comment, sentiment, category])

    if rows:
        ws.update(f"A2:H{len(rows)+1}", rows)

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
