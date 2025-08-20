import os
import re
import json
import time
import requests
import datetime
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from openai import OpenAI

# ========= 設定 ==========
NEWS_KEYWORD = os.getenv("NEWS_KEYWORD", "日産")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GCP_SERVICE_ACCOUNT_KEY = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# ========= Google Sheets 認証 ==========
def get_gspread_client():
    keyfile_dict = json.loads(GCP_SERVICE_ACCOUNT_KEY)
    creds = Credentials.from_service_account_info(
        keyfile_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

# ========= Yahooコメント数 ==========
def get_yahoo_comment_count(url: str) -> int:
    try:
        if "news.yahoo.co.jp" not in url:
            return 0
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        html = res.text
        m = re.search(r'コメント数.*?([0-9,]+)', html)
        if m:
            return int(m.group(1).replace(",", ""))
        # 予備: data 属性に含まれるパターン
        m = re.search(r'"commentCount":\s*([0-9]+)', html)
        if m:
            return int(m.group(1))
    except Exception:
        return 0
    return 0

# ========= ニュース取得 ==========
def fetch_google_news(keyword):
    url = f"https://news.google.com/rss/search?q={keyword}+when:1d&hl=ja&gl=JP&ceid=JP:ja"
    res = requests.get(url)
    soup = BeautifulSoup(res.content, "xml")
    items = []
    for item in soup.find_all("item"):
        title = item.title.text
        link = item.link.text
        pub_date = item.pubDate.text if item.pubDate else ""
        source = item.source.text if item.source else ""
        # pubDate を日付整形
        try:
            dt = datetime.datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z")
            pub_date = dt.strftime("%Y/%m/%d %H:%M")
        except Exception:
            pass
        items.append({
            "source": "Google",
            "url": link,
            "title": title,
            "date": pub_date,
            "origin": source,
            "comment": 0
        })
    return items

def fetch_yahoo_news(keyword):
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    items = []
    for a in soup.select("a.newsFeed_item_link"):
        title = a.get("title")
        link = a.get("href")
        if not title or not link:
            continue
        date = ""
        origin = "Yahoo!ニュース"
        # コメント数
        comment_count = get_yahoo_comment_count(link)
        items.append({
            "source": "Yahoo",
            "url": link,
            "title": title,
            "date": date,
            "origin": origin,
            "comment": comment_count
        })
    return items

def fetch_msn_news(keyword):
    url = f"https://www.bing.com/news/search?q={keyword}&qft=interval%3d%227%22&form=YFNR"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    items = []
    for a in soup.select("a.title"):
        title = a.text.strip()
        link = a.get("href")
        if not title or not link:
            continue
        date = ""
        origin = "MSN"
        items.append({
            "source": "MSN",
            "url": link,
            "title": title,
            "date": date,
            "origin": origin,
            "comment": 0
        })
    return items

# ========= Gemini 分析 ==========
def analyze_titles_gemini(titles):
    client = OpenAI(api_key=GEMINI_API_KEY, base_url="https://generativelanguage.googleapis.com/v1beta/openai/")
    prompt = """あなたは敏腕雑誌記者です。 以下のWebニュースのタイトルについて、
①ポジティブ/ネガティブ/ニュートラルを判別
②記事のカテゴリーを判別（会社・車・技術（EV/e-POWER/e-4ORCE/AD/ADAS/その他）・モータースポーツ・株式・政治・経済・スポーツ・その他）
出力は JSON 配列で、各要素が {"title": "...", "sentiment": "...", "category": "..."} という形式でお願いします。
タイトル一覧:
""" + "\n".join([f"- {t}" for t in titles])
    resp = client.chat.completions.create(
        model="gemini-1.5-flash",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    text = resp.choices[0].message.content
    try:
        data = json.loads(text)
        return {d["title"]: (d["sentiment"], d["category"]) for d in data}
    except Exception:
        return {}

# ========= 集約してシートに出力 ==========
def build_daily_sheet(sh, items):
    today = datetime.datetime.now()
    start = (today - datetime.timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    end = today.replace(hour=14, minute=59, second=0, microsecond=0)
    sheet_name = today.strftime("%y%m%d")

    filtered = {"MSN": [], "Google": [], "Yahoo": []}
    no_date = 0
    for it in items:
        try:
            if it["date"]:
                dt = datetime.datetime.strptime(it["date"], "%Y/%m/%d %H:%M")
                if start <= dt <= end:
                    filtered[it["source"]].append(it)
            else:
                no_date += 1
        except Exception:
            no_date += 1

    # Gemini 分析
    titles = [it["title"] for group in filtered.values() for it in group]
    results = analyze_titles_gemini(titles)

    try:
        ws = sh.worksheet(sheet_name)
        sh.del_worksheet(ws)
    except Exception:
        pass
    ws = sh.add_worksheet(title=sheet_name, rows="1000", cols="8")

    headers = ["ソース", "URL", "タイトル", "投稿日", "引用元", "コメント数", "ポジネガ", "カテゴリ"]
    ws.update("A1:H1", [headers])

    rows = []
    for src in ["MSN", "Google", "Yahoo"]:
        for it in filtered[src]:
            sentiment, category = results.get(it["title"], ("", ""))
            rows.append([
                it["source"], it["url"], it["title"], it["date"],
                it["origin"], it["comment"], sentiment, category
            ])

    if rows:
        ws.update("A2", rows)

    print(f"🕒 集約期間: {start.strftime('%Y/%m/%d %H:%M')} 〜 {end.strftime('%Y/%m/%d %H:%M')} → シート名: {sheet_name}")
    print(f"📊 フィルタ結果: MSN={len(filtered['MSN'])}, Google={len(filtered['Google'])}, Yahoo={len(filtered['Yahoo'])}, 日付無しスキップ={no_date}")
    print(f"✅ 集約シート {sheet_name}: {len(rows)} 件")
    return sheet_name

# ========= Main ==========
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
    print(f"✅ Googleニュース: {len(google_items)} 件（投稿日取得 {sum(1 for i in google_items if i['date'])} 件）")
    print(f"✅ Yahoo!ニュース: {len(yahoo_items)} 件（投稿日取得 {sum(1 for i in yahoo_items if i['date'])} 件）")
    print(f"✅ MSNニュース: {len(msn_items)} 件（投稿日取得/推定 {sum(1 for i in msn_items if i['date'])} 件）")

    all_items = google_items + yahoo_items + msn_items
    build_daily_sheet(sh, all_items)

if __name__ == "__main__":
    main()
