# -*- coding: utf-8 -*-
import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import gspread
from google.oauth2.service_account import Credentials
import google.generativeai as genai

# ========= 設定 =========
NEWS_KEYWORD = os.environ.get("NEWS_KEYWORD", "日産")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")  # 必須
GCP_SERVICE_ACCOUNT_KEY = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")  # 必須(JSON)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")  # 任意（未設定なら分類スキップ）

JST = timezone(timedelta(hours=9))

def now_jst() -> datetime:
    return datetime.now(JST)

def fmt_jst(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y/%m/%d %H:%M")

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ========= Google Sheets 認証 =========
def get_gspread_client():
    if not GCP_SERVICE_ACCOUNT_KEY:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_KEY が未設定です。")
    creds_json = json.loads(GCP_SERVICE_ACCOUNT_KEY)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(credentials)

# ========= 共通：HTML取得 =========
def fetch_html(url: str, timeout: int = 15) -> str:
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.ok:
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
    except Exception:
        pass
    return ""

# ========= 日付抽出 =========
def try_parse_jst(dt_str: str):
    pats = [
        "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
        "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f%z",
    ]
    for p in pats:
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
    # <time datetime>
    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        dt = try_parse_jst(t["datetime"].strip())
        if dt:
            return fmt_jst(dt)
    # OGP
    for prop in ("article:published_time", "article:modified_time", "og:updated_time"):
        m = soup.find("meta", attrs={"property": prop, "content": True})
        if m and m.get("content"):
            dt = try_parse_jst(m["content"].strip())
            if dt:
                return fmt_jst(dt)
    return ""

# ========= Googleニュース（RSS） =========
def fetch_google_news(keyword: str):
    url = f"https://news.google.com/rss/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    r = requests.get(url, headers=UA, timeout=15)
    r.raise_for_status()
    # lxml-xml → xml → html.parser の順でフォールバック
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
        src = (it.source.text if it.source else "Google").strip()
        pub = ""
        if it.pubDate and it.pubDate.text:
            try:
                dt = parsedate_to_datetime(it.pubDate.text.strip())
                pub = fmt_jst(dt)
            except Exception:
                pub = ""
        if title and link:
            items.append(("Google", link, title, pub, src))
    return items

# ========= MSNニュース（簡易スクレイプ） =========
def fetch_msn_news(keyword: str):
    url = f"https://www.bing.com/news/search?q={keyword}&cc=jp"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.select("a.title, h2 a, h3 a"):
        link = a.get("href") or ""
        title = a.get_text(strip=True)
        if not title or not link:
            continue
        pub = fmt_jst(now_jst())  # 取得時刻
        src = "MSN"
        items.append(("MSN", link, title, pub, src))
    return items

# ========= Yahooニュース（検索→記事抽出）＋コメント数 =========
YAHOO_COMMENT_RE = re.compile(r"コメント[（(]\s*([0-9,]+)\s*[)）]")
def resolve_yahoo_article_url(html: str, fallback_url: str) -> str:
    if not html:
        return fallback_url
    soup = BeautifulSoup(html, "html.parser")
    # canonical
    can = soup.find("link", rel="canonical")
    if can and can.get("href"):
        href = can["href"]
        if "news.yahoo.co.jp/articles/" in href:
            return href
    a = soup.select_one('a[href*="news.yahoo.co.jp/articles/"]')
    if a and a.get("href"):
        return a["href"]
    return fallback_url

def extract_yahoo_title_source(html: str) -> tuple[str, str]:
    title, source = "", "Yahoo"
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
                        source = str(pub["name"]).strip() or "Yahoo"
        except Exception:
            continue
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
    if source == "Yahoo":
        m = soup.find("meta", attrs={"name": "source", "content": True})
        if m and m.get("content"):
            source = m["content"].strip() or "Yahoo"
    return title, source

def extract_yahoo_comment_count(html: str) -> int:
    if not html:
        return 0
    # 1) JSON-LDに commentCount がある場合
    try:
        for tag in BeautifulSoup(html, "html.parser").find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(tag.string or "{}")
                objs = data if isinstance(data, list) else [data]
                for obj in objs:
                    if isinstance(obj, dict):
                        if "commentCount" in obj and str(obj["commentCount"]).isdigit():
                            return int(obj["commentCount"])
                        # InteractionStatistic 経由
                        stats = obj.get("interactionStatistic")
                        if isinstance(stats, list):
                            for st in stats:
                                if isinstance(st, dict) and str(st.get("interactionType","")).lower().find("comment") >= 0:
                                    val = st.get("userInteractionCount")
                                    if isinstance(val, int):
                                        return val
            except Exception:
                continue
    except Exception:
        pass
    # 2) テキストから 「コメント（N）」 を抽出
    try:
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        m = YAHOO_COMMENT_RE.search(text)
        if m:
            return int(m.group(1).replace(",", ""))
    except Exception:
        pass
    return 0

def fetch_yahoo_news(keyword: str):
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&ts=0&st=n&sr=1&sk=all"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    # 検索ページから記事候補URLを収集（/articles/ と /pickup/）
    cand_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "news.yahoo.co.jp/articles/" in href or "news.yahoo.co.jp/pickup/" in href:
            cand_urls.append(href)
    # 正規化＆重複除去
    seen, targets = set(), []
    for u in cand_urls:
        if u.startswith("//"):
            u = "https:" + u
        if not u.startswith("http"):
            continue
        if u not in seen:
            seen.add(u); targets.append(u)

    items = []
    for u in targets:
        try:
            html0 = fetch_html(u)
            art_url = resolve_yahoo_article_url(html0, u)
            if "news.yahoo.co.jp/pickup/" in art_url and art_url == u:
                # pickup で記事が見つからない場合はスキップ
                continue
            html1 = html0 if art_url == u else fetch_html(art_url)
            if not html1:
                continue

            title, source = extract_yahoo_title_source(html1)
            if not title or title == "Yahoo!ニュース":
                # 最低限OGP
                og = BeautifulSoup(html1, "html.parser").find("meta", attrs={"property": "og:title", "content": True})
                if og and og.get("content"):
                    t = og["content"].strip()
                    if t and t != "Yahoo!ニュース":
                        title = t
            pub = extract_datetime_from_article(html1) or fmt_jst(now_jst())
            cmt = extract_yahoo_comment_count(html1)

            items.append(("Yahoo", art_url, title, pub, source or "Yahoo!ニュース", cmt))
            # Yahoo 側に優しく：短いスリープ
            time.sleep(0.25)
        except Exception:
            continue
    return items

# ========= Gemini（バッチ、JSON強制） =========
def classify_titles_gemini_batched(titles: list[str], batch_size: int = 80) -> list[tuple[str, str]]:
    """titles と同じ長さの [(sentiment, category)] を返す。失敗時は ("","")。"""
    if not GEMINI_API_KEY or not titles:
        return [("", "") for _ in titles]

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        "gemini-1.5-flash",
        generation_config={"response_mime_type": "application/json"}
    )

    results = [("", "")] * len(titles)
    for start in range(0, len(titles), batch_size):
        batch = titles[start:start + batch_size]
        # row は 0-based の絶対インデックスにする
        payload = [{"row": start + i, "title": t} for i, t in enumerate(batch)]
        sys_prompt = (
            "あなたは敏腕雑誌記者です。与えられたタイトルごとに以下を判定して、"
            "JSON配列のみで返してください。各要素は "
            '{"row": 数値, "sentiment": "ポジティブ|ネガティブ|ニュートラル", '
            '"category": "会社|車|車（競合）|技術（EV）|技術（e-POWER）|技術（e-4ORCE）|'
            '技術（AD/ADAS）|技術|モータースポーツ|株式|政治・経済|スポーツ|その他"} '
            "の形式。タイトルは改変しないこと。カテゴリは最も関連が高い1つのみ。"
        )
        try:
            resp = model.generate_content([sys_prompt, {"mime_type": "application/json", "text": json.dumps(payload, ensure_ascii=False)}])
            text = (getattr(resp, "text", "") or "").strip()
            arr = json.loads(text) if text else []
            if isinstance(arr, dict):
                arr = [arr]
            for obj in arr:
                try:
                    idx = int(obj.get("row"))
                    if 0 <= idx < len(results):
                        s = str(obj.get("sentiment", "")).strip()
                        c = str(obj.get("category", "")).strip()
                        results[idx] = (s, c)
                except Exception:
                    continue
        except Exception as e:
            # このバッチは空で埋める（ログのみ）
            print(f"Geminiバッチ失敗: {e}")
            continue
        time.sleep(0.3)
    return results

# ========= 集約（昨日15:00〜今日14:59、シート名=今日のYYMMDD） =========
def build_daily_sheet(sh, msn_items, google_items, yahoo_items):
    now = now_jst()
    today_1500 = now.replace(hour=15, minute=0, second=0, microsecond=0)
    start = today_1500 - timedelta(days=1)  # 昨日15:00
    end = today_1500                        # 今日14:59:59 まで（< end）
    sheet_name = now.strftime("%y%m%d")

    # フィルタ（投稿日がレンジ内のもの）
    def in_window(pub_str: str) -> bool:
        try:
            dt = datetime.strptime(pub_str, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
            return start <= dt < end
        except Exception:
            return False

    msn_f = [x for x in msn_items if in_window(x[3])]
    google_f = [x for x in google_items if in_window(x[3])]
    yahoo_f = [x for x in yahoo_items if in_window(x[3])]

    print(f"📊 フィルタ結果: MSN={len(msn_f)}, Google={len(google_f)}, Yahoo={len(yahoo_f)}")

    # 並び順：MSN→Google→Yahoo
    ordered = msn_f + google_f + yahoo_f

    # タイトルを一括分類
    titles = [row[2] for row in ordered]
    senti_cate = classify_titles_gemini_batched(titles)

    # シート再生成（存在すればクリア）
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows="4000", cols="10")

    headers = ["ソース", "URL", "タイトル", "投稿日", "引用元", "コメント数", "ポジネガ", "カテゴリ"]
    ws.update(values=[headers], range_name="A1:H1")

    # 行データ生成（G/H は Gemini 結果）
    rows = []
    for i, row in enumerate(ordered):
        source, url, title, pub, origin = row[:5]
        comment = row[5] if len(row) > 5 else ""
        s, c = senti_cate[i] if i < len(senti_cate) else ("", "")
        rows.append([source, url, title, pub, origin, comment, s, c])

    if rows:
        ws.update(values=rows, range_name=f"A2:H{len(rows)+1}")

    print(f"🕒 集約期間: {start.strftime('%Y/%m/%d %H:%M')} 〜 {(end - timedelta(minutes=1)).strftime('%Y/%m/%d %H:%M')} → シート名: {sheet_name}")
    print(f"✅ 集約シート {sheet_name}: {len(rows)} 件")
    return sheet_name

# ========= Main =========
def main():
    print(f"🔎 キーワード: {NEWS_KEYWORD}")
    print(f"📄 SPREADSHEET_ID: {SPREADSHEET_ID}")
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID が未設定です。")

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    print(f"📘 Opened spreadsheet title: {sh.title}")

    print("\n--- 取得 ---")
    google_items = fetch_google_news(NEWS_KEYWORD)
    yahoo_items = fetch_yahoo_news(NEWS_KEYWORD)
    msn_items = fetch_msn_news(NEWS_KEYWORD)

    print(f"✅ Googleニュース: {len(google_items)} 件（投稿日取得 {sum(1 for i in google_items if i[3])} 件）")
    print(f"✅ Yahoo!ニュース: {len(yahoo_items)} 件（投稿日取得 {sum(1 for i in yahoo_items if i[3])} 件・コメント数取得対象）")
    print(f"✅ MSNニュース: {len(msn_items)} 件（投稿日取得/推定 {sum(1 for i in msn_items if i[3])} 件）")

    print("\n--- 集約（まとめシートのみ / A列=ソース / 順=MSN→Google→Yahoo） ---")
    build_daily_sheet(sh, msn_items, google_items, yahoo_items)

if __name__ == "__main__":
    main()
