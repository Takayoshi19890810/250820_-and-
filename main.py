# -*- coding: utf-8 -*-
"""
まとめシートのみ出力 / 今日の YYMMDD に、昨日15:00〜今日14:59 の記事を集約
+ Gemini を「バッチ推論」で使用し、C列タイトル → G列(ポジ/ネガ/ニュートラル)・H列(カテゴリ) を一括付与
+ Yahoo 記事のコメント数を取得して F列に記載（/comments?page=N を Selenium で巡回して数える）
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

# === Gemini ===
import google.generativeai as genai

# ====== 設定 ======
KEYWORD = os.getenv("NEWS_KEYWORD", "日産")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1Vs4Cx8QPN4H2NOgtwaviOCe8zBTpUNDgJjqkHr51IZE")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# ====== 共通ユーティリティ ======
def jst_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=9)

def fmt(dt: datetime) -> str:
    return dt.strftime("%Y/%m/%d %H:%M")

def try_parse_jst(dt_str: str):
    if not dt_str or dt_str == "取得不可":
        return None
    patterns = [
        "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f%z",
    ]
    for p in patterns:
        try:
            dt = datetime.strptime(dt_str, p)
            if p.endswith("Z"):
                dt = dt + timedelta(hours=9)
            elif "%z" in p:
                dt = dt.astimezone(tz=None); dt = dt + timedelta(hours=9)
            return dt
        except Exception:
            pass
    return None

def parse_relative_time(label: str, base: datetime) -> str:
    s = (label or "").strip()
    try:
        m = re.search(r"(\d+)\s*分前", s)
        if m: return fmt(base - timedelta(minutes=int(m.group(1))))
        h = re.search(r"(\d+)\s*時間前", s)
        if h: return fmt(base - timedelta(hours=int(h.group(1))))
        d = re.search(r"(\d+)\s*日前", s)
        if d: return fmt(base - timedelta(days=int(d.group(1))))
        if re.match(r"\d{1,2}月\d{1,2}日$", s):
            dt = datetime.strptime(f"{base.year}年{s}", "%Y年%m月%d日")
            return fmt(dt)
        if re.match(r"\d{4}/\d{1,2}/\d{1,2}$", s):
            dt = datetime.strptime(s, "%Y/%m/%d")
            return fmt(dt)
        if re.match(r"\d{1,2}:\d{2}$", s):
            t = datetime.strptime(s, "%H:%M").time()
            dt = datetime.combine(base.date(), t)
            if dt > base: dt -= timedelta(days=1)
            return fmt(dt)
    except Exception:
        pass
    return "取得不可"

def get_last_modified_datetime(url: str) -> str:
    try:
        r = requests.head(url, timeout=6, allow_redirects=True)
        if "Last-Modified" in r.headers:
            dt = parsedate_to_datetime(r.headers["Last-Modified"])
            if dt.tzinfo:
                dt = dt.astimezone(tz=None); dt = dt + timedelta(hours=9)
            else:
                dt = dt + timedelta(hours=9)
            return fmt(dt)
    except Exception:
        pass
    return "取得不可"

def fetch_html(url: str, timeout: int = 10):
    try:
        hdrs = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=hdrs, timeout=timeout)
        if r.ok: return r.text
    except Exception:
        pass
    return ""

def extract_datetime_from_article(html: str) -> str:
    if not html: return "取得不可"
    soup = BeautifulSoup(html, "html.parser")
    # JSON-LD
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if not isinstance(obj, dict): continue
                for key in ["datePublished", "dateModified", "uploadDate"]:
                    if obj.get(key):
                        dt = try_parse_jst(str(obj[key]).strip())
                        if dt: return fmt(dt)
        except Exception:
            continue
    # <time datetime>
    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        dt = try_parse_jst(t["datetime"].strip())
        if dt: return fmt(dt)
    # OG
    for prop in ["article:published_time", "article:modified_time", "og:updated_time"]:
        m = soup.find("meta", attrs={"property": prop, "content": True})
        if m and m.get("content"):
            dt = try_parse_jst(m["content"].strip())
            if dt: return fmt(dt)
    return "取得不可"

def extract_title_and_source_from_yahoo(html: str):
    title, source = "", "Yahoo"
    if not html: return title, source
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
                    if source == "Yahoo":
                        pub = obj.get("publisher")
                        if isinstance(pub, dict) and pub.get("name"):
                            source = str(pub["name"]).strip() or "Yahoo"
        except Exception:
            continue
    # <h1> / twitter:title / og:title
    if not title:
        h1 = soup.find("h1")
        if h1: title = h1.get_text(strip=True)
    if not title:
        tw = soup.find("meta", attrs={"name": "twitter:title", "content": True})
        if tw and tw["content"].strip() != "Yahoo!ニュース":
            title = tw["content"].strip()
    if not title:
        og = soup.find("meta", attrs={"property": "og:title", "content": True})
        if og and og["content"].strip() != "Yahoo!ニュース":
            title = og["content"].strip()
    # 出典
    if source == "Yahoo":
        src_meta = soup.find("meta", attrs={"name": "source", "content": True})
        if src_meta and src_meta.get("content"):
            source = src_meta["content"].strip() or "Yahoo"
    if source == "Yahoo":
        cand = soup.find(["span","div"], string=True)
        if cand:
            txt = cand.get_text(strip=True)
            if 2 <= len(txt) <= 30 and not txt.isdigit():
                source = txt
    return title, source

def resolve_yahoo_article_url(html: str, orig_url: str) -> str:
    if not html:
        return orig_url
    soup = BeautifulSoup(html, "html.parser")
    a = soup.select_one('a[href*="news.yahoo.co.jp/articles/"]')
    if a and a.get("href"):
        return a["href"]
    can = soup.find("link", rel="canonical")
    if can and can.get("href"):
        return can["href"]
    return orig_url

def chrome_driver():
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--disable-gpu")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1920,1080")
    opt.add_argument("--lang=ja-JP")
    # ヘッドレス検知を弱める
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opt)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass
    return driver

# ====== 取得：Google ======
def get_google_news(keyword: str):
    driver = chrome_driver()
    url = f"https://news.google.com/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
    driver.get(url)
    time.sleep(5)
    for _ in range(4):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.2)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data, with_time = [], 0
    for art in soup.find_all("article"):
        try:
            a_tag = art.select_one("a.JtKRv") or art.select_one("a.WwrzSb") or art.select_one("a.DY5T1d") or art.select_one("h3 a")
            time_el = art.select_one("time[datetime]") or art.find("time")
            src_el = art.select_one("div.vr1PYe") or art.select_one("div.SVJrMe")
            if not a_tag: continue
            title = a_tag.get_text(strip=True)
            href  = a_tag.get("href")
            if not title or not href: continue
            url = "https://news.google.com" + href[1:] if href.startswith("./") else href
            if not url.startswith("http"): continue
            pub = "取得不可"
            if time_el and time_el.get("datetime"):
                dt = try_parse_jst(time_el.get("datetime").strip())
                if dt: pub = fmt(dt); with_time += 1
            if pub == "取得不可":
                html = fetch_html(url)
                pub = extract_datetime_from_article(html)
            source = (src_el.get_text(strip=True) if src_el else "Google") or "Google"
            data.append({"タイトル": title, "URL": url, "投稿日": pub, "引用元": source, "ソース": "Google"})
        except Exception:
            continue
    print(f"✅ Googleニュース: {len(data)} 件（投稿日取得 {with_time} 件）")
    return data

# ====== 取得：Yahoo ======
def get_yahoo_news(keyword: str):
    driver = chrome_driver()
    url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    def is_article(u: str) -> bool:
        return u and u.startswith("http") and ("news.yahoo.co.jp/articles/" in u or "news.yahoo.co.jp/pickup/" in u)

    raw_links = [a.get("href") for a in soup.find_all("a", href=True)]
    article_links = [u for u in raw_links if is_article(u)]

    data, with_time = [], 0
    seen = set()
    for href in article_links:
        if href in seen: continue
        seen.add(href)
        try:
            html0 = fetch_html(href)
            real_url = resolve_yahoo_article_url(html0, href)  # pickup→記事へ解決
            html = fetch_html(real_url) if real_url != href else html0
            if not html: continue

            title, source = extract_title_and_source_from_yahoo(html)
            pub = extract_datetime_from_article(html)

            # タイトル最低限ガード
            if not title or title == "Yahoo!ニュース":
                continue
            if pub != "取得不可": with_time += 1

            data.append({"タイトル": title, "URL": real_url, "投稿日": pub, "引用元": source, "ソース": "Yahoo"})
        except Exception:
            continue

    print(f"✅ Yahoo!ニュース: {len(data)} 件（投稿日取得 {with_time} 件）")
    return data

# ====== 取得：MSN（Bing News） ======
def get_msn_news(keyword: str):
    base = jst_now()
    driver = chrome_driver()
    url = ("https://www.bing.com/news/search"
           f"?q={keyword}"
           "&qft=sortbydate%3d'1'&setlang=ja-JP&mkt=ja-JP&cc=JP&form=YFNR")
    driver.get(url)
    time.sleep(5)
    for _ in range(4):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.2)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    data, with_time = [], 0

    cards = soup.select("div.news-card[data-title][data-url]") or []
    for c in cards:
        try:
            title = (c.get("data-title") or "").strip()
            link  = (c.get("data-url") or "").strip()
            source = (c.get("data-author") or "").strip() or "MSN"
            if not title or not link.startswith("http"):
                continue
            lab = ""
            s = c.find("span", attrs={"aria-label": True})
            if s and s.has_attr("aria-label"):
                lab = s["aria-label"].strip()
            pub = parse_relative_time(lab, base)
            if pub == "取得不可":
                html = fetch_html(link)
                pub = extract_datetime_from_article(html)
                if pub == "取得不可":
                    pub = get_last_modified_datetime(link)
            else:
                with_time += 1
            data.append({"タイトル": title, "URL": link, "投稿日": pub, "引用元": source, "ソース": "MSN"})
        except Exception:
            continue

    if not data:
        items = soup.select("a.title, h2 a, h3 a, a[href*='/news/']")
        for a in items:
            try:
                href = a.get("href"); title = a.get_text(strip=True)
                if not href or not href.startswith("http") or not title:
                    continue
                cont = a.find_parent(["div","li","article"]) or soup
                lab = ""
                s = cont.find("span", attrs={"aria-label": True})
                if s and s.has_attr("aria-label"): lab = s["aria-label"].strip()
                pub = parse_relative_time(lab, base)
                if pub == "取得不可":
                    html = fetch_html(href)
                    pub = extract_datetime_from_article(html)
                    if pub == "取得不可":
                        pub = get_last_modified_datetime(href)
                else:
                    with_time += 1
                source = "MSN"
                src_el = cont.find(["span","div"], class_=re.compile("source|provider"))
                if src_el:
                    st = src_el.get_text(strip=True)
                    if st: source = st
                data.append({"タイトル": title, "URL": href, "投稿日": pub, "引用元": source, "ソース": "MSN"})
            except Exception:
                continue

    print(f"✅ MSNニュース: {len(data)} 件（投稿日取得/推定 {with_time} 件）")
    return data

# ====== Yahoo コメント数 ======
def count_yahoo_comments_with_driver(driver, url: str, max_pages: int = 10, sleep_sec: float = 2.0) -> int:
    """
    Yahooニュース記事に対し /comments?page=N を開いて <p class='sc-169yn8p-10'> を数える方式。
    参照いただいたスクリプトのロジックを簡略化してカウント専用にしています。
    """
    total = 0
    prev_first = None
    for page in range(1, max_pages + 1):
        c_url = f"{url.rstrip('/')}/comments?page={page}"
        try:
            driver.get(c_url)
            time.sleep(sleep_sec)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            elems = soup.find_all("p", class_="sc-169yn8p-10")
            if not elems:
                break
            first_text = elems[0].get_text(strip=True) if elems else None
            # 同じ内容がループし始めたら終了
            if prev_first and first_text == prev_first:
                break
            prev_first = first_text
            total += len(elems)
        except Exception:
            break
    return total

def get_yahoo_comment_counts(urls: list, sleep_sec: float = 2.0) -> dict:
    """
    複数URLを1つのドライバで順にカウントして、{url: count} を返す
    """
    if not urls:
        return {}
    driver = chrome_driver()
    out = {}
    try:
        for u in urls:
            try:
                out[u] = count_yahoo_comments_with_driver(driver, u, sleep_sec=sleep_sec)
            except Exception:
                out[u] = 0
    finally:
        driver.quit()
    return out

# ====== スプレッドシート ======
def get_gspread_client():
    key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    if key:
        try:
            creds = json.loads(key)
            return gspread.service_account_from_dict(creds)
        except Exception as e:
            raise RuntimeError(f"GCP_SERVICE_ACCOUNT_KEY のJSONが不正です: {e}")
    return gspread.service_account(filename="credentials.json")

def compute_window(now_jst: datetime):
    today = now_jst.date()
    today_1500 = datetime.combine(today, dtime(hour=15, minute=0))
    start = today_1500 - timedelta(days=1)        # 昨日15:00
    end   = today_1500 - timedelta(seconds=1)     # 今日14:59:59
    label = today.strftime("%y%m%d")              # 今日 → YYMMDD
    return start, end, label

def build_daily_sheet(sh, rows_all: list):
    now = jst_now()
    start, end, label = compute_window(now)
    print(f"🕒 集約期間: {fmt(start)} 〜 {fmt(end)} → シート名: {label}")

    filtered = {"MSN": [], "Google": [], "Yahoo": []}
    no_date = 0
    for r in rows_all:
        dt = try_parse_jst(r.get("投稿日", ""))
        if not dt:
            no_date += 1
            continue
        if start <= dt <= end:
            src = r.get("ソース","")
            if src in filtered:
                filtered[src].append(r)

    print(f"📊 フィルタ結果: MSN={len(filtered['MSN'])}, Google={len(filtered['Google'])}, Yahoo={len(filtered['Yahoo'])}, 日付無しスキップ={no_date}")

    def dedup_sort(lst):
        seen = set(); uniq = []
        for d in lst:
            if d["URL"] not in seen:
                seen.add(d["URL"]); uniq.append(d)
        uniq.sort(key=lambda x: try_parse_jst(x["投稿日"]) or datetime(1970,1,1), reverse=True)
        return uniq

    ordered = []
    for src in ["MSN", "Google", "Yahoo"]:
        ordered.extend(dedup_sort(filtered[src]))

    # --- Yahooコメント数を一括取得 ---
    yahoo_urls = [d["URL"] for d in ordered if d.get("ソース") == "Yahoo"]
    cmt_map = get_yahoo_comment_counts(sorted(set(yahoo_urls))) if yahoo_urls else {}

    # ヘッダー: A..F まで使用（G/H は Gemini 用）
    headers = ["ソース", "URL", "タイトル", "投稿日", "引用元", "コメント数"]  # A..F
    try:
        ws = sh.worksheet(label)
        ws.clear(); ws.append_row(headers)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=label, rows=str(max(2, len(ordered)+5)), cols="9")
        ws.append_row(headers)

    if ordered:
        rows = []
        for d in ordered:
            cnt = ""
            if d["ソース"] == "Yahoo":
                cnt = cmt_map.get(d["URL"], 0)
            rows.append([d["ソース"], d["URL"], d["タイトル"], d["投稿日"], d["引用元"], cnt])
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"✅ 集約シート {label}: {len(rows)} 件（Yahooコメント数 付与: {len(yahoo_urls)} 件）")
    else:
        print(f"⚠️ 集約シート {label}: 対象記事なし")

    # G/H ヘッダー
    ws.update("G1:H1", [["ポジネガ", "カテゴリ"]])

    return label

# ====== Gemini バッチ分類 ======
GEMINI_SYSTEM_PROMPT = """あなたは敏腕雑誌記者です。与えられた「ニュースのタイトル」一覧について、
各タイトルごとに以下を判定してください。
①ポジティブ／ネガティブ／ニュートラル のいずれか1つ
②カテゴリ（以下の中から最も関連性が高い1つだけ）：
会社、車、車（競合）、技術（EV）、技術（e-POWER）、技術（e-4ORCE）、技術（AD/ADAS）、技術、モータースポーツ、株式、政治・経済、スポーツ、その他

追加ルール：
- 「会社」：ニッサン、トヨタ、ホンダ、スバル、マツダ、スズキ、ミツビシ、ダイハツの記事は () に企業名。その他は「その他」。
- 「車」：車名が含まれる場合のみ（会社名だけは不可）。新型/現行/旧型 + 名称を () 付で記載（例：新型リーフ、現行セレナ、旧型スカイライン）。日産以外は「車（競合）」。
- 技術（EV / e-POWER / e-4ORCE / AD/ADAS）：該当すればそれを優先。その他の技術は「技術」。
- 出力は JSON配列で、各要素は {"row": 数値, "sentiment":"ポジティブ|ネガティブ|ニュートラル", "category":"..."} の形式。行番号 row は与えたIDをそのまま返すこと。
- タイトル文言は改変しないこと。
"""

def setup_gemini():
    if not GEMINI_API_KEY:
        print("⚠️ GEMINI_API_KEY が未設定のため、分類はスキップします。")
        return None
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel("gemini-1.5-flash")

def build_batch_prompt(tuples):
    data = [{"row": r, "title": t} for (r, t) in tuples]
    payload = json.dumps(data, ensure_ascii=False)
    prompt = GEMINI_SYSTEM_PROMPT + "\n\nデータ:\n" + payload + "\n\n上記に対する回答のみをJSON配列で返してください。余計な説明は不要です。"
    return prompt

def parse_batch_response(text):
    if not text:
        return []
    m = re.search(r"\[\s*\{.*\}\s*\]", text, re.S)
    if m:
        try:
            arr = json.loads(m.group(0))
            if isinstance(arr, list):
                return arr
        except Exception:
            pass
    m2 = re.search(r"\{.*\}", text, re.S)
    if m2:
        try:
            obj = json.loads(m2.group(0))
            if isinstance(obj, dict):
                return [obj]
        except Exception:
            pass
    return []

def classify_titles_in_batches(sh, sheet_name: str, batch_size: int = 80, sleep_sec: float = 0.5):
    model = setup_gemini()
    if model is None:
        return

    ws = sh.worksheet(sheet_name)
    values = ws.get_all_values()
    if not values or len(values[0]) < 3:
        print("⚠️ タイトル列が見つかりません。")
        return

    # 2行目以降の C列をまとめて判定
    items = []
    for idx, row in enumerate(values[1:], start=2):
        title = row[2] if len(row) > 2 else ""
        if title:
            items.append((idx, title))

    if not items:
        print("⚠️ Gemini分類対象なし。"); return

    results_map = {}

    for i in range(0, len(items), batch_size):
        batch = items[i:i+batch_size]
        prompt = build_batch_prompt(batch)
        try:
            resp = model.generate_content(prompt)
            text = (getattr(resp, "text", "") or "").strip()
        except Exception as e:
            print(f"Geminiバッチ失敗: {e}")
            for r,_ in batch:
                results_map[r] = ("ニュートラル", "その他")
            time.sleep(sleep_sec)
            continue

        arr = parse_batch_response(text)
        if not arr:
            for r,_ in batch:
                results_map[r] = ("ニュートラル", "その他")
        else:
            covered = set()
            for obj in arr:
                try:
                    r = int(obj.get("row"))
                    s = str(obj.get("sentiment","")).strip() or "ニュートラル"
                    c = str(obj.get("category","")).strip() or "その他"
                    results_map[r] = (s, c)
                    covered.add(r)
                except Exception:
                    continue
            for (r, _) in batch:
                if r not in covered:
                    results_map[r] = ("ニュートラル", "その他")

        time.sleep(sleep_sec)

    # 一括書き込み（G/H）
    updates = []
    min_row = 2
    max_row = max(results_map.keys()) if results_map else 1
    for r in range(min_row, max_row + 1):
        if r in results_map:
            s, c = results_map[r]
        else:
            s, c = ("", "")
        updates.append([s, c])

    if updates:
        ws.update(f"G{min_row}:H{min_row + len(updates) - 1}", updates, value_input_option="USER_ENTERED")
        print(f"✅ Geminiバッチ分類完了: {len(items)} タイトル / 呼び出し {((len(items)-1)//batch_size)+1} 回")
    else:
        print("⚠️ Gemini分類の書き込み対象がありません。")

# ====== メイン ======
def main():
    print(f"🔎 キーワード: {KEYWORD}")
    print(f"📄 SPREADSHEET_ID: {SPREADSHEET_ID}")

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    print(f"📘 Opened spreadsheet title: {sh.title}")

    print("\n--- 取得 ---")
    google_items = get_google_news(KEYWORD)
    yahoo_items  = get_yahoo_news(KEYWORD)
    msn_items    = get_msn_news(KEYWORD)

    all_items = []
    all_items.extend(msn_items)
    all_items.extend(google_items)
    all_items.extend(yahoo_items)

    print("\n--- 集約（まとめシートのみ / A列=ソース / 順=MSN→Google→Yahoo） ---")
    sheet_name = build_daily_sheet(sh, all_items)

    print("\n--- Gemini（無料枠節約のバッチ）でポジ/ネガ＆カテゴリ付与（G列/H列） ---")
    classify_titles_in_batches(sh, sheet_name, batch_size=80, sleep_sec=0.5)

if __name__ == "__main__":
    main()
