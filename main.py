# -*- coding: utf-8 -*-
import os
import re
import json
import time
import random
import traceback
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials

# Selenium (待機＆同意回避対応)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ========= 基本設定 =========
JST = timezone(timedelta(hours=9))
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

KEYWORD = os.getenv("KEYWORD", os.getenv("NEWS_KEYWORD", "日産")).strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

# Selenium: Selenium Manager 推奨（USE_WDM=0）。必要なら webdriver-manager を利用（=1）
USE_WDM = int(os.getenv("USE_WDM", "0"))
SCROLL_SLEEP = float(os.getenv("SCROLL_SLEEP", "1.2"))
SCROLLS_GOOGLE = int(os.getenv("SCROLLS_GOOGLE", "6"))
SCROLLS_YAHOO  = int(os.getenv("SCROLLS_YAHOO", "6"))
ALLOW_PICKUP_FALLBACK = int(os.getenv("ALLOW_PICKUP_FALLBACK", "1"))

# ========= 共通ヘルパ =========
def soup(html: str):
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def fmt_jst(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y/%m/%d %H:%M")

def parse_last_modified(url: str) -> str:
    try:
        r = requests.head(url, headers=UA, timeout=10, allow_redirects=True)
        lm = r.headers.get("Last-Modified")
        if lm:
            dt = parsedate_to_datetime(lm).astimezone(JST)
            return fmt_jst(dt)
    except Exception:
        pass
    return ""

def fetch_html(url: str, timeout=15) -> str:
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception:
        return ""

# ========= 期間ウィンドウ（前日15:00〜当日14:59） =========
def compute_window_and_sheet_name(now: datetime):
    today = now.astimezone(JST).date()
    start = datetime.combine(today - timedelta(days=1), datetime.min.time()).replace(tzinfo=JST) + timedelta(hours=15)
    end   = datetime.combine(today, datetime.min.time()).replace(tzinfo=JST) + timedelta(hours=14, minutes=59, seconds=59)
    sheet_name = now.astimezone(JST).strftime("%y%m%d")
    return start, end, sheet_name

def in_window(pub_str: str, start: datetime, end: datetime) -> bool:
    try:
        dt = datetime.strptime(pub_str, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
        return start <= dt <= end
    except Exception:
        return False

# ========= Selenium =========
def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,2000")
    options.add_argument("--lang=ja-JP")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"--user-agent={UA['User-Agent']}")

    if USE_WDM:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)  # Selenium Manager に任せる
    return driver

def smooth_scroll(driver, times=4, sleep=1.2):
    last_h = driver.execute_script("return document.body.scrollHeight")
    for _ in range(times):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(sleep)
        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == last_h:
            break
        last_h = new_h

# ========= MSN =========
def fetch_msn(keyword: str):
    items = []
    driver = None
    try:
        driver = get_driver()
        q = quote(keyword)
        url = f"https://www.bing.com/news/search?q={q}&qft=sortbydate%3d'1'&form=YFNR"
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div,li,article"))
        )
        smooth_scroll(driver, times=3, sleep=SCROLL_SLEEP)
        sp = soup(driver.page_source)
        driver.quit()

        cards = sp.select("div.news-card")
        if not cards:
            # 代替セレクタ（レイアウト変化時）
            cards = sp.select("a.title, h2 a, .source a")

        for c in cards:
            try:
                title = c.get("data-title", "").strip() if c.has_attr("data-title") else c.get_text(strip=True)
                url = c.get("data-url", "").strip() if c.has_attr("data-url") else (c.get("href") or "").strip()
                if not (title and url and url.startswith("http")):
                    continue
                source = c.get("data-author", "").strip() if c.has_attr("data-author") else "MSN"
                pub = parse_last_modified(url)
                if pub:
                    items.append(("MSN", url, title, pub, source))
            except Exception:
                continue
    except Exception:
        traceback.print_exc()
    finally:
        if driver:
            try: driver.quit()
            except: pass
    return items

# ========= Google =========
def fetch_google(keyword: str):
    items = []
    driver = None
    try:
        driver = get_driver()

        # 同意画面回避のため、先にトップへアクセスして CONSENT クッキーを投入
        driver.get("https://news.google.com/?hl=ja&gl=JP&ceid=JP:ja")
        try:
            driver.add_cookie({"name": "CONSENT", "value": "YES+1", "domain": ".google.com"})
        except Exception:
            pass

        q = quote(keyword)
        url = f"https://news.google.com/search?q={q}&hl=ja&gl=JP&ceid=JP:ja"
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "article")))
        smooth_scroll(driver, times=SCROLLS_GOOGLE, sleep=SCROLL_SLEEP)
        sp = soup(driver.page_source)
        driver.quit()

        seen = set()
        for art in sp.find_all("article"):
            a = art.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            if "/articles/" not in href:
                continue
            if href.startswith("./"):
                full = "https://news.google.com" + href[1:]
            elif href.startswith("/"):
                full = "https://news.google.com" + href
            else:
                full = href
            title = a.get_text(strip=True)

            # pub: <time datetime> を最優先
            pub = ""
            t = art.find("time")
            if t and t.has_attr("datetime"):
                try:
                    dt = datetime.fromisoformat(t["datetime"].replace("Z", "+00:00")).astimezone(JST)
                    pub = fmt_jst(dt)
                except Exception:
                    pass

            # 最終URL解決 & Last-Modified 補完
            try:
                r = requests.get(full, headers=UA, timeout=10, allow_redirects=True)
                final = r.url
            except Exception:
                final = full
            if not pub:
                pub = parse_last_modified(final)

            if final in seen or not title or not pub:
                continue
            seen.add(final)

            src_div = art.find("div", class_=re.compile("vr1PYe"))
            source = src_div.get_text(strip=True) if src_div else ""
            items.append(("Google", final, title, pub, source))
            time.sleep(0.05)

        # RSSフォールバック（0件時）
        if not items:
            rss = requests.get(
                f"https://news.google.com/rss/search?q={q}&hl=ja&gl=JP&ceid=JP:ja",
                headers=UA,
                timeout=10
            ).text
            sp_rss = soup(rss)
            for it in sp_rss.find_all("item"):
                title = it.title.get_text(strip=True) if it.title else ""
                link = it.link.get_text(strip=True) if it.link else ""
                pub = it.pubdate.get_text(strip=True) if it.pubdate else ""
                try:
                    dt = parsedate_to_datetime(pub).astimezone(JST)
                    pub = fmt_jst(dt)
                except Exception:
                    pub = ""
                if title and link and pub:
                    items.append(("Google", link, title, pub, ""))
    except Exception:
        traceback.print_exc()
    finally:
        if driver:
            try: driver.quit()
            except: pass
    return items

# ========= Yahoo =========
def fetch_yahoo(keyword: str):
    items = []
    driver = None
    try:
        driver = get_driver()
        q = quote(keyword)
        url = (
            "https://news.yahoo.co.jp/search"
            f"?p={q}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
        )
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul,ol,section,article"))
        )
        smooth_scroll(driver, times=SCROLLS_YAHOO, sleep=SCROLL_SLEEP)
        sp = soup(driver.page_source)
        driver.quit()

        cand = []
        for a in sp.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("http"):
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("/"):
                    href = "https://news.yahoo.co.jp" + href
            if "news.yahoo.co.jp/articles/" in href or "news.yahoo.co.jp/pickup/" in href:
                cand.append(href)

        seen = set()
        for u in cand:
            if u in seen:
                continue
            seen.add(u)

            html0 = fetch_html(u)
            art_url = _resolve_yahoo_article_url(html0, u)

            html1 = html0 if art_url == u else fetch_html(art_url)
            title = _extract_yahoo_title(html1) if html1 else ""
            pub = _extract_yahoo_datetime(html1) if html1 else ""
            if not pub and art_url:
                pub = parse_last_modified(art_url)

            final = art_url or u
            if (title or (ALLOW_PICKUP_FALLBACK and final)) and pub:
                items.append(("Yahoo", final, title or "", pub, ""))
            time.sleep(0.08)
    except Exception:
        traceback.print_exc()
    finally:
        if driver:
            try: driver.quit()
            except: pass
    return items

def _resolve_yahoo_article_url(html: str, url: str) -> str:
    try:
        sp = soup(html)
        og = sp.find("meta", attrs={"property": "og:url", "content": True})
        if og and og["content"].startswith("http"):
            return og["content"]
        link = sp.find("link", attrs={"rel": "canonical", "href": True})
        if link and link["href"].startswith("http"):
            return link["href"]
    except Exception:
        pass
    return url

def _extract_yahoo_datetime(html: str) -> str:
    sp = soup(html)
    m = sp.find("meta", attrs={"itemprop": "datePublished", "content": True})
    if m:
        try:
            dt = datetime.fromisoformat(m["content"].replace("Z", "+00:00")).astimezone(JST)
            return fmt_jst(dt)
        except Exception:
            pass
    t = sp.find("time")
    if t and t.has_attr("datetime"):
        try:
            dt = datetime.fromisoformat(t["datetime"].replace("Z", "+00:00")).astimezone(JST)
            return fmt_jst(dt)
        except Exception:
            pass
    if t and t.get_text(strip=True):
        txt = re.sub(r"\([月火水木金土日]\)", "", t.get_text(strip=True))
        try:
            dt = datetime.strptime(txt, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
            return fmt_jst(dt)
        except Exception:
            pass
    return ""

def _extract_yahoo_title(html: str) -> str:
    sp = soup(html)
    h1 = sp.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    og = sp.find("meta", attrs={"property": "og:title", "content": True})
    if og and og["content"].strip():
        return og["content"].strip()
    return (sp.title.get_text(strip=True) if sp.title else "").strip()

# ========= Gemini 分類（番号+タイトル → 4列TSVで返させる） =========
GEMINI_PROMPT = """あなたは敏腕雑誌記者です。 上記Webニュースのタイトルを以下の視点で判断してほしい。
①ポジティブ、ネガティブ、ニュートラルの判別。
②記事のカテゴリーの判別。　以下に例を記載してほしい。
会社：企業の施策や生産、販売台数など。　ニッサン、トヨタ、ホンダ、スバル、マツダ、スズキ、ミツビシ、ダイハツの記事の場合、()付で企業名を書いて。それ以外はその他。
車：クルマの名称が含まれているもの（会社名だけの場合は車に分類しない）
新型/現行/旧型+名称を()付で記載して。（例・・新型リーフ、現行セレナ、旧型スカイライン）
日産以外の車の場合は、車（競合）と記載して。
技術（EV）：電気自動車の技術に関わるもの（バッテリー工場建設や企業の施策は含まない）
技術（e-POWER）：e-POWERに関わるもの
技術（e-4ORCE）：4WDや2WD、AWDに関わるもの
技術（AD/ADAS）：自動運転や先進運転システムに関わるもの
技術：上記以外の技術に関わるもの
モータースポーツ：F1やラリー、フォミュラーEなど、自動車のレースに関わるもの
株式：株式発行や株価の値動き、投資に関わるもの
政治・経済：政治家や選挙、税金、経済に関わるもの
スポーツ：野球やサッカー、バレーボールなどに関わるもの
その他：上記に含まれないもの

出力形式は、下の「番号. タイトル」一覧に対し、入力の番号と同じ番号・同じタイトルをそのまま用いて、
「番号\tタイトル\tポジティブ|ネガティブ|ニュートラル\tカテゴリ」
の4列TSVで、入力と同じ件数・同じ順序で出力してください。
※タイトルは一切変更・修正しないこと。カテゴリは並記せず最も関連性が高い1つだけを選ぶこと。
"""

def classify_with_gemini(titles):
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY が設定されていないため、AI分類を実行できません。")
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_MODEL)

    results_by_idx = {}
    BATCH = 20
    idx_offset = 0
    for i in range(0, len(titles), BATCH):
        chunk = titles[i:i+BATCH]
        numbered = [f"{idx_offset+j+1}. {t}" for j, t in enumerate(chunk)]
        prompt = GEMINI_PROMPT + "\n番号付きタイトル一覧:\n" + "\n".join(numbered)

        resp_text = ""
        for attempt in range(4):
            try:
                resp = model.generate_content(prompt)
                resp_text = (resp.text or "").strip()
                if resp_text:
                    break
            except Exception as e:
                wait = 2 + attempt * 3 + random.random() * 2
                print(f"⚠️ Gemini API リトライ {attempt+1}/4: {e}（待機 {wait:.1f}s）")
                time.sleep(wait)
        if not resp_text:
            raise RuntimeError("Gemini の応答が空でした。")

        lines = [ln for ln in resp_text.splitlines() if ln.strip()]
        for ln in lines:
            parts = [p.strip() for p in ln.split("\t")]
            if len(parts) >= 4:
                num_str = parts[0]
                m = re.match(r"^(\d+)", num_str)
                if not m:
                    continue
                idx = int(m.group(1))
                senti, cate = parts[2], parts[3]
                results_by_idx[idx] = (senti, cate)
        idx_offset += len(chunk)

    results = []
    for i in range(1, len(titles)+1):
        results.append(results_by_idx.get(i, ("ニュートラル", "その他")))
    return results

# ========= Sheets =========
def open_sheet(spreadsheet_id: str):
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID が設定されていません。")
    blob = os.getenv("GCP_SERVICE_ACCOUNT_KEY", "")
    if not blob:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY が空です（サービスアカウントJSON本文を設定してください）。")
    try:
        data = json.loads(blob)
        creds = Credentials.from_service_account_info(data, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    except Exception:
        path = "credentials.json"
        with open(path, "w", encoding="utf-8") as f:
            f.write(blob)
        creds = Credentials.from_service_account_file(path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    return gc.open_by_key(spreadsheet_id)

def upsert_single_sheet(sh, sheet_name: str, rows: list):
    headers = ["ソース", "URL", "タイトル", "投稿日", "引用元", "ポジネガ", "カテゴリ"]
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=str(max(1000, len(rows)+10)), cols=str(len(headers)))
    # gspread の DeprecationWarning 回避：values を先、range_name を後
    ws.update(values=[headers], range_name="A1:G1")
    if rows:
        ws.update(values=rows, range_name=f"A2:G{len(rows)+1}")

# ========= メイン =========
def main():
    now = datetime.now(JST)
    start, end, sheet_name = compute_window_and_sheet_name(now)
    print(f"🔎 キーワード: {KEYWORD}")
    print(f"📅 期間: {fmt_jst(start)}〜{fmt_jst(end)} / シート: {sheet_name}")

    # 取得（順序: MSN → Google → Yahoo）
    msn    = fetch_msn(KEYWORD)
    print(f"MSN raw: {len(msn)}")
    google = fetch_google(KEYWORD)
    print(f"Google raw: {len(google)}")
    yahoo  = fetch_yahoo(KEYWORD)
    print(f"Yahoo raw: {len(yahoo)}")

    # 結合（順序維持）＆ URL重複は先勝ち（MSN優先）＆ ウィンドウ内のみ
    merged = []
    seen = set()
    for row in (msn + google + yahoo):
        src, url, title, pub, origin = row
        if url in seen:
            continue
        seen.add(url)
        if not pub or not in_window(pub, start, end):
            continue
        merged.append(row)

    print(f"📦 フィルタ後: {len(merged)} 件")

    # シートは毎日1枚（YYMMDD）に集約、分類はAIで必ず実施
    if not merged:
        sh = open_sheet(SPREADSHEET_ID)
        upsert_single_sheet(sh, sheet_name, [])
        print("⚠️ 期間内の記事が見つかりませんでした。")
        return

    titles = [t for (_, _, t, _, _) in merged]
    labels = classify_with_gemini(titles)  # [(senti, cate)]

    def norm_sent(s):
        s = s.strip()
        if s.startswith("ポジ"): return "ポジティブ"
        if s.startswith("ネガ"): return "ネガティブ"
        if "ニュートラル" in s or "neutral" in s.lower(): return "ニュートラル"
        return s or "ニュートラル"

    rows = []
    for (src, url, title, pub, origin), (senti, cate) in zip(merged, labels):
        rows.append([src, url, title, pub, origin or "", norm_sent(senti), cate])

    sh = open_sheet(SPREADSHEET_ID)
    upsert_single_sheet(sh, sheet_name, rows)
    print(f"✅ 書き込み完了: {sheet_name}（{len(rows)}件）")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ エラー:", e)
        traceback.print_exc()
        raise
