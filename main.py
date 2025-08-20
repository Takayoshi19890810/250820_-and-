# -*- coding: utf-8 -*-
import os
import re
import json
import time
import random
import traceback
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials

# ========= 基本設定 =========
JST = timezone(timedelta(hours=9))
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"}

KEYWORD = os.getenv("KEYWORD", os.getenv("NEWS_KEYWORD", "日産")).strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

USE_WDM = int(os.getenv("USE_WDM", "0"))   # 0: Selenium Manager, 1: webdriver-manager
SCROLL_SLEEP = float(os.getenv("SCROLL_SLEEP", "1.2"))
SCROLLS_GOOGLE = int(os.getenv("SCROLLS_GOOGLE", "4"))
SCROLLS_YAHOO = int(os.getenv("SCROLLS_YAHOO", "4"))
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
    end = datetime.combine(today, datetime.min.time()).replace(tzinfo=JST) + timedelta(hours=14, minutes=59, seconds=59)
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
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,2000")
    options.add_argument("--lang=ja-JP")
    if USE_WDM:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)  # Selenium Manager
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
    try:
        driver = get_driver()
        url = f"https://www.bing.com/news/search?q={keyword}&qft=sortbydate%3d'1'&form=YFNR"
        driver.get(url)
        time.sleep(3)
        smooth_scroll(driver, times=2, sleep=SCROLL_SLEEP)
        sp = soup(driver.page_source)
        driver.quit()
        cards = sp.select("div.news-card")
        for c in cards:
            title = c.get("data-title", "").strip()
            url = c.get("data-url", "").strip()
            source = c.get("data-author", "").strip() or "MSN"
            pub = parse_last_modified(url) if url else ""
            if title and url and pub:
                items.append(("MSN", url, title, pub, source))
    except Exception:
        traceback.print_exc()
    return items

# ========= Google =========
def fetch_google(keyword: str):
    items = []
    driver = None
    try:
        driver = get_driver()
        url = f"https://news.google.com/search?q={keyword}&hl=ja&gl=JP&ceid=JP:ja"
        driver.get(url)
        time.sleep(3)
        smooth_scroll(driver, times=SCROLLS_GOOGLE, sleep=SCROLL_SLEEP)
        sp = soup(driver.page_source)
        driver.quit()
        seen = set()
        for a in sp.find_all("a", href=True):
            href = a["href"]
            if "/articles/" not in href:
                continue
            full = "https://news.google.com" + href[1:] if href.startswith("./") else ("https://news.google.com"+href if href.startswith("/") else href)
            title = a.get_text(strip=True)
            try:
                final = requests.get(full, headers=UA, timeout=10, allow_redirects=True).url
            except Exception:
                final = full
            pub = parse_last_modified(final)
            if final not in seen and title and pub:
                seen.add(final)
                items.append(("Google", final, title, pub, ""))
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
        url = (
            "https://news.yahoo.co.jp/search"
            f"?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
        )
        driver.get(url)
        time.sleep(3)
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
            if u in seen: continue
            seen.add(u)
            html = fetch_html(u)
            sp1 = soup(html)
            h1 = sp1.find("h1")
            title = h1.get_text(strip=True) if h1 else ""
            pub = parse_last_modified(u)
            if (title or (ALLOW_PICKUP_FALLBACK and u)) and pub:
                items.append(("Yahoo", u, title or "", pub, ""))
    except Exception:
        traceback.print_exc()
    finally:
        if driver:
            try: driver.quit()
            except: pass
    return items

# ========= Gemini 分類（番号+タイトル→4列TSVで返させる） =========
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
        # 番号付きリストを作成（1始まり、バッチ内はオフセット加算）
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
                # 期待: 番号, タイトル, 判定, カテゴリ
                num_str, title_out, senti, cate = parts[0], parts[1], parts[2], parts[3]
                m = re.match(r"^\d+$", num_str)
                if not m:
                    # "12. " などに来ても拾えるように
                    m2 = re.match(r"^(\d+)\.?", num_str)
                    if m2:
                        num_str = m2.group(1)
                    else:
                        continue
                idx = int(num_str)
                results_by_idx[idx] = (senti, cate)
        idx_offset += len(chunk)

    # 並びを元の順に復元（欠損はニュートラル/その他で埋める）
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
        # 文字列がJSONでない場合はファイルとして書き出して読む
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
    ws.update("A1:G1", [headers])
    if rows:
        ws.update(f"A2:G{len(rows)+1}", rows)

# ========= メイン =========
def main():
    now = datetime.now(JST)
    start, end, sheet_name = compute_window_and_sheet_name(now)
    print(f"🔎 キーワード: {KEYWORD}")
    print(f"📅 期間: {fmt_jst(start)}〜{fmt_jst(end)} / シート: {sheet_name}")

    # 取得（順序: MSN → Google → Yahoo）
    msn = fetch_msn(KEYWORD)
    print(f"MSN raw: {len(msn)}")

    google = fetch_google(KEYWORD)
    print(f"Google raw: {len(google)}")

    yahoo = fetch_yahoo(KEYWORD)
    print(f"Yahoo raw: {len(yahoo)}")

    # 結合（順序維持）＆ URL重複先勝ち（MSN優先）＆ ウィンドウ内のみ
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

    if not merged:
        # 何も無い場合でもシートは作成（ヘッダのみ）
        sh = open_sheet(SPREADSHEET_ID)
        upsert_single_sheet(sh, sheet_name, [])
        print("⚠️ 期間内の記事が見つかりませんでした。")
        return

    # 分類（番号+タイトルでAIに依頼）
    titles = [t for (_, _, t, _, _) in merged]
    labels = classify_with_gemini(titles)  # [(sentiment, category)]

    # 出力整形
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
