#!/usr/bin/env python3
# Step 3b — Multithreading: API & BS4 threaded; Selenium visits detail pages to fill ALL fields
import os, re, time, csv, json, random, argparse, concurrent.futures
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ---------- Config ----------
BASE_URL = "https://www.themoviedb.org"
DEFAULT_API_KEY = "a69d199fd91304d9dfc454bc6e0dc3e9"  # fallback; set TMDB_API_KEY in env for production
API_KEY = os.getenv("TMDB_API_KEY", DEFAULT_API_KEY)
MAX_MOVIES = 60
THREADS_API = 10
THREADS_BS4 = 10
DELAY = 0.25  # polite base delay; we add jitter

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# unified schema required by the brief
HEADERS = ["tmdb_id","title","release_date","rating","overview","genres","poster_url","movie_url","method"]


# ---------- Metrics ----------
class Metrics:
    def __init__(self):
        self.count = 0; self.bytes = 0; self.latencies = []
    def record(self, latency, size):
        self.count += 1; self.bytes += max(0, int(size)); self.latencies.append(max(0.0, float(latency)))
    def summary(self):
        avg_ms = (sum(self.latencies)/len(self.latencies)*1000.0) if self.latencies else 0.0
        return {"requests": self.count, "bytes": self.bytes, "avg_latency_ms": round(avg_ms,2), "total_latency_s": round(sum(self.latencies),3)}


# ---------- Helpers ----------
def normalize_date(s):
    dt = pd.to_datetime(s, errors="coerce")
    return dt.strftime("%Y-%m-%d") if not pd.isna(dt) else "N/A"

def normalize_rating(x):
    try:
        v = float(x);  v = v/10.0 if v > 10 else v
        return round(max(0.0, min(10.0, v)), 1)
    except Exception:
        return "N/A"

def parse_id(url_or_path):
    m = re.search(r"/movie/(\d+)", url_or_path or "")
    return int(m.group(1)) if m else None

def make_session():
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
    s = requests.Session()
    retry = Retry(
        total=6, connect=6, read=6, backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"])
    )
    s.mount("http://", HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50))
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    return s


# ---------- API (multithreaded, full details) ----------
def fetch_movie_api(mid, metrics, session):
    url = f"https://api.themoviedb.org/3/movie/{mid}?api_key={API_KEY}&language=en-US"
    t0 = time.time()
    try:
        r = session.get(url, timeout=20); latency = time.time()-t0
        metrics.record(latency, len(r.content))
        if not r.ok: return None
        d = r.json()
        return {
            "tmdb_id": d.get("id"),
            "title": d.get("title") or d.get("original_title","N/A"),
            "release_date": normalize_date(d.get("release_date")),
            "rating": normalize_rating(d.get("vote_average")),
            "overview": d.get("overview") or "N/A",
            "genres": " | ".join([g.get("name") for g in d.get("genres", [])]) or "N/A",
            "poster_url": f"https://image.tmdb.org/t/p/w500{d.get('poster_path')}" if d.get("poster_path") else "N/A",
            "movie_url": f"{BASE_URL}/movie/{d.get('id')}",
            "method": "API_MT"
        }
    except Exception:
        return None

def get_popular_ids_api(pages, metrics, session):
    ids = []
    for page in range(1, pages+1):
        url = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=en-US&page={page}"
        t0 = time.time()
        r = session.get(url, timeout=20); metrics.record(time.time()-t0, len(r.content))
        results = (r.json() or {}).get("results", []) if r.ok else []
        for it in results:
            if it.get("id"): ids.append(int(it["id"]))
        time.sleep(DELAY + random.uniform(0.0, 0.25))
    return ids

def scrape_api_mt(max_movies=MAX_MOVIES):
    print("🚀 API (multithreaded)")
    metrics = Metrics(); session = make_session()
    ids = get_popular_ids_api(pages=8, metrics=metrics, session=session)[:max_movies*2]
    rows = []; start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS_API) as ex:
        futs = [ex.submit(fetch_movie_api, mid, metrics, session) for mid in ids[:max_movies]]
        for f in concurrent.futures.as_completed(futs):
            row = f.result()
            if row: rows.append(row)
    elapsed = round(time.time()-start, 2)
    m = metrics.summary(); m["elapsed_s"] = elapsed
    print(f"✅ API_MT done in {elapsed}s — {len(rows)} movies")
    return rows[:max_movies], m


# ---------- BS4 (robust listing → multithreaded details) ----------
def collect_ids_from_listing(pages, metrics, session):
    ids = []
    for page in range(1, pages+1):
        try:
            list_url = f"{BASE_URL}/movie?page={page}"
            t0 = time.time()
            r = session.get(list_url, timeout=20)
            metrics.record(time.time()-t0, len(r.content))
            if not r.ok: continue
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.select("div.card.style_1 h2 a, div.card.style_2 h2 a, div.card.style_3 h2 a"):
                mid = parse_id(a.get("href"))
                if mid: ids.append(mid)
            time.sleep(DELAY + random.uniform(0.0, 0.25))
        except requests.exceptions.RequestException:
            time.sleep(1.0); continue
    return ids

def fetch_bs4_detail(mid, metrics, session):
    url = f"{BASE_URL}/movie/{mid}"
    t0 = time.time()
    try:
        r = session.get(url, timeout=25); latency = time.time()-t0
        metrics.record(latency, len(r.content))
        if not r.ok: return None
        soup = BeautifulSoup(r.text, "html.parser")
        title_tag = soup.select_one("h2.title, h2 a, h2"); title = title_tag.text.strip() if title_tag else f"Movie {mid}"
        release_date = soup.select_one("span.release") or soup.select_one("span[itemprop='datePublished']")
        release_date = release_date.text.strip() if release_date else "N/A"
        overview_tag = soup.select_one("div.overview p, #overview p, div.facts p")
        overview = overview_tag.text.strip() if overview_tag else "N/A"
        genre_tags = soup.select("span.genres a, a[href*='/genre/']")
        genres = " | ".join([g.text.strip() for g in genre_tags]) if genre_tags else "N/A"
        rating_tag = soup.select_one("div.user_score_chart")  # data-percent 0..100
        rating = normalize_rating(rating_tag.get("data-percent")) if rating_tag else "N/A"
        poster_tag = soup.select_one("img.poster")
        poster_url = (poster_tag.get("data-src") or poster_tag.get("src")) if poster_tag else "N/A"
        return {
            "tmdb_id": mid, "title": title, "release_date": release_date, "rating": rating,
            "overview": overview, "genres": genres, "poster_url": poster_url, "movie_url": url, "method": "BS4_MT"
        }
    except requests.exceptions.RequestException:
        return None

def scrape_bs4_mt(max_movies=MAX_MOVIES):
    print("🚀 BS4 (multithreaded, robust)")
    metrics = Metrics(); session = make_session()
    ids = collect_ids_from_listing(pages=6, metrics=metrics, session=session)[:max_movies*2]
    if not ids:
        raise RuntimeError("No IDs collected from listing pages (check network/UFW/proxy).")
    rows = []; start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS_BS4) as ex:
        futs = [ex.submit(fetch_bs4_detail, mid, metrics, session) for mid in ids[:max_movies]]
        for f in concurrent.futures.as_completed(futs):
            row = f.result()
            if row: rows.append(row)
    elapsed = round(time.time()-start, 2)
    m = metrics.summary(); m["elapsed_s"] = elapsed
    print(f"✅ BS4_MT done in {elapsed}s — {len(rows)} movies")
    return rows[:max_movies], m


# ---------- Selenium (DETAIL pages; fills ALL fields) ----------
def safe_get(driver, url, retries=2, wait_between=3.0):
    for attempt in range(retries+1):
        try:
            driver.get(url)
            return True
        except Exception as e:
            if attempt == retries: return False
            time.sleep(wait_between)

def scrape_selenium_detail(max_movies=MAX_MOVIES, headless=True):
    """
    Selenium visits listing pages to collect movie URLs, then opens each detail page
    to extract ALL fields (release_date, overview, genres, rating, poster_url).
    More accurate than single-page; aligns with your Step-1 Selenium.  :contentReference[oaicite:5]{index=5}
    """
    print("🚀 Selenium (detail pages — full fields)")
    options = Options()
    if headless: options.add_argument("--headless=new")
    options.add_argument("--disable-gpu"); options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.page_load_strategy = "eager"
    # Disable image loading to save bandwidth; attributes are still present in DOM
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    metrics = Metrics(); rows = []; page = 1; visited = set(); start = time.time()
    try:
        while len(rows) < max_movies:
            list_url = f"{BASE_URL}/movie?page={page}"
            t0 = time.time()
            if not safe_get(driver, list_url):
                break
            html = driver.page_source
            metrics.record(time.time()-t0, len(html.encode("utf-8")))
            soup = BeautifulSoup(html, "html.parser")

            cards = soup.select("div.card.style_1, div.card.style_2, div.card.style_3")
            if not cards: break

            # gather candidate detail URLs from this page
            detail_urls = []
            for c in cards:
                a = c.select_one("h2 a[href^='/movie/']")
                if not a: continue
                href = a.get("href"); mid = parse_id(href)
                if mid and mid not in visited:
                    visited.add(mid)
                    detail_urls.append((mid, urljoin(BASE_URL, href)))

            # visit each detail page to fill all fields
            for mid, mu in detail_urls:
                if len(rows) >= max_movies: break
                t1 = time.time()
                if not safe_get(driver, mu):
                    continue
                html2 = driver.page_source
                metrics.record(time.time()-t1, len(html2.encode("utf-8")))
                psoup = BeautifulSoup(html2, "html.parser")

                title_tag = psoup.select_one("h2.title, h2 a, h2")
                title = title_tag.text.strip() if title_tag else f"Movie {mid}"
                release_date = psoup.select_one("span.release") or psoup.select_one(
                    "span[itemprop='datePublished']")
                release_date = release_date.text.strip() if release_date else "N/A"
                overview_tag = psoup.select_one("div.overview p, #overview p, div.facts p")
                overview = overview_tag.text.strip() if overview_tag else "N/A"
                genre_tags = psoup.select("span.genres a, a[href*='/genre/']")
                genres = " | ".join([g.text.strip() for g in genre_tags]) if genre_tags else "N/A"
                rating_tag = psoup.select_one("div.user_score_chart")   # 0-100 in data-percent
                rating = normalize_rating(rating_tag.get("data-percent")) if rating_tag else "N/A"
                poster_tag = psoup.select_one("img.poster")
                poster_url = (poster_tag.get("data-src") or poster_tag.get("src")) if poster_tag else "N/A"

                rows.append({
                    "tmdb_id": mid, "title": title, "release_date": release_date, "rating": rating,
                    "overview": overview, "genres": genres, "poster_url": poster_url,
                    "movie_url": mu, "method": "Selenium_MP"
                })
                time.sleep(DELAY + random.uniform(0.0, 0.25))

            page += 1
            time.sleep(DELAY + random.uniform(0.0, 0.25))
    finally:
        driver.quit()

    elapsed = round(time.time()-start, 2)
    m = metrics.summary(); m["elapsed_s"] = elapsed
    print(f"✅ Selenium_MP done in {elapsed}s — {len(rows)} movies")
    return rows[:max_movies], m


# ---------- Save helpers ----------
def save_rows(rows, path):
    pd.DataFrame(rows, columns=HEADERS).to_csv(path, index=False)
    print(f"💾 {path}")

def save_metrics_table(out_csv, *triples):
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["Method","Bandwidth(KB)","Time(s)","#Requests","Avg Latency(ms)"])
        for name, m in triples:
            if not m: continue
            w.writerow([name, round(m.get("bytes",0)/1024.0,2), m.get("elapsed_s",0), m.get("requests",0), m.get("avg_latency_ms",0.0)])
    print(f"💾 {out_csv}")


# ---------- Main ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=MAX_MOVIES)
    args = parser.parse_args()

    # API & BS4 stay multithreaded for throughput
    api_rows, api_m = scrape_api_mt(max_movies=args.max)
    bs4_rows, bs4_m = scrape_bs4_mt(max_movies=args.max)

    # Selenium now visits detail pages to fill ALL fields
    sel_rows, sel_m = scrape_selenium_detail(max_movies=args.max)

    save_rows(api_rows, "mt_movies_api.csv")
    save_rows(bs4_rows, "mt_movies_bs4.csv")
    save_rows(sel_rows, "mt_movies_selenium_mp.csv")

    save_metrics_table("mt_scrape_network_metrics.csv",
                       ("API_MT", api_m), ("BS4_MT", bs4_m), ("Selenium_MP", sel_m))

    with open("mt_scrape_metrics_summary.json","w",encoding="utf-8") as fp:
        json.dump({"api_mt": api_m, "bs4_mt": bs4_m, "selenium_mp": sel_m}, fp, indent=2)

    print("✅ Done. Metrics & CSVs saved.")
