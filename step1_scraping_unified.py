#!/usr/bin/env python3
"""
Step 1 — Unified Scraping Runner
- Implements API, BS4, and Selenium scrapers
- Ensures SAME fields across methods
- Saves per-method CSVs and a common join by tmdb_id
- Writes a compact network metrics CSV for Step 2

Meets: PDF Step-1 (a–e): Selenium, BS4, API; same fields; save to CSV.
"""

import os, re, time, csv, json, random, argparse
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

# --- Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- Requests robustness
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ============ CONFIG ============
BASE_URL = "https://www.themoviedb.org"
DEFAULT_API_KEY = "a69d199fd91304d9dfc454bc6e0dc3e9"  # fallback to your current constant
API_KEY = os.getenv("TMDB_API_KEY", DEFAULT_API_KEY)

MAX_MOVIES = 30
DELAY = 0.40  # polite delay (adds small jitter per request)
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# Canonical, identical across all three methods
HEADERS = [
    "tmdb_id", "title", "release_date", "rating",
    "overview", "genres", "poster_url", "movie_url", "method"
]

# Output artifacts
CSV_API = "movies_api.csv"
CSV_BS4 = "movies_bs4.csv"
CSV_SELENIUM = "movies_selenium.csv"
CSV_COMMON = "movies_common.csv"
CSV_NET = "scrape_network_metrics.csv"
JSON_SUMMARY = "scrape_metrics_summary.json"


# ============ METRICS ============
class Metrics:
    def __init__(self):
        self.request_count = 0
        self.total_bytes = 0
        self.latencies = []

    def record(self, latency_s: float, bytes_count: int):
        self.request_count += 1
        self.total_bytes += max(0, int(bytes_count))
        self.latencies.append(max(0.0, float(latency_s)))

    def summary(self):
        if self.latencies:
            avg_ms = 1000.0 * (sum(self.latencies) / len(self.latencies))
        else:
            avg_ms = 0.0
        return {
            "requests": self.request_count,
            "bytes": self.total_bytes,
            "avg_latency_ms": round(avg_ms, 2),
            "total_latency_s": round(sum(self.latencies), 3),
        }


# ============ HELPERS ============
def make_session():
    s = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    return s

def normalize_date(s):
    dt = pd.to_datetime(s, errors="coerce")
    return dt.strftime("%Y-%m-%d") if not pd.isna(dt) else "N/A"

def normalize_rating(x):
    try:
        v = float(x)
        if v > 10:  # guard against 0–100 scale
            v = v / 10.0
        return round(max(0.0, min(10.0, v)), 1)
    except Exception:
        return "N/A"

def parse_tmdb_id_from_url(url: str):
    m = re.search(r"/movie/(\d+)", url or "")
    return int(m.group(1)) if m else None

def save_csv_rows(rows, filename):
    df = pd.DataFrame(rows, columns=HEADERS)
    df.to_csv(filename, index=False, encoding="utf-8")
    print(f"✅ Saved {len(rows)} rows → {filename}")


# ============ API SCRAPER ============
def fetch_genre_map(session, metrics: Metrics):
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}&language=en-US"
    t0 = time.time()
    resp = session.get(url, timeout=15)
    metrics.record(time.time() - t0, len(resp.content))
    data = resp.json() if resp.ok else {"genres": []}
    return {g.get("id"): g.get("name") for g in data.get("genres", [])}

def scrape_api(max_movies=MAX_MOVIES):
    print("\n🎯 Starting API scraper...")
    session = make_session()
    metrics = Metrics()
    genre_map = fetch_genre_map(session, metrics)

    movies, page, fetched = [], 1, 0
    start_total = time.time()

    while fetched < max_movies:
        url = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=en-US&page={page}"
        t0 = time.time()
        resp = session.get(url, timeout=15)
        latency = time.time() - t0
        metrics.record(latency, len(resp.content))

        data = resp.json() if resp.ok else {}
        results = data.get("results", [])
        if not results:
            break

        for item in results:
            if fetched >= max_movies:
                break
            tmdb_id = item.get("id")
            genre_names = [genre_map.get(gid, str(gid)) for gid in item.get("genre_ids", [])]
            movies.append({
                "tmdb_id": tmdb_id,
                "title": item.get("title") or item.get("original_title", "N/A"),
                "release_date": normalize_date(item.get("release_date")),
                "rating": normalize_rating(item.get("vote_average")),
                "overview": item.get("overview") or "N/A",
                "genres": " | ".join(genre_names) if genre_names else "N/A",
                "poster_url": f"https://image.tmdb.org/t/p/w500{item.get('poster_path')}" if item.get("poster_path") else "N/A",
                "movie_url": f"{BASE_URL}/movie/{tmdb_id}" if tmdb_id else "N/A",
                "method": "API",
            })
            fetched += 1

        page += 1
        time.sleep(DELAY + random.uniform(0.0, 0.25))

    elapsed = round(time.time() - start_total, 2)
    msum = metrics.summary(); msum["elapsed_s"] = elapsed
    print(f"✅ API done in {elapsed}s — {len(movies)} movies, metrics: {msum}")
    return movies, msum


# ============ BS4 SCRAPER ============
def get_movie_details_requests(session, movie_url, metrics: Metrics):
    """Loads detail page to fill release_date, overview, genres, rating, poster_url."""
    try:
        t0 = time.time()
        resp = session.get(movie_url, timeout=20)
        latency = time.time() - t0
        metrics.record(latency, len(resp.content))
        soup = BeautifulSoup(resp.text, "html.parser")

        release_date = soup.select_one("span.release") or soup.select_one("span[itemprop='datePublished']")
        release_date = release_date.text.strip() if release_date else "N/A"

        overview_tag = soup.select_one("div.overview p, div.facts p, #overview p")
        overview = overview_tag.text.strip() if overview_tag else "N/A"

        genre_tags = soup.select("span.genres a, a[href*='/genre/']")
        genres = " | ".join([g.text.strip() for g in genre_tags]) if genre_tags else "N/A"

        rating_tag = soup.select_one("div.user_score_chart")  # data-percent (0–100)
        rating = normalize_rating(rating_tag.get("data-percent")) if rating_tag else "N/A"

        poster_tag = soup.select_one("img.poster")
        poster_url = (poster_tag.get("data-src") or poster_tag.get("src")) if poster_tag else "N/A"

        return release_date, overview, genres, rating, poster_url
    except Exception:
        return "N/A", "N/A", "N/A", "N/A", "N/A"

def scrape_bs4(max_movies=MAX_MOVIES):
    print("\n🎯 Starting BeautifulSoup scraper...")
    session = make_session()
    metrics = Metrics()
    movies, page = [], 1
    start_total = time.time()

    while len(movies) < max_movies:
        list_url = f"{BASE_URL}/movie?page={page}"
        t0 = time.time()
        resp = session.get(list_url, timeout=20)
        latency = time.time() - t0
        metrics.record(latency, len(resp.content))
        soup = BeautifulSoup(resp.text, "html.parser")

        cards = soup.select("div.card.style_1, div.card.style_2, div.card.style_3")
        if not cards:
            break

        for card in cards:
            if len(movies) >= max_movies:
                break
            a = card.select_one("h2 a[href^='/movie/']")
            if not a:
                continue
            movie_url = urljoin(BASE_URL, a.get("href"))
            tmdb_id = parse_tmdb_id_from_url(movie_url)
            title = a.text.strip()

            release_date, overview, genres, rating, poster_url = get_movie_details_requests(session, movie_url, metrics)

            movies.append({
                "tmdb_id": tmdb_id,
                "title": title,
                "release_date": release_date,
                "rating": rating,
                "overview": overview,
                "genres": genres,
                "poster_url": poster_url,
                "movie_url": movie_url,
                "method": "BS4",
            })
            time.sleep(DELAY + random.uniform(0.0, 0.25))
        page += 1

    elapsed = round(time.time() - start_total, 2)
    msum = metrics.summary(); msum["elapsed_s"] = elapsed
    print(f"✅ BS4 done in {elapsed}s — {len(movies)} movies, metrics: {msum}")
    return movies, msum


# ============ SELENIUM SCRAPER ============
def scrape_selenium(max_movies=MAX_MOVIES, headless=True):
    print("\n🎯 Starting Selenium scraper...")
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.page_load_strategy = "eager"
    # reduce bandwidth (no images)
    chrome_options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 20)

    metrics = Metrics()
    movies, page = [], 1
    start_total = time.time()

    try:
        while len(movies) < max_movies:
            list_url = f"{BASE_URL}/movie?page={page}"
            t0 = time.time()
            driver.get(list_url)
            latency = time.time() - t0
            html = driver.page_source
            metrics.record(latency, len(html.encode("utf-8")))

            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.card.style_1,div.card.style_2,div.card.style_3")))
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select("div.card.style_1, div.card.style_2, div.card.style_3")
            if not cards:
                break

            for card in cards:
                if len(movies) >= max_movies:
                    break
                a = card.select_one("h2 a[href^='/movie/']")
                if not a:
                    continue
                movie_url = urljoin(BASE_URL, a.get("href"))
                tmdb_id = parse_tmdb_id_from_url(movie_url)
                title = a.text.strip()

                t1 = time.time()
                driver.get(movie_url)
                latency_page = time.time() - t1
                html_movie = driver.page_source
                metrics.record(latency_page, len(html_movie.encode("utf-8")))

                psoup = BeautifulSoup(html_movie, "html.parser")
                release_date = psoup.select_one("span.release") or psoup.select_one(
                    "span[itemprop='datePublished']")
                release_date = release_date.text.strip() if release_date else "N/A"
                overview_tag = psoup.select_one("div.overview p, #overview p")
                overview = overview_tag.text.strip() if overview_tag else "N/A"
                genre_tags = psoup.select("span.genres a, a[href*='/genre/']")
                genres = " | ".join([g.text.strip() for g in genre_tags]) if genre_tags else "N/A"
                rating_tag = psoup.select_one("div.user_score_chart")
                rating = normalize_rating(rating_tag.get("data-percent")) if rating_tag else "N/A"
                poster_tag = psoup.select_one("img.poster")
                poster_url = (poster_tag.get("data-src") or poster_tag.get("src")) if poster_tag else "N/A"

                movies.append({
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "release_date": release_date,
                    "rating": rating,
                    "overview": overview,
                    "genres": genres,
                    "poster_url": poster_url,
                    "movie_url": movie_url,
                    "method": "Selenium",
                })
                time.sleep(DELAY + random.uniform(0.0, 0.25))
            page += 1
    finally:
        driver.quit()

    elapsed = round(time.time() - start_total, 2)
    msum = metrics.summary(); msum["elapsed_s"] = elapsed
    print(f"✅ Selenium done in {elapsed}s — {len(movies)} movies, metrics: {msum}")
    return movies, msum


# ============ SAVE / JOIN ============
def save_per_method(api_movies, bs4_movies, selenium_movies):
    save_csv_rows(api_movies, CSV_API)
    save_csv_rows(bs4_movies, CSV_BS4)
    save_csv_rows(selenium_movies, CSV_SELENIUM)


def build_common_by_id(api_movies, bs4_movies, selenium_movies):
    # Handle empty inputs - if any method was skipped, return empty
    if not api_movies or not bs4_movies or not selenium_movies:
        empty_df = pd.DataFrame(columns=["tmdb_id", "title", "release_date", "rating",
                                         "overview", "genres", "poster_url", "movie_url"])
        empty_df.to_csv(CSV_COMMON, index=False)
        print(f"⚠️  Skipped common join - not all methods were run")
        return 0

    # Side-by-side join by tmdb_id to keep intersection of all three
    a = pd.DataFrame(api_movies)
    b = pd.DataFrame(bs4_movies)
    s = pd.DataFrame(selenium_movies)

    a_ = a[["tmdb_id", "title", "release_date", "rating", "overview", "genres", "poster_url", "movie_url"]].rename(
        columns=lambda c: f"{c}_api" if c != "tmdb_id" else c)
    b_ = b[["tmdb_id", "title", "release_date", "rating", "overview", "genres", "poster_url", "movie_url"]].rename(
        columns=lambda c: f"{c}_bs4" if c != "tmdb_id" else c)
    s_ = s[["tmdb_id", "title", "release_date", "rating", "overview", "genres", "poster_url", "movie_url"]].rename(
        columns=lambda c: f"{c}_selenium" if c != "tmdb_id" else c)

    common = a_.merge(b_, on="tmdb_id").merge(s_, on="tmdb_id")

    # Also produce a tidy one‑row‑per‑movie CSV using API as canonical values
    tidy = common[["tmdb_id",
                   "title_api", "release_date_api", "rating_api",
                   "overview_api", "genres_api", "poster_url_api", "movie_url_api"]].rename(columns={
        "title_api": "title",
        "release_date_api": "release_date",
        "rating_api": "rating",
        "overview_api": "overview",
        "genres_api": "genres",
        "poster_url_api": "poster_url",
        "movie_url_api": "movie_url",
    })

    tidy.to_csv(CSV_COMMON, index=False)
    print(f"✅ Saved {len(tidy)} common rows → {CSV_COMMON}")
    return len(tidy)
def write_metrics_table(api_metrics, bs4_metrics, selenium_metrics):
    with open(CSV_NET, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Method", "Bandwidth(KB)", "Time(s)", "#Requests", "Avg Latency(ms)"])
        for name, m in [("API", api_metrics), ("BS4", bs4_metrics), ("Selenium", selenium_metrics)]:
            writer.writerow([
                name,
                round(m.get("bytes", 0) / 1024.0, 2),
                m.get("elapsed_s", 0),
                m.get("requests", 0),
                m.get("avg_latency_ms", 0.0),
            ])
    print(f"✅ Saved network metrics → {CSV_NET}")

def compare_and_save(api_res, bs4_res, selenium_res):
    api_movies, api_metrics = api_res
    bs4_movies, bs4_metrics = bs4_res
    selenium_movies, selenium_metrics = selenium_res

    # per‑method CSVs
    save_per_method(api_movies, bs4_movies, selenium_movies)

    # intersection by tmdb_id
    common_count = build_common_by_id(api_movies, bs4_movies, selenium_movies)

    # console summary
    print("\n🔍 Summary")
    print(f"{'Method':10s} | {'rows':4s} | {'req':3s} | {'bytes':10s} | {'avg_lat(ms)':10s} | {'time(s)':7s}")
    print("-" * 64)
    for name, mm, rows in [
        ("API", api_metrics, len(api_movies)),
        ("BS4", bs4_metrics, len(bs4_movies)),
        ("Selenium", selenium_metrics, len(selenium_movies)),
    ]:
        print(f"{name:10s} | {rows:4d} | {mm['requests']:3d} | {mm['bytes']:10d} | {mm['avg_latency_ms']:10.2f} | {mm.get('elapsed_s',0):7.2f}")
    print(f"{'Common':10s} | {common_count:4d} | {'-':3s} | {'-':10s} | {'-':10s} | {'-':7s}")

    # JSON metrics
    with open(JSON_SUMMARY, "w", encoding="utf-8") as fp:
        json.dump({
            "api": api_metrics,
            "bs4": bs4_metrics,
            "selenium": selenium_metrics,
            "common_rows": common_count
        }, fp, indent=2)
    print(f"✅ Saved JSON metrics → {JSON_SUMMARY}")

    # tabular metrics for Step 2
    write_metrics_table(api_metrics, bs4_metrics, selenium_metrics)


# ============ ENTRY POINT ============
def main(run_api=True, run_bs4=True, run_selenium=True, max_movies=MAX_MOVIES, headless=True):
    api_res = ([], {"requests": 0, "bytes": 0, "avg_latency_ms": 0, "elapsed_s": 0})
    bs4_res = ([], {"requests": 0, "bytes": 0, "avg_latency_ms": 0, "elapsed_s": 0})
    sel_res = ([], {"requests": 0, "bytes": 0, "avg_latency_ms": 0, "elapsed_s": 0})

    if run_api:
        try:
            api_res = scrape_api(max_movies=max_movies)
        except Exception as e:
            print("❌ API scraper failed:", e)

    if run_bs4:
        try:
            bs4_res = scrape_bs4(max_movies=max_movies)
        except Exception as e:
            print("❌ BS4 scraper failed:", e)

    if run_selenium:
        try:
            sel_res = scrape_selenium(max_movies=max_movies, headless=headless)
        except Exception as e:
            print("❌ Selenium scraper failed:", e)

    compare_and_save(api_res, bs4_res, sel_res)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--no-api", action="store_true")
    p.add_argument("--no-bs4", action="store_true")
    p.add_argument("--no-selenium", action="store_true")
    p.add_argument("--max", type=int, default=MAX_MOVIES)
    p.add_argument("--no-headless", action="store_true", help="Run Selenium with a visible browser")
    args = p.parse_args()

    main(
        run_api=not args.no_api,
        run_bs4=not args.no_bs4,
        run_selenium=not args.no_selenium,
        max_movies=args.max,
        headless=not args.no_headless
    )
