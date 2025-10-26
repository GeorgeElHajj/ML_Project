#!/usr/bin/env python3
# Step 3c — Proxy (Tor) runner with COMPLETE fields (detail-page fetch)
# Robust against Tor slowness: capped Selenium timeouts + fallback to requests.
import os, re, time, csv, json, argparse, random
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://www.themoviedb.org"
DEFAULT_API_KEY = "a69d199fd91304d9dfc454bc6e0dc3e9"
API_KEY = os.getenv("TMDB_API_KEY", DEFAULT_API_KEY)
TOR_PROXY = "socks5h://127.0.0.1:9050"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
DELAY = 0.25

# Selenium timeouts/tuning (you can tweak via env vars)
SEL_PAGELOAD_TIMEOUT = int(os.getenv("SEL_PAGELOAD_TIMEOUT", "60"))   # seconds (< urllib3 120s)
SEL_RETRIES = int(os.getenv("SEL_RETRIES", "2"))                      # retries per URL

HEADERS = ["tmdb_id","title","release_date","rating","overview","genres","poster_url","movie_url","method"]

class Metrics:
    def __init__(self): self.n=0; self.b=0; self.ts=[]
    def rec(self, dt, sz): self.n+=1; self.b+=max(0,int(sz)); self.ts.append(max(0.0,float(dt)))
    def sum(self):
        avg = (sum(self.ts)/len(self.ts)*1000.0) if self.ts else 0.0
        return {"requests": self.n, "bytes": self.b, "avg_latency_ms": round(avg,2), "total_latency_s": round(sum(self.ts),3)}

def norm_date(v):
    dt = pd.to_datetime(v, errors="coerce")
    return dt.strftime("%Y-%m-%d") if not pd.isna(dt) else "N/A"
def norm_rating(x):
    try:
        v=float(x); v = v/10.0 if v>10 else v; return round(max(0.0,min(10.0,v)),1)
    except: return "N/A"
def mid(path):
    m = re.search(r"/movie/(\d+)", path or ""); return int(m.group(1)) if m else None

def make_session(proxies=None):
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.8,
              status_forcelist=[429,500,502,503,504], allowed_methods=frozenset(["GET"]))
    s.mount("http://", HTTPAdapter(max_retries=r, pool_connections=50, pool_maxsize=50))
    s.mount("https://", HTTPAdapter(max_retries=r, pool_connections=50, pool_maxsize=50))
    s.headers.update({"User-Agent": UA, "Accept-Language":"en-US,en;q=0.9"})
    if proxies: s.proxies.update(proxies)
    return s

def check_tor():
    try:
        r = requests.get("https://check.torproject.org/api/ip",
                         proxies={"http":TOR_PROXY,"https":TOR_PROXY}, timeout=12)
        ok = r.ok
        print("✅ Tor reachable." if ok else "⚠️ Tor endpoint reachable but not verified.")
        return ok
    except Exception as e:
        print(f"❌ Tor not reachable: {e}")
        return False

# ---------- Genre ID → name map (API) ----------
def fetch_genre_map(session, metrics):
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}&language=en-US"
    t0 = time.time()
    r = session.get(url, timeout=20)
    metrics.rec(time.time()-t0, len(r.content))
    if not r.ok:
        return {}
    data = r.json() or {}
    return {g.get("id"): g.get("name") for g in data.get("genres", [])}

# -------- API via Tor ----------
def scrape_api(max_movies, proxies=None):
    print("🌐 API via", ("Tor" if proxies else "direct"))
    M = Metrics(); rows=[]; page=1; got=0; start=time.time()
    session = make_session(proxies)
    genre_map = fetch_genre_map(session, M)
    while got < max_movies:
        url = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=en-US&page={page}"
        t0=time.time(); r = session.get(url, timeout=20); M.rec(time.time()-t0, len(r.content))
        data = r.json() if r.ok else {}
        for it in data.get("results", []):
            if got>=max_movies: break
            _id = it.get("id")
            gids = it.get("genre_ids", []) or []
            gnames = [genre_map.get(g, str(g)) for g in gids]
            genres_str = " | ".join([g for g in gnames if g]) if gnames else "N/A"
            rows.append({
                "tmdb_id": _id,
                "title": it.get("title") or it.get("original_title","N/A"),
                "release_date": norm_date(it.get("release_date")),
                "rating": norm_rating(it.get("vote_average")),
                "overview": it.get("overview") or "N/A",
                "genres": genres_str,
                "poster_url": f"https://image.tmdb.org/t/p/w500{it.get('poster_path')}" if it.get("poster_path") else "N/A",
                "movie_url": f"{BASE_URL}/movie/{_id}" if _id else "N/A",
                "method": "API_TOR" if proxies else "API"
            })
            got+=1
        page+=1; time.sleep(DELAY + random.uniform(0.0,0.2))
    S = M.sum(); S["elapsed_s"]=round(time.time()-start,2)
    print(f"✅ API done in {S['elapsed_s']}s — {len(rows)} movies")
    return rows, S

# -------- BS4 via Tor (DETAIL PAGES) ----------
def get_ids_from_listing(max_ids, session, metrics):
    ids=[]; page=1
    while len(ids) < max_ids:
        list_url=f"{BASE_URL}/movie?page={page}"
        t0=time.time(); r = session.get(list_url, timeout=25); metrics.rec(time.time()-t0, len(r.content))
        if not r.ok: break
        soup=BeautifulSoup(r.text,"html.parser")
        cards = soup.select("div.card.style_1, div.card.style_2, div.card.style_3")
        for c in cards:
            a=c.select_one("h2 a[href^='/movie/']");
            if a and a.get("href"):
                i = mid(a.get("href"));
                if i: ids.append(i)
            if len(ids)>=max_ids: break
        page+=1; time.sleep(DELAY + random.uniform(0.0,0.25))
    return ids

def fetch_detail_bs4(mid_, session, metrics):
    url=f"{BASE_URL}/movie/{mid_}"
    t0=time.time(); r = session.get(url, timeout=30); metrics.rec(time.time()-t0, len(r.content))
    if not r.ok: return None
    soup=BeautifulSoup(r.text,"html.parser")
    title_tag=soup.select_one("h2.title, h2 a, h2"); title=title_tag.text.strip() if title_tag else f"Movie {mid_}"
    release_date = soup.select_one("span.release") or soup.select_one(
        "span[itemprop='datePublished']")
    release_date = release_date.text.strip() if release_date else "N/A"
    overview_tag = soup.select_one("div.overview p, #overview p, div.facts p"); overview = overview_tag.text.strip() if overview_tag else "N/A"
    genres_tags = soup.select("span.genres a, a[href*='/genre/']"); genres = " | ".join([g.text.strip() for g in genres_tags]) if genres_tags else "N/A"
    rating_tag = soup.select_one("div.user_score_chart"); rating = norm_rating(rating_tag.get("data-percent")) if rating_tag else "N/A"
    poster_tag = soup.select_one("img.poster"); poster_url = (poster_tag.get("data-src") or poster_tag.get("src")) if poster_tag else "N/A"
    return {
        "tmdb_id": mid_, "title": title, "release_date": release_date, "rating": rating,
        "overview": overview, "genres": genres, "poster_url": poster_url, "movie_url": url, "method": "BS4_TOR"
    }

def scrape_bs4(max_movies, proxies=None):
    print("🌐 BS4 via", ("Tor" if proxies else "direct"))
    session = make_session(proxies); M = Metrics(); rows=[]; start=time.time()
    ids = get_ids_from_listing(max_movies*2, session, M)[:max_movies]
    for i in ids:
        row = fetch_detail_bs4(i, session, M)
        if row: rows.append(row); time.sleep(DELAY + random.uniform(0.0,0.25))
    S = M.sum(); S["elapsed_s"]=round(time.time()-start,2)
    print(f"✅ BS4 done in {S['elapsed_s']}s — {len(rows)} movies")
    return rows, S

# -------- Selenium via Tor (DETAIL PAGES) ----------
def safe_get(driver, url, timeout=SEL_PAGELOAD_TIMEOUT, retries=SEL_RETRIES):
    """
    Robust navigation for slow Tor circuits:
    - cap page-load time
    - on TimeoutException, stop loading and use partial DOM
    - retry on WebDriver/transport errors
    Returns (ok: bool, mode: 'ok'|'partial'|'error', exc: Optional[Exception])
    """
    for attempt in range(retries + 1):
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            return True, "ok", None
        except TimeoutException as e:
            # stop and use what we have; DOMContentLoaded is usually present with pageLoadStrategy='eager'
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            return True, "partial", e
        except WebDriverException as e:
            if attempt >= retries:
                return False, "error", e
            time.sleep(3.0 + attempt * 2.0)
        except Exception as e:
            if attempt >= retries:
                return False, "error", e
            time.sleep(3.0 + attempt * 2.0)
    return False, "error", None

def scrape_selenium(max_movies, use_tor=True, headless=True):
    print("🧭 Selenium via", ("Tor" if use_tor else "direct"))
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080"); opts.page_load_strategy="eager"
    # reduce bandwidth/stalls
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--dns-prefetch-disable")
    opts.add_argument("--disable-client-side-phishing-detection")
    opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    if use_tor: opts.add_argument("--proxy-server=socks5://127.0.0.1:9050")

    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_script_timeout(60)

    # session for fallback when Selenium navigation fails
    fallback_session = make_session({"http": TOR_PROXY, "https": TOR_PROXY} if use_tor else None)

    M = Metrics(); rows=[]; page=1; start=time.time()
    try:
        # collect IDs from listing pages
        ids=[]
        while len(ids) < max_movies:
            url=f"{BASE_URL}/movie?page={page}"
            ok, mode, exc = safe_get(drv, url)
            t0 = time.time()
            html = drv.page_source
            M.rec(time.time()-t0, len(html.encode("utf-8")))
            if not ok and mode == "error":
                print(f"⚠️ Failed to open listing {url}: {exc}")
                break

            soup=BeautifulSoup(html,"html.parser")
            cards=soup.select("div.card.style_1, div.card.style_2, div.card.style_3")
            for c in cards:
                a=c.select_one("h2 a[href^='/movie/']");
                if a and a.get("href"):
                    i=mid(a.get("href"))
                    if i and i not in ids: ids.append(i)
                if len(ids)>=max_movies: break
            page+=1; time.sleep(DELAY + random.uniform(0.0,0.25))

        # visit each detail page
        for i in ids[:max_movies]:
            url=f"{BASE_URL}/movie/{i}"
            ok, mode, exc = safe_get(drv, url)
            t1 = time.time()
            html = drv.page_source
            M.rec(time.time()-t1, len(html.encode("utf-8")))
            if not ok and mode == "error":
                # fallback via requests over Tor (so we still fill fields)
                try:
                    rt0 = time.time()
                    rr = fallback_session.get(url, timeout=30)
                    M.rec(time.time()-rt0, len(rr.content))
                    if rr.ok:
                        s=BeautifulSoup(rr.text,"html.parser")
                    else:
                        print(f"⚠️ Fallback HTTP {rr.status_code} for {url}")
                        continue
                except Exception as e:
                    print(f"❌ Fallback failed for {url}: {e}")
                    continue
            else:
                s=BeautifulSoup(html,"html.parser")

            title_tag=s.select_one("h2.title, h2 a, h2"); title=title_tag.text.strip() if title_tag else f"Movie {i}"
            release_date = s.select_one("span.release") or s.select_one(
                "span[itemprop='datePublished']")
            release_date = release_date.text.strip() if release_date else "N/A"
            overview_tag = s.select_one("div.overview p, #overview p, div.facts p"); overview = overview_tag.text.strip() if overview_tag else "N/A"
            genres_tags = s.select("span.genres a, a[href*='/genre/']"); genres = " | ".join([g.text.strip() for g in genres_tags]) if genres_tags else "N/A"
            rating_tag = s.select_one("div.user_score_chart"); rating = norm_rating(rating_tag.get("data-percent")) if rating_tag else "N/A"
            poster_tag = s.select_one("img.poster"); poster_url = (poster_tag.get("data-src") or poster_tag.get("src")) if poster_tag else "N/A"

            rows.append({
                "tmdb_id": i,"title": title,"release_date": release_date,"rating": rating,
                "overview": overview,"genres": genres,"poster_url": poster_url,
                "movie_url": url,"method": "Selenium_TOR" if use_tor else "Selenium"
            })
            time.sleep(DELAY + random.uniform(0.0,0.3))
    finally:
        drv.quit()
    S = M.sum(); S["elapsed_s"]=round(time.time()-start,2)
    print(f"✅ Selenium done in {S['elapsed_s']}s — {len(rows)} movies")
    return rows, S

def save_rows(rows, path): pd.DataFrame(rows, columns=HEADERS).to_csv(path, index=False); print(f"💾 {path}")
def save_metrics_table(path, entries):
    with open(path,"w",newline="") as f:
        w=csv.writer(f); w.writerow(["Method","Bandwidth(KB)","Time(s)","#Requests","Avg Latency(ms)"])
        for name,m in entries:
            w.writerow([name, round(m.get("bytes",0)/1024.0,2), m.get("elapsed_s",0), m.get("requests",0), m.get("avg_latency_ms",0.0)])
    print(f"💾 {path}")

if __name__=="__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--proxy", choices=["tor","none"], default="tor")
    ap.add_argument("--max", type=int, default=30)
    args = ap.parse_args()

    use_tor = (args.proxy == "tor")
    proxies = {"http": TOR_PROXY, "https": TOR_PROXY} if (use_tor and check_tor()) else None

    api_rows, api_m = scrape_api(args.max, proxies=proxies)
    bs_rows, bs_m  = scrape_bs4(args.max, proxies=proxies)
    se_rows, se_m  = scrape_selenium(args.max, use_tor=(proxies is not None))

    save_rows(api_rows, "proxy_movies_api.csv")
    save_rows(bs_rows,  "proxy_movies_bs4.csv")
    save_rows(se_rows,  "proxy_movies_selenium.csv")
    save_metrics_table("proxy_scrape_network_metrics.csv", [
        (api_rows[0]["method"] if api_rows else ("API_TOR" if proxies else "API"), api_m),
        (bs_rows[0]["method"]  if bs_rows  else ("BS4_TOR" if proxies else "BS4"), bs_m),
        (se_rows[0]["method"]  if se_rows  else ("Selenium_TOR" if proxies else "Selenium"), se_m),
    ])

    with open("proxy_scrape_metrics_summary.json","w",encoding="utf-8") as fp:
        json.dump({"api":api_m,"bs4":bs_m,"selenium":se_m,"proxy":("tor" if proxies else "none")}, fp, indent=2)
    print("✅ Completed proxy run.")
