#!/usr/bin/env python3
# Step 4a — Build a balanced 2,000-row dataset from TMDB API (equal per 5 genres)
import os, time, argparse, random
from collections import defaultdict
from typing import Dict, List, Set

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------- Config ----------------------
DEFAULT_API_KEY = "a69d199fd91304d9dfc454bc6e0dc3e9"  # fallback
API_KEY = os.getenv("TMDB_API_KEY", DEFAULT_API_KEY)

DEFAULT_GENRES = ["Drama", "Comedy", "Action", "Horror", "Romance"]
TARGET_PER_GENRE = 400            # 5 * 400 = 2000
LANGUAGE = "en-US"
MIN_VOTES = 50
SLEEP = (0.15, 0.35)
OUT_CSV = "movies_tmdb_balanced_2000.csv"

# ---------------------- HTTP session w/ Retry ----------------------
def make_session() -> requests.Session:
    s = requests.Session()
    r = Retry(total=8, connect=8, read=8, backoff_factor=0.7,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=frozenset(["GET"]))
    s.mount("http://", HTTPAdapter(max_retries=r, pool_connections=20, pool_maxsize=20))
    s.mount("https://", HTTPAdapter(max_retries=r, pool_connections=20, pool_maxsize=20))
    s.headers.update({"Accept": "application/json"})
    return s

# ---------------------- API helpers ----------------------
def fetch_genre_map(session: requests.Session) -> Dict[int, str]:
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}&language={LANGUAGE}"
    r = session.get(url, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    return {g["id"]: g["name"] for g in data.get("genres", [])}

def discover_by_genre(session: requests.Session, gid: int, page: int) -> dict:
    url = (
        "https://api.themoviedb.org/3/discover/movie"
        f"?api_key={API_KEY}&language={LANGUAGE}"
        f"&sort_by=popularity.desc&include_adult=false&include_video=false"
        f"&with_genres={gid}&vote_count.gte={MIN_VOTES}&page={page}"
    )
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return r.json() or {}

def normalize_row(rec: dict, genre_map: Dict[int,str], primary_genre: str) -> dict:
    gids = rec.get("genre_ids") or []
    names = [genre_map.get(g, str(g)) for g in gids]
    genres_str = " | ".join([n for n in names if n]) if names else primary_genre
    poster_url = f"https://image.tmdb.org/t/p/w500{rec.get('poster_path')}" if rec.get("poster_path") else "N/A"
    return {
        "tmdb_id": rec.get("id"),
        "title": rec.get("title") or rec.get("original_title") or "N/A",
        "release_date": rec.get("release_date") or "N/A",
        "rating": float(rec.get("vote_average") or 0.0),
        "overview": rec.get("overview") or "N/A",
        "genres": genres_str or primary_genre,
        "poster_url": poster_url,
        "movie_url": f"https://www.themoviedb.org/movie/{rec.get('id')}",
        "primary_genre": primary_genre,
        "method": "API"
    }

def collect_balanced(session: requests.Session,
                     genre_map: Dict[int,str],
                     target_per_genre: int,
                     chosen_genres: List[str]) -> List[dict]:
    # ✅ FIXED: proper name→id map
    name2id = {name.lower(): gid for gid, name in genre_map.items()}
    rows: List[dict] = []
    have: Set[int] = set()
    per_genre_counts = defaultdict(int)

    for gname in chosen_genres:
        gname_norm = gname.strip().lower()
        if gname_norm not in name2id:
            raise ValueError(f"Genre '{gname}' not found in TMDB list. Available: {sorted(genre_map.values())}")
        gid = name2id[gname_norm]

        page = 1
        added_here = 0
        print(f"\n🎯 Collecting for genre: {gname} (id={gid}) target={target_per_genre}")
        while added_here < target_per_genre:
            data = discover_by_genre(session, gid, page)
            results = data.get("results", [])
            if not results:
                print(f"⚠️ No results on page {page}; stopping early.")
                break

            for rec in results:
                if added_here >= target_per_genre:
                    break
                mid = rec.get("id")
                if not mid or mid in have:
                    continue
                if not rec.get("overview") or not rec.get("title"):
                    continue
                row = normalize_row(rec, genre_map, primary_genre=gname)
                if row["overview"] == "N/A" or row["title"] == "N/A":
                    continue
                have.add(mid)
                rows.append(row)
                per_genre_counts[gname] += 1
                added_here += 1

            total_pages = int(data.get("total_pages", 1))
            page += 1
            if page > total_pages and added_here < target_per_genre:
                print(f"⚠️ Ran out of pages at {added_here}/{target_per_genre}. Restarting search from page 1.")
                page = 1
            elif added_here >= target_per_genre:
                break

            time.sleep(random.uniform(*SLEEP))

        print(f"✅ {gname}: added {added_here} movies.")

    print("\nSummary per genre:", dict(per_genre_counts))
    return rows

# ---------------------- Main ----------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--genres", type=str, default="Drama,Comedy,Action,Horror,Romance",
                    help="Comma-separated list of 5 genres to balance.")
    ap.add_argument("--per-genre", type=int, default=TARGET_PER_GENRE,
                    help="Target count per genre (default 400 → total 2000).")
    ap.add_argument("--language", type=str, default=LANGUAGE, help="TMDB language, e.g., en-US.")
    ap.add_argument("--min-votes", type=int, default=MIN_VOTES, help="vote_count.gte filter.")
    ap.add_argument("--out", type=str, default=OUT_CSV, help="Output CSV path.")
    args = ap.parse_args()

    chosen = [g.strip() for g in args.genres.split(",") if g.strip()]
    if len(chosen) != 5:
        raise SystemExit("Please pass exactly 5 genres via --genres")

    LANGUAGE = args.language
    session = make_session()

    print("🔎 Fetching TMDB genre map…")
    genre_map = fetch_genre_map(session)
    print("✅ Genres:", sorted(genre_map.values()))

    rows = collect_balanced(session, genre_map, args.per_genre, chosen)
    df = pd.DataFrame(rows).drop_duplicates(subset=["tmdb_id"]).reset_index(drop=True)

    df = df[(df["overview"].str.len() >= 20) & (df["rating"] > 0)]
    TOTAL = args.per_genre * len(chosen)
    if len(df) > TOTAL:
        df = (df.groupby("primary_genre", group_keys=False)
                .apply(lambda g: g.sample(n=args.per_genre, random_state=42))
                .reset_index(drop=True))

    print(f"\n📦 Final dataset shape: {df.shape} (expected total {TOTAL})")
    df.to_csv(args.out, index=False)
    print(f"💾 Saved: {args.out}")
    print("\nColumns:", list(df.columns))
    print(df.groupby("primary_genre").size())
