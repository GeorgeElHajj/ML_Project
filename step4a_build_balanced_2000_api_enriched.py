#!/usr/bin/env python3
# Step 4a — Build an enriched balanced 2 000-row TMDB dataset
import os, time, argparse, random, json
from collections import defaultdict
from typing import Dict, List, Set
import requests, pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config ----------
DEFAULT_API_KEY = "a69d199fd91304d9dfc454bc6e0dc3e9"
API_KEY = os.getenv("TMDB_API_KEY", DEFAULT_API_KEY)
DEFAULT_GENRES = ["Drama","Comedy","Action","Horror","Romance"]
TARGET_PER_GENRE = 400
LANGUAGE = "en-US"
MIN_VOTES = 50
SLEEP = (0.2,0.4)
OUT_CSV = "movies_tmdb_balanced_2000_enriched.csv"

# ---------- Session ----------
def make_session():
    s = requests.Session()
    r = Retry(total=8, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"Accept":"application/json"})
    return s

# ---------- API helpers ----------
def fetch_genre_map(s):
    url=f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}&language={LANGUAGE}"
    data=s.get(url,timeout=15).json()
    return {g["id"]:g["name"] for g in data.get("genres",[])}

def discover_by_genre(s,gid,page):
    url=("https://api.themoviedb.org/3/discover/movie"
         f"?api_key={API_KEY}&language={LANGUAGE}&sort_by=popularity.desc"
         f"&include_adult=false&vote_count.gte={MIN_VOTES}&with_genres={gid}&page={page}")
    return s.get(url,timeout=20).json()

def fetch_movie_details(s,mid,cache):
    if mid in cache: return cache[mid]
    url=f"https://api.themoviedb.org/3/movie/{mid}?api_key={API_KEY}&language={LANGUAGE}"
    try:
        r=s.get(url,timeout=15)
        if not r.ok: return {}
        data=r.json()
        cache[mid]=data
        return data
    except Exception:
        return {}

def normalize_row(rec,genre_map,primary_genre,detail):
    gids=rec.get("genre_ids") or []
    names=[genre_map.get(g,str(g)) for g in gids]
    genres_str=" | ".join([n for n in names if n]) or primary_genre
    poster=f"https://image.tmdb.org/t/p/w500{rec.get('poster_path')}" if rec.get("poster_path") else "N/A"
    return {
        "tmdb_id":rec.get("id"),
        "title":rec.get("title") or rec.get("original_title") or "N/A",
        "release_date":rec.get("release_date") or "N/A",
        "rating":float(rec.get("vote_average") or 0.0),
        "vote_count":int(detail.get("vote_count",rec.get("vote_count",0))),
        "popularity":float(detail.get("popularity",rec.get("popularity",0))),
        "runtime":detail.get("runtime",None),
        "budget":detail.get("budget",None),
        "revenue":detail.get("revenue",None),
        "original_language":detail.get("original_language","N/A"),
        "overview":rec.get("overview") or "N/A",
        "genres":genres_str,
        "poster_url":poster,
        "movie_url":f"https://www.themoviedb.org/movie/{rec.get('id')}",
        "primary_genre":primary_genre,
        "method":"API_ENRICHED"
    }

# ---------- Collector ----------
def collect_balanced(s,genre_map,target_per_genre,chosen):
    name2id={v.lower():k for k,v in genre_map.items()}
    rows,have,per_count=[],set(),defaultdict(int)
    cache={}

    for gname in chosen:
        gid=name2id.get(gname.lower())
        if not gid: raise ValueError(f"Genre '{gname}' not found")
        added,page=0,1
        print(f"\n🎯 Collecting {gname} (id={gid})… target={target_per_genre}")
        while added<target_per_genre:
            data=discover_by_genre(s,gid,page)
            results=data.get("results",[])
            if not results: break
            for rec in results:
                if added>=target_per_genre: break
                mid=rec.get("id")
                if not mid or mid in have: continue
                if not rec.get("overview"): continue
                detail=fetch_movie_details(s,mid,cache)
                if not detail: continue
                row=normalize_row(rec,genre_map,gname,detail)
                if row["rating"]<=0: continue
                rows.append(row); have.add(mid)
                per_count[gname]+=1; added+=1
                time.sleep(random.uniform(*SLEEP))
            page+=1
            if page>data.get("total_pages",1): break
        print(f"✅ {gname}: added {added}")
    print("Summary:",dict(per_count))
    return rows

# ---------- Main ----------
if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--genres",type=str,default="Drama,Comedy,Action,Horror,Romance")
    ap.add_argument("--per-genre",type=int,default=TARGET_PER_GENRE)
    ap.add_argument("--out",type=str,default=OUT_CSV)
    args=ap.parse_args()

    chosen=[g.strip() for g in args.genres.split(",") if g.strip()]
    if len(chosen)!=5: raise SystemExit("Please choose 5 genres")

    s=make_session()
    print("🔎 Fetching TMDB genre map…")
    genre_map=fetch_genre_map(s)
    print("✅ Available genres:",list(genre_map.values()))
    rows=collect_balanced(s,genre_map,args.per_genre,chosen)

    df=pd.DataFrame(rows).drop_duplicates("tmdb_id")
    TOTAL=args.per_genre*len(chosen)
    if len(df)>TOTAL:
        df=(df.groupby("primary_genre",group_keys=False)
              .apply(lambda g:g.sample(n=args.per_genre,random_state=42))
              .reset_index(drop=True))

    print(f"\n📦 Final dataset: {df.shape}")
    df.to_csv(args.out,index=False)
    print(f"💾 Saved: {args.out}")
