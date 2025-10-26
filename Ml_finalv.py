# -*- coding: utf-8 -*-
# ============================================================
# 📘 Step-4: TMDB Movie Intelligence — EDA + ML + Recommender
# Author: Georges Tia
# Environment: Google Colab / Python 3.10+
# ============================================================
# This notebook performs full movie data analysis, prediction,
# and recommendation using TMDB API-sourced dataset.
# ============================================================

# ============================================================
# 1️⃣ Introduction & Objectives
# ============================================================
# - Perform EDA (Exploratory Data Analysis) of movies dataset
# - Classify movies as High or Low rated based on overview/genres
# - Predict numerical rating using regression (XGBoost)
# - Cluster movies into semantic groups (KMeans)
# - Recommend similar movies using a hybrid method (text + genre + image)
# ============================================================

# ============================================================
# 2️⃣ Setup & Imports
# ============================================================

import os, re, json, random, requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
from typing import List, Tuple

# ML Imports
import nltk
from nltk.corpus import stopwords
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, r2_score, mean_absolute_error
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from scipy.sparse import hstack
from xgboost import XGBRegressor

# Image Processing
import cv2

# ============================================================
# 3️⃣ Load & Clean Data
# ============================================================
CSV_PATH = "movies_tmdb_balanced_2000_enriched.csv"
assert os.path.exists(CSV_PATH), f"❌ File not found: {CSV_PATH}. Upload it to Colab."

df = pd.read_csv(CSV_PATH)
print(f"✅ Loaded dataset: {CSV_PATH} | Shape = {df.shape}")

# Clean data
df = df.drop_duplicates(subset=["tmdb_id"]).dropna(subset=["overview", "genres", "rating"])
df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
df = df[df["rating"].between(0.1, 10)]
df = df[df["overview"].str.len() >= 20]
df["year"] = pd.to_datetime(df["release_date"], errors="coerce").dt.year.fillna(2000)
df["genres"] = df["genres"].astype(str)
print(f"📊 After cleaning: {df.shape}")

# ============================================================
# 4️⃣ Exploratory Data Analysis (EDA)
# ============================================================
plt.figure(figsize=(10,5))
genre_avg = (df.assign(genre1=df["genres"].str.split(r"\s*\|\s*").str[0])
               .groupby("genre1")["rating"].mean().sort_values(ascending=False).head(12))
sns.barplot(x=genre_avg.values, y=genre_avg.index)
plt.title("Average Rating by (Primary) Genre")
plt.xlabel("Average Rating"); plt.ylabel("Genre"); plt.tight_layout(); plt.show()

plt.figure(figsize=(10,4))
trend = df.dropna(subset=["year"]).groupby("year")["rating"].mean()
trend.plot(marker="o")
plt.title("Average Rating Over Time"); plt.ylabel("Average Rating"); plt.tight_layout(); plt.show()

plt.figure(figsize=(10,4))
top_genres = (df["genres"].str.split(r"\s*\|\s*").explode().value_counts().head(15))
sns.barplot(x=top_genres.values, y=top_genres.index)
plt.title("Most Common Genres"); plt.xlabel("Movies"); plt.tight_layout(); plt.show()

# ============================================================
# 5️⃣ Classification: High vs Low Rated
# ============================================================
nltk.download('stopwords')
STOPWORDS = stopwords.words('english')

text_series = (df["overview"].astype(str) + " " + df["genres"].astype(str))
tfidf = TfidfVectorizer(stop_words=STOPWORDS, max_features=8000, ngram_range=(1,2))
X_text = tfidf.fit_transform(text_series)

threshold = df["rating"].median()
df["high_rated"] = (df["rating"] >= threshold).astype(int)

X_train, X_test, y_train, y_test = train_test_split(X_text, df["high_rated"], test_size=0.2, random_state=42, stratify=df["high_rated"])
clf = LogisticRegression(max_iter=400)
clf.fit(X_train, y_train)
y_pred = clf.predict(X_test)

print("\n🤖 Improved Classification (High vs Low):")
print(classification_report(y_test, y_pred, digits=3))
cm = confusion_matrix(y_test, y_pred)
print("Confusion matrix:\n", cm)

# ============================================================
# 6️⃣ Regression: Predict Ratings (Improved XGBoost)
# ============================================================
df["overview_len"] = df["overview"].apply(lambda x: len(str(x).split()))
df["genre_count"] = df["genres"].apply(lambda x: len(str(x).split("|")) if isinstance(x,str) else 0)
df["is_recent"] = (df["year"] >= 2015).astype(int)

X_num = df[["overview_len", "genre_count", "year", "is_recent"]].values
scaler = StandardScaler()
X_num_scaled = scaler.fit_transform(X_num)

X_all = hstack([X_text, X_num_scaled])

Xtr_r, Xte_r, ytr_r, yte_r = train_test_split(X_all, df["rating"], test_size=0.2, random_state=42)
reg = XGBRegressor(n_estimators=500, learning_rate=0.05, max_depth=8, subsample=0.8, colsample_bytree=0.9, random_state=42)
reg.fit(Xtr_r, ytr_r)
pred_r = reg.predict(Xte_r)

print("\n📉 Tuned Regression (rating):")
print(f"  R²  = {r2_score(yte_r, pred_r):.3f}")
print(f"  MAE = {mean_absolute_error(yte_r, pred_r):.3f}")

# ============================================================
# 7️⃣ Clustering: Discover Genre/Content Groups
# ============================================================
k = 6
kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
clusters = kmeans.fit_predict(X_text)
df["cluster"] = clusters

print("\n🌀 Cluster rating means:")
print(df.groupby("cluster")["rating"].mean().round(2))

terms = np.array(tfidf.get_feature_names_out())
centers = kmeans.cluster_centers_
print("\n🔠 Top terms per cluster:")
for ci in range(k):
    idx = centers[ci].argsort()[-10:][::-1]
    print(f"  Cluster {ci}: {', '.join(terms[idx])}")

# ============================================================
# 8️⃣ Hybrid Recommender: Text + Genre + Poster Similarity
# ============================================================
def jaccard_genres(a: str, b: str) -> float:
    sa = set([x.strip() for x in str(a).split('|') if x.strip()])
    sb = set([x.strip() for x in str(b).split('|') if x.strip()])
    if not sa and not sb: return 0.0
    inter = len(sa & sb); union = len(sa | sb)
    return inter / union if union else 0.0

def fetch_and_hist(url: str) -> np.ndarray:
    if not url or url == 'N/A': return np.zeros(96, dtype=np.float32)
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200: return np.zeros(96, dtype=np.float32)
        img = np.frombuffer(resp.content, np.uint8)
        img = cv2.imdecode(img, cv2.IMREAD_COLOR)
        if img is None: return np.zeros(96, dtype=np.float32)
        img = cv2.resize(img, (128, 192))
        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
        hist_h = cv2.calcHist([hsv],[0],None,[32],[0,180]).flatten()
        hist_s = cv2.calcHist([hsv],[1],None,[32],[0,256]).flatten()
        hist_v = cv2.calcHist([hsv],[2],None,[32],[0,256]).flatten()
        vec = np.concatenate([hist_h, hist_s, hist_v]).astype(np.float32)
        return vec / (np.linalg.norm(vec)+1e-9)
    except Exception:
        return np.zeros(96, dtype=np.float32)

TEXT_SIM = cosine_similarity(X_text)

def recommend(title: str, n=8, alpha=0.7, beta=0.2, gamma=0.1, poster_k=150):
    hits = df[df['title'].str.lower() == title.lower()]
    if hits.empty:
        alt = df[df['title'].str.lower().str.contains(title.lower())].head(10)['title'].tolist()
        print(f"⚠️ Not found. Did you mean one of: {alt}")
        return None

    idx = hits.index[0]
    text_sim = TEXT_SIM[idx]
    candidates = np.argsort(-text_sim)[1:poster_k+1]

    q_poster = fetch_and_hist(df.at[idx, 'poster_url'])
    cand_poster = np.stack([fetch_and_hist(df.iloc[j]['poster_url']) for j in candidates], axis=0)
    poster_sim = cand_poster @ q_poster

    qg = df.at[idx, 'genres']
    genre_sim = np.array([jaccard_genres(qg, df.iloc[j]['genres']) for j in candidates], dtype=np.float32)

    t_sim = text_sim[candidates]
    hybrid = alpha*t_sim + beta*genre_sim + gamma*poster_sim

    order = np.argsort(-hybrid)[:n]
    rec_idx = candidates[order]
    out = df.iloc[rec_idx][['title','genres','rating','year']].copy()
    out['score'] = hybrid[order]
    print(f"\n🎬 Because you liked **{df.at[idx,'title']}**:")
    print(out.to_string(index=False))
    return out

# ============================================================
# 9️⃣ Summary & User Interaction
# ============================================================
try:
    user_title = input("\nType a movie you liked: ").strip()
    if user_title:
        _ = recommend(user_title, n=8)
    else:
        sample = df.sample(1).iloc[0]['title']
        print(f"(No input) Trying sample: {sample}")
        _ = recommend(sample, n=8)
except EOFError:
    sample = df.sample(1).iloc[0]['title']
    print(f"(No stdin) Trying sample: {sample}")
    _ = recommend(sample, n=8)

print("\n✅ Step-4 EDA + ML + Hybrid Recommendation finished (optimized).")
