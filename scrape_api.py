#!/usr/bin/env python3
from step1_scraping_unified import scrape_api, save_csv_rows, HEADERS, CSV_API

if __name__ == "__main__":
    movies, metrics = scrape_api()
    save_csv_rows(movies, CSV_API)
    print("API metrics:", metrics)
