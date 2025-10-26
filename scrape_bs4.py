#!/usr/bin/env python3
from step1_scraping_unified import scrape_bs4, save_csv_rows, HEADERS, CSV_BS4

if __name__ == "__main__":
    movies, metrics = scrape_bs4()
    save_csv_rows(movies, CSV_BS4)
    print("BS4 metrics:", metrics)
