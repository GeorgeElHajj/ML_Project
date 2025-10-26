#!/usr/bin/env python3
from step1_scraping_unified import scrape_selenium, save_csv_rows, HEADERS, CSV_SELENIUM

if __name__ == "__main__":
    movies, metrics = scrape_selenium(headless=True)
    save_csv_rows(movies, CSV_SELENIUM)
    print("Selenium metrics:", metrics)
