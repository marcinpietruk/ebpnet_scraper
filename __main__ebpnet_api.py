import requests
import logging
import csv
import os
import json
import uuid
import base64
from pathlib import Path
from slugify import slugify
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# --- Config ---
BASE_URL = "https://ebpnet.be/nl/api/v1/guideline/search"
SITE_URL = "https://ebpnet.be"

# Current working directory
OUTPUT_DIR = os.getcwd()  # main folder
# PDFs subfolder
DOWNLOADS_DIR = os.path.join(OUTPUT_DIR, "pdfs")
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# Test mode: only process a few guidelines
TEST_MODE = True
MAX_TEST_GUIDELINES = 2

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- PDF Generation ---
def html_to_pdf(url: str, name: str) -> str:
    """
    Generate a PDF from the given URL, using `name` for the filename.
    Waits for the main content to load before printing.
    Returns the path to the saved PDF.
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=options)
    pdf_path = ""
    try:
        driver.get(url)

        # Wait up to 15 seconds for the main content to appear
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "editorial-text"))
        )

        # Optional: small sleep to ensure dynamic content loads
        time.sleep(2)

        # Filename from title + uuid
        filename = slugify(f"{name}-{str(uuid.uuid4())[:8]}")
        pdf_path = os.path.join(DOWNLOADS_DIR, f"{filename}.pdf")
        logger.info(f"Generating PDF for {name} at {pdf_path}")

        # Print to PDF
        print_options = webdriver.common.print_page_options.PrintOptions()
        print_options.background = False
        print_options.header_template = ""
        print_options.footer_template = ""
        pdf = driver.print_page(print_options)

        with open(pdf_path, "wb") as f:
            f.write(base64.b64decode(pdf))
    except Exception as e:
        logger.exception(f"Failed to generate PDF for {url}: {e}")
        return ""
    finally:
        driver.quit()

    return pdf_path

# --- API scraper functions ---
def fetch_all_guidelines() -> list[dict]:
    all_guidelines = []
    offset = 1
    limit = 100

    params = {
        'searchTerm': '',
        'professions': '',
        'sourceType': '',
        'sourceCategory': '44934',
        'publisher': '',
        'page[limit]': limit,
        'page[offset]': offset,
        'activity_ref': '',
        'published_date': 'desc'
    }

    while True:
        params['page[offset]'] = offset
        logger.info(f"Fetching page {offset} with limit {limit}")
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            guidelines = data.get('guidelines', [])

            if not guidelines:
                logger.info(f"No guidelines found at page {offset}. Stopping pagination.")
                break

            all_guidelines.extend(guidelines)
            pagination = data.get('pagination', {})
            total_pages = pagination.get('totalPages', 0)

            if total_pages > 0 and offset >= total_pages:
                break

            offset += 1
        except Exception as e:
            logger.error(f"Error at page {offset}: {e}")
            break

    logger.info(f"Total guidelines fetched: {len(all_guidelines)}")
    return all_guidelines

def filter_public_guidelines(guidelines: list[dict]) -> list[dict]:
    public_guidelines = []
    for guideline in guidelines:
        guideline_json = json.dumps(guideline)
        if '"isLoginOnly": false' in guideline_json or '"isLoginOnly":false' in guideline_json:
            public_guidelines.append(guideline)
    logger.info(f"Filtered to {len(public_guidelines)} public guidelines out of {len(guidelines)} total")
    return public_guidelines

def extract_guideline_data(guideline: dict) -> tuple:
    title = guideline.get('title', 'Unknown Title')

    dates = guideline.get('dates', {})
    date = dates.get('publishedBySource', '') if dates else ''

    publishers = guideline.get('publishers', [])
    if publishers:
        if isinstance(publishers, list):
            publisher_names = []
            for pub in publishers:
                if isinstance(pub, str):
                    publisher_names.append(pub)
                elif isinstance(pub, dict):
                    publisher_names.append(pub.get('name', pub.get('label', '')))
            publisher = ' | '.join(filter(None, publisher_names))
        else:
            publisher = str(publishers)
    else:
        publisher = ''

    metadata = guideline.get('metadata', {})
    professions = ""
    if metadata:
        profession_list = metadata.get('professions', [])
        if isinstance(profession_list, list):
            prof_names = []
            for prof in profession_list:
                if isinstance(prof, str):
                    prof_names.append(prof)
                elif isinstance(prof, dict):
                    prof_names.append(prof.get('name', prof.get('label', '')))
            professions = ', '.join(filter(None, prof_names))
        elif isinstance(profession_list, str):
            professions = profession_list

    type_info = guideline.get('type', {})
    source_label = type_info.get('label', '') if type_info else ''
    source_type = type_info.get('sourceType', '') if type_info else ''

    # Only return fields needed for CSV
    return title, date, publisher, professions, source_label, source_type

def process_guidelines(public_guidelines: list[dict]) -> list[tuple]:
    detailed_results = []
    guidelines_to_process = public_guidelines
    if TEST_MODE:
        guidelines_to_process = public_guidelines[:MAX_TEST_GUIDELINES]
        logger.info(f"Test mode enabled: only processing first {MAX_TEST_GUIDELINES} guidelines")

    for i, guideline in enumerate(guidelines_to_process, 1):
        title = guideline.get('title', 'Unknown Title')
        logger.info(f"Processing {i}/{len(guidelines_to_process)}: {title}")
        result = extract_guideline_data(guideline)

        # Keep full URL for PDF generation
        frontend_url = guideline.get('frontendUrl', '')
        full_url = f"{SITE_URL}{frontend_url}" if frontend_url else ''
        pdf_path = ""
        if full_url:
            pdf_path = html_to_pdf(full_url, title)

        # Add PDF path to CSV, URL itself is not in CSV
        detailed_results.append(result + (pdf_path,))

    return detailed_results

def save_detailed_csv(detailed_results: list[tuple], filename: str = "public_guidelines_detailed.csv"):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, filename)

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'title', 'published_date', 'publisher',
            'professions', 'source_label', 'source_type', 'pdf_path'
        ])
        writer.writerows(detailed_results)

    logger.info(f"Saved detailed information for {len(detailed_results)} guidelines to {csv_path}")
    return csv_path

# --- Main ---
def main():
    logger.info("Starting EBPNet guideline scraper with PDF generation")

    all_guidelines = fetch_all_guidelines()
    if not all_guidelines:
        logger.error("No guidelines were fetched.")
        return

    public_guidelines = filter_public_guidelines(all_guidelines)
    if not public_guidelines:
        logger.warning("No public guidelines found (all require login).")
        return

    detailed_results = process_guidelines(public_guidelines)
    csv_path = save_detailed_csv(detailed_results)

    logger.info("Processing complete!")
    logger.info(f"Results saved to: {csv_path}")

if __name__ == "__main__":
    main()
