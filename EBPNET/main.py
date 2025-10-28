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
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import concurrent.futures
import threading
from functools import partial

# --- Config ---
BASE_URL = "https://ebpnet.be/nl/api/v1/guideline/search"
SITE_URL = "https://ebpnet.be"
MAX_WORKERS = 5  # Number of parallel threads for PDF generation
CSV_FILENAME = "Ebpnet.csv"

# API & Selector Config (for universal settings)
API_PAYLOAD = {
    'searchTerm': '',
    'professions': '',
    'sourceType': '',
    'sourceCategory': '44934',  # API filter for specific guidelines
    'publisher': '',
    'page[limit]': 100,
    
    'activity_ref': '',
    'published_date': 'desc'
}

# Selectors for PDF generation
CONTENT_SELECTOR = (By.CLASS_NAME, "editorial-text")
PDF_LINK_SELECTOR = (By.XPATH, "//a[contains(@class, 'btn-blue') and contains(translate(., 'PDF', 'pdf'), 'pdf')]")
# This XPath selector waits for EITHER of the above elements to exist
WAIT_FOR_ELEMENT_XPATH = "//*[contains(@class, 'editorial-text') or (self::a and contains(@class, 'btn-blue') and contains(translate(., 'PDF', 'pdf'), 'pdf'))]"

# Current working directory
OUTPUT_DIR = os.getcwd()  # main folder
# PDFs subfolder
DOWNLOADS_DIR = os.path.join(OUTPUT_DIR, "pdfs")
os.makedirs(DOWNLOADS_DIR, exist_ok=True)
# Path to the output CSV
CSV_PATH = os.path.join(OUTPUT_DIR, CSV_FILENAME)

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger('webdriver_manager').setLevel(logging.WARNING)
logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)

# --- PDF Generation (Refactored) ---

def _print_page_to_pdf(driver: webdriver.Chrome, output_path: str):
    """(Helper) Prints the currently loaded Selenium page to a PDF file."""
    logger.info(f"Generating PDF from page content at {output_path}")
    print_options = webdriver.common.print_page_options.PrintOptions()
    print_options.background = False
    print_options.header_template = ""
    print_options.footer_template = ""
    pdf = driver.print_page(print_options)

    with open(output_path, "wb") as f:
        f.write(base64.b64decode(pdf))

def _download_linked_pdf(driver: webdriver.Chrome, output_path: str):
    """(Helper) Finds element by PDF_LINK_SELECTOR and downloads its linked file."""
    logger.info("Found 'Open pdf' button. Downloading file...")
    try:
        # Use the selector from config
        pdf_link_element = driver.find_element(*PDF_LINK_SELECTOR)
        pdf_url = pdf_link_element.get_attribute('href')
        
        if not pdf_url:
            raise Exception("Found link element but 'href' attribute was empty.")
            
        logger.info(f"Downloading from {pdf_url}")
        pdf_response = requests.get(pdf_url)
        pdf_response.raise_for_status()
        
        with open(output_path, "wb") as f:
            f.write(pdf_response.content)
        logger.info(f"Successfully downloaded and saved PDF to {output_path}")
        
    except Exception as e_download:
        logger.error(f"Found 'Open pdf' button but failed to download: {e_download}")
        raise  # Re-raise the exception to be caught by the main function

def save_guideline_pdf(url: str, name: str) -> str:
    """
    Saves a guideline as a PDF.
    
    Tries to print the page to PDF. If that fails,
    it tries to download a linked PDF instead.
    Returns the path to the saved file or "" on failure.
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
 

    driver = None
    filename = slugify(f"{name}-{str(uuid.uuid4())[:8]}")
    pdf_path = os.path.join(DOWNLOADS_DIR, f"{filename}.pdf")
    
    try:
        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()), 
            options=options
        )
        driver.get(url)

        
        wait_condition = (By.XPATH, WAIT_FOR_ELEMENT_XPATH)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located(wait_condition))
        

        
        try:
            # CASE 1: Try to find the main text content first
            # Use the selector from config
            driver.find_element(*CONTENT_SELECTOR)
            _print_page_to_pdf(driver, pdf_path)
            
        except:
            # CASE 2: Main text not found, try to download the linked file
            _download_linked_pdf(driver, pdf_path)

    except Exception as e:
        logger.exception(f"Failed to process PDF for {url}: {e}")
        return "" 
    finally:
        if driver:
            driver.quit()

    return pdf_path

# --- API scraper functions ---
def fetch_all_guidelines() -> list[dict]:
    all_guidelines = []
    offset = 1
    # Use API_PAYLOAD from config
    limit = API_PAYLOAD.get('page[limit]', 100)
    params = API_PAYLOAD.copy()

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
        # Check for both keys
        if guideline.get('isLoginOnly') is False and guideline.get('isRedirectedExternally') is False:
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

    # Return fields for CSV
    return title, date, publisher, professions, source_label, source_type

# --- CSV and Resume Functions ---

CSV_HEADER = [
    'title', 'published_date', 'publisher', 'professions', 
    'source_label', 'source_type', 'frontend_url', 'pdf_path'
]
# Use the frontend_url (column index 6) as the unique key
UNIQUE_KEY_COLUMN_INDEX = 6 

def initialize_csv(csv_path: str):
    """Creates the CSV file with a header if it doesn't exist."""
    if not os.path.exists(csv_path):
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(CSV_HEADER)
            logger.info(f"Created new CSV file at {csv_path}")
        except IOError as e:
            logger.error(f"Failed to create CSV file: {e}")
            raise

def load_processed_guidelines(csv_path: str) -> set[str]:
    """Reads the CSV and returns a set of processed 'frontend_url's for skipping."""
    processed_urls = set()
    if not os.path.exists(csv_path):
        return processed_urls

    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None) # Skip header

            if not header or header != CSV_HEADER:
                logger.warning("CSV file has an invalid or missing header. Starting fresh.")
                return processed_urls

            for row in reader:
                if len(row) > UNIQUE_KEY_COLUMN_INDEX:
                    processed_urls.add(row[UNIQUE_KEY_COLUMN_INDEX])
                    
    except Exception as e:
        logger.error(f"Error reading CSV file at {csv_path}: {e}. Treating as empty.")
        return set() # Return empty set on error

    logger.info(f"Loaded {len(processed_urls)} processed guidelines from CSV.")
    return processed_urls

def append_to_csv(csv_path: str, data_row: tuple, lock: threading.Lock):
    """Appends a single row to the CSV file in a thread-safe manner."""
    try:
        with lock:
            with open(csv_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(data_row)
    except IOError as e:
        logger.error(f"Failed to append row to CSV: {e}")

# --- Processing Functions ---

def worker_process_and_save(guideline: dict, csv_path: str, csv_lock: threading.Lock):
    """
    Single worker function to be run in a thread.
    Processes one guideline, generates its PDF, and saves it to the CSV.
    """
    title = guideline.get('title', 'Unknown Title')
    try:
        # 1. Extract metadata
        result_data = extract_guideline_data(guideline)

        # 2. Get URL and generate PDF
        frontend_url = guideline.get('frontendUrl', '')
        full_url = f"{SITE_URL}{frontend_url}" if frontend_url else ''
        pdf_path = ""
        if full_url:
            # Use the new refactored function
            pdf_path = save_guideline_pdf(full_url, title)

        # 3. Combine all data
        final_row = result_data + (frontend_url, pdf_path)

        # 4. Save to CSV (thread-safe)
        append_to_csv(csv_path, final_row, csv_lock)
        
        if pdf_path: # Log success only if PDF was actually created
            logger.info(f"Successfully processed and saved: {title}")
        else:
            logger.warning(f"Processed {title}, but PDF failed to generate.")
            
    except Exception as e:
        logger.error(f"Failed to process guideline {title}: {e}")


def process_guidelines(public_guidelines: list[dict], processed_urls: set[str], csv_path: str):
    """
    Processes all new guidelines using a thread pool.
    """
    csv_lock = threading.Lock()
    
    # Filter out guidelines that are already processed or have no URL
    guidelines_to_process = []
    for g in public_guidelines:
        url = g.get('frontendUrl', '')
        title = g.get('title', 'Unknown Title')
        
        if not url:
            logger.warning(f"Skipping guideline with no frontendUrl: {title}")
            continue
            
        if url in processed_urls:
            continue
            
        guidelines_to_process.append(g)

    total_to_process = len(guidelines_to_process)
    if total_to_process == 0:
        logger.info("No new guidelines to process.")
        return

    logger.info(f"Found {total_to_process} new guidelines to process. Starting {MAX_WORKERS} workers...")

    # Use functools.partial to "pre-load" the csv_path and csv_lock arguments
    task = partial(worker_process_and_save, csv_path=csv_path, csv_lock=csv_lock)

    # Run tasks in a thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(executor.map(task, guidelines_to_process))
        
    logger.info(f"Finished processing all {total_to_process} guidelines.")


# --- Main ---
def main():
    logger.info("Starting EBPNet guideline scraper")

    # Ensure CSV file exists with header
    initialize_csv(CSV_PATH)

    # Load set of already processed URLs to skip them
    processed_urls = load_processed_guidelines(CSV_PATH)

    # Fetch all guideline data from API
    all_guidelines = fetch_all_guidelines()
    if not all_guidelines:
        logger.error("No guidelines were fetched.")
        return

    # Filter for public guidelines only
    public_guidelines = filter_public_guidelines(all_guidelines)
    if not public_guidelines:
        logger.warning("No public guidelines found (all require login or are external).")
        return

    # Process all new guidelines using threads
    process_guidelines(public_guidelines, processed_urls, CSV_PATH)

    logger.info("Processing complete!")
    logger.info(f"Results saved to: {CSV_PATH}")
    logger.info(f"PDFs saved in: {DOWNLOADS_DIR}")

if __name__ == "__main__":
    main()
