
import pandas as pd
import time
import random
from tqdm import tqdm
import re
import os
import sys
import logging
import argparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse

# --- Helper function for normalization ---
def normalize_name(name):
    """Normalise un nom d'entreprise pour la comparaison avec un domaine."""
    name = name.lower()
    name = re.sub(r'[^a-z0-9]', '', name)
    return name

# --- Setup Logging ---
log_file = 'website_finder.log'
# Clear log file at the beginning of a full run
# This logic needs to be careful if run in a pipeline - maybe clear only on explicit start
# For now, keeping as is, but noting for future
if not ('--limit' in sys.argv or os.path.exists('websites_selenium_results.csv')):
    if os.path.exists(log_file):
        os.remove(log_file)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

def _has_linux_chrome():
    """Vérifie si un Chrome/Chromium Linux est disponible."""
    import shutil
    return any(shutil.which(b) for b in ('google-chrome', 'google-chrome-stable', 'chromium-browser', 'chromium'))

def _get_win_chrome_version():
    """Détecte la version de Chrome Windows depuis le dossier d'installation."""
    chrome_dir = '/mnt/c/Program Files/Google/Chrome/Application'
    if os.path.isdir(chrome_dir):
        for entry in os.listdir(chrome_dir):
            if entry[0].isdigit() and '.' in entry:
                return entry  # ex: "144.0.7559.133"
    return None

def _get_wsl_chromedriver(chrome_version):
    """Télécharge le chromedriver win64 compatible pour WSL et retourne son chemin."""
    import zipfile
    import urllib.request

    # Le .exe doit être sur le filesystem Windows pour s'exécuter depuis WSL
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cache_dir = os.path.join(script_dir, '.chromedriver', chrome_version)
    driver_path = os.path.join(cache_dir, 'chromedriver.exe')
    if os.path.exists(driver_path):
        return driver_path

    os.makedirs(cache_dir, exist_ok=True)
    url = f'https://storage.googleapis.com/chrome-for-testing-public/{chrome_version}/win64/chromedriver-win64.zip'
    logging.info(f"Downloading win64 chromedriver from {url}")
    zip_path = os.path.join(cache_dir, 'chromedriver.zip')
    urllib.request.urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for member in zf.namelist():
            if member.endswith('chromedriver.exe'):
                with zf.open(member) as src, open(driver_path, 'wb') as dst:
                    dst.write(src.read())
                break
    os.remove(zip_path)
    os.chmod(driver_path, 0o755)
    logging.info(f"Chromedriver saved to {driver_path}")
    return driver_path

DIRECTORY_DOMAINS = {
    'societe.com', 'pagesjaunes.fr', 'pappers.fr',
    'annuaire-entreprises.data.gouv.fr', 'verif.com',
    'entreprises.lefigaro.fr', 'fr.kompass.com', 'facebook.com',
    'linkedin.com', 'youtube.com', 'wikipedia.org', 'doctrine.fr',
    'app.dataprospects.fr', 'service-de-reparation-de-bateaux.autour-de-moi.com',
    'entreprises.lagazettefrance.fr', 'reseauexcellence.fr', 'actunautique.com'
}

def _is_not_french_url(url: str) -> bool:
    """Retourne True si l'URL est clairement non-française (à rejeter).

    Rejette :
    - les chemins en version anglaise : /en/, /en-gb/, etc.
    - les TLD canadiens : .ca
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace('www.', '')
        path = parsed.path.lower()
        if re.search(r'/(en|en-[a-z]{2})(/|$)', path):
            return True
        if domain.endswith('.ca'):
            return True
        return False
    except Exception:
        return False

def _tld_priority(url: str) -> int:
    """Priorité de TLD pour trier les candidats. Valeur basse = meilleur.

    .fr  → 0  (priorité maximale, site clairement français)
    autres → 1
    """
    try:
        domain = urlparse(url).netloc.lower().replace('www.', '')
        return 0 if domain.endswith('.fr') else 1
    except Exception:
        return 1

def get_website_with_selenium(denomination: str):
    """
    Performs a DuckDuckGo search and validates results using keyword matching.
    Returns status, URL, and rank.
    """
    driver = None 
    search_query = denomination
    logging.info(f"Initiating Selenium search for: '{search_query}'")
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        # Use Chrome for Testing (portable Linux) if available, else system Chrome
        cft_chrome = os.path.expanduser('~/.chrome-for-testing/chrome-linux64/chrome')
        cft_driver = os.path.expanduser('~/.chrome-for-testing/chromedriver-linux64/chromedriver')
        if os.path.exists(cft_chrome) and os.path.exists(cft_driver):
            chrome_options.binary_location = cft_chrome
            logging.info(f"Using Chrome for Testing: {cft_chrome}")
            driver = webdriver.Chrome(
                service=ChromeService(executable_path=cft_driver),
                options=chrome_options,
            )
        else:
            driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        
        url_to_search = f"https://duckduckgo.com/?q={search_query}&ia=web"
        logging.info(f"Navigating to {url_to_search}")
        driver.get(url_to_search)
        
        time.sleep(random.uniform(1, 3))
        
        results = driver.find_elements(By.CSS_SELECTOR, 'a[data-testid="result-title-a"]')
        logging.info(f"Found {len(results)} potential results.")
        
        if not results:
            logging.warning("No search results found on page.")
            return 'NON TROUVÉ', None, None

        stop_words = {'sa', 'sas', 'sarl', 'eurl', 'snc', 'ste', 'et', 'de', 'la', 'les', 'des'}

        # Collecte tous les candidats valides parmi les 5 premiers résultats,
        # puis choisit le meilleur en préférant les TLD .fr aux autres.
        candidates = []  # liste de (tld_priority, rank, url)
        keywords = [word for word in re.split(r'[\s-]+', denomination) if word.lower() not in stop_words and len(word) > 2]

        for rank, result in enumerate(results[:5], 1):
            url = result.get_attribute('href')
            if not url:
                continue

            domain = urlparse(url).netloc.replace('www.', '')
            cleaned_domain = domain.replace('.', '').replace('-', '')
            logging.info(f"Checking URL: {url} (Domain: {domain}) for '{denomination}'")

            if domain in DIRECTORY_DOMAINS:
                logging.warning(f"URL is a known directory: {domain}. Skipping.")
                continue

            if _is_not_french_url(url):
                logging.warning(f"URL {url} rejetée (non-française : chemin /en/ ou TLD .ca).")
                continue

            is_match_found = False
            for keyword in keywords:
                normalized_keyword = normalize_name(keyword)
                if normalized_keyword and normalized_keyword in cleaned_domain:
                    logging.info(f"Keyword '{normalized_keyword}' from '{denomination}' found in domain '{domain}'.")
                    is_match_found = True
                    break

            if is_match_found:
                candidates.append((_tld_priority(url), rank, url))
            else:
                logging.info(f"URL {url} does not match any keyword from '{denomination}'. Skipping.")

        if candidates:
            # Tri : d'abord par priorité TLD (.fr=0 avant les autres=1), puis par rang DDG
            candidates.sort(key=lambda x: (x[0], x[1]))
            best_priority, best_rank, best_url = candidates[0]
            logging.info(f"Meilleur candidat retenu : {best_url} (TLD priority={best_priority}, rank={best_rank})")
            return 'TROUVÉ', best_url, best_rank

        logging.warning("No valid French URL found after checking top 5 results.")
        return 'NON TROUVÉ', None, None

    except Exception as e:
        logging.error(f"An error occurred during Selenium search for query '{search_query}': {e}", exc_info=True)
        return 'ERREUR', None, None
    finally:
        if driver:
            driver.quit()

def main(input_csv_path, output_dir, limit=None):
    """
    Finds company websites using Selenium and saves results to a CSV file.

    Args:
        input_csv_path (str): Path to the input CSV file containing company data.
        output_dir (str): Directory where the output CSV will be saved.
        limit (int, optional): Limit the number of companies to process for testing. Defaults to None.
    """
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.basename(input_csv_path)
    name_without_ext = os.path.splitext(base_name)[0]
    output_filename = os.path.join(output_dir, f"{name_without_ext}_websites.csv")

    logging.info(f"Script started. Input: '{input_csv_path}', Output: '{output_filename}', Limit: {limit}")

    try:
        df_input = pd.read_csv(input_csv_path)
    except FileNotFoundError:
        logging.error(f"The input file '{input_csv_path}' was not found.")
        return

    if os.path.exists(output_filename):
        logging.info(f"Found existing results file: '{output_filename}'. Resuming...")
        df_output = pd.read_csv(output_filename)
        # Ensure 'site_web', 'statut_recherche', and 'source_site_web' columns exist after reading
        if 'site_web' not in df_output.columns:
            df_output['site_web'] = ''
        if 'statut_recherche' not in df_output.columns:
            df_output['statut_recherche'] = ''
        if 'source_site_web' not in df_output.columns: # NEW COLUMN
            df_output['source_site_web'] = ''
    else:
        logging.info("No existing results file found. Starting from scratch.")
        df_output = df_input.copy()
        df_output['site_web'] = ''
        df_output['statut_recherche'] = ''
        df_output['source_site_web'] = '' # NEW COLUMN

    df_output['site_web'] = df_output['site_web'].fillna('')
    df_output['statut_recherche'] = df_output['statut_recherche'].fillna('')
    df_output['source_site_web'] = df_output['source_site_web'].fillna('') # NEW COLUMN

    # Determine which rows to process: only those not yet searched or with ERREUR status to retry
    rows_to_process = df_output[df_output['statut_recherche'].isin(['', 'ERREUR'])].copy()
    
    if limit:
        rows_to_process = rows_to_process.head(limit)

    if rows_to_process.empty:
        logging.info("No new companies to process or all companies already have a search status (excluding 'ERREUR' which will be retried).")
        return

    try:
        for original_index, row in tqdm(rows_to_process.iterrows(), total=rows_to_process.shape[0], desc="Finding websites (Selenium)"):
            denomination = row['denominationUniteLegale']
            # siren = row['siren'] # SIREN is not directly used in get_website_with_selenium anymore

            # Call with denomination, expect rank
            status, website, rank = get_website_with_selenium(denomination)
            
            # Update the original DataFrame (df_output) using its original index
            df_output.loc[original_index, 'statut_recherche'] = status
            df_output.loc[original_index, 'site_web'] = website if status == 'TROUVÉ' else ''
            df_output.loc[original_index, 'source_site_web'] = f"DDG Rank {rank}" if status == 'TROUVÉ' else '' # NEW
            
            df_output.to_csv(output_filename, index=False, encoding='utf-8')

            time.sleep(random.uniform(3, 8)) # Keep varied delay

    except KeyboardInterrupt:
        logging.warning("\n[STOP] Script interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"\n[FATAL] An unexpected error occurred: {e}. Progress has been saved.", exc_info=True)
        sys.exit(1)

    logging.info(f"\nProcessing complete. Results saved to '{output_filename}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find company websites using Selenium.")
    parser.add_argument("input_csv", type=str, help="Path to the input CSV file.")
    parser.add_argument("--output_dir", type=str, default="Results", help="Directory to save the output CSV. Defaults to 'Results'.")
    parser.add_argument("--limit", type=int, help="Limit the number of companies to process for testing.")
    args = parser.parse_args()
    main(args.input_csv, args.output_dir, args.limit)

