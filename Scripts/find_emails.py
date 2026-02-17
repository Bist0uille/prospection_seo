import csv
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import time
import os

# Add headers to mimic a browser request
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def normalize_url(url):
    """Normalizes a URL to ensure it's processable."""
    url = url.strip()
    if url.startswith('//'):
        return 'https:' + url
    if not url.startswith(('http://', 'https://')):
        return 'https://' + url
    return url

def extract_emails_from_text(text):
    """Extracts all unique, cleaned email addresses from a given text."""
    email_regex = re.compile(
        r'[a-zA-Z0-9._%+-]+(?:\s*\[at\]\s*|\s*\(at\)\s*|@)[a-zA-Z0-9.-]+(?:\s*\[dot\]\s*|\s*\(dot\)\s*|\.)[a-zA-Z]{2,}',
        re.IGNORECASE
    )
    mailto_regex = re.compile(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE)
    
    raw_emails = email_regex.findall(text)
    raw_mailto_emails = mailto_regex.findall(text)

    cleaned_emails = []
    for email in raw_emails:
        cleaned_email = email.lower().replace(' [at] ', '@').replace(' (at) ', '@').replace(' [dot] ', '.').replace(' (dot) ', '.')
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', cleaned_email):
            cleaned_emails.append(cleaned_email)
    
    cleaned_emails.extend([m.lower() for m in raw_mailto_emails]) # Add mailto emails
    return list(set(cleaned_emails)) # Return unique emails

def score_email(email, website_domain):
    """
    Scores an email based on its relevance to the website domain and common junk patterns.
    Higher score means more relevant/desirable.
    """
    score = 0
    email_domain = email.split('@')[-1]

    # Punish common junk/tracking/service domains
    junk_patterns = ['wixpress.com', 'user@domain.com', 'demo@email.com', 'no-reply', 'sentry.io', 'mail-tester.com', 'example.com', 'info@google.com']
    if any(jp in email_domain for jp in junk_patterns) or any(jp in email for jp in junk_patterns):
        return -100 # Strongly penalize

    # Prioritize emails matching the website's root domain
    # Example: website.com -> email@website.com
    # Also handle subdomains: www.website.com -> email@website.com
    website_root_domain = '.'.join(website_domain.split('.')[-2:]) # e.g., example.com from www.example.com
    email_root_domain = '.'.join(email_domain.split('.')[-2:])

    if website_root_domain == email_root_domain:
        score += 10 # Strong match
    elif website_domain in email_domain: # Subdomain match
        score += 5

    # Reward common contact prefixes
    contact_prefixes = ['info', 'contact', 'accueil', 'agence', 'studio', 'atelier']
    if any(email.startswith(prefix + '@') for prefix in contact_prefixes):
        score += 3

    # Penalize generic email providers if a direct domain match is not found
    generic_providers = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'orange.fr', 'wanadoo.fr']
    if website_root_domain != email_root_domain and email_domain in generic_providers:
        score -= 1

    return score


def find_email_on_website(base_url):
    """
    Attempts to find the most relevant email address on the main page and common contact pages of a website.
    """
    all_found_emails = {} # Store emails with their original page context for scoring
    visited_urls = set()
    parsed_base_url = urlparse(base_url)
    website_domain = parsed_base_url.netloc

    print(f"  Searching for email on {base_url} (Domain: {website_domain})...")

    # Helper to fetch and extract emails from a URL
    def fetch_and_extract(url, context_score_modifier=0):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract emails from general text
            emails_from_body = extract_emails_from_text(response.text)
            for email in emails_from_body:
                all_found_emails[email] = all_found_emails.get(email, 0) + score_email(email, website_domain) + context_score_modifier

            # Additionally, check footer specifically for higher priority
            footer = soup.find('footer')
            if footer:
                emails_from_footer = extract_emails_from_text(str(footer))
                for email in emails_from_footer:
                    all_found_emails[email] = all_found_emails.get(email, 0) + score_email(email, website_domain) + 5 + context_score_modifier # Bonus for footer

            return soup
        except requests.exceptions.RequestException as e:
            print(f"    Error fetching {url}: {e}")
            return None

    # 1. Fetch main page
    soup = fetch_and_extract(base_url, context_score_modifier=2) # Main page emails get a slight boost
    visited_urls.add(base_url)

    if soup:
        # 2. Look for contact links on the main page
        contact_keywords = ['contact', 'about', 'mentions-legales', 'a-propos', 'information', 'nous-contacter', 'team']
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Check if keyword is in the link text or href
            if any(kw in href.lower() or any(kw in s.lower() for s in link.strings) for kw in contact_keywords):
                contact_url = urljoin(base_url, href)
                # Avoid re-visiting the same page and only follow links within the same domain
                if contact_url not in visited_urls and urlparse(contact_url).netloc == website_domain:
                    visited_urls.add(contact_url)
                    time.sleep(1) # Be polite, add a delay
                    fetch_and_extract(contact_url, context_score_modifier=3) # Contact page emails get a bigger boost

    # Sort and select the best email
    if all_found_emails:
        # Filter out emails with very low scores (e.g., junk/tracking)
        filtered_emails = {email: score for email, score in all_found_emails.items() if score > -50} # Threshold to exclude strong negatives

        if filtered_emails:
            sorted_emails = sorted(filtered_emails.items(), key=lambda item: item[1], reverse=True)
            best_email = sorted_emails[0][0]
            # print(f"    Selected best email: {best_email} (Score: {sorted_emails[0][1]})") # For debugging
            return best_email
    
    return 'N/A' # No email found after trying main and contact pages
def main():
    input_csv = 'resultats_analyse_architectes_complet.csv'
    output_csv = 'resultats_avec_emails.csv'
    
    if not os.path.exists(input_csv):
        print(f"Error: Input file '{input_csv}' not found. Please ensure it has been generated.")
        return

    results_with_emails = []
    
    # Counter for the limit
    processed_count = 0
    max_sites_to_process = 10 # Limit to 10 sites as requested

    with open(input_csv, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['Found Email'] # Add new column header
        
        for i, row in enumerate(reader):
            # Remove the processing limit
            # if processed_count >= max_sites_to_process:
            #     break 

            url_raw = row['URL']
            if url_raw and url_raw.strip() != 'N/A':
                normalized_url = normalize_url(url_raw) # Normalize the URL here
                print(f"Processing site {i+1}: {row['Architect Name']}")
                found_email = find_email_on_website(normalized_url) # Pass normalized URL
                row['Found Email'] = found_email
            else:
                row['Found Email'] = 'N/A' # No URL to search
                print(f"Processing site {i+1}: {row['Architect Name']} - Skipping email search (no URL).")
            results_with_emails.append(row)
            # processed_count += 1 # Increment processed count (no longer needed if no limit)

    with open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results_with_emails)
        
    print(f"\nFinal report with emails generated: {output_csv}")

if __name__ == '__main__':
    main()
