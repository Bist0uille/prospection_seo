
import csv
import json
import os
import subprocess
import re
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
import sys
import argparse # Import argparse for command line arguments

def normalize_url(url):
    """Normalizes a URL to ensure it's processable."""
    url = url.strip()
    if url.startswith('//'):
        return 'https:' + url
    if not url.startswith(('http://', 'https://')):
        return 'https://' + url
    return url

def get_filename_from_url(url):
    """Creates a filesystem-safe filename from a URL."""
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    # Replace characters that are invalid in filenames
    safe_filename = re.sub(r'[\\/*?:"+<>|]', '_', domain)
    return f"{safe_filename}.json"

def run_lighthouse_audit(url, output_path):
    """Runs a Lighthouse audit on a given URL."""
    print(f"Running Lighthouse audit for {url}...")
    try:
        command = [
            'npx', 'lighthouse', url,
            '--output=json',
            f'--output-path={output_path}',
            '--quiet',
            '--chrome-flags="--headless --no-sandbox"',
            '--emulated-form-factor=desktop'
        ]
        # Using shell=True on Windows to correctly handle npm/npx commands
        subprocess.run(command, check=True, shell=True)
        print(f"Successfully generated report for {url}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running Lighthouse for {url}: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred for {url}: {e}")
        return False

def analyze_report(report_path):
    """Analyzes a single Lighthouse JSON report."""
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            report = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Could not read or parse report {report_path}: {e}")
        return None

    # Scores are from 0 to 1, multiply by 100
    performance_score = (report['categories']['performance'].get('score', 0) or 0) * 100
    accessibility_score = (report['categories']['accessibility'].get('score', 0) or 0) * 100
    seo_score = (report['categories']['seo'].get('score', 0) or 0) * 100

    potential_score = 100 - performance_score # Simplified calculation for now
    suggested_actions = []

    # Example: +20 points if images very heavy and not optimized are detected
    if report['audits'].get('uses-responsive-images', {}).get('score', 1) == 0:
        potential_score += 20
        suggested_actions.append("Optimize images")

    # +15 points if no caching policy is in place
    if report['audits'].get('uses-long-cache-ttl', {}).get('score', 1) == 0:
        potential_score += 15
        suggested_actions.append("Implement caching policy")

    # +15 points if render-blocking resources
    render_blocking_audit = report['audits'].get('render-blocking-resources', {})
    if render_blocking_audit.get('score', 1) == 0 and render_blocking_audit.get('details', {}).get('items'):
        potential_score += 15
        suggested_actions.append("Reduce render-blocking resources")

    # +10 points for each major security header missing (CSP)
    if report['audits'].get('content-security-policy', {}).get('score', 1) == 0:
        potential_score += 10
        suggested_actions.append("Add Content-Security-Policy header")

    # +10 points if basic SEO tags are absent (meta-description)
    if report['audits'].get('meta-description', {}).get('score', 1) == 0:
        potential_score += 10
        suggested_actions.append("Add meta description")

    # +5 points for simple accessibility issues (main landmark)
    if report['audits'].get('main-landmark', {}).get('score', 1) == 0:
        potential_score += 5
        suggested_actions.append("Add main landmark for accessibility")
        
    return {
        'performance': round(performance_score),
        'accessibility': round(accessibility_score),
        'seo': round(seo_score),
        'potential_score': round(potential_score),
        'suggested_actions': ', '.join(suggested_actions) or 'N/A'
    }

def verify_website(url: str, siren: str, company_name: str) -> bool:
    """
    Attempts to verify if the given URL is the legitimate website for the company
    by searching for SIREN and company name in the page content.
    """
    print(f"Verifying website {url} for SIREN: {siren}, Company: {company_name}...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text().lower()

        # Check for SIREN and company name (case-insensitive)
        siren_found = siren.lower() in page_text
        company_name_found = company_name.lower() in page_text
        
        if siren_found or company_name_found:
            print(f"Verification successful for {company_name} at {url}.")
            return True
        else:
            print(f"Verification failed for {company_name} at {url}. SIREN found: {siren_found}, Name found: {company_name_found}.")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url} for verification: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during verification for {url}: {e}")
        return False

def write_final_report(results, output_csv_path):
    """Writes the final analysis results to a CSV file."""
    if not results:
        print("No results to write.")
        return
        
    # Sort results by potential_score descending
    sorted_results = sorted(results, key=lambda x: x['analysis']['potential_score'] if x['analysis'] else 0, reverse=True)
    
    headers = [
        'Siren', 'DenominationUniteLegale', 'ActivitePrincipaleUniteLegale',
        'TrancheEffectifsUniteLegale', 'CategorieEntreprise',
        'DateCreationUniteLegale', 'EtatAdministratifUniteLegale',
        'URL', 'Website_Verified', 'Performance_Score', 'Accessibility_Score',
        'SEO_Score', 'Potential_Score', 'Suggested_Actions'
    ]
    
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for res in sorted_results:
            original_row_dict = res['original_row_dict'] # Use the dictionary for original row data
            writer.writerow([
                original_row_dict.get('siren', ''),
                original_row_dict.get('denominationUniteLegale', ''),
                original_row_dict.get('activitePrincipaleUniteLegale', ''),
                original_row_dict.get('trancheEffectifsUniteLegale', ''),
                original_row_dict.get('categorieEntreprise', ''),
                original_row_dict.get('dateCreationUniteLegale', ''),
                original_row_dict.get('etatAdministratifUniteLegale', ''),
                res['url'],
                res['website_verified'], # New column
                res['analysis']['performance'] if res['analysis'] else 'N/A',
                res['analysis']['accessibility'] if res['analysis'] else 'N/A',
                res['analysis']['seo'] if res['analysis'] else 'N/A',
                res['analysis']['potential_score'] if res['analysis'] else 'N/A',
                res['analysis']['suggested_actions'] if res['analysis'] else 'N/A'
            ])
    print(f"\nFinal report generated: {output_csv_path}")

def main(input_csv_path, output_reports_dir, final_report_csv_path):
    """Main function to process company websites."""
    os.makedirs(output_reports_dir, exist_ok=True)
    all_results = []
    
    if not os.path.exists(input_csv_path):
        print(f"Error: Input file '{input_csv_path}' not found.")
        return

    # Read the input CSV to get company data and URLs
    companies_to_process = []
    with open(input_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies_to_process.append(row)

    total_sites = len(companies_to_process)
    print(f"Found {total_sites} sites to process from {input_csv_path}.")

    for i, company_data in enumerate(companies_to_process):
        siren = company_data.get('siren', '')
        denomination = company_data.get('denominationUniteLegale', '')
        url_raw = company_data.get('site_web', '') # Assuming 'site_web' is the column from find_websites.py

        if not url_raw or url_raw.strip() == 'N/A':
            print(f"Skipping '{denomination}' (row {i+1}) due to missing URL.")
            continue

        print(f"\n--- Processing site {i+1}/{total_sites}: {denomination} ---")

        normalized_url = normalize_url(url_raw)
        
        # --- Step 1: Verify Website ---
        website_verified = verify_website(normalized_url, siren, denomination)
        
        analysis_result = None
        if website_verified:
            # --- Step 2: Run Lighthouse Audit ---
            report_filename = get_filename_from_url(normalized_url)
            report_path = os.path.join(output_reports_dir, report_filename)
            
            if not os.path.exists(report_path):
                run_lighthouse_audit(normalized_url, report_path)
            else:
                print(f"Lighthouse report for {normalized_url} already exists. Skipping audit.")

            # --- Step 3: Analyze Report ---
            if os.path.exists(report_path):
                analysis_result = analyze_report(report_path)
        else:
            print(f"Website {normalized_url} for {denomination} not verified. Skipping Lighthouse audit.")

        all_results.append({
            'original_row_dict': company_data, # Store the entire original row as a dict
            'url': normalized_url,
            'website_verified': website_verified,
            'analysis': analysis_result # Will be None if not verified or audit failed
        })

    # --- Step 4: Write Final Report ---
    write_final_report(all_results, final_report_csv_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Analyze company websites with Lighthouse and verify ownership.")
    parser.add_argument("input_csv", type=str, help="Path to the input CSV file (output from find_websites.py).")
    parser.add_argument("--output_reports_dir", type=str, default="Reports", help="Directory to save individual Lighthouse JSON reports. Defaults to 'Reports'.")
    parser.add_argument("--final_report_csv", type=str, default="Results/website_analysis_report.csv", help="Path for the final aggregated CSV report. Defaults to 'Results/website_analysis_report.csv'.")
    args = parser.parse_args()
    main(args.input_csv, args.output_reports_dir, args.final_report_csv)
