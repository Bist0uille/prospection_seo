import subprocess
import os
import argparse
import sys
import datetime

# Ensure the Results and Reports directories exist
os.makedirs("Results", exist_ok=True)
os.makedirs("Reports", exist_ok=True)

def run_script(script_path, *args):
    """Helper function to run a Python script as a subprocess."""
    cmd = [sys.executable, script_path] + list(args)
    print(f"\n--- Running: {' '.join(cmd)} ---")
    try:
        # Use Popen to allow for real-time output if needed, but here we just check for completion
        # capture_output=False ensures that output is streamed directly to the console
        process = subprocess.run(cmd, check=True, capture_output=False, text=True)
        print(f"--- Successfully finished: {os.path.basename(script_path)} ---")
    except subprocess.CalledProcessError as e:
        print(f"Error running {os.path.basename(script_path)}: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: Python interpreter not found or script '{script_path}' does not exist.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Orchestrates the company data processing pipeline.")
    parser.add_argument("naf_code_prefix", type=str, help="NAF code prefix to filter companies (e.g., '47.71Z' or '47').")
    parser.add_argument("input_siren_csv", type=str, help="Path to the initial SIREN CSV database (e.g., 'DataBase/StockUniteLegale_utf8.csv').")
    parser.add_argument("--min_employees_code_start", type=str, default="C",
                        help="Minimum employee tranche code to filter by (e.g., 'C' for 10-19 employees). Defaults to 'C' (10+ employees).")
    parser.add_argument("--limit_websites", type=int, help="Limit the number of companies to find websites for (for testing purposes).")
    parser.add_argument("--limit_analysis", type=int, help="Limit the number of websites to analyze (for testing purposes).")
    args = parser.parse_args()

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Step 1: Filter Companies
    print("\n[STEP 1/3] Filtering companies based on NAF code and employee count...")
    
    # Construct the expected output path from filter_companies.py
    base_name_input_siren = os.path.splitext(os.path.basename(args.input_siren_csv))[0]
    filtered_companies_filename = f"{base_name_input_siren}_filtered_naf_{args.naf_code_prefix}_employees_{args.min_employees_code_start}.csv"
    path_from_filter_companies = os.path.join("Results", filtered_companies_filename)

    run_script(
        "Scripts/filter_companies.py",
        args.naf_code_prefix,
        args.input_siren_csv,
        args.min_employees_code_start
    )
    
    if not os.path.exists(path_from_filter_companies):
        print(f"Error: Filtered companies file not found at {path_from_filter_companies}. Exiting.")
        sys.exit(1)
    
    print(f"Filtered companies saved to: {path_from_filter_companies}")

    # Step 2: Find Websites
    print("\n[STEP 2/3] Finding websites for filtered companies...")
    
    # Construct the expected output path from find_websites.py
    websites_output_filename = f"{os.path.splitext(os.path.basename(path_from_filter_companies))[0]}_websites.csv"
    websites_output_path = os.path.join("Results", websites_output_filename)

    find_websites_args = [path_from_filter_companies]
    if args.limit_websites:
        find_websites_args.extend(["--limit", str(args.limit_websites)])
    
    run_script("Scripts/find_websites.py", *find_websites_args)

    if not os.path.exists(websites_output_path):
        print(f"Error: Websites output file not found at {websites_output_path}. Exiting.")
        sys.exit(1)
    print(f"Websites found and saved to: {websites_output_path}")

    # Step 3: Analyze Websites
    print("\n[STEP 3/3] Analyzing websites (Lighthouse & Verification)...")
    
    # Construct the final report path
    final_report_name = f"website_analysis_report_{args.naf_code_prefix.replace('.', '_')}_{args.min_employees_code_start}_{timestamp}.csv"
    final_report_path = os.path.join("Results", final_report_name)

    analyze_websites_args = [websites_output_path, "--output_reports_dir", "Reports", "--final_report_csv", final_report_path]
    if args.limit_analysis:
        analyze_websites_args.extend(["--limit", str(args.limit_analysis)])

    run_script("Scripts/analyze_websites.py", *analyze_websites_args)

    if os.path.exists(final_report_path):
        print(f"\n--- Pipeline Complete! Final report available at: {final_report_path} ---")
    else:
        print(f"\n--- Pipeline finished, but final report was not generated at {final_report_path}. Check for errors. ---")

if __name__ == "__main__":
    main()