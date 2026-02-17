from Scripts.prospect_analyzer import verify_websites_by_domain, run_lighthouse_reports, create_prospect_scoring
import os

os.makedirs('Results', exist_ok=True)
os.makedirs('Reports/Lighthouse', exist_ok=True)


FILTERED_CSV = 'Results/filtered_companies.csv'
VERIFIED_CSV = 'Results/verified_websites.csv'
LIGHTHOUSE_CSV = 'Results/lighthouse_reports.csv'
FINAL_REPORT_CSV = 'Results/final_prospect_report.csv'

print("Starting full re-analysis with updated verification logic...")

# Ensure FILTERED_CSV exists for the next step, assuming filter_companies_by_employees was run previously.
# If it doesn't exist, this script would need to call filter_companies_by_employees first.
# For now, we assume it exists as the user was checking results from a previous full run.

# Step 2: Rerun website verification with the new logic
verified_file = verify_websites_by_domain(FILTERED_CSV, VERIFIED_CSV)

# Step 3: Rerun Lighthouse reports
lighthouse_file = run_lighthouse_reports(verified_file, LIGHTHOUSE_CSV)

# Step 4: Rerun prospect scoring
create_prospect_scoring(lighthouse_file, FINAL_REPORT_CSV)

print("Full re-analysis finished.")
