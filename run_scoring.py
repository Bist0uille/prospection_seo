from Scripts.prospect_analyzer import create_prospect_scoring
import os


# Ensure the Results directory exists
os.makedirs('Results', exist_ok=True)

LIGHTHOUSE_CSV = 'Results/lighthouse_reports.csv'
FINAL_REPORT_CSV = 'Results/final_prospect_report.csv'

print(f"Starting prospect scoring from {LIGHTHOUSE_CSV}...")
create_prospect_scoring(LIGHTHOUSE_CSV, FINAL_REPORT_CSV)
print("Scoring process finished.")
