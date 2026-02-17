import pandas as pd

# List of company names provided by the user
company_names_to_test = [
    "SA CHANTIERS AMEL",
    "CONSTRUCTION NAVALE BORDEAUX",
    "J P 3",
    "DUBOURDIEU 1800",
    "ATLANTIQUE SELLERIE CHRISTIAN COSTES",
    "STE FERNAND HERVE SARL",
    "CHANTIER NAVAL COUACH - CNC",
    "SURVITEC SAS",
    "AP YACHT CONCEPTION",
    "NEEL TRIMARANS",
    "BY PAINT SERVICE"
]

# Load the filtered companies CSV
input_filtered_csv = 'Results/filtered_companies.csv'
df_filtered = pd.read_csv(input_filtered_csv)

# Filter the DataFrame
df_test = df_filtered[df_filtered['denominationUniteLegale'].isin(company_names_to_test)].copy()

# Define output path for the test CSV
output_test_csv = 'Results/test_companies_for_website_finder.csv'

# Save the filtered DataFrame to the new CSV
df_test.to_csv(output_test_csv, index=False)

print(f"Created test CSV with {len(df_test)} companies: {output_test_csv}")