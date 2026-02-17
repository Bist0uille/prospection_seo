
import pandas as pd
import numpy as np

def refine_results(file_path):
    """
    Cleans the results file by removing links from known directory/aggregator sites.
    """
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File not found: {file_path}. Nothing to refine.")
        return

    bad_domains = [
        'pappers.fr', 
        'annuaire-entreprises.data.gouv.fr', 
        'actunautique.com', 
        'doctrine.fr',
        'societe.com' # Just in case
    ]

    # Create a regex pattern: domain1|domain2|domain3
    pattern = '|'.join(bad_domains)

    # Find rows where 'site_web' contains any of the bad domains
    # We use na=False to treat non-string (NaN) values as not containing the pattern
    mask = df['site_web'].str.contains(pattern, na=False)
    
    print(f"Found {mask.sum()} entries to clean. Cleaning them now...")

    # Set site_web to NaN for these rows, which will be treated as empty
    df.loc[mask, 'site_web'] = np.nan
    
    # Save the cleaned dataframe back to the same file
    df.to_csv(file_path, index=False, encoding='utf-8')
    print("Refinement complete. The results file is ready for the next run.")

if __name__ == "__main__":
    refine_results('websites_selenium_results.csv')
