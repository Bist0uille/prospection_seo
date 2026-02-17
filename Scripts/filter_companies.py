import csv
import sys
import os

def filter_companies_by_criteria(naf_code_prefix, min_employees_codes, input_file):
    """
    Filters companies from the input CSV file based on NAF code prefix, active status,
    and a list of allowed employee tranche codes.

    Args:
        naf_code_prefix (str): The NAF code prefix to filter by (e.g., '47').
        min_employees_codes (list): A list of employee tranche codes that meet the minimum employee criteria.
        input_file (str): The path to the input CSV file.
    """
    base_name = os.path.basename(input_file)
    name_without_ext = os.path.splitext(base_name)[0]
    # Adjust output filename to reflect new filtering criteria
    output_file = f"Results/{name_without_ext}_filtered_naf_{naf_code_prefix}_employees_{min_employees_codes[0] if min_employees_codes else 'none'}.csv"

    if not os.path.exists(input_file):
        print(f"Erreur : Le fichier d'entrée '{input_file}' n'a pas été trouvé.")
        return

    print(f"Filtrage des entreprises avec le code NAF commençant par '{naf_code_prefix}' et employés dans {min_employees_codes} dans {input_file}")

    selected_header = [
        'siren', 'denominationUniteLegale', 'activitePrincipaleUniteLegale',
        'trancheEffectifsUniteLegale', 'categorieEntreprise',
        'dateCreationUniteLegale', 'etatAdministratifUniteLegale'
    ]

    try:
        with open(input_file, 'r', encoding='utf-8') as f_in:
            reader = csv.reader(f_in)
            full_header = next(reader) # Read the header from the actual file

        # Get column indices dynamically
        siren_idx = full_header.index('siren')
        denomination_idx = full_header.index('denominationUniteLegale')
        naf_idx = full_header.index('activitePrincipaleUniteLegale')
        effectifs_idx = full_header.index('trancheEffectifsUniteLegale')
        categorie_idx = full_header.index('categorieEntreprise')
        creation_date_idx = full_header.index('dateCreationUniteLegale')
        status_idx = full_header.index('etatAdministratifUniteLegale')
    except (ValueError, StopIteration) as e:
        print(f"Erreur: Colonne manquante ou fichier CSV vide : {e}")
        return

    filtered_count = 0
    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', newline='', encoding='utf-8') as f_out:

        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        writer.writerow(selected_header) # Write the selected header to the new file

        next(reader) # Skip header in input file

        for row in reader:
            try:
                if len(row) > max(siren_idx, denomination_idx, naf_idx, effectifs_idx, categorie_idx, creation_date_idx, status_idx):
                    # Check for active status ('A'), correct NAF code prefix, and minimum employee count
                    if (row[status_idx] == 'A' and
                        row[naf_idx].startswith(naf_code_prefix) and
                        row[effectifs_idx] in min_employees_codes):
                        selected_data = [
                            row[siren_idx],
                            row[denomination_idx],
                            row[naf_idx],
                            row[effectifs_idx],
                            row[categorie_idx],
                            row[creation_date_idx],
                            row[status_idx]
                        ]
                        writer.writerow(selected_data)
                        filtered_count += 1
            except IndexError:
                # Log or handle rows that are too short if necessary
                continue

    print(f"Filtrage terminé. {filtered_count} entreprises trouvées.")
    print(f"Les résultats sont enregistrés dans : {output_file}")


if __name__ == "__main__":
    # Define employee codes for "10 employees or more"
    # These are common codes, actual codes might vary slightly based on dataset
    EMPLOYEE_CODES_GE_10 = ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']

    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python filter_companies.py <NAF_CODE_PREFIX> <INPUT_CSV_FILE> [OPTIONAL: MIN_EMPLOYEES_CODE_START]")
        print("Example: python filter_companies.py 4771Z DataBase/annuaire.csv C") # C for 10-19 employees
        sys.exit(1)

    naf_code_to_filter_prefix = sys.argv[1]
    input_csv_file = sys.argv[2]

    min_employee_codes_for_filter = []
    # If a specific min_employees_code start is provided, use it to slice the list
    if len(sys.argv) == 4:
        start_code = sys.argv[3].upper()
        try:
            start_index = EMPLOYEE_CODES_GE_10.index(start_code)
            min_employee_codes_for_filter = EMPLOYEE_CODES_GE_10[start_index:]
        except ValueError:
            print(f"Warning: Provided MIN_EMPLOYEES_CODE_START '{start_code}' is not recognized. Using default: all codes for 10+ employees.")
            min_employee_codes_for_filter = EMPLOYEE_CODES_GE_10
    else:
        min_employee_codes_for_filter = EMPLOYEE_CODES_GE_10

    filter_companies_by_criteria(naf_code_to_filter_prefix, min_employee_codes_for_filter, input_csv_file)