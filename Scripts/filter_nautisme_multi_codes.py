# -*- coding: utf-8 -*-
import csv
import sys
import os

def filter_companies_by_multiple_naf(naf_codes, min_employees_codes, input_file, output_file=None):
    """
    Filters companies from the input CSV file based on multiple NAF codes, active status,
    and a list of allowed employee tranche codes.

    Args:
        naf_codes (list): List of NAF codes to filter by (e.g., ['3012Z', '3011Z', '3315Z']).
        min_employees_codes (list): A list of employee tranche codes that meet the minimum employee criteria.
        input_file (str): The path to the input CSV file.
        output_file (str): Optional output file path.
    """
    if output_file is None:
        base_name = os.path.basename(input_file)
        name_without_ext = os.path.splitext(base_name)[0]
        output_file = f"Results/{name_without_ext}_filtered_nautisme.csv"

    if not os.path.exists(input_file):
        print(f"Erreur : Le fichier d'entr�e '{input_file}' n'a pas �t� trouv�.")
        return None

    # Ensure Results directory exists
    os.makedirs("Results", exist_ok=True)

    print(f"Filtrage des entreprises nautiques avec les codes NAF: {', '.join(naf_codes)}")
    print(f"Filtrage des entreprises avec employ�s dans {min_employees_codes}")
    print(f"Fichier d'entr�e: {input_file}")

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
        return None

    filtered_count = 0
    naf_stats = {code: 0 for code in naf_codes}

    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', newline='', encoding='utf-8') as f_out:

        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        writer.writerow(selected_header) # Write the selected header to the new file

        next(reader) # Skip header in input file

        for row in reader:
            try:
                if len(row) > max(siren_idx, denomination_idx, naf_idx, effectifs_idx, categorie_idx, creation_date_idx, status_idx):
                    # Extract NAF code and normalize it (remove dots: "30.12Z" -> "3012Z")
                    naf_code = row[naf_idx].replace('.', '')
                    # Check if the NAF code matches any of our target codes (exact or with suffix)
                    matches_naf = any(naf_code.startswith(code) for code in naf_codes)

                    # Check for active status ('A'), correct NAF code, and minimum employee count
                    if (row[status_idx] == 'A' and
                        matches_naf and
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

                        # Track stats per NAF code
                        for code in naf_codes:
                            if naf_code.startswith(code):
                                naf_stats[code] += 1
                                break
            except IndexError:
                # Log or handle rows that are too short if necessary
                continue

    print(f"\n=== Filtrage termin� ===")
    print(f"Total: {filtered_count} entreprises trouv�es")
    print(f"\nR�partition par code NAF:")
    for code, count in naf_stats.items():
        print(f"  {code}: {count} entreprises")
    print(f"\nLes r�sultats sont enregistr�s dans : {output_file}")

    return output_file


if __name__ == "__main__":
    # Codes NAF pour le secteur nautisme
    NAUTISME_NAF_CODES = [
        '3012Z',  # Construction de bateaux de plaisance
        '3011Z',  # Construction de navires et de structures flottantes
        '3315Z',  # R�paration et maintenance navale
        '5010Z',  # Transports maritimes et c�tiers de passagers
        '5020Z',  # Transports maritimes et c�tiers de fret
        '5222Z',  # Services auxiliaires des transports par eau
        '7734Z',  # Location et location-bail de mat�riels de transport par eau
    ]

    # Define employee codes for "10 employees or more"
    EMPLOYEE_CODES_GE_10 = ['11', '12', '21', '22', '31', '32', '41', '42', '51', '52', '53']
    # Old format codes
    EMPLOYEE_CODES_GE_10_OLD = ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']
    # Combine both for compatibility
    ALL_EMPLOYEE_CODES = EMPLOYEE_CODES_GE_10 + EMPLOYEE_CODES_GE_10_OLD

    if len(sys.argv) < 2:
        print("Usage: python filter_nautisme_multi_codes.py <INPUT_CSV_FILE> [OUTPUT_CSV_FILE]")
        print("Example: python filter_nautisme_multi_codes.py DataBase/annuaire-des-entreprises-nouvelle_aquitaine.csv")
        sys.exit(1)

    input_csv_file = sys.argv[1]
    output_csv_file = sys.argv[2] if len(sys.argv) > 2 else None

    filter_companies_by_multiple_naf(NAUTISME_NAF_CODES, ALL_EMPLOYEE_CODES, input_csv_file, output_csv_file)
