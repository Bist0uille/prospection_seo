import csv
import sys
import os

def clean_and_deduplicate_company_data(input_file):
    """
    Cleans the company data from the input CSV file.
    It keeps only active companies with at least 2 employees,
    a selection of useful columns, removes duplicates based on 'siren',
    and filters out companies without a valid 'denominationUniteLegale'.

    Args:
        input_file (str): The path to the input CSV file.
    """
    base_name = os.path.basename(input_file)
    name_without_ext = os.path.splitext(base_name)[0]
    output_file = f"cleaned_and_deduplicated_filtered_employees_{name_without_ext}.csv"
    
    if not os.path.exists(input_file):
        print(f"Erreur : Le fichier d'entrée '{input_file}' n'a pas été trouvé.")
        return

    print(f"Nettoyage, déduplication et filtrage par effectifs pour le fichier : {input_file}")
    
    selected_header = [
        'siren', 'denominationUniteLegale', 'activitePrincipaleUniteLegale', 
        'trancheEffectifsUniteLegale', 'categorieEntreprise', 
        'dateCreationUniteLegale', 'etatAdministratifUniteLegale'
    ]
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f_in:
            reader = csv.reader(f_in)
            full_header = next(reader)
            
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

    processed_sirens = set()
    cleaned_count = 0
    excluded_effectifs = {'NN', '00', '01'}

    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        
        writer.writerow(selected_header)
        
        next(reader) # Skip header

        for row in reader:
            try:
                if len(row) > max(siren_idx, status_idx, denomination_idx, effectifs_idx):
                    siren = row[siren_idx]
                    denomination = row[denomination_idx].strip()
                    effectif_tranche = row[effectifs_idx]
                    
                    # Apply all filtering conditions
                    if (row[status_idx] == 'A' and
                        siren not in processed_sirens and
                        denomination not in ['', '[ND]'] and
                        effectif_tranche not in excluded_effectifs):
                        
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
                        processed_sirens.add(siren)
                        cleaned_count += 1
            except IndexError:
                continue

    print(f"Opération terminée. {cleaned_count} entreprises (actives, uniques, avec >= 2 employés) trouvées.")
    print(f"Les résultats sont enregistrés dans : {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python clean_company_data.py <INPUT_CSV_FILE>")
        sys.exit(1)
    
    input_csv_file = sys.argv[1]
    clean_and_deduplicate_company_data(input_csv_file)
