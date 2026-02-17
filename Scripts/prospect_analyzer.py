import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import os
import subprocess
import json
import time
from urllib.parse import urlparse

def get_domain(url):
    """Extrait le nom de domaine principal d'une URL."""
    try:
        return urlparse(url).netloc.replace('www.', '')
    except Exception:
        return ''

def normalize_name(name):
    """Normalise un nom d'entreprise pour la comparaison avec un domaine."""
    name = name.lower()
    name = re.sub(r'[^a-z0-9]', '', name)
    return name

EMPLOYEE_CODES_DEFAULT = ['11', '12', '21', '22', '31', '32', '41', '42', '51', '52', '53']

def filter_companies_by_employees(input_path, output_path, naf_codes=None,
                                  naf_code_prefixes=None, employee_codes=None):
    """Filtre les entreprises par tranche d'effectifs, codes NAF et statut actif.

    Args:
        input_path: Chemin du CSV source (annuaire entreprises).
        output_path: Chemin du CSV filtré en sortie.
        naf_codes: Liste de codes NAF exacts (ex. ['3012Z', '3011Z']).
        naf_code_prefixes: Liste de préfixes NAF (rétro-compatible).
        employee_codes: Liste de codes tranche d'effectifs à conserver.
    """
    print(f"Filtrage des entreprises par taille, codes APE/NAF et statut actif...")
    df = pd.read_csv(input_path, dtype=str)

    initial_count = len(df)
    print(f"  Entreprises dans la base : {initial_count}")

    # --- Nettoyage du code NAF : retirer les points (30.12Z → 3012Z) ---
    if 'activitePrincipaleUniteLegale' in df.columns:
        df['activitePrincipaleUniteLegale'] = (
            df['activitePrincipaleUniteLegale'].astype(str).str.replace('.', '', regex=False)
        )

    # --- Filtrage entreprises actives uniquement ---
    if 'etatAdministratifUniteLegale' in df.columns:
        df = df[df['etatAdministratifUniteLegale'] == 'A'].copy()
        print(f"  Après filtre actives (état admin = A) : {len(df)}")

    # --- Filtrage par tranche d'effectifs ---
    codes = employee_codes or EMPLOYEE_CODES_DEFAULT
    df['trancheEffectifsUniteLegale'] = df['trancheEffectifsUniteLegale'].replace('nan', np.nan)
    df.dropna(subset=['trancheEffectifsUniteLegale'], inplace=True)
    filtered_df = df[df['trancheEffectifsUniteLegale'].isin(codes)].copy()
    print(f"  Après filtre effectifs ({codes}) : {len(filtered_df)}")

    # --- Déduplication par SIREN (garder le siège ou le premier établissement) ---
    if 'siren' in filtered_df.columns:
        before_dedup = len(filtered_df)
        if 'etablissementSiege' in filtered_df.columns:
            filtered_df = filtered_df.sort_values('etablissementSiege', ascending=False)
        filtered_df = filtered_df.drop_duplicates(subset='siren', keep='first').copy()
        print(f"  Après déduplication SIREN : {len(filtered_df)} (supprimé {before_dedup - len(filtered_df)} doublons)")

    # --- Filtrage par codes NAF exacts ---
    if naf_codes:
        filtered_df = filtered_df[
            filtered_df['activitePrincipaleUniteLegale'].isin(naf_codes)
        ].copy()
        print(f"  Après filtre NAF exact ({len(naf_codes)} codes) : {len(filtered_df)}")

    # --- Filtrage par préfixes NAF (rétro-compatibilité) ---
    elif naf_code_prefixes:
        filtered_df = filtered_df[
            filtered_df['activitePrincipaleUniteLegale'].apply(
                lambda x: any(str(x).startswith(prefix) for prefix in naf_code_prefixes)
            )
        ].copy()
        print(f"  Après filtre NAF préfixes : {len(filtered_df)}")

    filtered_df.to_csv(output_path, index=False)
    print(f"-> {len(filtered_df)} entreprises filtrées. Fichier : {output_path}")
    return output_path

def verify_websites_by_domain(input_path, output_path):
    print(f"\nÉtape 2: Vérification des sites par nom de domaine")
    df = pd.read_csv(input_path)
    df.loc[:, 'site_verifie'] = False
    df.loc[:, 'verification_raison'] = ""
    verified_count = 0
    
    # Define the blocklist
    blocklist = [
        'reseauexcellence.fr', 'verif.com', 'app.dataprospects.fr', 
        'lagazettefrance.fr', 'kompass.com', 'france3-regions.franceinfo.fr'
    ]
    
    for index, row in df.iterrows():
        url, company_name = row.get('site_web', ''), row.get('denominationUniteLegale', '')
        if not url:
            df.loc[index, 'verification_raison'] = "URL manquante"
            continue
            
        domain = get_domain(url)
        
        # Check against blocklist
        if any(blocked_domain in domain for blocked_domain in blocklist):
            df.loc[index, 'site_verifie'] = False
            df.loc[index, 'verification_raison'] = "Domaine sur la liste de blocage"
            continue

        # Existing verification logic
        normalized_name = normalize_name(company_name)
        domain_clean = domain.replace('.', '').replace('-', '')
        
        is_verified = False
        reason = "Aucune correspondance trouvée"

        if not normalized_name or not domain_clean:
            reason = "Nom ou domaine nettoyé manquant"
        elif normalized_name in domain_clean:
            is_verified = True
            reason = "Nom complet trouvé dans le domaine"
        elif len(normalized_name) > 3 and normalized_name[:4] in domain_clean:
            is_verified = True
            reason = "Début du nom trouvé dans le domaine"
        elif len(domain_clean) > 3 and domain_clean[:4] in normalized_name:
            is_verified = True
            reason = "Début du domaine trouvé dans le nom"
        
        df.loc[index, 'site_verifie'] = is_verified
        df.loc[index, 'verification_raison'] = reason
        if is_verified:
            verified_count += 1
            
    df.to_csv(output_path, index=False)
    print(f"-> {verified_count} sites vérifiés. Fichier : {output_path}")
    return output_path

def run_lighthouse_reports(input_path, output_path, reports_dir='Reports/Lighthouse'):
    print(f"\nÉtape 3: Exécution des rapports Lighthouse")
    df = pd.read_csv(input_path)
    os.makedirs(reports_dir, exist_ok=True)
    df.loc[:, 'lighthouse_report_path'] = ""
    report_count = 0
    for index, row in df.iterrows():
        if row['site_verifie']:
            url, siren = row['site_web'], row['siren']
            report_filename = os.path.join(reports_dir, f"{siren}_report.json")
            
            # Check if report already exists
            if os.path.exists(report_filename):
                print(f"  Rapport existant trouvé pour {url}. Analyse ignorée.")
                df.loc[index, 'lighthouse_report_path'] = report_filename
                report_count += 1
                continue

            print(f"  Analyse de {url}...")
            try:
                command = f"cmd.exe /c npx lighthouse {url} --output=json --output-path={report_filename} --quiet --chrome-flags=\"--headless --no-sandbox\""
                process = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=120)
                if process.returncode == 0 or os.path.exists(report_filename):
                    df.loc[index, 'lighthouse_report_path'] = report_filename
                    report_count += 1
                else:
                    df.loc[index, 'lighthouse_report_path'] = f"Erreur: {process.stderr.strip()[:150]}"
            except Exception as e:
                df.loc[index, 'lighthouse_report_path'] = f"Erreur script: {e}"
    df.to_csv(output_path, index=False)
    print(f"-> {report_count} rapports Lighthouse générés. Fichier : {output_path}")
    return output_path

def create_prospect_scoring(input_path, output_path):
    """Analyse les rapports Lighthouse et crée un score de prospection."""
    print(f"\nÉtape 4: Création du scoring de prospection")
    df = pd.read_csv(input_path)
    
    # Initialiser les colonnes de score
    score_cols = ['performance', 'accessibilite', 'bonnes_pratiques', 'seo', 'prospect_score']
    for col in score_cols:
        df[col] = 0.0
    df['prospect_summary'] = ''

    if 'lighthouse_report_path' not in df.columns:
        df['lighthouse_report_path'] = ''

    for index, row in df.iterrows():
        report_path = row['lighthouse_report_path']
        if pd.isna(report_path) or not str(report_path).endswith('.json'):
            continue

        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            # Extraire les scores (valeur de 0 à 1)
            perf = report['categories']['performance']['score'] or 0
            acc = report['categories']['accessibility']['score'] or 0
            bp = report['categories']['best-practices']['score'] or 0
            seo = report['categories']['seo']['score'] or 0

            # Calculer un score de prospection (de 1 à 10)
            # Plus les scores sont mauvais, plus le prospect est intéressant
            prospect_score = ((1 - seo) * 1.5 + (1 - perf) * 1.2 + (1 - acc) * 0.8) / 3.5 * 10
            prospect_score = max(1, min(10, round(prospect_score, 1)))

            # Stocker les scores
            df.loc[index, 'performance'] = int(perf * 100)
            df.loc[index, 'accessibilite'] = int(acc * 100)
            df.loc[index, 'bonnes_pratiques'] = int(bp * 100)
            df.loc[index, 'seo'] = int(seo * 100)
            df.loc[index, 'prospect_score'] = prospect_score
            
            # Créer un résumé
            summary = f"Score de prospection: {prospect_score}/10. "
            if prospect_score > 7:
                summary += "Excellent prospect. Points faibles majeurs en "
                if seo < 0.8: summary += "SEO, "
                if perf < 0.7: summary += "Performance, "
            elif prospect_score > 4:
                summary += "Prospect modéré. Améliorations possibles en "
                if seo < 0.9: summary += "SEO, "
                if perf < 0.8: summary += "Performance, "
            else:
                summary += "Prospect faible. Site déjà bien optimisé."
            
            df.loc[index, 'prospect_summary'] = summary.strip(', ') + '.'

        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            print(f"  Erreur lors de l'analyse du rapport {report_path}: {e}")
            df.loc[index, 'prospect_summary'] = "Erreur d'analyse du rapport."

    # Sélectionner et réorganiser les colonnes pour le rapport final
    final_cols = [
        'siren', 'denominationUniteLegale', 'trancheEffectifsUniteLegale', 'site_web', 
        'prospect_score', 'performance', 'seo', 'accessibilite', 'bonnes_pratiques',
        'prospect_summary', 'lighthouse_report_path'
    ]
    final_df = df.reindex(columns=final_cols).sort_values(by='prospect_score', ascending=False)
    
    final_df.to_csv(output_path, index=False)
    print(f"-> Scoring terminé. Rapport final : {output_path}")
    return output_path


if __name__ == '__main__':
    INPUT_CSV = 'Results/websites_selenium_results.csv'
    FILTERED_CSV = 'Results/filtered_companies.csv'
    VERIFIED_CSV = 'Results/verified_websites.csv'
    LIGHTHOUSE_CSV = 'Results/lighthouse_reports.csv'
    FINAL_REPORT_CSV = 'Results/final_prospect_report.csv'
    
    filtered_file = filter_companies_by_employees(INPUT_CSV, FILTERED_CSV)
    verified_file = verify_websites_by_domain(filtered_file, VERIFIED_CSV)
    lighthouse_file = run_lighthouse_reports(verified_file, LIGHTHOUSE_CSV)
    create_prospect_scoring(lighthouse_file, FINAL_REPORT_CSV)

    print("\n--- Processus terminé. ---")
