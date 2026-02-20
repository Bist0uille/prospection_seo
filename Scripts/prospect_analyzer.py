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

# Mots à ignorer lors de l'extraction des mots-clés (même liste que find_websites.py)
_STOP_WORDS = {'sa', 'sas', 'sarl', 'eurl', 'snc', 'ste', 'et', 'de', 'la', 'les', 'des'}

def extract_keywords(company_name: str) -> list:
    """Extrait les mots-clés significatifs d'un nom d'entreprise.

    Même logique que find_websites.py :
    - split sur espaces et tirets
    - exclure les mots de la stop list
    - exclure les mots de 2 caractères ou moins
    Retourne une liste de mots-clés normalisés (minuscules, alphanumériques).
    """
    words = re.split(r'[\s-]+', company_name)
    keywords = [
        normalize_name(w)
        for w in words
        if w.lower() not in _STOP_WORDS and len(w) > 2
    ]
    return [k for k in keywords if k]  # retirer les chaînes vides après normalisation

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
    """Vérifie que chaque URL trouvée correspond bien à l'entreprise.

    Utilise exactement la même logique de mots-clés que find_websites.py :
    - Split du nom en mots significatifs (stop_words retirés, len > 2)
    - Pour chaque mot-clé : vérifie sa présence dans le domaine nettoyé
    → Cohérence garantie entre les deux étapes du pipeline.
    """
    print(f"\nVérification des sites par correspondance mots-clés")
    df = pd.read_csv(input_path)
    df.loc[:, 'site_verifie'] = False
    df.loc[:, 'verification_raison'] = ""
    verified_count = 0

    # Même blocklist que find_websites.py + entrées supplémentaires
    blocklist = {
        'societe.com', 'pagesjaunes.fr', 'pappers.fr',
        'annuaire-entreprises.data.gouv.fr', 'verif.com',
        'entreprises.lefigaro.fr', 'fr.kompass.com', 'facebook.com',
        'linkedin.com', 'youtube.com', 'wikipedia.org', 'doctrine.fr',
        'app.dataprospects.fr', 'reseauexcellence.fr', 'actunautique.com',
        'lagazettefrance.fr', 'kompass.com', 'france3-regions.franceinfo.fr',
    }

    for index, row in df.iterrows():
        url = str(row.get('site_web', '') or '')
        company_name = str(row.get('denominationUniteLegale', '') or '')

        if not url or url == 'nan':
            df.loc[index, 'verification_raison'] = "URL manquante"
            continue

        domain = get_domain(url)

        # Blocklist
        if any(blocked in domain for blocked in blocklist):
            df.loc[index, 'verification_raison'] = "Domaine sur la liste de blocage"
            continue

        # Filtre non-français : chemin /en/ ou TLD .ca
        parsed_url = urlparse(url)
        path_lower = parsed_url.path.lower()
        domain_lower = domain.lower()
        if re.search(r'/(en|en-[a-z]{2})(/|$)', path_lower):
            df.loc[index, 'verification_raison'] = "URL rejetée : chemin en version anglaise (/en/)"
            continue
        if domain_lower.endswith('.ca'):
            df.loc[index, 'verification_raison'] = "URL rejetée : TLD canadien (.ca)"
            continue

        cleaned_domain = domain.replace('.', '').replace('-', '')
        if not cleaned_domain:
            df.loc[index, 'verification_raison'] = "Domaine vide après nettoyage"
            continue

        # Même logique que find_websites.py : mots-clés significatifs dans le domaine
        keywords = extract_keywords(company_name)
        if not keywords:
            df.loc[index, 'verification_raison'] = "Aucun mot-clé extractible du nom"
            continue

        is_verified = False
        matched_keyword = None
        for keyword in keywords:
            if keyword in cleaned_domain:
                is_verified = True
                matched_keyword = keyword
                break

        if is_verified:
            df.loc[index, 'site_verifie'] = True
            df.loc[index, 'verification_raison'] = f"Mot-clé '{matched_keyword}' trouvé dans le domaine"
            verified_count += 1
        else:
            df.loc[index, 'verification_raison'] = f"Aucun mot-clé {keywords} dans '{domain}'"
            
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


def create_prospect_scoring_v2(input_path, output_path):
    """Scoring d'opportunité business (v2 révisé).

    Mesure la probabilité de deal agence web, pas la qualité SEO académique.

    Signaux positifs (opportunité) :
      Blog abandonné (blog_status)          : +5
      Blog semi-actif                       : +2
      Pas de blog                           : +1  (site vitrine souvent obsolète)
      nb_pages < 5                          : +3
      nb_pages 5–9                          : +1
      mots_moyen_par_page < 150             : +2
      ratio_texte_html < 0.15               : +2
      CMS non détecté (bricolé)             : +2
      CMS Wix ou Squarespace                : +1
      Pas de sitemap                        : +1
      pages sans meta desc  > 20 %          : +0.5 par tranche de 20 %
      pages sans H1         > 20 %          : +0.5 par tranche de 20 %
      titles dupliqués      > 30 % (ratio)  : +0.5
      pages vides           > 20 %          : +0.5 par tranche de 20 %

    Signaux négatifs (prospect moins prioritaire) :
      Blog actif + publication hebdo/mensuelle : -4
      nb_pages > 50                            : -3
      mots_moyen_par_page > 400                : -2
    """
    print(f"\nScoring d'opportunité business v2 (révisé)")
    df = pd.read_csv(input_path)

    df['prospect_score'] = 0.0
    df['prospect_summary'] = ''

    for index, row in df.iterrows():
        if not row.get('site_verifie', False):
            continue
        nb_pages = row.get('nb_pages', 0) or 0
        if nb_pages == 0:
            continue

        score = 0.0

        # ── Activité blog (signal le plus fort) ────────────────────────────────
        has_blog = bool(row.get('has_blog', False))
        blog_status = str(row.get('blog_status', '') or '').lower()
        frequence = str(row.get('frequence_publication', '') or '').lower()

        if has_blog:
            if blog_status == 'abandonné':
                score += 5
            elif blog_status == 'semi-actif':
                score += 2
            elif blog_status == 'actif':
                if frequence in ('hebdomadaire', 'mensuelle'):
                    score -= 4  # blog vivant et fréquent → prospect peu prioritaire
        else:
            score += 1  # sans blog = site vitrine souvent vieux

        # ── Taille du site ─────────────────────────────────────────────────────
        if nb_pages < 5:
            score += 3
        elif nb_pages < 10:
            score += 1
        elif nb_pages > 50:
            score -= 3

        # ── Densité de contenu ─────────────────────────────────────────────────
        mots_moyen = row.get('mots_moyen_par_page', 0) or 0
        if mots_moyen < 150:
            score += 2
        elif mots_moyen > 400:
            score -= 2

        ratio_texte = row.get('ratio_texte_html', 0) or 0
        if ratio_texte < 0.15:
            score += 2

        # ── CMS (signal tech) ──────────────────────────────────────────────────
        cms = str(row.get('cms_detecte', '') or '').strip()
        if not cms or cms.lower() in ('none', ''):
            score += 2  # site bricolé / technologie inconnue
        elif cms in ('Wix', 'Squarespace'):
            score += 1  # marché classique refonte

        # ── Pas de sitemap ─────────────────────────────────────────────────────
        if not row.get('has_sitemap', False):
            score += 1

        # ── Problèmes SEO (angles d'attaque commerciaux) ───────────────────────
        pages_sans_meta = row.get('pages_sans_meta_desc', 0) or 0
        pages_sans_h1 = row.get('pages_sans_h1', 0) or 0
        # titles_dupliques est maintenant un ratio (0.0–1.0) depuis seo_auditor v2
        title_ratio = row.get('titles_dupliques', 0) or 0
        pages_vides = row.get('pages_vides', 0) or 0

        ratio_meta = pages_sans_meta / nb_pages
        ratio_h1 = pages_sans_h1 / nb_pages
        ratio_vides = pages_vides / nb_pages

        score += 0.5 * int(ratio_meta / 0.2)
        score += 0.5 * int(ratio_h1 / 0.2)
        if title_ratio > 0.30:
            score += 0.5
        score += 0.5 * int(ratio_vides / 0.2)

        # ── Clamp 1–10 ─────────────────────────────────────────────────────────
        score = max(1.0, min(10.0, round(score, 1)))
        df.loc[index, 'prospect_score'] = score

        # ── Résumé textuel ─────────────────────────────────────────────────────
        opportunities = []
        if has_blog:
            if blog_status == 'abandonné':
                opportunities.append('blog abandonné')
            elif blog_status == 'semi-actif':
                opportunities.append('blog semi-actif')
        else:
            opportunities.append('pas de blog')
        if not row.get('has_sitemap', False):
            opportunities.append('pas de sitemap')
        if nb_pages < 5:
            opportunities.append(f'{int(nb_pages)} pages seulement')
        if mots_moyen < 150:
            opportunities.append(f'contenu faible ({int(mots_moyen)} mots/page)')
        if ratio_texte < 0.15:
            opportunities.append(f'ratio texte/HTML faible ({ratio_texte:.0%})')
        if cms and cms.lower() not in ('none', ''):
            opportunities.append(f'CMS : {cms}')
        if pages_sans_meta > 0:
            opportunities.append(f'{int(pages_sans_meta)} pages sans meta desc')
        if pages_sans_h1 > 0:
            opportunities.append(f'{int(pages_sans_h1)} pages sans H1')

        summary = f"Score {score}/10."
        if opportunities:
            summary += ' Opportunités : ' + ', '.join(opportunities) + '.'
        df.loc[index, 'prospect_summary'] = summary

    # Extraire l'année de création de l'entreprise (YYYY-MM-DD → YYYY)
    if 'dateCreationUniteLegale' in df.columns:
        df['annee_creation'] = df['dateCreationUniteLegale'].astype(str).str[:4].replace('nan', '')

    # Colonnes utiles pour une rédactrice SEO + renommage lisible
    col_map = {
        'denominationUniteLegale': 'entreprise',
        'site_web':                'site_web',
        'prospect_score':          'score',
        'annee_creation':          'annee_creation',
        'cms_detecte':             'cms',
        'nb_pages':                'nb_pages',
        'has_blog':                'blog',
        'blog_url':                'blog_url',
        'has_rss':                 'rss',
        'derniere_maj_blog':       'derniere_maj_blog',
        'frequence_publication':   'frequence_publication',
        'activite_status':         'activite',
        'derniere_date':           'derniere_maj_site',
        'has_sitemap':             'sitemap',
        'pages_sans_meta_desc':    'pages_sans_meta_desc',
        'pages_sans_h1':           'pages_sans_h1',
        'mots_moy_page':           'mots_moy_page',
        'mots_moyen_par_page':     'mots_moy_page',
        'prospect_summary':        'resume',
    }
    existing = {k: v for k, v in col_map.items() if k in df.columns}
    # dédoublonner si les deux variantes de mots_moy_page sont présentes
    seen_targets, deduped = set(), {}
    for src, tgt in existing.items():
        if tgt not in seen_targets:
            deduped[src] = tgt
            seen_targets.add(tgt)

    # Garder uniquement les entreprises avec un site audité (score > 0)
    df = df[df['prospect_score'] > 0].copy()

    final_df = (
        df.reindex(columns=list(deduped.keys()))
          .rename(columns=deduped)
          .sort_values(by='score', ascending=False)
    )

    final_df.to_csv(output_path, index=False)
    print(f"-> Scoring v2 terminé. Rapport final : {output_path}")
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
