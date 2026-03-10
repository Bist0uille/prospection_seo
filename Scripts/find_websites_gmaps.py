#!/usr/bin/env python3
"""
Passe supplémentaire Google Maps Places pour les entreprises sans site trouvé.

Lit le CSV compilé, cherche chaque entreprise NON TROUVÉ via l'API Google Maps
Places (TextSearch + PlaceDetails), valide le secteur et met à jour le CSV.

Nécessite une clé API Google Maps Platform :
  export GOOGLE_MAPS_API_KEY="AIza..."
  python Scripts/find_websites_gmaps.py --limit 20

Tarification Google Maps API (indicatif) :
  - Text Search  : ~0.032 $/appel → $200/mois gratuit ≈ 6 250 appels gratuits
  - Place Details: ~0.017 $/appel (si nécessaire)

Usage:
  python Scripts/find_websites_gmaps.py --limit 20          # test 20 entreprises
  python Scripts/find_websites_gmaps.py                     # toutes les NON TROUVÉ
  python Scripts/find_websites_gmaps.py --output results.csv
"""
from __future__ import annotations

import os
import sys
import time
import argparse
import logging
from pathlib import Path

import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()  # charge .env automatiquement

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_COMPILED = Path("Results/nautisme_na/filtered_companies_websites_compiled.csv")
OUTPUT_DEFAULT = Path("Results/nautisme_na/filtered_companies_websites_gmaps.csv")

TEXTSEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

DELAY = 0.3   # secondes entre appels
TIMEOUT = 10

# Mots-clés secteur nautisme (même liste que find_websites.py)
_SECTEUR_KEYWORDS = [
    "bateau", "voilier", "yacht", "plaisance", "chantier", "accastillage",
    "marine", "navigation", "nautique", "nautisme", "port", "marina",
    "sailing", "boatyard", "hull", "rigging", "chandlery", "nautical",
    "shipyard", "catamaran", "trimaran", "moteur", "hors-bord",
]

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def _api_key() -> str:
    key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    if not key:
        sys.exit(
            "ERREUR : variable d'environnement GOOGLE_MAPS_API_KEY non définie.\n"
            "  export GOOGLE_MAPS_API_KEY='AIza...'"
        )
    return key


def _is_secteur_ok(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in _SECTEUR_KEYWORDS)


def textsearch_new(query: str, key: str) -> list[dict]:
    """Places API (New) — POST avec field mask en header."""
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask": "places.displayName,places.websiteUri,places.formattedAddress",
    }
    body = {
        "textQuery": query,
        "pageSize": 3,
        "languageCode": "fr",
    }
    try:
        r = requests.post(TEXTSEARCH_URL, headers=headers, json=body, timeout=TIMEOUT)
        if r.status_code != 200:
            log.warning("TextSearch HTTP %s: %s — query: %s", r.status_code, r.text[:120], query)
            return []
        return r.json().get("places", [])
    except Exception as e:
        log.warning("TextSearch error: %s", e)
        return []


def find_website_gmaps(
    nom: str,
    commune: str,
    code_postal: str,
    key: str,
) -> tuple[str | None, float]:
    """
    Cherche le site web d'une entreprise via Google Maps Places API (New).
    Retourne (url, confiance) ou (None, 0.0).
    """
    # Extraire l'alias entre parenthèses si présent, sinon nom légal nettoyé
    import re as _re
    alias_match = _re.search(r'\(([^)]{4,})\)', nom)
    nom_clean = alias_match.group(1).split(" OU ")[0].strip() if alias_match else nom.split("(")[0].strip()

    places = textsearch_new(f"{nom_clean} {commune}", key)

    if not places:
        # 2e tentative avec code postal
        places = textsearch_new(f"{nom_clean} {code_postal} France", key)

    if not places:
        return None, 0.0

    place = places[0]
    website = place.get("websiteUri", "").strip()

    if not website:
        return None, 0.0

    # ── Validation secteur : on fetche le site et on vérifie les mots-clés ──
    try:
        resp = requests.get(
            website, timeout=8,
            headers={"User-Agent": "Mozilla/5.0"},
            allow_redirects=True,
        )
        if resp.status_code in (200, 403):
            # 403 = anti-bot, on garde l'URL sans valider le secteur
            if resp.status_code == 403:
                snippet = ""
                secteur_ok = None  # inconnu
            else:
                import re as _re2
                text = resp.text[:8000]
                def _extract(pattern):
                    m = _re2.search(pattern, text, _re2.I | _re2.S)
                    return _re2.sub(r'<[^>]+>', '', m.group(1)) if m else ""
                snippet = " ".join(filter(None, [
                    _extract(r'<title[^>]*>(.*?)</title>'),
                    _extract(r'<h1[^>]*>(.*?)</h1>'),
                    _extract(r'<p[^>]*>(.*?)</p>'),
                ]))
                secteur_ok = _is_secteur_ok(snippet)
                if not secteur_ok:
                    log.debug("  secteur KO (%s) → rejeté", website)
                    return None, 0.0
        else:
            # Site down ou erreur → on rejette
            return None, 0.0
    except Exception:
        # Timeout, DNS error → on rejette
        return None, 0.0

    # Score de confiance
    conf = 0.4
    name_maps = place.get("displayName", {}).get("text", "").lower()
    nom_kw = nom_clean.lower().split()[0] if nom_clean else ""
    if nom_kw and len(nom_kw) >= 4 and nom_kw in name_maps:
        conf += 0.4
    address = place.get("formattedAddress", "")
    if code_postal and code_postal in address:
        conf += 0.2
    if secteur_ok is None:
        conf = min(conf, 0.6)  # anti-bot : confiance plafonnée

    return website, round(conf, 2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Recherche sites via Google Maps Places")
    parser.add_argument("--limit", type=int, default=None, help="Limiter à N entreprises")
    parser.add_argument("--output", type=Path, default=OUTPUT_DEFAULT, help="Fichier de sortie")
    parser.add_argument("--input", type=Path, default=INPUT_COMPILED, help="CSV compilé d'entrée")
    parser.add_argument("--min-employees", type=int, default=1,
                        help="Effectif minimum (1=exclure 0 et NN, 0=tous)")
    args = parser.parse_args()

    key = _api_key()

    # Codes INSEE effectifs → ordre numérique (plus grand = plus de salariés)
    _TRANCHE_ORDER = {
        "53": 15, "52": 14, "51": 13, "42": 12, "41": 11,
        "32": 10, "31": 9,  "22": 8,  "21": 7,  "12": 6,
        "11": 5,  "03": 4,  "02": 3,  "01": 2,  "00": 1, "NN": 0,
    }

    df = pd.read_csv(args.input).fillna("")
    non_trouve = df[df["statut_final"] == "NON TROUVÉ"].copy()

    # Joindre l'effectif
    companies_csv = args.input.parent / "filtered_companies.csv"
    if companies_csv.exists():
        eff = pd.read_csv(companies_csv, usecols=["siren", "trancheEffectifsUniteLegale"])
        eff["siren"] = eff["siren"].astype(str)
        non_trouve["siren"] = non_trouve["siren"].astype(str)
        non_trouve = non_trouve.merge(eff, on="siren", how="left")
        non_trouve["trancheEffectifsUniteLegale"] = non_trouve["trancheEffectifsUniteLegale"].fillna("NN")
    else:
        non_trouve["trancheEffectifsUniteLegale"] = "NN"

    # Filtre effectif minimum
    if args.min_employees > 0:
        # Exclure NN (inconnu) et tranches en-dessous du seuil
        _EXCLUDED = {"NN", "00"}  # 0 salarié ou inconnu
        before = len(non_trouve)
        non_trouve = non_trouve[~non_trouve["trancheEffectifsUniteLegale"].isin(_EXCLUDED)]
        log.info("Filtre effectif >0 : %d → %d entreprises", before, len(non_trouve))

    # Tri par effectif décroissant
    non_trouve["_eff_sort"] = non_trouve["trancheEffectifsUniteLegale"].map(_TRANCHE_ORDER).fillna(0)
    non_trouve = non_trouve.sort_values("_eff_sort", ascending=False).drop(columns=["_eff_sort"])
    log.info("Trié par effectif décroissant")

    if args.limit:
        non_trouve = non_trouve.head(args.limit)

    log.info("Recherche Google Maps pour %d entreprises NON TROUVÉ", len(non_trouve))

    results = []
    found = 0

    for i, (_, row) in enumerate(non_trouve.iterrows(), 1):
        nom      = str(row["denominationUniteLegale"])
        commune  = str(row.get("libelleCommuneEtablissement", ""))
        cp       = str(row.get("codePostalEtablissement", ""))
        siren    = str(row["siren"])

        log.info("[%d/%d] %s (%s)", i, len(non_trouve), nom, commune)

        website, conf = find_website_gmaps(nom, commune, cp, key)

        if website:
            found += 1
            log.info("  ✓ %s (conf=%.2f)", website, conf)
        else:
            log.info("  — non trouvé")

        results.append({
            "siren":    siren,
            "denomination": nom,
            "commune":  commune,
            "site_gmaps": website or "",
            "confiance_gmaps": conf if website else "",
            "statut_gmaps": "TROUVÉ" if website else "NON TROUVÉ",
        })

        time.sleep(DELAY)

    out_df = pd.DataFrame(results)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.output, index=False)

    log.info(
        "\nTerminé : %d/%d trouvés (%.1f%%)  → %s",
        found, len(non_trouve),
        100 * found / len(non_trouve) if non_trouve.shape[0] else 0,
        args.output,
    )


if __name__ == "__main__":
    main()
