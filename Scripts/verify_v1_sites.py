"""Vérifie les sites trouvés en v1 avec la méthode secteur_ok de la v2."""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
import requests
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from Scripts.find_websites_v2 import _extract_snippet, _is_secteur_ok

INPUT = PROJECT_ROOT / "Results/nautisme_na/filtered_companies_websites.csv"
OUTPUT = PROJECT_ROOT / "Results/nautisme_na/v1_verification.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

df = pd.read_csv(INPUT)
v1 = df[df["statut_recherche"] == "TROUVÉ"].copy()
print(f"{len(v1)} sites v1 à vérifier")

results = []

for _, row in tqdm(v1.iterrows(), total=len(v1), desc="verify-v1"):
    url = str(row.get("site_web", "")).strip()
    naf = str(row.get("activitePrincipaleUniteLegale", "")).strip()
    secteur_ok = False
    snippet = ""
    http_status = ""

    if not url:
        http_status = "NO_URL"
    else:
        try:
            resp = requests.get(url, timeout=8, allow_redirects=True, headers=HEADERS)
            http_status = str(resp.status_code)
            if resp.status_code == 200:
                snippet = _extract_snippet(resp.text)
                secteur_ok = _is_secteur_ok(snippet, naf)
            elif resp.status_code in {403, 429, 503}:
                http_status = f"{resp.status_code}_ANTIBOT"
                snippet = "(anti-bot)"
        except requests.exceptions.ConnectionError:
            http_status = "DNS_ERROR"
        except requests.exceptions.Timeout:
            http_status = "TIMEOUT"
        except Exception as e:
            http_status = f"ERROR:{e}"

    is_antibot = "ANTIBOT" in http_status
    is_down = any(s in http_status for s in ("DNS_ERROR", "TIMEOUT", "ERROR", "404", "410"))

    results.append({
        "siren": row["siren"],
        "denominationUniteLegale": row["denominationUniteLegale"],
        "activitePrincipaleUniteLegale": naf,
        "codePostalEtablissement": row.get("codePostalEtablissement", ""),
        "libelleCommuneEtablissement": row.get("libelleCommuneEtablissement", ""),
        "site_web_v1": url,
        "http_status": http_status,
        "antibot": is_antibot,
        "down_erreur": is_down,
        "secteur_ok": secteur_ok,
        "snippet": snippet,
    })

df_out = pd.DataFrame(results)
df_out.to_csv(OUTPUT, index=False)

total = len(df_out)
ok = df_out["secteur_ok"].sum()
ko = total - ok
antibot = df_out["http_status"].str.contains("ANTIBOT").sum()
down = df_out["http_status"].str.contains("DNS_ERROR|TIMEOUT|ERROR|40[04]").sum()

print(f"\n=== Résultats ===")
print(f"Total v1       : {total}")
print(f"secteur_ok=True: {ok} ({100*ok/total:.1f}%) — confirmés nautisme")
print(f"secteur_ok=False: {ko} ({100*ko/total:.1f}%) — douteux")
print(f"  dont anti-bot : {antibot} (non vérifiables)")
print(f"  dont down     : {down}")
print(f"\nFichier : {OUTPUT}")
