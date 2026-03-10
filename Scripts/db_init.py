#!/usr/bin/env python3
"""
Initialise et maintient la base SQLite du projet.

Tables :
  entreprises   — données SIRENE (1 ligne / SIREN)
  sites_web     — URLs découvertes + signaux qualité
  site_health   — résultats health-checker
  seo_audits    — résultats crawl SEO
  contacts      — emails et téléphones extraits

Usage :
  # Première migration (depuis CSV nautisme_na)
  python Scripts/db_init.py --sector nautisme_na

  # Mettre à jour les dates manquantes via API SIRENE
  python Scripts/db_init.py --sector nautisme_na --fetch-only

  # Ne migrer sans appel API
  python Scripts/db_init.py --sector nautisme_na --no-fetch

  # Stats uniquement
  python Scripts/db_init.py --sector nautisme_na --stats-only
"""
from __future__ import annotations

import argparse
import sqlite3
import time
import logging
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

DB_PATH  = Path("DataBase/prospection.db")
API_URL  = "https://recherche-entreprises.api.gouv.fr/search"
DELAY    = 0.15
TIMEOUT  = 10

# ── Schéma complet ────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS entreprises (
    siren                TEXT    NOT NULL,
    secteur              TEXT    NOT NULL,   -- ex: nautisme_na, vins_bordeaux
    denomination         TEXT,
    naf                  TEXT,
    tranche_effectifs    TEXT,
    etat_administratif   TEXT    DEFAULT 'A',
    code_postal          TEXT,
    commune              TEXT,
    date_creation        TEXT,               -- YYYY-MM-DD
    api_fetched          INTEGER DEFAULT 0,
    PRIMARY KEY (siren, secteur)
);

CREATE TABLE IF NOT EXISTS sites_web (
    siren                TEXT    NOT NULL,
    secteur              TEXT    NOT NULL,
    url                  TEXT,
    statut               TEXT,   -- TROUVÉ / NON TROUVÉ
    source               TEXT,   -- v1 / v2 / gmaps
    confiance            REAL,
    secteur_ok           INTEGER,           -- 1 / 0 / NULL
    antibot              INTEGER DEFAULT 0,
    down_erreur          INTEGER DEFAULT 0,
    under_construction   INTEGER DEFAULT 0,
    snippet              TEXT,
    last_checked         TEXT,
    PRIMARY KEY (siren, secteur),
    FOREIGN KEY (siren, secteur) REFERENCES entreprises(siren, secteur)
);

CREATE TABLE IF NOT EXISTS site_health (
    siren                TEXT    NOT NULL,
    secteur              TEXT    NOT NULL,
    url                  TEXT,
    signal               TEXT,   -- pas_de_site / down / lent / site_ancien / sans_blog / ok
    priorite_score       REAL,
    is_down              INTEGER DEFAULT 0,
    down_reason          TEXT,
    response_time_ms     INTEGER,
    has_blog             INTEGER DEFAULT 0,
    blog_url             TEXT,
    agence_detectee      INTEGER DEFAULT 0,
    agence_nom           TEXT,
    annee_copyright      INTEGER,
    reseaux_sociaux      TEXT,   -- JSON
    checked_at           TEXT,
    PRIMARY KEY (siren, secteur),
    FOREIGN KEY (siren, secteur) REFERENCES entreprises(siren, secteur)
);

CREATE TABLE IF NOT EXISTS seo_audits (
    siren                TEXT    NOT NULL,
    secteur              TEXT    NOT NULL,
    url                  TEXT,
    score_prospect       REAL,
    nb_pages             INTEGER,
    has_sitemap          INTEGER DEFAULT 0,
    has_blog             INTEGER DEFAULT 0,
    blog_url             TEXT,
    blog_status          TEXT,   -- actif / semi-actif / abandonné / absent
    derniere_maj_blog    TEXT,
    frequence_pub        TEXT,
    activite_status      TEXT,
    cms_detecte          TEXT,
    mots_moyen_par_page  REAL,
    ratio_texte_html     REAL,
    titles_dupliques     REAL,
    pages_sans_meta_desc INTEGER,
    pages_sans_h1        INTEGER,
    pages_vides          INTEGER,
    resume               TEXT,
    audited_at           TEXT,
    PRIMARY KEY (siren, secteur),
    FOREIGN KEY (siren, secteur) REFERENCES entreprises(siren, secteur)
);

CREATE TABLE IF NOT EXISTS contacts (
    siren                TEXT    NOT NULL,
    secteur              TEXT    NOT NULL,
    email                TEXT,
    telephone            TEXT,
    source_page          TEXT,
    extracted_at         TEXT,
    PRIMARY KEY (siren, secteur),
    FOREIGN KEY (siren, secteur) REFERENCES entreprises(siren, secteur)
);

-- Index pour filtres fréquents
CREATE INDEX IF NOT EXISTS idx_entreprises_secteur ON entreprises(secteur);
CREATE INDEX IF NOT EXISTS idx_entreprises_naf     ON entreprises(naf);
CREATE INDEX IF NOT EXISTS idx_entreprises_cp      ON entreprises(code_postal);
CREATE INDEX IF NOT EXISTS idx_sites_statut        ON sites_web(secteur, statut);
CREATE INDEX IF NOT EXISTS idx_health_signal       ON site_health(secteur, signal);
"""


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
    log.info("Schéma initialisé → %s", DB_PATH)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bool(val) -> int | None:
    if str(val) == "True"  or val == 1: return 1
    if str(val) == "False" or val == 0: return 0
    return None

def _uc(snippet: str) -> int:
    _kw = ["under construction", "coming soon", "en construction",
           "site en cours", "actuellement en", "maintenance", "bientot"]
    return int(any(k in str(snippet).lower() for k in _kw))

def _conf(val) -> float | None:
    try:    return float(val) if val and str(val) not in ("", "nan") else None
    except: return None


# ── Migration entreprises ─────────────────────────────────────────────────────

def migrate_entreprises(conn: sqlite3.Connection, sector: str, csv_path: Path) -> int:
    if not csv_path.exists():
        log.warning("CSV introuvable : %s", csv_path)
        return 0

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    rows = [(
        r["siren"], sector,
        r.get("denominationUniteLegale", ""),
        r.get("activitePrincipaleUniteLegale", ""),
        r.get("trancheEffectifsUniteLegale", ""),
        r.get("etatAdministratifUniteLegale", "A"),
        r.get("codePostalEtablissement", ""),
        r.get("libelleCommuneEtablissement", ""),
        None, 0,
    ) for _, r in df.iterrows()]

    conn.executemany("""
        INSERT OR IGNORE INTO entreprises
        (siren, secteur, denomination, naf, tranche_effectifs, etat_administratif,
         code_postal, commune, date_creation, api_fetched)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    log.info("entreprises [%s] : %d lignes", sector, len(rows))
    return len(rows)


# ── Migration sites_web ───────────────────────────────────────────────────────

def migrate_sites_web(conn: sqlite3.Connection, sector: str,
                      compiled_csv: Path, verif_csv: Path | None = None) -> int:
    if not compiled_csv.exists():
        log.warning("CSV compilé introuvable : %s", compiled_csv)
        return 0

    df = pd.read_csv(compiled_csv, dtype=str).fillna("")

    if verif_csv and verif_csv.exists():
        verif = pd.read_csv(verif_csv, dtype=str)[
            ["siren", "secteur_ok", "antibot", "down_erreur", "snippet"]
        ].copy()
        df = df.merge(verif, on="siren", how="left")
    else:
        df["secteur_ok"] = df.get("secteur_ok", "")
        df["antibot"]    = df.get("antibot", "False")
        df["down_erreur"]= df.get("down_erreur", "False")
        df["snippet"]    = ""

    # Sources v2/gmaps : secteur garanti
    mask_new = df["source"].isin(["v2", "gmaps"])
    df.loc[mask_new, "secteur_ok"]  = "True"
    df.loc[mask_new, "antibot"]     = "False"
    df.loc[mask_new, "down_erreur"] = "False"

    # S'assurer que les SIRENs existent
    conn.executemany(
        "INSERT OR IGNORE INTO entreprises(siren, secteur) VALUES(?,?)",
        [(r["siren"], sector) for _, r in df.iterrows()]
    )

    rows = [(
        r["siren"], sector,
        r.get("site_web_final", "") or None,
        r.get("statut_final", "NON TROUVÉ"),
        r.get("source", "") or None,
        _conf(r.get("confiance_final")),
        _bool(r.get("secteur_ok", "")),
        1 if r.get("antibot") == "True" else 0,
        1 if r.get("down_erreur") == "True" else 0,
        _uc(r.get("snippet", "")),
        r.get("snippet", "") or None,
        None,
    ) for _, r in df.iterrows()]

    conn.executemany("""
        INSERT OR REPLACE INTO sites_web
        (siren, secteur, url, statut, source, confiance, secteur_ok,
         antibot, down_erreur, under_construction, snippet, last_checked)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    log.info("sites_web [%s] : %d lignes", sector, len(rows))
    return len(rows)


# ── Migration site_health ─────────────────────────────────────────────────────

def migrate_site_health(conn: sqlite3.Connection, sector: str, csv_path: Path) -> int:
    if not csv_path.exists():
        log.info("site_health introuvable (skip) : %s", csv_path)
        return 0

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    rows = []
    for _, r in df.iterrows():
        rs = r.get("reseaux_sociaux", "")
        # Normaliser en JSON si c'est déjà un dict-like string
        import json
        try:
            rs_json = json.dumps(json.loads(rs.replace("'", '"'))) if rs else None
        except Exception:
            rs_json = None

        rows.append((
            r.get("siren", ""), sector,
            r.get("site_web", "") or None,
            r.get("signal", ""),
            _conf(r.get("priorite_score")),
            1 if r.get("is_down") in ("True", "1") else 0,
            r.get("down_reason", "") or None,
            _conf(r.get("response_time_ms")),
            1 if r.get("has_blog") in ("True", "1") else 0,
            r.get("blog_url", "") or None,
            1 if r.get("agence_detectee") in ("True", "1") else 0,
            r.get("agence_nom", "") or None,
            _conf(r.get("annee_copyright")),
            rs_json,
            None,
        ))

    conn.executemany(
        "INSERT OR IGNORE INTO entreprises(siren, secteur) VALUES(?,?)",
        [(r[0], sector) for r in rows if r[0]]
    )
    conn.executemany("""
        INSERT OR REPLACE INTO site_health
        (siren, secteur, url, signal, priorite_score, is_down, down_reason,
         response_time_ms, has_blog, blog_url, agence_detectee, agence_nom,
         annee_copyright, reseaux_sociaux, checked_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    log.info("site_health [%s] : %d lignes", sector, len(rows))
    return len(rows)


# ── Migration seo_audits ──────────────────────────────────────────────────────

def migrate_seo_audits(conn: sqlite3.Connection, sector: str, csv_path: Path) -> int:
    if not csv_path.exists():
        log.info("seo_audit introuvable (skip) : %s", csv_path)
        return 0

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    rows = [(
        r.get("siren", ""), sector,
        r.get("site_web", "") or None,
        _conf(r.get("score")),
        _conf(r.get("nb_pages")),
        1 if r.get("has_sitemap") in ("True","1") else 0,
        1 if r.get("has_blog") in ("True","1") else 0,
        r.get("blog_url", "") or None,
        r.get("blog_status", "") or None,
        r.get("derniere_maj_blog", "") or None,
        r.get("frequence_publication", "") or None,
        r.get("activite_status", "") or None,
        r.get("cms_detecte", "") or None,
        _conf(r.get("mots_moyen_par_page")),
        _conf(r.get("ratio_texte_html")),
        _conf(r.get("titles_dupliques")),
        _conf(r.get("pages_sans_meta_desc")),
        _conf(r.get("pages_sans_h1")),
        _conf(r.get("pages_vides")),
        r.get("resume", "") or None,
        None,
    ) for _, r in df.iterrows()]

    conn.executemany(
        "INSERT OR IGNORE INTO entreprises(siren, secteur) VALUES(?,?)",
        [(r[0], sector) for r in rows if r[0]]
    )
    conn.executemany("""
        INSERT OR REPLACE INTO seo_audits
        (siren, secteur, url, score_prospect, nb_pages, has_sitemap, has_blog,
         blog_url, blog_status, derniere_maj_blog, frequence_pub, activite_status,
         cms_detecte, mots_moyen_par_page, ratio_texte_html, titles_dupliques,
         pages_sans_meta_desc, pages_sans_h1, pages_vides, resume, audited_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    log.info("seo_audits [%s] : %d lignes", sector, len(rows))
    return len(rows)


# ── Fetch dates manquantes ────────────────────────────────────────────────────

def fetch_missing_dates(conn: sqlite3.Connection, sector: str) -> None:
    cur = conn.execute(
        "SELECT siren FROM entreprises WHERE secteur=? AND (date_creation IS NULL OR date_creation='')",
        (sector,)
    )
    sirens = [r["siren"] for r in cur.fetchall()]
    if not sirens:
        log.info("Toutes les dates sont déjà renseignées pour le secteur %s", sector)
        return

    log.info("Dates manquantes à fetcher : %d", len(sirens))
    session = requests.Session()
    session.headers["Accept"] = "application/json"
    updated = errors = 0

    for i, siren in enumerate(sirens, 1):
        try:
            resp = session.get(API_URL, params={"q": siren, "page": 1, "per_page": 1},
                               timeout=TIMEOUT)
            results = resp.json().get("results", [])
            date = results[0].get("date_creation", "") if results else ""
            conn.execute(
                "UPDATE entreprises SET date_creation=?, api_fetched=1 WHERE siren=? AND secteur=?",
                (date or None, siren, sector)
            )
            updated += 1
        except Exception as e:
            log.warning("[%d/%d] SIREN %s — %s", i, len(sirens), siren, e)
            errors += 1

        if i % 100 == 0:
            conn.commit()
            log.info("  [%d/%d] %d dates récupérées", i, len(sirens), updated)

        time.sleep(DELAY)

    conn.commit()
    log.info("Fetch [%s] : %d mis à jour, %d erreurs", sector, updated, errors)


# ── Stats ─────────────────────────────────────────────────────────────────────

def print_stats(conn: sqlite3.Connection) -> None:
    tables = ["entreprises", "sites_web", "site_health", "seo_audits", "contacts"]
    print(f"\n{'─'*55}")
    print(f"  Base : {DB_PATH}  ({DB_PATH.stat().st_size//1024} Ko)")
    print(f"{'─'*55}")
    for t in tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        sectors = conn.execute(
            f"SELECT secteur, COUNT(*) FROM {t} GROUP BY secteur"
        ).fetchall()
        detail = "  |  ".join(f"{s['secteur']}: {s['COUNT(*)']}" for s in sectors)
        print(f"  {t:<16} {n:>5} lignes   {detail}")
    n_dates = conn.execute(
        "SELECT COUNT(*) FROM entreprises WHERE date_creation IS NOT NULL AND date_creation != ''"
    ).fetchone()[0]
    n_total = conn.execute("SELECT COUNT(*) FROM entreprises").fetchone()[0]
    print(f"\n  dates renseignées : {n_dates}/{n_total}")
    print(f"{'─'*55}\n")


# ── Secteurs connus ───────────────────────────────────────────────────────────

SECTOR_PATHS = {
    "nautisme_na": {
        "companies":  Path("Results/nautisme_na/filtered_companies.csv"),
        "compiled":   Path("Results/nautisme_na/filtered_companies_websites_compiled.csv"),
        "verif":      Path("Results/nautisme_na/v1_verification.csv"),
        "health":     Path("Results/nautisme_na/site_health.csv"),
        "seo":        Path("Results/nautisme_na/seo_audit.csv"),
    },
    "nautisme": {
        "companies":  Path("Results/nautisme/filtered_companies.csv"),
        "compiled":   Path("Results/nautisme/filtered_companies_websites.csv"),
        "verif":      None,
        "health":     Path("Results/nautisme/site_health.csv"),
        "seo":        Path("Results/nautisme/seo_audit.csv"),
    },
    "vins_bordeaux": {
        "companies":  Path("Results/vins_bordeaux/filtered_companies.csv"),
        "compiled":   Path("Results/vins_bordeaux/filtered_companies_websites.csv"),
        "verif":      None,
        "health":     Path("Results/vins_bordeaux/site_health.csv"),
        "seo":        Path("Results/vins_bordeaux/seo_audit.csv"),
    },
}


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Init/migration base SQLite prospection")
    parser.add_argument("--sector", "-s", default="nautisme_na",
                        choices=list(SECTOR_PATHS.keys()),
                        help="Secteur à migrer (défaut: nautisme_na)")
    parser.add_argument("--no-fetch",    action="store_true", help="Ne pas fetcher l'API SIRENE")
    parser.add_argument("--fetch-only",  action="store_true", help="Seulement fetcher les dates manquantes")
    parser.add_argument("--stats-only",  action="store_true", help="Afficher les stats uniquement")
    args = parser.parse_args()

    conn = get_conn()

    if args.stats_only:
        print_stats(conn)
        conn.close()
        return

    paths = SECTOR_PATHS[args.sector]

    if not args.fetch_only:
        init_schema(conn)
        migrate_entreprises(conn, args.sector, paths["companies"])
        migrate_sites_web(conn, args.sector, paths["compiled"], paths.get("verif"))
        migrate_site_health(conn, args.sector, paths["health"])
        migrate_seo_audits(conn, args.sector, paths["seo"])

    if not args.no_fetch:
        fetch_missing_dates(conn, args.sector)

    print_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
