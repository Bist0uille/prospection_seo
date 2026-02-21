-- ============================================================================
-- botparser — PostgreSQL schema
-- INSEE unités légales + établissements (structure annuaire complet)
-- Toutes les colonnes en TEXT pour éviter les problèmes de type/NULL avec
-- les CSV INSEE qui mélangent formats et valeurs manquantes.
-- ============================================================================

CREATE TABLE IF NOT EXISTS unites_legales (
    -- Identifiants
    siren                                       TEXT PRIMARY KEY,
    nic                                         TEXT,
    siret                                       TEXT,

    -- Diffusion
    "statutDiffusionEtablissement"              TEXT,
    "statutDiffusionUniteLegale"                TEXT,
    "unitePurgeeUniteLegale"                    TEXT,

    -- Dates
    "dateCreationEtablissement"                 TEXT,
    "dateCreationUniteLegale"                   TEXT,
    "dateDernierTraitementEtablissement"        TEXT,
    "dateDernierTraitementUniteLegale"          TEXT,

    -- Effectifs établissement
    "trancheEffectifsEtablissement"             TEXT,
    "anneeEffectifsEtablissement"               TEXT,

    -- Activité établissement
    "activitePrincipaleRegistreMetiersEtablissement" TEXT,
    "activitePrincipaleEtablissement"           TEXT,
    "nomenclatureActivitePrincipaleEtablissement" TEXT,
    "activitePrincipaleNAF25Etablissement"      TEXT,

    -- Établissement flags
    "etablissementSiege"                        TEXT,
    "etatAdministratifEtablissement"            TEXT,
    "caractereEmployeurEtablissement"           TEXT,

    -- Unité légale — état
    "etatAdministratifUniteLegale"              TEXT,

    -- Unité légale — identité
    "categorieJuridiqueUniteLegale"             TEXT,
    "denominationUniteLegale"                   TEXT,
    "sigleUniteLegale"                          TEXT,
    "sexeUniteLegale"                           TEXT,
    "nomUniteLegale"                            TEXT,
    "nomUsageUniteLegale"                       TEXT,
    "prenom1UniteLegale"                        TEXT,
    "prenom2UniteLegale"                        TEXT,
    "prenom3UniteLegale"                        TEXT,
    "prenom4UniteLegale"                        TEXT,
    "prenomUsuelUniteLegale"                    TEXT,
    "pseudonymeUniteLegale"                     TEXT,

    -- Unité légale — activité
    "activitePrincipaleUniteLegale"             TEXT,
    "nomenclatureActivitePrincipaleUniteLegale" TEXT,

    -- Unité légale — effectifs
    "trancheEffectifsUniteLegale"               TEXT,
    "anneeEffectifsUniteLegale"                 TEXT,
    "nicSiegeUniteLegale"                       TEXT,

    -- Unité légale — autres
    "identifiantAssociationUniteLegale"         TEXT,
    "economieSocialeSolidaireUniteLegale"       TEXT,
    "societeMissionUniteLegale"                 TEXT,
    "categorieEntreprise"                       TEXT,
    "anneeCategorieEntreprise"                  TEXT,

    -- Adresse établissement
    "complementAdresseEtablissement"            TEXT,
    "numeroVoieEtablissement"                   TEXT,
    "indiceRepetitionEtablissement"             TEXT,
    "dernierNumeroVoieEtablissement"            TEXT,
    "typeVoieEtablissement"                     TEXT,
    "libelleVoieEtablissement"                  TEXT,
    "codePostalEtablissement"                   TEXT,
    "libelleCommuneEtablissement"               TEXT,
    "libelleCommuneEtrangerEtablissement"       TEXT,
    "codeCommuneEtablissement"                  TEXT,
    "codePaysEtrangerEtablissement"             TEXT,
    "libellePaysEtrangerEtablissement"          TEXT,
    "identifiantAdresseEtablissement"           TEXT,
    "coordonneeLambertAbscisseEtablissement"    TEXT,
    "coordonneeLambertOrdonneeEtablissement"    TEXT,

    -- Enseigne
    "enseigne1Etablissement"                    TEXT,
    "enseigne2Etablissement"                    TEXT,
    "enseigne3Etablissement"                    TEXT,
    "denominationUsuelleEtablissement"          TEXT
);

-- ── Index pour les requêtes pipeline ─────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_naf
    ON unites_legales ("activitePrincipaleUniteLegale");

CREATE INDEX IF NOT EXISTS idx_etat
    ON unites_legales ("etatAdministratifUniteLegale");

CREATE INDEX IF NOT EXISTS idx_tranche
    ON unites_legales ("trancheEffectifsUniteLegale");

CREATE INDEX IF NOT EXISTS idx_siege
    ON unites_legales ("etablissementSiege");
