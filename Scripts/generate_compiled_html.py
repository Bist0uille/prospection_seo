"""Génère un rapport HTML filtrable à partir du CSV compilé v1+v2 et de la vérification v1."""
import pandas as pd
import json
from pathlib import Path

INPUT_COMPILED = Path("Results/nautisme_na/filtered_companies_websites_compiled.csv")
INPUT_VERIF = Path("Results/nautisme_na/v1_verification.csv")
OUTPUT = Path("Results/nautisme_na/compiled_report.html")

df = pd.read_csv(INPUT_COMPILED).fillna("")

# Intègre les colonnes de vérification v1
if INPUT_VERIF.exists():
    verif = pd.read_csv(INPUT_VERIF)[["siren", "secteur_ok", "antibot", "down_erreur", "snippet"]].copy()
    verif = verif.rename(columns={
        "secteur_ok": "secteur_ok_v1",
        "antibot":    "antibot_v1",
        "down_erreur":"down_v1",
        "snippet":    "snippet_v1",
    })
    verif["siren"] = verif["siren"].astype(str)
    df["siren"] = df["siren"].astype(str)
    df = df.merge(verif, on="siren", how="left")
    df["secteur_ok_v1"] = df["secteur_ok_v1"].fillna("").astype(str)
    df["antibot_v1"]    = df["antibot_v1"].fillna(False)
    df["down_v1"]       = df["down_v1"].fillna(False)
    df["snippet_v1"]    = df["snippet_v1"].fillna("")

rows = df.to_dict(orient="records")
naf_values  = sorted(df["activitePrincipaleUniteLegale"].unique().tolist())
villes      = sorted(df["libelleCommuneEtablissement"].unique().tolist())

NAF_LABELS = {
    "3315Z": "Réparation navires",
    "5010Z": "Transport maritime passagers",
    "3012Z": "Construction bateaux plaisance",
    "5222Z": "Services auxiliaires transport eau",
    "7734Z": "Location bateaux",
    "3011Z": "Construction navires",
    "5020Z": "Transport maritime fret",
}

rows_json   = json.dumps(rows,       ensure_ascii=False)
naf_json    = json.dumps(naf_values, ensure_ascii=False)
villes_json = json.dumps(villes,     ensure_ascii=False)
naf_labels_json = json.dumps(NAF_LABELS, ensure_ascii=False)

html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Prospection Nautisme — Rapport compilé</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, -apple-system, sans-serif; background: #f5f7fa; color: #333; font-size: .875rem; }}
  header {{ background: #0a2540; color: white; padding: 16px 28px; display: flex; align-items: center; gap: 20px; flex-wrap: wrap; }}
  header h1 {{ font-size: 1.15rem; font-weight: 600; }}
  .stats {{ display: flex; gap: 10px; margin-left: auto; flex-wrap: wrap; }}
  .stat {{ background: rgba(255,255,255,.12); border-radius: 8px; padding: 6px 14px; text-align: center; }}
  .stat-val {{ font-size: 1.3rem; font-weight: 700; }}
  .stat-label {{ font-size: .65rem; opacity: .8; text-transform: uppercase; letter-spacing: .05em; }}
  .filters {{ background: white; border-bottom: 1px solid #e5e7eb; padding: 12px 28px; display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; }}
  .fg {{ display: flex; flex-direction: column; gap: 3px; }}
  .fg label {{ font-size: .7rem; font-weight: 600; color: #6b7280; text-transform: uppercase; letter-spacing: .05em; }}
  select, input[type=text] {{ border: 1px solid #d1d5db; border-radius: 6px; padding: 6px 9px; font-size: .8rem; background: white; min-width: 140px; }}
  select:focus, input:focus {{ outline: none; border-color: #0a2540; }}
  .btn {{ background: #e5e7eb; color: #374151; border: none; border-radius: 6px; padding: 7px 14px; cursor: pointer; font-size: .8rem; white-space: nowrap; align-self: flex-end; }}
  .count-bar {{ padding: 8px 28px; font-size: .8rem; color: #6b7280; background: #f9fafb; border-bottom: 1px solid #e5e7eb; }}
  .count-bar span {{ font-weight: 600; color: #0a2540; }}
  .table-wrap {{ overflow: auto; max-height: calc(100vh - 200px); }}
  table {{ width: 100%; border-collapse: collapse; font-size: .8rem; }}
  th {{ background: #f9fafb; padding: 8px 10px; text-align: left; font-weight: 600; color: #374151; border-bottom: 2px solid #e5e7eb; position: sticky; top: 0; z-index: 1; cursor: pointer; white-space: nowrap; user-select: none; }}
  th:hover {{ background: #f3f4f6; }}
  th.sorted {{ background: #eef2ff; color: #0a2540; }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #f3f4f6; vertical-align: middle; max-width: 220px; word-break: break-word; }}
  tr:hover td {{ background: #f9fafb; }}
  .badge {{ display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: .7rem; font-weight: 600; }}
  .b-trouve {{ background: #dcfce7; color: #166534; }}
  .b-non    {{ background: #fee2e2; color: #991b1b; }}
  .b-v1     {{ background: #dbeafe; color: #1e40af; }}
  .b-v2     {{ background: #ede9fe; color: #6d28d9; }}
  .b-conf   {{ background: #fef9c3; color: #854d0e; }}
  .b-ok     {{ background: #dcfce7; color: #166534; }}
  .b-fp     {{ background: #fee2e2; color: #991b1b; }}
  .b-ab     {{ background: #fef3c7; color: #92400e; }}
  .b-dn     {{ background: #f3f4f6; color: #6b7280; }}
  a {{ color: #0a2540; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .siren {{ font-family: monospace; font-size: .75rem; color: #9ca3af; }}
  .no-results {{ padding: 48px; text-align: center; color: #9ca3af; }}
  .sort-icon {{ opacity: .35; margin-left: 3px; }}
  th.sorted .sort-icon {{ opacity: 1; }}
</style>
</head>
<body>
<header>
  <div>
    <div style="font-size:.7rem;opacity:.7;margin-bottom:3px">Prospection · Secteur nautisme</div>
    <h1>Rapport compilé — sites web</h1>
  </div>
  <div class="stats">
    <div class="stat"><div class="stat-val" id="s-total">—</div><div class="stat-label">Affichés</div></div>
    <div class="stat"><div class="stat-val" id="s-trouve">—</div><div class="stat-label">Trouvés</div></div>
    <div class="stat"><div class="stat-val" id="s-v2">—</div><div class="stat-label">Récupérés v2</div></div>
    <div class="stat"><div class="stat-val" id="s-fp">—</div><div class="stat-label">Douteux v1</div></div>
  </div>
</header>

<div class="filters">
  <div class="fg"><label>Recherche</label><input type="text" id="f-search" placeholder="Nom, SIREN…"></div>
  <div class="fg"><label>Statut</label>
    <select id="f-statut">
      <option value="">Tous</option>
      <option value="TROUVÉ">Trouvé</option>
      <option value="NON TROUVÉ">Non trouvé</option>
    </select>
  </div>
  <div class="fg"><label>Source</label>
    <select id="f-source">
      <option value="">Toutes</option>
      <option value="v1">v1</option>
      <option value="v2">v2 (nouvelles)</option>
    </select>
  </div>
  <div class="fg"><label>Qualité v1</label>
    <select id="f-qualite">
      <option value="">Tous</option>
      <option value="ok">Secteur confirmé</option>
      <option value="fp">Douteux</option>
      <option value="ab">Anti-bot</option>
      <option value="dn">Down/erreur</option>
    </select>
  </div>
  <div class="fg"><label>Code NAF</label>
    <select id="f-naf"><option value="">Tous</option></select>
  </div>
  <div class="fg"><label>Ville</label>
    <select id="f-ville"><option value="">Toutes</option></select>
  </div>
  <button class="btn" onclick="resetFilters()">Réinitialiser</button>
</div>

<div class="count-bar"><span id="count-display">—</span> résultats</div>

<div class="table-wrap">
<table>
  <thead>
    <tr>
      <th onclick="sortBy('denominationUniteLegale')" data-col="denominationUniteLegale">Entreprise <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('siren')" data-col="siren">SIREN <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('codePostalEtablissement')" data-col="codePostalEtablissement">CP <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('libelleCommuneEtablissement')" data-col="libelleCommuneEtablissement">Ville <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('activitePrincipaleUniteLegale')" data-col="activitePrincipaleUniteLegale">NAF <span class="sort-icon">↕</span></th>
      <th>Site web</th>
      <th onclick="sortBy('statut_final')" data-col="statut_final">Statut <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('source')" data-col="source">Source <span class="sort-icon">↕</span></th>
      <th>Qualité</th>
      <th onclick="sortBy('confiance_final')" data-col="confiance_final">Conf. <span class="sort-icon">↕</span></th>
    </tr>
  </thead>
  <tbody id="table-body"></tbody>
</table>
<div id="no-results" class="no-results" style="display:none">Aucun résultat.</div>
</div>

<script>
const DATA       = {rows_json};
const NAF_LABELS = {naf_labels_json};
const NAF_VALUES = {naf_json};
const VILLES     = {villes_json};

let currentSort = {{ col: '', dir: 1 }};
let filtered = [...DATA];

// Populate selects
NAF_VALUES.forEach(v => {{
  const o = document.createElement('option');
  o.value = v;
  o.textContent = v + (NAF_LABELS[v] ? ' — ' + NAF_LABELS[v] : '');
  document.getElementById('f-naf').appendChild(o);
}});
VILLES.forEach(v => {{
  if (!v) return;
  const o = document.createElement('option');
  o.value = o.textContent = v;
  document.getElementById('f-ville').appendChild(o);
}});

function qualite(r) {{
  if (r.source !== 'v1') return 'n/a';
  if (r.secteur_ok_v1 === 'True')   return 'ok';
  if (r.antibot_v1 === true || r.antibot_v1 === 'True') return 'ab';
  if (r.down_v1    === true || r.down_v1    === 'True') return 'dn';
  return 'fp';
}}

function applyFilters() {{
  const search  = document.getElementById('f-search').value.toLowerCase().trim();
  const statut  = document.getElementById('f-statut').value;
  const source  = document.getElementById('f-source').value;
  const qualF   = document.getElementById('f-qualite').value;
  const naf     = document.getElementById('f-naf').value;
  const ville   = document.getElementById('f-ville').value;

  filtered = DATA.filter(r => {{
    if (search && !r.denominationUniteLegale.toLowerCase().includes(search) && !String(r.siren).includes(search)) return false;
    if (statut && r.statut_final !== statut) return false;
    if (source && r.source !== source) return false;
    if (qualF  && qualite(r) !== qualF) return false;
    if (naf    && r.activitePrincipaleUniteLegale !== naf) return false;
    if (ville  && r.libelleCommuneEtablissement !== ville) return false;
    return true;
  }});

  if (currentSort.col) applySort(); else render();
}}

function sortBy(col) {{
  if (currentSort.col === col) currentSort.dir *= -1;
  else {{ currentSort.col = col; currentSort.dir = 1; }}
  document.querySelectorAll('th').forEach(t => t.classList.remove('sorted'));
  document.querySelector(`th[data-col="${{col}}"]`)?.classList.add('sorted');
  applySort();
}}

function applySort() {{
  const {{ col, dir }} = currentSort;
  filtered.sort((a, b) => String(a[col]||'').localeCompare(String(b[col]||''), 'fr', {{numeric:true}}) * dir);
  render();
}}

function render() {{
  const tbody = document.getElementById('table-body');
  const trouve = filtered.filter(r => r.statut_final === 'TROUVÉ').length;
  const v2     = filtered.filter(r => r.source === 'v2').length;
  const fp     = filtered.filter(r => qualite(r) === 'fp').length;

  document.getElementById('s-total').textContent  = filtered.length;
  document.getElementById('s-trouve').textContent = trouve;
  document.getElementById('s-v2').textContent     = v2;
  document.getElementById('s-fp').textContent     = fp;
  document.getElementById('count-display').textContent = filtered.length + ' / ' + DATA.length;

  if (!filtered.length) {{
    tbody.innerHTML = '';
    document.getElementById('no-results').style.display = 'block';
    return;
  }}
  document.getElementById('no-results').style.display = 'none';

  tbody.innerHTML = filtered.map(r => {{
    const statBadge = r.statut_final === 'TROUVÉ'
      ? '<span class="badge b-trouve">TROUVÉ</span>'
      : '<span class="badge b-non">NON TROUVÉ</span>';

    const srcBadge = r.source === 'v1' ? '<span class="badge b-v1">v1</span>'
                   : r.source === 'v2' ? '<span class="badge b-v2">v2</span>' : '';

    const q = qualite(r);
    const qualBadge = q === 'ok' ? '<span class="badge b-ok">✓ secteur</span>'
                    : q === 'fp' ? '<span class="badge b-fp">⚠ douteux</span>'
                    : q === 'ab' ? '<span class="badge b-ab">🔒 anti-bot</span>'
                    : q === 'dn' ? '<span class="badge b-dn">↓ down</span>' : '';

    const confBadge = r.confiance_final ? `<span class="badge b-conf">${{r.confiance_final}}</span>` : '';
    const site = r.site_web_final
      ? `<a href="${{r.site_web_final}}" target="_blank" rel="noopener">${{r.site_web_final.replace(/^https?:\\/\\//, '').replace(/\\/$/, '').substring(0,35)}}</a>`
      : '<span style="color:#d1d5db">—</span>';
    const naf = r.activitePrincipaleUniteLegale;
    const nafSub = NAF_LABELS[naf] ? `<br><span style="color:#9ca3af;font-size:.7rem">${{NAF_LABELS[naf]}}</span>` : '';

    return `<tr>
      <td>${{r.denominationUniteLegale}}</td>
      <td><span class="siren">${{r.siren}}</span></td>
      <td>${{r.codePostalEtablissement}}</td>
      <td>${{r.libelleCommuneEtablissement}}</td>
      <td>${{naf}}${{nafSub}}</td>
      <td>${{site}}</td>
      <td>${{statBadge}}</td>
      <td>${{srcBadge}}</td>
      <td>${{qualBadge}}</td>
      <td>${{confBadge}}</td>
    </tr>`;
  }}).join('');
}}

function resetFilters() {{
  ['f-search','f-statut','f-source','f-qualite','f-naf','f-ville'].forEach(id => {{
    const el = document.getElementById(id);
    if (el.tagName === 'INPUT') el.value = ''; else el.value = '';
  }});
  applyFilters();
}}

['f-statut','f-source','f-qualite','f-naf','f-ville'].forEach(id =>
  document.getElementById(id).addEventListener('change', applyFilters));
document.getElementById('f-search').addEventListener('input', applyFilters);

applyFilters();
</script>
</body>
</html>
"""

OUTPUT.write_text(html, encoding="utf-8")
print(f"Généré : {OUTPUT}  ({OUTPUT.stat().st_size // 1024} Ko)")
