"""Génère un rapport HTML filtrable à partir du CSV compilé."""
import pandas as pd
import json
from pathlib import Path

INPUT_COMPILED   = Path("Results/nautisme_na/filtered_companies_websites_compiled.csv")
INPUT_VERIF      = Path("Results/nautisme_na/v1_verification.csv")
INPUT_COMPANIES  = Path("Results/nautisme_na/filtered_companies.csv")
OUTPUT = Path("Results/nautisme_na/compiled_report.html")

# Codes INSEE tranche effectifs → (label court, ordre de tri)
TRANCHE_EFFECTIFS = {
    "NN": ("NC",         0),
    "00": ("0",          1),
    "01": ("1–2",        2),
    "02": ("3–5",        3),
    "03": ("6–9",        4),
    "11": ("10–19",      5),
    "12": ("20–49",      6),
    "21": ("50–99",      7),
    "22": ("100–199",    8),
    "31": ("200–249",    9),
    "32": ("250–499",   10),
    "41": ("500–999",   11),
    "42": ("1 000–1 999", 12),
    "51": ("2 000–4 999", 13),
    "52": ("5 000–9 999", 14),
    "53": ("10 000+",   15),
}

df = pd.read_csv(INPUT_COMPILED).fillna("")

# Joindre la tranche d'effectifs depuis filtered_companies.csv
if INPUT_COMPANIES.exists():
    eff = pd.read_csv(INPUT_COMPANIES, usecols=["siren", "trancheEffectifsUniteLegale"]).copy()
    eff["siren"] = eff["siren"].astype(str)
    df["siren"]  = df["siren"].astype(str)
    df = df.merge(eff, on="siren", how="left")
    df["trancheEffectifsUniteLegale"] = df["trancheEffectifsUniteLegale"].fillna("NN").astype(str)
else:
    df["trancheEffectifsUniteLegale"] = "NN"

# Intègre les signaux de qualité (secteur confirmé, anti-bot, down)
# Pour les sites trouvés par l'ancienne méthode, on a la vérif manuelle.
# Pour les sites trouvés par la nouvelle méthode, secteur_ok=True par construction.
_UC_KEYWORDS = [
    "under construction", "coming soon", "en construction",
    "site en cours", "en cours de", "actuellement en", "maintenance",
    "bientôt", "bientot", "we're working", "we are working",
    "site is down", "offline", "be back soon",
]

def _is_under_construction(snippet: str) -> bool:
    s = str(snippet).lower()
    return any(k in s for k in _UC_KEYWORDS)

if INPUT_VERIF.exists():
    verif = pd.read_csv(INPUT_VERIF)[["siren","secteur_ok","antibot","down_erreur","snippet"]].copy()
    verif["siren"] = verif["siren"].astype(str)
    verif["under_construction"] = verif["snippet"].apply(_is_under_construction)
    df["siren"] = df["siren"].astype(str)
    df = df.merge(verif, on="siren", how="left")
    # Sites trouvés par la nouvelle méthode : secteur garanti
    mask_new = df["source"] == "v2"
    df.loc[mask_new, "secteur_ok"]         = True
    df.loc[mask_new, "antibot"]            = False
    df.loc[mask_new, "down_erreur"]        = False
    df.loc[mask_new, "under_construction"] = False
    df["secteur_ok"]         = df["secteur_ok"].fillna("").astype(str)
    df["antibot"]            = df["antibot"].fillna("False").astype(str)
    df["down_erreur"]        = df["down_erreur"].fillna("False").astype(str)
    df["under_construction"] = df["under_construction"].fillna(False).astype(bool)
else:
    df["secteur_ok"]         = ""
    df["antibot"]            = "False"
    df["down_erreur"]        = "False"
    df["under_construction"] = False

# Faux positifs clairs (hors secteur, pas UC, pas antibot, pas down) → traités comme NON TROUVÉ
mask_fp = (
    (df["statut_final"] == "TROUVÉ") &
    (df["secteur_ok"] == "False") &
    (~df["under_construction"]) &
    (df["antibot"] != "True") &
    (df["down_erreur"] != "True")
)
df.loc[mask_fp, "statut_final"] = "NON TROUVÉ"
df.loc[mask_fp, "site_web_final"] = ""

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

TRANCHE_JS = {code: {"label": v[0], "sort": v[1]} for code, v in TRANCHE_EFFECTIFS.items()}

rows_json        = json.dumps(rows,        ensure_ascii=False)
naf_json         = json.dumps(naf_values,  ensure_ascii=False)
villes_json      = json.dumps(villes,      ensure_ascii=False)
naf_labels_json  = json.dumps(NAF_LABELS,  ensure_ascii=False)
tranche_json     = json.dumps(TRANCHE_JS,  ensure_ascii=False)

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
  .b-conf   {{ background: #fef9c3; color: #854d0e; }}
  .b-ok     {{ background: #dcfce7; color: #166534; }}
  .b-fp     {{ background: #fee2e2; color: #991b1b; }}
  .b-ab     {{ background: #fef3c7; color: #92400e; }}
  .b-dn     {{ background: #f3f4f6; color: #6b7280; }}
  .b-uc     {{ background: #fef9c3; color: #713f12; }}
  a {{ color: #0a2540; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .siren {{ font-family: monospace; font-size: .75rem; color: #9ca3af; }}
  .no-results {{ padding: 48px; text-align: center; color: #9ca3af; }}
  .sort-icon {{ opacity: .35; margin-left: 3px; }}
  th.sorted .sort-icon {{ opacity: 1; }}
  .btn-mark {{ border: none; border-radius: 6px; padding: 2px 8px; cursor: pointer; font-size: .75rem; font-weight: 600; background: #f3f4f6; color: #9ca3af; transition: all .15s; white-space: nowrap; }}
  .btn-mark:hover {{ background: #fee2e2; color: #dc2626; }}
  .btn-mark.bad  {{ background: #fee2e2; color: #dc2626; }}
  .btn-mark.good {{ background: #dcfce7; color: #166534; }}
  tr.is-bad td {{ opacity: .4; text-decoration: line-through; }}
  tr.is-bad .btn-mark {{ opacity: 1; text-decoration: none; }}
  tr.is-good td {{ background: #f0fdf4; }}
  tr.is-good .btn-mark {{ opacity: 1; }}
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
    <div class="stat"><div class="stat-val" id="s-non">—</div><div class="stat-label">Non trouvés</div></div>
    <div class="stat"><div class="stat-val" id="s-uc">—</div><div class="stat-label">🚧 Constr.</div></div>
    <div class="stat" style="background:rgba(239,68,68,.2)"><div class="stat-val" id="s-bad">0</div><div class="stat-label">Mauvais ✗</div></div>
    <div class="stat" style="background:rgba(34,197,94,.2)"><div class="stat-val" id="s-good">0</div><div class="stat-label">Bons ✓</div></div>
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
  <div class="fg"><label>Fiabilité</label>
    <select id="f-qualite">
      <option value="">Tous</option>
      <option value="ok">✓ Sûr</option>
      <option value="ab">⚡ Anti-bot</option>
      <option value="uc">🚧 Construction</option>
      <option value="dn">↓ Down</option>
    </select>
  </div>
  <div class="fg"><label>Code NAF</label>
    <select id="f-naf"><option value="">Tous</option></select>
  </div>
  <div class="fg"><label>Ville</label>
    <select id="f-ville"><option value="">Toutes</option></select>
  </div>
  <div class="fg"><label>Confiance min</label>
    <select id="f-conf">
      <option value="">Toutes</option>
      <option value="3.0">≥ 3.0</option>
      <option value="3.5">≥ 3.5</option>
      <option value="4.0">≥ 4.0</option>
      <option value="4.5">≥ 4.5</option>
    </select>
  </div>
  <div class="fg"><label>Marquage</label>
    <select id="f-bad">
      <option value="">Tous</option>
      <option value="hide-bad">Masquer mauvais</option>
      <option value="only-bad">Mauvais seulement</option>
      <option value="only-good">Bons seulement</option>
      <option value="only-neutral">Non marqués</option>
    </select>
  </div>
  <button class="btn" onclick="resetFilters()">Réinitialiser</button>
  <button class="btn" style="background:#fee2e2;color:#dc2626" onclick="clearMarks()">Effacer marquages</button>
</div>

<div class="count-bar"><span id="count-display">—</span> résultats</div>

<div class="table-wrap">
<table>
  <thead>
    <tr>
      <th style="width:50px;text-align:center">✗</th>
      <th onclick="sortBy('denominationUniteLegale')" data-col="denominationUniteLegale">Entreprise <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('siren')" data-col="siren">SIREN <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('codePostalEtablissement')" data-col="codePostalEtablissement">CP <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('libelleCommuneEtablissement')" data-col="libelleCommuneEtablissement">Ville <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('activitePrincipaleUniteLegale')" data-col="activitePrincipaleUniteLegale">NAF <span class="sort-icon">↕</span></th>
      <th onclick="sortBy('trancheEffectifsUniteLegale')" data-col="trancheEffectifsUniteLegale">Effectif <span class="sort-icon">↕</span></th>
      <th>Site web</th>
      <th onclick="sortBy('statut_final')" data-col="statut_final">Statut / Fiabilité <span class="sort-icon">↕</span></th>
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
const TRANCHES   = {tranche_json};

let currentSort = {{ col: '', dir: 1 }};
let filtered = [...DATA];

// ── Marquage 3 états persisté dans localStorage ───────────────────────────
// markMap : siren → 'bad' | 'good'  (absent = neutre)
const LS_KEY = 'nautisme_marks_v2';
let markMap = new Map(Object.entries(JSON.parse(localStorage.getItem(LS_KEY) || '{{}}')));

function saveMarks() {{
  localStorage.setItem(LS_KEY, JSON.stringify(Object.fromEntries(markMap)));
  document.getElementById('s-bad').textContent  = [...markMap.values()].filter(v => v === 'bad').length;
  document.getElementById('s-good').textContent = [...markMap.values()].filter(v => v === 'good').length;
}}

// Cycle : neutre → bad → good → neutre
function toggleMark(siren) {{
  const cur = markMap.get(siren) || 'neutral';
  const next = cur === 'neutral' ? 'bad' : cur === 'bad' ? 'good' : 'neutral';
  if (next === 'neutral') markMap.delete(siren);
  else markMap.set(siren, next);
  saveMarks();
  // Mise à jour visuelle de la ligne sans re-render
  const btn = document.querySelector(`button[data-siren="${{siren}}"]`);
  const row = btn?.closest('tr');
  if (!btn || !row) return;
  row.className = next === 'bad' ? 'is-bad' : next === 'good' ? 'is-good' : '';
  btn.className = 'btn-mark' + (next === 'bad' ? ' bad' : next === 'good' ? ' good' : '');
  btn.textContent = next === 'bad' ? '✗ mauvais' : next === 'good' ? '✓ bon' : '· marquer';
  if (document.getElementById('f-bad').value) applyFilters();
}}

function clearMarks() {{
  const total = markMap.size;
  if (!total || !confirm(`Effacer les ${{total}} marquages ?`)) return;
  markMap.clear();
  saveMarks();
  applyFilters();
}}

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
  if (r.statut_final !== 'TROUVÉ') return '';
  if (r.under_construction === true || r.under_construction === 'True') return 'uc';
  if (r.secteur_ok === 'True' || r.secteur_ok === true)   return 'ok';
  if (r.antibot === true || r.antibot === 'True')          return 'ab';
  if (r.down_erreur === true || r.down_erreur === 'True')  return 'dn';
  return 'ok';  // v2 garanti secteur
}}

// Score pour tri fiabilité : plus haut = plus fiable
function fiabiliteScore(r) {{
  if (r.statut_final !== 'TROUVÉ') return 0;
  const q = qualite(r);
  if (q === 'ok') return 4;
  if (q === 'ab') return 3;  // anti-bot : on sait pas, peut être ok
  if (q === 'uc') return 2;  // en construction : à surveiller
  if (q === 'dn') return 1;  // down : inaccessible
  return 1;
}}

function applyFilters() {{
  const search  = document.getElementById('f-search').value.toLowerCase().trim();
  const statut  = document.getElementById('f-statut').value;
  const qualF   = document.getElementById('f-qualite').value;
  const naf     = document.getElementById('f-naf').value;
  const ville   = document.getElementById('f-ville').value;
  const conf    = parseFloat(document.getElementById('f-conf').value) || 0;
  const badF    = document.getElementById('f-bad').value;

  filtered = DATA.filter(r => {{
    if (search && !r.denominationUniteLegale.toLowerCase().includes(search) && !String(r.siren).includes(search)) return false;
    if (statut && r.statut_final !== statut) return false;
    if (qualF && qualite(r) !== qualF) return false;
    if (naf    && r.activitePrincipaleUniteLegale !== naf) return false;
    if (ville  && r.libelleCommuneEtablissement !== ville) return false;
    if (conf   && r.statut_final === 'TROUVÉ' && (parseFloat(r.confiance_final) || 0) < conf) return false;
    const mark = markMap.get(String(r.siren)) || 'neutral';
    if (badF === 'hide-bad'    && mark === 'bad')     return false;
    if (badF === 'only-bad'    && mark !== 'bad')     return false;
    if (badF === 'only-good'   && mark !== 'good')    return false;
    if (badF === 'only-neutral'&& mark !== 'neutral') return false;
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
  filtered.sort((a, b) => {{
    if (col === 'statut_final') {{
      return (fiabiliteScore(b) - fiabiliteScore(a)) * dir;
    }}
    if (col === 'trancheEffectifsUniteLegale') {{
      const sa = (TRANCHES[a[col]] || {{sort:0}}).sort;
      const sb = (TRANCHES[b[col]] || {{sort:0}}).sort;
      return (sa - sb) * dir;
    }}
    return String(a[col]||'').localeCompare(String(b[col]||''), 'fr', {{numeric:true}}) * dir;
  }});
  render();
}}

function render() {{
  const tbody = document.getElementById('table-body');
  const trouve = filtered.filter(r => r.statut_final === 'TROUVÉ').length;
  const non    = filtered.filter(r => r.statut_final === 'NON TROUVÉ').length;
  const uc     = filtered.filter(r => qualite(r) === 'uc').length;

  document.getElementById('s-total').textContent  = filtered.length;
  document.getElementById('s-trouve').textContent = trouve;
  document.getElementById('s-non').textContent    = non;
  document.getElementById('s-uc').textContent     = uc;
  document.getElementById('count-display').textContent = filtered.length + ' / ' + DATA.length;

  if (!filtered.length) {{
    tbody.innerHTML = '';
    document.getElementById('no-results').style.display = 'block';
    return;
  }}
  document.getElementById('no-results').style.display = 'none';

  tbody.innerHTML = filtered.map(r => {{
    const q = qualite(r);
    const statBadge = r.statut_final !== 'TROUVÉ'
      ? '<span class="badge b-non">NON TROUVÉ</span>'
      : q === 'ok' ? '<span class="badge b-ok">✓ sûr</span>'
      : q === 'ab' ? '<span class="badge b-ab">⚡ anti-bot</span>'
      : q === 'uc' ? '<span class="badge b-uc">🚧 construction</span>'
      : q === 'dn' ? '<span class="badge b-dn">↓ down</span>'
      : '<span class="badge b-ok">✓ sûr</span>';

    const raisonBadge = '';

    const confBadge = r.confiance_final ? `<span class="badge b-conf">${{r.confiance_final}}</span>` : '';
    const site = r.site_web_final
      ? `<a href="${{r.site_web_final}}" target="_blank" rel="noopener">${{r.site_web_final.replace(/^https?:\\/\\//, '').replace(/\\/$/, '').substring(0,40)}}</a>`
      : '<span style="color:#d1d5db">—</span>';
    const naf = r.activitePrincipaleUniteLegale;
    const nafSub = NAF_LABELS[naf] ? `<br><span style="color:#9ca3af;font-size:.7rem">${{NAF_LABELS[naf]}}</span>` : '';

    const tranche = r.trancheEffectifsUniteLegale || 'NN';
    const trancheLabel = (TRANCHES[tranche] || {{label:'NC'}}).label;
    const trancheStyle = tranche === 'NN' ? 'color:#d1d5db' : (TRANCHES[tranche]||{{sort:0}}).sort >= 7 ? 'color:#166534;font-weight:600' : '';

    const mark = markMap.get(String(r.siren)) || 'neutral';
    const markLabel = mark === 'bad' ? '✗ mauvais' : mark === 'good' ? '✓ bon' : '· marquer';
    const markBtn = `<button class="btn-mark${{mark === 'bad' ? ' bad' : mark === 'good' ? ' good' : ''}}" data-siren="${{r.siren}}" onclick="toggleMark('${{r.siren}}')">${{markLabel}}</button>`;
    return `<tr class="${{mark === 'bad' ? 'is-bad' : mark === 'good' ? 'is-good' : ''}}">
      <td style="text-align:center">${{markBtn}}</td>
      <td>${{r.denominationUniteLegale}}</td>
      <td><span class="siren">${{r.siren}}</span></td>
      <td>${{r.codePostalEtablissement}}</td>
      <td>${{r.libelleCommuneEtablissement}}</td>
      <td>${{naf}}${{nafSub}}</td>
      <td style="text-align:center;font-size:.75rem;${{trancheStyle}}">${{trancheLabel}}</td>
      <td>${{site}}</td>
      <td>${{statBadge}}${{raisonBadge}}</td>
      <td>${{confBadge}}</td>
    </tr>`;
  }}).join('');
}}

function resetFilters() {{
  ['f-search','f-statut','f-qualite','f-naf','f-ville','f-conf','f-bad'].forEach(id =>
    document.getElementById(id).value = '');
  applyFilters();
}}

['f-statut','f-qualite','f-naf','f-ville','f-conf','f-bad'].forEach(id =>
  document.getElementById(id).addEventListener('change', applyFilters));
document.getElementById('f-search').addEventListener('input', applyFilters);

saveMarks();  // initialise les compteurs depuis localStorage
applyFilters();
</script>
</body>
</html>
"""

OUTPUT.write_text(html, encoding="utf-8")
print(f"Généré : {OUTPUT}  ({OUTPUT.stat().st_size // 1024} Ko)")
