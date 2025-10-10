const els = {
  yearFromSel: document.getElementById('year_from_select'),
  yearToSel: document.getElementById('year_to_select'),
  yearFromInp: document.getElementById('year_from_input'),
  yearToInp: document.getElementById('year_to_input'),
  yearsList: document.getElementById('years_list'),

  originSel: document.getElementById('origin_select'),
  originInp: document.getElementById('origin_input'),
  originsList: document.getElementById('origins_list'),

  modeRadios: document.querySelectorAll('input[name="mode"]'),
  loader: document.getElementById('loader'),
  status: document.getElementById('status'),
  go: document.getElementById('go'),
  link: document.getElementById('download'),
};

let OPTIONS = { years: [], origins: [], quarters: ["1","2","3","4"] };

function showLoader(msg){
  els.loader.classList.remove('hidden');
  els.status.textContent = msg || '';
}
function hideLoader(){
  els.loader.classList.add('hidden');
}

function setMode(mode){
  const useDropdown = mode === 'dropdown';
  document.querySelectorAll('.mode-dropdown').forEach(el => el.classList.toggle('hidden', !useDropdown));
  document.querySelectorAll('.mode-typeahead').forEach(el => el.classList.toggle('hidden', useDropdown));
}

async function loadOptions(){
  // ❌ no spinner here anymore
  try{
    const res = await fetch('/api/list');
    if(!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    OPTIONS = {
      years: data.years || [],
      origins: data.origins || ["ALL"],
      quarters: data.quarters || ["1","2","3","4"]
    };

    // populate dropdowns
    [els.yearFromSel, els.yearToSel].forEach(sel => {
      sel.innerHTML = '';
      OPTIONS.years.forEach(y => sel.add(new Option(y, y)));
    });
    els.originSel.innerHTML = '';
    OPTIONS.origins.forEach(o => els.originSel.add(new Option(o, o)));

    // populate datalists (typeahead)
    els.yearsList.innerHTML = OPTIONS.years.map(y => `<option value="${y}">`).join('');
    els.originsList.innerHTML = OPTIONS.origins.map(o => `<option value="${o}">`).join('');

    // defaults
    if(OPTIONS.years.length){
      els.yearFromSel.value = OPTIONS.years[0];
      els.yearToSel.value = OPTIONS.years[OPTIONS.years.length-1];
      els.yearFromInp.value = els.yearFromSel.value;
      els.yearToInp.value = els.yearToSel.value;
    }
    if(OPTIONS.origins.length){
      els.originSel.value = OPTIONS.origins[0];
      els.originInp.value = els.originSel.value;
    }

    els.status.textContent = ''; // keep page clean
  }catch(err){
    console.error(err);
    els.status.textContent = 'Failed to load options.';
  }
}

function getSelectedQuarters(){
  return Array.from(document.querySelectorAll('input[name="quarters"]:checked')).map(el => el.value);
}

function getValuePair(dropdownEl, inputEl){
  const inTypeahead = !inputEl.classList.contains('hidden');
  return (inTypeahead ? inputEl.value : dropdownEl.value || '').trim();
}

async function downloadFile(){
  els.link.classList.add('hidden');
  els.status.textContent = '';
  els.go.disabled = true;           // prevent double clicks
  showLoader("Building your file…"); // ✅ spinner only during build

  const yf = getValuePair(els.yearFromSel, els.yearFromInp);
  const yt = getValuePair(els.yearToSel, els.yearToInp);
  const origin = (getValuePair(els.originSel, els.originInp) || 'ALL').toUpperCase();

  if(!/^\d{4}$/.test(yf) || !/^\d{4}$/.test(yt)){
    hideLoader();
    els.go.disabled = false;
    els.status.textContent = 'Please provide valid 4-digit years.';
    return;
  }
  const quarters = getSelectedQuarters().join(',') || '1,2,3,4';

  try{
    const res = await fetch(`/api/download?year_from=${encodeURIComponent(yf)}&year_to=${encodeURIComponent(yt)}&quarters=${encodeURIComponent(quarters)}&origin=${encodeURIComponent(origin)}`);
    if(!res.ok){
      const txt = await res.text();
      throw new Error(txt || `HTTP ${res.status}`);
    }
    const data = await res.json();
    els.status.textContent = data.cached ? 'Ready (served from cache).' : 'Ready.';
    els.link.href = data.download_url;
    els.link.textContent = `Download ${data.rows ? `(${Number(data.rows).toLocaleString()} rows)` : ''}`;
    els.link.classList.remove('hidden');
  }catch(err){
    console.error(err);
    els.status.textContent = `Error: ${err.message || err}`;
  }finally{
    hideLoader();            // ✅ stop spinner after build
    els.go.disabled = false; // re-enable button
  }
}

// events
els.modeRadios.forEach(r => {
  r.addEventListener('change', e => setMode(e.target.value));
});
document.getElementById('go').addEventListener('click', downloadFile);

// init
setMode('dropdown');
window.onload = loadOptions;

// init
setMode('dropdown');

document.addEventListener('DOMContentLoaded', () => {
  hideLoader();     // <- make sure it's off on first paint
  loadOptions();    // this no longer shows the loader
});

