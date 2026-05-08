document.addEventListener('DOMContentLoaded', () => {
  const form          = document.getElementById('search-form');
  const industryEl    = document.getElementById('industry');
  const mspEl         = document.getElementById('msp');
  const clientNameEl  = document.getElementById('client-name');
  const locationEl    = document.getElementById('location');
  const searchBtn     = document.getElementById('search-btn');
  const searchIcon    = document.getElementById('search-icon');
  const spinnerIcon   = document.getElementById('spinner-icon');

  const geocodeAlert  = document.getElementById('geocode-alert');
  const geocodeMsg    = document.getElementById('geocode-msg');
  const errorAlert    = document.getElementById('error-alert');
  const errorMsg      = document.getElementById('error-msg');

  const emptyState    = document.getElementById('empty-state');
  const noResults     = document.getElementById('no-results');
  const resultsWrapper = document.getElementById('results-wrapper');
  const resultsBody   = document.getElementById('results-body');
  const resultCount   = document.getElementById('result-count');
  const countNum      = document.getElementById('count-num');

  // -------------------------------------------------------------------------
  // Format DB enum value for display: "CATERING_COMPANY" → "Catering Company"
  // -------------------------------------------------------------------------
  function formatIndustry(val) {
    if (!val) return '—';
    return val
      .replace(/_/g, ' ')
      .toLowerCase()
      .replace(/\b\w/g, c => c.toUpperCase());
  }

  // -------------------------------------------------------------------------
  // Load options (MSPs + industries)
  // -------------------------------------------------------------------------
  async function loadOptions() {
    try {
      const resp = await fetch('/similar-client-report/api/options');
      const data = await resp.json();

      data.industries.forEach(ind => {
        const opt = document.createElement('option');
        opt.value = ind;               // raw DB enum sent to backend
        opt.textContent = formatIndustry(ind);  // human-readable label
        industryEl.appendChild(opt);
      });

      data.msps.forEach(m => {
        const opt = document.createElement('option');
        opt.value = m.id;
        opt.textContent = m.name;
        mspEl.appendChild(opt);
      });
    } catch (e) {
      console.error('Failed to load options:', e);
    }
  }

  loadOptions();

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------
  function setLoading(loading) {
    searchBtn.disabled = loading;
    searchIcon.classList.toggle('hidden', loading);
    spinnerIcon.classList.toggle('hidden', !loading);
  }

  function hideAlerts() {
    geocodeAlert.classList.add('hidden');
    errorAlert.classList.add('hidden');
  }

  function showGeocodeWarning(msg) {
    geocodeMsg.textContent = msg;
    geocodeAlert.classList.remove('hidden');
  }

  function showError(msg) {
    errorMsg.textContent = msg;
    errorAlert.classList.remove('hidden');
  }

  function showState(state) {
    emptyState.classList.add('hidden');
    noResults.classList.add('hidden');
    resultsWrapper.classList.add('hidden');
    resultCount.classList.add('hidden');

    if (state === 'empty')   emptyState.classList.remove('hidden');
    if (state === 'none')    noResults.classList.remove('hidden');
    if (state === 'results') {
      resultsWrapper.classList.remove('hidden');
      resultCount.classList.remove('hidden');
    }
  }

  function formatDate(dateStr) {
    if (!dateStr) return '—';
    try {
      const d = new Date(dateStr);
      return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    } catch {
      return dateStr;
    }
  }

  function formatMiles(miles) {
    if (miles === null || miles === undefined) return '—';
    return miles.toLocaleString('en-US', { maximumFractionDigits: 1 }) + ' mi';
  }

  function shiftBadge(count) {
    const cls = count >= 50
      ? 'bg-emerald-100 text-emerald-800'
      : count >= 10
        ? 'bg-blue-100 text-blue-800'
        : count > 0
          ? 'bg-gray-100 text-gray-700'
          : 'bg-red-50 text-red-600';
    return `<span class="inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${cls}">${count}</span>`;
  }

  function renderResults(results) {
    resultsBody.innerHTML = '';

    results.forEach((r, i) => {
      const tr = document.createElement('tr');
      tr.className = i % 2 === 0 ? 'bg-white hover:bg-emerald-50/30 transition-colors' : 'bg-gray-50/50 hover:bg-emerald-50/30 transition-colors';

      const locationStr = [r.city, r.state].filter(Boolean).join(', ') || '—';

      tr.innerHTML = `
        <td class="px-6 py-4 text-gray-400 font-medium">${i + 1}</td>
        <td class="px-6 py-4">
          <div class="font-semibold text-gray-900">${r.client_name || '—'}</div>
          <div class="text-xs text-gray-400 mt-0.5">${locationStr}</div>
        </td>
        <td class="px-6 py-4">
          <span class="inline-block px-2.5 py-1 rounded-full bg-emerald-50 text-emerald-700 text-xs font-medium border border-emerald-100">
            ${formatIndustry(r.industry)}
          </span>
        </td>
        <td class="px-6 py-4 text-gray-700">${r.msp || '—'}</td>
        <td class="px-6 py-4 text-right">${shiftBadge(r.shifts_last_year || 0)}</td>
        <td class="px-6 py-4 text-gray-600">${formatDate(r.last_shift_date)}</td>
        <td class="px-6 py-4 text-right text-gray-600">${formatMiles(r.miles)}</td>
      `;
      resultsBody.appendChild(tr);
    });
  }

  // -------------------------------------------------------------------------
  // Form submit
  // -------------------------------------------------------------------------
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    hideAlerts();

    const industry = industryEl.value.trim();
    if (!industry) {
      showError('Please select an industry before searching.');
      return;
    }

    setLoading(true);

    try {
      const resp = await fetch('/similar-client-report/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          client_name: clientNameEl.value.trim(),
          industry,
          location: locationEl.value.trim(),
          msp_id: mspEl.value || null,
        }),
      });

      const data = await resp.json();

      if (!resp.ok) {
        showError(data.error || 'An error occurred. Please try again.');
        showState('empty');
        return;
      }

      if (data.geocode_error) {
        showGeocodeWarning(data.geocode_error);
      }

      const results = data.results || [];
      if (results.length === 0) {
        showState('none');
      } else {
        renderResults(results);
        countNum.textContent = results.length;
        showState('results');
      }
    } catch (err) {
      showError('Network error. Please check your connection and try again.');
      showState('empty');
    } finally {
      setLoading(false);
    }
  });
});
