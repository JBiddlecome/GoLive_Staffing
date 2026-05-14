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
  const ALL_INDUSTRIES_VALUE = '__ALL__';

  // AI Modal DOM Elements
  const aiModal = document.getElementById('ai-modal');
  const closeAiModalBtn = document.getElementById('close-ai-modal');
  const aiModalClientName = document.getElementById('ai-modal-client-name');
  const aiLoading = document.getElementById('ai-loading');
  const aiResult = document.getElementById('ai-result');
  const aiError = document.getElementById('ai-error');

  closeAiModalBtn.addEventListener('click', () => {
    aiModal.classList.add('hidden');
  });

  window.runAiAnalysis = async function(clientName) {
    aiModal.classList.remove('hidden');
    aiModalClientName.textContent = clientName;
    aiLoading.classList.remove('hidden');
    aiResult.classList.add('hidden');
    aiError.classList.add('hidden');

    try {
      const formData = new FormData();
      formData.append('client_name', clientName);
      formData.append('custom_prompt', 'Just basic information presented in a very short reply. A brief summary of who is being communicated with (client email or name), brief summary of what they order and when, and when the last communication was.');

      const resp = await fetch('/client-communication-summary/analyze', {
        method: 'POST',
        body: formData,
      });

      const data = await resp.json();

      if (!resp.ok) {
        aiError.textContent = data.error || 'An error occurred during analysis.';
        aiError.classList.remove('hidden');
      } else {
        aiResult.textContent = data.analysis;
        aiResult.classList.remove('hidden');
      }
    } catch (e) {
      aiError.textContent = 'Network error. Please try again.';
      aiError.classList.remove('hidden');
    } finally {
      aiLoading.classList.add('hidden');
    }
  };

  // -------------------------------------------------------------------------
  // Weights Toggle & Sliders
  // -------------------------------------------------------------------------
  const weightsToggle = document.getElementById('weights-toggle');
  const weightsPanel = document.getElementById('weights-panel');
  const weightsToggleIcon = document.getElementById('weights-toggle-icon');

  const weightIndustry = document.getElementById('weight-industry');
  const weightIndustryVal = document.getElementById('weight-industry-val');
  const weightMsp = document.getElementById('weight-msp');
  const weightMspVal = document.getElementById('weight-msp-val');
  const weightShifts = document.getElementById('weight-shifts');
  const weightShiftsVal = document.getElementById('weight-shifts-val');
  const weightProximity = document.getElementById('weight-proximity');
  const weightProximityVal = document.getElementById('weight-proximity-val');

  weightsToggle.addEventListener('click', () => {
    const isHidden = weightsPanel.classList.contains('hidden');
    weightsPanel.classList.toggle('hidden', !isHidden);
    weightsToggleIcon.classList.toggle('rotate-180', isHidden);
  });

  weightIndustry.addEventListener('input', () => {
    weightIndustryVal.textContent = Number(weightIndustry.value);
  });

  weightMsp.addEventListener('input', () => {
    weightMspVal.textContent = Number(weightMsp.value);
  });

  weightShifts.addEventListener('input', () => {
    weightShiftsVal.textContent = Number(weightShifts.value);
  });

  weightProximity.addEventListener('input', () => {
    weightProximityVal.textContent = Number(weightProximity.value);
  });

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

      industryEl.innerHTML = '';
      const allOpt = document.createElement('option');
      allOpt.value = ALL_INDUSTRIES_VALUE;
      allOpt.textContent = 'All industries';
      allOpt.selected = true;
      industryEl.appendChild(allOpt);

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

  industryEl.addEventListener('change', () => {
    const selectedValues = Array.from(industryEl.selectedOptions).map(opt => opt.value);
    if (selectedValues.includes(ALL_INDUSTRIES_VALUE) && selectedValues.length > 1) {
      Array.from(industryEl.options).forEach(opt => {
        if (opt.value === ALL_INDUSTRIES_VALUE) opt.selected = false;
      });
    }
  });

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

  // Handle key validation for score sorting and display
  function formatScore(score) {
    if (score === null || score === undefined) return '0.0';
    return Number(score).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  }

  function showError(msg) {
    errorMsg.textContent = msg;
    errorAlert.classList.remove('hidden');
  }

  function getSelectedIndustryPayload() {
    const selectedValues = Array.from(industryEl.selectedOptions)
      .map(opt => opt.value.trim())
      .filter(Boolean);
    const allIndustries = selectedValues.length === 0 || selectedValues.includes(ALL_INDUSTRIES_VALUE);
    return {
      all_industries: allIndustries,
      industries: allIndustries
        ? []
        : selectedValues.filter(value => value !== ALL_INDUSTRIES_VALUE),
    };
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

  // Handle plural/singular and empty values for miles
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
      const escapedName = (r.client_name || '').replace(/'/g, "\\'").replace(/"/g, '&quot;');

      tr.innerHTML = `
        <td class="px-6 py-4 text-gray-400 font-medium">${i + 1}</td>
        <td class="px-6 py-4">
          <div class="font-semibold text-emerald-700 hover:text-emerald-800 cursor-pointer underline" onclick="runAiAnalysis('${escapedName}')">${r.client_name || '—'}</div>
          <div class="text-xs text-gray-400 mt-0.5">${locationStr}</div>
        </td>
        <td class="px-6 py-4">
          <span class="inline-block px-2.5 py-1 rounded-full bg-gray-50 text-gray-700 text-xs font-medium border border-gray-200">
            ${r.status || 'Unknown'}
          </span>
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
        <td class="px-6 py-4 text-right font-bold text-emerald-600">${formatScore(r.score)}</td>
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

    const industryPayload = getSelectedIndustryPayload();

    setLoading(true);

    try {
      const resp = await fetch('/similar-client-report/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          client_name: clientNameEl.value.trim(),
          industries: industryPayload.industries,
          all_industries: industryPayload.all_industries,
          location: locationEl.value.trim(),
          msp_id: mspEl.value || null,
          weight_industry: parseFloat(weightIndustry.value),
          weight_msp: parseFloat(weightMsp.value),
          weight_shifts: parseFloat(weightShifts.value),
          weight_proximity: parseFloat(weightProximity.value),
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
