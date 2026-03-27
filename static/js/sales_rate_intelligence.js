document.addEventListener('DOMContentLoaded', () => {
  const elPosition = document.getElementById('sri-pos');
  const elCounty = document.getElementById('sri-county');
  const elIndustry = document.getElementById('sri-industry');
  const elDateStart = document.getElementById('sri-date-start');
  const elDateEnd = document.getElementById('sri-date-end');
  const btnApply = document.getElementById('sri-btn-apply');
  const btnExport = document.getElementById('sri-btn-export');

  // KPI Els
  const elKpiBill = document.getElementById('sri-kpi-bill');
  const elKpiPay = document.getElementById('sri-kpi-pay');
  const elKpiSpread = document.getElementById('sri-kpi-spread');
  const elKpiMarkup = document.getElementById('sri-kpi-markup');
  const elKpiClients = document.getElementById('sri-kpi-clients');
  const elKpiRecords = document.getElementById('sri-kpi-records');
  const elWarning = document.getElementById('sri-warning');
  const elWarningMsg = document.getElementById('sri-warning-msg');

  // Benchmark Panel Els
  const elBillMin = document.getElementById('sri-bill-min');
  const elBill25 = document.getElementById('sri-bill-25');
  const elBillMedian = document.getElementById('sri-bill-median');
  const elBill75 = document.getElementById('sri-bill-75');
  const elBillMax = document.getElementById('sri-bill-max');
  
  const elBillBar = document.getElementById('sri-bill-bar');

  const elPayMin = document.getElementById('sri-pay-min');
  const elPay25 = document.getElementById('sri-pay-25');
  const elPayMedian = document.getElementById('sri-pay-median');
  const elPay75 = document.getElementById('sri-pay-75');
  const elPayMax = document.getElementById('sri-pay-max');
  
  const elPayBar = document.getElementById('sri-pay-bar');

  // Table Els
  const elTableBody = document.getElementById('sri-table-body');
  
  let initialHtml = '<tr><td colspan="8" style="text-align:center;">Please select your filters and click Refresh Benchmark to compile the data.</td></tr>';
  elTableBody.innerHTML = initialHtml;

  function initDates() {
    // default to last 12 months
    const today = new Date();
    const lastYear = new Date(today);
    lastYear.setFullYear(today.getFullYear() - 1);
    
    elDateEnd.value = today.toISOString().split('T')[0];
    elDateStart.value = lastYear.toISOString().split('T')[0];
  }

  async function loadFilters() {
    try {
      const resp = await fetch('/sales-rate-intelligence/api/filters');
      const data = await resp.json();
      
      const posHtml = data.positions.map(p => `<option value="${p.position_id}">${p.position_name}</option>`).join('');
      elPosition.innerHTML += posHtml;

      const countyHtml = data.counties.map(c => `<option value="${c.county_id}">${c.county_name}</option>`).join('');
      elCounty.innerHTML += countyHtml;

      const indHtml = data.industries.map(i => `<option value="${i}">${i}</option>`).join('');
      elIndustry.innerHTML += indHtml;

    } catch (err) {
      console.error("Failed to load filters", err);
    }
  }

  function getQueryString(isExport = false) {
    const params = new URLSearchParams();
    if (elPosition.value) params.append('position_id', elPosition.value);
    if (elCounty.value) params.append('county_id', elCounty.value);
    if (elIndustry.value) params.append('industry', elIndustry.value);
    if (elDateStart.value) params.append('date_start', elDateStart.value);
    if (elDateEnd.value) params.append('date_end', elDateEnd.value);
    if (isExport) params.append('export_csv', 'true');
    return params.toString();
  }

  function updateKpiValue(el, val, isCurrency, isPct) {
    if (val === undefined || val === null) {
      el.textContent = "-";
      return;
    }
    let formatted = val.toLocaleString(undefined, { minimumFractionDigits: isPct?1:2, maximumFractionDigits: isPct?1:2 });
    if (isCurrency) el.textContent = '$' + formatted;
    else if (isPct) el.textContent = formatted + '%';
    else el.textContent = val;
  }

  function updateBenchmarks(percentiles) {
    if (!percentiles || Object.keys(percentiles).length === 0) {
      elBillMin.textContent = "-"; elBill25.textContent = "-"; elBillMedian.textContent = "-"; elBill75.textContent = "-"; elBillMax.textContent = "-";
      elPayMin.textContent = "-"; elPay25.textContent = "-"; elPayMedian.textContent = "-"; elPay75.textContent = "-"; elPayMax.textContent = "-";
      if(elBillBar) { elBillBar.style.left = "0%"; elBillBar.style.right = "0%"; }
      if(elPayBar) { elPayBar.style.left = "0%"; elPayBar.style.right = "0%"; }
      return;
    }
    
    // Texts
    elBillMin.innerHTML = "<strong>Absolute Min</strong><br>$" + (percentiles.bill_min||0).toFixed(2);
    elBill25.innerHTML = "<strong>Lower/25th</strong><br>$" + (percentiles.bill_25||0).toFixed(2);
    elBillMedian.innerHTML = "<strong>Median (Market)</strong><br>$" + (percentiles.bill_median||0).toFixed(2);
    elBill75.innerHTML = "<strong>Upper/75th</strong><br>$" + (percentiles.bill_75||0).toFixed(2);
    elBillMax.innerHTML = "<strong>Absolute Max</strong><br>$" + (percentiles.bill_max||0).toFixed(2);

    elPayMin.innerHTML = "<strong>Absolute Min</strong><br>$" + (percentiles.pay_min||0).toFixed(2);
    elPay25.innerHTML = "<strong>Lower/25th</strong><br>$" + (percentiles.pay_25||0).toFixed(2);
    elPayMedian.innerHTML = "<strong>Median (Market)</strong><br>$" + (percentiles.pay_median||0).toFixed(2);
    elPay75.innerHTML = "<strong>Upper/75th</strong><br>$" + (percentiles.pay_75||0).toFixed(2);
    elPayMax.innerHTML = "<strong>Absolute Max</strong><br>$" + (percentiles.pay_max||0).toFixed(2);
    
    // Bars
    if(elBillBar && percentiles.bill_max > percentiles.bill_min) {
        let range = percentiles.bill_max - percentiles.bill_min;
        let leftPct = ((percentiles.bill_25 - percentiles.bill_min) / range) * 100;
        let rightPct = 100 - (((percentiles.bill_75 - percentiles.bill_min) / range) * 100);
        elBillBar.style.left = leftPct + "%";
        elBillBar.style.right = rightPct + "%";
    }
    
    if(elPayBar && percentiles.pay_max > percentiles.pay_min) {
        let range = percentiles.pay_max - percentiles.pay_min;
        let leftPct = ((percentiles.pay_25 - percentiles.pay_min) / range) * 100;
        let rightPct = 100 - (((percentiles.pay_75 - percentiles.pay_min) / range) * 100);
        elPayBar.style.left = leftPct + "%";
        elPayBar.style.right = rightPct + "%";
    }
  }

  function renderTable(comparables) {
    if (!comparables || comparables.length === 0) {
      elTableBody.innerHTML = '<tr><td colspan="8" style="text-align: center;">No comparable data found for these filters.</td></tr>';
      return;
    }

    const html = comparables.map(c => `
      <tr>
        <td><strong>${c.client_name}</strong><br><small style="color:var(--sri-text-muted)">${c.bundle} | Rep: ${c.sales_executive_name || 'N/A'}</small></td>
        <td>${c.industry_normalized}</td>
        <td>${c.county_name || 'N/A'}</td>
        <td>${c.position_name}</td>
        <td style="font-weight:600; color:var(--sri-success)">$${(c.bill_rate||0).toFixed(2)}</td>
        <td style="font-weight:600; color:var(--sri-accent)">$${(c.pay_rate||0).toFixed(2)}</td>
        <td>${(c.surcharge||0).toFixed(2)}%</td>
        <td style="font-size:0.85rem">
            ${c.start_date || 'N/A'} - ${c.end_date || 'N/A'}<br>
            <span style="color:var(--sri-text-muted)">Won: ${c.won_date || 'N/A'}</span>
        </td>
      </tr>
    `).join('');
    elTableBody.innerHTML = html;
  }

  async function loadData() {
    // Only require date bounds strictly
    if (!elDateStart.value || !elDateEnd.value) {
      alert("Please ensure a valid Date range is set.");
      return;
    }

    btnApply.textContent = "Loading...";
    btnApply.disabled = true;

    try {
      const resp = await fetch(`/sales-rate-intelligence/api/benchmark?${getQueryString()}`);
      if (!resp.ok) {
        throw new Error("HTTP " + resp.status);
      }
      const data = await resp.json();

      if (data.warning) {
        elWarning.classList.remove('sri-hidden');
        elWarningMsg.textContent = data.warning;
      } else {
        elWarning.classList.add('sri-hidden');
      }

      updateKpiValue(elKpiBill, data.kpis.avg_bill_rate, true, false);
      updateKpiValue(elKpiPay, data.kpis.avg_pay_rate, true, false);
      updateKpiValue(elKpiSpread, data.kpis.avg_spread, true, false);
      updateKpiValue(elKpiMarkup, data.kpis.avg_markup_pct, false, true);
      updateKpiValue(elKpiClients, data.kpis.sample_clients, false, false);
      updateKpiValue(elKpiRecords, data.kpis.sample_records, false, false);

      updateBenchmarks(data.percentiles);
      renderTable(data.comparables);

    } catch (err) {
      console.error(err);
      alert("Error loading benchmark data. Have you selected all filters properly?");
    } finally {
      btnApply.textContent = "Refresh Benchmark";
      btnApply.disabled = false;
    }
  }

  btnApply.addEventListener('click', loadData);
  btnExport.addEventListener('click', () => {
    if (!elDateStart.value || !elDateEnd.value) {
      alert("Please ensure a valid Date range is set.");
      return;
    }
    window.location.href = `/sales-rate-intelligence/api/benchmark?${getQueryString(true)}`;
  });

  // Init
  initDates();
  loadFilters();

});
