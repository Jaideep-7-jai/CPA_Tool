/**
 * CPA Tool – New Request Form Handler
 * Handles:
 *   - Doordash auto-fill (client, criteria, comp type, channel)
 *   - Multi-channel checkbox selection with single-click toggle
 *   - Request name uniqueness check (debounced)
 *   - Client name uniqueness check for the current day (debounced)
 *   - Criteria value field show/hide
 *   - File upload field show/hide
 *   - Form submission via /api/submit
 */

document.addEventListener('DOMContentLoaded', () => {
  const form            = document.getElementById('requestForm');
  const formMessage     = document.getElementById('formMessage');

  // Fields
  const requestNameEl   = document.getElementById('request_name');
  const nameStatusEl    = document.getElementById('nameStatus');
  const requestTypeEl   = document.getElementById('request_type');
  const clientNameEl    = document.getElementById('client_name');
  const clientNameStatusEl = document.getElementById('clientNameStatus');
  const criteriaTypeEl  = document.getElementById('criteria_type');
  const compTypeEl      = document.getElementById('comp_type');

  // Multi-channel checkboxes
  const chAll     = document.getElementById('chAll');
  const chGreen   = document.getElementById('chGreen');
  const chBlue    = document.getElementById('chBlue');
  const chOrange  = document.getElementById('chOrange');
  const chArcamax = document.getElementById('chArcamax');
  const chApptness = document.getElementById('chApptness');
  const channelWrapper = document.getElementById('channelWrapper');
  const channelGroup = document.getElementById('channelGroup');
  const individualChannels = [chGreen, chBlue, chOrange, chApptness, chArcamax];
  const apptnessLabel = chApptness.closest('label');

  // Conditional groups
  const criteriaValueGroup = document.getElementById('criteriaValueGroup');
  const criteriaValueEl    = document.getElementById('criteria_value');
  const criteriaValueLabel = document.getElementById('criteriaValueLabel');
  const criteriaValueHint  = document.getElementById('criteriaValueHint');
  const fileUploadGroup    = document.getElementById('fileUploadGroup');
  const compTypeGroup      = document.getElementById('compTypeGroup');

  // ── Multiple criteria builder (all selected criteria use OR logic) ────────
  const criteriaBuilder = document.getElementById('criteriaBuilder');
  const criteriaBuilderGroup = criteriaBuilder ? criteriaBuilder.closest('.form-group') : null;
  const criteriaJsonEl = document.getElementById('criteria_json');
  const addCriteriaBtn = document.getElementById('addCriteriaBtn');
  const mergeEnabledEl = document.getElementById('merge_enabled');
  const mergeRequestFields = document.getElementById('mergeRequestFields');
  const mergeSourceNameEl = document.getElementById('merge_source_request_name');
  const mergeSourceStatusEl = document.getElementById('mergeSourceStatus');
  const mergeSourceHintEl = document.getElementById('mergeSourceHint');
  const responderMatchEl = document.getElementById('responder_match');
  const responderDaysFields = document.getElementById('responderDaysFields');
  const responderDaysEl = document.getElementById('responder_days');
  const responderDaysValueEl = document.getElementById('responder_days_value');
  const criteriaRows = [];

  function syncResponderDaysValue() {
    if (!responderDaysEl || !responderDaysValueEl) return;
    responderDaysValueEl.textContent = `${responderDaysEl.value} day${responderDaysEl.value === '1' ? '' : 's'}`;
  }

  // ── Merge source eligibility check ───────────────────────────────────────
  // The submit API performs this same check authoritatively. This check makes
  // the result visible beside the Previous Request Name field.
  let mergeSourceCheckTimer = null;
  let mergeSourceIsValid = false;
  const mergeSourceDefaultHint =
    'Matching channel files are merged; the previous request remains unchanged.';

  function resetMergeSourceStatus() {
    mergeSourceIsValid = false;
    mergeSourceStatusEl.textContent = '';
    mergeSourceStatusEl.className = 'name-status';
    mergeSourceNameEl.classList.remove('error-input');
    mergeSourceHintEl.textContent = mergeSourceDefaultHint;
  }

  function checkMergeSource() {
    const sourceName = mergeSourceNameEl.value.trim();
    const currentType = requestTypeEl.value;
    if (!mergeEnabledEl.checked || !sourceName) {
      resetMergeSourceStatus();
      return;
    }

    mergeSourceIsValid = false;
    mergeSourceStatusEl.textContent = 'Checking…';
    mergeSourceStatusEl.className = 'name-status checking';
    mergeSourceNameEl.classList.remove('error-input');

    const params = new URLSearchParams({
      name: sourceName,
      request_type: currentType,
    });
    fetch(`/api/check-merge-source?${params.toString()}`)
      .then(response => response.json())
      .then(data => {
        mergeSourceHintEl.textContent = data.message || mergeSourceDefaultHint;
        if (data.available) {
          mergeSourceStatusEl.textContent = '✅ Eligible';
          mergeSourceStatusEl.className = 'name-status available';
          mergeSourceNameEl.classList.remove('error-input');
          mergeSourceIsValid = true;
        } else {
          mergeSourceStatusEl.textContent = '✖ Not eligible';
          mergeSourceStatusEl.className = 'name-status taken';
          mergeSourceNameEl.classList.add('error-input');
          mergeSourceIsValid = false;
        }
      })
      .catch(() => {
        mergeSourceStatusEl.textContent = '⚠ Check failed';
        mergeSourceStatusEl.className = 'name-status error';
        mergeSourceNameEl.classList.add('error-input');
        mergeSourceHintEl.textContent = 'Unable to validate the previous request. Try again.';
        mergeSourceIsValid = false;
      });
  }

  function scheduleMergeSourceCheck() {
    clearTimeout(mergeSourceCheckTimer);
    mergeSourceIsValid = false;
    mergeSourceCheckTimer = setTimeout(checkMergeSource, 400);
  }

  const supportedCriteria = ['age', 'state', 'zips', 'gender'];

  function criterionOptions(selected) {
    return supportedCriteria.map(type => {
      const label = type === 'age' ? 'Age'
        : type === 'state' ? 'State'
          : type === 'zips' ? 'ZIP' : 'Gender';
      return `<option value="${type}" ${type === selected ? 'selected' : ''}>${label}</option>`;
    }).join('');
  }

  function syncCriteriaJson() {
    const items = criteriaRows.map(row => {
      const type = row.querySelector('.criteria-kind').value;
      const comparison = row.querySelector('.criteria-comparison').value;
      const item = { type, comparison };
      if (type === 'age' && comparison === 'between') {
        item.from = row.querySelector('.age-from').value.trim();
        item.to = row.querySelector('.age-to').value.trim();
      } else if (type === 'age') {
        item.value = row.querySelector('.criteria-value').value.trim();
      } else if (type === 'state' || type === 'gender') {
        item.values = row.querySelector('.criteria-value').value
          .split(',').map(value => value.trim()).filter(Boolean);
      }
      return item;
    });
    criteriaJsonEl.value = JSON.stringify(items);
  }

  function renderCriterionValue(row) {
    const type = row.querySelector('.criteria-kind').value;
    const comparison = row.querySelector('.criteria-comparison');
    const valueWrap = row.querySelector('.criteria-row-value');
    if (type === 'age') {
      comparison.innerHTML = `
        <option value="greater">Greater Than</option>
        <option value="less">Lesser Than</option>
        <option value="between">Between</option>`;
      valueWrap.innerHTML = '<input class="criteria-value" type="number" min="0" placeholder="Age">';
    } else if (type === 'state' || type === 'gender') {
      comparison.innerHTML = '<option value="include">Include</option><option value="exclude">Exclude</option>';
      const placeholder = type === 'gender' ? 'M, F' : 'CA, TX, NY';
      valueWrap.innerHTML = `<input class="criteria-value" type="text" placeholder="${placeholder}">`;
    } else {
      comparison.innerHTML = '<option value="include">Include</option><option value="exclude">Exclude</option>';
      valueWrap.innerHTML = '<input type="file" name="zip_file" class="criteria-zip-file" accept=".csv,.txt"><span class="criteria-file-name">Upload ZIP file</span>';
    }
    syncCriteriaJson();
  }

  function addCriterion(type = null) {
    if (criteriaRows.length >= supportedCriteria.length) return;
    const selectedTypes = criteriaRows.map(row => row.querySelector('.criteria-kind').value);
    const criterionType = type || supportedCriteria.find(
      candidate => !selectedTypes.includes(candidate)
    );
    if (!criterionType || selectedTypes.includes(criterionType)) return;
    const row = document.createElement('div');
    row.className = 'criteria-row';
    row.innerHTML = `
      <select class="criteria-kind">${criterionOptions(criterionType)}</select>
      <select class="criteria-comparison"></select>
      <div class="criteria-row-value"></div>
      <button type="button" class="remove-criteria" title="Remove criterion"><i class="bi bi-trash"></i></button>`;
    criteriaBuilder.appendChild(row);
    criteriaRows.push(row);
    renderCriterionValue(row);
    row.querySelector('.criteria-kind').addEventListener('change', () => {
      renderCriterionValue(row);
      updateCriteriaControls();
    });
    row.querySelector('.criteria-comparison').addEventListener('change', () => {
      if (row.querySelector('.criteria-kind').value === 'age' && row.querySelector('.criteria-comparison').value === 'between') {
        row.querySelector('.criteria-row-value').innerHTML = '<input class="age-from" type="number" min="0" placeholder="From age"><input class="age-to" type="number" min="0" placeholder="To age">';
      } else {
        renderCriterionValue(row);
      }
      syncCriteriaJson();
    });
    row.addEventListener('input', syncCriteriaJson);
    row.addEventListener('change', event => {
      if (event.target.classList.contains('criteria-zip-file')) {
        const nameEl = row.querySelector('.criteria-file-name');
        nameEl.textContent = event.target.files[0] ? event.target.files[0].name : 'Upload ZIP file';
      }
      syncCriteriaJson();
    });
    row.querySelector('.remove-criteria').addEventListener('click', () => {
      if (criteriaRows.length === 1) return;
      criteriaRows.splice(criteriaRows.indexOf(row), 1);
      row.remove();
      updateCriteriaControls();
      syncCriteriaJson();
    });
    updateCriteriaControls();
  }

  function updateCriteriaControls() {
    if (!addCriteriaBtn) return;
    const selectedTypes = criteriaRows.map(row => row.querySelector('.criteria-kind').value);
    addCriteriaBtn.disabled = criteriaRows.length >= supportedCriteria.length;
    addCriteriaBtn.classList.toggle('hidden', criteriaRows.length >= supportedCriteria.length);
    criteriaRows.forEach(row => {
      const select = row.querySelector('.criteria-kind');
      Array.from(select.options).forEach(option => {
        option.disabled = option.value !== select.value && selectedTypes.includes(option.value);
      });
    });
  }

  function resetCriteriaBuilder() {
    criteriaRows.splice(0).forEach(row => row.remove());
    addCriterion('age');
  }

  if (criteriaBuilder) {
    addCriterion('age');
    addCriteriaBtn.addEventListener('click', () => addCriterion());
    mergeEnabledEl.addEventListener('change', () => {
      const enabled = mergeEnabledEl.checked;
      mergeRequestFields.classList.toggle('hidden', !enabled);
      if (!enabled) {
        mergeSourceNameEl.value = '';
        resetMergeSourceStatus();
      } else {
        scheduleMergeSourceCheck();
      }
    });
    mergeSourceNameEl.addEventListener('input', scheduleMergeSourceCheck);
    responderMatchEl.addEventListener('change', () => {
      const enabled = responderMatchEl.checked;
      responderDaysFields.classList.toggle('hidden', !enabled);
      if (enabled && !responderDaysEl.value) responderDaysEl.value = '30';
      syncResponderDaysValue();
    });
    responderDaysEl.addEventListener('input', syncResponderDaysValue);
    syncResponderDaysValue();
  }

  // ── Channel single-click toggle ──────────────────────────────────────────────
  document.querySelectorAll('.channel-option').forEach(lbl => {
    lbl.addEventListener('click', (e) => {
      e.preventDefault();
      const cb = lbl.querySelector('input[type=checkbox]');
      if (!cb || cb.disabled) return;

      const isAll = cb.value === 'ALL';

      if (isAll) {
        const newState = !cb.checked;
        cb.checked = newState;
        lbl.classList.toggle('checked', newState);
        if (newState) {
          individualChannels.forEach(ic => {
            ic.checked  = false;
            ic.disabled = true;
            ic.closest('label').classList.remove('checked');
          });
        } else {
          individualChannels.forEach(ic => { ic.disabled = false; });
        }
      } else {
        const newState = !cb.checked;
        cb.checked = newState;
        lbl.classList.toggle('checked', newState);
        if (newState) {
          chAll.checked = false;
          chAll.closest('label').classList.remove('checked');
        }
      }
      updateChannelChoices();
    });
  });

  function getSelectedChannels() {
    if (chAll.checked) return ['ALL'];
    return individualChannels.filter(cb => cb.checked).map(cb => cb.value);
  }

  function setChannelLock(locked, value = 'ALL') {
    [chAll, ...individualChannels].forEach(cb => {
      cb.checked  = false;
      cb.disabled = locked;
      cb.closest('label').classList.remove('checked');
    });
    if (locked && value === 'ALL') {
      chAll.checked = true;
      chAll.closest('label').classList.add('checked');
      individualChannels.forEach(cb => { cb.disabled = true; });
    }
    channelWrapper.classList.toggle('disabled', locked);
  }

  function updateChannelChoices() {
    const isDoordash = requestTypeEl.value === 'Doordash';

    // Apptness is an available channel only for Doordash requests.
    apptnessLabel.classList.toggle('hidden', !isDoordash);

    // Doordash always displays every channel name, but the complete group is
    // locked by applyDoordashDefaults with ALL selected.  Other request types
    // retain their editable channel selection and do not show Apptness.
    individualChannels.forEach(channel => {
      const label = channel.closest('label');
      if (channel !== chApptness) label.classList.remove('hidden');
      if (!isDoordash && channel === chApptness) {
        channel.checked = false;
        channel.disabled = false;
      }
    });
  }

  // ── Doordash auto-fill ────────────────────────────────────────────────────────
  function applyDoordashDefaults() {
    const isDoordash = requestTypeEl.value === 'Doordash';
    // Show channel choices as soon as the user selects a request type.
    channelGroup.classList.toggle('hidden', requestTypeEl.value === '');

    if (isDoordash) {
      clientNameEl.value  = 'Doordash';
      clientNameEl.setAttribute('readonly', true);
      // Doordash always runs ALL channels. Keep the selection and all labels
      // visible, while locking the group so the end user cannot change it.
      setChannelLock(true, 'ALL');
    } else {
      clientNameEl.removeAttribute('readonly');
      if (clientNameEl.value === 'Doordash') clientNameEl.value = '';
      setChannelLock(false);
    }
    if (criteriaBuilderGroup) {
      criteriaBuilderGroup.classList.toggle('hidden', isDoordash);
      if (isDoordash) {
        criteriaJsonEl.value = '';
      } else {
        syncCriteriaJson();
      }
    }
    scheduleClientNameCheck();
    updateChannelChoices();
    updateCriteriaFields();
    if (mergeEnabledEl.checked) scheduleMergeSourceCheck();
  }

  // ── Show/hide criteria value + file upload ──────────────────────────────────
  function updateCriteriaFields() {
    const criteria   = criteriaTypeEl.value;
    const isZips     = criteria === 'zips';
    const isDoordash = requestTypeEl.value === 'Doordash';
    const isAge      = criteria === 'age';

    if (isZips || isDoordash) {
      criteriaValueGroup.classList.add('hidden');
      criteriaValueEl.removeAttribute('required');
      criteriaValueEl.value = '';
    } else {
      criteriaValueGroup.classList.remove('hidden');
      criteriaValueEl.setAttribute('required', true);
      if (isAge) {
        criteriaValueLabel.textContent = 'Age Value';
        criteriaValueEl.placeholder    = 'e.g. 55';
        criteriaValueHint.textContent  = 'Enter a single age number.';
        criteriaValueEl.type           = 'number';
      } else {
        criteriaValueLabel.textContent = 'State Codes';
        criteriaValueEl.placeholder    = 'e.g. CA,TX,NY';
        criteriaValueHint.textContent  = 'Comma-separated state codes (e.g. CA, TX).';
        criteriaValueEl.type           = 'text';
      }
    }

    const doordashZipFile = document.getElementById('zip_file');
    if (fileUploadGroup && doordashZipFile) {
      fileUploadGroup.classList.toggle('hidden', !isDoordash);
      doordashZipFile.disabled = !isDoordash;
      doordashZipFile.required = isDoordash;
      if (!isDoordash) doordashZipFile.value = '';
    }

    updateCompTypeOptions(criteria);
  }

  function updateCompTypeOptions(criteria) {
    const isAge = criteria === 'age';
    compTypeEl.innerHTML = '';
    if (isAge) {
      compTypeEl.innerHTML = `
        <option value="greater">Greater Than or Equal To</option>
        <option value="less">Less Than</option>
      `;
    } else {
      compTypeEl.innerHTML = `
        <option value="include">Include</option>
        <option value="exclude">Exclude</option>
      `;
    }
    if (requestTypeEl.value === 'Doordash') {
      compTypeEl.value = 'include';
      compTypeEl.setAttribute('disabled', true);
    }
  }

  // ── Request name uniqueness check (debounced) ──────────────────────────────
  let nameCheckTimer = null;
  let nameIsValid    = false;

  function checkRequestName(name) {
    if (!name) {
      nameStatusEl.textContent = '';
      nameStatusEl.className   = 'name-status';
      nameIsValid = false;
      return;
    }
    nameStatusEl.textContent = 'Checking…';
    nameStatusEl.className   = 'name-status checking';

    fetch(`/api/check-name?name=${encodeURIComponent(name)}`)
      .then(r => r.json())
      .then(data => {
        if (data.available) {
          nameStatusEl.textContent = '✅ Available';
          nameStatusEl.className   = 'name-status available';
          nameIsValid = true;
        } else {
          nameStatusEl.textContent = '❌ Already taken';
          nameStatusEl.className   = 'name-status taken';
          nameIsValid = false;
        }
      })
      .catch(() => {
        nameStatusEl.textContent = '⚠ Check failed';
        nameStatusEl.className   = 'name-status error';
        nameIsValid = false;
      });
  }

  requestNameEl.addEventListener('input', () => {
    clearTimeout(nameCheckTimer);
    nameIsValid = false;
    nameCheckTimer = setTimeout(() => checkRequestName(requestNameEl.value.trim()), 500);
  });

  // ── Client name uniqueness check (current day, debounced) ─────────────────
  let clientNameCheckTimer = null;
  let clientNameIsValid = false;

  function checkClientName(clientName) {
    if (!clientName) {
      clientNameStatusEl.textContent = '';
      clientNameStatusEl.className = 'name-status';
      clientNameEl.classList.remove('error-input');
      clientNameIsValid = false;
      return;
    }

    clientNameStatusEl.textContent = 'Checking…';
    clientNameStatusEl.className = 'name-status checking';
    clientNameEl.classList.remove('error-input');

    fetch(`/api/check-client-name?client_name=${encodeURIComponent(clientName)}`)
      .then(r => r.json())
      .then(data => {
        if (data.available) {
          clientNameStatusEl.textContent = '✅ Available';
          clientNameStatusEl.className = 'name-status available';
          clientNameEl.classList.remove('error-input');
          clientNameIsValid = true;
        } else {
          clientNameStatusEl.textContent = '✖ Already used today';
          clientNameStatusEl.className = 'name-status taken';
          clientNameEl.classList.add('error-input');
          clientNameIsValid = false;
        }
      })
      .catch(() => {
        clientNameStatusEl.textContent = '⚠ Check failed';
        clientNameStatusEl.className = 'name-status error';
        clientNameEl.classList.add('error-input');
        clientNameIsValid = false;
      });
  }

  function scheduleClientNameCheck() {
    clearTimeout(clientNameCheckTimer);
    clientNameIsValid = false;
    clientNameCheckTimer = setTimeout(
      () => checkClientName(clientNameEl.value.trim()),
      500,
    );
  }

  clientNameEl.addEventListener('input', scheduleClientNameCheck);

  // ── Event listeners ────────────────────────────────────────────────────────────
  requestTypeEl.addEventListener('change', applyDoordashDefaults);
  criteriaTypeEl.addEventListener('change', updateCriteriaFields);

  // Drag & drop styling
  const dropArea = document.getElementById('fileDropArea');
  if (dropArea) {
    dropArea.addEventListener('dragover',  e => { e.preventDefault(); dropArea.classList.add('drag-over'); });
    dropArea.addEventListener('dragleave', () => dropArea.classList.remove('drag-over'));
    dropArea.addEventListener('drop', e => {
      e.preventDefault();
      dropArea.classList.remove('drag-over');
      const file = e.dataTransfer.files[0];
      if (file) {
        document.getElementById('zip_file').files = e.dataTransfer.files;
        document.getElementById('fileLabel').textContent = file.name;
      }
    });
    document.getElementById('zip_file').addEventListener('change', e => {
      const file = e.target.files[0];
      if (file) document.getElementById('fileLabel').textContent = file.name;
    });
  }

  // ── Form submit ────────────────────────────────────────────────────────────
  if (form) {
    form.addEventListener('submit', async (e) => {
      e.preventDefault();

      if (!nameIsValid) {
        formMessage.textContent = '⚠ Please wait for the name check or fix the request name.';
        formMessage.className   = 'message error';
        return;
      }

      if (!clientNameIsValid) {
        formMessage.textContent = '⚠ Please wait for the client name check or use a different client name.';
        formMessage.className   = 'message error';
        return;
      }

      if (mergeEnabledEl.checked && !mergeSourceIsValid) {
        formMessage.textContent = '⚠ Enter an eligible completed previous request before merging.';
        formMessage.className   = 'message error';
        return;
      }

      const selectedChannels = getSelectedChannels();
      if (selectedChannels.length === 0) {
        formMessage.textContent = '⚠ Please select at least one channel.';
        formMessage.className   = 'message error';
        return;
      }

      formMessage.textContent = 'Submitting…';
      formMessage.className   = 'message loading';

      // Re-enable channel checkboxes to allow FormData to collect them
      [chAll, ...individualChannels].forEach(cb => cb.removeAttribute('disabled'));

      const body = new FormData(form);

      // FIX: Remove all checkbox-submitted channel values, then re-append
      // each selected channel as a separate 'channel' key so Flask's
      // request.form.getlist('channel') receives ['GREEN', 'BLUE', ...]
      body.delete('channel');
      selectedChannels.forEach(ch => body.append('channel', ch));

      // Re-disable after collecting
      if (requestTypeEl.value === 'Doordash') setChannelLock(true, 'ALL');

      try {
        const res  = await fetch('/api/submit', { method: 'POST', body });
        const data = await res.json();

        if (!res.ok) {
          formMessage.textContent = data.error || 'Submission failed';
          formMessage.className   = 'message error';
          return;
        }

        form.reset();
        resetCriteriaBuilder();
        mergeRequestFields.classList.add('hidden');
        resetMergeSourceStatus();
        responderDaysFields.classList.add('hidden');
        responderDaysEl.value = '30';
        syncResponderDaysValue();
        // Reset channel visual state
        document.querySelectorAll('.channel-option').forEach(lbl => lbl.classList.remove('checked'));
        [chAll, ...individualChannels].forEach(cb => { cb.checked = false; cb.disabled = false; });
        applyDoordashDefaults();
        updateCriteriaFields();
        nameIsValid = false;
        nameStatusEl.textContent = '';
        clientNameIsValid = false;
        clientNameStatusEl.textContent = '';
        clientNameStatusEl.className = 'name-status';
        clientNameEl.classList.remove('error-input');

        formMessage.textContent = `✅ Request "${data.request_name}" submitted successfully!`;
        formMessage.className   = 'message success';

        setTimeout(() => location.reload(), 2000);
      } catch (err) {
        formMessage.textContent = `Error: ${err.message}`;
        formMessage.className   = 'message error';
      }
    });
  }

  // ── Init ───────────────────────────────────────────────────────────────────
  applyDoordashDefaults();
  updateCriteriaFields();
});
