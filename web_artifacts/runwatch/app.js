const $ = id => document.getElementById(id);
let state = null;
let refreshTimer = null;

const RUN_TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const RUN_LIVE = new Set(['starting', 'running', 'waiting_external', 'restarting', 'cancelling']);
const RESOURCE_ERRORS = new Set(['failed', 'monitor_error']);

const initialUrl = new URL(window.location.href);
if (initialUrl.searchParams.has('token')) {
  initialUrl.searchParams.delete('token');
  const query = initialUrl.searchParams.toString();
  window.history.replaceState({}, '', `${initialUrl.pathname}${query ? `?${query}` : ''}${initialUrl.hash}`);
}

const escapeHtml = value => String(value ?? '').replace(/[&<>'"]/g, character => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;',
}[character]));
const cssToken = value => String(value ?? 'unknown').toLowerCase().replace(/[^a-z0-9_-]/g, '-');
const finiteNumber = value => typeof value === 'number' && Number.isFinite(value);
const toDate = value => {
  if (value == null || value === '') return null;
  const date = finiteNumber(value) ? new Date(value < 1e12 ? value * 1000 : value) : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};
const fmtDuration = seconds => {
  if (!finiteNumber(seconds)) return '—';
  const total = Math.max(0, Math.floor(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const remainder = total % 60;
  if (hours) return `${hours}h ${minutes}m`;
  if (minutes) return `${minutes}m ${remainder}s`;
  return `${remainder}s`;
};
const fmtCompact = value => {
  if (!finiteNumber(value)) return '—';
  return Intl.NumberFormat(undefined, {
    notation: Math.abs(value) >= 10000 ? 'compact' : 'standard',
    maximumFractionDigits: Math.abs(value) >= 100 ? 0 : Math.abs(value) >= 10 ? 1 : 2,
  }).format(value);
};
const fmtValue = value => {
  if (value == null) return '—';
  if (typeof value === 'boolean') return value ? 'yes' : 'no';
  if (finiteNumber(value)) return fmtCompact(value);
  return String(value);
};
const fmtBytes = value => {
  if (!finiteNumber(value)) return fmtValue(value);
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  const magnitude = Math.abs(value);
  const unit = magnitude ? Math.min(Math.floor(Math.log(magnitude) / Math.log(1024)), units.length - 1) : 0;
  const scaled = value / (1024 ** unit);
  const digits = unit === 0 || Math.abs(scaled) >= 100 ? 0 : Math.abs(scaled) >= 10 ? 1 : 2;
  return `${scaled.toLocaleString(undefined, {maximumFractionDigits: digits})} ${units[unit]}`;
};
const fmtTimestamp = value => {
  const date = toDate(value);
  return date ? new Intl.DateTimeFormat(undefined, {dateStyle: 'medium', timeStyle: 'short'}).format(date) : fmtValue(value);
};
const relativeTime = value => {
  const date = toDate(value);
  if (!date) return '—';
  const seconds = Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000));
  if (seconds < 5) return 'just now';
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
};
const elapsed = run => {
  const start = toDate(run.started_at);
  if (!start) return null;
  const end = toDate(run.ended_at) || new Date();
  return Math.max(0, (end - start) / 1000);
};
const latestTimestamp = snapshot => {
  const candidates = [snapshot.run.updated_at, snapshot.run.ended_at, snapshot.run.started_at];
  snapshot.cells.forEach(cell => candidates.push(cell.updated_at, cell.ended_at, cell.started_at));
  snapshot.resources.forEach(resource => {
    candidates.push(resource.updated_at, resource.created_at);
    const observations = resource.observations || [];
    if (observations.length) candidates.push(observations[observations.length - 1].timestamp);
  });
  if (snapshot.events.length) candidates.push(snapshot.events[snapshot.events.length - 1].timestamp);
  return candidates.map(toDate).filter(Boolean).sort((left, right) => right - left)[0] || null;
};
const ageSeconds = value => {
  const date = toDate(value);
  return date ? Math.max(0, (Date.now() - date.getTime()) / 1000) : null;
};
const friendlyStatus = value => String(value || 'unknown').replaceAll('_', ' ');
const basename = value => {
  const text = String(value || '').replace(/\/+$/, '');
  return text.split(/[\\/]/).pop() || text;
};

function toast(message, error = false) {
  const element = $('toast');
  $('toast-message').textContent = message;
  element.classList.toggle('error', error);
  element.classList.add('show');
  setTimeout(() => element.classList.remove('show'), 3800);
}

async function api(path, options = {}) {
  const response = await fetch(path, {headers: {'Content-Type': 'application/json', ...(options.headers || {})}, ...options});
  if (!response.ok) {
    let detail = response.statusText;
    try { detail = (await response.json()).detail || detail; } catch {}
    throw new Error(detail);
  }
  return response.status === 204 ? null : response.json();
}

function setConnection(label) {
  $('connection').textContent = label;
  const chip = document.querySelector('.connection-chip');
  chip?.classList.toggle('reconnecting', label !== 'LIVE');
}

function scheduleRefresh(delay = 160) {
  if (refreshTimer) return;
  refreshTimer = setTimeout(async () => {
    refreshTimer = null;
    try {
      state = await api('/api/state');
      render();
      setConnection('LIVE');
    } catch {
      setConnection('RECONNECTING');
    }
  }, delay);
}

function latestProgress(events, current) {
  const matching = events.filter(event => {
    if (event.type !== 'notebook.progress') return false;
    if (!current) return true;
    const payload = event.payload || {};
    const sameCell = payload.cell_index === current.cell_index;
    const sameAttempt = payload.attempt == null || current.attempt == null || payload.attempt === current.attempt;
    return sameCell && sameAttempt;
  }).reverse();
  const latest = matching[0];
  if (latest?.payload?.metrics?.source !== 'tqdm') return latest;
  if ((latest.payload.metrics.position || 0) === 0) return latest;
  return matching.find(event =>
    event.payload?.metrics?.source === 'tqdm'
    && (event.payload.metrics.position || 0) === 0
  ) || latest;
}

function progressRate(events, current) {
  const payload = current?.payload || {};
  const metrics = payload.metrics || {};
  for (const key of ['rate', 'items_per_second', 'lines_per_second', 'rows_per_second', 'batches_per_second', 'throughput']) {
    if (finiteNumber(metrics[key]) && metrics[key] > 0) return metrics[key];
    if (finiteNumber(payload[key]) && payload[key] > 0) return payload[key];
  }
  const progressId = metrics.progress_id;
  const matches = events.filter(event => {
    if (event.type !== 'notebook.progress' || !finiteNumber(event.payload?.completed)) return false;
    const candidate = event.payload || {};
    if (progressId) return candidate.metrics?.progress_id === progressId;
    return candidate.cell_index === payload.cell_index
      && candidate.attempt === payload.attempt
      && candidate.unit === payload.unit;
  });
  const previous = [...matches].reverse().find(event => event !== current && event.payload.completed < payload.completed);
  const currentTime = toDate(current?.timestamp);
  const previousTime = toDate(previous?.timestamp);
  if (!previous || !currentTime || !previousTime) return null;
  const seconds = (currentTime - previousTime) / 1000;
  return seconds > 0 ? (payload.completed - previous.payload.completed) / seconds : null;
}

function renderProgress(run, cells, events) {
  const current = cells.find(cell => cell.cell_index === run.current_cell_index) || cells.find(cell => cell.status === 'running');
  const executable = cells.filter(cell => cell.cell_type === 'code');
  const done = executable.filter(cell => ['succeeded', 'skipped', 'not_replayed'].includes(cell.status)).length;
  const notebookPercent = executable.length ? 100 * done / executable.length : 0;

  if (current) {
    $('current-label').textContent = current.label || `Cell ${current.cell_index + 1}`;
    const codePosition = executable.findIndex(cell => cell.cell_index === current.cell_index);
    const position = codePosition >= 0 ? `Code cell ${codePosition + 1} of ${executable.length}` : `Cell ${current.cell_index + 1}`;
    $('current-meta').textContent = `${position}${current.attempt > 1 ? ` · retry ${current.attempt}` : ''}`;
  } else {
    const labels = {
      succeeded: 'Run completed',
      failed: 'Run failed',
      cancelled: 'Run cancelled',
      waiting_external: 'Notebook complete · waiting on remote work',
      paused: 'Paused for repair',
      starting: 'Starting notebook',
    };
    $('current-label').textContent = labels[run.status] || 'No active cell';
    $('current-meta').textContent = run.message || (RUN_TERMINAL.has(run.status) ? 'Final state recorded.' : 'Waiting for the next execution step.');
  }

  const progressEvent = latestProgress(events, current);
  const payload = progressEvent?.payload;
  let primaryPercent = notebookPercent;
  let ariaValue = Math.round(notebookPercent);
  if (payload) {
    const unit = payload.unit ? ` ${payload.unit}` : '';
    const hasTotal = finiteNumber(payload.total) && payload.total > 0;
    const percent = hasTotal && finiteNumber(payload.completed) ? Math.max(0, Math.min(100, 100 * payload.completed / payload.total)) : null;
    $('reported-progress').textContent = hasTotal
      ? `${fmtValue(payload.completed)} / ${fmtValue(payload.total)}${unit}`
      : `${fmtValue(payload.completed)}${unit}`;
    $('reported-percent').textContent = percent == null ? '' : `${Math.round(percent)}%`;
    $('reported-message').textContent = payload.message || 'Progress reported by the notebook';
    if (percent != null) {
      primaryPercent = percent;
      ariaValue = Math.round(percent);
    }
    const closed = payload.metrics?.closed === true;
    const rate = closed || (hasTotal && payload.completed >= payload.total) ? null : progressRate(events, progressEvent);
    const rateCopy = rate && payload.unit ? `${fmtCompact(rate)} ${payload.unit}/s` : rate ? `${fmtCompact(rate)}/s` : '';
    const remaining = hasTotal && rate ? Math.max(0, payload.total - payload.completed) / rate : null;
    $('progress-rate').textContent = [rateCopy, remaining != null ? `ETA ${fmtDuration(remaining)}` : ''].filter(Boolean).join(' · ');
  } else {
    $('reported-progress').textContent = executable.length ? `${done} / ${executable.length} code cells` : 'Preparing execution';
    $('reported-percent').textContent = executable.length ? `${Math.round(notebookPercent)}%` : '';
    $('reported-message').textContent = current ? 'Notebook execution progress' : 'Waiting for the notebook to start.';
    $('progress-rate').textContent = '';
  }

  if (RUN_TERMINAL.has(run.status)) $('progress-rate').textContent = '';

  const primaryTrack = $('reported-progress-bar');
  primaryTrack.value = primaryPercent;
  primaryTrack.setAttribute('aria-valuenow', String(ariaValue));
  $('progress-copy').textContent = `${done} / ${executable.length} code cells`;
  $('notebook-progress').value = notebookPercent;
}

function resourceName(resource) {
  const metrics = resource.metrics || {};
  if (resource.resource_type === 'sagemaker_processing_job') return resource.external_id;
  if (resource.resource_type === 'system_metrics') return 'System utilization';
  if (resource.resource_type === 'file_count') return basename(resource.external_id) || 'Output files';
  if (resource.resource_type === 'line_count') return basename(resource.external_id) || 'Output lines';
  if (resource.resource_type === 's3_prefix') return metrics.prefix ? `s3://${metrics.bucket}/${metrics.prefix}` : 'S3 output';
  if (resource.resource_type === 's3_manifest') return basename(metrics.key || resource.external_id) || 'S3 manifest';
  if (resource.resource_type === 'cloudwatch_metric') return metrics.metric_name || 'CloudWatch metric';
  if (resource.resource_type === 'cloudwatch_logs') return 'CloudWatch logs';
  return resource.external_id || friendlyStatus(resource.resource_type);
}

function resourceKind(resource) {
  const labels = {
    sagemaker_processing_job: 'SAGEMAKER PROCESSING',
    system_metrics: 'LOCAL SYSTEM',
    file_count: 'OUTPUT FILES',
    line_count: 'OUTPUT LINES',
    s3_prefix: 'S3 OUTPUT',
    s3_manifest: 'JOB MANIFEST',
    cloudwatch_metric: 'CLOUDWATCH METRIC',
    cloudwatch_logs: 'CLOUDWATCH LOGS',
  };
  return labels[resource.resource_type] || `${resource.provider} · ${friendlyStatus(resource.resource_type)}`.toUpperCase();
}

function isRemoteJob(resource) {
  return resource.resource_type === 'sagemaker_processing_job' || resource.resource_type.endsWith('_job');
}

function isMonitoredActive(resource, run) {
  if (RUN_TERMINAL.has(run.status) || resource.terminal || resource.monitor_closed || resource.disposition !== 'active') return false;
  return ['registered', 'pending', 'starting', 'running', 'stopping'].includes(resource.status);
}

function displayedResourceStatus(resource, run) {
  if (RESOURCE_ERRORS.has(resource.status)) return {label: friendlyStatus(resource.status), tone: resource.status};
  if (resource.monitor_closed && !resource.terminal) return {label: 'monitor ended', tone: 'observed'};
  if (RUN_TERMINAL.has(run.status) && !resource.terminal) return {label: `last seen ${friendlyStatus(resource.status)}`, tone: 'observed'};
  if (resource.resource_type === 'system_metrics' && resource.status === 'running') return {label: 'monitoring', tone: 'running'};
  return {label: friendlyStatus(resource.status), tone: resource.status};
}

function collectIssues(snapshot) {
  const issues = [];
  const {run, cells, resources} = snapshot;
  if (run.status === 'failed') issues.push({title: 'Run failed', detail: run.message || 'Execution ended unsuccessfully.', severity: 'error'});
  if (run.status === 'paused') issues.push({title: 'Run paused for repair', detail: run.message || 'A notebook cell needs attention before the run can continue.', severity: 'warning'});
  cells.filter(cell => cell.status === 'failed').forEach(cell => issues.push({
    title: cell.label || `Cell ${cell.cell_index + 1} failed`,
    detail: [cell.error_name, cell.error_value].filter(Boolean).join(': ') || 'Inspect the cell output for details.',
    severity: 'error',
  }));
  resources.forEach(resource => {
    if (RESOURCE_ERRORS.has(resource.status)) issues.push({
      title: `${resourceName(resource)} needs attention`,
      detail: resource.message || `Resource status: ${friendlyStatus(resource.status)}.`,
      severity: 'error',
    });
    if (resource.link?.status === 'failed') issues.push({
      title: `${resource.link.label || 'Linked dashboard'} is unavailable`,
      detail: resource.link.message || 'Runwatch could not prepare the dashboard link.',
      severity: 'warning',
    });
  });
  const latest = latestTimestamp(snapshot);
  const age = ageSeconds(latest);
  if (RUN_LIVE.has(run.status) && age != null && age > 120) issues.push({
    title: 'No recent updates',
    detail: `The last state change was ${relativeTime(latest)}. The run may be stalled or temporarily disconnected.`,
    severity: 'warning',
  });
  return issues;
}

function renderAttention(issues) {
  const section = $('attention-section');
  section.hidden = issues.length === 0;
  $('issue-count').textContent = String(issues.length);
  $('issues-signal').classList.toggle('has-issues', issues.length > 0);
  $('attention-list').innerHTML = issues.map(issue => `
    <article class="attention-item attention-${escapeHtml(issue.severity)}">
      <span class="attention-icon" aria-hidden="true">${issue.severity === 'error' ? '!' : '•'}</span>
      <span><strong>${escapeHtml(issue.title)}</strong><small>${escapeHtml(issue.detail)}</small></span>
    </article>`).join('');
}

function updateFreshness() {
  if (!state) return;
  const latest = latestTimestamp(state);
  const age = ageSeconds(latest);
  $('last-update').textContent = relativeTime(latest);
  const heartbeat = $('heartbeat');
  heartbeat.classList.remove('stale', 'settled');
  if (RUN_TERMINAL.has(state.run.status)) {
    heartbeat.classList.add('settled');
    $('heartbeat-copy').textContent = `Final state · ${relativeTime(latest)}`;
  } else if (age == null) {
    $('heartbeat-copy').textContent = 'Waiting for state…';
  } else if (age > 120) {
    heartbeat.classList.add('stale');
    $('heartbeat-copy').textContent = `No update for ${fmtDuration(age)}`;
  } else {
    $('heartbeat-copy').textContent = `Updated ${relativeTime(latest)}`;
  }
}

function metricCell(value, label, options = {}) {
  if (value == null || value === '') return '';
  return `<div class="resource-metric${options.wide ? ' metric-wide' : ''}"><strong title="${escapeHtml(value)}">${escapeHtml(value)}</strong><span>${escapeHtml(label)}</span></div>`;
}

function progressBlock(completed, total, unit, rate = null) {
  const hasCompleted = finiteNumber(completed);
  const hasTotal = finiteNumber(total) && total > 0;
  if (!hasCompleted) return '';
  const percent = hasTotal ? Math.max(0, Math.min(100, 100 * completed / total)) : null;
  const value = hasTotal ? `${fmtValue(completed)} / ${fmtValue(total)} ${unit}` : `${fmtValue(completed)} ${unit}`;
  const eta = hasTotal && rate > 0 ? (total - completed) / rate : null;
  return `<div class="resource-progress">
    <div><strong>${escapeHtml(value.trim())}</strong>${percent == null ? '' : `<span>${Math.round(percent)}%</span>`}</div>
    ${percent == null ? '' : `<progress class="resource-progress-track" aria-label="${escapeHtml(unit)} progress" max="100" value="${percent}"></progress>`}
    ${rate > 0 ? `<small>${escapeHtml(`${fmtCompact(rate)} ${unit}/s${eta != null ? ` · ETA ${fmtDuration(Math.max(0, eta))}` : ''}`)}</small>` : ''}
  </div>`;
}

function observationRate(resource, metric) {
  const points = (resource.observations || []).map(observation => ({
    time: toDate(observation.timestamp),
    value: observation.metrics?.[metric],
  })).filter(point => point.time && finiteNumber(point.value));
  if (points.length < 2) return null;
  const latest = points[points.length - 1];
  const previous = [...points.slice(0, -1)].reverse().find(point => point.value < latest.value);
  if (!previous) return null;
  const seconds = (latest.time - previous.time) / 1000;
  return seconds > 0 ? (latest.value - previous.value) / seconds : null;
}

function technicalDetails(resource) {
  const origin = resource.cell_index == null ? 'Configured resource' : `Cell ${resource.cell_index + 1}${resource.attempt > 1 ? `, attempt ${resource.attempt}` : ''}`;
  const arn = resource.resource_type === 'sagemaker_processing_job' ? resource.metrics?.processing_job_arn : null;
  const started = resource.resource_type === 'sagemaker_processing_job' ? resource.metrics?.start_time : null;
  const ended = resource.resource_type === 'sagemaker_processing_job' ? resource.metrics?.end_time : null;
  return `<details class="technical-details">
    <summary>Technical details</summary>
    <dl>
      <dt>Identifier</dt><dd>${escapeHtml(resource.external_id)}</dd>
      <dt>Provider</dt><dd>${escapeHtml(`${resource.provider}.${resource.resource_type}`)}</dd>
      <dt>Origin</dt><dd>${escapeHtml(origin)}</dd>
      ${resource.region ? `<dt>Region</dt><dd>${escapeHtml(resource.region)}</dd>` : ''}
      ${arn ? `<dt>ARN</dt><dd>${escapeHtml(arn)}</dd>` : ''}
      ${started ? `<dt>Started</dt><dd>${escapeHtml(fmtTimestamp(started))}</dd>` : ''}
      ${ended ? `<dt>Ended</dt><dd>${escapeHtml(fmtTimestamp(ended))}</dd>` : ''}
    </dl>
  </details>`;
}

function logsDetails(resource) {
  const logs = (resource.log_tail || []).slice(-35).join('\n');
  return logs ? `<details class="log-details"><summary>Recent logs</summary><pre class="log-tail">${escapeHtml(logs)}</pre></details>` : '';
}

function resourceActions(resource, capabilities) {
  const canStop = capabilities.controller_live
    && !resource.terminal
    && !resource.monitor_closed
    && resource.status !== 'stopping'
    && resource.disposition === 'active'
    && resource.ownership === 'exclusive'
    && resource.supports_stop;
  return canStop ? `<div class="resource-actions"><button class="soft-button danger-soft stop-resource" data-id="${escapeHtml(resource.internal_id)}"><span>Stop resource</span></button></div>` : '';
}

function resourceMessage(resource) {
  if (!resource.message || ['Completion condition satisfied', 'Expected line count reached'].includes(resource.message)) return '';
  const tone = RESOURCE_ERRORS.has(resource.status) ? ' error' : '';
  return `<p class="resource-message${tone}">${escapeHtml(resource.message)}</p>`;
}

function resourceShell(resource, run, capabilities, body, extraClass = '') {
  const display = displayedResourceStatus(resource, run);
  return `<article class="resource-card panel surface ${escapeHtml(extraClass)}">
    <div class="resource-header">
      <div><div class="resource-eyebrow">${escapeHtml(resourceKind(resource))}</div><h3>${escapeHtml(resourceName(resource))}</h3></div>
      <span class="status status-${cssToken(display.tone)}">${escapeHtml(display.label)}</span>
    </div>
    ${resourceMessage(resource)}
    ${body}
    ${logsDetails(resource)}
    ${resourceActions(resource, capabilities)}
    ${technicalDetails(resource)}
  </article>`;
}

function renderDashboardResource(resource) {
  const link = resource.link;
  const label = link?.label || resource.metadata?.name || 'dashboard';
  if (link?.status === 'ready' && link.href) {
    return `<div class="dashboard-launch"><a class="soft-button dashboard-button" href="${escapeHtml(link.href)}" target="_blank" rel="noopener"><span>Open ${escapeHtml(label)}</span></a></div>`;
  }
  const copy = link?.status === 'failed' ? `${label} unavailable` : `Preparing ${label}…`;
  return `<div class="dashboard-launch"><button class="soft-button dashboard-button disabled" disabled><span>${escapeHtml(copy)}</span></button></div>`;
}

function sagemakerBody(resource) {
  const metrics = resource.metrics || {};
  const metadata = resource.metadata || {};
  const count = metrics.instance_count ?? metrics.processing_instance_count ?? metrics.cluster_instance_count ?? metadata.instance_count;
  const type = metrics.instance_type ?? metrics.processing_instance_type ?? metrics.cluster_instance_type ?? metadata.instance_type;
  const volume = metrics.volume_size_gb ?? metrics.volume_size_in_gb ?? metrics.processing_volume_size_gb ?? metadata.volume_size_gb;
  const start = toDate(metrics.start_time || metrics.creation_time || resource.created_at);
  const end = toDate(metrics.end_time) || (resource.terminal ? toDate(resource.updated_at) : new Date());
  const duration = start && end ? (end - start) / 1000 : null;
  const capacity = count != null && type ? `${fmtValue(count)} × ${type}` : type || (count != null ? `${fmtValue(count)} instance${count === 1 ? '' : 's'}` : 'Capacity details pending');
  const supporting = [
    metricCell(duration == null ? null : fmtDuration(duration), 'duration'),
    metricCell(resource.region || 'default', 'region'),
    metricCell(volume == null ? null : `${fmtValue(volume)} GiB`, 'volume per instance'),
  ].join('');
  return `<div class="resource-primary"><strong>${escapeHtml(capacity)}</strong><span>processing capacity</span></div>${supporting ? `<div class="resource-metrics compact-metrics">${supporting}</div>` : ''}`;
}

function fileCountBody(resource) {
  const metrics = resource.metrics || {};
  const count = metrics.file_count;
  const total = metrics.expected_count;
  const rate = resource.terminal ? null : observationRate(resource, 'file_count');
  return `${progressBlock(count, total, 'files', rate)}<div class="resource-metrics compact-metrics">
    ${metricCell(metrics.total_bytes == null ? null : fmtBytes(metrics.total_bytes), 'total size')}
    ${metricCell(relativeTime(metrics.latest_modified_at || resource.updated_at), 'last output')}
  </div>`;
}

function lineCountBody(resource) {
  const metrics = resource.metrics || {};
  const rate = resource.terminal ? null : finiteNumber(metrics.lines_per_second) && metrics.lines_per_second > 0 ? metrics.lines_per_second : observationRate(resource, 'line_count');
  return `${progressBlock(metrics.line_count, metrics.expected_lines, 'lines', rate)}<div class="resource-metrics compact-metrics">
    ${metricCell(metrics.bytes == null ? null : fmtBytes(metrics.bytes), 'file size')}
    ${metricCell(relativeTime(metrics.modified_at || resource.updated_at), 'last write')}
  </div>`;
}

function s3PrefixBody(resource) {
  const metrics = resource.metrics || {};
  const rate = resource.terminal ? null : observationRate(resource, 'object_count');
  return `${progressBlock(metrics.object_count, metrics.expected_count, 'objects', rate)}<div class="resource-metrics compact-metrics">
    ${metricCell(metrics.total_bytes == null ? null : fmtBytes(metrics.total_bytes), 'total size')}
    ${metricCell(metrics.scan_in_progress ? 'catching up' : 'current', 'inventory')}
    ${metricCell(relativeTime(metrics.latest_object_time || resource.updated_at), 'latest object')}
  </div>`;
}

function manifestBody(resource) {
  const metrics = resource.metrics || {};
  return `${progressBlock(metrics.completed, metrics.total, 'items')}<div class="resource-metrics compact-metrics">
    ${metricCell(metrics.exists === false ? 'waiting' : friendlyStatus(resource.status), 'manifest state')}
    ${metricCell(relativeTime(resource.updated_at), 'last checked')}
  </div>`;
}

function systemBody(resource) {
  const metrics = resource.metrics || {};
  const cells = [
    metricCell(finiteNumber(metrics.host_cpu_percent) ? `${fmtValue(metrics.host_cpu_percent)}%` : null, 'host CPU'),
    metricCell(finiteNumber(metrics.host_memory_percent) ? `${fmtValue(metrics.host_memory_percent)}%` : null, 'host memory'),
    metricCell(finiteNumber(metrics.kernel_cpu_percent) ? `${fmtValue(metrics.kernel_cpu_percent)}%` : null, 'kernel CPU'),
    metricCell(metrics.kernel_memory_rss_bytes == null ? null : fmtBytes(metrics.kernel_memory_rss_bytes), 'kernel memory'),
    metricCell(finiteNumber(metrics.disk_percent) ? `${fmtValue(metrics.disk_percent)}%` : null, 'disk used'),
    metricCell(finiteNumber(metrics.gpu_0_utilization_percent) ? `${fmtValue(metrics.gpu_0_utilization_percent)}%` : null, 'GPU 0'),
  ].join('');
  return cells ? `<div class="resource-metrics system-metrics">${cells}</div>` : '<div class="empty small">System readings are not available yet.</div>';
}

function miniChart(series, label) {
  const values = (series || []).map(point => point?.value).filter(finiteNumber);
  if (values.length < 2) return '';
  const width = 320;
  const height = 54;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const points = values.map((value, index) => `${(index / (values.length - 1)) * width},${height - ((value - min) / span) * (height - 8) - 4}`).join(' ');
  return `<div class="spark"><span>${escapeHtml(label)}</span><svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true"><polyline points="${points}" /></svg></div>`;
}

function cloudWatchMetricBody(resource) {
  const metrics = resource.metrics || {};
  const unit = metrics.latest_unit ? ` ${metrics.latest_unit}` : '';
  const value = metrics.latest_value == null ? 'No datapoints yet' : `${fmtValue(metrics.latest_value)}${unit}`;
  return `<div class="resource-primary"><strong>${escapeHtml(value)}</strong><span>${escapeHtml(metrics.statistic || 'latest value')}</span></div>
    ${miniChart(metrics.series, metrics.metric_name || 'recent observations')}
    <div class="resource-metrics compact-metrics">${metricCell(relativeTime(metrics.latest_timestamp), 'latest datapoint')}</div>`;
}

function cloudWatchLogsBody(resource) {
  const metrics = resource.metrics || {};
  return `<div class="resource-primary"><strong>${escapeHtml(fmtValue(metrics.stream_count || 0))}</strong><span>log streams</span></div>
    <div class="resource-metrics compact-metrics">${metricCell(relativeTime(resource.updated_at), 'last checked')}</div>`;
}

function genericBody(resource) {
  const metrics = resource.metrics || {};
  if (finiteNumber(metrics.completed)) return progressBlock(metrics.completed, metrics.total, metrics.unit || 'items');
  const preferred = Object.entries(metrics).filter(([key, value]) =>
    value != null
    && ['string', 'number'].includes(typeof value)
    && /(count|total|progress|rate|throughput|percent|size|value)$/i.test(key)
    && !/(path|arn|id|timestamp|time)$/i.test(key)
  ).slice(0, 4);
  if (!preferred.length) return '<div class="resource-primary quiet"><strong>Monitoring</strong><span>structured resource</span></div>';
  return `<div class="resource-metrics compact-metrics">${preferred.map(([key, value]) => metricCell(fmtValue(value), key.replaceAll('_', ' '))).join('')}</div>`;
}

function renderResource(resource, run, capabilities) {
  if (resource.resource_type === 'dashboard') return renderDashboardResource(resource);
  const presenters = {
    sagemaker_processing_job: sagemakerBody,
    file_count: fileCountBody,
    line_count: lineCountBody,
    system_metrics: systemBody,
    s3_prefix: s3PrefixBody,
    s3_manifest: manifestBody,
    cloudwatch_metric: cloudWatchMetricBody,
    cloudwatch_logs: cloudWatchLogsBody,
  };
  const presenter = presenters[resource.resource_type] || genericBody;
  const extra = isRemoteJob(resource) ? 'remote-resource' : resource.resource_type === 'system_metrics' ? 'system-resource' : '';
  return resourceShell(resource, run, capabilities, presenter(resource), extra);
}

function resourcePriority(resource) {
  if (RESOURCE_ERRORS.has(resource.status) || resource.link?.status === 'failed') return 0;
  if (isRemoteJob(resource) && !resource.terminal) return 1;
  if (isRemoteJob(resource)) return 2;
  if (['file_count', 'line_count', 's3_prefix', 's3_manifest'].includes(resource.resource_type)) return 3;
  if (resource.resource_type === 'dashboard') return 4;
  if (resource.provider !== 'local') return 5;
  if (resource.resource_type === 'system_metrics') return 7;
  return 6;
}

function renderResources(resources, run, capabilities) {
  $('resource-count').textContent = `${resources.length} tracked`;
  const root = $('resources');
  if (!resources.length) {
    root.innerHTML = '<div class="panel surface empty">No remote jobs or output resources have been reported yet.</div>';
    return;
  }
  const ordered = [...resources].sort((left, right) => resourcePriority(left) - resourcePriority(right));
  root.innerHTML = ordered.map(resource => renderResource(resource, run, capabilities)).join('');
  root.querySelectorAll('.stop-resource').forEach(button => button.addEventListener('click', () => confirmStop(button.dataset.id)));
}

async function confirmStop(id) {
  const resource = state.resources.find(item => item.internal_id === id);
  if (!resource) return;
  const affected = state.resources.filter(item => item.internal_id !== id
    && !item.terminal
    && !item.monitor_closed
    && item.status !== 'stopping'
    && item.disposition === 'active'
    && item.lifecycle?.stop_on_cancel
    && item.ownership === 'exclusive'
    && item.supports_stop);
  $('stop-details').innerHTML = `<dt>Job</dt><dd>${escapeHtml(resource.external_id)}</dd><dt>Region</dt><dd>${escapeHtml(resource.region || 'default')}</dd><dt>Origin</dt><dd>${resource.cell_index == null ? 'configured' : `cell ${resource.cell_index + 1}, attempt ${resource.attempt}`}</dd><dt>Ownership</dt><dd>${escapeHtml(resource.ownership)}</dd>`;
  $('cascade-list').innerHTML = `<strong>${affected.length} other resource(s) eligible for provider stop</strong>${affected.map(item => `<div>${escapeHtml(item.external_id)}</div>`).join('')}`;
  const dialog = $('stop-dialog');
  dialog.showModal();
  dialog.addEventListener('close', async function handler() {
    dialog.removeEventListener('close', handler);
    if (dialog.returnValue !== 'confirm') return;
    try {
      const result = await api(`/api/resources/${id}/stop`, {method: 'POST', body: JSON.stringify({confirmation: 'STOP RESOURCE AND CANCEL RUN', expected_version: resource.version})});
      toast(`Cancellation action ${result.action_id.slice(0, 8)} queued`);
      scheduleRefresh(20);
    } catch (error) {
      toast(error.message, true);
    }
  });
}

function outputText(output) {
  if (output.output_type === 'error') return (output.traceback || []).join('\n');
  if (Array.isArray(output.text)) return output.text.join('');
  return output.text || JSON.stringify(output);
}

function cellRows(cells) {
  if (!cells.length) return '<div class="empty small">No relevant cells yet.</div>';
  return cells.map(cell => {
    const detail = [...(cell.output_tail || []).slice(-8).map(outputText), ...(cell.traceback || [])].filter(Boolean).join('\n');
    const stateCopy = friendlyStatus(cell.status);
    const attemptCopy = cell.attempt > 1 ? ` · retry ${cell.attempt}` : '';
    return `<details class="cell-row cell-${cssToken(cell.status)}">
      <summary><span class="cell-dot"></span><span><strong>${escapeHtml(cell.label || `Cell ${cell.cell_index + 1}`)}</strong><small>${escapeHtml(`${cell.cell_type} · ${stateCopy}${attemptCopy}${cell.error_name ? ` · ${cell.error_name}` : ''}`)}</small></span><span class="mono muted">${fmtDuration(cell.elapsed_seconds)}</span></summary>
      ${detail ? `<pre class="cell-output">${escapeHtml(detail)}</pre>` : '<div class="empty small">No captured output.</div>'}
    </details>`;
  }).join('');
}

function highlightedCells(cells, run) {
  const selected = new Map();
  const add = cell => { if (cell) selected.set(cell.cell_index, cell); };
  cells.filter(cell => cell.status === 'failed').forEach(add);
  add(cells.find(cell => cell.cell_index === run.current_cell_index));
  add(cells.find(cell => cell.status === 'running'));
  cells.filter(cell => cell.cell_type === 'code' && ['succeeded', 'skipped', 'not_replayed'].includes(cell.status)).slice(-2).forEach(add);
  const currentIndex = run.current_cell_index ?? -1;
  add(cells.find(cell => cell.cell_type === 'code' && cell.status === 'pending' && cell.cell_index > currentIndex));
  if (!selected.size) cells.filter(cell => cell.cell_type === 'code').slice(-3).forEach(add);
  const deduplicated = new Map();
  [...selected.values()].sort((left, right) => left.cell_index - right.cell_index).forEach(cell => {
    const label = cell.label || `Cell ${cell.cell_index + 1}`;
    const key = ['failed', 'running'].includes(cell.status) ? `cell:${cell.cell_index}` : `${label}:${cell.status}`;
    deduplicated.set(key, cell);
  });
  return [...deduplicated.values()].sort((left, right) => left.cell_index - right.cell_index);
}

function renderCells(cells, run) {
  const executable = cells.filter(cell => cell.cell_type === 'code');
  $('cell-count').textContent = `${executable.length} code cells`;
  $('timeline-count').textContent = `${cells.length} total`;
  $('cell-highlights').innerHTML = cellRows(highlightedCells(cells, run));
  $('cells').innerHTML = cellRows(cells);
}

function eventResourceName(event) {
  const internalId = event.payload?.internal_id;
  const resource = state?.resources.find(item => item.internal_id === internalId);
  return resource ? resourceName(resource) : event.payload?.resource?.id || 'Resource';
}

function humanEvent(event) {
  const payload = event.payload || {};
  const runLabels = {
    'run.started': ['Run started', 'Notebook kernel is being prepared.', 'info'],
    'run.waiting_external': ['Notebook finished', 'Waiting for remote resources to complete.', 'warning'],
    'run.succeeded': ['Run completed', 'Notebook and blocking resources succeeded.', 'success'],
    'run.failed_external': ['Remote work failed', 'A blocking external resource did not succeed.', 'error'],
    'run.external_timeout': ['Remote work timed out', 'A blocking resource exceeded its completion timeout.', 'error'],
    'run.runner_error': ['Runner failed', payload.error || 'The notebook runner stopped unexpectedly.', 'error'],
    'run.cancel_requested': ['Cancellation requested', 'Stopping the run and eligible owned resources.', 'warning'],
    'run.cancelled': ['Run cancelled', 'Execution was stopped.', 'warning'],
  };
  if (runLabels[event.type]) return runLabels[event.type];
  if (event.type === 'notebook.progress') {
    const unit = payload.unit ? ` ${payload.unit}` : '';
    const value = payload.total != null ? `${fmtValue(payload.completed)} / ${fmtValue(payload.total)}${unit}` : `${fmtValue(payload.completed)}${unit}`;
    return ['Progress updated', `${value}${payload.message ? ` · ${payload.message}` : ''}`, 'info'];
  }
  if (event.type === 'resource.registered') return [`${eventResourceName(event)} detected`, 'Runwatch started monitoring this resource.', 'info'];
  if (event.type === 'resource.link_ready') return ['Dashboard ready', 'The captured local dashboard can now be opened.', 'success'];
  if (event.type === 'resource.link_failed') return ['Dashboard unavailable', payload.message || 'The captured dashboard link could not be prepared.', 'error'];
  if (event.type === 'resource.monitor_error' || event.type === 'resource.monitor_failed') return [`${eventResourceName(event)} monitoring failed`, payload.error || 'Runwatch could not inspect this resource.', 'error'];
  if (event.type === 'resource.stop_requested') return [`Stopping ${eventResourceName(event)}`, 'Remote cancellation was requested.', 'warning'];
  if (event.type === 'resource.stop_confirmed') return [`${eventResourceName(event)} stopped`, 'The provider confirmed the stop request.', 'success'];
  if (event.type === 'resource.observed' && ['completed', 'failed', 'stopped'].includes(payload.status)) {
    const tone = payload.status === 'failed' ? 'error' : 'success';
    return [`${eventResourceName(event)} ${payload.status}`, payload.message || `Resource is now ${payload.status}.`, tone];
  }
  if (event.type.startsWith('action.') && ['action.failed', 'action.rejected', 'action.completed'].includes(event.type)) {
    const tone = event.type === 'action.completed' ? 'success' : 'error';
    return [friendlyStatus(event.type.replace('action.', 'Action ')), payload.error || payload.message || 'Run action state changed.', tone];
  }
  if (event.type === 'notebook.timeout_recovered') return ['Cell timeout recovered', 'Execution continued after interrupting the timed-out cell.', 'warning'];
  if (event.type === 'notebook.timeout_recovery_failed') return ['Cell timeout recovery failed', payload.error || 'The kernel could not recover cleanly.', 'error'];
  return null;
}

function meaningfulEvents(events) {
  const output = [];
  const seen = new Set();
  for (const event of [...events].reverse()) {
    const description = humanEvent(event);
    if (!description) continue;
    let key = event.type;
    if (event.type === 'notebook.progress') key = 'latest-progress';
    if (event.type === 'resource.observed') key = `${event.type}:${event.payload?.internal_id}:${event.payload?.status}`;
    if (seen.has(key)) continue;
    seen.add(key);
    output.push({event, description});
    if (output.length >= 9) break;
  }
  return output;
}

function renderActivity(events) {
  const activity = meaningfulEvents(events);
  $('activity').innerHTML = activity.length ? activity.map(({event, description}) => {
    const [title, detail, tone] = description;
    return `<div class="activity-item activity-${escapeHtml(tone)}"><span class="activity-dot"></span><span><strong>${escapeHtml(title)}</strong><small>${escapeHtml(detail)}</small></span><time>${escapeHtml(relativeTime(event.timestamp))}</time></div>`;
  }).join('') : '<div class="empty small">Waiting for a meaningful state change.</div>';
}

function renderEvents(events) {
  $('events').innerHTML = events.length ? events.slice(-100).reverse().map(event => `
    <div class="event-line"><time>${escapeHtml(event.timestamp?.slice(11, 19) || '—')}</time><strong>${escapeHtml(event.type)}</strong><span>${escapeHtml(JSON.stringify(event.payload))}</span></div>`).join('') : '<div class="empty small">No durable events yet.</div>';
}

function render() {
  const {run, cells, resources, events, capabilities} = state;
  $('run-name').textContent = run.name;
  $('run-message').textContent = run.message || 'Monitoring notebook execution.';
  $('run-status').textContent = friendlyStatus(run.status);
  $('run-status').className = `status status-${cssToken(run.status)}`;
  $('elapsed').textContent = fmtDuration(elapsed(run));
  renderProgress(run, cells, events);
  const issues = collectIssues(state);
  renderAttention(issues);
  $('remote-count').textContent = String(resources.filter(resource => isRemoteJob(resource) && isMonitoredActive(resource, run)).length);
  renderResources(resources, run, capabilities);
  renderCells(cells, run);
  renderActivity(events);
  renderEvents(events);
  updateFreshness();
}

const source = new EventSource('/api/events');
source.addEventListener('open', () => setConnection('LIVE'));
source.addEventListener('error', () => setConnection('RECONNECTING'));
source.addEventListener('runwatch', () => scheduleRefresh());
scheduleRefresh(0);
setInterval(() => {
  if (!state) return;
  $('elapsed').textContent = fmtDuration(elapsed(state.run));
  updateFreshness();
}, 1000);
setInterval(() => scheduleRefresh(0), 10000);
