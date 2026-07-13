const $ = id => document.getElementById(id);
let state = null;
let refreshTimer = null;

const initialUrl = new URL(window.location.href);
if (initialUrl.searchParams.has('token')) {
  initialUrl.searchParams.delete('token');
  const query = initialUrl.searchParams.toString();
  window.history.replaceState({}, '', `${initialUrl.pathname}${query ? `?${query}` : ''}${initialUrl.hash}`);
}

const escapeHtml = value => String(value ?? '').replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
const fmtDuration = seconds => {
  if (seconds == null) return '—';
  const s = Math.max(0, Math.floor(seconds));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
  return h ? `${h}h ${m}m` : m ? `${m}m ${r}s` : `${r}s`;
};
const fmtValue = value => {
  if (value == null) return '—';
  if (typeof value === 'boolean') return value ? 'yes' : 'no';
  if (typeof value === 'number') return Math.abs(value) >= 1000 ? value.toLocaleString() : Number(value.toFixed?.(3) ?? value);
  return String(value);
};
const isByteMetric = key => key === 'bytes' || key.endsWith('_bytes');
const isTimestampMetric = key => key.endsWith('_at') || key.endsWith('_timestamp');
const fmtBytes = value => {
  if (typeof value !== 'number' || !Number.isFinite(value)) return fmtValue(value);
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  const magnitude = Math.abs(value);
  const unit = magnitude ? Math.min(Math.floor(Math.log(magnitude) / Math.log(1024)), units.length - 1) : 0;
  const scaled = value / (1024 ** unit);
  const maximumFractionDigits = unit === 0 || Math.abs(scaled) >= 100 ? 0 : Math.abs(scaled) >= 10 ? 1 : 2;
  return `${scaled.toLocaleString(undefined, {maximumFractionDigits})} ${units[unit]}`;
};
const fmtTimestamp = value => {
  const date = typeof value === 'number' ? new Date(value * 1000) : new Date(value);
  if (Number.isNaN(date.getTime())) return fmtValue(value);
  return new Intl.DateTimeFormat(undefined, {dateStyle:'medium', timeStyle:'medium'}).format(date);
};
const fmtMetricValue = (key, value) => isByteMetric(key) ? fmtBytes(value) : isTimestampMetric(key) ? fmtTimestamp(value) : fmtValue(value);
const fmtMetricLabel = key => key === 'bytes' ? 'size' : key.replace(/_bytes$/, '').replaceAll('_',' ');
function toast(message, error=false) {
  const el = $('toast'); $('toast-message').textContent = message; el.classList.toggle('error', error);
  el.classList.add('show'); setTimeout(() => el.classList.remove('show'), 3800);
}
async function api(path, options={}) {
  const response = await fetch(path, {headers:{'Content-Type':'application/json', ...(options.headers||{})}, ...options});
  if (!response.ok) { let detail=response.statusText; try {detail=(await response.json()).detail||detail;} catch {} throw new Error(detail); }
  return response.status === 204 ? null : response.json();
}
function elapsed(run) {
  if (!run.started_at) return null;
  const end = run.ended_at ? new Date(run.ended_at) : new Date();
  return (end - new Date(run.started_at)) / 1000;
}
function scheduleRefresh(delay=160) {
  if (refreshTimer) return;
  refreshTimer = setTimeout(async()=>{
    refreshTimer=null;
    try { state=await api('/api/state'); render(); $('connection').textContent='LIVE'; }
    catch { $('connection').textContent='RECONNECTING'; }
  }, delay);
}
function latestProgress(events) {
  return [...events].reverse().find(e => e.type === 'notebook.progress')?.payload;
}
function render() {
  const {run,cells,resources,events,capabilities}=state;
  $('run-name').textContent=run.name; $('run-message').textContent=run.message||'';
  $('run-status').textContent=run.status.replaceAll('_',' '); $('run-status').className=`status status-${run.status}`;
  $('elapsed').textContent=fmtDuration(elapsed(run)); $('kernel-epoch').textContent=run.kernel_epoch;
  const current=cells.find(c=>c.cell_index===run.current_cell_index);
  $('current-label').textContent=current?(current.label||`Cell ${current.cell_index+1}`):run.status==='succeeded'?'Completed':run.status==='waiting_external'?'Notebook complete · waiting on resources':run.status==='paused'?'Paused for local repair':'No active cell';
  $('current-meta').textContent=current?`cell ${current.cell_index+1} · attempt ${current.attempt} · ${current.status}`:'—';
  const executable=cells.filter(c=>c.cell_type==='code');
  const done=executable.filter(c=>['succeeded','skipped','not_replayed'].includes(c.status)).length;
  const pct=executable.length?100*done/executable.length:0;
  $('notebook-progress').style.width=`${pct}%`; $('progress-copy').textContent=`${done} / ${executable.length} code cells`;
  const progress=latestProgress(events);
  if (progress) {
    $('reported-progress').textContent=progress.total!=null?`${fmtValue(progress.completed)} / ${fmtValue(progress.total)} ${progress.unit||''}`:`${fmtValue(progress.completed)} ${progress.unit||''}`;
    $('reported-message').textContent=progress.message||'Progress reported by notebook';
    $('reported-progress-fill').style.width=progress.total?`${Math.min(100,100*progress.completed/progress.total)}%`:'0%';
  }
  $('resource-active').textContent=resources.filter(r=>!r.terminal&&r.disposition==='active').length;
  $('resource-failed').textContent=resources.filter(r=>r.status==='failed'||r.status==='monitor_error').length;
  renderResources(resources, capabilities); renderCells(cells); renderEvents(events);
}
function primitiveMetrics(metrics) {
  return Object.entries(metrics||{}).filter(([,v])=>v!==null&&['string','number','boolean'].includes(typeof v)).slice(0,14);
}
function sparkline(resource) {
  const observations=resource.observations||[];
  const candidates={};
  observations.forEach(o=>Object.entries(o.metrics||{}).forEach(([k,v])=>{if(typeof v==='number'&&Number.isFinite(v))(candidates[k]??=[]).push(v);}));
  const entry=Object.entries(candidates).find(([,values])=>values.length>=2);
  if(!entry)return '';
  const [label,values]=entry, width=320,height=64,min=Math.min(...values),max=Math.max(...values),span=max-min||1;
  const points=values.map((v,i)=>`${(i/(values.length-1))*width},${height-((v-min)/span)*(height-8)-4}`).join(' ');
  return `<div class="spark"><div>${escapeHtml(label.replaceAll('_',' '))}</div><svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none"><polyline points="${points}" /></svg></div>`;
}
function renderResources(resources, capabilities) {
  $('resource-count').textContent=`${resources.length} tracked`; const root=$('resources');
  if(!resources.length){root.innerHTML='<div class="panel surface empty">No structured resources emitted yet.</div>';return;}
  root.innerHTML=resources.map(r=>{
    const metrics=primitiveMetrics(r.metrics).map(([k,v])=>{const formatted=fmtMetricValue(k,v), wide=isTimestampMetric(k)?' metric-wide':'';return `<div class="resource-metric${wide}"><strong title="${escapeHtml(formatted)}">${escapeHtml(formatted)}</strong><span>${escapeHtml(fmtMetricLabel(k))}</span></div>`;}).join('');
    const logs=(r.log_tail||[]).slice(-35).join('\n');
    const canStop=capabilities.controller_live&&!r.terminal&&r.status!=='stopping'&&r.disposition==='active'&&r.ownership==='exclusive'&&r.supports_stop;
    const link=r.link;
    const linkControl=link?.status==='ready'&&link.href?`<a class="soft-button resource-link" href="${escapeHtml(link.href)}" target="_blank" rel="noopener"><span>Open ${escapeHtml(link.label||'dashboard')}</span></a>`:link?`<span class="resource-link-state${link.status==='failed'?' failed':''}">${escapeHtml(link.status==='failed'?(link.message||'Dashboard link unavailable'):'Preparing dashboard link…')}</span>`:'';
    const actions=linkControl||canStop?`<div class="resource-actions">${linkControl}${canStop?`<button class="soft-button danger-soft stop-resource" data-id="${escapeHtml(r.internal_id)}"><span>Stop resource</span></button>`:''}</div>`:'';
    return `<article class="resource-card panel surface"><div class="resource-header"><div><h3>${escapeHtml(link?.label||r.external_id)}</h3><div class="resource-type">${escapeHtml(r.provider)}.${escapeHtml(r.resource_type)} · ${r.cell_index==null?'configured':`cell ${r.cell_index+1}`} · epoch ${r.kernel_epoch??'—'}</div></div><span class="status status-${escapeHtml(r.status)}">${escapeHtml(r.status)}</span></div>${r.message?`<p class="resource-message">${escapeHtml(r.message)}</p>`:''}${sparkline(r)}<div class="resource-metrics">${metrics}</div>${logs?`<details><summary>Recent logs</summary><pre class="log-tail">${escapeHtml(logs)}</pre></details>`:''}${actions}</article>`;
  }).join('');
  root.querySelectorAll('.stop-resource').forEach(button=>button.addEventListener('click',()=>confirmStop(button.dataset.id)));
}
async function confirmStop(id) {
  const resource=state.resources.find(r=>r.internal_id===id); if(!resource)return;
  const affected=state.resources.filter(r=>r.internal_id!==id&&!r.terminal&&r.status!=='stopping'&&r.disposition==='active'&&r.lifecycle?.stop_on_cancel&&r.ownership==='exclusive'&&r.supports_stop);
  $('stop-details').innerHTML=`<dt>Job</dt><dd>${escapeHtml(resource.external_id)}</dd><dt>Region</dt><dd>${escapeHtml(resource.region||'default')}</dd><dt>Origin</dt><dd>${resource.cell_index==null?'configured':`cell ${resource.cell_index+1}, attempt ${resource.attempt}`}</dd><dt>Ownership</dt><dd>${escapeHtml(resource.ownership)}</dd>`;
  $('cascade-list').innerHTML=`<strong>${affected.length} other resource(s) eligible for provider stop</strong>${affected.map(r=>`<div>${escapeHtml(r.external_id)}</div>`).join('')}`;
  const dialog=$('stop-dialog'); dialog.showModal();
  dialog.addEventListener('close',async function handler(){dialog.removeEventListener('close',handler);if(dialog.returnValue!=='confirm')return;try{const result=await api(`/api/resources/${id}/stop`,{method:'POST',body:JSON.stringify({confirmation:'STOP RESOURCE AND CANCEL RUN',expected_version:resource.version})});toast(`Cancellation action ${result.action_id.slice(0,8)} queued`);scheduleRefresh(20);}catch(error){toast(error.message,true);}});
}
function outputText(output) {
  if(output.output_type==='error')return (output.traceback||[]).join('\n');
  return output.text||JSON.stringify(output);
}
function renderCells(cells) {
  $('cell-count').textContent=`${cells.length} cells`; const root=$('cells');
  root.innerHTML=cells.map(c=>{const detail=[...(c.output_tail||[]).slice(-8).map(outputText),...(c.traceback||[])].filter(Boolean).join('\n');return `<details class="cell-row cell-${escapeHtml(c.status)}"><summary><span class="cell-dot"></span><span><strong>${escapeHtml(c.label||`Cell ${c.cell_index+1}`)}</strong><small>${escapeHtml(c.cell_type)} · attempt ${c.attempt} · ${escapeHtml(c.status)}${c.error_name?` · ${escapeHtml(c.error_name)}`:''}</small></span><span class="mono muted">${fmtDuration(c.elapsed_seconds)}</span></summary>${detail?`<pre class="cell-output">${escapeHtml(detail)}</pre>`:'<div class="empty small">No captured output.</div>'}</details>`;}).join('');
}
function renderEvents(events) {
  $('events').innerHTML=events.slice(-100).reverse().map(e=>`<div class="event-line"><time>${escapeHtml(e.timestamp.slice(11,19))}</time><strong>${escapeHtml(e.type)}</strong><span>${escapeHtml(JSON.stringify(e.payload))}</span></div>`).join('');
}

const source=new EventSource('/api/events');
source.addEventListener('open',()=>{$('connection').textContent='LIVE';});
source.addEventListener('error',()=>{$('connection').textContent='RECONNECTING';});
source.addEventListener('runwatch',()=>scheduleRefresh());
scheduleRefresh(0);
setInterval(()=>{if(state)$('elapsed').textContent=fmtDuration(elapsed(state.run));},1000);
setInterval(()=>scheduleRefresh(0),10000);
