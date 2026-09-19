const $ = s => document.querySelector(s);
// The admin key is kept in localStorage so a returning browser skips the prompt; any 401 drops it and re-locks.
const KEY_STORAGE = 'llm-proxy.adminKey';
class AuthError extends Error {}
let adminKey = '';
const readStoredKey = () => { try { return localStorage.getItem(KEY_STORAGE) || ''; } catch { return ''; } };
const writeStoredKey = value => { try { value ? localStorage.setItem(KEY_STORAGE, value) : localStorage.removeItem(KEY_STORAGE); } catch {} };
const api = async (path, options={}) => {
  options.headers = {'Content-Type':'application/json', ...(options.headers||{})};
  if (adminKey) options.headers.Authorization = 'Bearer ' + adminKey;
  const res = await fetch(path, options); const value = await res.json().catch(() => ({}));
  if (res.status === 401) { lock('管理 API Key 无效或已失效，请重新输入'); throw new AuthError('管理 API Key 无效或已失效'); }
  if (!res.ok) throw new Error(value.error?.message || res.statusText); return value;
};
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let providers = [], routes = [];
$('#logDate').value = new Date().toISOString().slice(0, 10);
function formObject(form) {
  const data = Object.fromEntries(new FormData(form));
  data.enabled = form.elements.enabled.checked;
  data.log_request_body = form.elements.log_request_body?.checked ?? false;
  data.log_response_body = form.elements.log_response_body?.checked ?? false;
  if (data.timeout_ms) data.timeout_ms = Number(data.timeout_ms);
  if (data.extra_headers !== undefined) {
    data.extra_headers = data.extra_headers ? JSON.parse(data.extra_headers) : {};
    // A masked value from an edit form means "keep the stored value".
    if (Object.values(data.extra_headers).some(value => value === '[saved]')) delete data.extra_headers;
  }
  return data;
}
function flash(id, message){ const el=$(id); el.textContent=message; el.classList.add('show'); clearTimeout(el._timer); el._timer=setTimeout(()=>{ el.textContent=''; el.classList.remove('show'); }, 2600); }
// The provider/route forms live in <dialog> elements and only appear on 新增/编辑.
function openProviderDialog(p){
  const f=$('#providerForm'); f.reset();
  f.elements.id.value=''; f.elements.extra_headers.value='';
  $('#providerDialogTitle').textContent='新增上游服务';
  if(p){
    for(const k of ['id','name','endpoint_url','auth_type','auth_header_name','timeout_ms']) f.elements[k].value=p[k]??'';
    f.elements.extra_headers.value=JSON.stringify(p.extra_headers||{},null,2);
    f.elements.enabled.checked=p.enabled; f.elements.log_request_body.checked=p.log_request_body; f.elements.log_response_body.checked=p.log_response_body;
    $('#providerDialogTitle').textContent='编辑上游服务';
  }
  $('#providerResult').textContent=''; $('#providerResult').className='';
  $('#providerDialog').showModal(); f.elements.name.focus();
}
function openRouteDialog(r){
  const f=$('#routeForm'); f.reset(); f.elements.id.value='';
  $('#routeDialogTitle').textContent='新增模型映射';
  if(r){
    for(const k of ['id','public_model','upstream_model','provider_id']) f.elements[k].value=r[k]??'';
    f.elements.enabled.checked=r.enabled;
    $('#routeDialogTitle').textContent='编辑模型映射';
  }
  $('#routeResult').textContent=''; $('#routeResult').className='';
  $('#routeDialog').showModal(); f.elements.public_model.focus();
}
function render(){
  $('#providersBody').innerHTML = providers.map(p => `<tr><td>${esc(p.name)}</td><td><code>${esc(p.endpoint_url)}</code></td><td><span class="pill">${esc(p.auth_type)}</span></td><td><span class="pill ${p.enabled?'on':'off'}">${p.enabled?'启用':'停用'}</span></td><td><button class="small" data-edit-provider="${esc(p.id)}">编辑</button><button class="small" data-test-provider="${esc(p.id)}">测试</button><button class="small danger" data-delete-provider="${esc(p.id)}">删除</button></td></tr>`).join('');
  $('#providerSelect').innerHTML = providers.filter(p=>p.enabled).map(p=>`<option value="${esc(p.id)}">${esc(p.name)}</option>`).join('');
  $('#routesBody').innerHTML = routes.map(r => `<tr><td>${esc(r.public_model)}</td><td><code>${esc(r.upstream_model)}</code></td><td>${esc(r.provider_name)}</td><td><span class="pill ${r.enabled&&r.provider_enabled?'on':'off'}">${r.enabled&&r.provider_enabled?'启用':'停用'}</span></td><td><button class="small" data-edit-route="${esc(r.id)}">编辑</button><button class="small danger" data-delete-route="${esc(r.id)}">删除</button></td></tr>`).join('');
  $('#providerCount').textContent = providers.length;
  $('#routeCount').textContent = routes.length;
}
async function load(){ try { providers=(await api('/api/admin/providers')).items; routes=(await api('/api/admin/model-routes')).items; render(); $('#pageError').textContent=''; } catch(e) { $('#pageError').textContent=e.message; } }
const statusPill = status => `<span class="pill ${Number(status) < 400 ? 'on' : 'bad'}">${esc(status)}</span>`;
let logItems = [];
async function loadLogs(){ const form=$('#logForm'); const params=new URLSearchParams(new FormData(form)); try { const result=await api('/api/admin/logs?'+params); logItems=result.items; $('#logsBody').innerHTML=result.items.map((item,i) => `<tr><td class="mono">${esc(item.time)}</td><td>${esc(item.model)}</td><td>${statusPill(item.status)}</td><td>${esc(item.latency_ms)} ms</td><td>${item.stream?'是':'否'}</td><td>${item.error?`<span class="pill bad">${esc(item.error)}</span>`:''}</td><td><button class="small" data-log="${i}">查看</button></td></tr>`).join(''); $('#logResult').className='muted'; $('#logResult').textContent=`共 ${result.items.length} 条（${esc(result.date)}）`; } catch(e) { $('#logResult').className='error'; $('#logResult').textContent=' '+e.message; } }
$('#providerForm').onsubmit = async e => { e.preventDefault(); const result=$('#providerResult'); try { const x=formObject(e.target); const id=x.id; delete x.id; if (!x.api_key) delete x.api_key; await api('/api/admin/providers'+(id?'/'+encodeURIComponent(id):''), {method:id?'PUT':'POST',body:JSON.stringify(x)}); $('#providerDialog').close(); flash('#providerFlash', id?'上游服务已更新':'上游服务已新增'); await load(); } catch(err){ result.className='error'; result.textContent=' '+err.message; } };
$('#routeForm').onsubmit = async e => { e.preventDefault(); const result=$('#routeResult'); try { const x=formObject(e.target); const id=x.id; delete x.id; delete x.log_request_body; delete x.log_response_body; delete x.extra_headers; delete x.timeout_ms; delete x.auth_type; delete x.api_key; await api('/api/admin/model-routes'+(id?'/'+encodeURIComponent(id):''), {method:id?'PUT':'POST',body:JSON.stringify(x)}); $('#routeDialog').close(); flash('#routeFlash', id?'模型映射已更新':'模型映射已新增'); await load(); } catch(err){ result.className='error'; result.textContent=' '+err.message; } };
$('#providerAdd').onclick = () => openProviderDialog();
$('#routeAdd').onclick = () => openRouteDialog();
document.querySelectorAll('dialog.modal').forEach(d => d.addEventListener('click', e => { if (e.target === d) d.close(); }));
document.addEventListener('click', e => { const closer = e.target.closest('[data-close]'); if (closer) closer.closest('dialog').close(); });
$('#testClose').onclick = () => { $('#testPanel').hidden = true; };
// One real upstream call per click so administrators can validate credentials and reachability.
async function runProviderTest(providerId){
  const provider = providers.find(p => p.id === providerId);
  const model = prompt(`用哪个模型测试「${provider?.name || providerId}」？`, 'gpt-4o');
  if (!model || !model.trim()) return;
  const panel=$('#testPanel'), status=$('#testStatus'), result=$('#testResult');
  panel.hidden = false; status.className='muted hint'; status.textContent=`正在用 ${model.trim()} 调用上游…`; result.textContent='';
  try {
    const res = await fetch('/api/admin/providers/'+encodeURIComponent(providerId)+'/test', {method:'POST', headers:{'Content-Type':'application/json', ...(adminKey?{Authorization:'Bearer '+adminKey}:{})}, body:JSON.stringify({model:model.trim()})});
    if (res.status === 401) { lock('管理 API Key 无效或已失效，请重新输入'); return; }
    const text = await res.text();
    let pretty = text;
    try { pretty = JSON.stringify(JSON.parse(text), null, 2); } catch {}
    status.className = res.ok ? 'ok hint' : 'error hint';
    status.textContent = `上游返回 ${res.status}${res.ok ? '，连通正常' : '，请检查地址、认证与模型名'}`;
    result.textContent = pretty || '(空响应)';
  } catch(err){ status.className='error hint'; status.textContent=' 请求失败：'+err.message; result.textContent=''; }
}
$('#logForm').onsubmit = e => { e.preventDefault(); loadLogs(); };
document.addEventListener('click', async e => { const t=e.target; try {
  if(t.dataset.editProvider){ openProviderDialog(providers.find(x=>x.id===t.dataset.editProvider)); }
  if(t.dataset.deleteProvider && confirm('删除该上游服务？')) { await api('/api/admin/providers/'+encodeURIComponent(t.dataset.deleteProvider),{method:'DELETE'}); await load(); }
  if(t.dataset.editRoute){ openRouteDialog(routes.find(x=>x.id===t.dataset.editRoute)); }
  if(t.dataset.deleteRoute && confirm('删除该模型映射？')) { await api('/api/admin/model-routes/'+encodeURIComponent(t.dataset.deleteRoute),{method:'DELETE'}); await load(); }
  if(t.dataset.testProvider) await runProviderTest(t.dataset.testProvider);
  if(t.dataset.log !== undefined) openLogDialog(logItems[Number(t.dataset.log)]);
 } catch(err){ $('#pageError').textContent=err.message; } });
const decodeCapture = cap => { if(!cap) return null; if(cap.encoding === 'base64'){ try { return atob(cap.body); } catch { return '(base64 解码失败)'; } } return cap.body; };
const prettyMaybe = text => { try { return JSON.stringify(JSON.parse(text), null, 2); } catch { return text; } };
const metaCell = (label, value) => (value === undefined || value === null || value === '') ? '' : `<div><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`;
// Show the captured request/response bodies instead of one raw JSON blob.
function openLogDialog(item){
  if(!item) return;
  $('#logDialogTitle').textContent = `调用详情 · ${item.model || item.request_id}`;
  $('#logMeta').innerHTML = [
    metaCell('请求 ID', item.request_id), metaCell('时间', item.time), metaCell('来源', item.source),
    metaCell('客户端模型', item.model), metaCell('上游模型', item.upstream_model),
    metaCell('HTTP 状态', item.status), metaCell('上游状态', item.upstream_status),
    metaCell('结果', item.outcome), metaCell('错误', item.error),
    metaCell('流式', item.stream ? '是' : '否'),
    metaCell('首字节', item.first_byte_latency_ms == null ? '' : `${item.first_byte_latency_ms} ms`),
    metaCell('总耗时', `${item.latency_ms} ms`),
    metaCell('请求字节', item.request_bytes), metaCell('响应字节', item.response_bytes),
  ].join('');
  const req = decodeCapture(item.request), res = decodeCapture(item.response);
  $('#logReqNote').textContent = item.request ? `${item.request.bytes} 字节 · ${item.request.encoding}${item.request.truncated ? ' · 已截断' : ''}` : '';
  $('#logReqBody').textContent = req === null ? '未记录：该上游服务未开启「记录请求正文」。' : prettyMaybe(req);
  $('#logResNote').textContent = item.response ? `${item.response.bytes} 字节 · ${item.response.encoding}${item.response.truncated ? ' · 已截断' : ''}${item.stream ? ' · 流式 SSE 拼接文本' : ''}` : '';
  $('#logResBody').textContent = res === null ? '未记录：该上游服务未开启「记录响应正文」。' : prettyMaybe(res);
  $('#logRaw').textContent = JSON.stringify(item, null, 2);
  $('#logDialog').showModal();
}
function lock(message='', note=''){
  adminKey = ''; writeStoredKey('');
  providers = []; routes = []; render();
  $('#app').hidden = true; $('#login').hidden = false;
  $('#loginError').textContent = message; $('#loginStatus').textContent = note;
  $('#adminKey').value = ''; $('#adminKey').focus();
}
function unlock(){
  $('#login').hidden = true; $('#app').hidden = false;
  $('#loginError').textContent = ''; $('#loginStatus').textContent = ''; $('#adminKey').value = '';
  load(); loadLogs();
}
async function verifyKey(key){
  const res = await fetch('/api/admin/access', {headers:{Authorization:'Bearer ' + key}});
  if (res.status === 401) throw new AuthError('管理 API Key 无效');
  if (!res.ok) { const value = await res.json().catch(() => ({})); throw new Error(value.error?.message || res.statusText); }
}
$('#loginForm').onsubmit = async e => {
  e.preventDefault();
  const key = $('#adminKey').value.trim();
  if (!key) { $('#loginError').textContent = '请输入管理 API Key'; return; }
  $('#loginSubmit').disabled = true; $('#loginError').textContent = ''; $('#loginStatus').textContent = '正在验证…';
  try { await verifyKey(key); adminKey = key; writeStoredKey(key); unlock(); }
  catch (err) { $('#loginStatus').textContent = ''; $('#loginError').textContent = err instanceof AuthError ? err.message : '登录失败：' + err.message; }
  finally { $('#loginSubmit').disabled = false; }
};
$('#logout').onclick = () => lock('', '已退出登录，本浏览器保存的管理 API Key 已清除。');
async function boot(){
  const stored = readStoredKey();
  $('#login').hidden = false;
  if (!stored) { $('#adminKey').focus(); return; }
  $('#loginStatus').textContent = '正在验证已保存的管理 API Key…';
  try { await verifyKey(stored); adminKey = stored; unlock(); }
  catch (err) {
    $('#loginStatus').textContent = '';
    if (err instanceof AuthError) { writeStoredKey(''); $('#loginError').textContent = '已保存的管理 API Key 无效或已失效，请重新输入。'; }
    else $('#loginError').textContent = '无法连接服务端：' + err.message;
    $('#adminKey').focus();
  }
}
boot();
