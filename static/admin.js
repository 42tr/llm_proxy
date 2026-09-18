const $ = s => document.querySelector(s);
const api = async (path, options={}) => {
  options.headers = {'Content-Type':'application/json', ...(options.headers||{})};
  const key = $('#adminKey').value; if (key) options.headers.Authorization = 'Bearer ' + key;
  const res = await fetch(path, options); const value = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(value.error?.message || res.statusText); return value;
};
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let providers = [], routes = [];
$('#adminKey').addEventListener('change', load);
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
function resetProvider(){ $('#providerForm').reset(); $('#providerForm').elements.id.value=''; $('#providerForm').elements.timeout_ms.value=120000; $('#providerCancel').hidden=true; }
function resetRoute(){ $('#routeForm').reset(); $('#routeForm').elements.id.value=''; $('#routeCancel').hidden=true; }
function render(){
  $('#providers').innerHTML = providers.map(p => `<tr><td>${esc(p.name)}</td><td><code>${esc(p.endpoint_url)}</code></td><td>${esc(p.auth_type)}</td><td>${p.enabled?'启用':'停用'}</td><td><button data-edit-provider="${esc(p.id)}">编辑</button><button data-delete-provider="${esc(p.id)}">删除</button></td></tr>`).join('');
  $('#providerSelect').innerHTML = providers.filter(p=>p.enabled).map(p=>`<option value="${esc(p.id)}">${esc(p.name)}</option>`).join('');
  $('#routes').innerHTML = routes.map(r => `<tr><td>${esc(r.public_model)}</td><td><code>${esc(r.upstream_model)}</code></td><td>${esc(r.provider_name)}</td><td>${r.enabled&&r.provider_enabled?'启用':'停用'}</td><td><button data-edit-route="${esc(r.id)}">编辑</button><button data-delete-route="${esc(r.id)}">删除</button></td></tr>`).join('');
}
async function load(){ try { providers=(await api('/api/admin/providers')).items; routes=(await api('/api/admin/model-routes')).items; render(); $('#pageError').textContent=''; } catch(e) { $('#pageError').textContent=e.message; } }
$('#providerForm').onsubmit = async e => { e.preventDefault(); const result=$('#providerResult'); try { const x=formObject(e.target); const id=x.id; delete x.id; if (!x.api_key) delete x.api_key; await api('/api/admin/providers'+(id?'/'+encodeURIComponent(id):''), {method:id?'PUT':'POST',body:JSON.stringify(x)}); result.className='ok'; result.textContent=' 已保存'; resetProvider(); await load(); } catch(err){ result.className='error'; result.textContent=' '+err.message; } };
$('#routeForm').onsubmit = async e => { e.preventDefault(); const result=$('#routeResult'); try { const x=formObject(e.target); const id=x.id; delete x.id; delete x.log_request_body; delete x.log_response_body; delete x.extra_headers; delete x.timeout_ms; delete x.auth_type; delete x.api_key; await api('/api/admin/model-routes'+(id?'/'+encodeURIComponent(id):''), {method:id?'PUT':'POST',body:JSON.stringify(x)}); result.className='ok'; result.textContent=' 已保存'; resetRoute(); await load(); } catch(err){ result.className='error'; result.textContent=' '+err.message; } };
$('#providerCancel').onclick=resetProvider; $('#routeCancel').onclick=resetRoute;
document.addEventListener('click', async e => { const t=e.target; try {
  if(t.dataset.editProvider){ const p=providers.find(x=>x.id===t.dataset.editProvider), f=$('#providerForm'); for(const k of ['id','name','endpoint_url','auth_type','auth_header_name','timeout_ms']) f.elements[k].value=p[k]??''; f.elements.api_key.value=''; f.elements.extra_headers.value=JSON.stringify(p.extra_headers||{},null,2); f.elements.enabled.checked=p.enabled; f.elements.log_request_body.checked=p.log_request_body; f.elements.log_response_body.checked=p.log_response_body; $('#providerCancel').hidden=false; }
  if(t.dataset.deleteProvider && confirm('删除该上游服务？')) { await api('/api/admin/providers/'+encodeURIComponent(t.dataset.deleteProvider),{method:'DELETE'}); await load(); }
  if(t.dataset.editRoute){ const r=routes.find(x=>x.id===t.dataset.editRoute), f=$('#routeForm'); for(const k of ['id','public_model','upstream_model','provider_id']) f.elements[k].value=r[k]??''; f.elements.enabled.checked=r.enabled; $('#routeCancel').hidden=false; }
  if(t.dataset.deleteRoute && confirm('删除该模型映射？')) { await api('/api/admin/model-routes/'+encodeURIComponent(t.dataset.deleteRoute),{method:'DELETE'}); await load(); }
 } catch(err){ $('#pageError').textContent=err.message; } });
load();
