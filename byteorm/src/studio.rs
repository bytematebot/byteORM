use axum::http::StatusCode;
use axum::{
    Json, Router,
    extract::Path,
    extract::Query,
    extract::State,
    routing::{get, post},
};
use byteorm_lib::Schema;
use byteorm_lib::db::DbPool;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio_postgres::Row;
use tokio_postgres::types::{Json as PgJson, ToSql};

#[derive(Clone)]
struct AppState {
    schema: Schema,
    pool: DbPool,
}

#[derive(Serialize)]
struct TableInfo {
    name: String,
    columns: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct UpdatePayload {
    values: HashMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct RowData {
    values: HashMap<String, serde_json::Value>,
    pk_columns: Vec<String>,
}

#[derive(Deserialize)]
struct KeyPath {
    name: String,
    pk: String,
}

#[derive(Deserialize)]
struct CompositePkPayload {
    values: HashMap<String, serde_json::Value>,
}

pub async fn run(
    schema: Schema,
    pool: DbPool,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState { schema, pool };

    let app = Router::new()
        .route("/api/tables", get(list_tables))
        .route("/api/table/:name", get(list_rows))
        .route("/api/table/:name/update/:pk", post(update_row_handler))
        .route("/api/table/:name/delete/:pk", post(delete_row_handler))
        .route("/", get(index))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> (StatusCode, axum::response::Html<&'static str>) {
    (StatusCode::OK, axum::response::Html(INDEX_HTML_TOP))
}
const INDEX_HTML_TOP: &str = r#"<!DOCTYPE html><html lang='en'>
<head>
    <meta charset='utf-8'/>
    <meta name='viewport' content='width=device-width, initial-scale=1'/>
    <title>ByteORM Studio</title>
    <link rel='preconnect' href='https://fonts.googleapis.com'>
    <link rel='preconnect' href='https://fonts.gstatic.com' crossorigin>
    <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap' rel='stylesheet'>
    <style>
        :root { --bg:#0f172a; --card:#0b1225; --muted:#64748b; --text:#e2e8f0; --accent:#6b21a8; --border:#243041; }
        * { box-sizing: border-box }
        body { font-family: 'Inter', ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; margin:0; background:var(--bg); color:var(--text); min-height:100vh; display:flex; flex-direction:column }
        header { padding:14px 16px; border-bottom:1px solid var(--border); background:linear-gradient(0deg, rgba(255,255,255,0.02), rgba(255,255,255,0.02)), var(--card); display:flex; flex-wrap:wrap; gap:10px; align-items:center; position:sticky; top:0; z-index:10 }
        header h1 { font-size:16px; margin:0; font-weight:600; letter-spacing:0.4px }
        .sep { flex:1 }
        .ctrl { display:flex; gap:8px; align-items:center }
        select, button, input[type='text'] { background:#0f1a33; color:var(--text); border:1px solid var(--border); border-radius:8px; padding:8px 10px; font-size:13px }
        button { cursor:pointer }
        main { padding:16px; width:100%; max-width:1600px; margin:0 auto; flex:1; display:flex; flex-direction:column; gap:12px }
        .card { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:14px }
        .title { display:flex; align-items:center; gap:10px; }
        .title h2 { font-size:15px; margin:0; font-weight:600 }
        .muted { color:var(--muted); font-size:12px }
        table { border-collapse: collapse; width:100%; margin-top:10px; font-size:13px }
        th, td { border:1px solid var(--border); padding:8px 10px; vertical-align:top; white-space: pre-wrap }
        thead th { background:#0f1a33; text-align:left; position:sticky; top:0 }
        tbody tr:hover { background:#0f1a33 }
        td[contenteditable='true'] { outline:none }
        .pk { font-weight:700; color:var(--accent) }
        .actions { white-space:nowrap }
        .actions button { font-size:12px; padding:6px 8px; margin-right:6px }
        .toolbar { display:flex; flex-wrap:wrap; gap:8px; align-items:center }
        tr.is-editing { outline: 1px dashed var(--accent); outline-offset: -2px; }
    </style>
    </head>
<body>
    <header>
        <h1>ByteORM Studio</h1>
        <div class='sep'></div>
        <div class='ctrl'>
            <label for='tableSelect' class='muted'>Table</label>
            <select id='tableSelect'></select>
            <button id='refreshBtn'>Refresh</button>
        </div>
    </header>
    <main>
        <div class='card'>
            <div class='title'>
                <h2 id='tableTitle'>Select table</h2>
                <span id='meta' class='muted'></span>
            </div>
            <div id='rows' style='overflow:auto; max-height: calc(100vh - 190px)'></div>
        </div>
    </main>
    <script>
        let lastTable = null;
        let editingPk = null;
        let lastDataCache = null;
        async function loadTables(){
            const r = await fetch('/api/tables');
            const tables = await r.json();
            const select = document.getElementById('tableSelect');
            select.innerHTML = '';
            for(const tb of tables){
                const opt = document.createElement('option');
                opt.value = tb.name; opt.textContent = tb.name; select.appendChild(opt);
            }
            if(tables.length){
                select.value = tables[0].name;
                loadTable(select.value);
            }
            select.onchange = () => loadTable(select.value);
            document.getElementById('refreshBtn').onclick = () => { if(lastTable) loadTable(lastTable); };
        }
        async function loadTable(name){
            lastTable = name;
            document.getElementById('tableTitle').textContent = name;
            const rowsDiv = document.getElementById('rows');
            rowsDiv.innerHTML = '<div class="muted">Loading...</div>';
            const r = await fetch('/api/table/'+name);
            const data = await r.json();
            lastDataCache = data;
            renderRows(name, data);
            const meta = document.getElementById('meta');
            meta.textContent = (data.rows?.length ?? 0) + ' rows (limit 200)';
        }
        function buildRowPk(row){
            const pkCols = row.pk_columns || ['id'];
            const pkData = {};
            pkCols.forEach(col => { pkData[col] = row.values[col]; });
            return JSON.stringify(pkData);
        }
        function escapeHtml(s){
            return String(s)
                .replaceAll('&','&amp;')
                .replaceAll('<','&lt;')
                .replaceAll('>','&gt;')
                .replaceAll('"','&quot;')
                .replaceAll("'",'&#39;');
        }
        function formatValue(v){
            if(v===null||v===undefined) return '';
            if(typeof v==='object'){
                try { return JSON.stringify(v, null, 2); } catch(e) { return String(v); }
            }
            return String(v);
        }
        function renderRows(name, data){
            const rowsDiv = document.getElementById('rows');
            if(!data.columns){ rowsDiv.innerHTML = '<div class="muted">No columns</div>'; return; }
            const firstRow = data.rows[0];
            const pkCols = firstRow ? (firstRow.pk_columns || ['id']) : ['id'];
            console.log('Rendering data:', data, 'Using PK columns:', pkCols);
            let html = '<table><thead><tr>';
            data.columns.forEach(c => html += '<th>'+c+'</th>');
            html += '<th>actions</th></tr></thead><tbody>';
            data.rows.forEach(row => {
                const pk = buildRowPk(row);
                const pkData = JSON.parse(pk);
                const isEditing = (editingPk !== null && editingPk === pk);
                const pkEscaped = pk.replaceAll('"','&quot;');
                html += '<tr data-pk="'+pkEscaped+'" data-table="'+name+'"'+(isEditing?' class="is-editing"':'')+'>';
                data.columns.forEach(col => {
                    const raw = row.values[col];
                    const display = formatValue(raw);
                    const isPkCol = pkCols.includes(col);
                    const editable = (isPkCol || !isEditing) ? 'false' : 'true';
                    const escapedDisplay = escapeHtml(display);
                    html += '<td '+(isPkCol?'class="pk"':'')+' contenteditable="'+editable+'" data-col="'+col+'" data-original="'+escapedDisplay+'">'+escapedDisplay+'</td>';
                });
                if(isEditing){
                    html += '<td class="actions">'
                             + '<button class="btn-save" data-action="save">💾 Save</button>'
                             + '<button class="btn-cancel" data-action="cancel">✖ Cancel</button>'
                             + '</td>';
                } else {
                    html += '<td class="actions">'
                             + '<button class="btn-edit" data-action="edit">✎ Edit</button>'
                             + '<button class="btn-delete" data-action="delete">🗑 Delete</button>'
                             + '</td>';
                }
                html += '</tr>';
            });
            html += '</tbody></table>';
            rowsDiv.innerHTML = html;
            
            rowsDiv.querySelectorAll('button[data-action]').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const action = e.target.dataset.action;
                    const row = e.target.closest('tr');
                    const pk = row.dataset.pk;
                    const table = row.dataset.table;
                    
                    if(action === 'edit') enterEdit(pk);
                    else if(action === 'cancel') cancelEdit();
                    else if(action === 'save') saveEditRow(table, pk, row);
                    else if(action === 'delete') deleteRow(table, pk);
                });
            });
        }
        function enterEdit(id){ editingPk = id; renderRows(lastTable, lastDataCache); }
        function cancelEdit(){ editingPk = null; renderRows(lastTable, lastDataCache); }

    async function saveEditRow(table,pk,rowEl){
        if(!rowEl){ console.error('Row not found'); return; }
        const cells=rowEl.querySelectorAll('td[data-col][contenteditable="true"]');
        const payload={};
        cells.forEach(c=>{ payload[c.dataset.col]=parseCell(c.textContent.trim()); });
        console.log('Save payload:', payload, 'PK:', pk);
        const btns = rowEl.querySelectorAll('button'); btns.forEach(b=>b.disabled=true);
        const resp = await fetch('/api/table/'+table+'/update/'+encodeURIComponent(pk)+'',
            { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({values:payload}) });
        if(!resp.ok){ 
            let errorMsg = 'Save failed: '+resp.status;
            try {
                const errorData = await resp.json();
                if(errorData.error) errorMsg = errorData.error;
            } catch(e) {
                errorMsg = await resp.text() || errorMsg;
            }
            console.error('Update failed:', resp.status, errorMsg); 
            alert(errorMsg); 
            btns.forEach(b=>b.disabled=false); 
            return; 
        }
        editingPk = null;
        await loadTable(table);
    }

    async function deleteRow(table,pk){
        if(!confirm('Delete row '+pk+'?')) return;
        const rowEl = document.querySelector('tr[data-pk="'+pk.replaceAll('"','&quot;')+'"]');
        if(rowEl){ const btns = rowEl.querySelectorAll('button'); btns.forEach(b=>b.disabled=true); }
        const resp = await fetch('/api/table/'+table+'/delete/'+encodeURIComponent(pk)+'',
            { method:'POST' });
        if(!resp.ok){ 
            let errorMsg = 'Delete failed: '+resp.status;
            try {
                const errorData = await resp.json();
                if(errorData.error) errorMsg = errorData.error;
            } catch(e) {
                errorMsg = await resp.text() || errorMsg;
            }
            console.error('Delete failed:', resp.status, errorMsg); 
            alert(errorMsg); 
        }
        if(editingPk === pk) editingPk = null;
        loadTable(table);
    }

        function parseCell(v){
            if(v==='null' || v.trim()==='') return null;
            if(v==='true') return true;
            if(v==='false') return false;
            if(!isNaN(Number(v)) && v.trim()!=='') return Number(v);
            try { return JSON.parse(v); } catch(e) { return v; }
        }
        let pollTimer = null;
        const POLL_INTERVAL = 5000;
        function startPolling(){
            if(pollTimer) clearInterval(pollTimer);
            pollTimer = setInterval(()=>{ if(lastTable && editingPk===null) softRefresh(lastTable); }, POLL_INTERVAL);
        }
        async function softRefresh(name){
            const r = await fetch('/api/table/'+name);
            const data = await r.json();
            lastDataCache = data;
            renderRows(name, data);
            const meta = document.getElementById('meta');
            meta.textContent = (data.rows?.length ?? 0) + ' rows (limit 200)';
        }
        window.addEventListener('focus', ()=>{ if(lastTable) softRefresh(lastTable); });
        loadTables();
        startPolling();
    </script>
</body></html>"#;
const INDEX_HTML: &str = r#"<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'/><title>ByteORM Studio</title><style>body{font-family:Arial,sans-serif;margin:0;padding:0;display:flex;height:100vh}#sidebar{width:240px;background:#1f1f24;color:#eee;padding:12px;overflow-y:auto}#content{flex:1;padding:16px;overflow:auto}h1{font-size:18px;margin:0 0 12px}button{cursor:pointer}table{border-collapse:collapse;margin-top:12px}th,td{border:1px solid #ccc;padding:4px 8px;font-size:13px;white-space:pre-wrap}tr:hover{background:#f5f5f5}.pk{font-weight:bold;color:#6b21a8}.actions button{margin-right:4px;font-size:11px}</style></head><body><div id='sidebar'><h1>ByteORM Studio</h1><div id='tables'></div></div><div id='content'><h2 id='tableTitle'>Select table</h2><div id='rows'></div></div><script>function escapeHtml(s){return String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');}function formatValue(v){if(v===null||v===undefined)return'';if(typeof v==='object'){try{return JSON.stringify(v,null,2);}catch(e){return String(v);}}return String(v);}async function loadTables(){const r=await fetch('/api/tables');const t=await r.json();const c=document.getElementById('tables');c.innerHTML='';t.forEach(tb=>{const b=document.createElement('button');b.textContent=tb.name;b.style.display='block';b.style.width='100%';b.style.marginBottom='4px';b.onclick=()=>loadTable(tb.name);c.appendChild(b);});}async function loadTable(name){document.getElementById('tableTitle').textContent=name;const r=await fetch('/api/table/'+name);const data=await r.json();renderRows(name,data);}function renderRows(name,data){const rowsDiv=document.getElementById('rows');if(!data.columns){rowsDiv.textContent='No columns';return;}let html='<table><thead><tr>';data.columns.forEach(c=>html+='<th>'+c+'</th>');html+='<th>actions</th></tr></thead><tbody>';data.rows.forEach(row=>{html+='<tr>';data.columns.forEach(col=>{const display=formatValue(row.values[col]);html+='<td '+(col==='id'?'class="pk"':'')+' contenteditable="true" data-col="'+col+'">'+escapeHtml(display)+'</td>';});html+='<td class="actions"><button onclick="saveRow(\''+name+'\','+row.values.id+')">💾</button><button onclick="deleteRow(\''+name+'\','+row.values.id+')">🗑</button></td>';html+='</tr>';});html+='</tbody></table>';rowsDiv.innerHTML=html;}async function saveRow(table,id){const rowEl=[...document.querySelectorAll('tr')].find(tr=>{const idCell=[...tr.querySelectorAll('td')].find(td=>td.dataset.col==='id');return idCell&&idCell.textContent==id;});if(!rowEl)return;const cells=rowEl.querySelectorAll('td[data-col]');const payload={};cells.forEach(c=>{payload[c.dataset.col]=parseCell(c.textContent);});await fetch('/api/table/'+table+'/update/'+id,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({values:payload})});loadTable(table);}async function deleteRow(table,id){await fetch('/api/table/'+table+'/delete/'+id,{method:'POST'});loadTable(table);}function parseCell(v){if(v==='null')return null;if(v==='true')return true;if(v==='false')return false;if(!isNaN(Number(v))&&v.trim()!=='')return Number(v);try{return JSON.parse(v);}catch(e){return v;}}loadTables();</script></body></html>"#;

async fn list_tables(State(state): State<AppState>) -> Json<Vec<TableInfo>> {
    let tables = state
        .schema
        .models
        .iter()
        .map(|m| TableInfo {
            name: m.name.clone(),
            columns: m.fields.iter().map(|f| f.name.clone()).collect(),
        })
        .collect();
    Json(tables)
}

#[derive(Serialize)]
struct RowsResponse {
    columns: Vec<String>,
    rows: Vec<RowData>,
}

async fn list_rows(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<RowsResponse>, StatusCode> {
    let model = state
        .schema
        .models
        .iter()
        .find(|m| m.name == name)
        .ok_or(StatusCode::NOT_FOUND)?;
    let table = model.name.to_lowercase();
    let columns: Vec<String> = model.fields.iter().map(|f| f.name.clone()).collect();

    let client = state
        .pool
        .get()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let order_expr = if columns.iter().any(|c| c == "id") {
        "id"
    } else {
        "1"
    };
    let sql = format!(
        "SELECT {} FROM {} ORDER BY {} DESC LIMIT 200",
        columns.join(", "),
        table,
        order_expr
    );
    let rows = client
        .query(sql.as_str(), &[])
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut out_rows = Vec::new();
    for row in rows {
        out_rows.push(row_to_rowdata(&model, &row));
    }

    Ok(Json(RowsResponse {
        columns,
        rows: out_rows,
    }))
}

fn row_to_rowdata(model: &byteorm_lib::Model, row: &Row) -> RowData {
    let mut map = HashMap::new();
    let pk_columns: Vec<String> = model
        .fields
        .iter()
        .filter(|f| {
            f.modifiers
                .iter()
                .any(|m| matches!(m, byteorm_lib::Modifier::PrimaryKey))
        })
        .map(|f| f.name.clone())
        .collect();

    for (i, field) in model.fields.iter().enumerate() {
        let col = &field.name;

        let v: serde_json::Value = if let Ok(val) = row.try_get::<_, i64>(i) {
            // For BigInt fields, return as string to avoid JavaScript precision issues
            if field.type_name == "BigInt" {
                serde_json::json!(val.to_string())
            } else {
                serde_json::json!(val)
            }
        } else if let Ok(val) = row.try_get::<_, i32>(i) {
            serde_json::json!(val)
        } else if let Ok(val) = row.try_get::<_, f64>(i) {
            serde_json::json!(val)
        } else if let Ok(val) = row.try_get::<_, bool>(i) {
            serde_json::json!(val)
        } else if let Ok(val) = row.try_get::<_, chrono::NaiveDateTime>(i) {
            serde_json::json!(val.format("%Y-%m-%d %H:%M:%S").to_string())
        } else if let Ok(val) = row.try_get::<_, chrono::DateTime<chrono::Utc>>(i) {
            serde_json::json!(val.to_rfc3339())
        } else if let Ok(val) = row.try_get::<_, serde_json::Value>(i) {
            val
        } else if let Ok(val) = row.try_get::<_, String>(i) {
            serde_json::json!(val)
        } else {
            serde_json::Value::Null
        };

        map.insert(col.clone(), v);
    }
    RowData {
        values: map,
        pk_columns,
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

async fn update_row_handler(
    Path(KeyPath { name, pk }): Path<KeyPath>,
    State(state): State<AppState>,
    Json(payload): Json<UpdatePayload>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let model = match state.schema.models.iter().find(|m| m.name == name) {
        Some(m) => m,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Table '{}' not found", name),
                }),
            ));
        }
    };
    let table = model.name.to_lowercase();

    let pk_data: HashMap<String, serde_json::Value> = serde_json::from_str(&pk).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Invalid PK format: {}", e),
            }),
        )
    })?;

    let mut assignments: Vec<String> = Vec::new();
    #[derive(Debug)]
    enum Param {
        I32(i32),
        I64(i64),
        F64(f64),
        Bool(bool),
        Json(serde_json::Value),
        Str(String),
        NaiveDateTime(chrono::NaiveDateTime),
        DateTimeUtc(chrono::DateTime<chrono::Utc>),
    }
    impl ToSql for Param {
        fn to_sql(
            &self,
            ty: &tokio_postgres::types::Type,
            out: &mut bytes::BytesMut,
        ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
        {
            match self {
                Param::I32(v) => v.to_sql(ty, out),
                Param::I64(v) => v.to_sql(ty, out),
                Param::F64(v) => v.to_sql(ty, out),
                Param::Bool(v) => v.to_sql(ty, out),
                Param::Json(v) => PgJson(v).to_sql(ty, out),
                Param::Str(v) => v.to_sql(ty, out),
                Param::NaiveDateTime(v) => v.to_sql(ty, out),
                Param::DateTimeUtc(v) => v.to_sql(ty, out),
            }
        }
        fn accepts(ty: &tokio_postgres::types::Type) -> bool {
            true
        }
        tokio_postgres::types::to_sql_checked!();
    }

    let mut typed_params: Vec<Param> = Vec::new();
    let mut param_index = 1;
    for field in &model.fields {
        let col = &field.name;
        if field
            .modifiers
            .iter()
            .any(|m| matches!(m, byteorm_lib::Modifier::PrimaryKey))
        {
            continue;
        }
        if let Some(value) = payload.values.get(col) {
            // Check if this is a null timestamp that should be skipped
            let should_skip = matches!(
                field.type_name.as_str(),
                "Timestamp" | "TimestamptZ" | "DateTime"
            ) && matches!(value, serde_json::Value::Null);

            if !should_skip {
                assignments.push(format!("{} = ${}", col, param_index));
                param_index += 1;
                match field.type_name.as_str() {
                    "Int" | "Serial" => {
                        let v = match value {
                            serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) as i32,
                            serde_json::Value::String(s) => s.parse::<i32>().unwrap_or(0),
                            serde_json::Value::Bool(b) => {
                                if *b {
                                    1
                                } else {
                                    0
                                }
                            }
                            _ => 0,
                        };
                        typed_params.push(Param::I32(v));
                    }
                    "BigInt" => {
                        let v = match value {
                            serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
                            serde_json::Value::String(s) => s.parse::<i64>().unwrap_or(0),
                            serde_json::Value::Bool(b) => {
                                if *b {
                                    1
                                } else {
                                    0
                                }
                            }
                            _ => 0,
                        };
                        typed_params.push(Param::I64(v));
                    }
                    "Real" => {
                        let v = match value {
                            serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
                            serde_json::Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                            _ => 0.0,
                        };
                        typed_params.push(Param::F64(v));
                    }
                    "Boolean" => {
                        let v = match value {
                            serde_json::Value::Bool(b) => *b,
                            serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) != 0,
                            serde_json::Value::String(s) => {
                                matches!(s.as_str(), "true" | "1" | "t" | "yes")
                            }
                            _ => false,
                        };
                        typed_params.push(Param::Bool(v));
                    }
                    "Timestamp" | "TimestamptZ" | "DateTime" => {
                        let dt = match value {
                            serde_json::Value::String(s) if !s.trim().is_empty() => {
                                if let Ok(dt) =
                                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                {
                                    Param::NaiveDateTime(dt)
                                } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                                    Param::DateTimeUtc(dt.with_timezone(&chrono::Utc))
                                } else {
                                    return Err((
                                        StatusCode::BAD_REQUEST,
                                        Json(ErrorResponse {
                                            error: format!(
                                                "Invalid timestamp format for field {}: {}",
                                                col, s
                                            ),
                                        }),
                                    ));
                                }
                            }
                            _ => {
                                return Err((
                                    StatusCode::BAD_REQUEST,
                                    Json(ErrorResponse {
                                        error: format!("Invalid type for timestamp field {}", col),
                                    }),
                                ));
                            }
                        };
                        typed_params.push(dt);
                    }
                    "JsonB" => {
                        typed_params.push(Param::Json(value.clone()));
                    }
                    _ => {
                        let s = match value {
                            serde_json::Value::String(s) => s.clone(),
                            _ => value.to_string(),
                        };
                        typed_params.push(Param::Str(s));
                    }
                }
            }
        }
    }
    if assignments.is_empty() {
        return Ok(StatusCode::OK);
    }

    let pk_fields: Vec<_> = model
        .fields
        .iter()
        .filter(|f| {
            f.modifiers
                .iter()
                .any(|m| matches!(m, byteorm_lib::Modifier::PrimaryKey))
        })
        .collect();
    let mut where_clauses = Vec::new();
    for pk_field in &pk_fields {
        let col = &pk_field.name;
        let pk_value = pk_data.get(col).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Missing PK field: {}", col),
                }),
            )
        })?;
        where_clauses.push(format!("{} = ${}", col, param_index));
        param_index += 1;
        let param = match pk_field.type_name.as_str() {
            "Int" | "Serial" => {
                let v = match pk_value {
                    serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) as i32,
                    serde_json::Value::String(s) => s.parse::<i32>().map_err(|e| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid Int value for PK {}: {}", col, e),
                            }),
                        )
                    })?,
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid type for PK field {}", col),
                            }),
                        ));
                    }
                };
                Param::I32(v)
            }
            "BigInt" => {
                let v = match pk_value {
                    serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
                    serde_json::Value::String(s) => s.parse::<i64>().map_err(|e| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid BigInt value for PK {}: {}", col, e),
                            }),
                        )
                    })?,
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid type for PK field {}", col),
                            }),
                        ));
                    }
                };
                Param::I64(v)
            }
            "Real" => {
                let v = match pk_value {
                    serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
                    serde_json::Value::String(s) => s.parse::<f64>().map_err(|e| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid Real value for PK {}: {}", col, e),
                            }),
                        )
                    })?,
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid type for PK field {}", col),
                            }),
                        ));
                    }
                };
                Param::F64(v)
            }
            "Boolean" => {
                let v = match pk_value {
                    serde_json::Value::Bool(b) => *b,
                    serde_json::Value::String(s) => {
                        matches!(s.as_str(), "true" | "1" | "t" | "yes")
                    }
                    _ => false,
                };
                Param::Bool(v)
            }
            "Timestamp" | "TimestamptZ" | "DateTime" => {
                let dt = match pk_value {
                    serde_json::Value::String(s) if !s.trim().is_empty() => {
                        if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        {
                            Param::NaiveDateTime(dt)
                        } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                            Param::DateTimeUtc(dt.with_timezone(&chrono::Utc))
                        } else {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                Json(ErrorResponse {
                                    error: format!(
                                        "Invalid timestamp format for PK field {}: {}",
                                        col, s
                                    ),
                                }),
                            ));
                        }
                    }
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid type for timestamp PK field {}", col),
                            }),
                        ));
                    }
                };
                dt
            }
            _ => {
                let s = match pk_value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => pk_value.to_string(),
                };
                Param::Str(s)
            }
        };
        typed_params.push(param);
    }

    let returning_cols: Vec<String> = pk_fields.iter().map(|f| f.name.clone()).collect();
    let sql = format!(
        "UPDATE {} SET {} WHERE {} RETURNING {}",
        table,
        assignments.join(","),
        where_clauses.join(" AND "),
        returning_cols.join(", ")
    );
    eprintln!("Update SQL: {}", sql);
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Database connection error: {}", e),
                }),
            ));
        }
    };

    let boxed: Vec<&(dyn ToSql + Sync)> = typed_params
        .iter()
        .map(|p| p as &(dyn ToSql + Sync))
        .collect();

    match client.query(sql.as_str(), &boxed).await {
        Ok(rows) => {
            if rows.is_empty() {
                Err((
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("Row with PK={} not found", pk),
                    }),
                ))
            } else {
                Ok(StatusCode::OK)
            }
        }
        Err(e) => {
            eprintln!("Update error: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Database error: {}", e),
                }),
            ))
        }
    }
}
async fn delete_row_handler(
    Path(KeyPath { name, pk }): Path<KeyPath>,
    State(state): State<AppState>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let model = match state.schema.models.iter().find(|m| m.name == name) {
        Some(m) => m,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Table '{}' not found", name),
                }),
            ));
        }
    };
    let table = model.name.to_lowercase();

    let pk_data: HashMap<String, serde_json::Value> = serde_json::from_str(&pk).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Invalid PK format: {}", e),
            }),
        )
    })?;

    #[derive(Debug)]
    enum Param {
        I32(i32),
        I64(i64),
        F64(f64),
        Bool(bool),
        Str(String),
        NaiveDateTime(chrono::NaiveDateTime),
        DateTimeUtc(chrono::DateTime<chrono::Utc>),
    }
    impl ToSql for Param {
        fn to_sql(
            &self,
            ty: &tokio_postgres::types::Type,
            out: &mut bytes::BytesMut,
        ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
        {
            match self {
                Param::I32(v) => v.to_sql(ty, out),
                Param::I64(v) => v.to_sql(ty, out),
                Param::F64(v) => v.to_sql(ty, out),
                Param::Bool(v) => v.to_sql(ty, out),
                Param::Str(v) => v.to_sql(ty, out),
                Param::NaiveDateTime(v) => v.to_sql(ty, out),
                Param::DateTimeUtc(v) => v.to_sql(ty, out),
            }
        }
        fn accepts(ty: &tokio_postgres::types::Type) -> bool {
            true
        }
        tokio_postgres::types::to_sql_checked!();
    }

    let pk_fields: Vec<_> = model
        .fields
        .iter()
        .filter(|f| {
            f.modifiers
                .iter()
                .any(|m| matches!(m, byteorm_lib::Modifier::PrimaryKey))
        })
        .collect();
    let mut where_clauses = Vec::new();
    let mut typed_params: Vec<Param> = Vec::new();
    let mut param_index = 1;

    for pk_field in &pk_fields {
        let col = &pk_field.name;
        let pk_value = pk_data.get(col).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Missing PK field: {}", col),
                }),
            )
        })?;
        where_clauses.push(format!("{} = ${}", col, param_index));
        param_index += 1;
        let param = match pk_field.type_name.as_str() {
            "Int" | "Serial" => {
                let v = match pk_value {
                    serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) as i32,
                    serde_json::Value::String(s) => s.parse::<i32>().map_err(|e| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid Int value for PK {}: {}", col, e),
                            }),
                        )
                    })?,
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid type for PK field {}", col),
                            }),
                        ));
                    }
                };
                Param::I32(v)
            }
            "BigInt" => {
                let v = match pk_value {
                    serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
                    serde_json::Value::String(s) => s.parse::<i64>().map_err(|e| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid BigInt value for PK {}: {}", col, e),
                            }),
                        )
                    })?,
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid type for PK field {}", col),
                            }),
                        ));
                    }
                };
                Param::I64(v)
            }
            "Real" => {
                let v = match pk_value {
                    serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
                    serde_json::Value::String(s) => s.parse::<f64>().map_err(|e| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid Real value for PK {}: {}", col, e),
                            }),
                        )
                    })?,
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid type for PK field {}", col),
                            }),
                        ));
                    }
                };
                Param::F64(v)
            }
            "Boolean" => {
                let v = match pk_value {
                    serde_json::Value::Bool(b) => *b,
                    serde_json::Value::String(s) => {
                        matches!(s.as_str(), "true" | "1" | "t" | "yes")
                    }
                    _ => false,
                };
                Param::Bool(v)
            }
            "Timestamp" | "TimestamptZ" | "DateTime" => {
                let dt = match pk_value {
                    serde_json::Value::String(s) if !s.trim().is_empty() => {
                        if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        {
                            Param::NaiveDateTime(dt)
                        } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                            Param::DateTimeUtc(dt.with_timezone(&chrono::Utc))
                        } else {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                Json(ErrorResponse {
                                    error: format!(
                                        "Invalid timestamp format for PK field {}: {}",
                                        col, s
                                    ),
                                }),
                            ));
                        }
                    }
                    _ => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!("Invalid type for timestamp PK field {}", col),
                            }),
                        ));
                    }
                };
                dt
            }
            _ => {
                let s = match pk_value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => pk_value.to_string(),
                };
                Param::Str(s)
            }
        };
        typed_params.push(param);
    }

    let returning_cols: Vec<String> = pk_fields.iter().map(|f| f.name.clone()).collect();
    let sql = format!(
        "DELETE FROM {} WHERE {} RETURNING {}",
        table,
        where_clauses.join(" AND "),
        returning_cols.join(", ")
    );
    eprintln!("Delete SQL: {}", sql);
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Database connection error: {}", e),
                }),
            ));
        }
    };

    let boxed: Vec<&(dyn ToSql + Sync)> = typed_params
        .iter()
        .map(|p| p as &(dyn ToSql + Sync))
        .collect();

    match client.query(sql.as_str(), &boxed).await {
        Ok(rows) => {
            if rows.is_empty() {
                Err((
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("Row with PK={} not found", pk),
                    }),
                ))
            } else {
                Ok(StatusCode::OK)
            }
        }
        Err(e) => {
            eprintln!("Delete error: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Database error: {}", e),
                }),
            ))
        }
    }
}
