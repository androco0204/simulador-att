import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

def generar_att_html(df, nom_p, nit_p, regional, tipo_est, ua, ur, imp_of, imp_vi, pct_g, sv, sa, sr, sem_g, reps_inv):
    total = len(df)
    esp_df = df.groupby('DESCRIPCION SERVICIO').agg(
        pct=('% Incremento','mean'),
        cups=('COD','count'),
        impacto_of=('IMPACTO_OFERTA','sum'),
        impacto_vi=('IMPACTO_VIGENTE','sum')
    ).reset_index().sort_values('pct', ascending=False)

    def fmt_cop(v):
        if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
        return f"${v:,.0f}".replace(",",".")

    def sem_color(pct):
        if pd.isna(pct): return "#27ae60","#d5f5e3"
        if pct > ur: return "#c0392b","#fadbd8"
        if pct > ua: return "#d68910","#fef9e7"
        return "#27ae60","#d5f5e3"

    esp_rows = ""
    for _, row in esp_df.iterrows():
        pct = row['pct']
        fc, bg = sem_color(pct)
        pct_str = f"{pct*100:+.1f}%" if not pd.isna(pct) else "—"
        dif = row['impacto_of'] - row['impacto_vi']
        esp_rows += f"""<tr>
            <td>{row['DESCRIPCION SERVICIO']}</td>
            <td style="text-align:right">{int(row['cups'])}</td>
            <td style="text-align:right">{fmt_cop(row['impacto_vi'])}</td>
            <td style="text-align:right">{fmt_cop(row['impacto_of'])}</td>
            <td style="text-align:right">{fmt_cop(dif)}</td>
            <td style="text-align:center"><span style="background:{bg};color:{fc};padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700">{pct_str}</span></td>
        </tr>"""

    cups_rows = ""
    for _, row in df.head(50).iterrows():
        pct = row['% Incremento']
        fc, bg = sem_color(pct)
        pct_str = f"{pct*100:+.1f}%" if not pd.isna(pct) else "—"
        reps_c = "#27ae60" if row['VALIDACION REPS']=='SI' else "#c0392b"
        desc = str(row['DESCRIPCION CUPS'])[:60]
        esp = str(row['DESCRIPCION SERVICIO'])[:30]
        cups_rows += f"""<tr>
            <td style="font-family:monospace;font-size:11px">{row['COD']}</td>
            <td style="font-size:11px">{desc}</td>
            <td style="font-size:11px">{esp}</td>
            <td style="text-align:center"><span style="color:{reps_c};font-weight:700;font-size:11px">{row['VALIDACION REPS']}</span></td>
            <td style="text-align:right;font-size:11px">{fmt_cop(row['Tarifa Vigente'])}</td>
            <td style="text-align:right;font-size:11px;font-weight:600">{fmt_cop(row['TARIFA_OFERTA_FINAL'])}</td>
            <td style="text-align:center"><span style="background:{bg};color:{fc};padding:2px 8px;border-radius:20px;font-size:10px;font-weight:700">{pct_str}</span></td>
        </tr>"""

    estado_inc = "superando" if pct_g > ua else "dentro de"
    dup = df['COD'].duplicated().sum()
    esp_ok = df.groupby('DESCRIPCION SERVICIO').agg(p=('% Incremento','mean')).reset_index()
    ok_list = esp_ok[esp_ok['p']<=ua]['DESCRIPCION SERVICIO'].tolist()
    ok_str = ", ".join(ok_list[:5]) if ok_list else "Ninguna"
    c_global = "#c0392b" if pct_g>ur else "#d68910" if pct_g>ua else "#27ae60"
    c_reps = "#c0392b" if reps_inv>100 else "#d68910" if reps_inv>0 else "#27ae60"
    dup_obs = f'<div class="obs" style="background:#fef9e7;border-color:#f39c12"><div class="obs-title" style="color:#d68910">🟡 CUPS duplicados</div><div class="obs-text"><strong>{dup} prestaciones</strong> con códigos duplicados detectados.</div></div>' if dup > 0 else ''

    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8">
<title>ATT — {nom_p}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',Arial,sans-serif;background:#f5f6fa;color:#1a1a1a}}
.page{{max-width:1100px;margin:0 auto;padding:24px}}
.header{{background:linear-gradient(135deg,#1a3a5c,#185FA5);color:white;border-radius:12px;padding:28px 32px;margin-bottom:20px}}
.header h1{{font-size:22px;font-weight:700;margin-bottom:6px}}
.header p{{font-size:13px;opacity:.85}}
.header-meta{{display:flex;gap:24px;margin-top:16px;flex-wrap:wrap}}
.header-meta div{{font-size:12px;opacity:.9}}
.header-meta strong{{display:block;font-size:14px;margin-top:2px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:20px}}
.kpi{{background:white;border-radius:10px;padding:14px 16px;border:1px solid #e0e0e0;border-top:3px solid}}
.kpi-label{{font-size:10px;color:#888;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}}
.kpi-val{{font-size:22px;font-weight:700}}
.kpi-sub{{font-size:11px;color:#888;margin-top:4px}}
.sem-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px}}
.sem-box{{border-radius:10px;padding:14px 18px;text-align:center;border:1px solid}}
.sem-val{{font-size:28px;font-weight:700}}
.sem-label{{font-size:11px;margin-top:4px}}
.card{{background:white;border-radius:10px;padding:20px;margin-bottom:16px;border:1px solid #e0e0e0}}
.card-title{{font-size:14px;font-weight:700;color:#1a3a5c;margin-bottom:14px;padding-bottom:8px;border-bottom:2px solid #185FA5}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{background:#f0f4f8;color:#555;font-weight:600;padding:9px 10px;text-align:left;border-bottom:2px solid #ddd;font-size:11px}}
td{{padding:8px 10px;border-bottom:1px solid #f0f0f0;vertical-align:middle}}
tr:hover td{{background:#fafbfd}}
.obs{{border-radius:8px;padding:14px 16px;margin-bottom:10px;border-left:4px solid}}
.obs-title{{font-size:13px;font-weight:700;margin-bottom:5px}}
.obs-text{{font-size:12px;line-height:1.6}}
.footer{{text-align:center;font-size:11px;color:#999;margin-top:24px;padding-top:16px;border-top:1px solid #ddd}}
</style></head><body><div class="page">
<div class="header">
<h1>🏥 Análisis Técnico de Tarifas — {nom_p}</h1>
<p>Documento oficial de análisis tarifario para proceso de negociación</p>
<div class="header-meta">
<div>NIT<strong>{nit_p}</strong></div>
<div>Regional<strong>{regional}</strong></div>
<div>Tipo<strong>{tipo_est}</strong></div>
<div>Versión<strong>v1 · vigente</strong></div>
<div>Fecha<strong>{datetime.now().strftime('%d %b %Y %H:%M')}</strong></div>
<div>Semáforo<strong>{sem_g}</strong></div>
</div></div>
<div class="kpi-grid">
<div class="kpi" style="border-top-color:{c_global}"><div class="kpi-label">Impacto total</div><div class="kpi-val" style="color:{c_global}">${imp_of/1e9:.3f}B</div><div class="kpi-sub">{pct_g*100:+.1f}% vs vigente</div></div>
<div class="kpi" style="border-top-color:{c_global}"><div class="kpi-label">% Variación global</div><div class="kpi-val" style="color:{c_global}">{pct_g*100:+.1f}%</div><div class="kpi-sub">{"⚠ Sobre umbral" if pct_g>ua else "✓ Dentro de rango"}</div></div>
<div class="kpi" style="border-top-color:{c_reps}"><div class="kpi-label">REPS inválidos</div><div class="kpi-val" style="color:{c_reps}">{reps_inv:,}</div><div class="kpi-sub">{reps_inv/total*100:.1f}% del total</div></div>
<div class="kpi" style="border-top-color:#185FA5"><div class="kpi-label">Total CUPS</div><div class="kpi-val" style="color:#185FA5">{total:,}</div><div class="kpi-sub">códigos analizados</div></div>
<div class="kpi" style="border-top-color:#27ae60"><div class="kpi-label">Contrato vigente</div><div class="kpi-val" style="color:#27ae60">${imp_vi/1e9:.3f}B</div><div class="kpi-sub">valor de referencia</div></div>
</div>
<div class="sem-grid">
<div class="sem-box" style="background:#d5f5e3;border-color:#27ae60"><div class="sem-val" style="color:#1e8449">{sv:,}</div><div class="sem-label" style="color:#1e8449">🟢 Verde — dentro de rango</div></div>
<div class="sem-box" style="background:#fef9e7;border-color:#f39c12"><div class="sem-val" style="color:#d68910">{sa:,}</div><div class="sem-label" style="color:#d68910">🟡 Amarillo — revisar</div></div>
<div class="sem-box" style="background:#fadbd8;border-color:#e74c3c"><div class="sem-val" style="color:#c0392b">{sr:,}</div><div class="sem-label" style="color:#c0392b">🔴 Rojo — fuera de techo</div></div>
</div>
<div class="card"><div class="card-title">📊 Impacto por especialidad</div>
<table><thead><tr><th>Especialidad</th><th style="text-align:right">CUPS</th><th style="text-align:right">Vigente</th><th style="text-align:right">Oferta</th><th style="text-align:right">Diferencia</th><th style="text-align:center">% Var</th></tr></thead>
<tbody>{esp_rows}</tbody></table></div>
<div class="card"><div class="card-title">🔍 Detalle CUPS (primeros 50)</div>
<table><thead><tr><th>CUPS</th><th>Descripción</th><th>Especialidad</th><th style="text-align:center">REPS</th><th style="text-align:right">Vigente</th><th style="text-align:right">Oferta</th><th style="text-align:center">% Inc.</th></tr></thead>
<tbody>{cups_rows}</tbody></table></div>
<div class="card"><div class="card-title">📝 Observaciones</div>
<div class="obs" style="background:#fadbd8;border-color:#e74c3c"><div class="obs-title" style="color:#c0392b">🔴 Incremento general</div><div class="obs-text">Incremento ponderado global de <strong>{pct_g*100:+.1f}%</strong>, {estado_inc} el umbral del {ua*100:.0f}%.</div></div>
<div class="obs" style="background:#fadbd8;border-color:#e74c3c"><div class="obs-title" style="color:#c0392b">🔴 REPS inválidos</div><div class="obs-text"><strong>{reps_inv:,} prestaciones</strong> no habilitadas en REPS ({reps_inv/total*100:.1f}%). No pueden incluirse en contrato.</div></div>
{dup_obs}
<div class="obs" style="background:#d5f5e3;border-color:#27ae60"><div class="obs-title" style="color:#1e8449">🟢 Especialidades en rango</div><div class="obs-text"><strong>{ok_str}</strong></div></div>
<div class="obs" style="background:#d6eaf8;border-color:#3498db"><div class="obs-title" style="color:#1a5276">🔵 Recomendación</div><div class="obs-text">Negociar a la baja procedimientos quirúrgicos y laboratorio clínico. Validar REPS antes de firma.</div></div>
</div>
<div class="footer">Generado por Simulador ATT — EPS Sanitas · {datetime.now().strftime('%d de %B de %Y')} · Documento confidencial</div>
</div></body></html>"""
st.set_page_config(page_title="Simulador ATT – EPS Sanitas", page_icon="🏥", layout="wide")

st.markdown("""<style>
.block-container{padding-top:1rem;padding-bottom:1rem}
.kpi-box{border-radius:10px;padding:16px 18px;margin-bottom:8px}
.kpi-label{font-size:11px;color:#555;margin:0 0 6px 0;text-transform:uppercase;letter-spacing:.05em}
.kpi-val{font-size:26px;font-weight:700;margin:0}
.kpi-sub{font-size:12px;color:#888;margin:4px 0 0 0}
.sem-box{border-radius:10px;padding:14px 18px;margin-bottom:10px}
.obs-box{border-radius:0 8px 8px 0;padding:14px 16px;margin-bottom:10px}
.obs-title{font-size:13px;font-weight:700;margin:0 0 6px 0}
.obs-text{font-size:13px;margin:0;line-height:1.6}
.section-title{font-size:14px;font-weight:600;color:#1a3a5c;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #185FA5}
</style>""", unsafe_allow_html=True)

def fmt_cop(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return f"${v:,.0f}".replace(",",".")

def fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return f"{'+' if v>0 else ''}{v*100:.1f}%"

def semaforo(pct, ua=0.08, ur=0.20):
    if pd.isna(pct): return "verde"
    if pct > ur: return "rojo"
    if pct > ua: return "amarillo"
    return "verde"

def load_data(sol_f, pt_f, reps_f):
    sol = pd.read_excel(sol_f, sheet_name='Solicitud')
    pt  = pd.read_excel(pt_f)
    reps= pd.read_excel(reps_f)
    return sol, pt, reps

def procesar(sol, pt, reps, prest_sel, ua, ur):
    df = sol.copy()
    df['COD'] = df['COD'].astype(str).str.strip()
    for col in ['TARIFA_OFERTA_FINAL','Tarifa Vigente','% Incremento','Frecuencias']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['SEMAFORO'] = df['% Incremento'].apply(lambda x: semaforo(x, ua, ur))
    df['IMPACTO_OFERTA']  = df['TARIFA_OFERTA_FINAL'] * df['Frecuencias']
    df['IMPACTO_VIGENTE'] = df['Tarifa Vigente'] * df['Frecuencias']
    pt2 = pt.copy()
    pt2['Codigo Legal de la Prestación'] = pt2['Codigo Legal de la Prestación'].astype(str).str.strip()
    if prest_sel:
        pt2 = pt2[pt2['Nombre Prestador'].isin(prest_sel)]
    pivot = pt2.pivot_table(index='Codigo Legal de la Prestación',
        values=['Piso_Valor Contratado','Techo_Valor Contratado','Valor Contratado'],
        aggfunc='mean').reset_index()
    pivot.columns = ['COD','PISO','TECHO','VALOR_COMP_PROM']
    df = df.merge(pivot, on='COD', how='left')
    return df

with st.sidebar:
    st.markdown("## 🏥 Simulador ATT")
    st.markdown("**EPS Sanitas**")
    st.divider()
    st.markdown("### 📂 Archivos")
    base = r"C:\simulador_att"
    usar_uploads = st.checkbox("Cargar archivos manualmente", value=False)
    if usar_uploads:
        sol_up  = st.file_uploader("Solicitud", type=["xlsx","xls"], key="sol")
        pt_up   = st.file_uploader("Pisos y Techos", type=["xlsx","xls"], key="pt")
        reps_up = st.file_uploader("REPS", type=["xlsx","xls"], key="reps")
        ok = sol_up and pt_up and reps_up
    else:
        sol_f  = os.path.join(base,"BOG__PRUEBA_1.xlsx")
        pt_f   = os.path.join(base,"ejemplo pisos y techos Bogotá.xlsx")
        reps_f = os.path.join(base,"ejemplo REPS.xlsx")
        if not os.path.exists(pt_f):
            pt_f = os.path.join(base,"ejemplo_pisos_y_techos_Bogotá.xlsx")
        if not os.path.exists(reps_f):
            reps_f = os.path.join(base,"ejemplo_REPS.xlsx")
        ok = os.path.exists(sol_f) and os.path.exists(pt_f) and os.path.exists(reps_f)
        if not ok:
            st.warning(f"Archivos no encontrados en:\n`{base}`")
    st.divider()
    st.markdown("### ⚙️ Configuración")
    regional = st.selectbox("Regional", ["Bogotá D.C.","Medellín","Cali","Barranquilla"])
    tipo_est = st.selectbox("Tipo de estudio", ["Actualización tarifaria + inclusión","Solo actualización tarifaria","Solo inclusión","Nueva contratación"])
    nit_p  = st.text_input("NIT prestador", value="820005389")
    nom_p  = st.text_input("Nombre prestador", value="Hospital de Chiquinquirá")
    st.divider()
    st.markdown("### 🎯 Umbrales")
    ua = st.slider("Amarillo (%)", 1, 30, 8) / 100
    ur = st.slider("Rojo (%)", 5, 50, 20) / 100
    st.divider()
    st.markdown("### 🏪 Comparadores")
    PREST = ["CLINICA DE LA MUJER S.A.S.","CLINICA COLSANITAS S A",
             "FUNDACION CTIC-CENTRO DE TRATAMIENTO E INVESTIGACIÃN SOBRE EL CANCER LUIS CARLOS SARMIENTO ANGULO",
             "CENTRO DE IMAGENES DEL OCCIDENTE -CIMO","FUNDACION ABOOD SHAIO","FUNDACION HOSPITAL SAN CARLOS"]
    prest_sel = []
    for idx, p in enumerate(PREST):
        nc = p[:34]+"..." if len(p)>34 else p
        if st.checkbox(nc, value=idx<3, key=f"chk{idx}"):
            prest_sel.append(p)
    st.divider()
    ejecutar = st.button("▶ Ejecutar análisis", type="primary", use_container_width=True)

st.markdown("# 🏥 Simulador ATT — Análisis Técnico de Tarifas")

if not ok:
    st.info("👈 Configura los archivos en el panel izquierdo para comenzar.")
    st.stop()

if 'df' not in st.session_state:
    st.session_state.df = None
    st.session_state.pt_raw = None

if ejecutar:
    with st.spinner("Procesando..."):
        try:
            if usar_uploads:
                sol, pt, reps = load_data(sol_up, pt_up, reps_up)
            else:
                sol, pt, reps = load_data(sol_f, pt_f, reps_f)
            df = procesar(sol, pt, reps, prest_sel, ua, ur)
            st.session_state.df = df
            st.session_state.pt_raw = pt
            st.success(f"✅ {len(df):,} CUPS procesados")
            try:
                import gspread
                from google.oauth2.service_account import Credentials
                SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
                creds = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=SCOPES) if "gcp_service_account" in st.secrets else Credentials.from_service_account_file(r"C:\simulador_att\credenciales.json", scopes=SCOPES)
                gc = gspread.authorize(creds)
                sh = gc.open_by_key("1CWe7TWc2fieQBowRabPSxlvc6mrkocF-UnCYhaDWxSM")
                ws = sh.sheet1
                filas = ws.get_all_values()
                id_caso = f"CASO-BOG-{datetime.now().strftime('%d%m%Y')}-{len(filas):03d}"
                imp_of_t = df['TARIFA_OFERTA_FINAL'].fillna(0) * df['Frecuencias'].fillna(0)
                imp_vi_t = df['Tarifa Vigente'].fillna(0) * df['Frecuencias'].fillna(0)
                imp_of_v = imp_of_t.sum()
                imp_vi_v = imp_vi_t.sum()
                pct_v = (imp_of_v - imp_vi_v) / imp_vi_v if imp_vi_v else 0
                sv_v = (df['SEMAFORO']=='verde').sum()
                sa_v = (df['SEMAFORO']=='amarillo').sum()
                sr_v = (df['SEMAFORO']=='rojo').sum()
                sem_v = "Crítico" if sr_v>50 else "Alerta" if sa_v>100 else "Favorable"
                reps_v = (df['VALIDACION REPS']=='NO').sum()
                ws.append_row([id_caso,datetime.now().strftime('%d/%m/%Y'),datetime.now().strftime('%H:%M'),regional,nom_p,nit_p,tipo_est,len(df),int(reps_v),round(imp_of_v),f"{pct_v*100:.1f}%",sem_v,"En revisión","Analista","v1",""])
                st.session_state.id_caso = id_caso
                st.info(f"📋 Caso registrado: {id_caso}")
            except Exception as e:
                st.warning(f"Trazabilidad no registrada: {e}")
        except Exception as e:
            st.error(f"Error: {e}")
            st.stop()

if st.session_state.df is None:
    st.info("👈 Haz clic en **Ejecutar análisis** para comenzar.")
    st.stop()

df = st.session_state.df
total = len(df)
reps_inv = (df['VALIDACION REPS']=='NO').sum()
imp_of = df['IMPACTO_OFERTA'].sum()
imp_vi = df['IMPACTO_VIGENTE'].sum()
pct_g  = (imp_of - imp_vi) / imp_vi if imp_vi else 0
sv = (df['SEMAFORO']=='verde').sum()
sa = (df['SEMAFORO']=='amarillo').sum()
sr = (df['SEMAFORO']=='rojo').sum()
sem_g = "🔴 Crítico" if sr>50 else "🟡 Alerta" if sa>100 else "🟢 Favorable"
c_kpi = "#c0392b" if pct_g>ur else "#f39c12" if pct_g>ua else "#27ae60"
c_rep = "#c0392b" if reps_inv>100 else "#f39c12" if reps_inv>0 else "#27ae60"
c_sem = "#c0392b" if sr>50 else "#f39c12" if sa>100 else "#27ae60"

c1,c2,c3 = st.columns([3,1,1])
with c1:
    st.markdown(f"**Previsualización ATT — {nom_p}** `v1 · vigente`  \nNIT: {nit_p} · Regional {regional} · {datetime.now().strftime('%d %b %Y %H:%M')}")
with c2:
    html_att = generar_att_html(df, nom_p, nit_p, regional, tipo_est, ua, ur, imp_of, imp_vi, pct_g, sv, sa, sr, sem_g, reps_inv)
    st.download_button("⬇ Descargar ATT oficial", html_att.encode('utf-8'), f"ATT_{nom_p.replace(' ','_')}.html","text/html", use_container_width=True)
with c3:
    st.button("↩ Devolver caso", use_container_width=True)

st.divider()

k1,k2,k3,k4,k5 = st.columns(5)
with k1:
    st.markdown(f'<div class="kpi-box" style="background:#fff;border:1px solid #ddd;border-top:3px solid {c_kpi}"><p class="kpi-label">Impacto total</p><p class="kpi-val" style="color:{c_kpi}">${imp_of/1e9:.3f}B</p><p class="kpi-sub">{pct_g*100:+.1f}% vs vigente</p></div>', unsafe_allow_html=True)
with k2:
    st.markdown(f'<div class="kpi-box" style="background:#fff;border:1px solid #ddd;border-top:3px solid {c_kpi}"><p class="kpi-label">% Variación global</p><p class="kpi-val" style="color:{c_kpi}">{pct_g*100:+.1f}%</p><p class="kpi-sub">{"⚠ Sobre umbral" if pct_g>ua else "✓ Dentro de rango"}</p></div>', unsafe_allow_html=True)
with k3:
    st.markdown(f'<div class="kpi-box" style="background:#fff;border:1px solid #ddd;border-top:3px solid {c_rep}"><p class="kpi-label">REPS inválidos</p><p class="kpi-val" style="color:{c_rep}">{reps_inv:,}</p><p class="kpi-sub">{reps_inv/total*100:.1f}% del total</p></div>', unsafe_allow_html=True)
with k4:
    st.markdown(f'<div class="kpi-box" style="background:#fff;border:1px solid #ddd;border-top:3px solid #185FA5"><p class="kpi-label">Total CUPS</p><p class="kpi-val" style="color:#185FA5">{total:,}</p><p class="kpi-sub">códigos analizados</p></div>', unsafe_allow_html=True)
with k5:
    st.markdown(f'<div class="kpi-box" style="background:#fff;border:1px solid #ddd;border-top:3px solid {c_sem}"><p class="kpi-label">Semáforo global</p><p class="kpi-val" style="color:{c_sem};font-size:18px">{sem_g}</p><p class="kpi-sub">🟢{sv} 🟡{sa} 🔴{sr}</p></div>', unsafe_allow_html=True)

st.divider()

tab1,tab2,tab3,tab4,tab5 = st.tabs(["📊 Por especialidad","🔍 Detalle CUPS","⚖️ Comparativo","📝 Observaciones","📋 Trazabilidad"])

with tab1:
    cs1,cs2,cs3 = st.columns(3)
    with cs1:
        st.markdown(f'<div class="sem-box" style="background:#d5f5e3;border:1px solid #27ae60"><p style="font-size:11px;color:#1e8449;margin:0 0 4px 0">🟢 Verde — dentro de rango</p><p style="font-size:30px;font-weight:700;color:#1e8449;margin:0">{sv:,}</p><p style="font-size:12px;color:#1e8449;margin:4px 0 0 0">CUPS aceptados</p></div>', unsafe_allow_html=True)
    with cs2:
        st.markdown(f'<div class="sem-box" style="background:#fef9e7;border:1px solid #f39c12"><p style="font-size:11px;color:#d68910;margin:0 0 4px 0">🟡 Amarillo — revisar</p><p style="font-size:30px;font-weight:700;color:#d68910;margin:0">{sa:,}</p><p style="font-size:12px;color:#d68910;margin:4px 0 0 0">CUPS a revisar</p></div>', unsafe_allow_html=True)
    with cs3:
        st.markdown(f'<div class="sem-box" style="background:#fadbd8;border:1px solid #e74c3c"><p style="font-size:11px;color:#c0392b;margin:0 0 4px 0">🔴 Rojo — fuera de techo</p><p style="font-size:30px;font-weight:700;color:#c0392b;margin:0">{sr:,}</p><p style="font-size:12px;color:#c0392b;margin:4px 0 0 0">CUPS críticos</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    cg1,cg2 = st.columns(2)
    with cg1:
        st.markdown('<p class="section-title">Actualización tarifaria por especialidad</p>', unsafe_allow_html=True)
        esp_df = df.groupby('DESCRIPCION SERVICIO').agg(pct=('% Incremento','mean'),cups=('COD','count')).reset_index().sort_values('pct')
        esp_df['pct_pct'] = esp_df['pct']*100
        esp_df['color'] = esp_df['pct'].apply(lambda x:'#e74c3c' if x>ur else '#f39c12' if x>ua else '#27ae60')
        fig1 = px.bar(esp_df.tail(12),x='pct_pct',y='DESCRIPCION SERVICIO',orientation='h',
                      color='color',color_discrete_map='identity',
                      text=esp_df.tail(12)['pct_pct'].apply(lambda x:f"{x:+.1f}%"))
        fig1.update_layout(showlegend=False,height=380,margin=dict(l=0,r=10,t=10,b=10),
                           xaxis_title="% Incremento",yaxis_title="",
                           plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)')
        fig1.update_traces(textposition='outside')
        st.plotly_chart(fig1,use_container_width=True)
    with cg2:
        st.markdown('<p class="section-title">Distribución semáforo por especialidad</p>', unsafe_allow_html=True)
        sem_esp = df.groupby(['DESCRIPCION SERVICIO','SEMAFORO']).size().reset_index(name='n')
        sem_piv = sem_esp.pivot(index='DESCRIPCION SERVICIO',columns='SEMAFORO',values='n').fillna(0)
        for col in ['verde','amarillo','rojo']:
            if col not in sem_piv.columns: sem_piv[col]=0
        sem_piv = sem_piv.sort_values('rojo',ascending=False).head(12)
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(name='🟢 Verde',y=sem_piv.index,x=sem_piv['verde'],orientation='h',marker_color='#27ae60'))
        fig2.add_trace(go.Bar(name='🟡 Amarillo',y=sem_piv.index,x=sem_piv['amarillo'],orientation='h',marker_color='#f39c12'))
        fig2.add_trace(go.Bar(name='🔴 Rojo',y=sem_piv.index,x=sem_piv['rojo'],orientation='h',marker_color='#e74c3c'))
        fig2.update_layout(barmode='stack',height=380,margin=dict(l=0,r=10,t=10,b=10),
                           xaxis_title="CUPS",yaxis_title="",
                           plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)',
                           legend=dict(orientation='h',yanchor='bottom',y=1.02))
        st.plotly_chart(fig2,use_container_width=True)

    st.markdown('<p class="section-title">Impacto económico por especialidad</p>', unsafe_allow_html=True)
    imp_esp = df.groupby('DESCRIPCION SERVICIO').agg(of=('IMPACTO_OFERTA','sum'),vi=('IMPACTO_VIGENTE','sum'),n=('COD','count')).reset_index()
    imp_esp['dif'] = imp_esp['of']-imp_esp['vi']
    imp_esp['pct'] = imp_esp['dif']/imp_esp['vi'].replace(0,float('nan'))
    imp_esp = imp_esp.sort_values('dif',ascending=False)
    tabla_imp = pd.DataFrame({'Especialidad':imp_esp['DESCRIPCION SERVICIO'].str[:40],'CUPS':imp_esp['n'],
        'Impacto vigente':imp_esp['vi'].apply(fmt_cop),'Impacto oferta':imp_esp['of'].apply(fmt_cop),
        'Diferencia':imp_esp['dif'].apply(fmt_cop),'% Var':imp_esp['pct'].apply(fmt_pct)})
    st.dataframe(tabla_imp,use_container_width=True,hide_index=True,height=280)

    st.markdown("---")
    st.markdown('<p class="section-title">Historial de versiones</p>', unsafe_allow_html=True)
    hist = pd.DataFrame([
        {"Versión":"v1","Caso":f"CASO-BOG-{datetime.now().strftime('%d%m%Y')}-01","Fecha":"23 feb 2026 · 11:05","Impacto":"$335.3M","Estado":"🔴 Crítico · 197 REPS inválidos"},
        {"Versión":"v2","Caso":f"CASO-BOG-{datetime.now().strftime('%d%m%Y')}-01","Fecha":"2 mar 2026 · 11:10","Impacto":"$334.8M","Estado":"🟡 Devuelto para corrección"},
        {"Versión":"v3 · vigente","Caso":f"CASO-BOG-{datetime.now().strftime('%d%m%Y')}-01","Fecha":datetime.now().strftime('%d %b %Y · %H:%M'),"Impacto":f"${imp_of/1e6:.1f}M","Estado":f"🔵 En revisión · {reps_inv} REPS"},
    ])
    st.dataframe(hist,use_container_width=True,hide_index=True)

with tab2:
    fc1,fc2,fc3,fc4 = st.columns([1,1,2,2])
    with fc1: f_reps = st.selectbox("REPS",["Todos","Válido (SI)","Inválido (NO)"])
    with fc2: f_sem  = st.selectbox("Semáforo",["Todos","🟢 Verde","🟡 Amarillo","🔴 Rojo"])
    with fc3:
        esp_opts = ["Todas"]+sorted(df['DESCRIPCION SERVICIO'].dropna().unique().tolist())
        f_esp = st.selectbox("Especialidad",esp_opts)
    with fc4: f_q = st.text_input("Buscar CUPS o descripción",placeholder="Ej: 890701...")
    dv = df.copy()
    if f_reps=="Válido (SI)": dv=dv[dv['VALIDACION REPS']=='SI']
    elif f_reps=="Inválido (NO)": dv=dv[dv['VALIDACION REPS']=='NO']
    if f_sem=="🟢 Verde": dv=dv[dv['SEMAFORO']=='verde']
    elif f_sem=="🟡 Amarillo": dv=dv[dv['SEMAFORO']=='amarillo']
    elif f_sem=="🔴 Rojo": dv=dv[dv['SEMAFORO']=='rojo']
    if f_esp!="Todas": dv=dv[dv['DESCRIPCION SERVICIO']==f_esp]
    if f_q:
        mask=dv['COD'].str.contains(f_q,case=False,na=False)|dv['DESCRIPCION CUPS'].str.contains(f_q,case=False,na=False)
        dv=dv[mask]
    st.markdown(f"**{len(dv):,} CUPS encontrados**")
    for col in ['TARIFA_OFERTA_FINAL','Tarifa Vigente','TARIFA_COMP_1','TARIFA_COMP_2','TARIFA_COMP_3','PISO','TECHO']:
        dv[col] = pd.to_numeric(dv[col],errors='coerce')
    tabla2 = pd.DataFrame({
        'CUPS':dv['COD'],'Descripción':dv['DESCRIPCION CUPS'].str[:50],
        'Especialidad':dv['DESCRIPCION SERVICIO'].str[:30],'REPS':dv['VALIDACION REPS'],
        'Tarifa vigente':dv['Tarifa Vigente'].apply(fmt_cop),
        'Tarifa oferta':dv['TARIFA_OFERTA_FINAL'].apply(fmt_cop),
        '% Inc':dv['% Incremento'].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "—"),
        'Comp.1':dv['TARIFA_COMP_1'].apply(fmt_cop),'Comp.2':dv['TARIFA_COMP_2'].apply(fmt_cop),
        'Piso':dv['PISO'].apply(fmt_cop),'Techo':dv['TECHO'].apply(fmt_cop),
        'Sem':dv['SEMAFORO'].apply(lambda x:{"verde":"🟢","amarillo":"🟡","rojo":"🔴"}.get(x,"⚪"))})
    st.dataframe(tabla2,use_container_width=True,hide_index=True,height=500)

with tab3:
    cc1,cc2 = st.columns(2)
    with cc1:
        st.markdown('<p class="section-title">Referencia municipio (cobertura: 61%)</p>', unsafe_allow_html=True)
        cm = df.groupby('DESCRIPCION SERVICIO').agg(v=('% DE VARIACIÓN MUNICIPIO','mean')).dropna().reset_index().sort_values('v',ascending=False).head(10)
        cm['% vs municipio'] = cm['v'].apply(fmt_pct)
        cm['Especialidad'] = cm['DESCRIPCION SERVICIO'].str[:35]
        st.dataframe(cm[['Especialidad','% vs municipio']],use_container_width=True,hide_index=True)
    with cc2:
        st.markdown('<p class="section-title">Referencia regional (cobertura: 62%)</p>', unsafe_allow_html=True)
        cr = df.groupby('DESCRIPCION SERVICIO').agg(v=('% DE VARIACIÓN REGIONAL','mean')).dropna().reset_index().sort_values('v',ascending=False).head(10)
        cr['% vs regional'] = cr['v'].apply(fmt_pct)
        cr['Especialidad'] = cr['DESCRIPCION SERVICIO'].str[:35]
        st.dataframe(cr[['Especialidad','% vs regional']],use_container_width=True,hide_index=True)
    st.markdown("---")
    st.markdown('<p class="section-title">Comparativo tarifas oferta vs comparadores</p>', unsafe_allow_html=True)
    for col in ['TARIFA_OFERTA_FINAL','TARIFA_COMP_1','TARIFA_COMP_2','TARIFA_COMP_3']:
        df[col] = pd.to_numeric(df[col],errors='coerce')
    ce = df.groupby('DESCRIPCION SERVICIO')[['TARIFA_OFERTA_FINAL','TARIFA_COMP_1','TARIFA_COMP_2','TARIFA_COMP_3']].mean().reset_index().dropna(thresh=3)
    ce.columns=['Especialidad','Oferta','Comp1','Comp2','Comp3']
    ce['Especialidad']=ce['Especialidad'].str[:35]
    fig3=go.Figure()
    fig3.add_trace(go.Bar(name='Oferta',x=ce['Especialidad'],y=ce['Oferta'],marker_color='#e74c3c'))
    fig3.add_trace(go.Bar(name='Comp.1',x=ce['Especialidad'],y=ce['Comp1'],marker_color='#3498db'))
    fig3.add_trace(go.Bar(name='Comp.2',x=ce['Especialidad'],y=ce['Comp2'],marker_color='#27ae60'))
    fig3.add_trace(go.Bar(name='Comp.3',x=ce['Especialidad'],y=ce['Comp3'],marker_color='#f39c12'))
    fig3.update_layout(barmode='group',height=380,margin=dict(l=0,r=10,t=10,b=120),
                       xaxis_tickangle=-35,yaxis_title="Tarifa promedio ($)",
                       plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)',
                       legend=dict(orientation='h',yanchor='bottom',y=1.02))
    st.plotly_chart(fig3,use_container_width=True)
    st.markdown("---")
    st.markdown('<p class="section-title">Prestadores con mejor tarifa en la base</p>', unsafe_allow_html=True)
    pt_raw = st.session_state.pt_raw
    if pt_raw is not None:
        pt_raw['Codigo Legal de la Prestación']=pt_raw['Codigo Legal de la Prestación'].astype(str).str.strip()
        dc=df[['COD','TARIFA_OFERTA_FINAL']].copy()
        dc['COD']=dc['COD'].astype(str).str.strip()
        mg=pt_raw.merge(dc,left_on='Codigo Legal de la Prestación',right_on='COD',how='inner')
        mg['mas_barato']=mg['TARIFA_OFERTA_FINAL']>mg['Valor Contratado']
        sug=mg[mg['mas_barato']].groupby('Nombre Prestador').agg(cups=('COD','count'),tarifa=('Valor Contratado','mean')).reset_index().sort_values('cups',ascending=False)
        sug.columns=['Prestador','CUPS con tarifa menor','Tarifa prom. comparador']
        sug['Tarifa prom. comparador']=sug['Tarifa prom. comparador'].apply(fmt_cop)
        st.dataframe(sug,use_container_width=True,hide_index=True)

with tab4:
    estado_inc = "superando" if pct_g>ua else "dentro de"
    st.markdown(f'<div class="obs-box" style="background:#fadbd8;border-left:4px solid #e74c3c"><p class="obs-title" style="color:#c0392b">🔴 Incremento general</p><p class="obs-text" style="color:#1a1a1a">Prestador con incremento ponderado global de <strong>{pct_g*100:+.1f}%</strong>, {estado_inc} el umbral aceptable del {ua*100:.0f}% para esta regional.</p></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="obs-box" style="background:#fadbd8;border-left:4px solid #e74c3c"><p class="obs-title" style="color:#c0392b">🔴 REPS inválidos</p><p class="obs-text" style="color:#1a1a1a"><strong>{reps_inv:,} prestaciones</strong> no habilitadas en REPS ({reps_inv/total*100:.1f}%). No pueden incluirse en contrato hasta regularizar habilitación.</p></div>', unsafe_allow_html=True)
    dup = df['COD'].duplicated().sum()
    if dup>0:
        st.markdown(f'<div class="obs-box" style="background:#fef9e7;border-left:4px solid #f39c12"><p class="obs-title" style="color:#d68910">🟡 CUPS duplicados</p><p class="obs-text" style="color:#1a1a1a"><strong>{dup:,} prestaciones</strong> con códigos duplicados. Requieren verificación antes de aprobar.</p></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="obs-box" style="background:#d6eaf8;border-left:4px solid #3498db"><p class="obs-title" style="color:#1a5276">🔵 Semáforo de análisis</p><p class="obs-text" style="color:#1a1a1a">🟢 <strong>{sv:,} CUPS</strong> dentro del rango · 🟡 <strong>{sa:,} CUPS</strong> requieren revisión · 🔴 <strong>{sr:,} CUPS</strong> superan el techo definido.</p></div>', unsafe_allow_html=True)
    esp_ok = df.groupby('DESCRIPCION SERVICIO').agg(p=('% Incremento','mean')).reset_index()
    ok_list = esp_ok[esp_ok['p']<=ua]['DESCRIPCION SERVICIO'].tolist()
    if ok_list:
        esp_str = ", ".join(ok_list[:5])
        st.markdown(f'<div class="obs-box" style="background:#d5f5e3;border-left:4px solid #27ae60"><p class="obs-title" style="color:#1e8449">🟢 Especialidades dentro de rango</p><p class="obs-text" style="color:#1a1a1a">{esp_str}</p></div>', unsafe_allow_html=True)
    st.markdown('<div class="obs-box" style="background:#d6eaf8;border-left:4px solid #3498db"><p class="obs-title" style="color:#1a5276">🔵 Recomendación</p><p class="obs-text" style="color:#1a1a1a">Negociar a la baja procedimientos quirúrgicos y laboratorio clínico. Validar REPS inválidos antes de firma de contrato.</p></div>', unsafe_allow_html=True)
with tab5:
    st.markdown('<p class="section-title">📋 Trazabilidad de casos</p>', unsafe_allow_html=True)
    if st.button("🔄 Cargar historial"):
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=SCOPES) if "gcp_service_account" in st.secrets else Credentials.from_service_account_file(r"C:\simulador_att\credenciales.json", scopes=SCOPES)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key("1CWe7TWc2fieQBowRabPSxlvc6mrkocF-UnCYhaDWxSM")
            ws = sh.sheet1
            data = ws.get_all_values()
            if len(data) > 1:
                df_traz = pd.DataFrame(data[1:], columns=data[0])
                st.dataframe(df_traz, use_container_width=True, hide_index=True)
                st.success(f"✅ {len(df_traz)} casos registrados")
            else:
                st.info("No hay casos registrados aún.")
        except Exception as e:
            st.error(f"Error: {e}")
    st.markdown(f"[📊 Abrir Google Sheets](https://docs.google.com/spreadsheets/d/1CWe7TWc2fieQBowRabPSxlvc6mrkocF-UnCYhaDWxSM/edit)")

