import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import io
import sqlite3
import hashlib

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
SHEETS_ID = "1CWe7TWc2fieQBowRabPSxlvc6mrkocF-UnCYhaDWxSM"

# ── BASE DE DATOS — CONECTOR DUAL SQLite / SQL Server ─────────────────────────
# Para producción con SQL Server, define variables de entorno:
#   SIMULADOR_DB=sqlserver
#   SIMULADOR_SQLSERVER_CONN=DRIVER={ODBC Driver 17 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...
# Sin esas variables el simulador usa SQLite automáticamente (desarrollo/demo).

_DB_BACKEND = os.environ.get("SIMULADOR_DB", "sqlite").lower()
_SQLSERVER_CONN_STR = os.environ.get(
    "SIMULADOR_SQLSERVER_CONN",
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=;DATABASE=simulador_att;UID=;PWD="
)
DB_PATH = r"C:\simulador_att\simulador_att.db"

def _get_db_conn():
    """Retorna conexión activa: SQL Server en producción, SQLite en desarrollo."""
    if _DB_BACKEND == "sqlserver":
        try:
            import pyodbc
            return pyodbc.connect(_SQLSERVER_CONN_STR, timeout=10)
        except Exception as _e:
            st.warning(f"SQL Server no disponible ({_e}). Usando SQLite como respaldo.")
    return _get_db_conn()

# ── AUTENTICACIÓN ─────────────────────────────────────────────────────────────
DOMINIOS_PERMITIDOS = ["keralty.com", "sanitas.com.co", "colsanitas.com", "epsanitas.com", "gmail.com"]
USUARIOS_ADMIN = ["admin@keralty.com", "admin@sanitas.com.co"]  # Agregar admins aquí

# ── UMBRALES POR DEFECTO (solo admin puede cambiarlos) ────────────────────────
UA_DEFAULT = 0.08  # 8% amarillo
UR_DEFAULT = 0.20  # 20% rojo
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
CRED_PATH = r"C:\simulador_att\credenciales.json"

# IDs de carpetas en Google Drive
FOLDERS = {
    "REPS":                   "1VGIZuKV9fYUXjNSdth1-uB5KP6z2HhJE",
    "Pisos_y_Techos":         "1XwJCKbKV6KA1ZHl8aLUoOxFaOgSs7TFG",
    "Convenios_Vigentes":     "1AeXL3yvUS7xMBiDOgOaqONdtYQsMEdIL",
    "Homologacion":           "163pJJLHc3h90P1PBd5p545OZi8UG0NNe",
    "Costo_medio_evento":     "1m34HUGPpsJx4xfGKjwt9jOv-ht7zkyAO",
    "Tabla_QX":               "1MxyLNDgR_A4MSXiw14sLoLV9soTM4o33",
    "Insumos_Dispositivos":   "1ZDVzRH1ZR1wY8rRwl0NI-PahAO6QgeVj",
    "Casuistica_Poblacional": "1gJth0DDMQLrX12AfofTkTLZm8zmCbgH2",
    "Medicamentos":           "1v-hFHukE5W77mi3fQTIMN8Bjnlx53mtA",
    "Parametrico":            "1MThBiRkvnAN-3ZBFS8R7jvcX8K-yxvkm",
    "Solicitudes":            "1xarjBaq3W6o4WgkaciSnPO1I-hujlsiu",
    # Carpetas por regional en 02_Solicitudes
    "SOL_BOG":  "1D1n-hcDZhw5KuVUXz8wZHqX1fOCbC6aj",
    "SOL_MED":  "1721rEGzbSqw09Npst_mqiTEOJisbCkQd",
    "SOL_CAL":  "1nXUQXcYS0bSvHIZTx-fGe8VsJpOTRCqQ",
    "SOL_BAR":  "1fNEPtLhhWuf8gmDSNmjkyQQNi8fK8vC7",
    "SOL_BUC":  "1uVGq_6zWzPImjBKak7TjjPVl3RNqiy9W",
    "SOL_COR":  "1RXHeQ_GaKaXn7efiOSj0WSi-PYEGBfgR",
}

# Mapa regional → clave de carpeta
REGIONAL_FOLDER = {
    "Bogotá D.C.":    "SOL_BOG",
    "Medellín":       "SOL_MED",
    "Cali":           "SOL_CAL",
    "Barranquilla":   "SOL_BAR",
    "Bucaramanga":    "SOL_BUC",
    "Centro Oriente": "SOL_COR",
}
# Carpeta raíz base (legacy)
DRIVE_FOLDER_ID = FOLDERS["Pisos_y_Techos"]

COLUMNAS_OBLIGATORIAS = {
    'ID_LINEA': 'A',
    'ID_CASO': 'B',
    'TIPO': 'C',
    'COD': 'I',
    'DESCRIPCION COD (Res Vigente)': 'J',
    'DESCRIPCION CUPS': 'N',
    'DESCRIPCION SERVICIO': 'O',
    'UVR/VALOR': 'S',
    'TARIFA DEFINIDA': 'T',
    'TARIFA_OFERTA_FINAL': 'Z',
    'Tipo de solicitud': 'AB',
    'Frecuencias': 'AE',
    'Tarifa Vigente': 'AF'
}

COLUMNAS_ESPERADAS = [
    'ID_LINEA','ID_CASO','TIPO','COD REPS','VALIDACION REPS','SUGERENCIA REGIONAL',
    'INTERDEPENDECIA','CODIGO DE HABILITACION - INTERDEPENDENCIA','COD',
    'DESCRIPCION COD (Res Vigente)','COD PSS','DESCRIPCION PSS','COD EPS',
    'DESCRIPCION CUPS','DESCRIPCION SERVICIO','HABILITA CONCEPTOS',
    'COD HOMOLOGO Manual tarifario','GRUPO_SALARIOS','UVR/VALOR','TARIFA DEFINIDA',
    'HONORARIO','ANESTESIA','AYUDANTIA','D SALA','MATERIALES','TARIFA_OFERTA_FINAL',
    'OBSERVACIONES','Tipo de solicitud','Facturación a Tarifas Vigentes','Frecuencias',
    'Tarifa Vigente','Nombre de Tarifa Vigente','Facturación a Tarifas Propuestas'
]

# ── CREDENCIALES GOOGLE ───────────────────────────────────────────────────────
def get_credentials():
    from google.oauth2.service_account import Credentials
    try:
        if "gcp_service_account" in st.secrets:
            return Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=SCOPES)
    except:
        pass
    return Credentials.from_service_account_file(CRED_PATH, scopes=SCOPES)

def get_google_client():
    import gspread
    return gspread.authorize(get_credentials())

# ── BASE DE DATOS SQLite ──────────────────────────────────────────────────────
def init_db():
    """Inicializa la base de datos SQLite con las tablas necesarias"""
    conn = _get_db_conn()
    c = conn.cursor()
    # Tabla de trazabilidad
    c.execute("""CREATE TABLE IF NOT EXISTS trazabilidad (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_caso TEXT UNIQUE,
        fecha TEXT, hora TEXT, regional TEXT,
        prestador TEXT, nit TEXT, tipo_estudio TEXT,
        total_cups INTEGER, reps_invalidos INTEGER,
        impacto_total REAL, pct_variacion TEXT,
        semaforo TEXT, estado TEXT, usuario TEXT,
        version TEXT, clasificacion TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Tabla de usuarios y sesiones
    c.execute("""CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE, nombre TEXT,
        regional TEXT, rol TEXT DEFAULT 'analista',
        activo INTEGER DEFAULT 1,
        ultimo_acceso TIMESTAMP
    )""")
    # Tabla de configuración de umbrales por regional
    c.execute("""CREATE TABLE IF NOT EXISTS config_umbrales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        regional TEXT, especialidad TEXT DEFAULT 'Global',
        umbral_amarillo REAL DEFAULT 0.08,
        umbral_rojo REAL DEFAULT 0.20,
        actualizado_por TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Tabla de caché de bases
    c.execute("""CREATE TABLE IF NOT EXISTS cache_bases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre_base TEXT, regional TEXT,
        registros INTEGER, fecha_archivo TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()

def registrar_trazabilidad_db(id_caso, nom_p, nit_p, regional, tipo_est, total,
                               reps_inv, imp_of, pct_g, sem_g, clasificacion,
                               usuario="Analista", version="v1"):
    """Registra el análisis en SQLite"""
    try:
        conn = _get_db_conn()
        c = conn.cursor()
        c.execute("""INSERT OR REPLACE INTO trazabilidad
            (id_caso, fecha, hora, regional, prestador, nit, tipo_estudio,
             total_cups, reps_invalidos, impacto_total, pct_variacion,
             semaforo, estado, usuario, version, clasificacion)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (id_caso, datetime.now().strftime('%d/%m/%Y'),
             datetime.now().strftime('%H:%M'), regional, nom_p, nit_p,
             tipo_est, total, int(reps_inv), round(imp_of),
             f"{pct_g*100:.1f}%", sem_g, "En revisión", usuario, version, clasificacion))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        return False

def obtener_historial_db(regional=None, limit=100):
    """Obtiene el historial de casos desde SQLite"""
    try:
        conn = _get_db_conn()
        if regional:
            df = pd.read_sql_query(
                "SELECT * FROM trazabilidad WHERE regional=? ORDER BY created_at DESC LIMIT ?",
                conn, params=(regional, limit))
        else:
            df = pd.read_sql_query(
                "SELECT * FROM trazabilidad ORDER BY created_at DESC LIMIT ?",
                conn, params=(limit,))
        conn.close()
        return df
    except:
        return pd.DataFrame()

def obtener_umbrales_db(regional, especialidad="Global"):
    """Obtiene los umbrales configurados para una regional"""
    try:
        conn = _get_db_conn()
        c = conn.cursor()
        c.execute("""SELECT umbral_amarillo, umbral_rojo FROM config_umbrales
                     WHERE regional=? AND especialidad=?
                     ORDER BY updated_at DESC LIMIT 1""", (regional, especialidad))
        row = c.fetchone()
        conn.close()
        if row:
            return row[0], row[1]
    except:
        pass
    return UA_DEFAULT, UR_DEFAULT

def guardar_umbrales_db(regional, ua, ur, usuario, especialidad="Global"):
    """Guarda umbrales en SQLite"""
    try:
        conn = _get_db_conn()
        c = conn.cursor()
        c.execute("""INSERT INTO config_umbrales
                     (regional, especialidad, umbral_amarillo, umbral_rojo, actualizado_por)
                     VALUES (?,?,?,?,?)""", (regional, especialidad, ua, ur, usuario))
        conn.commit()
        conn.close()
        return True
    except:
        return False

# ── AUTENTICACIÓN ──────────────────────────────────────────────────────────────
def verificar_acceso(email):
    """Verifica si el email tiene acceso al simulador"""
    if not email:
        return False
    dominio = email.split("@")[-1].lower()
    return dominio in DOMINIOS_PERMITIDOS

def es_admin(email):
    """Verifica si el usuario es administrador"""
    return email in USUARIOS_ADMIN

def pantalla_login():
    """Muestra la pantalla de login con identidad visual EPS Sanitas / Impulsa"""
    import base64, pathlib

    def img_to_b64(path):
        try:
            return base64.b64encode(pathlib.Path(path).read_bytes()).decode()
        except Exception:
            return ""

    # Intentar cargar logos desde la misma carpeta del script
    _dir = pathlib.Path(__file__).parent
    eps_b64    = img_to_b64(_dir / "EPS.png")
    impulsa_b64 = img_to_b64(_dir / "IMPULSA.png")

    eps_tag     = f'<img src="data:image/png;base64,{eps_b64}" style="height:38px;object-fit:contain">'     if eps_b64    else '<span style="font-weight:800;font-size:18px;color:#0E9ED7;letter-spacing:-0.5px">EPS Sanitas</span>'
    impulsa_tag = f'<img src="data:image/png;base64,{impulsa_b64}" style="height:34px;object-fit:contain">' if impulsa_b64 else '<span style="font-weight:700;font-size:16px;color:#0E9ED7">impulsa</span>'

    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700;800&display=swap');

    [data-testid="stAppViewContainer"] {{
        background-color: #020f1e;
        background-image:
            url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='680' height='420' viewBox='0 0 680 420'%3E%3Cdefs%3E%3ClinearGradient id='bg' x1='0' y1='0' x2='1' y2='1'%3E%3Cstop offset='0%25' stop-color='%23020f1e'/%3E%3Cstop offset='55%25' stop-color='%23051d3a'/%3E%3Cstop offset='100%25' stop-color='%23082d56'/%3E%3C/linearGradient%3E%3ClinearGradient id='bG' x1='0' y1='0' x2='0' y2='1'%3E%3Cstop offset='0%25' stop-color='%230E9ED7' stop-opacity='.85'/%3E%3Cstop offset='100%25' stop-color='%230a78a8' stop-opacity='.3'/%3E%3C/linearGradient%3E%3ClinearGradient id='bG2' x1='0' y1='0' x2='0' y2='1'%3E%3Cstop offset='0%25' stop-color='%2327ae60' stop-opacity='.8'/%3E%3Cstop offset='100%25' stop-color='%231e8449' stop-opacity='.25'/%3E%3C/linearGradient%3E%3ClinearGradient id='bG3' x1='0' y1='0' x2='0' y2='1'%3E%3Cstop offset='0%25' stop-color='%23f39c12' stop-opacity='.8'/%3E%3Cstop offset='100%25' stop-color='%23d68910' stop-opacity='.25'/%3E%3C/linearGradient%3E%3ClinearGradient id='lG' x1='0' y1='0' x2='1' y2='0'%3E%3Cstop offset='0%25' stop-color='%230E9ED7' stop-opacity='0'/%3E%3Cstop offset='20%25' stop-color='%230E9ED7' stop-opacity='.9'/%3E%3Cstop offset='100%25' stop-color='%230E9ED7' stop-opacity='.9'/%3E%3C/linearGradient%3E%3ClinearGradient id='aF' x1='0' y1='0' x2='0' y2='1'%3E%3Cstop offset='0%25' stop-color='%230E9ED7' stop-opacity='.16'/%3E%3Cstop offset='100%25' stop-color='%230E9ED7' stop-opacity='0'/%3E%3C/linearGradient%3E%3CclipPath id='lc'%3E%3Crect x='42' y='66' width='290' height='124'/%3E%3C/clipPath%3E%3CclipPath id='cc'%3E%3Crect x='42' y='200' width='290' height='140'/%3E%3C/clipPath%3E%3C/defs%3E%3Crect width='680' height='420' fill='url(%23bg)'/%3E%3Crect width='680' height='420' fill='url(%23dots)' opacity='.09'/%3E%3Cpattern id='dots' x='0' y='0' width='32' height='32' patternUnits='userSpaceOnUse'%3E%3Ccircle cx='1' cy='1' r='1' fill='%230E9ED7'/%3E%3C/pattern%3E%3Crect width='680' height='420' fill='url(%23dots)' opacity='.09'/%3E%3Crect x='28' y='40' width='318' height='156' rx='10' fill='%230a1e38' stroke='%230E9ED7' stroke-width='.5' stroke-opacity='.3'/%3E%3Ctext x='42' y='62' font-family='sans-serif' font-size='10' font-weight='600' fill='%230E9ED7' opacity='.55' letter-spacing='1.2'%3ETENDENCIA TARIFARIA%3C/text%3E%3Cg stroke='%23ffffff' stroke-opacity='.05' stroke-width='.5'%3E%3Cline x1='42' y1='76' x2='332' y2='76'/%3E%3Cline x1='42' y1='103' x2='332' y2='103'/%3E%3Cline x1='42' y1='130' x2='332' y2='130'/%3E%3Cline x1='42' y1='157' x2='332' y2='157'/%3E%3Cline x1='42' y1='184' x2='332' y2='184'/%3E%3C/g%3E%3Cpolygon clip-path='url(%23lc)' points='50,170 90,155 130,140 170,125 200,110 230,130 260,108 290,92 320,80 330,78 330,190 50,190' fill='url(%23aF)'/%3E%3Cpolyline clip-path='url(%23lc)' points='50,170 90,155 130,140 170,125 200,110 230,130 260,108 290,92 320,80 330,78' fill='none' stroke='url(%23lG)' stroke-width='2' stroke-linejoin='round' stroke-linecap='round'/%3E%3Cg fill='%230E9ED7'%3E%3Ccircle cx='50' cy='170' r='2.5' opacity='.6'/%3E%3Ccircle cx='90' cy='155' r='2.5' opacity='.7'/%3E%3Ccircle cx='130' cy='140' r='2.5' opacity='.8'/%3E%3Ccircle cx='170' cy='125' r='3' opacity='.9'/%3E%3Ccircle cx='200' cy='110' r='3' opacity='.9'/%3E%3Ccircle cx='230' cy='130' r='2.5' opacity='.75'/%3E%3Ccircle cx='260' cy='108' r='3' opacity='.9'/%3E%3Ccircle cx='290' cy='92' r='3' opacity='.95'/%3E%3Ccircle cx='320' cy='80' r='3.5'/%3E%3Ccircle cx='330' cy='78' r='3.5'/%3E%3C/g%3E%3Cline x1='42' y1='118' x2='332' y2='118' stroke='%23f39c12' stroke-width='1' stroke-dasharray='5,4' opacity='.55'/%3E%3Ctext x='334' y='121' font-family='sans-serif' font-size='8' fill='%23f39c12' opacity='.75'%3E8%25%3C/text%3E%3Crect x='28' y='208' width='318' height='148' rx='10' fill='%230a1e38' stroke='%230E9ED7' stroke-width='.5' stroke-opacity='.3'/%3E%3Ctext x='42' y='226' font-family='sans-serif' font-size='10' font-weight='600' fill='%230E9ED7' opacity='.55' letter-spacing='1.2'%3EIMPACTO POR ESPECIALIDAD%3C/text%3E%3Cg clip-path='url(%23cc)'%3E%3Crect x='52' y='312' width='28' height='28' rx='3' fill='url(%23bG)'/%3E%3Crect x='94' y='272' width='28' height='68' rx='3' fill='url(%23bG)'/%3E%3Crect x='136' y='295' width='28' height='45' rx='3' fill='url(%23bG2)'/%3E%3Crect x='178' y='255' width='28' height='85' rx='3' fill='url(%23bG3)'/%3E%3Crect x='220' y='268' width='28' height='72' rx='3' fill='url(%23bG)'/%3E%3Crect x='262' y='306' width='28' height='34' rx='3' fill='url(%23bG2)'/%3E%3Crect x='304' y='288' width='28' height='52' rx='3' fill='url(%23bG)'/%3E%3C/g%3E%3Crect x='360' y='40' width='292' height='316' rx='10' fill='%230a1e38' stroke='%230E9ED7' stroke-width='.5' stroke-opacity='.3'/%3E%3Ctext x='375' y='62' font-family='sans-serif' font-size='10' font-weight='600' fill='%230E9ED7' opacity='.55' letter-spacing='1.2'%3EANÁLISIS TÉCNICO ATT%3C/text%3E%3Crect x='375' y='74' width='262' height='56' rx='8' fill='%230d2444' stroke='%230E9ED7' stroke-width='.4' stroke-opacity='.2'/%3E%3Ccircle cx='398' cy='102' r='10' fill='%23e74c3c' opacity='.85'/%3E%3Ccircle cx='426' cy='102' r='10' fill='%23f39c12' opacity='.45'/%3E%3Ccircle cx='454' cy='102' r='10' fill='%2327ae60' opacity='.28'/%3E%3Ctext x='474' y='97' font-family='sans-serif' font-size='9' fill='%23ffffff' opacity='.4'%3EEstado actual%3C/text%3E%3Ctext x='474' y='113' font-family='sans-serif' font-size='12' font-weight='700' fill='%23e74c3c' opacity='.9'%3ECritico%3C/text%3E%3Crect x='375' y='142' width='80' height='58' rx='7' fill='%230d2444' stroke='%230E9ED7' stroke-width='.4' stroke-opacity='.25'/%3E%3Ctext x='415' y='160' font-family='sans-serif' font-size='8' fill='%23ffffff' opacity='.38' text-anchor='middle'%3ECUPS analizados%3C/text%3E%3Ctext x='415' y='181' font-family='sans-serif' font-size='19' font-weight='700' fill='%230E9ED7' text-anchor='middle' opacity='.9'%3E1,247%3C/text%3E%3Crect x='467' y='142' width='80' height='58' rx='7' fill='%230d2444' stroke='%23f39c12' stroke-width='.4' stroke-opacity='.3'/%3E%3Ctext x='507' y='160' font-family='sans-serif' font-size='8' fill='%23ffffff' opacity='.38' text-anchor='middle'%3E%25 Variacion%3C/text%3E%3Ctext x='507' y='181' font-family='sans-serif' font-size='19' font-weight='700' fill='%23f39c12' text-anchor='middle' opacity='.9'%3E+14.2%25%3C/text%3E%3Crect x='559' y='142' width='80' height='58' rx='7' fill='%230d2444' stroke='%23e74c3c' stroke-width='.4' stroke-opacity='.3'/%3E%3Ctext x='599' y='160' font-family='sans-serif' font-size='8' fill='%23ffffff' opacity='.38' text-anchor='middle'%3EImpacto total%3C/text%3E%3Ctext x='599' y='181' font-family='sans-serif' font-size='16' font-weight='700' fill='%23e74c3c' text-anchor='middle' opacity='.9'%3E%24482M%3C/text%3E%3Ctext x='375' y='220' font-family='sans-serif' font-size='9' fill='%23ffffff' opacity='.3'%3EPareto concentracion de impacto%3C/text%3E%3Crect x='375' y='228' width='262' height='56' rx='6' fill='%23071828' stroke='%230E9ED7' stroke-width='.3' stroke-opacity='.2'/%3E%3Crect x='385' y='236' width='180' height='9' rx='2' fill='%230E9ED7' opacity='.85'/%3E%3Crect x='385' y='250' width='110' height='9' rx='2' fill='%230E9ED7' opacity='.6'/%3E%3Crect x='385' y='264' width='68' height='9' rx='2' fill='%230E9ED7' opacity='.38'/%3E%3Crect x='385' y='278' width='40' height='9' rx='2' fill='%230E9ED7' opacity='.22'/%3E%3Ctext x='568' y='244' font-family='sans-serif' font-size='8' fill='%230E9ED7' opacity='.75' text-anchor='end'%3EOrtopedia 36%25%3C/text%3E%3Ctext x='568' y='258' font-family='sans-serif' font-size='8' fill='%230E9ED7' opacity='.55' text-anchor='end'%3ECirugia 22%25%3C/text%3E%3Ctext x='568' y='272' font-family='sans-serif' font-size='8' fill='%230E9ED7' opacity='.4' text-anchor='end'%3EOncologia 14%25%3C/text%3E%3Ctext x='568' y='286' font-family='sans-serif' font-size='8' fill='%230E9ED7' opacity='.25' text-anchor='end'%3EOtros 28%25%3C/text%3E%3Ctext x='375' y='302' font-family='sans-serif' font-size='9' fill='%23ffffff' opacity='.3'%3EValidacion REPS%3C/text%3E%3Crect x='375' y='308' width='262' height='32' rx='6' fill='%23071828' stroke='%230E9ED7' stroke-width='.3' stroke-opacity='.2'/%3E%3Crect x='385' y='316' width='130' height='8' rx='2' fill='%2327ae60' opacity='.8'/%3E%3Crect x='519' y='316' width='38' height='8' rx='2' fill='%23e74c3c' opacity='.75'/%3E%3Crect x='561' y='316' width='22' height='8' rx='2' fill='%23f39c12' opacity='.65'/%3E%3Cg opacity='.5'%3E%3Crect x='28' y='368' width='88' height='20' rx='10' fill='none' stroke='%230E9ED7' stroke-width='.7' stroke-opacity='.5'/%3E%3Crect x='126' y='368' width='72' height='20' rx='10' fill='none' stroke='%230E9ED7' stroke-width='.7' stroke-opacity='.5'/%3E%3Crect x='208' y='368' width='58' height='20' rx='10' fill='none' stroke='%2327ae60' stroke-width='.7' stroke-opacity='.5'/%3E%3Crect x='276' y='368' width='62' height='20' rx='10' fill='none' stroke='%230E9ED7' stroke-width='.7' stroke-opacity='.4'/%3E%3Crect x='348' y='368' width='76' height='20' rx='10' fill='none' stroke='%23f39c12' stroke-width='.7' stroke-opacity='.45'/%3E%3Crect x='434' y='368' width='72' height='20' rx='10' fill='none' stroke='%230E9ED7' stroke-width='.7' stroke-opacity='.4'/%3E%3Crect x='516' y='368' width='82' height='20' rx='10' fill='none' stroke='%230E9ED7' stroke-width='.7' stroke-opacity='.4'/%3E%3C/g%3E%3C/svg%3E");
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
        background-attachment: fixed;
        min-height: 100vh;
    }}
    [data-testid="stHeader"] {{ background: transparent !important; }}
    section[data-testid="stMain"] > div {{ padding-top: 0 !important; }}

    /* ── subtle dark overlay so card reads cleanly ── */
    [data-testid="stAppViewContainer"]::before {{
        content: '';
        position: fixed; inset: 0; pointer-events: none; z-index: 0;
        background: rgba(2, 15, 30, 0.55);
    }}

    /* ── card ── */
    .login-card {{
        position: relative; z-index: 1;
        max-width: 460px; margin: 60px auto 0;
        background: rgba(255,255,255,0.04);
        backdrop-filter: blur(18px);
        border: 1px solid rgba(14,158,215,0.25);
        border-radius: 20px;
        padding: 44px 48px 40px;
        box-shadow: 0 0 60px rgba(14,158,215,0.12), 0 24px 48px rgba(0,0,0,0.45);
        font-family: 'Outfit', sans-serif;
    }}

    /* ── logo bar ── */
    .logo-bar {{
        display: flex; align-items: center; justify-content: center;
        gap: 24px; margin-bottom: 28px;
    }}
    .logo-sep {{
        width: 1px; height: 32px;
        background: rgba(14,158,215,0.35);
    }}

    /* ── title ── */
    .login-title {{
        font-family: 'Outfit', sans-serif;
        font-size: 22px; font-weight: 800;
        color: #ffffff; text-align: center;
        letter-spacing: -0.3px; margin: 0 0 4px;
    }}
    .login-sub {{
        font-family: 'Outfit', sans-serif;
        font-size: 13px; font-weight: 300;
        color: rgba(255,255,255,0.5);
        text-align: center; margin: 0 0 32px;
        letter-spacing: 0.4px;
    }}

    /* ── divider ── */
    .login-divider {{
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(14,158,215,0.4), transparent);
        margin: 0 0 28px;
    }}

    /* ── label ── */
    .login-label {{
        font-family: 'Outfit', sans-serif;
        font-size: 11px; font-weight: 600;
        color: rgba(255,255,255,0.45);
        letter-spacing: 1.2px; text-transform: uppercase;
        margin-bottom: 8px;
    }}

    /* ── footer ── */
    .login-footer {{
        margin-top: 28px; text-align: center;
        font-family: 'Outfit', sans-serif;
        font-size: 11px; color: rgba(255,255,255,0.25);
        letter-spacing: 0.3px;
    }}
    .login-domains {{
        display: flex; flex-wrap: wrap; justify-content: center;
        gap: 6px; margin-top: 10px;
    }}
    .login-domain-badge {{
        background: rgba(14,158,215,0.12);
        border: 1px solid rgba(14,158,215,0.2);
        border-radius: 20px; padding: 2px 10px;
        font-size: 10px; color: rgba(14,158,215,0.7);
        font-family: 'Outfit', sans-serif; letter-spacing: 0.2px;
    }}

    /* Streamlit widget overrides inside card */
    div[data-testid="stTextInput"] input {{
        background: rgba(255,255,255,0.06) !important;
        border: 1px solid rgba(14,158,215,0.3) !important;
        border-radius: 10px !important;
        color: #ffffff !important;
        font-family: 'Outfit', sans-serif !important;
        font-size: 14px !important;
        padding: 12px 16px !important;
    }}
    div[data-testid="stTextInput"] input:focus {{
        border-color: rgba(14,158,215,0.8) !important;
        box-shadow: 0 0 0 3px rgba(14,158,215,0.15) !important;
    }}
    div[data-testid="stTextInput"] input::placeholder {{
        color: rgba(255,255,255,0.25) !important;
    }}
    div[data-testid="stTextInput"] label {{
        color: rgba(255,255,255,0.45) !important;
        font-family: 'Outfit', sans-serif !important;
        font-size: 11px !important; font-weight: 600 !important;
        letter-spacing: 1.2px !important; text-transform: uppercase !important;
    }}
    div[data-testid="stForm"] button[kind="primaryFormSubmit"] {{
        background: linear-gradient(135deg, #0E9ED7 0%, #0a78a8 100%) !important;
        border: none !important; border-radius: 10px !important;
        font-family: 'Outfit', sans-serif !important;
        font-size: 14px !important; font-weight: 700 !important;
        letter-spacing: 0.5px !important;
        padding: 14px !important; margin-top: 8px !important;
        box-shadow: 0 4px 20px rgba(14,158,215,0.35) !important;
        transition: all .2s ease !important;
    }}
    div[data-testid="stForm"] button[kind="primaryFormSubmit"]:hover {{
        box-shadow: 0 6px 28px rgba(14,158,215,0.55) !important;
        transform: translateY(-1px) !important;
    }}
    </style>

    <div class="login-card">
        <div class="logo-bar">
            {eps_tag}
            <div class="logo-sep"></div>
            {impulsa_tag}
        </div>
        <div class="login-divider"></div>
        <p class="login-title">Simulador ATT</p>
        <p class="login-sub">Análisis Técnico de Tarifas · Acceso institucional</p>
    </div>
    """, unsafe_allow_html=True)

    # Formulario Streamlit centrado
    col_l, col_m, col_r = st.columns([1, 2.2, 1])
    with col_m:
        with st.form("login_form"):
            email = st.text_input(
                "Correo institucional",
                placeholder="usuario@sanitas.com.co"
            )
            submitted = st.form_submit_button(
                "Ingresar al Simulador",
                use_container_width=True,
                type="primary"
            )
            if submitted:
                if not email:
                    st.error("Ingresa tu correo institucional.")
                elif not verificar_acceso(email):
                    st.error(
                        f"Acceso no autorizado. Dominios permitidos: "
                        f"{', '.join(DOMINIOS_PERMITIDOS)}"
                    )
                else:
                    st.session_state.usuario_email  = email
                    st.session_state.usuario_nombre = (
                        email.split("@")[0].replace(".", " ").title()
                    )
                    st.session_state.es_admin = es_admin(email)
                    try:
                        conn = _get_db_conn()
                        c2   = conn.cursor()
                        c2.execute(
                            """INSERT OR REPLACE INTO usuarios (email, nombre, ultimo_acceso)
                               VALUES (?,?,CURRENT_TIMESTAMP)""",
                            (email, st.session_state.usuario_nombre),
                        )
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass
                    st.rerun()

        # Dominios permitidos bajo el formulario
        badges = "".join(
            f'<span class="login-domain-badge">@{d}</span>'
            for d in DOMINIOS_PERMITIDOS
        )
        st.markdown(
            f'<div class="login-footer">Acceso exclusivo para colaboradores Keralty'
            f'<div class="login-domains">{badges}</div></div>',
            unsafe_allow_html=True,
        )

# ── RN-01: VALIDACIÓN ESTRUCTURA DEL ARCHIVO ─────────────────────────────────
def validar_estructura(df):
    errores = []
    advertencias = []

    # Validar columnas mínimas
    cols_faltantes = [c for c in COLUMNAS_OBLIGATORIAS if c not in df.columns]
    if cols_faltantes:
        errores.append(f"Faltan columnas obligatorias: {', '.join(cols_faltantes)}")

    # Validar que tenga al menos las 33 columnas esperadas
    if len(df.columns) < 33:
        errores.append(f"El archivo tiene {len(df.columns)} columnas. Se esperan al menos 33 (A hasta AG).")

    if errores:
        return False, errores, advertencias

    # Validar registros con campos obligatorios vacíos
    registros_invalidos = []
    for col in COLUMNAS_OBLIGATORIAS:
        if col in df.columns:
            vacios = df[df[col].isna() | (df[col].astype(str).str.strip() == '')].index.tolist()
            if vacios:
                if col == 'TARIFA_OFERTA_FINAL':
                    errores.append(f"⛔ {len(vacios)} registros sin TARIFA_OFERTA_FINAL — imposible calcular")
                else:
                    advertencias.append(f"⚠️ {len(vacios)} registros con {col} vacío")

    # Duplicados
    dups = df['COD'].duplicated().sum()
    if dups > 0:
        advertencias.append(f"⚠️ {dups} códigos duplicados detectados")

    ok = len(errores) == 0
    return ok, errores, advertencias

# ── RN-13: VALIDACIÓN REPS AUTOMÁTICA ────────────────────────────────────────
def validar_reps(df, reps_df):
    if reps_df is None or len(reps_df) == 0:
        df['VALIDACION REPS'] = 'SIN VERIFICAR'
        df['REPS_DETALLE'] = 'Base REPS no disponible'
        return df

    reps_codigos = set(reps_df['codigo_habilitacion'].astype(str).str.strip())
    cod_reps_col = 'COD REPS' if 'COD REPS' in df.columns else None

    def verificar(row):
        cod_reps = str(row.get('COD REPS', '')).strip() if cod_reps_col else ''
        if cod_reps and cod_reps in reps_codigos:
            return 'SI', 'Habilitado en REPS oficial'
        elif cod_reps:
            return 'NO', 'No encontrado en REPS oficial'
        else:
            return 'SIN COD REPS', 'Sin código de habilitación'

    resultados = df.apply(verificar, axis=1)
    df['VALIDACION REPS'] = resultados.apply(lambda x: x[0])
    df['REPS_DETALLE'] = resultados.apply(lambda x: x[1])
    return df

# ── RN-04: CLASIFICACIÓN SUFICIENCIA DE INFORMACIÓN ──────────────────────────
def clasificar_suficiencia(df, pt_df, nit_prestador):
    tiene_historico = False
    tiene_frecuencias = False
    tiene_comparativo = False

    if pt_df is not None and len(pt_df) > 0:
        nit_str = str(nit_prestador).strip()
        tiene_historico = nit_str in pt_df['Nit IPS'].astype(str).str.strip().values
        tiene_comparativo = True

    freq_col = pd.to_numeric(df.get('Frecuencias', pd.Series()), errors='coerce')
    tiene_frecuencias = freq_col.notna().sum() > len(df) * 0.5

    if tiene_historico and tiene_frecuencias and tiene_comparativo:
        return "✅ Análisis completo", "success"
    elif tiene_frecuencias and tiene_comparativo:
        return "🟡 Análisis con información parcial — prestador nuevo sin histórico", "warning"
    elif tiene_comparativo and not tiene_frecuencias:
        return "🟡 Análisis con referencia — sin frecuencias históricas", "warning"
    else:
        return "🔴 Análisis teórico — información insuficiente", "error"

# ── RN-05: COMPARATIVO TARIFARIO ─────────────────────────────────────────────
def calcular_comparativo(df, pt_df, prest_sel):
    if pt_df is None or len(pt_df) == 0:
        return df

    pt2 = pt_df.copy()
    pt2['Codigo Legal de la Prestación'] = pt2['Codigo Legal de la Prestación'].astype(str).str.strip()
    df['COD'] = df['COD'].astype(str).str.strip()

    # Pisos y techos globales
    pivot = pt2.pivot_table(
        index='Codigo Legal de la Prestación',
        values=['Piso_Valor Contratado','Techo_Valor Contratado','Valor Contratado',
                'VALOR SOAT\nSMMLV','VALOR SOAT\nUVT','VALOR TOTAL PLENO ISS'],
        aggfunc='mean'
    ).reset_index()
    pivot.columns = ['COD','PISO','VALOR_ISS','VALOR_SOAT_SMMLV','VALOR_SOAT_UVT','TECHO','VALOR_COMP_PROM']
    df = df.merge(pivot, on='COD', how='left')

    # Comparativo por prestador seleccionado
    if prest_sel:
        for i, prest in enumerate(prest_sel[:3], 1):
            pt_p = pt2[pt2['Nombre Prestador']==prest][['Codigo Legal de la Prestación','Valor Contratado']]
            pt_p = pt_p.rename(columns={'Codigo Legal de la Prestación':'COD','Valor Contratado':f'TARIFA_COMP_{i}'})
            df = df.merge(pt_p, on='COD', how='left')

    # Brecha vs manual según columna T
    def brecha_manual(row):
        tarifa_def = str(row.get('TARIFA DEFINIDA','')).upper()
        oferta = pd.to_numeric(row.get('TARIFA_OFERTA_FINAL'), errors='coerce')
        if pd.isna(oferta): return None
        if 'UVT' in tarifa_def:
            ref = pd.to_numeric(row.get('VALOR_SOAT_UVT'), errors='coerce')
        elif 'SMMLV' in tarifa_def:
            ref = pd.to_numeric(row.get('VALOR_SOAT_SMMLV'), errors='coerce')
        elif 'ISS' in tarifa_def:
            ref = pd.to_numeric(row.get('VALOR_ISS'), errors='coerce')
        else:
            ref = pd.to_numeric(row.get('VALOR_SOAT_UVT'), errors='coerce')
        if pd.isna(ref) or ref == 0: return None
        return (oferta - ref) / ref

    df['BRECHA_MANUAL'] = df.apply(brecha_manual, axis=1)
    df['BRECHA_MERCADO'] = df.apply(lambda r: (pd.to_numeric(r.get('TARIFA_OFERTA_FINAL'), errors='coerce') - pd.to_numeric(r.get('VALOR_COMP_PROM'), errors='coerce')) / pd.to_numeric(r.get('VALOR_COMP_PROM'), errors='coerce') if pd.notna(r.get('VALOR_COMP_PROM')) and pd.to_numeric(r.get('VALOR_COMP_PROM'), errors='coerce') > 0 else None, axis=1)

    return df

# ── RN-06: SEMÁFOROS ──────────────────────────────────────────────────────────
def calcular_semaforos(df, ua, ur):
    def sem(pct):
        if pd.isna(pct): return "verde"
        if pct > ur: return "rojo"
        if pct > ua: return "amarillo"
        return "verde"

    df['SEMAFORO'] = df['% Incremento'].apply(lambda x: sem(pd.to_numeric(x, errors='coerce')))
    if 'BRECHA_MANUAL' in df.columns:
        df['SEMAFORO_MANUAL'] = df['BRECHA_MANUAL'].apply(lambda x: sem(pd.to_numeric(x, errors='coerce')))
    if 'BRECHA_MERCADO' in df.columns:
        df['SEMAFORO_MERCADO'] = df['BRECHA_MERCADO'].apply(lambda x: sem(pd.to_numeric(x, errors='coerce')))

    def sem_global(row):
        sems = [row.get('SEMAFORO','verde'), row.get('SEMAFORO_MANUAL','verde'), row.get('SEMAFORO_MERCADO','verde')]
        if 'rojo' in sems: return 'rojo'
        if 'amarillo' in sems: return 'amarillo'
        return 'verde'

    df['SEMAFORO_GLOBAL'] = df.apply(sem_global, axis=1)
    return df

# ── RN-11: FRECUENCIAS ────────────────────────────────────────────────────────
def completar_frecuencias(df, pt_df, municipio):
    df['Frecuencias'] = pd.to_numeric(df['Frecuencias'], errors='coerce')
    sin_freq = df['Frecuencias'].isna().sum()

    if sin_freq > 0 and pt_df is not None:
        freq_mun = pt_df[pt_df['Nombre Municipio IPS'].str.upper()==municipio.upper()].groupby('Codigo Legal de la Prestación')['Frecuencia'].mean()
        freq_reg = pt_df.groupby('Codigo Legal de la Prestación')['Frecuencia'].mean()
        for idx, row in df[df['Frecuencias'].isna()].iterrows():
            cod = str(row['COD']).strip()
            if cod in freq_mun.index:
                df.at[idx,'Frecuencias'] = freq_mun[cod]
                df.at[idx,'FREQ_FUENTE'] = 'Municipio'
            elif cod in freq_reg.index:
                df.at[idx,'Frecuencias'] = freq_reg[cod]
                df.at[idx,'FREQ_FUENTE'] = 'Regional'
            else:
                df.at[idx,'FREQ_FUENTE'] = 'Sin frecuencia'
    return df, sin_freq

# ── RN-15: SCORE CRÍTICO ──────────────────────────────────────────────────────
def calcular_score_critico(df):
    df['TARIFA_OFERTA_FINAL'] = pd.to_numeric(df['TARIFA_OFERTA_FINAL'], errors='coerce')
    df['Frecuencias'] = pd.to_numeric(df['Frecuencias'], errors='coerce')
    df['% Incremento'] = pd.to_numeric(df['% Incremento'], errors='coerce')
    df['IMPACTO_OFERTA'] = df['TARIFA_OFERTA_FINAL'] * df['Frecuencias']
    df['Tarifa Vigente'] = pd.to_numeric(df['Tarifa Vigente'], errors='coerce')
    df['IMPACTO_VIGENTE'] = df['Tarifa Vigente'] * df['Frecuencias']

    imp_max = df['IMPACTO_OFERTA'].max()
    pct_max = df['% Incremento'].abs().max()

    if imp_max and imp_max > 0:
        df['SCORE_CRITICO'] = (df['% Incremento'].abs().fillna(0)/pct_max*0.4 + df['IMPACTO_OFERTA'].fillna(0)/imp_max*0.6)
    else:
        df['SCORE_CRITICO'] = 0
    return df

# ── TRAZABILIDAD (SQLite + Google Sheets como respaldo) ──────────────────────
def registrar_trazabilidad(nom_p, nit_p, regional, tipo_est, total, reps_inv, imp_of, pct_g, sem_g, clasificacion, version="v1"):
    usuario = st.session_state.get('usuario_email', 'Analista')
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trazabilidad")
        count = cur.fetchone()[0]
        conn.close()
        id_caso = f"CASO-{regional[:3].upper()}-{datetime.now().strftime('%d%m%Y')}-{count+1:03d}"
    except:
        id_caso = f"CASO-{regional[:3].upper()}-{datetime.now().strftime('%d%m%Y%H%M')}"
    # Guardar en SQLite
    registrar_trazabilidad_db(id_caso, nom_p, nit_p, regional, tipo_est, total,
                               reps_inv, imp_of, pct_g, sem_g, clasificacion, usuario, version)
    # Intentar Google Sheets como respaldo
    try:
        gc = get_google_client()
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.sheet1
        ws.append_row([
            id_caso, datetime.now().strftime('%d/%m/%Y'), datetime.now().strftime('%H:%M'),
            regional, nom_p, nit_p, tipo_est, total, int(reps_inv), round(imp_of),
            f"{pct_g*100:.1f}%", sem_g, "En revisión", usuario, version, clasificacion
        ])
    except:
        pass
    return id_caso

def _get_drive_service():
    """Cliente Drive cacheado — se construye una sola vez por sesión."""
    from googleapiclient.discovery import build
    creds = get_credentials()
    return build('drive', 'v3', credentials=creds)

def cargar_desde_drive(nombre_archivo, folder_id=None):
    if not nombre_archivo:
        return None
    try:
        from googleapiclient.http import MediaIoBaseDownload
        service = _get_drive_service()
        fid = folder_id or DRIVE_FOLDER_ID
        results = service.files().list(
            q=f"name='{nombre_archivo}' and '{fid}' in parents and trashed=false",
            fields="files(id, name)").execute()
        files = results.get('files', [])
        if not files:
            return None
        file_id = files[0]['id']
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        return buf
    except Exception as _e:
        return None

def listar_archivos_carpeta(folder_id):
    """Lista archivos de una carpeta Drive — resultado cacheado 30 min."""
    try:
        service = _get_drive_service()
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name, mimeType)",
            pageSize=50).execute()
        return results.get('files', [])
    except:
        return []

def cargar_txt_pipe(buf):
    """Lee archivo TXT con delimitador pipe |"""
    try:
        buf.seek(0)
        df = pd.read_csv(buf, sep='|', encoding='utf-8', dtype=str, low_memory=False)
        return df
    except:
        try:
            buf.seek(0)
            df = pd.read_csv(buf, sep='|', encoding='latin-1', dtype=str, low_memory=False)
            return df
        except:
            return None

def cargar_convenios_regional(regional):
    """Carga el archivo de convenios vigentes para la regional indicada"""
    archivos = listar_archivos_carpeta(FOLDERS["Convenios_Vigentes"])
    regional_upper = regional.upper().replace(" ", "_").replace(".", "")
    for f in archivos:
        nombre = f['name'].upper().replace(" ", "_")
        if regional_upper in nombre or any(r in nombre for r in [regional_upper[:4]]):
            buf = cargar_desde_drive(f['name'], FOLDERS["Convenios_Vigentes"])
            if buf:
                return cargar_txt_pipe(buf)
    # Si no hay match exacto cargar todos y filtrar por regional
    all_dfs = []
    for f in archivos:
        buf = cargar_desde_drive(f['name'], FOLDERS["Convenios_Vigentes"])
        if buf:
            df_tmp = cargar_txt_pipe(buf)
            if df_tmp is not None:
                all_dfs.append(df_tmp)
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return None

def cargar_base_excel(folder_key, nombre_archivo=None):
    """Carga un archivo Excel desde Drive — resultado cacheado 30 min."""
    folder_id = FOLDERS.get(folder_key)
    if not folder_id:
        return None
    if nombre_archivo:
        buf = cargar_desde_drive(nombre_archivo, folder_id)
    else:
        archivos = listar_archivos_carpeta(folder_id)
        xlsx_files = [f for f in archivos if f['name'].endswith(('.xlsx','.xls'))]
        if not xlsx_files:
            return None
        buf = cargar_desde_drive(xlsx_files[-1]['name'], folder_id)
    if buf:
        try:
            return pd.read_excel(buf, dtype=str)
        except:
            return None
    return None

# ── GENERADOR ATT HTML ────────────────────────────────────────────────────────
def generar_att_html(df, nom_p, nit_p, regional, tipo_est, ua, ur, imp_of, imp_vi, pct_g, sv, sa, sr, sem_g, reps_inv, id_caso, clasificacion, top_criticos):
    total = len(df)

    def fmt_cop(v):
        if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
        return f"${v:,.0f}".replace(",",".")

    def sem_color(pct):
        if pd.isna(pct): return "#27ae60","#d5f5e3"
        if pct > ur: return "#c0392b","#fadbd8"
        if pct > ua: return "#d68910","#fef9e7"
        return "#27ae60","#d5f5e3"

    esp_df = df.groupby('DESCRIPCION SERVICIO').agg(
        pct=('% Incremento','mean'), cups=('COD','count'),
        impacto_of=('IMPACTO_OFERTA','sum'), impacto_vi=('IMPACTO_VIGENTE','sum')
    ).reset_index().sort_values('impacto_of', ascending=False)

    esp_rows = ""
    for _, row in esp_df.iterrows():
        pct = row['pct']
        fc, bg = sem_color(pct)
        pct_str = f"{pct*100:+.1f}%" if not pd.isna(pct) else "—"
        dif = row['impacto_of'] - row['impacto_vi']
        esp_rows += f"<tr><td>{row['DESCRIPCION SERVICIO']}</td><td style='text-align:right'>{int(row['cups'])}</td><td style='text-align:right'>{fmt_cop(row['impacto_vi'])}</td><td style='text-align:right'>{fmt_cop(row['impacto_of'])}</td><td style='text-align:right'>{fmt_cop(dif)}</td><td style='text-align:center'><span style='background:{bg};color:{fc};padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700'>{pct_str}</span></td></tr>"

    cups_rows = ""
    for _, row in df.head(50).iterrows():
        pct = row.get('% Incremento')
        pct_n = pd.to_numeric(pct, errors='coerce')
        fc, bg = sem_color(pct_n)
        pct_str = f"{pct_n*100:+.1f}%" if not pd.isna(pct_n) else "—"
        reps_c = "#27ae60" if row.get('VALIDACION REPS')=='SI' else "#c0392b"
        cups_rows += f"<tr><td style='font-family:monospace;font-size:11px'>{row['COD']}</td><td style='font-size:11px'>{str(row.get('DESCRIPCION CUPS',''))[:60]}</td><td style='font-size:11px'>{str(row.get('DESCRIPCION SERVICIO',''))[:30]}</td><td style='text-align:center'><span style='color:{reps_c};font-weight:700;font-size:11px'>{row.get('VALIDACION REPS','')}</span></td><td style='text-align:right;font-size:11px'>{fmt_cop(pd.to_numeric(row.get('Tarifa Vigente'), errors='coerce'))}</td><td style='text-align:right;font-size:11px;font-weight:600'>{fmt_cop(pd.to_numeric(row.get('TARIFA_OFERTA_FINAL'), errors='coerce'))}</td><td style='text-align:center'><span style='background:{bg};color:{fc};padding:2px 8px;border-radius:20px;font-size:10px;font-weight:700'>{pct_str}</span></td></tr>"

    # Sección estratégica
    estado_inc = "superando" if pct_g > ua else "dentro de"
    sem_decision = "🔴 Devolver" if sr > 50 else "🟡 Negociar" if sa > 100 else "🟢 Aprobar"
    sem_decision_color = "#c0392b" if sr > 50 else "#d68910" if sa > 100 else "#27ae60"
    sem_decision_bg = "#fadbd8" if sr > 50 else "#fef9e7" if sa > 100 else "#d5f5e3"

    criticos_rows = ""
    if top_criticos is not None and len(top_criticos) > 0:
        for _, row in top_criticos.head(10).iterrows():
            pct_n = pd.to_numeric(row.get('% Incremento'), errors='coerce')
            fc, bg = sem_color(pct_n)
            pct_str = f"{pct_n*100:+.1f}%" if not pd.isna(pct_n) else "—"
            criticos_rows += f"<tr><td style='font-family:monospace;font-size:11px'>{row['COD']}</td><td style='font-size:11px'>{str(row.get('DESCRIPCION CUPS',''))[:50]}</td><td style='font-size:11px'>{str(row.get('DESCRIPCION SERVICIO',''))[:25]}</td><td style='text-align:center'><span style='background:{bg};color:{fc};padding:2px 8px;border-radius:20px;font-size:10px;font-weight:700'>{pct_str}</span></td><td style='text-align:right;font-size:11px;font-weight:600'>{fmt_cop(pd.to_numeric(row.get('IMPACTO_OFERTA'), errors='coerce'))}</td></tr>"

    dup = df['COD'].duplicated().sum()
    c_global = "#c0392b" if pct_g>ur else "#d68910" if pct_g>ua else "#27ae60"
    c_reps = "#c0392b" if reps_inv>100 else "#d68910" if reps_inv>0 else "#27ae60"

    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8"><title>ATT — {nom_p}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',Arial,sans-serif;background:#f5f6fa;color:#1a1a1a}}
.page{{max-width:1100px;margin:0 auto;padding:24px}}
.header{{background:linear-gradient(135deg,#1a3a5c,#185FA5);color:white;border-radius:12px;padding:28px 32px;margin-bottom:20px}}
.header h1{{font-size:22px;font-weight:700;margin-bottom:6px}}
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
.card{{background:white;border-radius:10px;padding:20px;margin-bottom:16px;border:1px solid #e0e0e0}}
.card-title{{font-size:14px;font-weight:700;color:#1a3a5c;margin-bottom:14px;padding-bottom:8px;border-bottom:2px solid #185FA5}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{background:#f0f4f8;color:#555;font-weight:600;padding:9px 10px;text-align:left;border-bottom:2px solid #ddd;font-size:11px}}
td{{padding:8px 10px;border-bottom:1px solid #f0f0f0;vertical-align:middle}}
.obs{{border-radius:8px;padding:14px 16px;margin-bottom:10px;border-left:4px solid}}
.obs-title{{font-size:13px;font-weight:700;margin-bottom:5px}}
.obs-text{{font-size:12px;line-height:1.6}}
.estrategia{{background:#f0f6fd;border:2px solid #185FA5;border-radius:12px;padding:20px;margin-bottom:16px}}
.estrategia-title{{font-size:16px;font-weight:700;color:#1a3a5c;margin-bottom:14px}}
.decision{{border-radius:10px;padding:16px 20px;text-align:center;margin-bottom:16px}}
.footer{{text-align:center;font-size:11px;color:#999;margin-top:24px;padding-top:16px;border-top:1px solid #ddd}}
@media print{{body{{background:white}}.page{{padding:12px}}}}
</style></head><body><div class="page">

<div class="header">
<h1>🏥 Análisis Técnico de Tarifas — {nom_p}</h1>
<p>Documento oficial · {id_caso} · {clasificacion}</p>
<div class="header-meta">
<div>NIT<strong>{nit_p}</strong></div>
<div>Regional<strong>{regional}</strong></div>
<div>Tipo<strong>{tipo_est}</strong></div>
<div>Caso<strong>{id_caso}</strong></div>
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
<div class="sem-box" style="background:#d5f5e3;border-color:#27ae60"><div style="font-size:28px;font-weight:700;color:#1e8449">{sv:,}</div><div style="font-size:11px;color:#1e8449;margin-top:4px">🟢 Verde — dentro de rango</div></div>
<div class="sem-box" style="background:#fef9e7;border-color:#f39c12"><div style="font-size:28px;font-weight:700;color:#d68910">{sa:,}</div><div style="font-size:11px;color:#d68910;margin-top:4px">🟡 Amarillo — revisar</div></div>
<div class="sem-box" style="background:#fadbd8;border-color:#e74c3c"><div style="font-size:28px;font-weight:700;color:#c0392b">{sr:,}</div><div style="font-size:11px;color:#c0392b;margin-top:4px">🔴 Rojo — fuera de techo</div></div>
</div>

<div class="estrategia">
<div class="estrategia-title">🎯 Análisis Estratégico y Orientación al Negociador</div>
<div class="obs" style="background:#e8f4fd;border-color:#3498db;margin-bottom:12px">
<div class="obs-title" style="color:#1a5276">📊 Resumen ejecutivo</div>
<div class="obs-text">La propuesta presenta un incremento global de <strong>{pct_g*100:+.1f}%</strong>, {estado_inc} el umbral del {ua*100:.0f}% para esta regional. Se analizaron <strong>{total:,} CUPS</strong> con <strong>{reps_inv} REPS inválidos</strong>. El impacto económico total es de <strong>${imp_of/1e9:.3f}B</strong> vs contrato vigente de <strong>${imp_vi/1e9:.3f}B</strong>.</div>
</div>
<div class="decision" style="background:{sem_decision_bg};border:2px solid {sem_decision_color}">
<div style="font-size:20px;font-weight:700;color:{sem_decision_color}">{sem_decision}</div>
<div style="font-size:13px;color:{sem_decision_color};margin-top:4px">{"Supera umbrales críticos — requiere corrección antes de continuar" if sr>50 else "Supera algunos umbrales — viable con ajustes en códigos críticos" if sa>100 else "Dentro de todos los umbrales — puede proceder con aprobación"}</div>
</div>
{"<div class='card' style='margin-top:12px'><div class='card-title'>🔴 Top 10 Códigos Críticos — Acción requerida</div><table><thead><tr><th>CUPS</th><th>Descripción</th><th>Especialidad</th><th style='text-align:center'>% Inc.</th><th style='text-align:right'>Impacto</th></tr></thead><tbody>" + criticos_rows + "</tbody></table></div>" if criticos_rows else ""}
</div>

<div class="card"><div class="card-title">📊 Impacto por especialidad</div>
<table><thead><tr><th>Especialidad</th><th style="text-align:right">CUPS</th><th style="text-align:right">Vigente</th><th style="text-align:right">Oferta</th><th style="text-align:right">Diferencia</th><th style="text-align:center">% Var</th></tr></thead>
<tbody>{esp_rows}</tbody></table></div>

<div class="card"><div class="card-title">🔍 Detalle CUPS (primeros 50)</div>
<table><thead><tr><th>CUPS</th><th>Descripción</th><th>Especialidad</th><th style="text-align:center">REPS</th><th style="text-align:right">Vigente</th><th style="text-align:right">Oferta</th><th style="text-align:center">% Inc.</th></tr></thead>
<tbody>{cups_rows}</tbody></table>
<p style="font-size:11px;color:#888;margin-top:10px;text-align:center">Primeros 50 CUPS. Descarga CSV para listado completo.</p></div>

<div class="card"><div class="card-title">📝 Observaciones</div>
<div class="obs" style="background:#fadbd8;border-color:#e74c3c"><div class="obs-title" style="color:#c0392b">🔴 Incremento general</div><div class="obs-text">Incremento ponderado de <strong>{pct_g*100:+.1f}%</strong>, {estado_inc} el umbral del {ua*100:.0f}%.</div></div>
<div class="obs" style="background:#fadbd8;border-color:#e74c3c"><div class="obs-title" style="color:#c0392b">🔴 REPS inválidos</div><div class="obs-text"><strong>{reps_inv:,} prestaciones</strong> no habilitadas en REPS ({reps_inv/total*100:.1f}%). No pueden incluirse en contrato.</div></div>
{"<div class='obs' style='background:#fef9e7;border-color:#f39c12'><div class='obs-title' style='color:#d68910'>🟡 CUPS duplicados</div><div class='obs-text'><strong>" + str(dup) + " prestaciones</strong> con códigos duplicados. Requieren verificación.</div></div>" if dup > 0 else ""}
<div class="obs" style="background:#d6eaf8;border-color:#3498db"><div class="obs-title" style="color:#1a5276">🔵 Clasificación del análisis</div><div class="obs-text">{clasificacion}</div></div>
</div>

<div class="footer">Generado por Simulador ATT — EPS Sanitas · {id_caso} · {datetime.now().strftime('%d de %B de %Y')} · Documento confidencial</div>
</div></body></html>"""

# ── STREAMLIT APP ─────────────────────────────────────────────────────────────
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
.alerta-error{background:#fadbd8;border:1px solid #e74c3c;border-radius:8px;padding:12px 16px;margin-bottom:8px}
.alerta-warning{background:#fef9e7;border:1px solid #f39c12;border-radius:8px;padding:12px 16px;margin-bottom:8px}
.alerta-success{background:#d5f5e3;border:1px solid #27ae60;border-radius:8px;padding:12px 16px;margin-bottom:8px}
</style>""", unsafe_allow_html=True)

def fmt_cop(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return f"${v:,.0f}".replace(",",".")

def fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return f"{'+' if v>0 else ''}{v*100:.1f}%"

# SIDEBAR
# ── INICIALIZAR DB ────────────────────────────────────────────────────────────
try:
    init_db()
except:
    pass

# ── CONTROL DE ACCESO ─────────────────────────────────────────────────────────
if 'usuario_email' not in st.session_state:
    st.session_state.usuario_email = None
    st.session_state.usuario_nombre = None
    st.session_state.es_admin = False

if not st.session_state.usuario_email:
    pantalla_login()
    st.stop()

# Usuario autenticado — continúa la app
usuario_email = st.session_state.usuario_email
usuario_nombre = st.session_state.usuario_nombre
es_admin_user = st.session_state.es_admin

with st.sidebar:
    st.markdown("## 🏥 Simulador ATT")
    st.markdown("**EPS Sanitas**")
    st.markdown(f"👤 {usuario_nombre}")
    if es_admin_user:
        st.markdown("⚙️ *Administrador*")
    if st.button("Cerrar sesión", use_container_width=True):
        st.session_state.usuario_email = None
        st.session_state.usuario_nombre = None
        st.session_state.es_admin = False
        st.rerun()
    st.divider()
    st.markdown("### ⚙️ Configuración")
    regional = st.selectbox("Regional", ["Bogotá D.C.","Medellín","Cali","Barranquilla","Bucaramanga","Centro Oriente"])

    st.divider()
    st.markdown("### 📂 Archivos del análisis")

    sol_up = st.file_uploader(
        "📋 Solicitud del prestador",
        type=["xlsx","xls"], key="sol",
        help="Excel con la propuesta tarifaria. Debe tener la hoja 'Solicitud'."
    )

    with st.expander("📦 Bases de referencia (opcional — si Drive no está disponible)"):
        st.caption("Si el sistema no puede conectarse a Drive, carga estas bases manualmente.")
        pt_up   = st.file_uploader("Pisos y techos", type=["xlsx","xls"], key="pt")
        reps_up = st.file_uploader("REPS", type=["xlsx","xls"], key="reps")

    if not sol_up:
        st.info("👆 Carga el archivo de solicitud para continuar.")
        ok = False
    else:
        st.success(f"✅ {sol_up.name} ({sol_up.size/1024:.0f} KB)")
        ok = True

    fuente        = "📤 Carga directa"
    usar_uploads  = True
    sol_folder_id = None

    st.divider()
    municipio  = st.text_input("Municipio prestador", value="BOGOTA D.C.")
    tipo_est   = st.selectbox("Tipo de estudio", ["Actualización tarifaria + inclusión","Solo actualización tarifaria","Solo inclusión","Renovación","Nueva adscripción"])
    nit_p      = st.text_input("NIT prestador", value="820005389")
    nom_p      = st.text_input("Nombre prestador", value="Hospital de Chiquinquirá")
    st.divider()
    # Cargar umbrales desde DB según regional
    _regional_tmp = "Bogotá D.C."  # Se actualizará después
    ua, ur = UA_DEFAULT, UR_DEFAULT
    if es_admin_user:
        st.markdown("### ⚙️ Configuración de umbrales")
        ua = st.slider("Amarillo (%)", 1, 30, int(UA_DEFAULT*100)) / 100
        ur = st.slider("Rojo (%)", 5, 50, int(UR_DEFAULT*100)) / 100
    st.divider()
    st.markdown("### 🏪 Comparadores")
    st.caption("Se cargan automáticamente al ejecutar el análisis según la regional y los archivos disponibles en Drive.")

    # Cargar prestadores dinámicamente desde pisos y techos o convenios
    prest_disponibles = []
    if fuente == "💻 Archivos locales" and not usar_uploads:
        try:
            pt_prev = pd.read_excel(pt_f)
            if 'Nombre Prestador' in pt_prev.columns:
                prest_disponibles = sorted(pt_prev['Nombre Prestador'].dropna().unique().tolist())
        except:
            pass
    elif fuente == "💻 Archivos locales" and usar_uploads and 'pt' in dir() and pt_up:
        try:
            pt_prev = pd.read_excel(pt_up)
            if 'Nombre Prestador' in pt_prev.columns:
                prest_disponibles = sorted(pt_prev['Nombre Prestador'].dropna().unique().tolist())
        except:
            pass

    if prest_disponibles:
        st.caption(f"**{len(prest_disponibles)} prestadores** disponibles en la base de pisos y techos.")
        prest_sel = st.multiselect(
            "Selecciona prestadores comparadores",
            options=prest_disponibles,
            default=prest_disponibles[:3] if len(prest_disponibles) >= 3 else prest_disponibles,
            max_selections=5
        )
    else:
        st.info("Los comparadores se cargarán automáticamente al ejecutar el análisis desde Google Drive.")
        prest_sel = []

    st.divider()
    ejecutar = st.button("▶ Ejecutar análisis", type="primary", use_container_width=True)


st.markdown("# 🏥 Simulador ATT — Análisis Técnico de Tarifas")

if not ok:
    st.info("👈 Configura los archivos en el panel izquierdo para comenzar.")
    st.stop()

if 'df' not in st.session_state:
    st.session_state.df = None
    st.session_state.pt_raw = None
    st.session_state.id_caso = None
    st.session_state.clasificacion = None
    st.session_state.errores_val = []
    st.session_state.advertencias_val = []
    st.session_state.convenios_df = None
    st.session_state.homologacion_df = None
    st.session_state.costo_medio_df = None
    st.session_state.tabla_qx_df = None
    st.session_state.insumos_df = None
    st.session_state.bases_cargadas = {}
    st.session_state.prest_disponibles_drive = []

if ejecutar:
    with st.spinner("Cargando y validando archivos..."):
        try:
            # ── Solicitud: cargada por el analista ───────────────────────────
            if not sol_up:
                st.error("⛔ Carga el archivo de solicitud del prestador antes de ejecutar.")
                st.stop()
            try:
                sol = pd.read_excel(sol_up, sheet_name='Solicitud')
            except Exception as _e:
                st.error(f"⛔ No se pudo leer la hoja 'Solicitud': {_e}\n\nVerifica que el Excel tiene una hoja llamada exactamente **Solicitud**.")
                st.stop()

            # ── Bases de referencia: manual si se subieron, si no desde Drive ──
            if pt_up:
                pt = pd.read_excel(pt_up)
                st.toast("✅ Pisos y techos cargados manualmente")
            else:
                with st.spinner("Cargando Pisos y techos desde Drive..."):
                    try:
                        _archivos_pt = listar_archivos_carpeta(FOLDERS["Pisos_y_Techos"])
                        _nombre_pt = next((f['name'] for f in _archivos_pt if f['name'].endswith(('.xlsx','.xls'))), None)
                        pt_buf = cargar_desde_drive(_nombre_pt, FOLDERS["Pisos_y_Techos"]) if _nombre_pt else None
                        pt = pd.read_excel(pt_buf) if pt_buf else None
                    except:
                        pt = None
                if pt is None:
                    st.warning("⚠️ No se pudo cargar Pisos y techos desde Drive. Puedes subirlo manualmente en el panel izquierdo.")

            if reps_up:
                reps = pd.read_excel(reps_up)
                st.toast("✅ REPS cargado manualmente")
            else:
                with st.spinner("Cargando REPS desde Drive..."):
                    try:
                        _archivos_reps = listar_archivos_carpeta(FOLDERS["REPS"])
                        _nombre_reps = next((f['name'] for f in _archivos_reps if f['name'].endswith(('.xlsx','.xls'))), None)
                        reps_buf = cargar_desde_drive(_nombre_reps, FOLDERS["REPS"]) if _nombre_reps else None
                        reps = pd.read_excel(reps_buf) if reps_buf else None
                    except:
                        reps = None
                if reps is None:
                    st.warning("⚠️ No se pudo cargar REPS desde Drive. Puedes subirlo manualmente en el panel izquierdo.")

            # RN-01: Validar estructura
            es_valido, errores, advertencias = validar_estructura(sol)
            st.session_state.errores_val = errores
            st.session_state.advertencias_val = advertencias

            if not es_valido:
                st.error("⛔ El archivo no cumple con la estructura mínima requerida. Corrija los errores antes de continuar.")
                for e in errores:
                    st.markdown(f'<div class="alerta-error">⛔ {e}</div>', unsafe_allow_html=True)
                st.stop()

            # Cargar bases adicionales desde Drive
            convenios_df = None
            homologacion_df = None
            costo_medio_df = None
            tabla_qx_df = None
            insumos_df = None
            bases_cargadas = {}

            # Bases adicionales desde Drive
            with st.spinner("Cargando bases de referencia desde Drive..."):
                try:
                    convenios_df = cargar_convenios_regional(regional)
                    if convenios_df is not None and len(convenios_df) > 0:
                        bases_cargadas["Convenios_Vigentes"] = {"ok": True, "registros": f"{len(convenios_df):,}"}
                        col_prest = next((c2 for c2 in convenios_df.columns if 'Nombre Prestador' in c2 or 'Prestador' in c2), None)
                        if col_prest:
                            st.session_state.prest_disponibles_drive = sorted(convenios_df[col_prest].dropna().unique().tolist())
                    else:
                        bases_cargadas["Convenios_Vigentes"] = {"ok": False}
                except: bases_cargadas["Convenios_Vigentes"] = {"ok": False}

                try:
                    homologacion_df = cargar_base_excel("Homologacion")
                    if homologacion_df is not None:
                        bases_cargadas["Homologacion"] = {"ok": True, "registros": f"{len(homologacion_df):,}"}
                    else:
                        bases_cargadas["Homologacion"] = {"ok": False}
                except: bases_cargadas["Homologacion"] = {"ok": False}

                try:
                    costo_medio_df = cargar_base_excel("Costo_medio_evento")
                    if costo_medio_df is not None:
                        bases_cargadas["Costo_medio_evento"] = {"ok": True, "registros": f"{len(costo_medio_df):,}"}
                    else:
                        bases_cargadas["Costo_medio_evento"] = {"ok": False}
                except: bases_cargadas["Costo_medio_evento"] = {"ok": False}

                try:
                    tabla_qx_df = cargar_base_excel("Tabla_QX")
                    if tabla_qx_df is not None:
                        bases_cargadas["Tabla_QX"] = {"ok": True, "registros": f"{len(tabla_qx_df):,}"}
                    else:
                        bases_cargadas["Tabla_QX"] = {"ok": False}
                except: bases_cargadas["Tabla_QX"] = {"ok": False}

                try:
                    insumos_df = cargar_base_excel("Insumos_Dispositivos")
                    if insumos_df is not None:
                        bases_cargadas["Insumos_Dispositivos"] = {"ok": True, "registros": f"{len(insumos_df):,}"}
                    else:
                        bases_cargadas["Insumos_Dispositivos"] = {"ok": False}
                except: bases_cargadas["Insumos_Dispositivos"] = {"ok": False}

                bases_cargadas["Casuistica_Poblacional"] = {"pendiente": True}
                bases_cargadas["Medicamentos"] = {"pendiente": True}

            # Siempre registrar REPS y Pisos y Techos
            if reps is not None:
                bases_cargadas["REPS"] = {"ok": True, "registros": f"{len(reps):,}"}
            if pt is not None:
                bases_cargadas["Pisos_y_Techos"] = {"ok": True, "registros": f"{len(pt):,}"}
                if 'Nombre Prestador' in pt.columns:
                    st.session_state.prest_disponibles_drive = sorted(pt['Nombre Prestador'].dropna().unique().tolist())
                    if not prest_sel:
                        prest_sel = st.session_state.prest_disponibles_drive[:3]

            # RN-13: Validación REPS automática
            sol = validar_reps(sol, reps)

            # RN-04: Clasificación suficiencia
            clasificacion, tipo_clas = clasificar_suficiencia(sol, pt, nit_p)

            # RN-11: Completar frecuencias
            sol, sin_freq = completar_frecuencias(sol, pt, municipio)

            # RN-05: Comparativo tarifario
            sol = calcular_comparativo(sol, pt, prest_sel)

            # RN-07 + RN-16: Cruce con convenios, homologación y costo medio
            if convenios_df is not None and len(convenios_df) > 0:
                try:
                    conv = convenios_df.copy()
                    conv.columns = [str(c2).strip() for c2 in conv.columns]
                    cod_col = next((c2 for c2 in conv.columns if 'Legal' in c2 and 'Prestaci' in c2), None)
                    val_col = next((c2 for c2 in conv.columns if 'Valor Pleno' in c2), None)
                    if cod_col:
                        conv[cod_col] = conv[cod_col].astype(str).str.strip()
                        conv_agg = conv.groupby(cod_col).size().reset_index(name='TIENE_CONVENIO')
                        conv_agg.columns = ['COD','TIENE_CONVENIO']
                        conv_agg['TIENE_CONVENIO'] = True
                        sol = sol.merge(conv_agg, on='COD', how='left')
                        sol['TIENE_CONVENIO'] = sol['TIENE_CONVENIO'].fillna(False)
                        if val_col:
                            conv[val_col] = pd.to_numeric(conv[val_col], errors='coerce')
                            val_agg = conv.groupby(cod_col)[val_col].mean().reset_index()
                            val_agg.columns = ['COD','VALOR_PLENO_CONVENIO']
                            sol = sol.merge(val_agg, on='COD', how='left')
                except Exception as e_conv:
                    pass

            if homologacion_df is not None and len(homologacion_df) > 0:
                try:
                    hom = homologacion_df.copy()
                    hom.columns = [str(c2).strip() for c2 in hom.columns]
                    cod_propio = next((c2 for c2 in hom.columns if 'propio' in c2.lower()), None)
                    cod_soat = next((c2 for c2 in hom.columns if 'soat' in c2.lower()), None)
                    if cod_propio:
                        hom[cod_propio] = hom[cod_propio].astype(str).str.strip()
                        hom_sel = hom[[cod_propio]].copy()
                        if cod_soat: hom_sel['COD_SOAT_HOMOLOGO'] = hom[cod_soat]
                        hom_sel = hom_sel.rename(columns={cod_propio:'COD'}).drop_duplicates()
                        sol = sol.merge(hom_sel, on='COD', how='left')
                        sol['TIENE_HOMOLOGACION'] = sol.get('COD_SOAT_HOMOLOGO', pd.Series(dtype=str)).notna()
                except:
                    pass

            if costo_medio_df is not None and len(costo_medio_df) > 0:
                try:
                    cm = costo_medio_df.copy()
                    cm.columns = [str(c2).strip() for c2 in cm.columns]
                    esp_col = next((c2 for c2 in cm.columns if 'especialidad' in c2.lower() or 'servicio' in c2.lower()), None)
                    costo_col = next((c2 for c2 in cm.columns if 'costo' in c2.lower() or 'precio' in c2.lower() or 'valor' in c2.lower()), None)
                    if esp_col and costo_col:
                        cm[costo_col] = pd.to_numeric(cm[costo_col], errors='coerce')
                        cm_agg = cm.groupby(esp_col)[costo_col].mean().reset_index()
                        cm_agg.columns = ['DESCRIPCION SERVICIO','COSTO_MEDIO_EVENTO']
                        sol = sol.merge(cm_agg, on='DESCRIPCION SERVICIO', how='left')
                        sol['IMPACTO_VS_COSTO_MEDIO'] = (sol['TARIFA_OFERTA_FINAL'] - pd.to_numeric(sol.get('COSTO_MEDIO_EVENTO'), errors='coerce')) * sol['Frecuencias']
                except:
                    pass

            # RN-06: Semáforos
            sol = calcular_semaforos(sol, ua, ur)

            # RN-15: Score crítico
            sol = calcular_score_critico(sol)

            st.session_state.df = sol
            st.session_state.pt_raw = pt
            st.session_state.clasificacion = clasificacion
            st.session_state.convenios_df = convenios_df
            st.session_state.homologacion_df = homologacion_df
            st.session_state.costo_medio_df = costo_medio_df
            st.session_state.tabla_qx_df = tabla_qx_df
            st.session_state.insumos_df = insumos_df
            st.session_state.bases_cargadas = bases_cargadas

            # Trazabilidad
            imp_of_t = (sol['TARIFA_OFERTA_FINAL'] * sol['Frecuencias']).sum()
            imp_vi_t = (sol['Tarifa Vigente'] * sol['Frecuencias']).sum()
            pct_g_t  = (imp_of_t - imp_vi_t) / imp_vi_t if imp_vi_t else 0
            sr_t = (sol['SEMAFORO']=='rojo').sum()
            sa_t = (sol['SEMAFORO']=='amarillo').sum()
            sem_g_t = "🔴 Crítico" if sr_t>50 else "🟡 Alerta" if sa_t>100 else "🟢 Favorable"
            reps_inv_t = (sol['VALIDACION REPS']=='NO').sum()

            id_caso = registrar_trazabilidad(nom_p, nit_p, regional, tipo_est, len(sol), reps_inv_t, imp_of_t, pct_g_t, sem_g_t, clasificacion)
            st.session_state.id_caso = id_caso

            st.success(f"✅ {len(sol):,} CUPS procesados · Caso: {id_caso} · {clasificacion}")

            if advertencias:
                for w in advertencias:
                    st.markdown(f'<div class="alerta-warning">{w}</div>', unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Error al procesar: {e}")
            st.stop()

if st.session_state.df is None:
    st.info("👈 Haz clic en **Ejecutar análisis** para comenzar.")
    st.stop()

df = st.session_state.df
clasificacion = st.session_state.clasificacion or ""
id_caso = st.session_state.id_caso or ""

total    = len(df)
reps_inv = (df['VALIDACION REPS']=='NO').sum()
imp_of   = df['IMPACTO_OFERTA'].sum()
imp_vi   = df['IMPACTO_VIGENTE'].sum()
pct_g    = (imp_of - imp_vi) / imp_vi if imp_vi else 0
sv = (df['SEMAFORO']=='verde').sum()
sa = (df['SEMAFORO']=='amarillo').sum()
sr = (df['SEMAFORO']=='rojo').sum()
sem_g = "🔴 Crítico" if sr>50 else "🟡 Alerta" if sa>100 else "🟢 Favorable"
c_kpi = "#c0392b" if pct_g>ur else "#f39c12" if pct_g>ua else "#27ae60"
c_rep = "#c0392b" if reps_inv>100 else "#f39c12" if reps_inv>0 else "#27ae60"
c_sem = "#c0392b" if sr>50 else "#f39c12" if sa>100 else "#27ae60"

top_criticos = df.nlargest(10, 'SCORE_CRITICO') if 'SCORE_CRITICO' in df.columns else None

# HEADER
c1,c2,c3 = st.columns([3,1,1])
with c1:
    st.markdown(f"**Previsualización ATT — {nom_p}** `v1 · vigente` `{id_caso}`  \nNIT: {nit_p} · Regional {regional} · {datetime.now().strftime('%d %b %Y %H:%M')}")
    if clasificacion:
        color_clas = "#27ae60" if "completo" in clasificacion else "#d68910" if "parcial" in clasificacion or "referencia" in clasificacion else "#c0392b"
        st.markdown(f'<span style="font-size:12px;color:{color_clas};font-weight:600">{clasificacion}</span>', unsafe_allow_html=True)
with c2:
    html_att = generar_att_html(df, nom_p, nit_p, regional, tipo_est, ua, ur, imp_of, imp_vi, pct_g, sv, sa, sr, sem_g, reps_inv, id_caso, clasificacion, top_criticos)
    # Agregar script de impresión al HTML
    html_att_print = html_att.replace('</head>', """<style>
    @media print {
        body { background: white !important; }
        .stButton, button { display: none !important; }
        .page { max-width: 100% !important; padding: 0 !important; }
    }
    </style>
    <script>
    function imprimirATT() { window.print(); }
    </script>
    </head>""").replace('<body>', '<body><div style="text-align:right;padding:10px;print-color-adjust:exact"><button onclick="imprimirATT()" style="background:#185FA5;color:white;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;font-size:14px">🖨️ Imprimir / Guardar PDF</button></div>')
    c1_att, c2_att = st.columns(2)
    with c1_att:
        st.download_button("⬇ Descargar ATT (HTML)", html_att_print.encode('utf-8'), f"ATT_{nom_p.replace(' ','_')}_{id_caso}.html","text/html", use_container_width=True)
    with c2_att:
        st.info("💡 Abre el HTML descargado y usa el botón 🖨️ para imprimir o guardar como PDF")
with c3:
    if st.button("↩ Devolver caso", use_container_width=True):
        try:
            gc = get_google_client()
            sh = gc.open_by_key(SHEETS_ID)
            ws = sh.sheet1
            data = ws.get_all_values()
            for i, row in enumerate(data):
                if row and row[0] == id_caso:
                    ws.update_cell(i+1, 13, "Devuelto para corrección")
            st.warning("Caso marcado como devuelto.")
        except:
            st.warning("Actualiza el estado manualmente en Sheets.")

st.divider()

# KPIs
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

tab1,tab2,tab3,tab4,tab5,tab6 = st.tabs(["📊 Por especialidad","🔍 Detalle CUPS","⚖️ Comparativo","📈 Impacto","📝 Observaciones","📋 Trazabilidad"])

with tab1:
    cs1,cs2,cs3 = st.columns(3)
    with cs1:
        st.markdown(f'<div class="sem-box" style="background:#d5f5e3;border:1px solid #27ae60"><p style="font-size:11px;color:#1e8449;margin:0 0 4px 0">🟢 Verde — dentro de rango</p><p style="font-size:30px;font-weight:700;color:#1e8449;margin:0">{sv:,}</p><p style="font-size:12px;color:#1e8449;margin:4px 0 0 0">CUPS aceptados</p></div>', unsafe_allow_html=True)
    with cs2:
        st.markdown(f'<div class="sem-box" style="background:#fef9e7;border:1px solid #f39c12"><p style="font-size:11px;color:#d68910;margin:0 0 4px 0">🟡 Amarillo — revisar</p><p style="font-size:30px;font-weight:700;color:#d68910;margin:0">{sa:,}</p><p style="font-size:12px;color:#d68910;margin:4px 0 0 0">CUPS a revisar</p></div>', unsafe_allow_html=True)
    with cs3:
        st.markdown(f'<div class="sem-box" style="background:#fadbd8;border:1px solid #e74c3c"><p style="font-size:11px;color:#c0392b;margin:0 0 4px 0">🔴 Rojo — fuera de techo</p><p style="font-size:30px;font-weight:700;color:#c0392b;margin:0">{sr:,}</p><p style="font-size:12px;color:#c0392b;margin:4px 0 0 0">CUPS críticos</p></div>', unsafe_allow_html=True)

    st.markdown("---")

    # Alerta estratégica
    sem_decision = "🔴 Devolver" if sr>50 else "🟡 Negociar" if sa>100 else "🟢 Aprobar"
    sem_color_d = "#fadbd8" if sr>50 else "#fef9e7" if sa>100 else "#d5f5e3"
    sem_border_d = "#e74c3c" if sr>50 else "#f39c12" if sa>100 else "#27ae60"
    sem_text_d = "#c0392b" if sr>50 else "#d68910" if sa>100 else "#1e8449"
    st.markdown(f'<div style="background:{sem_color_d};border:2px solid {sem_border_d};border-radius:10px;padding:14px 18px;margin-bottom:16px;text-align:center"><p style="font-size:18px;font-weight:700;color:{sem_text_d};margin:0">{sem_decision}</p><p style="font-size:12px;color:{sem_text_d};margin:4px 0 0 0">{"Supera umbrales críticos — requiere corrección" if sr>50 else "Viable con ajustes en códigos críticos" if sa>100 else "Dentro de todos los umbrales"}</p></div>', unsafe_allow_html=True)

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

    if top_criticos is not None:
        st.markdown("---")
        st.markdown('<p class="section-title">🔴 Top 10 Códigos Críticos — Mayor impacto y desviación</p>', unsafe_allow_html=True)
        tc = pd.DataFrame({
            'CUPS':top_criticos['COD'],
            'Descripción':top_criticos['DESCRIPCION CUPS'].str[:50],
            'Especialidad':top_criticos['DESCRIPCION SERVICIO'].str[:30],
            '% Inc':top_criticos['% Incremento'].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "—"),
            'Impacto':top_criticos['IMPACTO_OFERTA'].apply(fmt_cop),
            'Score':top_criticos['SCORE_CRITICO'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        })
        st.dataframe(tc,use_container_width=True,hide_index=True)

    st.markdown("---")
    st.markdown('<p class="section-title">Historial de versiones</p>', unsafe_allow_html=True)
    hist = pd.DataFrame([{"Versión":"v1 · vigente","Caso":id_caso,"Fecha":datetime.now().strftime('%d %b %Y · %H:%M'),
         "Impacto":f"${imp_of/1e6:.1f}M","Clasificación":clasificacion,"Estado":f"🔵 En revisión · {reps_inv} REPS"}])
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
        if col in dv.columns:
            dv[col] = pd.to_numeric(dv[col],errors='coerce')

    cols_tabla = {'CUPS':dv['COD'],'Descripción':dv['DESCRIPCION CUPS'].str[:50],
        'Especialidad':dv['DESCRIPCION SERVICIO'].str[:30],'REPS':dv['VALIDACION REPS'],
        'Tarifa vigente':dv['Tarifa Vigente'].apply(fmt_cop),
        'Tarifa oferta':dv['TARIFA_OFERTA_FINAL'].apply(fmt_cop),
        '% Inc':dv['% Incremento'].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "—"),
        'Sem':dv['SEMAFORO'].apply(lambda x:{"verde":"🟢","amarillo":"🟡","rojo":"🔴"}.get(x,"⚪"))}

    if 'TARIFA_COMP_1' in dv.columns: cols_tabla['Comp.1'] = dv['TARIFA_COMP_1'].apply(fmt_cop)
    if 'TARIFA_COMP_2' in dv.columns: cols_tabla['Comp.2'] = dv['TARIFA_COMP_2'].apply(fmt_cop)
    if 'PISO' in dv.columns: cols_tabla['Piso'] = dv['PISO'].apply(fmt_cop)
    if 'TECHO' in dv.columns: cols_tabla['Techo'] = dv['TECHO'].apply(fmt_cop)
    if 'BRECHA_MANUAL' in dv.columns: cols_tabla['Brecha manual'] = dv['BRECHA_MANUAL'].apply(fmt_pct)
    if 'BRECHA_MERCADO' in dv.columns: cols_tabla['Brecha mercado'] = dv['BRECHA_MERCADO'].apply(fmt_pct)

    tabla2 = pd.DataFrame(cols_tabla)
    st.dataframe(tabla2,use_container_width=True,hide_index=True,height=500)

    st.download_button("⬇ Descargar CUPS filtrados",
        dv.to_csv(index=False).encode('utf-8'),
        f"CUPS_{id_caso}.csv","text/csv")

with tab3:
    cc1,cc2 = st.columns(2)
    with cc1:
        st.markdown('<p class="section-title">Referencia municipio</p>', unsafe_allow_html=True)
        if '% DE VARIACIÓN MUNICIPIO' in df.columns:
            cm = df.groupby('DESCRIPCION SERVICIO').agg(v=('% DE VARIACIÓN MUNICIPIO','mean')).dropna().reset_index().sort_values('v',ascending=False).head(10)
            cm['% vs municipio'] = cm['v'].apply(fmt_pct)
            cm['Especialidad'] = cm['DESCRIPCION SERVICIO'].str[:35]
            st.dataframe(cm[['Especialidad','% vs municipio']],use_container_width=True,hide_index=True)
        else:
            st.info("No hay datos de referencia municipio en este archivo.")
    with cc2:
        st.markdown('<p class="section-title">Referencia regional</p>', unsafe_allow_html=True)
        if '% DE VARIACIÓN REGIONAL' in df.columns:
            cr = df.groupby('DESCRIPCION SERVICIO').agg(v=('% DE VARIACIÓN REGIONAL','mean')).dropna().reset_index().sort_values('v',ascending=False).head(10)
            cr['% vs regional'] = cr['v'].apply(fmt_pct)
            cr['Especialidad'] = cr['DESCRIPCION SERVICIO'].str[:35]
            st.dataframe(cr[['Especialidad','% vs regional']],use_container_width=True,hide_index=True)
        else:
            st.info("No hay datos de referencia regional en este archivo.")

    st.markdown("---")
    st.markdown('<p class="section-title">Comparativo tarifas oferta vs comparadores</p>', unsafe_allow_html=True)
    comp_cols = ['TARIFA_OFERTA_FINAL'] + [f'TARIFA_COMP_{i}' for i in range(1,4) if f'TARIFA_COMP_{i}' in df.columns]
    for col in comp_cols:
        df[col] = pd.to_numeric(df[col],errors='coerce')
    ce = df.groupby('DESCRIPCION SERVICIO')[comp_cols].mean().reset_index().dropna(thresh=2)
    ce['Especialidad'] = ce['DESCRIPCION SERVICIO'].str[:35]
    fig3=go.Figure()
    colors = ['#e74c3c','#3498db','#27ae60','#f39c12']
    names = ['Oferta','Comp.1','Comp.2','Comp.3']
    for i, col in enumerate(comp_cols):
        if col in ce.columns:
            fig3.add_trace(go.Bar(name=names[i],x=ce['Especialidad'],y=ce[col],marker_color=colors[i]))
    fig3.update_layout(barmode='group',height=380,margin=dict(l=0,r=10,t=10,b=120),
                       xaxis_tickangle=-35,yaxis_title="Tarifa promedio ($)",
                       plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)',
                       legend=dict(orientation='h',yanchor='bottom',y=1.02))
    st.plotly_chart(fig3,use_container_width=True)

    st.markdown("---")
    st.markdown('<p class="section-title">Prestadores con mejor tarifa disponibles</p>', unsafe_allow_html=True)
    pt_raw = st.session_state.pt_raw
    if pt_raw is not None:
        pt_raw2 = pt_raw.copy()
        pt_raw2['Codigo Legal de la Prestación']=pt_raw2['Codigo Legal de la Prestación'].astype(str).str.strip()
        dc=df[['COD','TARIFA_OFERTA_FINAL']].copy()
        dc['COD']=dc['COD'].astype(str).str.strip()
        mg=pt_raw2.merge(dc,left_on='Codigo Legal de la Prestación',right_on='COD',how='inner')
        mg['mas_barato']=mg['TARIFA_OFERTA_FINAL']>mg['Valor Contratado']
        sug=mg[mg['mas_barato']].groupby('Nombre Prestador').agg(cups=('COD','count'),tarifa=('Valor Contratado','mean')).reset_index().sort_values('cups',ascending=False)
        sug.columns=['Prestador','CUPS con tarifa menor','Tarifa prom. comparador']
        sug['Tarifa prom. comparador']=sug['Tarifa prom. comparador'].apply(fmt_cop)
        st.dataframe(sug,use_container_width=True,hide_index=True)

with tab4:
    st.markdown('<p class="section-title">📈 Análisis de Impacto — Filtros combinados</p>', unsafe_allow_html=True)

    fi1,fi2,fi3,fi4 = st.columns([1,1,1,1])
    with fi1:
        f_tipo_gest = st.selectbox("Tipo gestión",["Todos","Actualización tarifaria","Inclusión","Renovación","Nueva adscripción"])
    with fi2:
        f_sem_imp = st.selectbox("Semáforo ",["Todos","🟢 Verde","🟡 Amarillo","🔴 Rojo"])
    with fi3:
        f_esp_imp = st.selectbox("Especialidad ",["Todas"]+sorted(df['DESCRIPCION SERVICIO'].dropna().unique().tolist()))
    with fi4:
        f_reps_imp = st.selectbox("REPS ",["Todos","Válido","Inválido"])

    di = df.copy()
    if f_tipo_gest != "Todos" and 'Tipo de solicitud' in di.columns:
        di = di[di['Tipo de solicitud'].str.contains(f_tipo_gest, case=False, na=False)]
    if f_sem_imp == "🟢 Verde": di=di[di['SEMAFORO']=='verde']
    elif f_sem_imp == "🟡 Amarillo": di=di[di['SEMAFORO']=='amarillo']
    elif f_sem_imp == "🔴 Rojo": di=di[di['SEMAFORO']=='rojo']
    if f_esp_imp != "Todas": di=di[di['DESCRIPCION SERVICIO']==f_esp_imp]
    if f_reps_imp == "Válido": di=di[di['VALIDACION REPS']=='SI']
    elif f_reps_imp == "Inválido": di=di[di['VALIDACION REPS']=='NO']

    imp_fil = di['IMPACTO_OFERTA'].sum()
    imp_vi_fil = di['IMPACTO_VIGENTE'].sum()
    pct_fil = (imp_fil - imp_vi_fil) / imp_vi_fil if imp_vi_fil else 0
    c_fil = "#c0392b" if pct_fil>ur else "#f39c12" if pct_fil>ua else "#27ae60"

    ki1,ki2,ki3 = st.columns(3)
    with ki1:
        st.markdown(f'<div class="kpi-box" style="background:#fff;border:1px solid #ddd;border-top:3px solid {c_fil}"><p class="kpi-label">Impacto filtrado</p><p class="kpi-val" style="color:{c_fil}">${imp_fil/1e6:.1f}M</p><p class="kpi-sub">{pct_fil*100:+.1f}% vs vigente</p></div>', unsafe_allow_html=True)
    with ki2:
        st.markdown(f'<div class="kpi-box" style="background:#fff;border:1px solid #ddd;border-top:3px solid #185FA5"><p class="kpi-label">CUPS en filtro</p><p class="kpi-val" style="color:#185FA5">{len(di):,}</p><p class="kpi-sub">{len(di)/total*100:.1f}% del total</p></div>', unsafe_allow_html=True)
    with ki3:
        st.markdown(f'<div class="kpi-box" style="background:#fff;border:1px solid #ddd;border-top:3px solid {c_fil}"><p class="kpi-label">% Ponderado filtrado</p><p class="kpi-val" style="color:{c_fil}">{pct_fil*100:+.1f}%</p><p class="kpi-sub">promedio ponderado</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<p class="section-title">Análisis Pareto — Concentración del impacto</p>', unsafe_allow_html=True)
    pareto = di.groupby('DESCRIPCION SERVICIO').agg(impacto=('IMPACTO_OFERTA','sum')).reset_index().sort_values('impacto',ascending=False)
    pareto['% impacto'] = pareto['impacto']/pareto['impacto'].sum()*100
    pareto['% acumulado'] = pareto['% impacto'].cumsum()
    pareto['Especialidad'] = pareto['DESCRIPCION SERVICIO'].str[:35]

    fig_p = go.Figure()
    fig_p.add_trace(go.Bar(name='Impacto',x=pareto['Especialidad'],y=pareto['impacto'],marker_color='#e74c3c',yaxis='y'))
    fig_p.add_trace(go.Scatter(name='% Acumulado',x=pareto['Especialidad'],y=pareto['% acumulado'],mode='lines+markers',marker_color='#185FA5',yaxis='y2'))
    fig_p.add_hline(y=80,line_dash='dash',line_color='#f39c12',annotation_text='80%',yref='y2')
    fig_p.update_layout(
        height=380, margin=dict(l=0,r=10,t=10,b=120), xaxis_tickangle=-35,
        yaxis=dict(title='Impacto ($)'),
        yaxis2=dict(title='% Acumulado',overlaying='y',side='right',range=[0,105]),
        plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)',
        legend=dict(orientation='h',yanchor='bottom',y=1.02))
    st.plotly_chart(fig_p,use_container_width=True)

    st.markdown('<p class="section-title">Detalle por tipo de gestión</p>', unsafe_allow_html=True)
    if 'Tipo de solicitud' in di.columns:
        tipo_gest = di.groupby('Tipo de solicitud').agg(cups=('COD','count'),impacto=('IMPACTO_OFERTA','sum'),pct=('% Incremento','mean')).reset_index()
        tipo_gest['Impacto'] = tipo_gest['impacto'].apply(fmt_cop)
        tipo_gest['% Var'] = tipo_gest['pct'].apply(fmt_pct)
        st.dataframe(tipo_gest[['Tipo de solicitud','cups','Impacto','% Var']].rename(columns={'cups':'CUPS'}),use_container_width=True,hide_index=True)

with tab5:
    estado_inc = "superando" if pct_g>ua else "dentro de"
    st.markdown(f'<div class="obs-box" style="background:#fadbd8;border-left:4px solid #e74c3c"><p class="obs-title" style="color:#c0392b">🔴 Incremento general</p><p class="obs-text" style="color:#1a1a1a">Incremento ponderado de <strong>{pct_g*100:+.1f}%</strong>, {estado_inc} el umbral del {ua*100:.0f}% para esta regional.</p></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="obs-box" style="background:#fadbd8;border-left:4px solid #e74c3c"><p class="obs-title" style="color:#c0392b">🔴 REPS inválidos</p><p class="obs-text" style="color:#1a1a1a"><strong>{reps_inv:,} prestaciones</strong> no habilitadas ({reps_inv/total*100:.1f}%). No pueden incluirse hasta regularizar.</p></div>', unsafe_allow_html=True)
    dup = df['COD'].duplicated().sum()
    if dup>0:
        st.markdown(f'<div class="obs-box" style="background:#fef9e7;border-left:4px solid #f39c12"><p class="obs-title" style="color:#d68910">🟡 CUPS duplicados</p><p class="obs-text" style="color:#1a1a1a"><strong>{dup:,} prestaciones</strong> con códigos duplicados. Requieren verificación.</p></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="obs-box" style="background:#d6eaf8;border-left:4px solid #3498db"><p class="obs-title" style="color:#1a5276">🔵 Clasificación del análisis</p><p class="obs-text" style="color:#1a1a1a">{clasificacion}</p></div>', unsafe_allow_html=True)
    esp_ok = df.groupby('DESCRIPCION SERVICIO').agg(p=('% Incremento','mean')).reset_index()
    ok_list = esp_ok[esp_ok['p']<=ua]['DESCRIPCION SERVICIO'].tolist()
    if ok_list:
        st.markdown(f'<div class="obs-box" style="background:#d5f5e3;border-left:4px solid #27ae60"><p class="obs-title" style="color:#1e8449">🟢 Especialidades dentro de rango</p><p class="obs-text" style="color:#1a1a1a">{", ".join(ok_list[:5])}</p></div>', unsafe_allow_html=True)

    if top_criticos is not None and len(top_criticos) > 0:
        st.markdown("---")
        st.markdown('<p class="section-title">🎯 Orientación estratégica al negociador</p>', unsafe_allow_html=True)
        sem_decision = "🔴 Devolver" if sr>50 else "🟡 Negociar con ajustes" if sa>100 else "🟢 Aprobar"
        sem_color_bg = "#fadbd8" if sr>50 else "#fef9e7" if sa>100 else "#d5f5e3"
        sem_color_b = "#e74c3c" if sr>50 else "#f39c12" if sa>100 else "#27ae60"
        sem_color_t = "#c0392b" if sr>50 else "#d68910" if sa>100 else "#1e8449"
        st.markdown(f'<div style="background:{sem_color_bg};border:2px solid {sem_color_b};border-radius:10px;padding:16px;margin-bottom:12px"><p style="font-size:20px;font-weight:700;color:{sem_color_t};margin:0;text-align:center">{sem_decision}</p></div>', unsafe_allow_html=True)

        esp_df_obs = df.groupby('DESCRIPCION SERVICIO').agg(pct=('% Incremento','mean'),cups=('COD','count'),impacto_of=('IMPACTO_OFERTA','sum'),impacto_vi=('IMPACTO_VIGENTE','sum')).reset_index()
        esp_critica = esp_df_obs.sort_values('impacto_of', ascending=False).iloc[0] if len(esp_df_obs)>0 else None
        if esp_critica is not None:
            pct_cr = esp_critica['pct']
            pct_cr_str = f"{pct_cr*100:+.1f}%" if not pd.isna(pct_cr) else "—"
            st.markdown(f'<div class="obs-box" style="background:#fef0e7;border-left:4px solid #e67e22"><p class="obs-title" style="color:#a04000">💡 Especialidad más crítica: {esp_critica["DESCRIPCION SERVICIO"]}</p><p class="obs-text" style="color:#1a1a1a">Concentra el mayor impacto económico con un incremento de <strong>{pct_cr_str}</strong>. Se recomienda negociar a la baja los códigos con mayor score crítico en esta especialidad antes de aprobar la propuesta.</p></div>', unsafe_allow_html=True)

        st.markdown('<p style="font-size:13px;font-weight:600;color:#1a3a5c;margin:12px 0 8px 0">Top códigos a negociar prioritariamente:</p>', unsafe_allow_html=True)
        tc_obs = pd.DataFrame({
            'CUPS':top_criticos['COD'],
            'Descripción':top_criticos['DESCRIPCION CUPS'].str[:45],
            'Especialidad':top_criticos['DESCRIPCION SERVICIO'].str[:25],
            '% Inc':top_criticos['% Incremento'].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "—"),
            'Impacto':top_criticos['IMPACTO_OFERTA'].apply(fmt_cop),
            'Acción sugerida':top_criticos['% Incremento'].apply(lambda x: f"Reducir {abs(x*100-ua*100):.1f}% para llegar al umbral" if pd.notna(x) and x>ua else "✓ Dentro de rango")
        })
        st.dataframe(tc_obs,use_container_width=True,hide_index=True)

with tab6:
    st.markdown('<p class="section-title">📋 Trazabilidad de casos</p>', unsafe_allow_html=True)
    st.markdown(f'<div class="obs-box" style="background:#d5f5e3;border-left:4px solid #27ae60"><p class="obs-title" style="color:#1e8449">✅ Caso actual</p><p class="obs-text" style="color:#1a1a1a">ID: <strong>{id_caso}</strong> · {nom_p} · Regional {regional} · {datetime.now().strftime("%d %b %Y %H:%M")} · {clasificacion}</p></div>', unsafe_allow_html=True)

    # Filtros de historial
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        filtro_regional = st.selectbox("Filtrar por regional",
            ["Todas","Bogotá D.C.","Medellín","Cali","Barranquilla","Bucaramanga","Centro Oriente"])
    with col_f2:
        filtro_limit = st.selectbox("Mostrar últimos", [50, 100, 200, 500], index=1)

    if st.button("🔄 Cargar historial", use_container_width=True):
        reg_filtro = None if filtro_regional == "Todas" else filtro_regional
        df_traz = obtener_historial_db(regional=reg_filtro, limit=filtro_limit)
        if len(df_traz) > 0:
            st.dataframe(df_traz, use_container_width=True, hide_index=True, height=400)
            st.success(f"✅ {len(df_traz)} casos encontrados")
            st.markdown("---")
            hs1, hs2, hs3, hs4 = st.columns(4)
            with hs1:
                st.metric("Total casos", len(df_traz))
            with hs2:
                criticos = (df_traz['semaforo']=='🔴 Crítico').sum() if 'semaforo' in df_traz.columns else 0
                st.metric("Casos críticos", criticos)
            with hs3:
                regionales = df_traz['regional'].nunique() if 'regional' in df_traz.columns else 0
                st.metric("Regionales activas", regionales)
            with hs4:
                usuarios = df_traz['usuario'].nunique() if 'usuario' in df_traz.columns else 0
                st.metric("Usuarios activos", usuarios)
            st.download_button("⬇ Descargar historial",
                df_traz.to_csv(index=False).encode('utf-8'),
                "historial_att.csv", "text/csv")
        else:
            st.info("No hay casos registrados aún.")

    # Admin: estado de bases
    if es_admin_user:
        st.markdown("---")
        st.markdown("### ⚙️ Panel de administración")
        bases_cargadas = st.session_state.get('bases_cargadas', {})
        if bases_cargadas:
            st.markdown("**Estado de bases en el último análisis:**")
            for key, info in bases_cargadas.items():
                if info.get('ok'):
                    st.markdown(f"✅ **{key}** — {info.get('registros','')} registros")
                elif info.get('pendiente'):
                    st.markdown(f"🔴 **{key}** — Pendiente")
                else:
                    st.markdown(f"🟡 **{key}** — No disponible")
        st.markdown("---")
        st.markdown("**Configurar umbrales por regional:**")
        reg_admin = st.selectbox("Regional", ["Bogotá D.C.","Medellín","Cali","Barranquilla","Bucaramanga","Centro Oriente"], key="reg_admin")
        ua_admin = st.slider("Umbral amarillo (%)", 1, 30, int(UA_DEFAULT*100), key="ua_admin") / 100
        ur_admin = st.slider("Umbral rojo (%)", 5, 50, int(UR_DEFAULT*100), key="ur_admin") / 100
        if st.button("💾 Guardar umbrales", use_container_width=True):
            if guardar_umbrales_db(reg_admin, ua_admin, ur_admin, usuario_email):
                st.success(f"✅ Umbrales guardados para {reg_admin}")
            else:
                st.error("Error al guardar umbrales")

    st.markdown(f"[📊 Abrir Google Sheets](https://docs.google.com/spreadsheets/d/{SHEETS_ID}/edit)")
