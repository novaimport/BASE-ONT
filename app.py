# =====================================================================
# ONT MANAGER — ISP NOC  |  Backend: Google Sheets  |  Streamlit
# Usuario inicial: Admin / Admin2024@  (cámbialo al primer login)
# =====================================================================
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gspread
from google.oauth2.service_account import Credentials
import bcrypt, pytz, secrets, re, math
from datetime import datetime, date, timedelta
import calendar

# ─────────────────────────────────────────────────────────────────────
# 1. CONFIGURACIÓN GLOBAL
# ─────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ONT Manager · ISP",
    layout="wide",
    page_icon="📡",
    initial_sidebar_state="collapsed",
)

SV_TZ           = pytz.timezone('America/El_Salvador')
COLOR_PRIMARY   = '#f15c22'
COLOR_SECONDARY = '#1d2c59'
COLOR_TEAL      = '#29b09d'
COLOR_DANGER    = '#ff2b2b'
COLOR_WARN      = '#ff9f43'

MESES = ("Enero","Febrero","Marzo","Abril","Mayo","Junio",
         "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre")

SUPERADMIN    = 'Admin'
SUPERADMIN_PW = 'Admin2024@'

SH_USUARIOS = '_Usuarios'
SH_ZONAS    = '_Zonas'
SH_TECNICOS = '_Tecnicos'
SH_MOTIVOS  = '_Motivos'

DATA_COLS = [
    'ID','Fecha','Asesor','Tecnico','Zona',
    'SN_Eliminada','SN_Agregada','Motivo',
    'Cod_Cliente','Nombre_Cliente','Orden_Trabajo',
    'Descripcion','Timestamp','Eliminado'
]

USER_COLS = [
    'username','password_hash','role',
    'failed_attempts','locked_until','is_banned','session_token'
]

DEFAULT_MOTIVOS = [
    ("Instalacion Nueva",             "positive"),
    ("Reconexion",                    "positive"),
    ("Cambio de cable TV a DoblePlay","positive"),
    ("Desconexion",                   "negative"),
    ("Cambio por Inestabilidad",      "negative"),
    ("Cambio por ONT dañada",         "negative"),
    ("Cambio por lentitud",           "negative"),
    ("Cambio de Tecnologia",          "neutral"),
    ("Cambio por Renovacion",         "neutral"),
]

DEFAULT_ZONAS = [
    "El Rosario","ARG","Tepezontes","La Libertad","El Tunco",
    "Costa del Sol","Zacatecoluca","Zaragoza","Santiago Nonualco",
    "Rio Mar","San Salvador"
]

DEFAULT_TECNICOS = ["Tecnico 01","Tecnico 02","Tecnico 03"]

TIPO_LABEL = {
    'positive': '✅ Positivo (Instalac. / Reconex.)',
    'negative': '⚠️ Negativo (Problema / Desconex.)',
    'neutral':  '🔄 Neutral (Cambio)',
}
TIPO_COLOR = {'positive': COLOR_TEAL, 'negative': COLOR_DANGER, 'neutral': COLOR_WARN}
TIPO_EMOJI = {'positive': '🟢', 'negative': '🔴', 'neutral': '🟡'}

# ─────────────────────────────────────────────────────────────────────
# 2. CSS
# ─────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
div.stButton>button{border:none!important;border-radius:8px;width:100%;
  font-weight:600;transition:all .3s ease}
div.stButton>button:hover{transform:translateY(-2px)}
[data-testid="stMetricValue"]{color:#fff!important;font-size:30px!important;font-weight:800!important}
[data-testid="stMetricLabel"]{color:#a5a8b5!important;font-size:13px!important}
button[data-baseweb="tab"]{background:#1e1e2f!important;
  border-radius:12px 12px 0 0!important;margin-right:8px!important;
  padding:13px 26px!important;border:2px solid #333!important;border-bottom:none!important}
button[data-baseweb="tab"]:hover{background:#2a2a3f!important}
button[data-baseweb="tab"][aria-selected="true"]{background:#f15c22!important;border-color:#f15c22!important}
button[data-baseweb="tab"] p{font-size:15px!important;font-weight:700!important;color:#a5a8b5!important;margin:0}
button[data-baseweb="tab"][aria-selected="true"] p{color:#fff!important}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────
# 3. SESSION STATE
# ─────────────────────────────────────────────────────────────────────
_DEFAULTS = {
    'logged_in': False, 'role': '', 'username': '',
    'log_u': '', 'log_p': '', 'log_err': '',
    'flash_msg': '', 'flash_type': '',
    'data_version': 0, 'session_token': '', 'form_reset': 0,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

if st.session_state.flash_msg:
    if st.session_state.flash_type == 'error':
        st.error(st.session_state.flash_msg, icon="❌")
    else:
        st.toast(st.session_state.flash_msg, icon="✅")
    st.session_state.flash_msg = ''
    st.session_state.flash_type = ''

def _dv():    return st.session_state.get('data_version', 0)
def _inv():   st.session_state.data_version = _dv() + 1
def _flash(msg, kind='success'):
    st.session_state.flash_msg = msg
    st.session_state.flash_type = kind

# ─────────────────────────────────────────────────────────────────────
# 4. GOOGLE SHEETS CONNECTION
# ─────────────────────────────────────────────────────────────────────
_SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]

@st.cache_resource
def _gc():
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]), scopes=_SCOPES)
    return gspread.authorize(creds)

@st.cache_resource
def _ss():
    return _gc().open_by_key(st.secrets["sheets"]["spreadsheet_id"])

def _ws(name: str) -> gspread.Worksheet:
    return _ss().worksheet(name)

def _get_or_create_ws(name: str, headers: list) -> gspread.Worksheet:
    ss = _ss()
    try:
        return ss.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=name, rows=2000, cols=len(headers) + 2)
        ws.append_row(headers)
        return ws

# ─────────────────────────────────────────────────────────────────────
# 5. PASSWORD HELPERS
# ─────────────────────────────────────────────────────────────────────
def _hash(p: str) -> str:
    return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()

def _check(p: str, h: str) -> bool:
    try:
        return bcrypt.checkpw(p.encode(), h.encode())
    except Exception:
        return False

def _validate_pw(p: str):
    if len(p) < 8:
        return "Mínimo 8 caracteres."
    if not re.search(r'[A-Z]', p):
        return "Necesita al menos una mayúscula."
    if not re.search(r'[a-z]', p):
        return "Necesita al menos una minúscula."
    if not re.search(r'\d', p):
        return "Necesita al menos un número."
    if not re.search(r'[!@#$%^&*()\-_=+\[\]{};:\'",.<>/?`~\\|]', p):
        return "Necesita al menos un carácter especial."
    return None

# ─────────────────────────────────────────────────────────────────────
# 6. INIT SYSTEM (se ejecuta una sola vez al arrancar)
# ─────────────────────────────────────────────────────────────────────
def init_system():
    ss   = _ss()
    existing = {ws.title for ws in ss.worksheets()}

    # ── Usuarios ──
    if SH_USUARIOS not in existing:
        ws_u = ss.add_worksheet(SH_USUARIOS, rows=500, cols=10)
        ws_u.append_row(USER_COLS)
        ws_u.append_row([SUPERADMIN, _hash(SUPERADMIN_PW), 'admin', 0, '', 'FALSE', ''])
    else:
        ws_u   = ss.worksheet(SH_USUARIOS)
        data_u = ws_u.get_all_records()
        if not any(r.get('username') == SUPERADMIN for r in data_u):
            ws_u.append_row([SUPERADMIN, _hash(SUPERADMIN_PW), 'admin', 0, '', 'FALSE', ''])

    # ── Zonas ──
    if SH_ZONAS not in existing:
        ws_z = ss.add_worksheet(SH_ZONAS, rows=200, cols=2)
        ws_z.append_row(['Zona'])
        for z in DEFAULT_ZONAS:
            ws_z.append_row([z])

    # ── Técnicos ──
    if SH_TECNICOS not in existing:
        ws_t = ss.add_worksheet(SH_TECNICOS, rows=200, cols=2)
        ws_t.append_row(['Tecnico'])
        for t in DEFAULT_TECNICOS:
            ws_t.append_row([t])

    # ── Motivos ──
    if SH_MOTIVOS not in existing:
        ws_m = ss.add_worksheet(SH_MOTIVOS, rows=200, cols=2)
        ws_m.append_row(['Motivo', 'Tipo'])
        for mot, tipo in DEFAULT_MOTIVOS:
            ws_m.append_row([mot, tipo])

try:
    init_system()
except Exception as _ei:
    st.error(f"⚠️ Error inicializando el sistema: {_ei}")

# ─────────────────────────────────────────────────────────────────────
# 7. CATÁLOGOS (cacheados)
# ─────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def get_zonas(v=0) -> list:
    try:
        data = _ws(SH_ZONAS).get_all_records()
        return [r['Zona'] for r in data if r.get('Zona')] or DEFAULT_ZONAS[:]
    except Exception:
        return DEFAULT_ZONAS[:]

@st.cache_data(ttl=300, show_spinner=False)
def get_tecnicos(v=0) -> list:
    try:
        data = _ws(SH_TECNICOS).get_all_records()
        return [r['Tecnico'] for r in data if r.get('Tecnico')] or DEFAULT_TECNICOS[:]
    except Exception:
        return DEFAULT_TECNICOS[:]

@st.cache_data(ttl=300, show_spinner=False)
def get_motivos_df(v=0) -> pd.DataFrame:
    try:
        data = _ws(SH_MOTIVOS).get_all_records()
        df   = pd.DataFrame(data)
        if not df.empty and 'Motivo' in df.columns:
            return df
        return pd.DataFrame(DEFAULT_MOTIVOS, columns=['Motivo', 'Tipo'])
    except Exception:
        return pd.DataFrame(DEFAULT_MOTIVOS, columns=['Motivo', 'Tipo'])

def get_motivos_list(v=0) -> list:
    return get_motivos_df(v=v)['Motivo'].tolist()

def get_tipo_map(v=0) -> dict:
    df = get_motivos_df(v=v)
    if 'Tipo' in df.columns:
        return dict(zip(df['Motivo'], df['Tipo']))
    return {m: t for m, t in DEFAULT_MOTIVOS}

def clear_cats():
    get_zonas.clear()
    get_tecnicos.clear()
    get_motivos_df.clear()

# ─────────────────────────────────────────────────────────────────────
# 8. USER MANAGEMENT
# ─────────────────────────────────────────────────────────────────────
def _get_users_raw() -> list:
    try:
        return _ws(SH_USUARIOS).get_all_records()
    except Exception:
        return []

def _find_user(username: str):
    """Returns (row_idx_1based, user_dict) or (None, None)."""
    try:
        ws      = _ws(SH_USUARIOS)
        records = ws.get_all_records()
        for i, r in enumerate(records, start=2):
            if r.get('username') == username:
                return i, r, ws
        return None, None, ws
    except Exception:
        return None, None, None

def _update_user_fields(username: str, fields: dict) -> bool:
    try:
        ws     = _ws(SH_USUARIOS)
        header = ws.row_values(1)
        records = ws.get_all_records()
        for i, r in enumerate(records, start=2):
            if r.get('username') == username:
                for field, value in fields.items():
                    if field in header:
                        col = header.index(field) + 1
                        ws.update_cell(i, col, str(value) if value is not None else '')
                return True
        return False
    except Exception:
        return False

@st.cache_data(ttl=15, show_spinner=False)
def _get_db_token(username: str, v=0) -> str:
    _, user, _ = _find_user(username)
    return str(user.get('session_token', '')) if user else ''

# ─────────────────────────────────────────────────────────────────────
# 9. LOGIN
# ─────────────────────────────────────────────────────────────────────
def do_login():
    u = st.session_state.log_u
    p = st.session_state.log_p

    row_i, user, _ = _find_user(u)
    if not user:
        st.session_state.log_err = "❌ Credenciales incorrectas."
        st.session_state.log_u = st.session_state.log_p = ''
        return

    now       = datetime.now(SV_TZ).replace(tzinfo=None)
    is_banned = str(user.get('is_banned', 'FALSE')).upper() == 'TRUE'
    fa        = int(user.get('failed_attempts', 0) or 0)
    lu_str    = str(user.get('locked_until', ''))

    if lu_str:
        try:
            lu = datetime.fromisoformat(lu_str)
            if lu > now:
                mins = int((lu - now).total_seconds() // 60) + 1
                st.session_state.log_err = f"⏳ Cuenta bloqueada por {mins} min."
                return
        except Exception:
            pass

    if is_banned:
        st.session_state.log_err = "❌ Cuenta baneada permanentemente."
        return

    if _check(p, str(user.get('password_hash', ''))):
        token = secrets.token_hex(32)
        _update_user_fields(u, {
            'failed_attempts': 0, 'locked_until': '', 'session_token': token
        })
        _get_db_token.clear()
        st.session_state.update({
            'logged_in': True, 'role': str(user.get('role', 'viewer')),
            'username': u, 'log_err': '', 'session_token': token,
        })
    else:
        fa += 1
        if fa >= 6:
            _update_user_fields(u, {'is_banned': 'TRUE', 'failed_attempts': fa})
            st.session_state.log_err = "❌ Cuenta baneada permanentemente."
        elif fa % 3 == 0:
            lock_until = (now + timedelta(minutes=5)).isoformat()
            _update_user_fields(u, {'locked_until': lock_until, 'failed_attempts': fa})
            st.session_state.log_err = "⏳ Demasiados intentos. Bloqueado 5 min."
        else:
            _update_user_fields(u, {'failed_attempts': fa})
            st.session_state.log_err = "❌ Credenciales incorrectas."
        st.session_state.log_u = st.session_state.log_p = ''

# ── Pantalla de login ──
if not st.session_state.logged_in:
    st.markdown("<div style='margin-top:14vh;'></div>", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        with st.container(border=True):
            st.markdown("""
            <div style='text-align:center;padding:20px 0 12px'>
                <div style='font-size:52px'>📡</div>
                <h2 style='color:#fff;font-weight:700;margin:8px 0 4px'>ONT Manager</h2>
                <p style='color:#a5a8b5;margin:0'>ISP Network Operations</p>
            </div>""", unsafe_allow_html=True)
            if st.session_state.log_err:
                st.error(st.session_state.log_err, icon="⚠️")
                st.session_state.log_err = ''
            st.text_input("Usuario", key="log_u")
            st.text_input("Contraseña", key="log_p", type="password")
            st.button("Iniciar Sesión", type="primary",
                      on_click=do_login, use_container_width=True)
            st.caption("Contacta al administrador para gestionar tu acceso.")
    st.stop()

# ── Validación de sesión única ──
_local_tok = st.session_state.get('session_token', '')
if _local_tok:
    _db_tok = _get_db_token(st.session_state.username, v=_dv())
    if _db_tok and _db_tok != _local_tok:
        for _k, _v in _DEFAULTS.items():
            st.session_state[_k] = _v
        _flash("⚠️ Tu sesión fue cerrada porque el mismo usuario inició sesión desde otro dispositivo.", 'error')
        st.rerun()

# ─────────────────────────────────────────────────────────────────────
# 10. DATA FUNCTIONS (Google Sheets ↔ ONT Records)
# ─────────────────────────────────────────────────────────────────────
def _month_name(y: int, m: int) -> str:
    return f"{MESES[m - 1]} {y}"

def _get_month_ws(y: int, m: int) -> gspread.Worksheet:
    return _get_or_create_ws(_month_name(y, m), DATA_COLS)

@st.cache_data(ttl=60, show_spinner=False)
def load_month_data(y: int, m: int, v=0) -> pd.DataFrame:
    try:
        data = _ws(_month_name(y, m)).get_all_records()
        if not data:
            return pd.DataFrame(columns=DATA_COLS)
        df = pd.DataFrame(data)
        # Exclude soft-deleted rows
        if 'Eliminado' in df.columns:
            df = df[df['Eliminado'].astype(str).str.upper() != 'TRUE'].copy()
        return df
    except gspread.WorksheetNotFound:
        return pd.DataFrame(columns=DATA_COLS)
    except Exception:
        return pd.DataFrame(columns=DATA_COLS)

@st.cache_data(ttl=60, show_spinner=False)
def load_year_data(y: int, v=0) -> pd.DataFrame:
    dfs = []
    for m in range(1, 13):
        df = load_month_data(y, m, v=v)
        if not df.empty:
            df = df.copy()
            df['_mes']   = m
            df['_mes_nm'] = MESES[m - 1]
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=DATA_COLS)

def append_ont_record(record: dict) -> bool:
    try:
        fecha_str = record.get('Fecha', '')
        dt  = datetime.strptime(fecha_str, '%Y-%m-%d') if isinstance(fecha_str, str) else fecha_str
        ws  = _get_month_ws(dt.year, dt.month)
        n   = len(ws.get_all_values())  # includes header → n-1 existing rows → ID = n
        row = [
            n,                                        # ID
            record.get('Fecha', ''),
            record.get('Asesor', ''),
            record.get('Tecnico', ''),
            record.get('Zona', ''),
            record.get('SN_Eliminada', ''),
            record.get('SN_Agregada', ''),
            record.get('Motivo', ''),
            record.get('Cod_Cliente', ''),
            record.get('Nombre_Cliente', ''),
            record.get('Orden_Trabajo', ''),
            record.get('Descripcion', ''),
            datetime.now(SV_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            'FALSE',
        ]
        ws.append_row(row)
        load_month_data.clear()
        load_year_data.clear()
        return True
    except Exception as e:
        st.error(f"Error guardando registro: {e}")
        return False

def soft_delete_record(y: int, m: int, record_id) -> bool:
    try:
        ws     = _ws(_month_name(y, m))
        vals   = ws.get_all_values()
        header = vals[0] if vals else []
        if 'Eliminado' not in header or 'ID' not in header:
            return False
        elim_col = header.index('Eliminado') + 1
        id_col   = header.index('ID') + 1
        for i, row in enumerate(vals[1:], start=2):
            if len(row) >= max(elim_col, id_col) and str(row[id_col - 1]) == str(record_id):
                ws.update_cell(i, elim_col, 'TRUE')
                load_month_data.clear()
                load_year_data.clear()
                return True
        return False
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────
# 11. MÉTRICAS
# ─────────────────────────────────────────────────────────────────────
def calc_metrics(df: pd.DataFrame, tipo_map: dict) -> dict:
    base = {
        'total': 0, 'pos': 0, 'neg': 0, 'neu': 0, 'balance': 0,
        'por_motivo': {}, 'por_zona': {}, 'por_tecnico': {}, 'por_asesor': {},
        'por_zona_tipo': pd.DataFrame(),
    }
    if df.empty:
        return base

    total = len(df)
    pm    = df['Motivo'].value_counts().to_dict()   if 'Motivo'   in df.columns else {}
    pz    = df['Zona'].value_counts().to_dict()     if 'Zona'     in df.columns else {}
    pt    = df['Tecnico'].value_counts().to_dict()  if 'Tecnico'  in df.columns else {}
    pa    = df['Asesor'].value_counts().to_dict()   if 'Asesor'   in df.columns else {}

    pos = sum(v for k, v in pm.items() if tipo_map.get(k, 'neutral') == 'positive')
    neg = sum(v for k, v in pm.items() if tipo_map.get(k, 'neutral') == 'negative')
    neu = total - pos - neg

    # Per-zone by tipo
    pzt = pd.DataFrame()
    if 'Zona' in df.columns and 'Motivo' in df.columns:
        df2        = df.copy()
        df2['Tipo'] = df2['Motivo'].map(lambda x: tipo_map.get(x, 'neutral').capitalize())
        pzt         = df2.groupby(['Zona', 'Tipo']).size().reset_index(name='N')

    return {
        'total': total, 'pos': pos, 'neg': neg, 'neu': neu, 'balance': pos - neg,
        'por_motivo': pm, 'por_zona': pz, 'por_tecnico': pt, 'por_asesor': pa,
        'por_zona_tipo': pzt,
    }

def _delta_str(cur, prv) -> str:
    d = cur - prv
    return ('+' if d >= 0 else '') + str(d)

def _delta_color(cur, prv, tipo: str) -> str:
    """
    positive motives: more = better → normal (verde si sube)
    negative motives: less = better → inverse (verde si baja)
    neutral: normal
    """
    if tipo == 'negative':
        return 'inverse'
    return 'normal'

# ─────────────────────────────────────────────────────────────────────
# 12. SIDEBAR
# ─────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.caption(
        f"👤 **{st.session_state.username}** "
        f"({st.session_state.role.capitalize()})  |  ONT Manager v2.0"
    )
    st.divider()

    now_sv  = datetime.now(SV_TZ)
    anio    = st.selectbox("🗓️ Año",   [now_sv.year, now_sv.year - 1], index=0)
    mes_sel = st.selectbox("📅 Mes",   list(MESES), index=now_sv.month - 1)
    m_idx   = MESES.index(mes_sel) + 1

    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True):
        try:
            _update_user_fields(st.session_state.username, {'session_token': ''})
        except Exception:
            pass
        _get_db_token.clear()
        st.session_state.clear()
        st.rerun()

    st.markdown("""
        <div style='margin-top:60px;text-align:center;color:#555;font-size:11px'>
            📡 ISP ONT Manager<br>Powered by Google Sheets
        </div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────
# 13. TABS
# ─────────────────────────────────────────────────────────────────────
role = st.session_state.role

if   role == 'admin':   _tabs = ["📊 Dashboard","📝 Registrar","🗂️ Historial","⚙️ Configuración"]
elif role == 'auditor': _tabs = ["📊 Dashboard","📝 Registrar","🗂️ Historial"]
else:                   _tabs = ["📊 Dashboard"]

tabs  = st.tabs(_tabs)
t_idx = 0

# ═════════════════════════════════════════════════════════════════════
# TAB 0 — DASHBOARD
# ═════════════════════════════════════════════════════════════════════
with tabs[t_idx]:
    st.title(f"📊 Dashboard ONT — {mes_sel} {anio}")

    tipo_map = get_tipo_map(v=_dv())

    # Load current + previous month
    df_cur  = load_month_data(anio, m_idx, v=_dv())
    pm_anio = anio - 1 if m_idx == 1 else anio
    pm_mes  = 12 if m_idx == 1 else m_idx - 1
    df_prev = load_month_data(pm_anio, pm_mes, v=_dv())

    kpi  = calc_metrics(df_cur,  tipo_map)
    kpip = calc_metrics(df_prev, tipo_map)

    # ── KPIs globales ──
    st.markdown("### 📈 Resumen del Mes")
    st.caption(f"*Variación vs {MESES[pm_mes - 1]} {pm_anio}*")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📦 Total Operaciones",        kpi['total'],
              _delta_str(kpi['total'],   kpip['total']),   delta_color='normal')
    c2.metric("✅ Positivas (Inst./Reconex.)", kpi['pos'],
              _delta_str(kpi['pos'],     kpip['pos']),     delta_color=_delta_color(kpi['pos'],kpip['pos'],'positive'))
    c3.metric("⚠️ Negativas (Prob./Descon.)",  kpi['neg'],
              _delta_str(kpi['neg'],     kpip['neg']),     delta_color=_delta_color(kpi['neg'],kpip['neg'],'negative'))
    c4.metric("🔄 Neutrales (Cambios)",        kpi['neu'],
              _delta_str(kpi['neu'],     kpip['neu']),     delta_color='normal')
    c5.metric("⚖️ Balance Neto (+ vs −)",      kpi['balance'],
              _delta_str(kpi['balance'], kpip['balance']), delta_color=_delta_color(kpi['balance'],kpip['balance'],'positive'))

    if df_cur.empty:
        st.info("ℹ️ No hay registros para este mes. Comienza registrando un movimiento.")
    else:
        # ── Por motivo ──
        st.divider()
        st.markdown("### 📋 Desglose por Motivo")
        col_mot, col_pie = st.columns(2)

        with col_mot:
            pm_df = (pd.DataFrame(list(kpi['por_motivo'].items()), columns=['Motivo','Cantidad'])
                     .sort_values('Cantidad', ascending=True))
            pm_df['Color'] = pm_df['Motivo'].map(
                lambda x: TIPO_COLOR.get(tipo_map.get(x, 'neutral'), COLOR_WARN))
            fig_mot = px.bar(pm_df, x='Cantidad', y='Motivo', orientation='h',
                             text_auto=True, title="Operaciones por Motivo")
            fig_mot.update_traces(marker_color=pm_df['Color'].tolist())
            fig_mot.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=370,
                                  margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_mot, use_container_width=True)

        with col_pie:
            pie_df = pd.DataFrame({
                'Tipo':     ['Positivo','Negativo','Neutral'],
                'Cantidad': [kpi['pos'], kpi['neg'], kpi['neu']],
            })
            fig_pie = px.pie(pie_df, names='Tipo', values='Cantidad', hole=0.5,
                             color_discrete_sequence=[COLOR_TEAL, COLOR_DANGER, COLOR_WARN],
                             title="Balance por Tipo")
            fig_pie.update_traces(textinfo='percent+label', textposition='inside')
            fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=370,
                                  showlegend=False)
            st.plotly_chart(fig_pie, use_container_width=True)

        # ── Por zona ──
        st.divider()
        st.markdown("### 🗺️ Análisis por Zona")
        col_z1, col_z2 = st.columns(2)

        with col_z1:
            z_df = (pd.DataFrame(list(kpi['por_zona'].items()), columns=['Zona','Operaciones'])
                    .sort_values('Operaciones', ascending=False))
            fig_z = px.bar(z_df, x='Zona', y='Operaciones', text_auto=True,
                           color='Operaciones', color_continuous_scale='Blues',
                           title="Total Operaciones por Zona")
            fig_z.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=360,
                                margin=dict(l=0, r=0, t=40, b=0), xaxis_tickangle=-30)
            st.plotly_chart(fig_z, use_container_width=True)

        with col_z2:
            if not kpi['por_zona_tipo'].empty:
                fig_zt = px.bar(
                    kpi['por_zona_tipo'], x='Zona', y='N', color='Tipo',
                    barmode='stack', text_auto=True,
                    color_discrete_map={
                        'Positive': COLOR_TEAL,
                        'Negative': COLOR_DANGER,
                        'Neutral':  COLOR_WARN,
                    },
                    title="Tipo de Operación por Zona",
                )
                fig_zt.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=360,
                                     margin=dict(l=0, r=0, t=40, b=0), xaxis_tickangle=-30)
                st.plotly_chart(fig_zt, use_container_width=True)

        # ── Técnico y Asesor ──
        st.divider()
        col_tec, col_as = st.columns(2)

        with col_tec:
            if kpi['por_tecnico']:
                tec_df = (pd.DataFrame(list(kpi['por_tecnico'].items()),
                                       columns=['Técnico', 'Operaciones'])
                          .sort_values('Operaciones', ascending=True))
                fig_tec = px.bar(tec_df, x='Operaciones', y='Técnico', orientation='h',
                                 text_auto=True, color_discrete_sequence=[COLOR_PRIMARY],
                                 title="🔧 Operaciones por Técnico de Campo")
                fig_tec.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=340,
                                      margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_tec, use_container_width=True)

        with col_as:
            if kpi['por_asesor']:
                as_df = (pd.DataFrame(list(kpi['por_asesor'].items()),
                                      columns=['Asesor', 'Registros'])
                         .sort_values('Registros', ascending=True))
                fig_as = px.bar(as_df, x='Registros', y='Asesor', orientation='h',
                                text_auto=True, color_discrete_sequence=[COLOR_TEAL],
                                title="👤 Registros por Asesor (Quién ingresó los datos)")
                fig_as.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=340,
                                     margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_as, use_container_width=True)

        # ── Balance Instalaciones vs Desconexiones ──
        st.divider()
        st.markdown("### ⚖️ Balance: Instalaciones & Reconexiones vs Desconexiones")
        b_col1, b_col2, b_col3 = st.columns(3)

        inst_rc = kpi['por_motivo'].get('Instalacion Nueva', 0) + kpi['por_motivo'].get('Reconexion', 0)
        descon  = kpi['por_motivo'].get('Desconexion', 0)
        neto    = inst_rc - descon

        b_col1.metric("📥 Inst. + Reconexiones", inst_rc)
        b_col2.metric("📤 Desconexiones",         descon)
        color_neto = "normal" if neto >= 0 else "inverse"
        b_col3.metric("⚖️ Neto de Clientes",      neto,
                      f"{'▲' if neto >= 0 else '▼'} {abs(neto)}",
                      delta_color=color_neto)

        # ── Comparación entre meses ──
        st.divider()
        st.markdown("### 📅 Comparación entre Meses")

        with st.expander("🔍 Configurar comparación", expanded=True):
            cc1, cc2 = st.columns([3, 1])
            meses_sel = cc1.multiselect(
                "Selecciona los meses a comparar",
                options=list(MESES),
                default=[MESES[pm_mes - 1], mes_sel],
                max_selections=6,
            )
            anio_comp = cc2.selectbox("Año", [anio, anio - 1], key="anio_comp")

        if len(meses_sel) >= 2:
            comp_rows = []
            for mc in meses_sel:
                mi = MESES.index(mc) + 1
                dfc = load_month_data(anio_comp, mi, v=_dv())
                km  = calc_metrics(dfc, tipo_map)
                comp_rows.append({
                    'Mes': mc, 'Total': km['total'],
                    'Positivos': km['pos'], 'Negativos': km['neg'],
                    'Neutrales': km['neu'], 'Balance': km['balance'],
                })
            df_comp = pd.DataFrame(comp_rows)

            fig_comp = px.bar(
                df_comp.melt(id_vars='Mes',
                             value_vars=['Positivos', 'Negativos', 'Neutrales']),
                x='Mes', y='value', color='variable', barmode='group',
                text_auto=True,
                color_discrete_map={
                    'Positivos': COLOR_TEAL,
                    'Negativos': COLOR_DANGER,
                    'Neutrales': COLOR_WARN,
                },
                category_orders={'Mes': meses_sel},
                title=f"Comparativo Mensual — {anio_comp}",
            )
            fig_comp.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=400,
                                   margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_comp, use_container_width=True)

            # Delta table
            st.markdown("**📉📈 Variación entre meses consecutivos seleccionados:**")
            for i in range(1, len(comp_rows)):
                cur_c = comp_rows[i]
                prv_c = comp_rows[i - 1]

                def _cell(label, cur_v, prv_v, tipo):
                    d   = cur_v - prv_v
                    sig = '+' if d >= 0 else ''
                    if   tipo == 'positive': clr = COLOR_TEAL   if d >= 0 else COLOR_DANGER
                    elif tipo == 'negative': clr = COLOR_TEAL   if d <= 0 else COLOR_DANGER
                    else:                    clr = COLOR_WARN
                    return (f"<div style='text-align:center'><b style='font-size:12px;"
                            f"color:#a5a8b5'>{label}</b><br>"
                            f"<span style='color:{clr};font-size:22px;font-weight:800'>"
                            f"{cur_v}</span><br>"
                            f"<span style='color:{clr};font-size:13px'>({sig}{d})</span></div>")

                rc1, rc2, rc3, rc4, rc5 = st.columns(5)
                rc1.markdown(
                    f"<div style='padding-top:14px;font-weight:700;"
                    f"color:#a5a8b5;font-size:13px'>"
                    f"📆 {cur_c['Mes']}<br><span style='color:#555'>vs {prv_c['Mes']}</span></div>",
                    unsafe_allow_html=True,
                )
                rc2.markdown(_cell("✅ Positivos",  cur_c['Positivos'], prv_c['Positivos'], 'positive'), unsafe_allow_html=True)
                rc3.markdown(_cell("⚠️ Negativos",  cur_c['Negativos'], prv_c['Negativos'], 'negative'), unsafe_allow_html=True)
                rc4.markdown(_cell("🔄 Neutrales",  cur_c['Neutrales'], prv_c['Neutrales'], 'neutral'),  unsafe_allow_html=True)
                rc5.markdown(_cell("⚖️ Balance",    cur_c['Balance'],   prv_c['Balance'],   'positive'), unsafe_allow_html=True)
                st.divider()

        # ── Resumen Anual ──
        st.divider()
        st.markdown(f"### 📆 Tendencia Anual — {anio}")
        with st.spinner("Cargando datos anuales…"):
            df_year = load_year_data(anio, v=_dv())

        if not df_year.empty:
            annual_rows = []
            for mi in range(1, 13):
                df_mi = df_year[df_year['_mes'] == mi] if '_mes' in df_year.columns else pd.DataFrame()
                km    = calc_metrics(df_mi, tipo_map)
                annual_rows.append({
                    'Mes':       MESES[mi - 1],
                    'Total':     km['total'],
                    'Positivos': km['pos'],
                    'Negativos': km['neg'],
                    'Balance':   km['balance'],
                })
            df_annual = pd.DataFrame(annual_rows)
            fig_ann   = px.line(
                df_annual, x='Mes',
                y=['Total', 'Positivos', 'Negativos', 'Balance'],
                markers=True,
                color_discrete_map={
                    'Total':     COLOR_PRIMARY,
                    'Positivos': COLOR_TEAL,
                    'Negativos': COLOR_DANGER,
                    'Balance':   '#83c9ff',
                },
                title=f"Tendencia de Operaciones ONT — {anio}",
                category_orders={'Mes': list(MESES)},
            )
            fig_ann.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=380,
                                  margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_ann, use_container_width=True)

t_idx += 1

# ═════════════════════════════════════════════════════════════════════
# TAB 1 — REGISTRAR
# ═════════════════════════════════════════════════════════════════════
if role in ('admin', 'auditor'):
    with tabs[t_idx]:
        st.title("📝 Registrar Movimiento de ONT")
        fk = st.session_state.form_reset

        zonas_l = get_zonas(v=_dv())
        tecs_l  = get_tecnicos(v=_dv())
        mots_l  = get_motivos_list(v=_dv())
        today   = datetime.now(SV_TZ).date()

        with st.container(border=True):
            st.info(
                f"👤 **Asesor registrado automáticamente:** `{st.session_state.username}`  "
                f"|  📅 **Hoy:** `{today.strftime('%d/%m/%Y')}`"
            )

            cc1, cc2 = st.columns(2)
            tec    = cc1.selectbox("🔧 Técnico de Campo *", tecs_l, key=f"tec_{fk}")
            zona   = cc2.selectbox("🗺️ Zona *",            zonas_l, key=f"zon_{fk}")
            motivo = st.selectbox("📋 Motivo / Tipo de Operación *", mots_l, key=f"mot_{fk}")

            tipo_sel = tipo_map.get(motivo, 'neutral')
            if   tipo_sel == 'positive': st.success(f"✅ {TIPO_LABEL['positive']}")
            elif tipo_sel == 'negative': st.warning(f"⚠️ {TIPO_LABEL['negative']}")
            else:                        st.info(f"🔄 {TIPO_LABEL['neutral']}")

            st.divider()
            sc1, sc2 = st.columns(2)
            sn_elim = sc1.text_input("🔴 SN ONT Retirada / Eliminada (dejar en blanco si no aplica)", key=f"sne_{fk}")
            sn_agr  = sc2.text_input("🟢 SN ONT Instalada / Agregada (dejar en blanco si no aplica)", key=f"sna_{fk}")

            st.divider()
            dc1, dc2, dc3 = st.columns(3)
            cod_cl   = dc1.text_input("🆔 Código de Cliente *",              key=f"cod_{fk}")
            nom_cl   = dc2.text_input("👤 Nombre Completo del Cliente *",    key=f"nom_{fk}")
            ord_trab = dc3.text_input("📄 N° de Orden de Trabajo *",         key=f"ot_{fk}")

            desc       = st.text_area("📝 Descripción Adicional (Opcional)", key=f"desc_{fk}", height=80)
            fecha_reg  = st.date_input("📅 Fecha del Movimiento", value=today, key=f"fecha_{fk}")

            st.write("")
            if st.button("💾 Guardar Registro", type="primary", use_container_width=True):
                if not all([tec, zona, motivo, cod_cl, nom_cl, ord_trab]):
                    _flash("❌ Completa todos los campos obligatorios (*) antes de guardar.", 'error')
                    st.rerun()
                else:
                    record = {
                        'Fecha':          fecha_reg.strftime('%Y-%m-%d'),
                        'Asesor':         st.session_state.username,
                        'Tecnico':        tec,
                        'Zona':           zona,
                        'SN_Eliminada':   sn_elim.strip(),
                        'SN_Agregada':    sn_agr.strip(),
                        'Motivo':         motivo,
                        'Cod_Cliente':    cod_cl.strip(),
                        'Nombre_Cliente': nom_cl.strip(),
                        'Orden_Trabajo':  ord_trab.strip(),
                        'Descripcion':    desc.strip(),
                    }
                    with st.spinner("Guardando en Google Sheets…"):
                        ok = append_ont_record(record)
                    if ok:
                        _inv()
                        st.session_state.form_reset += 1
                        _flash("✅ Registro guardado exitosamente en Google Sheets.")
                        st.rerun()

    t_idx += 1

# ═════════════════════════════════════════════════════════════════════
# TAB 2 — HISTORIAL
# ═════════════════════════════════════════════════════════════════════
if role in ('admin', 'auditor'):
    with tabs[t_idx]:
        st.markdown("### 🗂️ Historial de Movimientos")

        hc1, hc2 = st.columns([2, 1])
        with hc1:
            ver_todo = st.toggle("📆 Ver todos los meses del año seleccionado")
        with hc2:
            bq = st.text_input("🔎 Buscar:", placeholder="Técnico, SN, nombre cliente…")

        if ver_todo:
            with st.spinner("Cargando historial anual…"):
                df_hist = load_year_data(anio, v=_dv())
            st.caption(f"Mostrando todos los registros de {anio}")
        else:
            df_hist = load_month_data(anio, m_idx, v=_dv())
            st.caption(f"Mostrando registros de {mes_sel} {anio}")

        if bq and not df_hist.empty:
            df_hist = df_hist[
                df_hist.astype(str)
                .apply(lambda x: x.str.contains(bq, case=False, na=False))
                .any(axis=1)
            ]

        # Hide internal columns
        _hide = ['Eliminado', '_mes', '_mes_nm']
        df_show = df_hist.drop(columns=[c for c in _hide if c in df_hist.columns], errors='ignore')
        st.dataframe(df_show, use_container_width=True, hide_index=True)
        st.caption(f"📊 Total registros mostrados: **{len(df_show)}**")

        # ── Eliminar (solo admin) ──
        if role == 'admin' and not df_hist.empty and 'ID' in df_hist.columns:
            st.divider()
            with st.expander("🗑️ Eliminar un Registro", expanded=False):
                ids_avail = df_hist['ID'].tolist()

                def _lbl_id(x):
                    r = df_hist[df_hist['ID'] == x]
                    if r.empty:
                        return f"ID {x}"
                    return (f"ID {x} | {r['Nombre_Cliente'].values[0]} | "
                            f"{r['Motivo'].values[0]} | {r['Zona'].values[0]}")

                sel_del = st.selectbox("Selecciona el registro", ids_avail,
                                       format_func=_lbl_id)
                if sel_del is not None:
                    r_del = df_hist[df_hist['ID'] == sel_del].iloc[0]
                    fecha_del_str = str(r_del.get('Fecha', f"{anio}-{m_idx:02d}-01"))
                    try:
                        dt_del = datetime.strptime(fecha_del_str[:7], '%Y-%m')
                        y_del, m_del = dt_del.year, dt_del.month
                    except Exception:
                        y_del, m_del = anio, m_idx

                    if st.button("🗑️ Confirmar Eliminación", type="secondary", use_container_width=True):
                        if soft_delete_record(y_del, m_del, sel_del):
                            _inv()
                            _flash("🗑️ Registro eliminado del historial.")
                            st.rerun()
                        else:
                            _flash("❌ No se pudo eliminar el registro.", 'error')
                            st.rerun()

        st.divider()
        st.download_button(
            "📥 Descargar CSV",
            df_show.to_csv(index=False).encode('utf-8'),
            f"ONT_{mes_sel}_{anio}.csv", "text/csv",
            use_container_width=True,
        )

    t_idx += 1

# ═════════════════════════════════════════════════════════════════════
# TAB 3 — CONFIGURACIÓN (admin only)
# ═════════════════════════════════════════════════════════════════════
if role == 'admin' and len(tabs) > t_idx:
    with tabs[t_idx]:
        st.markdown("### ⚙️ Configuración del Sistema")
        st.caption("Los cambios en catálogos se reflejan inmediatamente en todos los formularios.")

        t_z, t_tec, t_mot, t_usr = st.tabs(
            ["🗺️ Zonas", "🔧 Técnicos", "📋 Motivos", "👤 Usuarios"])

        # ── Zonas ──
        with t_z:
            st.markdown("#### Gestión de Zonas / Nodos")
            for z in get_zonas(v=_dv()):
                cz1, cz2 = st.columns([5, 1])
                cz1.text(f"🗺️  {z}")
                if cz2.button("🗑️", key=f"dz_{z}", help="Eliminar zona"):
                    try:
                        ws_z     = _ws(SH_ZONAS)
                        recs     = ws_z.get_all_records()
                        for i, r in enumerate(recs, 2):
                            if r.get('Zona') == z:
                                ws_z.delete_rows(i)
                                break
                        clear_cats(); _inv()
                        _flash("🗑️ Zona eliminada.")
                        st.rerun()
                    except Exception as e:
                        _flash(f"❌ Error: {e}", 'error'); st.rerun()
            st.divider()
            with st.form("add_zona", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Zona**")
                nz = st.text_input("Nombre del nodo / zona")
                if st.form_submit_button("Agregar Zona") and nz.strip():
                    _get_or_create_ws(SH_ZONAS, ['Zona'])
                    _ws(SH_ZONAS).append_row([nz.strip()])
                    clear_cats(); _inv()
                    _flash("✅ Zona agregada.")
                    st.rerun()

        # ── Técnicos ──
        with t_tec:
            st.markdown("#### Gestión de Técnicos de Campo")
            for tec in get_tecnicos(v=_dv()):
                ct1, ct2 = st.columns([5, 1])
                ct1.text(f"🔧  {tec}")
                if ct2.button("🗑️", key=f"dt_{tec}", help="Eliminar técnico"):
                    try:
                        ws_t = _ws(SH_TECNICOS)
                        recs = ws_t.get_all_records()
                        for i, r in enumerate(recs, 2):
                            if r.get('Tecnico') == tec:
                                ws_t.delete_rows(i)
                                break
                        clear_cats(); _inv()
                        _flash("🗑️ Técnico eliminado.")
                        st.rerun()
                    except Exception as e:
                        _flash(f"❌ Error: {e}", 'error'); st.rerun()
            st.divider()
            with st.form("add_tec", clear_on_submit=True):
                st.markdown("**➕ Agregar Nuevo Técnico**")
                nt = st.text_input("Nombre completo del técnico")
                if st.form_submit_button("Agregar Técnico") and nt.strip():
                    _get_or_create_ws(SH_TECNICOS, ['Tecnico'])
                    _ws(SH_TECNICOS).append_row([nt.strip()])
                    clear_cats(); _inv()
                    _flash("✅ Técnico agregado.")
                    st.rerun()

        # ── Motivos ──
        with t_mot:
            st.markdown("#### Gestión de Motivos de Operación")
            st.info("🎨 **Verde** = Positivo (meta: subir)  |  **Rojo** = Negativo (meta: bajar)  |  **Naranja** = Neutral")
            df_mots = get_motivos_df(v=_dv())
            for _, mr in df_mots.iterrows():
                cm1, cm2, cm3 = st.columns([4, 2, 1])
                cm1.text(f"📋  {mr.get('Motivo','')}")
                tipo_m = str(mr.get('Tipo', 'neutral'))
                cm2.markdown(
                    f"<span style='color:{TIPO_COLOR.get(tipo_m, COLOR_WARN)};font-weight:700'>"
                    f"{TIPO_EMOJI.get(tipo_m,'⚪')} {tipo_m.capitalize()}</span>",
                    unsafe_allow_html=True,
                )
                if cm3.button("🗑️", key=f"dm_{mr.get('Motivo','')}"):
                    try:
                        ws_m = _ws(SH_MOTIVOS)
                        recs = ws_m.get_all_records()
                        for i, r in enumerate(recs, 2):
                            if r.get('Motivo') == mr.get('Motivo'):
                                ws_m.delete_rows(i)
                                break
                        clear_cats(); _inv()
                        _flash("🗑️ Motivo eliminado.")
                        st.rerun()
                    except Exception as e:
                        _flash(f"❌ Error: {e}", 'error'); st.rerun()
            st.divider()
            with st.form("add_mot", clear_on_submit=True):
                st.markdown("**➕ Agregar Nuevo Motivo**")
                nm  = st.text_input("Descripción del motivo")
                ntm = st.selectbox(
                    "Tipo de impacto",
                    ['positive', 'negative', 'neutral'],
                    format_func=lambda x: TIPO_LABEL[x],
                )
                if st.form_submit_button("Agregar Motivo") and nm.strip():
                    _ws(SH_MOTIVOS).append_row([nm.strip(), ntm])
                    clear_cats(); _inv()
                    _flash("✅ Motivo agregado.")
                    st.rerun()

        # ── Usuarios ──
        with t_usr:
            st.markdown("#### Control de Accesos y Usuarios")
            st.info("🔒 **Política:** 8+ caracteres · Mayúscula · Minúscula · Número · Carácter especial")

            u_col1, u_col2 = st.columns([1, 2])

            with u_col1:
                with st.form("add_usr", clear_on_submit=True):
                    st.markdown("**Crear Nuevo Usuario**")
                    nu_u = st.text_input("Nombre de usuario")
                    nu_p = st.text_input("Contraseña", type="password")
                    nu_r = st.selectbox("Rol", ['viewer', 'auditor', 'admin'],
                                        format_func=lambda x: {
                                            'viewer':  '👁️ Solo Vista',
                                            'auditor': '📝 Auditor (Puede registrar)',
                                            'admin':   '⚙️ Administrador',
                                        }[x])
                    if st.form_submit_button("✅ Crear Usuario") and nu_u and nu_p:
                        err_p = _validate_pw(nu_p)
                        if err_p:
                            st.error(err_p)
                        else:
                            df_u_ex = pd.DataFrame(_get_users_raw())
                            if not df_u_ex.empty and nu_u in df_u_ex.get('username', pd.Series()).values:
                                st.error("❌ Ese nombre de usuario ya existe.")
                            else:
                                _get_or_create_ws(SH_USUARIOS, USER_COLS)
                                _ws(SH_USUARIOS).append_row(
                                    [nu_u, _hash(nu_p), nu_r, 0, '', 'FALSE', ''])
                                _flash("✅ Usuario creado.")
                                st.rerun()

                with st.form("reset_pw_form", clear_on_submit=True):
                    st.markdown("**Restablecer Contraseña**")
                    rpu = st.text_input("Usuario exacto")
                    rpp = st.text_input("Nueva Contraseña", type="password")
                    if st.form_submit_button("🔑 Restablecer") and rpu and rpp:
                        err_p = _validate_pw(rpp)
                        if err_p:
                            st.error(err_p)
                        else:
                            ok = _update_user_fields(rpu, {
                                'password_hash': _hash(rpp),
                                'failed_attempts': 0,
                                'locked_until': '',
                            })
                            if ok:
                                _flash("✅ Contraseña actualizada.")
                                st.rerun()
                            else:
                                st.error("❌ Usuario no encontrado.")

            with u_col2:
                users_raw = _get_users_raw()
                if users_raw:
                    df_u_all = pd.DataFrame(users_raw)

                    # Super admin — read-only
                    df_sa = df_u_all[df_u_all['username'] == SUPERADMIN]
                    df_ot = df_u_all[df_u_all['username'] != SUPERADMIN].reset_index(drop=True)

                    if not df_sa.empty:
                        st.markdown("🛡️ **Super Administrador (no editable)**")
                        st.dataframe(
                            df_sa[['username', 'role']].rename(columns={'username': 'Usuario', 'role': 'Rol'}),
                            hide_index=True, use_container_width=True,
                        )
                        st.divider()

                    if not df_ot.empty:
                        st.markdown("**Usuarios Registrados:**")
                        for _, ur in df_ot.iterrows():
                            uc1, uc2, uc3, uc4, uc5 = st.columns([2, 1, 1, 1, 1])
                            uc1.markdown(f"**{ur['username']}**")
                            uc2.caption(ur.get('role', 'viewer'))
                            is_banned_u = str(ur.get('is_banned', 'FALSE')).upper() == 'TRUE'
                            fa_u        = int(ur.get('failed_attempts', 0) or 0)
                            uc3.caption(f"{'🚫 Baneado' if is_banned_u else '✅ Activo'}")
                            uc4.caption(f"{fa_u} int. fallidos")

                            with uc5:
                                if is_banned_u:
                                    if st.button("♻️", key=f"unban_{ur['username']}",
                                                 help="Desbanear usuario"):
                                        _update_user_fields(ur['username'], {
                                            'is_banned': 'FALSE', 'failed_attempts': 0,
                                        })
                                        _flash(f"✅ Usuario {ur['username']} desbaneado.")
                                        st.rerun()
                                else:
                                    if st.button("🚫", key=f"ban_{ur['username']}",
                                                 help="Banear usuario"):
                                        _update_user_fields(ur['username'], {'is_banned': 'TRUE'})
                                        _flash(f"🚫 Usuario {ur['username']} baneado.")
                                        st.rerun()

                            col_del = st.columns(1)[0]
                            if col_del.button(f"🗑️ Eliminar {ur['username']}",
                                              key=f"del_u_{ur['username']}",
                                              use_container_width=True):
                                if ur['username'] == st.session_state.username:
                                    _flash("❌ No puedes eliminar tu propia cuenta.", 'error')
                                    st.rerun()
                                else:
                                    ws_u  = _ws(SH_USUARIOS)
                                    recs  = ws_u.get_all_records()
                                    for i, r in enumerate(recs, 2):
                                        if r.get('username') == ur['username']:
                                            ws_u.delete_rows(i)
                                            break
                                    _flash(f"🗑️ Usuario {ur['username']} eliminado.")
                                    st.rerun()
                            st.divider()
                else:
                    st.info("No hay usuarios registrados aún.")
