import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import bcrypt, pytz, secrets, re
from datetime import datetime, date, timedelta
from sqlalchemy import text

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
COLOR_NEW       = '#83c9ff'
COLOR_REC       = '#a29bfe'

MESES = ("Enero","Febrero","Marzo","Abril","Mayo","Junio",
         "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre")

SUPERADMIN = 'Admin'

TIPO_LABEL = {
    'positive': '✅ Positivo (Instalac. / Reconex. / Cambio / Renov.)',
    'negative': '⚠️ Negativo (Problema / Desconex.)',
    'neutral':  '🔄 Neutral (Otros)',
}
TIPO_COLOR = {'positive': COLOR_TEAL, 'negative': COLOR_DANGER, 'neutral': COLOR_WARN}
TIPO_EMOJI = {'positive': '🟢', 'negative': '🔴', 'neutral': '🟡'}

ESTADO_EQUIPO_OPS = ["No Aplica", "Nuevo", "Recuperado"]

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
# 3. SESSION STATE Y DB CONNECTION
# ─────────────────────────────────────────────────────────────────────
_DEFAULTS = {
    'logged_in': False, 'role': '', 'username': '',
    'log_u': '', 'log_p': '', 'log_err': '',
    'flash_msg': '', 'flash_type': '',
    'session_token': '', 'form_reset': 0,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

def _flash(msg, kind='success'):
    st.session_state.flash_msg  = msg
    st.session_state.flash_type = kind

if st.session_state.flash_msg:
    if st.session_state.flash_type == 'error':
        st.error(st.session_state.flash_msg, icon="❌")
    else:
        st.toast(st.session_state.flash_msg, icon="✅")
    st.session_state.flash_msg  = ''
    st.session_state.flash_type = ''

# Conexión a Neon PostgreSQL
try:
    conn = st.connection("postgresql", type="sql", pool_pre_ping=True)
except Exception as e:
    st.error(f"⚠️ Error de conexión a la base de datos: {e}")
    st.info("Verifica tu archivo `.streamlit/secrets.toml` y que tengas instalada la librería `psycopg2-binary`.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────
# 4. PASSWORD HELPERS
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
# 5. CATÁLOGOS (vía BD)
# ─────────────────────────────────────────────────────────────────────
def get_zonas() -> list:
    try:
        df = conn.query("SELECT nombre FROM zonas ORDER BY nombre ASC", ttl=10)
        if df.empty:
            return []
        col = df.columns[0]
        return df[col].tolist()
    except Exception as e:
        st.error(f"Error cargando zonas: {e}")
        return []

def get_tecnicos() -> list:
    try:
        df = conn.query("SELECT nombre FROM tecnicos ORDER BY nombre ASC", ttl=10)
        if df.empty:
            return []
        col = df.columns[0]
        return df[col].tolist()
    except Exception as e:
        st.error(f"Error cargando técnicos: {e}")
        return []

def get_motivos_df() -> pd.DataFrame:
    try:
        df = conn.query("SELECT motivo, tipo FROM motivos ORDER BY motivo ASC", ttl=10)
        if df.empty:
            return pd.DataFrame(columns=['Motivo', 'Tipo'])
        df.columns = [c.lower() for c in df.columns]
        return df.rename(columns={'motivo': 'Motivo', 'tipo': 'Tipo'})
    except Exception as e:
        st.error(f"Error cargando motivos: {e}")
        return pd.DataFrame(columns=['Motivo', 'Tipo'])

def get_motivos_list() -> list:
    df = get_motivos_df()
    return df["Motivo"].tolist() if not df.empty else []

def get_tipo_map() -> dict:
    df = get_motivos_df()
    if not df.empty:
        return dict(zip(df["Motivo"], df["Tipo"]))
    return {}

# ─────────────────────────────────────────────────────────────────────
# 6. USER MANAGEMENT
# ─────────────────────────────────────────────────────────────────────
def _get_users_raw() -> pd.DataFrame:
    try:
        return conn.query(
            "SELECT username, role, is_banned, failed_attempts FROM usuarios ORDER BY username ASC",
            ttl=5,
        )
    except Exception as e:
        st.error(f"Error cargando usuarios: {e}")
        return pd.DataFrame()

def _find_user(username: str):
    try:
        with conn.session as s:
            result = s.execute(
                text("SELECT * FROM usuarios WHERE username = :u"),
                {"u": username},
            )
            row = result.fetchone()
            if row:
                return dict(row._mapping)
        return None
    except Exception as e:
        st.error(f"Error buscando usuario: {e}")
        return None

def _update_user_fields(username: str, fields: dict) -> bool:
    if not fields:
        return True
    set_clause = ", ".join([f"{k} = :{k}" for k in fields.keys()])
    sql = text(f"UPDATE usuarios SET {set_clause} WHERE username = :username")
    params = fields.copy()
    params["username"] = username
    try:
        with conn.session as s:
            s.execute(sql, params)
            s.commit()
        return True
    except Exception as e:
        st.error(f"Error actualizando usuario: {e}")
        return False

def _get_db_token(username: str) -> str:
    user = _find_user(username)
    if user is None:
        return ''
    return str(user.get('session_token') or '')

# ─────────────────────────────────────────────────────────────────────
# 7. LOGIN
# ─────────────────────────────────────────────────────────────────────
def do_login():
    u = st.session_state.log_u
    p = st.session_state.log_p

    user = _find_user(u)
    if not user:
        st.session_state.log_err = "❌ Credenciales incorrectas."
        st.session_state.log_u   = ''
        st.session_state.log_p   = ''
        return

    now = datetime.now(SV_TZ).replace(tzinfo=None)

    is_banned = bool(user.get('is_banned', False))
    fa        = int(user.get('failed_attempts', 0) or 0)
    lu_str    = user.get('locked_until')

    if lu_str is not None and lu_str != '':
        try:
            if isinstance(lu_str, str):
                lu = datetime.fromisoformat(lu_str)
            elif isinstance(lu_str, datetime):
                lu = lu_str
            else:
                lu = None

            if lu is not None:
                if lu.tzinfo is not None:
                    lu = lu.astimezone(SV_TZ).replace(tzinfo=None)
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
            'failed_attempts': 0,
            'locked_until':    None,
            'session_token':   token,
        })
        role_db = str(user.get('role', 'auditor')).lower()
        if role_db not in ['admin', 'auditor']:
            role_db = 'auditor'
        st.session_state.update({
            'logged_in':     True,
            'role':          role_db,
            'username':      u,
            'log_err':       '',
            'session_token': token,
        })
    else:
        fa += 1
        if fa >= 6:
            _update_user_fields(u, {'is_banned': True, 'failed_attempts': fa})
            st.session_state.log_err = "❌ Cuenta baneada permanentemente."
        elif fa % 3 == 0:
            lock_until = now + timedelta(minutes=5)
            _update_user_fields(u, {'locked_until': lock_until, 'failed_attempts': fa})
            st.session_state.log_err = "⏳ Demasiados intentos. Bloqueado 5 min."
        else:
            _update_user_fields(u, {'failed_attempts': fa})
            st.session_state.log_err = "❌ Credenciales incorrectas."
        st.session_state.log_u = ''
        st.session_state.log_p = ''

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
            st.text_input("Usuario",    key="log_u")
            st.text_input("Contraseña", key="log_p", type="password")
            st.button("Iniciar Sesión", type="primary",
                      on_click=do_login, use_container_width=True)
            st.caption("Contacta al administrador para gestionar tu acceso.")
    st.stop()

# ── Validación de sesión única ──
_local_tok = st.session_state.get('session_token', '')
if _local_tok:
    _db_tok = _get_db_token(st.session_state.username)
    if _db_tok and _db_tok != _local_tok:
        for _k, _v in _DEFAULTS.items():
            st.session_state[_k] = _v
        _flash("⚠️ Tu sesión fue cerrada porque el mismo usuario inició sesión desde otro dispositivo.", 'error')
        st.rerun()

# ─────────────────────────────────────────────────────────────────────
# 8. DATA FUNCTIONS (PostgreSQL)
# ─────────────────────────────────────────────────────────────────────
_COL_MAP = {
    'id':                 'ID',
    'fecha':              'Fecha',
    'asesor':             'Asesor',
    'tecnico':            'Tecnico',
    'zona':               'Zona',
    'sn_eliminada':       'SN_Eliminada',
    'sn_agregada':        'SN_Agregada',
    'motivo':             'Motivo',
    'cod_cliente':        'Cod_Cliente',
    'nombre_cliente':     'Nombre_Cliente',
    'orden_trabajo':      'Orden_Trabajo',
    'descripcion':        'Descripcion',
    'equipo_recuperado':  'Equipo_Recuperado',
}

def _rename_cols(df: pd.DataFrame, extra: dict = None) -> pd.DataFrame:
    df.columns = [c.lower() for c in df.columns]
    mapping = dict(_COL_MAP)
    if extra:
        mapping.update(extra)
    return df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})

def load_month_data(y: int, m: int) -> pd.DataFrame:
    sql = f"""
        SELECT id, fecha, asesor, tecnico, zona,
               sn_eliminada, sn_agregada, motivo,
               cod_cliente, nombre_cliente, orden_trabajo, descripcion,
               equipo_recuperado
        FROM registros_ont
        WHERE EXTRACT(YEAR  FROM fecha) = {int(y)}
          AND EXTRACT(MONTH FROM fecha) = {int(m)}
          AND eliminado = FALSE
        ORDER BY timestamp DESC
    """
    try:
        df = conn.query(sql, ttl=600)
        if df.empty:
            return df
        return _rename_cols(df)
    except Exception as e:
        st.error(f"Error cargando datos del mes: {e}")
        return pd.DataFrame()

def load_year_data(y: int) -> pd.DataFrame:
    sql = f"""
        SELECT id, fecha, asesor, tecnico, zona,
               sn_eliminada, sn_agregada, motivo,
               cod_cliente, nombre_cliente, orden_trabajo, descripcion,
               equipo_recuperado,
               EXTRACT(MONTH FROM fecha) AS mes_num
        FROM registros_ont
        WHERE EXTRACT(YEAR FROM fecha) = {int(y)}
          AND eliminado = FALSE
        ORDER BY timestamp DESC
    """
    try:
        df = conn.query(sql, ttl=600)
        if df.empty:
            return df
        df = _rename_cols(df, {'mes_num': '_mes'})
        return df
    except Exception as e:
        st.error(f"Error cargando datos del año: {e}")
        return pd.DataFrame()

def append_ont_record(record: dict) -> bool:
    sql = text("""
        INSERT INTO registros_ont
        (fecha, asesor, tecnico, zona, sn_eliminada, sn_agregada,
         motivo, cod_cliente, nombre_cliente, orden_trabajo, descripcion,
         equipo_recuperado)
        VALUES (:f, :a, :t, :z, :sne, :sna, :m, :cc, :nc, :ot, :d, :er)
    """)
    params = {
        "f":   record.get('Fecha'),
        "a":   record.get('Asesor'),
        "t":   record.get('Tecnico'),
        "z":   record.get('Zona'),
        "sne": record.get('SN_Eliminada', ''),
        "sna": record.get('SN_Agregada',  ''),
        "m":   record.get('Motivo'),
        "cc":  record.get('Cod_Cliente'),
        "nc":  record.get('Nombre_Cliente'),
        "ot":  record.get('Orden_Trabajo'),
        "d":   record.get('Descripcion', ''),
        "er":  record.get('Equipo_Recuperado'),   # None | True | False
    }
    try:
        with conn.session as s:
            s.execute(sql, params)
            s.commit()
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error guardando registro: {e}")
        return False

def soft_delete_record(record_id) -> bool:
    sql = text("UPDATE registros_ont SET eliminado = TRUE WHERE id = :id")
    try:
        with conn.session as s:
            s.execute(sql, {"id": record_id})
            s.commit()
        st.cache_data.clear()
        return True
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────
# 9. MÉTRICAS
# ─────────────────────────────────────────────────────────────────────
def calc_metrics(df: pd.DataFrame, tipo_map: dict) -> dict:
    base = {
        'total': 0, 'pos': 0, 'neg': 0, 'neu': 0, 'balance': 0,
        'por_motivo': {}, 'por_zona': {}, 'por_tecnico': {}, 'por_asesor': {},
        'por_zona_tipo': pd.DataFrame(),
        'por_zona_motivo': pd.DataFrame(),
        'balance_por_zona': pd.DataFrame(),
        'recuperados_total': 0,
        'nuevos_total': 0,
        'no_aplica_total': 0,
        'recuperados_por_zona': pd.DataFrame(),
    }
    if df is None or df.empty:
        return base

    total = len(df)
    pm = df['Motivo'].value_counts().to_dict()  if 'Motivo'  in df.columns else {}
    pz = df['Zona'].value_counts().to_dict()    if 'Zona'    in df.columns else {}
    pt = df['Tecnico'].value_counts().to_dict() if 'Tecnico' in df.columns else {}
    pa = df['Asesor'].value_counts().to_dict()  if 'Asesor'  in df.columns else {}

    pos = sum(v for k, v in pm.items() if tipo_map.get(k, 'neutral') == 'positive')
    neg = sum(v for k, v in pm.items() if tipo_map.get(k, 'neutral') == 'negative')
    neu = total - pos - neg

    # ── Acciones por tipo por zona (stacked) ──
    pzt = pd.DataFrame()
    if 'Zona' in df.columns and 'Motivo' in df.columns:
        df2         = df.copy()
        df2['Tipo'] = df2['Motivo'].map(lambda x: tipo_map.get(x, 'neutral').capitalize())
        pzt         = df2.groupby(['Zona', 'Tipo']).size().reset_index(name='N')

    # ── Acciones por motivo por zona (para gráfica detallada) ──
    pzm = pd.DataFrame()
    if 'Zona' in df.columns and 'Motivo' in df.columns:
        pzm = df.groupby(['Zona', 'Motivo']).size().reset_index(name='N')

    # ── Balance neto por zona (positivos - negativos) ──
    bpz = pd.DataFrame()
    if 'Zona' in df.columns and 'Motivo' in df.columns:
        df3          = df.copy()
        df3['Tipo']  = df3['Motivo'].map(lambda x: tipo_map.get(x, 'neutral'))
        pos_z        = df3[df3['Tipo'] == 'positive'].groupby('Zona').size().rename('Positivos')
        neg_z        = df3[df3['Tipo'] == 'negative'].groupby('Zona').size().rename('Negativos')
        bpz          = pd.concat([pos_z, neg_z], axis=1).fillna(0).reset_index()
        bpz['Balance'] = bpz['Positivos'] - bpz['Negativos']
        bpz[['Positivos', 'Negativos', 'Balance']] = bpz[['Positivos', 'Negativos', 'Balance']].astype(int)

    # ── Equipos recuperados / nuevos ──
    rec_total  = 0
    new_total  = 0
    nap_total  = 0
    rec_zona   = pd.DataFrame()

    if 'Equipo_Recuperado' in df.columns:
        rec_total = int(df['Equipo_Recuperado'].eq(True).sum())
        new_total = int(df['Equipo_Recuperado'].eq(False).sum())
        nap_total = int(df['Equipo_Recuperado'].isna().sum())

        if 'Zona' in df.columns:
            df4 = df.copy()
            df4['Estado'] = df4['Equipo_Recuperado'].apply(
                lambda x: 'Recuperado' if x is True or x == True
                else ('Nuevo' if x is False or x == False else 'No Aplica')
            )
            rec_zona = df4.groupby(['Zona', 'Estado']).size().reset_index(name='N')

    return {
        'total': total, 'pos': pos, 'neg': neg, 'neu': neu, 'balance': pos - neg,
        'por_motivo': pm, 'por_zona': pz, 'por_tecnico': pt, 'por_asesor': pa,
        'por_zona_tipo': pzt,
        'por_zona_motivo': pzm,
        'balance_por_zona': bpz,
        'recuperados_total': rec_total,
        'nuevos_total': new_total,
        'no_aplica_total': nap_total,
        'recuperados_por_zona': rec_zona,
    }

def _delta_str(cur, prv) -> str:
    d = cur - prv
    return ('+' if d >= 0 else '') + str(d)

def _delta_color(cur, prv, tipo: str) -> str:
    if tipo == 'negative':
        return 'inverse'
    return 'normal'

# ─────────────────────────────────────────────────────────────────────
# 10. SIDEBAR
# ─────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.caption(
        f"👤 **{st.session_state.username}** "
        f"({st.session_state.role.capitalize()})  |  ONT Manager v3.1 (Neon)"
    )
    st.divider()

    now_sv  = datetime.now(SV_TZ)
    anio    = st.selectbox("🗓️ Año",  [now_sv.year, now_sv.year - 1], index=0)
    mes_sel = st.selectbox("📅 Mes",  list(MESES), index=now_sv.month - 1)
    m_idx   = MESES.index(mes_sel) + 1

    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True):
        try:
            _update_user_fields(st.session_state.username, {'session_token': None})
        except Exception:
            pass
        st.session_state.clear()
        st.rerun()

    st.markdown("""
        <div style='margin-top:60px;text-align:center;color:#555;font-size:11px'>
            📡 ISP ONT Manager<br>Powered by Neon DB
        </div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────
# 11. TABS
# ─────────────────────────────────────────────────────────────────────
role = st.session_state.role

if role == 'admin':
    _tabs = ["📊 Dashboard", "📝 Registrar", "🗂️ Historial", "⚙️ Configuración"]
else:
    _tabs = ["📊 Dashboard", "📝 Registrar", "🗂️ Historial"]

tabs  = st.tabs(_tabs)
t_idx = 0

# ═════════════════════════════════════════════════════════════════════
# TAB 0 — DASHBOARD
# ═════════════════════════════════════════════════════════════════════
with tabs[t_idx]:
    st.title(f"📊 Dashboard ONT — {mes_sel} {anio}")

    tipo_map = get_tipo_map()
    df_cur   = load_month_data(anio, m_idx)
    kpi      = calc_metrics(df_cur, tipo_map)

    # ─────────────────────────────────────────────────────────────
    # KPI 1 — Cantidad total de acciones realizadas en general
    # ─────────────────────────────────────────────────────────────
    st.markdown("### 1️⃣ Acciones Realizadas en General")

    if df_cur.empty:
        st.info("ℹ️ No hay registros para este mes. Comienza registrando un movimiento.")
        pass  # Eliminado st.stop() para no romper tabs
    else:

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("📦 Total Acciones",          kpi['total'])
        c2.metric("✅ Positivas",               kpi['pos'])
        c3.metric("⚠️ Negativas",              kpi['neg'])
        c4.metric("🔄 Neutrales",              kpi['neu'])
        c5.metric("⚖️ Balance Neto",           kpi['balance'],
                  delta_color="normal" if kpi['balance'] >= 0 else "inverse")

        st.divider()

        # ─────────────────────────────────────────────────────────────
        # KPI 2 — Cantidad de acciones por tipo (motivo)
        # ─────────────────────────────────────────────────────────────
        st.markdown("### 2️⃣ Acciones por Tipo de Operación (Motivo)")

        if kpi['por_motivo']:
            pm_df = (
                pd.DataFrame(list(kpi['por_motivo'].items()), columns=['Motivo', 'Cantidad'])
                .sort_values('Cantidad', ascending=True)
            )
            pm_df['Tipo']  = pm_df['Motivo'].map(lambda x: tipo_map.get(x, 'neutral'))
            pm_df['Color'] = pm_df['Tipo'].map(lambda x: TIPO_COLOR.get(x, COLOR_WARN))

            col_bar, col_pie = st.columns(2)
            with col_bar:
                fig_mot = px.bar(
                    pm_df, x='Cantidad', y='Motivo', orientation='h',
                    text_auto=True,
                    title="Cantidad de Acciones por Motivo",
                )
                fig_mot.update_traces(marker_color=pm_df['Color'].tolist())
                fig_mot.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', height=380,
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_mot, use_container_width=True)

            with col_pie:
                pie_df = pd.DataFrame({
                    'Tipo':     ['Positivo', 'Negativo', 'Neutral'],
                    'Cantidad': [kpi['pos'], kpi['neg'], kpi['neu']],
                })
                fig_pie = px.pie(
                    pie_df, names='Tipo', values='Cantidad', hole=0.5,
                    color_discrete_sequence=[COLOR_TEAL, COLOR_DANGER, COLOR_WARN],
                    title="Distribución por Tipo",
                )
                fig_pie.update_traces(textinfo='percent+label', textposition='inside')
                fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=380, showlegend=False)
                st.plotly_chart(fig_pie, use_container_width=True)

            # Mini KPIs por motivo
            motivo_items = sorted(kpi['por_motivo'].items(), key=lambda x: x[1], reverse=True)
            cols_per_row = 4
            for i in range(0, len(motivo_items), cols_per_row):
                row_items = motivo_items[i:i + cols_per_row]
                cols = st.columns(cols_per_row)
                for j, (mot, cnt) in enumerate(row_items):
                    tipo_m = tipo_map.get(mot, 'neutral')
                    emoji  = TIPO_EMOJI.get(tipo_m, '⚪')
                    cols[j].metric(f"{emoji} {mot}", cnt)

        st.divider()

        # ─────────────────────────────────────────────────────────────
        # KPI 3 — Cantidad de acciones por zona (general)
        # ─────────────────────────────────────────────────────────────
        st.markdown("### 3️⃣ Acciones por Zona (General)")

        if kpi['por_zona']:
            z_df = (
                pd.DataFrame(list(kpi['por_zona'].items()), columns=['Zona', 'Operaciones'])
                .sort_values('Operaciones', ascending=False)
            )
            fig_z = px.bar(
                z_df, x='Zona', y='Operaciones', text_auto=True,
                color='Operaciones',
                color_continuous_scale=[[0, '#1d2c59'], [0.5, COLOR_PRIMARY], [1, COLOR_TEAL]],
                title=f"Total de Acciones por Zona — {mes_sel} {anio}",
            )
            fig_z.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', height=380,
                margin=dict(l=0, r=0, t=40, b=0), xaxis_tickangle=-30,
            )
            st.plotly_chart(fig_z, use_container_width=True)

            # Mini KPIs por zona
            zona_items = sorted(kpi['por_zona'].items(), key=lambda x: x[1], reverse=True)
            cols_per_row = 4
            for i in range(0, len(zona_items), cols_per_row):
                row_items = zona_items[i:i + cols_per_row]
                cols = st.columns(cols_per_row)
                for j, (zona, cnt) in enumerate(row_items):
                    cols[j].metric(f"🗺️ {zona}", cnt)

        st.divider()

        # ─────────────────────────────────────────────────────────────
        # KPI 4 — Acciones por tipo (motivo) por zona
        # ─────────────────────────────────────────────────────────────
        st.markdown("### 4️⃣ Acciones por Tipo de Operación · Por Zona")

        col4a, col4b = st.columns(2)

        # Gráfica apilada por tipo (Positivo / Negativo / Neutral) por zona
        with col4a:
            if not kpi['por_zona_tipo'].empty:
                fig_zt = px.bar(
                    kpi['por_zona_tipo'], x='Zona', y='N', color='Tipo',
                    barmode='stack', text_auto=True,
                    color_discrete_map={
                        'Positive': COLOR_TEAL,
                        'Negative': COLOR_DANGER,
                        'Neutral':  COLOR_WARN,
                    },
                    title="Tipo de Operación por Zona (apilado)",
                )
                fig_zt.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', height=400,
                    margin=dict(l=0, r=0, t=40, b=0), xaxis_tickangle=-30,
                )
                st.plotly_chart(fig_zt, use_container_width=True)

        # Gráfica agrupada por motivo específico por zona
        with col4b:
            if not kpi['por_zona_motivo'].empty:
                fig_zm = px.bar(
                    kpi['por_zona_motivo'], x='Zona', y='N', color='Motivo',
                    barmode='group', text_auto=True,
                    title="Motivo Específico por Zona (agrupado)",
                )
                fig_zm.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', height=400,
                    margin=dict(l=0, r=0, t=40, b=0), xaxis_tickangle=-30,
                )
                st.plotly_chart(fig_zm, use_container_width=True)

        st.divider()

        # ─────────────────────────────────────────────────────────────
        # KPI 5 — Balance por zona según cada acción
        # ─────────────────────────────────────────────────────────────
        st.markdown("### 5️⃣ Balance por Zona")

        if not kpi['balance_por_zona'].empty:
            bpz = kpi['balance_por_zona']

            col5a, col5b = st.columns(2)

            with col5a:
                # Balance neto por zona
                colors_balance = [COLOR_TEAL if b >= 0 else COLOR_DANGER for b in bpz['Balance']]
                fig_bal = go.Figure()
                fig_bal.add_trace(go.Bar(
                    x=bpz['Zona'],
                    y=bpz['Balance'],
                    text=bpz['Balance'],
                    textposition='outside',
                    marker_color=colors_balance,
                    name='Balance Neto',
                ))
                fig_bal.update_layout(
                    title="Balance Neto por Zona (Positivos − Negativos)",
                    paper_bgcolor='rgba(0,0,0,0)',
                    height=380,
                    margin=dict(l=0, r=0, t=40, b=0),
                    xaxis_tickangle=-30,
                )
                fig_bal.add_hline(y=0, line_dash="dash", line_color="#555")
                st.plotly_chart(fig_bal, use_container_width=True)

            with col5b:
                # Positivos vs Negativos por zona lado a lado
                fig_pn = go.Figure()
                fig_pn.add_trace(go.Bar(
                    x=bpz['Zona'], y=bpz['Positivos'],
                    name='Positivos', marker_color=COLOR_TEAL, text=bpz['Positivos'],
                    textposition='outside',
                ))
                fig_pn.add_trace(go.Bar(
                    x=bpz['Zona'], y=bpz['Negativos'],
                    name='Negativos', marker_color=COLOR_DANGER, text=bpz['Negativos'],
                    textposition='outside',
                ))
                fig_pn.update_layout(
                    barmode='group',
                    title="Positivos vs Negativos por Zona",
                    paper_bgcolor='rgba(0,0,0,0)',
                    height=380,
                    margin=dict(l=0, r=0, t=40, b=0),
                    xaxis_tickangle=-30,
                )
                st.plotly_chart(fig_pn, use_container_width=True)

            # Mini KPIs de balance por zona
            bpz_sorted = bpz.sort_values('Balance', ascending=False)
            cols_per_row = 4
            for i in range(0, len(bpz_sorted), cols_per_row):
                row_items = bpz_sorted.iloc[i:i + cols_per_row]
                cols = st.columns(cols_per_row)
                for j, (_, row) in enumerate(row_items.iterrows()):
                    bal = int(row['Balance'])
                    cols[j].metric(
                        f"⚖️ {row['Zona']}",
                        bal,
                        delta_color="normal" if bal >= 0 else "inverse",
                    )

        st.divider()

        # ─────────────────────────────────────────────────────────────
        # KPI 6 — Equipos recuperados (general y por zona)
        # ─────────────────────────────────────────────────────────────
        st.markdown("### 6️⃣ Estado de Equipos Instalados (Recuperados vs Nuevos)")

        c6a, c6b, c6c = st.columns(3)
        c6a.metric("♻️ Equipos Recuperados", kpi['recuperados_total'])
        c6b.metric("🆕 Equipos Nuevos",      kpi['nuevos_total'])
        c6c.metric("➖ No Aplica",           kpi['no_aplica_total'])

        if not kpi['recuperados_por_zona'].empty:
            rec_zona_df = kpi['recuperados_por_zona']

            col6a, col6b = st.columns(2)

            with col6a:
                # Stacked: Recuperado / Nuevo / No Aplica por zona
                fig_rec = px.bar(
                    rec_zona_df[rec_zona_df['Estado'] != 'No Aplica'],
                    x='Zona', y='N', color='Estado',
                    barmode='stack', text_auto=True,
                    color_discrete_map={
                        'Recuperado': COLOR_REC,
                        'Nuevo':      COLOR_NEW,
                    },
                    title="Recuperados vs Nuevos por Zona",
                )
                fig_rec.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', height=380,
                    margin=dict(l=0, r=0, t=40, b=0), xaxis_tickangle=-30,
                )
                st.plotly_chart(fig_rec, use_container_width=True)

            with col6b:
                # Pie total recuperados vs nuevos
                if kpi['recuperados_total'] + kpi['nuevos_total'] > 0:
                    pie_eq = pd.DataFrame({
                        'Estado':   ['Recuperado', 'Nuevo'],
                        'Cantidad': [kpi['recuperados_total'], kpi['nuevos_total']],
                    })
                    fig_pie_eq = px.pie(
                        pie_eq, names='Estado', values='Cantidad', hole=0.5,
                        color_discrete_map={'Recuperado': COLOR_REC, 'Nuevo': COLOR_NEW},
                        title="Distribución Global: Recuperados vs Nuevos",
                    )
                    fig_pie_eq.update_traces(textinfo='percent+label', textposition='inside')
                    fig_pie_eq.update_layout(
                        paper_bgcolor='rgba(0,0,0,0)', height=380, showlegend=False,
                    )
                    st.plotly_chart(fig_pie_eq, use_container_width=True)

            # Mini KPIs: recuperados por zona
            rec_only = rec_zona_df[rec_zona_df['Estado'] == 'Recuperado'].set_index('Zona')['N'].to_dict()
            if rec_only:
                st.markdown("**♻️ Equipos Recuperados por Zona:**")
                zona_rec_items = sorted(rec_only.items(), key=lambda x: x[1], reverse=True)
                cols_per_row   = 4
                for i in range(0, len(zona_rec_items), cols_per_row):
                    row_items = zona_rec_items[i:i + cols_per_row]
                    cols = st.columns(cols_per_row)
                    for j, (zona, cnt) in enumerate(row_items):
                        cols[j].metric(f"♻️ {zona}", cnt)

t_idx += 1

# ═════════════════════════════════════════════════════════════════════
# TAB 1 — REGISTRAR
# ═════════════════════════════════════════════════════════════════════
if role in ('admin', 'auditor'):
    with tabs[t_idx]:
        st.title("📝 Registrar Movimiento de ONT")
        fk = st.session_state.form_reset

        zonas_l = get_zonas()
        tecs_l  = get_tecnicos()
        mots_l  = get_motivos_list()
        today   = datetime.now(SV_TZ).date()

        if not zonas_l or not tecs_l or not mots_l:
            st.warning("⚠️ Faltan catálogos (zonas, técnicos o motivos). "
                       "Ve a ⚙️ Configuración y agrégalos primero.")
        else:
            _tipo_map_reg = get_tipo_map()

            with st.container(border=True):
                st.info(
                    f"👤 **Asesor registrado automáticamente:** `{st.session_state.username}`  "
                    f"|  📅 **Hoy:** `{today.strftime('%d/%m/%Y')}`"
                )

                cc1, cc2 = st.columns(2)
                tec    = cc1.selectbox("🔧 Técnico de Campo *",          tecs_l,  key=f"tec_{fk}")
                zona   = cc2.selectbox("🗺️ Zona *",                      zonas_l, key=f"zon_{fk}")
                motivo = st.selectbox("📋 Motivo / Tipo de Operación *", mots_l,  key=f"mot_{fk}")

                tipo_sel = _tipo_map_reg.get(motivo, 'neutral')
                if   tipo_sel == 'positive': st.success(f"✅ {TIPO_LABEL['positive']}")
                elif tipo_sel == 'negative': st.warning(f"⚠️ {TIPO_LABEL['negative']}")
                else:                        st.info(f"🔄 {TIPO_LABEL['neutral']}")

                st.divider()
                sc1, sc2 = st.columns(2)
                sn_elim = sc1.text_input(
                    "🔴 SN ONT Retirada / Eliminada (dejar en blanco si no aplica)",
                    key=f"sne_{fk}",
                )
                sn_agr = sc2.text_input(
                    "🟢 SN ONT Instalada / Agregada (dejar en blanco si no aplica)",
                    key=f"sna_{fk}",
                )

                # ── NUEVO: Estado del equipo instalado ──────────────────
                st.divider()
                st.markdown("#### 📦 Estado del Equipo Instalado")
                estado_equipo = st.radio(
                    "¿El equipo instalado/colocado es nuevo o recuperado?",
                    options=ESTADO_EQUIPO_OPS,
                    index=0,
                    horizontal=True,
                    key=f"eq_{fk}",
                    help=(
                        "**No Aplica:** No se colocó equipo (p.ej. solo desconexión).\n"
                        "**Nuevo:** Equipo recién adquirido, sin uso previo.\n"
                        "**Recuperado:** Equipo previamente usado / retirado de otro cliente."
                    ),
                )
                if estado_equipo == "Recuperado":
                    st.success("♻️ Equipo **recuperado** — se contará en métricas de recuperación.")
                elif estado_equipo == "Nuevo":
                    st.info("🆕 Equipo **nuevo**.")
                else:
                    st.caption("➖ No aplica para este tipo de operación.")

                # Convertir a valor booleano / None
                if   estado_equipo == "Recuperado": equipo_recuperado = True
                elif estado_equipo == "Nuevo":       equipo_recuperado = False
                else:                                equipo_recuperado = None
                # ────────────────────────────────────────────────────────

                st.divider()
                dc1, dc2, dc3 = st.columns(3)
                cod_cl   = dc1.text_input("🆔 Código de Cliente *",           key=f"cod_{fk}")
                nom_cl   = dc2.text_input("👤 Nombre Completo del Cliente *", key=f"nom_{fk}")
                ord_trab = dc3.text_input("📄 N° de Orden de Trabajo *",      key=f"ot_{fk}")

                desc      = st.text_area("📝 Descripción Adicional (Opcional)", key=f"desc_{fk}", height=80)
                fecha_reg = st.date_input("📅 Fecha del Movimiento", value=today, key=f"fecha_{fk}")

                st.write("")
                if st.button("💾 Guardar Registro", type="primary", use_container_width=True):
                    if not all([tec, zona, motivo, cod_cl.strip(), nom_cl.strip(), ord_trab.strip()]):
                        _flash("❌ Completa todos los campos obligatorios (*) antes de guardar.", 'error')
                        st.rerun()
                    else:
                        record = {
                            'Fecha':             fecha_reg.strftime('%Y-%m-%d'),
                            'Asesor':            st.session_state.username,
                            'Tecnico':           tec,
                            'Zona':              zona,
                            'SN_Eliminada':      sn_elim.strip(),
                            'SN_Agregada':       sn_agr.strip(),
                            'Motivo':            motivo,
                            'Cod_Cliente':       cod_cl.strip(),
                            'Nombre_Cliente':    nom_cl.strip(),
                            'Orden_Trabajo':     ord_trab.strip(),
                            'Descripcion':       desc.strip(),
                            'Equipo_Recuperado': equipo_recuperado,
                        }
                        with st.spinner("Guardando en la base de datos…"):
                            ok = append_ont_record(record)
                        if ok:
                            st.session_state.form_reset += 1
                            _flash("✅ Registro guardado exitosamente en Neon DB.")
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
                df_hist = load_year_data(anio)
            st.caption(f"Mostrando todos los registros de {anio}")
        else:
            df_hist = load_month_data(anio, m_idx)
            st.caption(f"Mostrando registros de {mes_sel} {anio}")

        if bq and not df_hist.empty:
            df_hist = df_hist[
                df_hist.astype(str)
                .apply(lambda x: x.str.contains(bq, case=False, na=False))
                .any(axis=1)
            ]

        # Eliminar columna auxiliar _mes si existe
        if '_mes' in df_hist.columns:
            df_hist = df_hist.drop(columns=['_mes'])

        # Hacer la columna Equipo_Recuperado más legible en historial
        if 'Equipo_Recuperado' in df_hist.columns:
            df_hist_display = df_hist.copy()
            df_hist_display['Equipo_Recuperado'] = df_hist_display['Equipo_Recuperado'].apply(
                lambda x: '♻️ Recuperado' if x is True or x == True
                else ('🆕 Nuevo' if x is False or x == False else '➖ No Aplica')
            )
        else:
            df_hist_display = df_hist

        st.dataframe(df_hist_display, use_container_width=True, hide_index=True)
        st.caption(f"📊 Total registros mostrados: **{len(df_hist)}**")

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

                sel_del = st.selectbox("Selecciona el registro", ids_avail, format_func=_lbl_id)
                if sel_del is not None:
                    if st.button("🗑️ Confirmar Eliminación", type="secondary", use_container_width=True):
                        if soft_delete_record(sel_del):
                            _flash("🗑️ Registro eliminado del historial.")
                            st.rerun()
                        else:
                            _flash("❌ No se pudo eliminar el registro.", 'error')
                            st.rerun()

        st.divider()
        csv_data = df_hist.to_csv(index=False).encode('utf-8') if not df_hist.empty else b""
        st.download_button(
            "📥 Descargar CSV",
            csv_data,
            f"ONT_{mes_sel}_{anio}.csv",
            "text/csv",
            use_container_width=True,
            disabled=df_hist.empty,
        )

    t_idx += 1

# ═════════════════════════════════════════════════════════════════════
# TAB 3 — CONFIGURACIÓN (admin only)
# ═════════════════════════════════════════════════════════════════════
if role == 'admin' and len(tabs) > t_idx:
    with tabs[t_idx]:
        st.markdown("### ⚙️ Configuración del Sistema")
        st.caption("Los cambios en catálogos se reflejan inmediatamente en la base de datos.")

        t_z, t_tec, t_mot, t_usr = st.tabs(
            ["🗺️ Zonas", "🔧 Técnicos", "📋 Motivos", "👤 Usuarios"])

        # ── Zonas ──
        with t_z:
            st.markdown("#### Gestión de Zonas / Nodos")
            for z in get_zonas():
                cz1, cz2 = st.columns([5, 1])
                cz1.text(f"🗺️  {z}")
                if cz2.button("🗑️", key=f"dz_{z}", help="Eliminar zona"):
                    try:
                        with conn.session as s:
                            s.execute(text("DELETE FROM zonas WHERE nombre = :z"), {"z": z})
                            s.commit()
                        st.cache_data.clear()
                        _flash("🗑️ Zona eliminada.")
                        st.rerun()
                    except Exception as e:
                        _flash(f"❌ Error: {e}", 'error')
                        st.rerun()
            st.divider()
            with st.form("add_zona", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Zona**")
                nz = st.text_input("Nombre del nodo / zona")
                if st.form_submit_button("Agregar Zona") and nz.strip():
                    try:
                        with conn.session as s:
                            s.execute(text("INSERT INTO zonas (nombre) VALUES (:z)"), {"z": nz.strip()})
                            s.commit()
                        st.cache_data.clear()
                        _flash("✅ Zona agregada.")
                        st.rerun()
                    except Exception as e:
                        _flash("❌ La zona ya existe o hubo un error.", "error")

        # ── Técnicos ──
        with t_tec:
            st.markdown("#### Gestión de Técnicos de Campo")
            for tec in get_tecnicos():
                ct1, ct2 = st.columns([5, 1])
                ct1.text(f"🔧  {tec}")
                if ct2.button("🗑️", key=f"dt_{tec}", help="Eliminar técnico"):
                    try:
                        with conn.session as s:
                            s.execute(text("DELETE FROM tecnicos WHERE nombre = :t"), {"t": tec})
                            s.commit()
                        st.cache_data.clear()
                        _flash("🗑️ Técnico eliminado.")
                        st.rerun()
                    except Exception as e:
                        _flash(f"❌ Error: {e}", 'error')
                        st.rerun()
            st.divider()
            with st.form("add_tec", clear_on_submit=True):
                st.markdown("**➕ Agregar Nuevo Técnico**")
                nt = st.text_input("Nombre completo del técnico")
                if st.form_submit_button("Agregar Técnico") and nt.strip():
                    try:
                        with conn.session as s:
                            s.execute(text("INSERT INTO tecnicos (nombre) VALUES (:t)"), {"t": nt.strip()})
                            s.commit()
                        st.cache_data.clear()
                        _flash("✅ Técnico agregado.")
                        st.rerun()
                    except Exception:
                        _flash("❌ El técnico ya existe o hubo un error.", "error")

        # ── Motivos ──
        with t_mot:
            st.markdown("#### Gestión de Motivos de Operación")
            st.info("🎨 **Verde** = Positivo  |  **Rojo** = Negativo  |  **Naranja** = Neutral")
            df_mots = get_motivos_df()
            for _, mr in df_mots.iterrows():
                cm1, cm2, cm3 = st.columns([4, 2, 1])
                cm1.text(f"📋  {mr.get('Motivo', '')}")
                tipo_m = str(mr.get('Tipo', 'neutral'))
                cm2.markdown(
                    f"<span style='color:{TIPO_COLOR.get(tipo_m, COLOR_WARN)};font-weight:700'>"
                    f"{TIPO_EMOJI.get(tipo_m, '⚪')} {tipo_m.capitalize()}</span>",
                    unsafe_allow_html=True,
                )
                if cm3.button("🗑️", key=f"dm_{mr.get('Motivo', '')}"):
                    try:
                        with conn.session as s:
                            s.execute(text("DELETE FROM motivos WHERE motivo = :m"), {"m": mr.get('Motivo')})
                            s.commit()
                        st.cache_data.clear()
                        _flash("🗑️ Motivo eliminado.")
                        st.rerun()
                    except Exception as e:
                        _flash(f"❌ Error: {e}", 'error')
                        st.rerun()
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
                    try:
                        with conn.session as s:
                            s.execute(
                                text("INSERT INTO motivos (motivo, tipo) VALUES (:m, :t)"),
                                {"m": nm.strip(), "t": ntm},
                            )
                            s.commit()
                        st.cache_data.clear()
                        _flash("✅ Motivo agregado.")
                        st.rerun()
                    except Exception:
                        _flash("❌ El motivo ya existe o hubo un error.", "error")

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
                    nu_r = st.selectbox(
                        "Rol",
                        ['auditor', 'admin'],
                        format_func=lambda x: {
                            'auditor': '📝 Auditor (Puede registrar y ver)',
                            'admin':   '⚙️ Administrador (Acceso total)',
                        }[x],
                    )
                    if st.form_submit_button("✅ Crear Usuario") and nu_u and nu_p:
                        err_p = _validate_pw(nu_p)
                        if err_p:
                            st.error(err_p)
                        else:
                            try:
                                with conn.session as s:
                                    s.execute(
                                        text("INSERT INTO usuarios (username, password_hash, role) "
                                             "VALUES (:u, :p, :r)"),
                                        {"u": nu_u, "p": _hash(nu_p), "r": nu_r},
                                    )
                                    s.commit()
                                _flash("✅ Usuario creado.")
                                st.rerun()
                            except Exception:
                                st.error("❌ Ese nombre de usuario ya existe o hubo un error.")

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
                                'password_hash':   _hash(rpp),
                                'failed_attempts': 0,
                                'locked_until':    None,
                            })
                            if ok:
                                _flash("✅ Contraseña actualizada.")
                                st.rerun()

            with u_col2:
                df_u_all = _get_users_raw()
                if not df_u_all.empty:
                    df_u_all.columns = [c.lower() for c in df_u_all.columns]

                    df_sa = df_u_all[df_u_all['username'] == SUPERADMIN]
                    df_ot = df_u_all[df_u_all['username'] != SUPERADMIN].reset_index(drop=True)

                    if not df_sa.empty:
                        st.markdown("🛡️ **Super Administrador (no editable)**")
                        st.dataframe(
                            df_sa[['username', 'role']].rename(
                                columns={'username': 'Usuario', 'role': 'Rol'}),
                            hide_index=True, use_container_width=True,
                        )
                        st.divider()

                    if not df_ot.empty:
                        st.markdown("**Usuarios Registrados:**")
                        for _, ur in df_ot.iterrows():
                            uc1, uc2, uc3, uc4, uc5 = st.columns([2, 1, 1, 1, 1])
                            uc1.markdown(f"**{ur['username']}**")
                            uc2.caption(ur.get('role', 'auditor'))
                            is_banned_u = bool(ur.get('is_banned', False))
                            fa_u        = int(ur.get('failed_attempts', 0) or 0)
                            uc3.caption(f"{'🚫 Baneado' if is_banned_u else '✅ Activo'}")
                            uc4.caption(f"{fa_u} int. fallidos")

                            with uc5:
                                if is_banned_u:
                                    if st.button("♻️", key=f"unban_{ur['username']}", help="Desbanear"):
                                        _update_user_fields(ur['username'], {'is_banned': False, 'failed_attempts': 0})
                                        _flash(f"✅ Usuario {ur['username']} desbaneado.")
                                        st.rerun()
                                else:
                                    if st.button("🚫", key=f"ban_{ur['username']}", help="Banear"):
                                        _update_user_fields(ur['username'], {'is_banned': True})
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
                                    try:
                                        with conn.session as s:
                                            s.execute(
                                                text("DELETE FROM usuarios WHERE username = :u"),
                                                {"u": ur['username']},
                                            )
                                            s.commit()
                                        _flash(f"🗑️ Usuario {ur['username']} eliminado.")
                                        st.rerun()
                                    except Exception as e:
                                        _flash(f"❌ Error al eliminar: {e}", "error")
                            st.divider()
                else:
                    st.info("No hay usuarios registrados aún.")
