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

MESES = ("Enero","Febrero","Marzo","Abril","Mayo","Junio",
         "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre")

SUPERADMIN = 'Admin'

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
    conn = st.connection("postgresql", type="sql")
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
    df = conn.query("SELECT nombre as \"Zona\" FROM zonas ORDER BY nombre ASC", ttl=10)
    return df["Zona"].tolist() if not df.empty else []

def get_tecnicos() -> list:
    df = conn.query("SELECT nombre as \"Tecnico\" FROM tecnicos ORDER BY nombre ASC", ttl=10)
    return df["Tecnico"].tolist() if not df.empty else []

def get_motivos_df() -> pd.DataFrame:
    df = conn.query("SELECT motivo as \"Motivo\", tipo as \"Tipo\" FROM motivos ORDER BY motivo ASC", ttl=10)
    return df

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
    return conn.query(
        "SELECT username, role, is_banned, failed_attempts FROM usuarios ORDER BY username ASC",
        ttl=5,
    )

# BUG FIX #1: Antes usaba %(u)s (sintaxis psycopg2 nativo) que SQLAlchemy
# no reconoce. Ahora usa conn.session con text() y :param style, igual que
# el resto de las funciones de escritura.
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
    # BUG FIX #2: Antes podía lanzar AttributeError si user era None
    # y se accedía sin comprobar. Ahora es seguro.
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

    # BUG FIX #3: Se usaba datetime.now(SV_TZ).replace(tzinfo=None) para
    # quitar la zona horaria y luego se comparaba contra lu (que podría tener
    # o no tzinfo). Ahora siempre se trabaja en naive UTC local para coherencia.
    now = datetime.now(SV_TZ).replace(tzinfo=None)

    is_banned = bool(user.get('is_banned', False))
    fa        = int(user.get('failed_attempts', 0) or 0)
    lu_str    = user.get('locked_until')

    # BUG FIX #4: La conversión de locked_until era frágil; se añade manejo
    # explícito para objetos datetime con tzinfo (los quita para comparar).
    if lu_str is not None and lu_str != '':
        try:
            if isinstance(lu_str, str):
                lu = datetime.fromisoformat(lu_str)
            elif isinstance(lu_str, datetime):
                lu = lu_str
            else:
                lu = None

            if lu is not None:
                # Normalizar a naive
                if lu.tzinfo is not None:
                    lu = lu.astimezone(SV_TZ).replace(tzinfo=None)
                if lu > now:
                    mins = int((lu - now).total_seconds() // 60) + 1
                    st.session_state.log_err = f"⏳ Cuenta bloqueada por {mins} min."
                    return
        except Exception:
            pass  # Si no se puede parsear, ignoramos el bloqueo

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
# BUG FIX #5: Si _find_user falla (DB caída, etc.) el token devuelto era ''
# y se cerraba la sesión erróneamente. Ahora solo expulsa si el token de DB
# es NO vacío y diferente al local.
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
def load_month_data(y: int, m: int) -> pd.DataFrame:
    sql = f"""
        SELECT id as "ID", fecha as "Fecha", asesor as "Asesor", tecnico as "Tecnico",
               zona as "Zona", sn_eliminada as "SN_Eliminada", sn_agregada as "SN_Agregada",
               motivo as "Motivo", cod_cliente as "Cod_Cliente", nombre_cliente as "Nombre_Cliente",
               orden_trabajo as "Orden_Trabajo", descripcion as "Descripcion"
        FROM registros_ont
        WHERE EXTRACT(YEAR  FROM fecha) = {int(y)}
          AND EXTRACT(MONTH FROM fecha) = {int(m)}
          AND eliminado = FALSE
        ORDER BY timestamp DESC
    """
    return conn.query(sql, ttl=0)

def load_year_data(y: int) -> pd.DataFrame:
    sql = f"""
        SELECT id as "ID", fecha as "Fecha", asesor as "Asesor", tecnico as "Tecnico",
               zona as "Zona", sn_eliminada as "SN_Eliminada", sn_agregada as "SN_Agregada",
               motivo as "Motivo", cod_cliente as "Cod_Cliente", nombre_cliente as "Nombre_Cliente",
               orden_trabajo as "Orden_Trabajo", descripcion as "Descripcion",
               EXTRACT(MONTH FROM fecha) as "_mes"
        FROM registros_ont
        WHERE EXTRACT(YEAR FROM fecha) = {int(y)}
          AND eliminado = FALSE
        ORDER BY timestamp DESC
    """
    return conn.query(sql, ttl=0)

def append_ont_record(record: dict) -> bool:
    sql = text("""
        INSERT INTO registros_ont
        (fecha, asesor, tecnico, zona, sn_eliminada, sn_agregada,
         motivo, cod_cliente, nombre_cliente, orden_trabajo, descripcion)
        VALUES (:f, :a, :t, :z, :sne, :sna, :m, :cc, :nc, :ot, :d)
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
    }
    try:
        with conn.session as s:
            s.execute(sql, params)
            s.commit()
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

    pzt = pd.DataFrame()
    if 'Zona' in df.columns and 'Motivo' in df.columns:
        df2          = df.copy()
        df2['Tipo']  = df2['Motivo'].map(lambda x: tipo_map.get(x, 'neutral').capitalize())
        pzt          = df2.groupby(['Zona', 'Tipo']).size().reset_index(name='N')

    return {
        'total': total, 'pos': pos, 'neg': neg, 'neu': neu, 'balance': pos - neg,
        'por_motivo': pm, 'por_zona': pz, 'por_tecnico': pt, 'por_asesor': pa,
        'por_zona_tipo': pzt,
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
        f"({st.session_state.role.capitalize()})  |  ONT Manager v3.0 (Neon)"
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

    df_cur  = load_month_data(anio, m_idx)
    pm_anio = anio - 1 if m_idx == 1 else anio
    pm_mes  = 12      if m_idx == 1 else m_idx - 1
    df_prev = load_month_data(pm_anio, pm_mes)

    kpi  = calc_metrics(df_cur,  tipo_map)
    kpip = calc_metrics(df_prev, tipo_map)

    st.markdown("### 📈 Resumen del Mes")
    st.caption(f"*Variación vs {MESES[pm_mes - 1]} {pm_anio}*")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📦 Total Operaciones",
              kpi['total'],   _delta_str(kpi['total'],   kpip['total']),   delta_color='normal')
    c2.metric("✅ Positivas (Inst./Reconex.)",
              kpi['pos'],     _delta_str(kpi['pos'],     kpip['pos']),     delta_color=_delta_color(kpi['pos'],     kpip['pos'],     'positive'))
    c3.metric("⚠️ Negativas (Prob./Descon.)",
              kpi['neg'],     _delta_str(kpi['neg'],     kpip['neg']),     delta_color=_delta_color(kpi['neg'],     kpip['neg'],     'negative'))
    c4.metric("🔄 Neutrales (Cambios)",
              kpi['neu'],     _delta_str(kpi['neu'],     kpip['neu']),     delta_color='normal')
    c5.metric("⚖️ Balance Neto (+ vs −)",
              kpi['balance'], _delta_str(kpi['balance'], kpip['balance']), delta_color=_delta_color(kpi['balance'], kpip['balance'], 'positive'))

    if df_cur.empty:
        st.info("ℹ️ No hay registros para este mes. Comienza registrando un movimiento.")
    else:
        st.divider()
        st.markdown("### 📋 Desglose por Motivo")
        col_mot, col_pie = st.columns(2)

        with col_mot:
            pm_df = (pd.DataFrame(list(kpi['por_motivo'].items()), columns=['Motivo', 'Cantidad'])
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
                'Tipo':     ['Positivo', 'Negativo', 'Neutral'],
                'Cantidad': [kpi['pos'], kpi['neg'],  kpi['neu']],
            })
            fig_pie = px.pie(pie_df, names='Tipo', values='Cantidad', hole=0.5,
                             color_discrete_sequence=[COLOR_TEAL, COLOR_DANGER, COLOR_WARN],
                             title="Balance por Tipo")
            fig_pie.update_traces(textinfo='percent+label', textposition='inside')
            fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=370,
                                  showlegend=False)
            st.plotly_chart(fig_pie, use_container_width=True)

        st.divider()
        st.markdown("### 🗺️ Análisis por Zona")
        col_z1, col_z2 = st.columns(2)

        with col_z1:
            z_df = (pd.DataFrame(list(kpi['por_zona'].items()), columns=['Zona', 'Operaciones'])
                    .sort_values('Operaciones', ascending=False))
            fig_z = px.bar(z_df, x='Zona', y='Operaciones', text_auto=True,
                           color='Operaciones', color_continuous_scale='Blues',
                           title="Total Operaciones por Zona")
            fig_z.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=360,
                                margin=dict(l=0, r=0, t=40, b=0), xaxis_tickangle=-30)
            st.plotly_chart(fig_z, use_container_width=True)

        with col_z2:
            if not kpi['por_zona_tipo'].empty:
                # BUG FIX #6: El color_discrete_map usaba claves capitalizadas
                # ('Positive','Negative','Neutral') que coinciden con el .capitalize()
                # aplicado en calc_metrics — se verifica que sean consistentes.
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

        st.divider()
        col_tec, col_as = st.columns(2)

        with col_tec:
            if kpi['por_tecnico']:
                tec_df = (pd.DataFrame(list(kpi['por_tecnico'].items()),
                                       columns=['Técnico', 'Operaciones'])
                          .sort_values('Operaciones', ascending=True))
                fig_tec = px.bar(tec_df, x='Operaciones', y='Técnico', orientation='h',
                                 text_auto=True,
                                 color_discrete_sequence=[COLOR_PRIMARY],
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
                                text_auto=True,
                                color_discrete_sequence=[COLOR_TEAL],
                                title="👤 Registros por Asesor")
                fig_as.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=340,
                                     margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_as, use_container_width=True)

        st.divider()
        st.markdown("### ⚖️ Balance: Instalaciones & Reconexiones vs Desconexiones")
        b_col1, b_col2, b_col3 = st.columns(3)

        inst_rc = (kpi['por_motivo'].get('Instalacion Nueva', 0)
                   + kpi['por_motivo'].get('Reconexion', 0))
        descon  = kpi['por_motivo'].get('Desconexion', 0)
        neto    = inst_rc - descon

        b_col1.metric("📥 Inst. + Reconexiones", inst_rc)
        b_col2.metric("📤 Desconexiones",         descon)
        b_col3.metric("⚖️ Neto de Clientes",      neto,
                      f"{'▲' if neto >= 0 else '▼'} {abs(neto)}",
                      delta_color="normal" if neto >= 0 else "inverse")

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
                mi  = MESES.index(mc) + 1
                dfc = load_month_data(anio_comp, mi)
                km  = calc_metrics(dfc, tipo_map)
                comp_rows.append({
                    'Mes':       mc,
                    'Total':     km['total'],
                    'Positivos': km['pos'],
                    'Negativos': km['neg'],
                    'Neutrales': km['neu'],
                    'Balance':   km['balance'],
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

            st.markdown("**📉📈 Variación entre meses consecutivos seleccionados:**")
            for i in range(1, len(comp_rows)):
                cur_c = comp_rows[i]
                prv_c = comp_rows[i - 1]

                def _cell(label, cur_v, prv_v, tipo):
                    d   = cur_v - prv_v
                    sig = '+' if d >= 0 else ''
                    if   tipo == 'positive': clr = COLOR_TEAL  if d >= 0 else COLOR_DANGER
                    elif tipo == 'negative': clr = COLOR_TEAL  if d <= 0 else COLOR_DANGER
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
                rc2.markdown(_cell("✅ Positivos", cur_c['Positivos'], prv_c['Positivos'], 'positive'), unsafe_allow_html=True)
                rc3.markdown(_cell("⚠️ Negativos", cur_c['Negativos'], prv_c['Negativos'], 'negative'), unsafe_allow_html=True)
                rc4.markdown(_cell("🔄 Neutrales", cur_c['Neutrales'], prv_c['Neutrales'], 'neutral'),  unsafe_allow_html=True)
                rc5.markdown(_cell("⚖️ Balance",   cur_c['Balance'],   prv_c['Balance'],   'positive'), unsafe_allow_html=True)
                st.divider()

        st.divider()
        st.markdown(f"### 📆 Tendencia Anual — {anio}")
        with st.spinner("Cargando datos anuales…"):
            df_year = load_year_data(anio)

        if not df_year.empty:
            annual_rows = []
            for mi in range(1, 13):
                # BUG FIX #7: Antes comparaba df_year['_mes'] == mi pero la columna
                # EXTRACT devuelve float en algunos drivers (ej. 1.0 en vez de 1).
                # Se convierte a int para la comparación.
                if '_mes' in df_year.columns:
                    df_mi = df_year[df_year['_mes'].astype(float).astype(int) == mi]
                else:
                    df_mi = pd.DataFrame()
                km = calc_metrics(df_mi, tipo_map)
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

        zonas_l = get_zonas()
        tecs_l  = get_tecnicos()
        mots_l  = get_motivos_list()
        today   = datetime.now(SV_TZ).date()

        # BUG FIX #8: Si los catálogos están vacíos los selectbox fallan.
        # Se muestra advertencia en lugar de crash.
        if not zonas_l or not tecs_l or not mots_l:
            st.warning("⚠️ Faltan catálogos (zonas, técnicos o motivos). "
                       "Ve a ⚙️ Configuración y agrégalos primero.")
        else:
            with st.container(border=True):
                st.info(
                    f"👤 **Asesor registrado automáticamente:** `{st.session_state.username}`  "
                    f"|  📅 **Hoy:** `{today.strftime('%d/%m/%Y')}`"
                )

                cc1, cc2 = st.columns(2)
                tec    = cc1.selectbox("🔧 Técnico de Campo *",              tecs_l,  key=f"tec_{fk}")
                zona   = cc2.selectbox("🗺️ Zona *",                          zonas_l, key=f"zon_{fk}")
                motivo = st.selectbox("📋 Motivo / Tipo de Operación *",     mots_l,  key=f"mot_{fk}")

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

        st.dataframe(df_hist, use_container_width=True, hide_index=True)
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
        st.download_button(
            "📥 Descargar CSV",
            df_hist.to_csv(index=False).encode('utf-8'),
            f"ONT_{mes_sel}_{anio}.csv",
            "text/csv",
            use_container_width=True,
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
