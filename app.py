# ==============================
# app.py (v3.2 – Devolução por empréstimo individual + pendentes)
# ==============================
# Sala de Leitura – Empréstimos de Livros e Jogos
# UI: Streamlit | Banco: SQLite (sala_leitura.db)

from __future__ import annotations
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, date
import sqlite3
from io import BytesIO

DB_PATH = Path("sala_leitura.db")

# ---------------- Campos padrão ----------------
COLS_MOV = [
    "timestamp","data","hora","tipo",           # Emprestimo, Devolucao
    "item_nome","categoria",
    "aluno_nome","aluno_sobrenome","aluno_serie",
    "responsavel","prev_devolucao","observacoes",
    "quantidade",
    "beneficiario_tipo",
    "beneficiario_nome",
    "loan_id",                                  # <- NOVO: id do empréstimo original
]

COLS_ITENS = [
    "item_nome","categoria",
    "titulo","autor","editora","genero","isbn","edicao",
    "quant_total",
]

COLS_ALUNOS = ["nome","sobrenome","serie"]

# ---------------- SQLite helpers ----------------

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def _table_has_column(con, table, column):
    cur = con.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]  # name is index 1
    return column in cols

def init_db():
    with get_conn() as con:
        cur = con.cursor()
        # movimentacoes
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS movimentacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                data TEXT,
                hora TEXT,
                tipo TEXT,
                item_nome TEXT,
                categoria TEXT,
                aluno_nome TEXT,
                aluno_sobrenome TEXT,
                aluno_serie TEXT,
                responsavel TEXT,
                prev_devolucao TEXT,
                observacoes TEXT,
                quantidade INTEGER DEFAULT 1,
                beneficiario_tipo TEXT DEFAULT '',
                beneficiario_nome TEXT DEFAULT '',
                loan_id INTEGER
            );
            """
        )
        # itens (catálogo)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS itens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_nome TEXT UNIQUE,
                categoria TEXT,
                titulo TEXT,
                autor TEXT,
                editora TEXT,
                genero TEXT,
                isbn TEXT,
                edicao TEXT,
                quant_total INTEGER DEFAULT 1
            );
            """
        )
        # alunos (autocomplete)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alunos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT,
                sobrenome TEXT,
                serie TEXT,
                UNIQUE(nome, sobrenome, serie)
            );
            """
        )
        con.commit()

        # Migrações leves (garantia de colunas)
        for col, ddl in [
            ("quantidade", "INTEGER DEFAULT 1"),
            ("beneficiario_tipo", "TEXT DEFAULT ''"),
            ("beneficiario_nome", "TEXT DEFAULT ''"),
            ("loan_id", "INTEGER"),
        ]:
            if not _table_has_column(con, "movimentacoes", col):
                con.execute(f"ALTER TABLE movimentacoes ADD COLUMN {col} {ddl}")
        for col, ddl in [
            ("titulo","TEXT"),("autor","TEXT"),("editora","TEXT"),("genero","TEXT"),
            ("isbn","TEXT"),("edicao","TEXT"),("quant_total","INTEGER DEFAULT 1"),
        ]:
            if not _table_has_column(con, "itens", col):
                con.execute(f"ALTER TABLE itens ADD COLUMN {col} {ddl}")
        con.commit()

def _benef_key(row: pd.Series) -> tuple:
    """Chave do beneficiário para casar devoluções antigas com empréstimos."""
    bt = (row.get("beneficiario_tipo") or "").strip().lower()
    if bt == "professor":
        return ("professor", (row.get("beneficiario_nome") or "").strip())
    # aluno
    return ("aluno",
            (row.get("aluno_nome") or "").strip(),
            (row.get("aluno_sobrenome") or "").strip(),
            (row.get("aluno_serie") or "").strip())

def migrate_link_old_returns():
    """Vincula loan_id dos empréstimos e tenta associar devoluções antigas (sem loan_id) ao(s) empréstimo(s) corretos (FIFO)."""
    with get_conn() as con:
        con.row_factory = sqlite3.Row

        # 1) loan_id = id para todas as linhas de tipo Emprestimo sem loan_id
        con.execute("""
            UPDATE movimentacoes
               SET loan_id = id
             WHERE (loan_id IS NULL OR loan_id = 0)
               AND lower(tipo) LIKE 'emprest%';
        """)

        # 2) Carrega empréstimos ordenados por tempo
        df_emp = pd.read_sql_query("""
            SELECT id, item_nome, quantidade, beneficiario_tipo, beneficiario_nome,
                   aluno_nome, aluno_sobrenome, aluno_serie, timestamp
              FROM movimentacoes
             WHERE lower(tipo) LIKE 'emprest%'
             ORDER BY datetime(timestamp)
        """, con)
        if df_emp.empty:
            return

        # saldo disponível por empréstimo (inicialmente a própria quantidade menos devoluções já vinculadas)
        df_devs_linked = pd.read_sql_query("""
            SELECT loan_id, SUM(quantidade) as q
              FROM movimentacoes
             WHERE lower(tipo) LIKE 'devolu%' AND loan_id IS NOT NULL
             GROUP BY loan_id
        """, con)
        linked_map = {int(r["loan_id"]): int(r["q"]) for _, r in df_devs_linked.iterrows()} if not df_devs_linked.empty else {}

        # constrói estrutura FIFO por (item, beneficiario)
        fifo = {}  # key -> list of dicts {id, disponivel}
        for _, e in df_emp.iterrows():
            key = ( (e["item_nome"] or "").strip(), _benef_key(e) )
            disponivel = int(e["quantidade"] or 0) - int(linked_map.get(int(e["id"]), 0))
            if disponivel <= 0:
                continue
            fifo.setdefault(key, []).append({"id": int(e["id"]), "disponivel": disponivel})

        # 3) Processa devoluções SEM loan_id
        df_dev = pd.read_sql_query("""
            SELECT id, item_nome, quantidade, beneficiario_tipo, beneficiario_nome,
                   aluno_nome, aluno_sobrenome, aluno_serie, timestamp, data, hora,
                   responsavel, prev_devolucao, observacoes, categoria
              FROM movimentacoes
             WHERE lower(tipo) LIKE 'devolu%'
               AND (loan_id IS NULL OR loan_id = 0)
             ORDER BY datetime(timestamp)
        """, con)

        for _, d in df_dev.iterrows():
            key = ( (d["item_nome"] or "").strip(), _benef_key(d) )
            fila = fifo.get(key, [])
            if not fila:
                # Não achou empréstimo correspondente — mantém sem loan_id
                continue
            restante = int(d["quantidade"] or 0)
            # vamos fatiar se necessário: criar novas devoluções coladas com loan_id
            for bucket in fila:
                if restante <= 0:
                    break
                if bucket["disponivel"] <= 0:
                    continue
                aloca = min(restante, bucket["disponivel"])
                # cria nova linha de devolução vinculada
                con.execute("""
                    INSERT INTO movimentacoes (
                        timestamp, data, hora, tipo, item_nome, categoria,
                        aluno_nome, aluno_sobrenome, aluno_serie, responsavel,
                        prev_devolucao, observacoes, quantidade,
                        beneficiario_tipo, beneficiario_nome, loan_id
                    ) VALUES (?, ?, ?, 'Devolucao', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    d["timestamp"], d["data"], d["hora"], d["item_nome"], d["categoria"],
                    d["aluno_nome"], d["aluno_sobrenome"], d["aluno_serie"], d["responsavel"],
                    d["prev_devolucao"], d["observacoes"], int(aloca),
                    d["beneficiario_tipo"], d["beneficiario_nome"], int(bucket["id"])
                ))
                bucket["disponivel"] -= aloca
                restante -= aloca

            if restante <= 0:
                # Remove a devolução antiga original (não vinculada)
                con.execute("DELETE FROM movimentacoes WHERE id = ?", (int(d["id"]),))
            else:
                # Atualiza a devolução original com o que sobrou (sem loan_id mesmo)
                con.execute("UPDATE movimentacoes SET quantidade=? WHERE id=?", (int(restante), int(d["id"])))

        con.commit()

def ensure_migrations():
    init_db()
    migrate_link_old_returns()

@st.cache_data(ttl=5)
def df_mov() -> pd.DataFrame:
    ensure_migrations()
    with get_conn() as con:
        df = pd.read_sql_query(
            "SELECT timestamp,data,hora,tipo,item_nome,categoria,aluno_nome,aluno_sobrenome,aluno_serie,"
            "responsavel,prev_devolucao,observacoes,quantidade,beneficiario_tipo,beneficiario_nome,loan_id "
            "FROM movimentacoes",
            con,
        )
    if df.empty:
        return pd.DataFrame(columns=COLS_MOV)
    # coerção
    if "quantidade" in df.columns:
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0).astype(int)
    if "loan_id" in df.columns:
        df["loan_id"] = pd.to_numeric(df["loan_id"], errors="coerce").fillna(0).astype(int)
    # preenche colunas faltantes
    for c in COLS_MOV:
        if c not in df.columns:
            df[c] = "" if c not in ("quantidade","loan_id") else 0
    return df[COLS_MOV].copy()

@st.cache_data(ttl=5)
def df_itens() -> pd.DataFrame:
    ensure_migrations()
    with get_conn() as con:
        df = pd.read_sql_query(
            "SELECT item_nome,categoria,titulo,autor,editora,genero,isbn,edicao,quant_total FROM itens",
            con,
        )
    if df.empty:
        return pd.DataFrame(columns=COLS_ITENS)
    df["quant_total"] = pd.to_numeric(df["quant_total"], errors="coerce").fillna(0).astype(int)
    return df[COLS_ITENS].fillna("")

@st.cache_data(ttl=5)
def df_alunos() -> pd.DataFrame:
    ensure_migrations()
    with get_conn() as con:
        df = pd.read_sql_query("SELECT nome,sobrenome,serie FROM alunos", con)
    if df.empty:
        return pd.DataFrame(columns=COLS_ALUNOS)
    return df[COLS_ALUNOS].fillna("")

# CRUD helpers

def inserir_movimento(reg: dict) -> int:
    """Insere movimento. Se for Emprestimo sem loan_id, atribui loan_id=id do próprio registro.
       Retorna o id do movimento inserido."""
    ensure_migrations()
    # valores padrão
    base = {c: (0 if c in ("quantidade","loan_id") else "") for c in COLS_MOV}
    reg2 = {**base, **reg}
    if not reg2.get("quantidade"):
        reg2["quantidade"] = 1
    with get_conn() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO movimentacoes (
                timestamp,data,hora,tipo,item_nome,categoria,aluno_nome,aluno_sobrenome,aluno_serie,
                responsavel,prev_devolucao,observacoes,quantidade,beneficiario_tipo,beneficiario_nome,loan_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [reg2.get(c, "") for c in COLS_MOV],
        )
        mid = cur.lastrowid
        # Se for empréstimo e loan_id não veio, marca loan_id = id
        if str(reg2.get("tipo","")).lower().startswith("emprest") and (not reg2.get("loan_id")):
            cur.execute("UPDATE movimentacoes SET loan_id = ? WHERE id = ?", (int(mid), int(mid)))
        con.commit()
    # limpa caches
    df_mov.clear(); df_itens.clear()
    return int(mid)

def upsert_item_catalogo(**campos):
    # chave preferencial: isbn; fallback: titulo+autor+edicao; para Jogo: item_nome
    ensure_migrations()
    with get_conn() as con:
        isbn = (campos.get("isbn") or "").strip()
        titulo = (campos.get("titulo") or "").strip()
        autor = (campos.get("autor") or "").strip()
        edicao = (campos.get("edicao") or "").strip()
        item_nome = (campos.get("item_nome") or "").strip()
        categoria = (campos.get("categoria") or "Livro").strip()

        row = None
        if categoria.lower() == "jogo":
            if item_nome:
                cur = con.execute("SELECT id FROM itens WHERE item_nome=?", (item_nome,))
                row = cur.fetchone()
        else:
            if isbn:
                cur = con.execute("SELECT id FROM itens WHERE isbn=?", (isbn,))
                row = cur.fetchone()
            else:
                cur = con.execute(
                    "SELECT id FROM itens WHERE (titulo=? AND autor=? AND IFNULL(edicao,'')=?)",
                    (titulo, autor, edicao),
                )
                row = cur.fetchone()

        campos.setdefault("quant_total", 1)
        campos.setdefault("categoria", categoria or "Livro")
        if not item_nome:
            campos["item_nome"] = (titulo or isbn or autor)

        if row:
            set_clause = ",".join([f"{k}=?" for k in ["item_nome","categoria","titulo","autor","editora","genero","isbn","edicao","quant_total"]])
            params = [campos.get(k) for k in ["item_nome","categoria","titulo","autor","editora","genero","isbn","edicao","quant_total"]] + [row[0]]
            con.execute(f"UPDATE itens SET {set_clause} WHERE id=?", params)
        else:
            con.execute(
                """
                INSERT INTO itens (item_nome,categoria,titulo,autor,editora,genero,isbn,edicao,quant_total)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                [campos.get(k) for k in ["item_nome","categoria","titulo","autor","editora","genero","isbn","edicao","quant_total"]],
            )
        con.commit()
    df_itens.clear()

# ---------------- Regras de saldo e status ----------------

def saldo_por_item(dfm: pd.DataFrame) -> pd.DataFrame:
    dfi = df_itens()
    base = (dfi[["item_nome","titulo","quant_total"]].copy()
            if not dfi.empty else pd.DataFrame(columns=["item_nome","titulo","quant_total"]))
    if dfm.empty:
        base["emprestado"] = 0
        base["disponivel"] = base["quant_total"]
        return base
    mov = dfm.copy()
    mov["sinal"] = mov["tipo"].str.lower().map(lambda t: 1 if t.startswith("emprest") else (-1 if t.startswith("devolu") else 0))
    mov["delta"] = mov["quantidade"].astype(int) * mov["sinal"]
    agg = mov.groupby("item_nome")["delta"].sum().rename("emprestado").reset_index()
    out = base.merge(agg, on="item_nome", how="left").fillna({"emprestado":0})
    out["emprestado"] = out["emprestado"].astype(int).clip(lower=0)
    out["disponivel"] = (out["quant_total"] - out["emprestado"]).clip(lower=0)
    return out

def status_itens(dfm: pd.DataFrame) -> pd.DataFrame:
    dfi = df_itens()
    sal = saldo_por_item(dfm)
    if dfm.empty:
        ult = pd.DataFrame(columns=["item_nome","categoria","status","aluno","turma","prev_devolucao"])
    else:
        df = dfm.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        idx = df.sort_values("timestamp").groupby("item_nome").tail(1).index
        ult = df.loc[idx, ["item_nome","categoria","tipo","aluno_nome","aluno_sobrenome","aluno_serie","prev_devolucao"]].copy()
        ult["status"] = ult["tipo"].apply(lambda t: "Emprestado" if str(t).lower().startswith("emprest") else "Disponível")
        ult["aluno"] = (ult["aluno_nome"].fillna("") + " " + ult["aluno_sobrenome"].fillna("")).str.strip()
        ult["turma"] = ult["aluno_serie"].fillna("")
        ult = ult[["item_nome","categoria","status","aluno","turma","prev_devolucao"]]
    if not dfi.empty:
        ult = ult.merge(dfi[["item_nome","titulo"]], on="item_nome", how="right").fillna("")
    else:
        ult["titulo"] = ult["item_nome"]
    ult = ult.merge(sal[["item_nome","quant_total","emprestado","disponivel"]],
                    on="item_nome", how="left").fillna({"quant_total":0,"emprestado":0,"disponivel":0})
    return ult

# ---------------- Empréstimos pendentes (por loan) ----------------

def emprestimos_pendentes_df() -> pd.DataFrame:
    """Retorna cada empréstimo com saldo pendente (>0), incluindo aluno/professor."""
    dfm = df_mov()
    if dfm.empty:
        return pd.DataFrame(columns=[
            "loan_id","item_nome","categoria","titulo","data","hora","prev_devolucao",
            "beneficiario_tipo","beneficiario_nome","aluno_nome","aluno_sobrenome","aluno_serie",
            "q_emprestado","q_devolvido","q_pendente","atrasado"
        ])
    # base: todos empréstimos (com loan_id == id, garantido pela migração)
    emp = dfm[dfm["tipo"].str.lower().str.startswith("emprest")].copy()
    if emp.empty:
        return pd.DataFrame(columns=[
            "loan_id","item_nome","categoria","titulo","data","hora","prev_devolucao",
            "beneficiario_tipo","beneficiario_nome","aluno_nome","aluno_sobrenome","aluno_serie",
            "q_emprestado","q_devolvido","q_pendente","atrasado"
        ])

    # devoluções por loan_id
    dev = dfm[dfm["tipo"].str.lower().str.startswith("devolu") & (dfm["loan_id"]>0)][["loan_id","quantidade"]].groupby("loan_id").sum()
    emp["q_devolvido"] = emp["loan_id"].map(dev["quantidade"]) if not dev.empty else 0
    emp["q_devolvido"] = emp["q_devolvido"].fillna(0).astype(int)
    emp["q_emprestado"] = emp["quantidade"].astype(int)
    emp["q_pendente"] = (emp["q_emprestado"] - emp["q_devolvido"]).clip(lower=0).astype(int)

    # junta título
    dfi = df_itens()
    if not dfi.empty:
        emp = emp.merge(dfi[["item_nome","titulo"]], on="item_nome", how="left")
    else:
        emp["titulo"] = emp["item_nome"]

    # atraso
    def _is_late(s):
        try:
            if not s: return False
            d = datetime.strptime(s, "%d/%m/%Y").date()
            return d < date.today()
        except Exception:
            return False
    emp["atrasado"] = emp["prev_devolucao"].apply(_is_late)

    # apenas pendentes
    cols = ["loan_id","item_nome","categoria","titulo","data","hora","prev_devolucao",
            "beneficiario_tipo","beneficiario_nome","aluno_nome","aluno_sobrenome","aluno_serie",
            "q_emprestado","q_devolvido","q_pendente","atrasado"]
    out = emp.loc[emp["q_pendente"]>0, cols].sort_values(["atrasado","prev_devolucao","data","hora"], ascending=[False, True, True, True])
    return out.reset_index(drop=True)

# ---------------- UI ----------------

st.set_page_config(page_title="Sala de Leitura - Sistema", page_icon="📚", layout="wide")
st.title("📚 Sala de Leitura – Sistema Oficial")

with st.sidebar:
    st.header("Configurações rápidas")
    resp = st.text_input("Responsável de hoje", value=st.session_state.get("responsavel", ""), key="resp_dia")
    st.session_state["responsavel"] = resp
    st.caption(f"Banco: {DB_PATH.resolve()}")

abas = st.tabs([
    "➕ Empréstimo Aluno",
    "👩‍🏫 Empréstimo Professor",
    "↩️ Devolução (por empréstimo)",
    "📚 Catálogo",
    "🔎 Consulta / Exportar",
])

# ------ Dados atuais ------
dfm = df_mov()
dfi = df_itens()
dfa = df_alunos()
saldos = saldo_por_item(dfm)

# Autocomplete de aluno
alunos_options = []
if not dfa.empty:
    alunos_options = [f"{r.nome} {r.sobrenome} — {r.serie}".strip() for r in dfa.itertuples(index=False)]

def _base_registro(tipo:str, item_nome:str, categoria:str, quantidade:int, prev_dev:datetime|None, observ:str,
                   aluno_nome:str="", aluno_sobrenome:str="", aluno_serie:str="",
                   beneficiario_tipo:str="aluno", beneficiario_nome:str="", loan_id:int=0) -> int:
    agora = datetime.now()
    return inserir_movimento({
        "timestamp": agora.isoformat(timespec='seconds'),
        "data": agora.strftime('%d/%m/%Y'),
        "hora": agora.strftime('%H:%M:%S'),
        "tipo": tipo,
        "item_nome": item_nome,
        "categoria": categoria or "Livro",
        "aluno_nome": aluno_nome,
        "aluno_sobrenome": aluno_sobrenome,
        "aluno_serie": aluno_serie,
        "responsavel": st.session_state.get("responsavel", ""),
        "prev_devolucao": prev_dev.strftime('%d/%m/%Y') if prev_dev else "",
        "observacoes": observ,
        "quantidade": int(quantidade) if quantidade else 1,
        "beneficiario_tipo": beneficiario_tipo,
        "beneficiario_nome": beneficiario_nome,
        "loan_id": int(loan_id or 0),
    })

# ------ Aba: Empréstimo Aluno ------
with abas[0]:
    st.subheader("Empréstimo para Aluno")

    col1, col2 = st.columns([2, 1])
    with col1:
        # labels com categoria
        labels = {}
        if not dfi.empty:
            for r in dfi.itertuples(index=False):
                disp = int(saldos.loc[saldos['item_nome']==r.item_nome,'disponivel'].values[0]) if (saldos['item_nome']==r.item_nome).any() else int(r.quant_total)
                nome_visivel = (r.titulo or r.item_nome)
                labels[r.item_nome] = f"[{r.categoria or 'Livro'}] {nome_visivel} (disp: {disp})"
        escolha = st.selectbox("Item", options=list(labels.keys()) if labels else [], format_func=lambda k: labels.get(k, k), index=None, key="sel_item_aluno")
        quantidade = st.number_input("Quantidade", min_value=1, value=1, key="qtd_aluno")
        dias = st.number_input("Prazo (dias)", min_value=1, max_value=60, value=7, key="prazo_aluno")
        prev_dev = datetime.now() + timedelta(days=int(dias))
        st.caption(f"Prev. devolução: {prev_dev.strftime('%d/%m/%Y')}")

        st.markdown("### Aluno")
        aluno_sel = st.selectbox("Aluno (autocomplete)", alunos_options if alunos_options else [""], index=None, placeholder="Buscar aluno já cadastrado...", key="autocomp_aluno")
        nome_pref = sobrenome_pref = serie_pref = ""
        if aluno_sel:
            try:
                nome_pref, rest = aluno_sel.split(" ", 1)
                if "—" in rest:
                    sobrenome_pref, serie_pref = [p.strip() for p in rest.split("—", 1)]
            except Exception:
                nome_pref = aluno_sel
        c1,c2,c3 = st.columns(3)
        with c1: nome = st.text_input("Nome *", value=nome_pref, key="aluno_nome")
        with c2: sobrenome = st.text_input("Sobrenome *", value=sobrenome_pref, key="aluno_sobrenome")
        with c3: serie = st.text_input("Série *", value=serie_pref, key="aluno_serie")
        observ = st.text_input("Observações", key="obs_aluno")

    with col2:
        pode = bool(escolha) and nome.strip() and sobrenome.strip() and serie.strip() and quantidade>0
        if st.button("✅ Registrar Empréstimo", use_container_width=True, disabled=not pode, key="btn_emp_aluno"):
            if nome.strip() or sobrenome.strip():
                with get_conn() as con:
                    con.execute("INSERT OR IGNORE INTO alunos (nome,sobrenome,serie) VALUES (?,?,?)", (nome.strip(), sobrenome.strip(), serie.strip()))
                    con.commit()
                df_alunos.clear()
            cat_sel = dfi.loc[dfi["item_nome"]==escolha, "categoria"].values
            categoria_item = (cat_sel[0] if len(cat_sel) else "Livro")
            _base_registro(
                tipo="Emprestimo",
                item_nome=escolha,
                categoria=categoria_item,
                quantidade=quantidade,
                prev_dev=prev_dev,
                observ=observ,
                aluno_nome=nome, aluno_sobrenome=sobrenome, aluno_serie=serie,
                beneficiario_tipo="aluno", beneficiario_nome="",
            )
            st.success("Empréstimo registrado.")

    st.markdown("---")
    st.caption("Saldos do catálogo")
    st.dataframe(status_itens(df_mov()), use_container_width=True, hide_index=True)

# ------ Aba: Empréstimo Professor ------
with abas[1]:
    st.subheader("Empréstimo para Professor (multitítulo / multiquantidade)")
    if dfi.empty:
        st.warning("Catálogo vazio. Adicione na aba Catálogo.")
    else:
        prof = st.text_input("Nome do Professor *", key="prof_nome")
        dias_p = st.number_input("Prazo (dias)", min_value=1, max_value=120, value=14, key="prazo_prof")
        prev_p = datetime.now() + timedelta(days=int(dias_p))
        st.caption(f"Prev. devolução: {prev_p.strftime('%d/%m/%Y')}")

        st.markdown("### Seleção de itens")
        labels = {
            r.item_nome: f"[{r.categoria or 'Livro'}] {r.titulo or r.item_nome} "
                         f"(disp: {int(saldos.loc[saldos['item_nome']==r.item_nome,'disponivel'].values[0]) if (saldos['item_nome']==r.item_nome).any() else int(r.quant_total)})"
            for r in dfi.itertuples(index=False)
        }
        escolhidos = st.multiselect("Escolha itens", options=list(labels.keys()), format_func=lambda k: labels.get(k,k), key="multi_prof")

        qts = {}
        for k in escolhidos:
            disp = int(saldos.loc[saldos['item_nome']==k,'disponivel'].values[0]) if (saldos['item_nome']==k).any() \
                   else int(dfi.loc[dfi['item_nome']==k,'quant_total'].values[0])
            qts[k] = st.number_input(f"Quantidade para {labels[k]}", min_value=1, max_value=max(1, disp if disp>0 else 1),
                                     value=min(1, disp) if disp>0 else 1, key=f"q_{k}")
        observ_p = st.text_input("Observações gerais", key="obs_prof")

        pode = prof.strip() and len(escolhidos)>0 and all((qts[k] or 0)>0 for k in escolhidos)
        if st.button("✅ Registrar Empréstimos do Professor", use_container_width=True, disabled=not pode, key="btn_emp_prof"):
            for k in escolhidos:
                cat_sel = dfi.loc[dfi["item_nome"]==k, "categoria"].values
                categoria_item = (cat_sel[0] if len(cat_sel) else "Livro")
                _base_registro(
                    tipo="Emprestimo",
                    item_nome=k,
                    categoria=categoria_item,
                    quantidade=int(qts[k]),
                    prev_dev=prev_p,
                    observ=observ_p,
                    beneficiario_tipo="professor", beneficiario_nome=prof.strip(),
                )
            st.success(f"Empréstimos registrados para {prof}.")

# ------ Aba: Devolução (por empréstimo) ------
with abas[2]:
    st.subheader("Devolução por empréstimo (parcial ou total)")

    pend = emprestimos_pendentes_df()
    if pend.empty:
        st.info("Não há empréstimos pendentes no momento.")
    else:
        # label amigável
        def _label(row):
            who = (row["beneficiario_nome"] if row["beneficiario_tipo"]=="professor"
                   else f"{row['aluno_nome']} {row['aluno_sobrenome']} — {row['aluno_serie']}".strip())
            prev = row["prev_devolucao"] or "s/prev."
            cat = row["categoria"] or "Livro"
            tit = row["titulo"] or row["item_nome"]
            atras = "⚠️ ATRASADO " if row["atrasado"] else ""
            return f"{atras}[{cat}] {tit} • {who} • pendente: {int(row['q_pendente'])} • prev: {prev}"

        options = {int(r.loan_id): _label(r) for _, r in pend.iterrows()}
        sel_loan = st.selectbox("Escolha o empréstimo", options=list(options.keys()), format_func=lambda k: options.get(k,str(k)), key="sel_dev_loan")
        row_sel = pend[pend["loan_id"]==int(sel_loan)].iloc[0]
        max_dev = int(row_sel["q_pendente"])
        qtd_dev = st.number_input("Quantidade a devolver", min_value=1, max_value=max_dev, value=max_dev, key="qtd_dev")
        observ_d = st.text_input("Observações", key="obs_dev")

        if st.button("↩️ Registrar Devolução", use_container_width=True, key="btn_dev"):
            # Registrar devolução vinculada ao loan_id selecionado
            _base_registro(
                tipo="Devolucao",
                item_nome=row_sel["item_nome"],
                categoria=row_sel["categoria"],
                quantidade=int(qtd_dev),
                prev_dev=None,
                observ=observ_d,
                aluno_nome=row_sel.get("aluno_nome","") or "",
                aluno_sobrenome=row_sel.get("aluno_sobrenome","") or "",
                aluno_serie=row_sel.get("aluno_serie","") or "",
                beneficiario_tipo=row_sel.get("beneficiario_tipo","") or "",
                beneficiario_nome=row_sel.get("beneficiario_nome","") or "",
                loan_id=int(sel_loan),
            )
            st.success("Devolução registrada.")
            st.rerun()

    st.markdown("---")
    st.caption("Saldos atuais por item")
    st.dataframe(status_itens(df_mov()), use_container_width=True, hide_index=True)

# ------ Aba: Catálogo ------
with abas[3]:
    st.subheader("Catálogo (importar, adicionar, editar/excluir, exportar)")

    colA, colB = st.columns([1,1])
    with colA:
        st.markdown("### Importar Planilha de Livros (.xlsx)")
        up = st.file_uploader("Selecione a planilha do catálogo", type=["xlsx"], key="upload_cat")
        if up is not None:
            try:
                excel = pd.read_excel(up)
                st.write("Pré-visualização (10 linhas):")
                st.dataframe(excel.head(10), use_container_width=True, hide_index=True)
                st.markdown("#### Mapeamento de colunas")
                cols = ["— ignorar —"] + list(excel.columns)
                map_titulo   = st.selectbox("Título (Nome do Livro)", cols, index=(cols.index("Nome do Livro") if "Nome do Livro" in cols else 0), key="map_titulo")
                map_autor    = st.selectbox("Autor", cols, index=(cols.index("autor") if "autor" in cols else 0), key="map_autor")
                map_editora  = st.selectbox("Editora", cols, index=(cols.index("Editora") if "Editora" in cols else 0), key="map_editora")
                map_genero   = st.selectbox("Gênero", cols, index=(cols.index("genero") if "genero" in cols else 0), key="map_genero")
                map_isbn     = st.selectbox("ISBN", cols, index=(cols.index("isbn") if "isbn" in cols else 0), key="map_isbn")
                map_edicao   = st.selectbox("Edição (Número)", cols, index=(cols.index("Número") if "Número" in cols else 0), key="map_edicao")
                map_quant    = st.selectbox("Unidades", cols, index=(cols.index("unidades") if "unidades" in cols else 0), key="map_quant")

                if st.button("📥 Importar catálogo (Livros)", key="btn_import_cat"):
                    n_ok = 0
                    for _, r in excel.iterrows():
                        def pick(c):
                            return (str(r[c]).strip() if (c and c in excel.columns and pd.notna(r[c])) else "")
                        campos = {
                            "titulo": pick(map_titulo) if map_titulo!="— ignorar —" else "",
                            "autor": pick(map_autor) if map_autor!="— ignorar —" else "",
                            "editora": pick(map_editora) if map_editora!="— ignorar —" else "",
                            "genero": pick(map_genero) if map_genero!="— ignorar —" else "",
                            "isbn": pick(map_isbn) if map_isbn!="— ignorar —" else "",
                            "edicao": pick(map_edicao) if map_edicao!="— ignorar —" else "",
                        }
                        qt = pick(map_quant) if map_quant!="— ignorar —" else ""
                        try:
                            qt_i = int(float(qt)) if str(qt).strip()!="" else 1
                        except Exception:
                            qt_i = 1
                        campos["quant_total"] = max(1, qt_i)
                        campos["item_nome"] = campos["titulo"] or campos["isbn"] or campos["autor"]
                        campos["categoria"] = "Livro"
                        upsert_item_catalogo(**campos)
                        n_ok += 1
                    st.success(f"Importação concluída: {n_ok} livro(s).")
            except Exception as e:
                st.error(f"Falha ao ler Excel: {e}")

    with colB:
        st.markdown("### Adicionar Manualmente")
        with st.form("form_add_item"):
            tipo_item = st.radio("Tipo de item", ["Livro", "Jogo"], horizontal=True, key="tipo_item_add")

            if tipo_item == "Livro":
                c1,c2 = st.columns([2,1])
                with c1:
                    titulo_in = st.text_input("Título *", key="add_titulo")
                    autor_in = st.text_input("Autor", key="add_autor")
                    editora_in = st.text_input("Editora", key="add_editora")
                    genero_in = st.text_input("Gênero", key="add_genero")
                with c2:
                    isbn_in = st.text_input("ISBN", key="add_isbn")
                    edicao_in = st.text_input("Edição (Número)", key="add_edicao")
                    quant_in = st.number_input("Unidades", min_value=1, value=1, key="add_qtd")
            else:
                nome_jogo = st.text_input("Nome do Jogo *", key="add_jogo_nome")
                quant_in = st.number_input("Unidades", min_value=1, value=1, key="add_jogo_qtd")

            enviado = st.form_submit_button("Adicionar/Atualizar")
            if enviado:
                if tipo_item == "Livro" and titulo_in.strip():
                    upsert_item_catalogo(
                        titulo=titulo_in.strip(), autor=autor_in.strip(), editora=editora_in.strip(), genero=genero_in.strip(),
                        isbn=isbn_in.strip(), edicao=edicao_in.strip(), quant_total=int(quant_in),
                        item_nome=titulo_in.strip(), categoria="Livro"
                    )
                    st.success("Livro adicionado/atualizado no catálogo.")
                elif tipo_item == "Jogo" and nome_jogo.strip():
                    upsert_item_catalogo(
                        titulo="", autor="", editora="", genero="", isbn="", edicao="",
                        quant_total=int(quant_in), item_nome=nome_jogo.strip(), categoria="Jogo"
                    )
                    st.success("Jogo adicionado/atualizado no catálogo.")
                else:
                    st.warning("Preencha os campos obrigatórios.")

    st.markdown("---")
    st.markdown("### Editar / Excluir itens existentes")

    # Auxiliares locais
    def df_itens_full():
        ensure_migrations()
        with get_conn() as con:
            return pd.read_sql_query(
                "SELECT id,item_nome,categoria,titulo,autor,editora,genero,isbn,edicao,quant_total FROM itens",
                con,
            )

    def update_item(item_id:int, **fields):
        if not fields:
            return
        ensure_migrations()
        cols = [k for k in ["item_nome","categoria","titulo","autor","editora","genero","isbn","edicao","quant_total"] if k in fields]
        if not cols:
            return
        set_clause = ",".join([f"{k}=?" for k in cols])
        vals = [fields[k] for k in cols] + [item_id]
        with get_conn() as con:
            con.execute(f"UPDATE itens SET {set_clause} WHERE id=?", vals)
            con.commit()
        df_itens.clear()

    def delete_item(item_id:int):
        ensure_migrations()
        with get_conn() as con:
            con.execute("DELETE FROM itens WHERE id=?", (item_id,))
            con.commit()
        df_itens.clear()

    dff = df_itens_full()
    if dff.empty:
        st.info("Catálogo vazio.")
    else:
        labels = {
            int(r.id): f"[{r.categoria or 'Livro'}] {r.titulo or r.item_nome} — ISBN: {r.isbn or 's/ISBN'} (Unid: {int(r.quant_total)})"
            for r in dff.itertuples(index=False)
        }
        sel_id = st.selectbox("Escolha um item do catálogo", options=list(labels.keys()),
                              format_func=lambda i: labels.get(int(i), str(i)), key="sel_edit_item")
        item_row = dff[dff["id"]==int(sel_id)].iloc[0]

        # estoque emprestado para bloqueio de exclusão
        try:
            emp_q = int(saldos.loc[saldos["item_nome"]==item_row["item_nome"], "emprestado"].values[0])
        except Exception:
            emp_q = 0

        with st.form("form_edit_item"):
            c1,c2 = st.columns([2,1])
            with c1:
                titulo_e = st.text_input("Título *", value=item_row["titulo"] or "", key="edit_titulo")
                autor_e = st.text_input("Autor", value=item_row["autor"] or "", key="edit_autor")
                editora_e = st.text_input("Editora", value=item_row["editora"] or "", key="edit_editora")
                genero_e = st.text_input("Gênero", value=item_row["genero"] or "", key="edit_genero")
            with c2:
                isbn_e = st.text_input("ISBN", value=item_row["isbn"] or "", key="edit_isbn")
                edicao_e = st.text_input("Edição (Número)", value=item_row["edicao"] or "", key="edit_edicao")
                quant_e = st.number_input("Unidades", min_value=1,
                                          value=int(item_row["quant_total"]) if pd.notna(item_row["quant_total"]) else 1,
                                          key="edit_qtd")
                categoria_e = st.selectbox("Categoria", ["Livro","Jogo"],
                                           index=(0 if (item_row["categoria"] or "Livro")=="Livro" else 1),
                                           key="edit_categoria")
            colbtn1, colbtn2 = st.columns([1,1])
            salvar = colbtn1.form_submit_button("💾 Salvar alterações")
            excluir = colbtn2.form_submit_button("🗑️ Excluir item", disabled=(emp_q>0))

        if salvar:
            item_nome_new = (titulo_e.strip() or isbn_e.strip() or autor_e.strip())
            if categoria_e == "Jogo" and not item_nome_new:
                item_nome_new = item_row["item_nome"]  # fallback
            update_item(
                int(sel_id),
                titulo=titulo_e.strip(), autor=autor_e.strip(), editora=editora_e.strip(),
                genero=genero_e.strip(), isbn=isbn_e.strip(), edicao=edicao_e.strip(),
                quant_total=int(quant_e), item_nome=item_nome_new, categoria=categoria_e,
            )
            st.success("Item atualizado.")
        if excluir:
            if emp_q>0:
                st.warning("Não é possível excluir: há unidades emprestadas.")
            else:
                delete_item(int(sel_id))
                st.success("Item excluído do catálogo.")
                st.rerun()

    st.markdown("---")
    st.caption("Catálogo atual")
    st.dataframe(df_itens(), use_container_width=True, hide_index=True)

    # Exportar catálogo
    buf = BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            dcurr = df_itens()
            if dcurr.empty:
                pd.DataFrame([{ "Info": "Catálogo vazio" }]).to_excel(w, sheet_name='catalogo', index=False)
            else:
                dcurr.to_excel(w, sheet_name='catalogo', index=False)
    except Exception:
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            dcurr = df_itens()
            if dcurr.empty:
                pd.DataFrame([{ "Info": "Catálogo vazio" }]).to_excel(w, sheet_name='catalogo', index=False)
            else:
                dcurr.to_excel(w, sheet_name='catalogo', index=False)
    buf.seek(0)
    st.download_button("⬇️ Exportar catálogo (.xlsx)", data=buf.getvalue(),
                       file_name="catalogo_sala_leitura.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_cat")

# ------ Aba: Consulta / Exportar ------
with abas[4]:
    st.subheader("Histórico, Busca e Exportações")
    dfm_now = df_mov()
    if dfm_now.empty:
        st.info("Sem movimentações ainda.")
    else:
        colf = st.columns(4)
        with colf[0]:
            filtro_nome = st.text_input("Nome/Sobrenome contém", key="f_nome")
        with colf[1]:
            filtro_item = st.text_input("Item contém", key="f_item")
        with colf[2]:
            filtro_tipo = st.selectbox("Tipo", ["Todos","Emprestimo","Devolucao"], index=0, key="f_tipo")
        with colf[3]:
            somente_prof = st.checkbox("Somente Professor", key="f_prof")

        view = dfm_now.copy()
        if filtro_nome:
            mask = view['aluno_nome'].fillna("").str.contains(filtro_nome, case=False) | \
                   view['aluno_sobrenome'].fillna("").str.contains(filtro_nome, case=False) | \
                   view['beneficiario_nome'].fillna("").str.contains(filtro_nome, case=False)
            view = view[mask]
        if filtro_item:
            view = view[view['item_nome'].fillna("").str.contains(filtro_item, case=False)]
        if filtro_tipo != "Todos":
            view = view[view['tipo']==filtro_tipo]
        if somente_prof:
            view = view[view['beneficiario_tipo']=="professor"]

        st.dataframe(view.sort_values("timestamp", ascending=False), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("📤 Exportar (período)")
        hoje = date.today()
        c1,c2,c3 = st.columns([1,1,1])
        with c1:
            data_ini = st.date_input("Início", value=hoje - timedelta(days=7), key="exp_ini")
        with c2:
            data_fim = st.date_input("Fim", value=hoje, key="exp_fim")
        with c3:
            incluir_status = st.checkbox("Incluir aba 'status_atual'", value=True, key="exp_status")

        if st.button("Gerar planilha de movimentações (.xlsx)", key="btn_exp_mov"):
            per = view.copy()
            per['ts'] = pd.to_datetime(per['timestamp'], errors='coerce')
            ini = datetime.combine(data_ini, datetime.min.time())
            fim = datetime.combine(data_fim, datetime.max.time())
            per = per[(per['ts'] >= ini) & (per['ts'] <= fim)].drop(columns=['ts'])

            cols_novas = [
                ("Data","data"),("Hora","hora"),("Tipo","tipo"),("Item","item_nome"),("Categoria","categoria"),
                ("Quantidade","quantidade"),("BeneficiárioTipo","beneficiario_tipo"),("BeneficiárioNome","beneficiario_nome"),
                ("Nome","aluno_nome"),("Sobrenome","aluno_sobrenome"),("Série","aluno_serie"),
                ("Prev. Devolução","prev_devolucao"),("Responsável","responsavel"),("Obs.","observacoes"),
            ]
            per_export = per[[c for _,c in cols_novas]].rename(columns=dict(cols_novas)) if not per.empty else pd.DataFrame(columns=[k for k,_ in cols_novas])
            buffer = BytesIO()
            try:
                with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                    order_cols = [c for c in ['Data','Hora'] if c in per_export.columns]
                    per_sorted = per_export.sort_values(order_cols, ascending=True) if order_cols else per_export
                    if per_sorted.empty:
                        pd.DataFrame([{ "Info": f"Sem registros entre {data_ini:%d/%m/%Y} e {data_fim:%d/%m/%Y}" }]).to_excel(writer, sheet_name='emprestimos', index=False)
                    else:
                        per_sorted.to_excel(writer, sheet_name='emprestimos', index=False)
                    if incluir_status:
                        sa = status_itens(df_mov())
                        sa.to_excel(writer, sheet_name='status_atual', index=False)
            except Exception:
                with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    order_cols = [c for c in ['Data','Hora'] if c in per_export.columns]
                    per_sorted = per_export.sort_values(order_cols, ascending=True) if order_cols else per_export
                    if per_sorted.empty:
                        pd.DataFrame([{ "Info": f"Sem registros entre {data_ini:%d/%m/%Y} e {data_fim:%d/%m/%Y}" }]).to_excel(writer, sheet_name='emprestimos', index=False)
                    else:
                        per_sorted.to_excel(writer, sheet_name='emprestimos', index=False)
                    if incluir_status:
                        sa = status_itens(df_mov())
                        sa.to_excel(writer, sheet_name='status_atual', index=False)
            buffer.seek(0)
            nome = f"movimentacoes_{data_ini:%Y%m%d}_{data_fim:%Y%m%d}.xlsx"
            st.download_button("⬇️ Baixar movimentações", data=buffer.getvalue(), file_name=nome,
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_mov")

        st.markdown("### 📥 Exportar Empréstimos Pendentes")
        pend = emprestimos_pendentes_df()
        if pend.empty:
            st.caption("Não há pendências no momento.")
        else:
            # planilha de pendentes
            bufp = BytesIO()
            with pd.ExcelWriter(bufp, engine="xlsxwriter") as w:
                export_cols = [
                    ("LoanID","loan_id"),
                    ("Data","data"),("Hora","hora"),
                    ("Categoria","categoria"),("Título/Item","titulo"),
                    ("Item (chave)","item_nome"),
                    ("Qtd Emp.","q_emprestado"),("Qtd Dev.","q_devolvido"),("Qtd Pendente","q_pendente"),
                    ("Prev. Devolução","prev_devolucao"),
                    ("Tipo Beneficiário","beneficiario_tipo"),
                    ("Nome Beneficiário","beneficiario_nome"),
                    ("Aluno Nome","aluno_nome"),("Aluno Sobrenome","aluno_sobrenome"),("Série","aluno_serie"),
                    ("Atrasado","atrasado"),
                ]
                dfexp = pend[[c for _,c in export_cols]].rename(columns=dict(export_cols))
                dfexp.to_excel(w, sheet_name="pendentes", index=False)
            bufp.seek(0)
            st.download_button("⬇️ Baixar pendentes (.xlsx)", data=bufp.getvalue(),
                               file_name="emprestimos_pendentes.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_pendentes")
