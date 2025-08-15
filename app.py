# ==============================
# app.py (v3.0 – Catálogo + Empréstimo Professor + Devolução parcial)
# ==============================
# Sala de Leitura – Empréstimos de Livros e Jogos
# UI: Streamlit | Banco: SQLite (sala_leitura.db)
#
# Novidades v3.0
# - Aba **Catálogo** com importação de planilha (mapeamento de colunas)
# - Campos do catálogo: titulo, autor, editora, genero, isbn, edicao (Numero), quant_total (unidades)
# - **Empréstimo Professor**: multitítulo e multiquantidade
# - **Devolução parcial** por quantidade
# - Exportar Catálogo completo e Todos os Empréstimos
# - Mantém fluxo de aluno, autocomplete, exportação por período
#
# Requisitos:
#   pip install -r requirements.txt
#
# Rodar:
#   streamlit run app.py

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
    "timestamp","data","hora","tipo",           # Emprestimo, Devolucao, Renovacao (se desejar futuramente)
    "item_nome","categoria",                      # categoria mantida para compatibilidade
    "aluno_nome","aluno_sobrenome","aluno_serie",
    "responsavel","prev_devolucao","observacoes",
    # novos:
    "quantidade",                                   # int (padrão 1)
    "beneficiario_tipo",                            # 'aluno' | 'professor' | ''
    "beneficiario_nome",                            # nome do professor (ou vazio)
]

COLS_ITENS = [
    "item_nome","categoria",                      # compatibilidade
    "titulo","autor","editora","genero","isbn","edicao",
    "quant_total",                                  # unidades no acervo
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
                beneficiario_nome TEXT DEFAULT ''
            );
            """
        )
        # itens (catálogo)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS itens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_nome TEXT UNIQUE,      -- rótulo curto/antigo (opcional)
                categoria TEXT,             -- compatibilidade (p.ex. Livro/Jogo)
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

        # Migrações leves de segurança
        for col, ddl in [
            ("quantidade", "INTEGER DEFAULT 1"),
            ("beneficiario_tipo", "TEXT DEFAULT ''"),
            ("beneficiario_nome", "TEXT DEFAULT ''"),
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

@st.cache_data(ttl=5)
def df_mov() -> pd.DataFrame:
    init_db()
    with get_conn() as con:
        df = pd.read_sql_query(
            "SELECT timestamp,data,hora,tipo,item_nome,categoria,aluno_nome,aluno_sobrenome,aluno_serie,"
            "responsavel,prev_devolucao,observacoes,quantidade,beneficiario_tipo,beneficiario_nome FROM movimentacoes",
            con,
        )
    if df.empty:
        return pd.DataFrame(columns=COLS_MOV)
    for c in COLS_MOV:
        if c not in df.columns:
            df[c] = "" if c not in ("quantidade",) else 0
    if "quantidade" in df.columns:
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0).astype(int)
    return df[COLS_MOV].copy()

@st.cache_data(ttl=5)
def df_itens() -> pd.DataFrame:
    init_db()
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
    init_db()
    with get_conn() as con:
        df = pd.read_sql_query("SELECT nome,sobrenome,serie FROM alunos", con)
    if df.empty:
        return pd.DataFrame(columns=COLS_ALUNOS)
    return df[COLS_ALUNOS].fillna("")

# CRUD helpers

def inserir_movimento(reg: dict):
    init_db()
    reg2 = {**{c: (0 if c=="quantidade" else "") for c in COLS_MOV}, **reg}
    if not reg2.get("quantidade"):
        reg2["quantidade"] = 1
    with get_conn() as con:
        con.execute(
            """
            INSERT INTO movimentacoes (
                timestamp,data,hora,tipo,item_nome,categoria,aluno_nome,aluno_sobrenome,aluno_serie,
                responsavel,prev_devolucao,observacoes,quantidade,beneficiario_tipo,beneficiario_nome
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [reg2.get(c, "") for c in COLS_MOV],
        )
        con.commit()
    df_mov.clear()

def upsert_item_catalogo(**campos):
    # chave preferencial: isbn; fallback: titulo+autor+edicao
    init_db()
    with get_conn() as con:
        isbn = (campos.get("isbn") or "").strip()
        titulo = (campos.get("titulo") or "").strip()
        autor = (campos.get("autor") or "").strip()
        edicao = (campos.get("edicao") or "").strip()
        # tenta localizar
        if isbn:
            cur = con.execute("SELECT id FROM itens WHERE isbn=?", (isbn,))
            row = cur.fetchone()
        else:
            cur = con.execute("SELECT id FROM itens WHERE (titulo=? AND autor=? AND IFNULL(edicao,'')=?)",
                              (titulo, autor, edicao))
            row = cur.fetchone()
        campos.setdefault("quant_total", 1)
        campos.setdefault("categoria", "Livro")
        campos.setdefault("item_nome", titulo or isbn or autor)
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

# ---------------- Disponibilidade / saldos ----------------

def saldo_por_item(dfm: pd.DataFrame) -> pd.DataFrame:
    """Retorna DataFrame com cols: item_nome, titulo, quant_total, emprestado, disponivel"""
    dfi = df_itens()
    if dfi.empty:
        base = pd.DataFrame(columns=["item_nome","titulo","quant_total"]).copy()
    else:
        base = dfi[["item_nome","titulo","quant_total"]].copy()
    if dfm.empty:
        base["emprestado"] = 0
        base["disponivel"] = base["quant_total"]
        return base
    mov = dfm.copy()
    mov["quantidade"] = pd.to_numeric(mov["quantidade"], errors="coerce").fillna(0).astype(int)
    mov["sinal"] = mov["tipo"].str.lower().map(lambda t: 1 if t.startswith("emprest") else (-1 if t.startswith("devolu") else 0))
    mov["delta"] = mov["quantidade"] * mov["sinal"]
    agg = mov.groupby("item_nome")["delta"].sum().rename("emprestado").reset_index()
    out = base.merge(agg, on="item_nome", how="left").fillna({"emprestado":0})
    out["emprestado"] = out["emprestado"].astype(int).clip(lower=0)
    out["disponivel"] = (out["quant_total"] - out["emprestado"]).clip(lower=0)
    return out

# status para visualização (último evento + saldos)

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
    # junta saldos e titulo
    if not dfi.empty:
        ult = ult.merge(dfi[["item_nome","titulo"]], on="item_nome", how="right").fillna("")
    else:
        ult["titulo"] = ult["item_nome"]
    ult = ult.merge(sal[["item_nome","quant_total","emprestado","disponivel"]], on="item_nome", how="left").fillna({"quant_total":0,"emprestado":0,"disponivel":0})
    return ult

# ---------------- UI ----------------

st.set_page_config(page_title="Sala de Leitura - Sistema", page_icon="📚", layout="wide")
st.title("📚 Sala de Leitura – Sistema Oficial")

with st.sidebar:
    st.header("Configurações rápidas")
    resp = st.text_input("Responsável de hoje", value=st.session_state.get("responsavel", ""))
    st.session_state["responsavel"] = resp
    st.caption(f"Banco: {DB_PATH.resolve()}")

abas = st.tabs([
    "➕ Empréstimo Aluno",
    "👩‍🏫 Empréstimo Professor",
    "↩️ Devolução (parcial)",
    "📚 Catálogo",
    "🔎 Consulta / Exportar",
])

# ------ Dados atuais ------
dfm = df_mov()
dfi = df_itens()
dfa = df_alunos()
stat = status_itens(dfm)
saldos = saldo_por_item(dfm)

# Autocomplete de aluno
alunos_options = []
if not dfa.empty:
    alunos_options = [f"{r.nome} {r.sobrenome} — {r.serie}".strip() for r in dfa.itertuples(index=False)]

# Helpers de registro

def _base_registro(tipo:str, item_nome:str, categoria:str, quantidade:int, prev_dev:datetime|None, observ:str,
                   aluno_nome:str="", aluno_sobrenome:str="", aluno_serie:str="",
                   beneficiario_tipo:str="aluno", beneficiario_nome:str=""):
    agora = datetime.now()
    inserir_movimento({
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
    })

# ------ Aba: Empréstimo Aluno ------
with abas[0]:
    st.subheader("Empréstimo para Aluno")

    col1, col2 = st.columns([2, 1])
    with col1:
        # escolha de item do catálogo
        if dfi.empty:
            st.warning("Catálogo vazio. Adicione na aba Catálogo.")
            opcoes = []
        else:
            opcoes = dfi.sort_values(["titulo"]).apply(lambda r: f"{r['titulo']} (disp: {int(saldos.loc[saldos['item_nome']==r['item_nome'],'disponivel'].values[0]) if (saldos['item_nome']==r['item_nome']).any() else r['quant_total']})", axis=1).tolist()
        # map label -> item_nome
        labels = {}
        if not dfi.empty:
            for r in dfi.itertuples(index=False):
                disp = int(saldos.loc[saldos['item_nome']==r.item_nome,'disponivel'].values[0]) if (saldos['item_nome']==r.item_nome).any() else int(r.quant_total)
                labels[r.item_nome] = f"{r.titulo or r.item_nome} (disp: {disp})"
        escolha = st.selectbox("Livro", options=list(labels.keys()) if labels else [], format_func=lambda k: labels.get(k, k), index=None)
        quantidade = st.number_input("Quantidade", min_value=1, value=1)
        dias = st.number_input("Prazo (dias)", min_value=1, max_value=60, value=7)
        prev_dev = datetime.now() + timedelta(days=int(dias))
        st.caption(f"Prev. devolução: {prev_dev.strftime('%d/%m/%Y')}")

        st.markdown("### Aluno")
        aluno_sel = st.selectbox("Aluno (autocomplete)", alunos_options if alunos_options else [""], index=None, placeholder="Buscar aluno já cadastrado...")
        nome_pref = sobrenome_pref = serie_pref = ""
        if aluno_sel:
            try:
                nome_pref, rest = aluno_sel.split(" ", 1)
                if "—" in rest:
                    sobrenome_pref, serie_pref = [p.strip() for p in rest.split("—", 1)]
            except Exception:
                nome_pref = aluno_sel
        c1,c2,c3 = st.columns(3)
        with c1: nome = st.text_input("Nome *", value=nome_pref)
        with c2: sobrenome = st.text_input("Sobrenome *", value=sobrenome_pref)
        with c3: serie = st.text_input("Série *", value=serie_pref)
        observ = st.text_input("Observações")

    with col2:
        pode = bool(escolha) and nome.strip() and sobrenome.strip() and serie.strip() and quantidade>0
        if st.button("✅ Registrar Empréstimo", use_container_width=True, disabled=not pode):
            # salva aluno para autocomplete
            if nome.strip() or sobrenome.strip():
                with get_conn() as con:
                    con.execute("INSERT OR IGNORE INTO alunos (nome,sobrenome,serie) VALUES (?,?,?)", (nome.strip(), sobrenome.strip(), serie.strip()))
                    con.commit()
                df_alunos.clear()
            _base_registro(
                tipo="Emprestimo",
                item_nome=escolha,
                categoria="Livro",
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
        prof = st.text_input("Nome do Professor *")
        dias_p = st.number_input("Prazo (dias)", min_value=1, max_value=120, value=14)
        prev_p = datetime.now() + timedelta(days=int(dias_p))
        st.caption(f"Prev. devolução: {prev_p.strftime('%d/%m/%Y')}")

        st.markdown("### Seleção de livros")
        # multiseleção por item_nome
        labels = {r.item_nome: f"{r.titulo or r.item_nome} (disp: {int(saldos.loc[saldos['item_nome']==r.item_nome,'disponivel'].values[0]) if (saldos['item_nome']==r.item_nome).any() else int(r.quant_total)})" for r in dfi.itertuples(index=False)}
        escolhidos = st.multiselect("Escolha livros", options=list(labels.keys()), format_func=lambda k: labels.get(k,k))

        qts = {}
        for k in escolhidos:
            disp = int(saldos.loc[saldos['item_nome']==k,'disponivel'].values[0]) if (saldos['item_nome']==k).any() else int(dfi.loc[dfi['item_nome']==k,'quant_total'].values[0])
            qts[k] = st.number_input(f"Quantidade para {labels[k]}", min_value=1, max_value=max(1, disp if disp>0 else 1), value=min(1, disp) if disp>0 else 1, key=f"q_{k}")
        observ_p = st.text_input("Observações gerais")

        pode = prof.strip() and len(escolhidos)>0 and all((qts[k] or 0)>0 for k in escolhidos)
        if st.button("✅ Registrar Empréstimos do Professor", use_container_width=True, disabled=not pode):
            for k in escolhidos:
                _base_registro(
                    tipo="Emprestimo",
                    item_nome=k,
                    categoria="Livro",
                    quantidade=int(qts[k]),
                    prev_dev=prev_p,
                    observ=observ_p,
                    beneficiario_tipo="professor", beneficiario_nome=prof.strip(),
                )
            st.success(f"Empréstimos registrados para {prof}.")

# ------ Aba: Devolução (parcial) ------
with abas[2]:
    st.subheader("Devolução (parcial ou total)")
    if dfi.empty:
        st.info("Catálogo vazio.")
    else:
        # Mostra itens com emprestado>0
        sal_emprest = saldos[saldos["emprestado"]>0].copy()
        if sal_emprest.empty:
            st.info("Não há itens emprestados no momento.")
        else:
            labels = {r.item_nome: f"{r.item_nome} – {int(r.emprestado)} emprestado(s)" for r in sal_emprest.itertuples(index=False)}
            chosen = st.selectbox("Selecione o item", options=list(labels.keys()), format_func=lambda k: labels.get(k,k))
            emprestado_q = int(sal_emprest.loc[sal_emprest['item_nome']==chosen,'emprestado'].values[0])
            qtd_dev = st.number_input("Quantidade a devolver", min_value=1, max_value=emprestado_q, value=emprestado_q)
            observ_d = st.text_input("Observações")
            if st.button("↩️ Registrar Devolução", use_container_width=True):
                _base_registro(
                    tipo="Devolucao",
                    item_nome=chosen,
                    categoria="Livro",
                    quantidade=int(qtd_dev),
                    prev_dev=None,
                    observ=observ_d,
                    beneficiario_tipo="", beneficiario_nome="",
                )
                st.success("Devolução registrada.")

    st.markdown("---")
    st.caption("Saldos atuais")
    st.dataframe(status_itens(df_mov()), use_container_width=True, hide_index=True)

# ------ Aba: Catálogo ------
with abas[3]:
    st.subheader("Catálogo (importar, adicionar, exportar)")

    colA, colB = st.columns([1,1])
    with colA:
        st.markdown("### Importar Planilha (.xlsx)")
        up = st.file_uploader("Selecione a planilha do catálogo", type=["xlsx"])
        if up is not None:
            try:
                excel = pd.read_excel(up)
                st.write("Pré-visualização (10 linhas):")
                st.dataframe(excel.head(10), use_container_width=True, hide_index=True)
                st.markdown("#### Mapeamento de colunas")
                cols = ["— ignorar —"] + list(excel.columns)
                map_titulo   = st.selectbox("Título (Nome do Livro)", cols, index=(cols.index("Nome do Livro") if "Nome do Livro" in cols else 0))
                map_autor    = st.selectbox("Autor", cols, index=(cols.index("autor") if "autor" in cols else 0))
                map_editora  = st.selectbox("Editora", cols, index=(cols.index("Editora") if "Editora" in cols else 0))
                map_genero   = st.selectbox("Gênero", cols, index=(cols.index("genero") if "genero" in cols else 0))
                map_isbn     = st.selectbox("ISBN", cols, index=(cols.index("isbn") if "isbn" in cols else 0))
                map_edicao   = st.selectbox("Edição (Número)", cols, index=(cols.index("Número") if "Número" in cols else 0))
                map_quant    = st.selectbox("Unidades", cols, index=(cols.index("unidades") if "unidades" in cols else 0))

                if st.button("📥 Importar catálogo"):
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
                        upsert_item_catalogo(**campos)
                        n_ok += 1
                    st.success(f"Importação concluída: {n_ok} registro(s).")
            except Exception as e:
                st.error(f"Falha ao ler Excel: {e}")

    with colB:
        st.markdown("### Adicionar Manualmente")
        with st.form("form_add_item"):
            c1,c2 = st.columns([2,1])
            with c1:
                titulo_in = st.text_input("Título *")
                autor_in = st.text_input("Autor")
                editora_in = st.text_input("Editora")
                genero_in = st.text_input("Gênero")
            with c2:
                isbn_in = st.text_input("ISBN")
                edicao_in = st.text_input("Edição (Número)")
                quant_in = st.number_input("Unidades", min_value=1, value=1)
            enviado = st.form_submit_button("Adicionar/Atualizar")
            if enviado and titulo_in.strip():
                upsert_item_catalogo(
                    titulo=titulo_in.strip(), autor=autor_in.strip(), editora=editora_in.strip(), genero=genero_in.strip(),
                    isbn=isbn_in.strip(), edicao=edicao_in.strip(), quant_total=int(quant_in), item_nome=titulo_in.strip(), categoria="Livro"
                )
                st.success("Catálogo atualizado.")

    st.markdown("---")
    st.caption("Catálogo atual")
    st.dataframe(df_itens(), use_container_width=True, hide_index=True)

    # Exportar catálogo
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine=("xlsxwriter" if st.runtime.exists() else "openpyxl")) as w:
        if dfi.empty:
            pd.DataFrame([{ "Info": "Catálogo vazio" }]).to_excel(w, sheet_name='catalogo', index=False)
        else:
            dfi.to_excel(w, sheet_name='catalogo', index=False)
    buf.seek(0)
    st.download_button("⬇️ Exportar catálogo (.xlsx)", data=buf.getvalue(), file_name="catalogo_sala_leitura.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ------ Aba: Consulta / Exportar ------
with abas[4]:
    st.subheader("Histórico, Busca e Exportações")
    dfm_now = df_mov()
    if dfm_now.empty:
        st.info("Sem movimentações ainda.")
    else:
        colf = st.columns(4)
        with colf[0]:
            filtro_nome = st.text_input("Nome/Sobrenome contém")
        with colf[1]:
            filtro_item = st.text_input("Item contém")
        with colf[2]:
            filtro_tipo = st.selectbox("Tipo", ["Todos","Emprestimo","Devolucao"], index=0)
        with colf[3]:
            somente_prof = st.checkbox("Somente Professor")

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
            data_ini = st.date_input("Início", value=hoje - timedelta(days=7))
        with c2:
            data_fim = st.date_input("Fim", value=hoje)
        with c3:
            incluir_status = st.checkbox("Incluir aba 'status_atual'", value=True)

        if st.button("Gerar planilha de movimentações (.xlsx)"):
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
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                try:
                    order_cols = [c for c in ['Data','Hora'] if c in per_export.columns]
                    per_sorted = per_export.sort_values(order_cols, ascending=True) if order_cols else per_export
                except Exception:
                    per_sorted = per_export
                if per_sorted.empty:
                    pd.DataFrame([{ "Info": f"Sem registros entre {data_ini:%d/%m/%Y} e {data_fim:%d/%m/%Y}" }]).to_excel(writer, sheet_name='emprestimos', index=False)
                else:
                    per_sorted.to_excel(writer, sheet_name='emprestimos', index=False)
                if incluir_status:
                    sa = status_itens(df_mov())
                    sa.to_excel(writer, sheet_name='status_atual', index=False)
            buffer.seek(0)
            nome = f"movimentacoes_{data_ini:%Y%m%d}_{data_fim:%Y%m%d}.xlsx"
            st.download_button("⬇️ Baixar movimentações", data=buffer.getvalue(), file_name=nome, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Export catálogo completo (atalho extra)
        buf2 = BytesIO()
        with pd.ExcelWriter(buf2, engine="xlsxwriter") as w:
            dfi_now = df_itens()
            (dfi_now if not dfi_now.empty else pd.DataFrame([{ "Info": "Catálogo vazio" }])).to_excel(w, sheet_name='catalogo', index=False)
        buf2.seek(0)
        st.download_button("⬇️ Baixar catálogo completo", data=buf2.getvalue(), file_name="catalogo_completo.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
