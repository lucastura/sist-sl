# ==============================
# app.py (v2 – alunos e catálogo automáticos)
# ==============================
# Sala de Leitura – Empréstimos de Livros e Jogos
# UI: Streamlit | Banco: SQLite (sala_leitura.db)
# Mudanças:
# - Removeu campo "ID do item" (usa apenas nome do item)
# - Aluno separado em: Nome, Sobrenome, Série
# - Cadastro automático de alunos ao emprestar (com autocomplete)
# - Catálogo automático de itens ao emprestar (mantém aba Itens para cadastro manual)
# - Exportação Excel com layout mais simples e legível

from __future__ import annotations
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, date
import sqlite3
from io import BytesIO

DB_PATH = Path("sala_leitura.db")
ABA_ITENS_TEXTO = "🗂️ Itens (opcional)"

COLS_MOV = [
    "timestamp",      # ISO
    "data",           # dd/mm/yyyy
    "hora",           # HH:MM:SS
    "tipo",           # Emprestimo|Devolucao
    "item_nome",
    "categoria",      # Livro|Jogo|Outro
    "aluno_nome",
    "aluno_sobrenome",
    "aluno_serie",
    "responsavel",    # quem operou
    "prev_devolucao", # dd/mm/yyyy
    "observacoes",
]

COLS_ITENS = ["item_nome", "categoria"]
COLS_ALUNOS = ["nome", "sobrenome", "serie"]

# ---------------- SQLite helpers ----------------

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


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
                observacoes TEXT
            );
            """
        )
        # itens (sem item_id)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS itens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_nome TEXT UNIQUE,
                categoria TEXT
            );
            """
        )
        # alunos
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


@st.cache_data(ttl=5)
def df_mov() -> pd.DataFrame:
    init_db()
    with get_conn() as con:
        df = pd.read_sql_query(
            "SELECT timestamp,data,hora,tipo,item_nome,categoria,aluno_nome,aluno_sobrenome,aluno_serie,responsavel,prev_devolucao,observacoes FROM movimentacoes",
            con,
        )
    if df.empty:
        return pd.DataFrame(columns=COLS_MOV)
    for c in COLS_MOV:
        if c not in df.columns:
            df[c] = ""
    return df[COLS_MOV].copy()


@st.cache_data(ttl=5)
def df_itens() -> pd.DataFrame:
    init_db()
    with get_conn() as con:
        df = pd.read_sql_query("SELECT item_nome,categoria FROM itens", con)
    if df.empty:
        return pd.DataFrame(columns=COLS_ITENS)
    return df[COLS_ITENS].fillna("")


@st.cache_data(ttl=5)
def df_alunos() -> pd.DataFrame:
    init_db()
    with get_conn() as con:
        df = pd.read_sql_query("SELECT nome,sobrenome,serie FROM alunos", con)
    if df.empty:
        return pd.DataFrame(columns=COLS_ALUNOS)
    return df[COLS_ALUNOS].fillna("")


def inserir_movimento(reg: dict):
    init_db()
    with get_conn() as con:
        con.execute(
            """
            INSERT INTO movimentacoes (
                timestamp,data,hora,tipo,item_nome,categoria,aluno_nome,aluno_sobrenome,aluno_serie,responsavel,prev_devolucao,observacoes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [reg.get(c, "") for c in COLS_MOV],
        )
        con.commit()
    df_mov.clear()


def upsert_item(item_nome: str, categoria: str):
    if not item_nome.strip():
        return
    init_db()
    with get_conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO itens (item_nome,categoria) VALUES (?,?)",
            (item_nome.strip(), categoria.strip() or "Livro"),
        )
        # Se já existia, opcionalmente atualizar categoria se vier vazia → manter
        con.commit()
    df_itens.clear()


def upsert_aluno(nome: str, sobrenome: str, serie: str):
    if not (nome.strip() or sobrenome.strip()):
        return
    init_db()
    with get_conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO alunos (nome,sobrenome,serie) VALUES (?,?,?)",
            (nome.strip(), sobrenome.strip(), serie.strip()),
        )
        con.commit()
    df_alunos.clear()


# ---------------- Regras de disponibilidade ----------------

def status_itens(dfm: pd.DataFrame) -> pd.DataFrame:
    if dfm.empty:
        return pd.DataFrame(columns=["item_nome", "categoria", "status", "aluno", "turma", "prev_devolucao"])    
    df = dfm.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    # último registro por item
    idx = df.sort_values('timestamp').groupby('item_nome').tail(1).index
    ult = df.loc[idx].copy()
    ult['status'] = ult['tipo'].apply(lambda t: 'Emprestado' if str(t).lower().startswith('emprest') else 'Disponível')
    ult['aluno'] = (ult['aluno_nome'].fillna("") + ' ' + ult['aluno_sobrenome'].fillna("")).str.strip()
    ult['turma'] = ult['aluno_serie'].fillna("")
    out = ult[["item_nome", "categoria", "status", "aluno", "turma", "prev_devolucao"]].reset_index(drop=True)
    return out


# ---------------- UI ----------------

st.set_page_config(page_title="Sala de Leitura - Empréstimos", page_icon="📚", layout="wide")
st.title("📚 Sala de Leitura – Empréstimos de Livros e Jogos")

with st.sidebar:
    st.header("Configurações rápidas")
    resp = st.text_input("Responsável de hoje", value=st.session_state.get("responsavel", ""))
    st.session_state["responsavel"] = resp
    padrao_prazo = st.number_input("Dias de empréstimo (padrão)", min_value=1, max_value=30, value=3)
    st.markdown("---")
    st.caption("Banco de dados:")
    st.code(str(DB_PATH.resolve()))

abas = st.tabs(["➕ Empréstimo", "↩️ Devolução", "🔎 Consulta / Exportar", ABA_ITENS_TEXTO])

# ------ Dados atuais ------
dfm = df_mov()
dfi = df_itens()
dfa = df_alunos()
dfs = status_itens(dfm)

# Helpers de autocomplete de aluno
alunos_options = []
if not dfa.empty:
    alunos_options = [f"{r.nome} {r.sobrenome} — {r.serie}".strip() for r in dfa.itertuples(index=False)]

# ------ Função registrar ------

def registrar_mov(tipo: str, item_nome: str, categoria: str, nome: str, sobrenome: str, serie: str, prev_dev: datetime | None, responsavel: str, observ: str):
    agora = datetime.now()
    reg = {
        "timestamp": agora.isoformat(timespec='seconds'),
        "data": agora.strftime('%d/%m/%Y'),
        "hora": agora.strftime('%H:%M:%S'),
        "tipo": tipo,
        "item_nome": item_nome,
        "categoria": categoria,
        "aluno_nome": nome,
        "aluno_sobrenome": sobrenome,
        "aluno_serie": serie,
        "responsavel": responsavel,
        "prev_devolucao": prev_dev.strftime('%d/%m/%Y') if prev_dev else "",
        "observacoes": observ,
    }
    # cadastro automático de aluno e item
    upsert_aluno(nome, sobrenome, serie)
    upsert_item(item_nome, categoria)
    inserir_movimento(reg)
    st.success(f"{tipo} registrado: {item_nome} → {nome} {sobrenome} ({serie}).")


# ------ Aba Empréstimo ------
with abas[0]:
    st.subheader("Registrar Empréstimo")

    col1, col2 = st.columns([2, 1])
    with col1:
        usar_catalogo = st.toggle("Selecionar item a partir do catálogo (aba Itens)", value=not df_itens().empty)
        if usar_catalogo and not df_itens().empty:
            # NOVO: não filtra emprestados. Mostra status ao lado do nome.
            itens_all = df_itens().copy()
            status_map = {r.item_nome: r.status for r in status_itens(dfm).itertuples(index=False)}
            # opções pelo nome do item; formatador mostra categoria + status
            opcoes = itens_all['item_nome'].tolist()
            escolha = st.selectbox(
                "Item do catálogo (pode estar emprestado)",
                opcoes,
                index=None,
                placeholder="Digite para buscar...",
                format_func=lambda x: f"{x} ({itens_all.loc[itens_all['item_nome']==x, 'categoria'].values[0]}) — {status_map.get(x, 'Disponível')}",
            )
            # Campo opcional para item novo mesmo em modo catálogo
            novo_item = st.text_input("Ou digite um novo item (será adicionado automaticamente)")
            if novo_item.strip():
                item_nome = novo_item.strip()
                categoria = st.selectbox("Categoria do novo item", ["Livro", "Jogo", "Outro"], index=0, key="cat_novo_item")
            elif escolha:
                item_nome = escolha
                categoria = itens_all.loc[itens_all['item_nome']==escolha, 'categoria'].values[0]
            else:
                item_nome = ""; categoria = "Livro"
        else:
            item_nome = st.text_input("Nome do item *")
            categoria = st.selectbox("Categoria", ["Livro", "Jogo", "Outro"], index=0)

        st.markdown("### Aluno")
        aluno_sel = st.selectbox(
            "Aluno (autocomplete)",
            alunos_options if alunos_options else [""],
            index=None,
            placeholder="Buscar aluno já cadastrado...",
        )
        if aluno_sel:
            try:
                nome_pref, rest = aluno_sel.split(" ", 1)
                # dividir melhor: pega última palavra como sobrenome quando não há travessão
                if "—" in rest:
                    sobrenome_pref, serie_pref = [p.strip() for p in rest.split("—", 1)]
                else:
                    partes = rest.strip().split()
                    sobrenome_pref = partes[-1] if partes else ""
                    serie_pref = ""
            except Exception:
                nome_pref = aluno_sel; sobrenome_pref = ""; serie_pref = ""
        else:
            nome_pref = sobrenome_pref = serie_pref = ""

        c1, c2, c3 = st.columns(3)
        with c1:
            nome = st.text_input("Nome *", value=nome_pref)
        with c2:
            sobrenome = st.text_input("Sobrenome *", value=sobrenome_pref)
        with c3:
            serie = st.text_input("Série *", value=serie_pref, placeholder="Ex.: 1º B")

        dias = st.number_input("Prazo (dias)", min_value=1, max_value=30, value=int(st.session_state.get("prazo_padrao", 3)))
        prev_dev = datetime.now() + timedelta(days=int(dias))
        st.write("**Prev. devolução:** ", prev_dev.strftime('%d/%m/%Y'))
        observ = st.text_input("Observações")

    with col2:
        st.markdown("### ")
        pode = (item_nome.strip() != "" and nome.strip() != "" and sobrenome.strip() != "" and serie.strip() != "")
        if st.button("✅ Registrar Empréstimo", use_container_width=True, disabled=not pode):
            registrar_mov(
                tipo="Emprestimo",
                item_nome=item_nome,
                categoria=categoria,
                nome=nome,
                sobrenome=sobrenome,
                serie=serie,
                prev_dev=prev_dev,
                responsavel=st.session_state.get("responsavel", ""),
                observ=observ,
            )

    st.markdown("---")
    st.caption("Itens atualmente emprestados")
    st.dataframe(dfs[dfs['status']=="Emprestado"], use_container_width=True, hide_index=True)

# ------ Aba Devolução ------
with abas[1]:
    st.subheader("Registrar Devolução")
    emprestados = dfs[dfs['status']=="Emprestado"].copy()
    if emprestados.empty:
        st.info("Não há itens emprestados no momento.")
    else:
        chave = st.selectbox(
            "Selecione o item a devolver",
            emprestados['item_nome'].tolist(),
            format_func=lambda k: (
                f"{k} – {emprestados.loc[emprestados['item_nome']==k, 'aluno'].values[0]}"
            ),
        )
        if st.button("↩️ Registrar Devolução", use_container_width=True):
            linha = emprestados[emprestados['item_nome']==chave].iloc[0]
            registrar_mov(
                tipo="Devolucao",
                item_nome=linha['item_nome'],
                categoria=linha['categoria'],
                nome=str(linha['aluno']).split()[0] if str(linha['aluno']) else "",
                sobrenome=" ".join(str(linha['aluno']).split()[1:]) if str(linha['aluno']) else "",
                serie=linha['turma'],
                prev_dev=None,
                responsavel=st.session_state.get("responsavel", ""),
                observ="",
            )

    st.markdown("---")
    st.caption("Resumo de disponibilidade")
    st.dataframe(dfs, use_container_width=True, hide_index=True)

# ------ Aba Consulta / Exportar ------
with abas[2]:
    st.subheader("Histórico, Busca e Exportação para Excel")
    if dfm.empty:
        st.info("Sem registros ainda.")
    else:
        colf = st.columns(3)
        with colf[0]:
            filtro_nome = st.text_input("Nome/Sobrenome contém")
        with colf[1]:
            filtro_item = st.text_input("Item contém")
        with colf[2]:
            filtro_tipo = st.selectbox("Tipo", ["Todos", "Emprestimo", "Devolucao"], index=0)

        view = dfm.copy()
        if filtro_nome:
            mask = view['aluno_nome'].fillna("").str.contains(filtro_nome, case=False) | \
                   view['aluno_sobrenome'].fillna("").str.contains(filtro_nome, case=False)
            view = view[mask]
        if filtro_item:
            view = view[view['item_nome'].fillna("").str.contains(filtro_item, case=False)]
        if filtro_tipo != "Todos":
            view = view[view['tipo']==filtro_tipo]
        st.dataframe(view.sort_values("timestamp", ascending=False), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("📤 Exportar para Excel por período (layout simples)")
        hoje = date.today()
        d1, d2, d3 = st.columns([1,1,1])
        with d1:
            data_ini = st.date_input("Início", value=hoje - timedelta(days=7))
        with d2:
            data_fim = st.date_input("Fim", value=hoje)
        with d3:
            incluir_status = st.checkbox("Incluir aba 'status_atual'", value=True)

        if st.button("Gerar planilha (.xlsx)"):
            # filtra por período
            per = dfm.copy()
            per['ts'] = pd.to_datetime(per['timestamp'], errors='coerce')
            ini = datetime.combine(data_ini, datetime.min.time())
            fim = datetime.combine(data_fim, datetime.max.time())
            per = per[(per['ts'] >= ini) & (per['ts'] <= fim)].drop(columns=['ts'])

            # Reorganiza colunas para ficar fácil de ler
            cols_novas = [
                ("Data", "data"),
                ("Hora", "hora"),
                ("Tipo", "tipo"),
                ("Item", "item_nome"),
                ("Categoria", "categoria"),
                ("Nome", "aluno_nome"),
                ("Sobrenome", "aluno_sobrenome"),
                ("Série", "aluno_serie"),
                ("Prev. Devolução", "prev_devolucao"),
                ("Responsável", "responsavel"),
                ("Obs.", "observacoes"),
            ]
            per_export = per[[c for _, c in cols_novas]].rename(columns=dict(cols_novas)) if not per.empty else pd.DataFrame(columns=[k for k,_ in cols_novas])

            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                # Sempre criar pelo menos uma planilha visível
                if per_export.empty:
                    # cria uma aba com cabeçalhos e uma linha informativa
                    vazio = per_export.copy()
                    vazio.loc[0] = [""] * len(vazio.columns)
                    vazio.loc[0, 'Obs.'] = f"Sem registros entre {data_ini.strftime('%d/%m/%Y')} e {data_fim.strftime('%d/%m/%Y')}"
                    vazio.to_excel(writer, sheet_name='emprestimos', index=False)
                else:
                    per_export.sort_values(['Data','Hora'], ascending=True).to_excel(writer, sheet_name='emprestimos', index=False)

                # opcional: status atual simples (só cria se solicitado e houver dados)
                if incluir_status and not dfs.empty:
                    sa = dfs.copy()
                    sa = sa.rename(columns={
                        'item_nome':'Item', 'categoria':'Categoria', 'status':'Status', 'aluno':'Aluno', 'turma':'Série', 'prev_devolucao':'Prev. Devolução'
                    })
                    sa.to_excel(writer, sheet_name='status_atual', index=False)

                # formatação leve (só se a planilha existir)
                if 'emprestimos' in writer.sheets:
                    ws = writer.sheets['emprestimos']
                    for col in ws.columns:
                        maxlen = 10
                        for cell in col:
                            try:
                                maxlen = max(maxlen, len(str(cell.value)))
                            except Exception:
                                pass
                        ws.column_dimensions[col[0].column_letter].width = min(maxlen+2, 40)
                if 'status_atual' in writer.sheets:
                    ws2 = writer.sheets['status_atual']
                    for col in ws2.columns:
                        maxlen = 10
                        for cell in col:
                            try:
                                maxlen = max(maxlen, len(str(cell.value)))
                            except Exception:
                                pass
                        ws2.column_dimensions[col[0].column_letter].width = min(maxlen+2, 40)

            buffer.seek(0)
            nome = f"sala_leitura_{data_ini.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.xlsx"
            st.download_button(
                label=f"⬇️ Baixar {nome}",
                data=buffer.getvalue(),
                file_name=nome,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

# ------ Aba Itens (opcional) ------
with abas[3]:
    st.subheader("Catálogo de Itens (opcional)")
    st.caption("Mesmo sem cadastrar aqui, itens são adicionados automaticamente quando emprestados.")

    if dfi.empty:
        st.info("Nenhum item cadastrado manualmente. Adicione abaixo se quiser.")

    with st.form("novo_item"):
        c1, c2 = st.columns([2,1])
        with c1:
            item_nome_in = st.text_input("Nome do item *")
        with c2:
            categoria_in = st.selectbox("Categoria", ["Livro", "Jogo", "Outro"], index=0)
        enviado = st.form_submit_button("Adicionar Item")
        if enviado and item_nome_in.strip() != "":
            upsert_item(item_nome_in, categoria_in)
            st.success("Item adicionado ao catálogo.")

    if not df_itens().empty:
        st.markdown("### Itens cadastrados")
        st.dataframe(df_itens(), use_container_width=True, hide_index=True)
        st.caption("Para remover/editar rapidamente: use um editor SQLite (opcional) ou empreste/devolva normalmente.")
