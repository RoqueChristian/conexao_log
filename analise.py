import pandas as pd
import numpy as np
import streamlit as st
import locale
import streamlit as st
import streamlit_authenticator as stauth
import pandas as pd
import plotly.graph_objects as go
from loguru import logger
import os

# ==============================
# CONFIGURAÇÕES INICIAIS
# ==============================

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except locale.Error:
        locale.setlocale(locale.LC_ALL, '')

FILE_CONEXAO = 'dados_conexao_unificada.csv'
FILE_PEDIDOS = 'dados_pedidos.csv'
COLUNA_ESTADO = 'ESTADO' 

# ==============================
# FUNÇÕES DE SUPORTE
# ==============================

def limpar_cnpj(cnpj_series):

    cnpj_series = cnpj_series.astype(str).fillna('')
    cnpj_series = cnpj_series.str.replace(r'\.0$', '', regex=True)
    return cnpj_series.str.replace(r'\D', '', regex=True)


# Configuração de credenciais
config = {
    'credentials':{
        'usernames':{
            'DIRETORIA':{
                'name': 'Diretoria Execultiva',
                'password': '$2b$12$Ent0v.CnalsqHIMJhZ/ku.Dm/6md0A8COQ/0SJzr4pT5Q7qfTlSJi'
            },
            'CE':{
                'name': 'Gestão Ceará',
                'password': '$2b$12$nVS.QWOTB1dLBbfj5u4h0.35YuUdue2lCIvWCspjrA8aPy9pV1ZMe'
            },
            'PI':{
                'name': 'Gestão Piauí',
                'password': '$2b$12$sfqvn5N4BrYsSlAlUmX8/.wyCNvfOE8b3sJDF0IXIojtPrjb9Q5li'
            },
            'PE':{
                'name': 'Gestão Pernambuco',
                'password': '$2b$12$6FY/JOjEn1455nz0RvcSre3FDGY1IMHqj6bfRf/7R5x/WSsLJuNwG'
            },
            'BA':{
                'name': 'Gestão Baia',
                'password': '$2b$12$RPXPBzin04ubvZhQtuVSoeCuSzpw2iywHbfLb0uDMj92knXTgS1PG'
            },
            'MA':{
                'name': 'Gestão Maranhão',
                'password': '$2b$12$sH9pw/mQzjjb3R91jv47YeHWqoBDefZ8CZIVUwQtyTJFHCJi.tDiO'
            }
        }
    },
    'cookie':{
        'expiry_days': 90,
        'key': 'chave_secreta_dashboard',
        'name': 'conexao'
    }
}

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

name, authentication_status, username = authenticator.login('main')

# if authentication_status:
#     logger.info(f'LOGIN SUCESSO | Usuário: {username}')

#     with st.sidebar:
#         authenticator.logout('Sair', 'sidebar')
#         st.header('Filtros Globais')

#     @st.cache_data(ttl=3600)
#     def load_and_process_data():
#         tr


@st.cache_data
def carregar_dados_brutos():
    df_empty = pd.DataFrame()
    
    try:

        df_conexao = pd.read_csv(FILE_CONEXAO, sep=';', decimal=',', encoding='utf-8-sig')
        df_conexao.columns = df_conexao.columns.str.upper()

        df_conexao = df_conexao.rename(columns={
            'CLIENTE': 'CLIENTE_NOME_FATURADO',
            'CNPJ_CLIENTE': 'CLIENTE_CNPJ_BASE',
            'TOTAL_FATURADO': 'VALOR_FATURADO',
            'VALOR_DEVOLVIDO': 'VALOR_DEVOLVIDO',
            'FORNECEDOR': 'FORNECEDOR_NOME_FATURADO',
            'CNPJ_FORNECEDOR': 'FORNECEDOR_CNPJ_FATURADO', 
            'CODFILIAL': 'CODFILIAL_FATURAMENTO'
        }, errors='ignore')


        df_pedidos = pd.read_csv(FILE_PEDIDOS, sep=';', decimal='.', encoding='utf-8')
        df_pedidos.columns = df_pedidos.columns.str.upper()

        df_pedidos = df_pedidos.rename(columns={
            'CLIENTE_NOME': 'CLIENTE_NOME',
            'CLIENTE_CNPJ': 'CLIENTE_CNPJ_BASE',
            'TOTAL_VALOR_PEDIDO': 'VALOR_PEDIDO',
            'TOTAL_PEDIDOS_QTD': 'PEDIDOS_QTD',
            'FORNECEDOR_NOME': 'FORNECEDOR_NOME_PEDIDO',
            'FORNECEDOR_CNPJ': 'FORNECEDOR_CNPJ_PEDIDO', 
            'CODFILIAL': 'CODFILIAL_PEDIDO'
        }, errors='ignore')

    except FileNotFoundError:
        st.error(f"⚠️ Erro: Um ou ambos os arquivos ({FILE_CONEXAO}, {FILE_PEDIDOS}) não foram encontrados.")
        return df_empty, df_empty


    

    df_conexao['CLIENTE_CNPJ_LIMPO'] = limpar_cnpj(df_conexao['CLIENTE_CNPJ_BASE'])
    df_pedidos['CLIENTE_CNPJ_LIMPO'] = limpar_cnpj(df_pedidos['CLIENTE_CNPJ_BASE'])

    df_conexao['FORNECEDOR_CNPJ_LIMPO'] = limpar_cnpj(
        df_conexao.get('FORNECEDOR_CNPJ_FATURADO', df_conexao.get('FORNECEDOR_NOME_FATURADO'))
    )
    df_pedidos['FORNECEDOR_CNPJ_LIMPO'] = limpar_cnpj(
        df_pedidos.get('FORNECEDOR_CNPJ_PEDIDO', df_pedidos.get('FORNECEDOR_NOME_PEDIDO'))
    )
    

    if 'CODFILIAL_FATURAMENTO' not in df_conexao.columns and 'CODFILIAL' in df_conexao.columns:
        df_conexao['CODFILIAL_FATURAMENTO'] = df_conexao['CODFILIAL']
    elif 'CODFILIAL_FATURAMENTO' not in df_conexao.columns:
        df_conexao['CODFILIAL_FATURAMENTO'] = 'FILIAL_UNICA'
    
    return df_conexao, df_pedidos


def calcular_metricas_agregadas(df_conexao: pd.DataFrame, df_pedidos: pd.DataFrame, coluna_estado: str = 'ESTADO'):

    
    df_empty = pd.DataFrame()
    if df_conexao.empty and df_pedidos.empty:
        return df_empty, df_empty, df_empty, df_empty


    df_conexao_agg_cliente = df_conexao.groupby('CLIENTE_CNPJ_LIMPO').agg({
        'CLIENTE_NOME_FATURADO': 'first',
        'VALOR_FATURADO': 'sum',
        'VALOR_DEVOLVIDO': 'sum'
    }).reset_index()

    df_pedidos_agg_cliente = df_pedidos.groupby('CLIENTE_CNPJ_LIMPO').agg({
        'CLIENTE_NOME': 'first',
        'VALOR_PEDIDO': 'sum'
    }).reset_index()

    df_merged = pd.merge(df_pedidos_agg_cliente, df_conexao_agg_cliente, on='CLIENTE_CNPJ_LIMPO', how='outer')
    df_merged['VALOR_PEDIDO'] = df_merged['VALOR_PEDIDO'].fillna(0)
    df_merged['VALOR_FATURADO'] = df_merged['VALOR_FATURADO'].fillna(0)
    df_merged['VALOR_DEVOLVIDO'] = df_merged['VALOR_DEVOLVIDO'].fillna(0)
    df_merged['CLIENTE'] = df_merged['CLIENTE_NOME'].fillna(df_merged['CLIENTE_NOME_FATURADO'])
    df_merged['DIFERENCA_FLUXO'] = df_merged['VALOR_PEDIDO'] - df_merged['VALOR_FATURADO']
    df_merged['VALOR_LIQUIDO_FATURADO'] = df_merged['VALOR_FATURADO'] - df_merged['VALOR_DEVOLVIDO']
    df_analise_cliente = df_merged.copy()

    # --- Análise por Fornecedor ---
    df_pedidos_forn = df_pedidos.groupby('FORNECEDOR_CNPJ_LIMPO').agg(
        FORNECEDOR_NOME=('FORNECEDOR_NOME_PEDIDO', 'first'),
        VALOR_PEDIDO_TOTAL=('VALOR_PEDIDO', 'sum')
    ).reset_index().rename(columns={'FORNECEDOR_CNPJ_LIMPO': 'FORNECEDOR_CHAVE'})

    df_conexao_forn = df_conexao.groupby('FORNECEDOR_CNPJ_LIMPO').agg(
        FORNECEDOR_NOME=('FORNECEDOR_NOME_FATURADO', 'first'), 
        VALOR_FATURADO_TOTAL=('VALOR_FATURADO', 'sum'),
        VALOR_DEVOLVIDO_TOTAL=('VALOR_DEVOLVIDO', 'sum')
    ).reset_index().rename(columns={'FORNECEDOR_CNPJ_LIMPO': 'FORNECEDOR_CHAVE'})

    df_analise_fornecedor_agg = pd.merge(
        df_pedidos_forn, df_conexao_forn, on='FORNECEDOR_CHAVE', how='outer', suffixes=('_PEDIDO', '_FATURADO')
    ).fillna(0)
    
    df_analise_fornecedor_agg['FORNECEDOR'] = df_analise_fornecedor_agg['FORNECEDOR_NOME_PEDIDO'].replace(0, np.nan).fillna(df_analise_fornecedor_agg['FORNECEDOR_NOME_FATURADO'])
    df_analise_fornecedor_agg['FORNECEDOR'] = df_analise_fornecedor_agg['FORNECEDOR'].str.upper().str.strip()
    df_analise_fornecedor_agg['DIFERENCA_FLUXO'] = (
        df_analise_fornecedor_agg['VALOR_PEDIDO_TOTAL'] - df_analise_fornecedor_agg['VALOR_FATURADO_TOTAL']
    )
    df_analise_fornecedor = df_analise_fornecedor_agg.rename(columns={
        'VALOR_PEDIDO_TOTAL': 'VALOR_PEDIDO',
        'VALOR_FATURADO_TOTAL': 'VALOR_FATURADO',
        'VALOR_DEVOLVIDO_TOTAL': 'VALOR_DEVOLVIDO'
    })
    df_analise_fornecedor['FORNECEDOR_CNPJ_LIMPO'] = df_analise_fornecedor['FORNECEDOR_CHAVE']
    df_analise_fornecedor = df_analise_fornecedor[['FORNECEDOR', 'FORNECEDOR_CNPJ_LIMPO', 'VALOR_PEDIDO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'DIFERENCA_FLUXO']].copy()
    
    # --- Análise por Filial ---
    df_analise_filial = df_conexao.groupby('CODFILIAL_FATURAMENTO').agg(
        VALOR_FATURADO=('VALOR_FATURADO', 'sum'),
        VALOR_DEVOLVIDO=('VALOR_DEVOLVIDO', 'sum')
    ).reset_index().rename(columns={'CODFILIAL_FATURAMENTO': 'FILIAL'})

    df_analise_filial['VALOR_LIQUIDO_FATURADO'] = (
        df_analise_filial['VALOR_FATURADO'] - df_analise_filial['VALOR_DEVOLVIDO']
    )
    df_analise_filial['VALOR_PEDIDO'] = 0.0
    df_analise_filial['DIFERENCA_FLUXO'] = 0 - df_analise_filial['VALOR_FATURADO']
    
    # --- Análise por Estado ---
    if coluna_estado in df_conexao.columns:
        df_analise_estado = df_conexao.groupby(coluna_estado).agg(
            VALOR_FATURADO=('VALOR_FATURADO', 'sum'),
            VALOR_DEVOLVIDO=('VALOR_DEVOLVIDO', 'sum')
        ).reset_index().rename(columns={coluna_estado: 'ESTADO'})

        df_analise_estado['VALOR_LIQUIDO_FATURADO'] = (
            df_analise_estado['VALOR_FATURADO'] - df_analise_estado['VALOR_DEVOLVIDO']
        )
        df_analise_estado['VALOR_PEDIDO'] = 0.0
        df_analise_estado['DIFERENCA_FLUXO'] = 0 - df_analise_estado['VALOR_FATURADO']
    else:
        df_analise_estado = pd.DataFrame(columns=['ESTADO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'VALOR_LIQUIDO_FATURADO', 'VALOR_PEDIDO', 'DIFERENCA_FLUXO'])
    
    return df_analise_cliente, df_analise_fornecedor, df_analise_filial, df_analise_estado


def formatar_moeda(valor):
    try:
        return locale.currency(valor, grouping=True)
    except:
        return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


# ==============================
# INTERFACE STREAMLIT
# ==============================

def main(username, name):
    st.set_page_config(layout="wide", page_title="Dashboard Conexão")

    # Sidebar unificada com Logout
    with st.sidebar:
        st.image('logo_total.png', width=200)
        authenticator.logout('Sair do Sistema', 'sidebar')
        st.divider()
        st.header('Configurações de Acesso')

    df_conexao_bruto, df_pedidos_bruto = carregar_dados_brutos()

    # =========================================================
    # GOVERNANÇA DE DADOS (RBAC)
    # =========================================================
    if username == 'DIRETORIA':
        # Diretoria: Acesso irrestrito
        lista_estados_permitidos = ['Todos'] + sorted(df_conexao_bruto[COLUNA_ESTADO].unique().tolist())
        index_default = 0
        bloquear_filtro = False
    else:
        # Regionais: Acesso restrito ao próprio username (CE, PI, etc)
        lista_estados_permitidos = [username]
        index_default = 0
        bloquear_filtro = True

    with st.sidebar:
        estado_selecionado = st.selectbox(
            'Estado sob Visualização:',
            lista_estados_permitidos,
            index=index_default,
            disabled=bloquear_filtro,
            help='Sua conta possui restrição de visualização geográfica.' if bloquear_filtro else ''
        )

    # Aplicação do Filtro Global (Dataframes principais)
    df_conexao_filtrado = df_conexao_bruto.copy()
    df_pedidos_filtrado = df_pedidos_bruto.copy()

    if str(estado_selecionado).upper() != 'TODOS' and str(estado_selecionado).upper() != 'DIRETORIA':
        df_conexao_filtrado = df_conexao_bruto[df_conexao_bruto[COLUNA_ESTADO].str.upper() == estado_selecionado.upper()].copy()
        # Filtro relacional nos pedidos baseado nos clientes do estado
        clientes_alvo = df_conexao_filtrado['CLIENTE_CNPJ_LIMPO'].unique()
        df_pedidos_filtrado = df_pedidos_bruto[df_pedidos_bruto['CLIENTE_CNPJ_LIMPO'].isin(clientes_alvo)].copy()
            
    # ==============================
    # CÁLCULO DAS MÉTRICAS 
    # ==============================
    df_analise_cliente, df_analise_fornecedor, df_analise_filial, df_analise_estado = \
        calcular_metricas_agregadas(df_conexao_filtrado, df_pedidos_filtrado, COLUNA_ESTADO)
    
    if df_analise_cliente.empty:
        st.info(f"Nenhum dado encontrado para o Estado: **{estado_selecionado}**.")
        return

    # ==============================
    # 1. VISÃO GERAL
    # ==============================
    if estado_selecionado == 'Todos':
        st.header(f"1. Visão Geral")
    else:
        st.header(f"1. Visão Geral: {estado_selecionado}")

    total_pedido = df_analise_cliente['VALOR_PEDIDO'].sum()
    total_faturado = df_analise_cliente['VALOR_FATURADO'].sum()
    total_devolvido = df_analise_cliente['VALOR_DEVOLVIDO'].sum()
    total_diferenca = df_analise_cliente['DIFERENCA_FLUXO'].sum()

    TOTAL_CLIENTES_PRESENTES = 500
    dados_clientes_cargos = {
        "Acompanhante": 229,
        "Balconista": 23,
        "Comprador": 337,
        "Proprietário": 641,
        "Outros": 397
    }

    main_kpi_col, main_kpi_tabela_col = st.columns([2, 1])

    with main_kpi_col:
        col_pedido, col_faturado, col_devolvido = st.columns(3)

        with col_pedido:
            st.metric("Valor Total de Pedidos", formatar_moeda(total_pedido))
        with col_faturado:
            st.metric("Valor Total Faturado", formatar_moeda(total_faturado))
        with col_devolvido:
            st.metric("Valor Total Devolvido", formatar_moeda(total_devolvido))

        st.markdown("##")

        col_diferenca, col_clientes = st.columns(2)
        with col_diferenca:
            delta_color = "normal" if total_diferenca < 0 else "inverse"
            st.metric(
                "Diferença (Pedido - Faturado)",
                formatar_moeda(total_diferenca),
                delta="Potencial ou Divergência Total",
                delta_color=delta_color
            )
        with col_clientes:
            
            st.metric("Clientes Únicos", TOTAL_CLIENTES_PRESENTES)

    with main_kpi_tabela_col:
        st.subheader("Cargos - Cliente")
        st.table(dados_clientes_cargos)

    # ==============================
    # 2. TOP PERFORMANCE
    # ==============================
    st.header("2. Análise de Top Performance (Evento)")

    col_top_clientes, col_top_fornecedores = st.columns(2)


    with col_top_clientes:
        st.subheader("Clientes - Top 10 por Receita Líquida")
        df_top_clientes = df_analise_cliente[df_analise_cliente['VALOR_LIQUIDO_FATURADO'] > 0] \
            .sort_values(by='VALOR_LIQUIDO_FATURADO', ascending=False).head(10).copy()

        if not df_top_clientes.empty:
            df_top_clientes_display = df_top_clientes[['CLIENTE', 'VALOR_LIQUIDO_FATURADO']].copy()
            df_top_clientes_display.columns = ['Cliente', 'Receita Líquida']
            df_top_clientes_display['Receita Líquida'] = df_top_clientes_display['Receita Líquida'].apply(formatar_moeda)
            st.dataframe(df_top_clientes_display, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum cliente com Receita Líquida positiva encontrado.")


    with col_top_fornecedores:
        st.subheader("Fornecedores - Top 10 por Faturamento")
        df_top_fornecedores = df_analise_fornecedor[df_analise_fornecedor['VALOR_FATURADO'] > 0] \
            .sort_values(by='VALOR_FATURADO', ascending=False).head(10).copy()

        if not df_top_fornecedores.empty:
            df_top_fornecedores_display = df_top_fornecedores[['FORNECEDOR', 'FORNECEDOR_CNPJ_LIMPO', 'VALOR_FATURADO']].copy()
            df_top_fornecedores_display.columns = ['Fornecedor', 'CNPJ', 'Valor Faturado']
            df_top_fornecedores_display['Valor Faturado'] = df_top_fornecedores_display['Valor Faturado'].apply(formatar_moeda)
            st.dataframe(df_top_fornecedores_display, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum fornecedor com Faturamento positivo encontrado.")

    # ==============================
    # 3. ANÁLISE POR FILIAL 
    # ==============================
    st.header("3. Análise de Desempenho por Filial")

    if not df_analise_filial.empty:
        df_display_filial = df_analise_filial.sort_values(by='VALOR_LIQUIDO_FATURADO', ascending=False).copy()
        cols_to_display_filial = ['FILIAL', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'VALOR_LIQUIDO_FATURADO']
        for col in cols_to_display_filial[1:]:
            df_display_filial[col] = df_display_filial[col].apply(formatar_moeda)
        st.dataframe(df_display_filial[cols_to_display_filial], use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma filial encontrada para análise.")

    #===============================
    # 4. ANÁLISE POR ESTADO 
    #===============================

    st.header('4. Análise por Estado')

    if not df_analise_estado.empty:
        df_display_estado = df_analise_estado.sort_values(by='VALOR_LIQUIDO_FATURADO', ascending=False).copy()
        cols_to_display_estado = ['ESTADO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'VALOR_LIQUIDO_FATURADO']
        for col in cols_to_display_estado[1:]:
            df_display_estado[col] = df_display_estado[col].apply(formatar_moeda)
        st.dataframe(df_display_estado[cols_to_display_estado], use_container_width=True, hide_index=True)
    else:
        st.info('Nenhuma Estado encontrado para análise.')

    # ==============================
    # 5. TABELA DETALHADA POR CLIENTE
    # ==============================
    st.header("5. Tabela Detalhada por Cliente")

    if not df_analise_cliente.empty:
        df_display_cliente = df_analise_cliente.sort_values(by='DIFERENCA_FLUXO', ascending=False).copy()
        cols = ['CLIENTE', 'CLIENTE_CNPJ_LIMPO', 'VALOR_PEDIDO', 'VALOR_FATURADO',
                'VALOR_DEVOLVIDO', 'DIFERENCA_FLUXO', 'VALOR_LIQUIDO_FATURADO']
        for col in cols[2:]:
            df_display_cliente[col] = df_display_cliente[col].apply(formatar_moeda)
        
        display_cols = dict(zip(cols, ['Cliente', 'CNPJ', 'Valor Pedido', 'Valor Faturado', 'Valor Devolvido', 'Diferença Fluxo', 'Receita Líquida']))
        df_display_cliente_final = df_display_cliente[cols].rename(columns=display_cols)
        st.dataframe(df_display_cliente_final, use_container_width=True)
    else:
        st.info("Nenhum cliente encontrado.")

    # ==============================
    # 6. TABELA DETALHADA POR FORNECEDOR 
    # ==============================
    st.header("6. Tabela Detalhada por Fornecedor")

    if not df_analise_fornecedor.empty:
        df_display_fornecedor = df_analise_fornecedor.sort_values(by='DIFERENCA_FLUXO', ascending=False).copy()
        cols = ['FORNECEDOR', 'FORNECEDOR_CNPJ_LIMPO', 'VALOR_PEDIDO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'DIFERENCA_FLUXO']
        for col in cols[2:]:
            df_display_fornecedor[col] = df_display_fornecedor[col].apply(formatar_moeda)
        
        display_cols_forn = dict(zip(cols, ['Fornecedor', 'CNPJ', 'Valor Pedido', 'Valor Faturado', 'Valor Devolvido', 'Diferença Fluxo']))
        df_display_fornecedor_final = df_display_fornecedor[cols].rename(columns=display_cols_forn)
        st.dataframe(df_display_fornecedor_final, use_container_width=True)
    else:
        st.info("Nenhum fornecedor encontrado.")

    # =========================================================
    # 7. CONEXÃO 2025 - CLIENTES FATURADOS (VERSÃO CORRIGIDA)
    # =========================================================
    st.header("7. Conexão 2025 - Clientes Faturados")
    arquivo_novo = 'conexao_2025_clientes fat.xlsx'

    try:
        # 1. Carregar e limpar nomes de colunas
        df_novo = pd.read_excel(arquivo_novo)
        df_novo.columns = df_novo.columns.astype(str).str.strip().str.upper()

        # 2. Mapeamento de normalização
        mapa_estados = {
            'BAIA': 'BA', 'CEARA': 'CE', 'IMPERATRIZ': 'MA',
            'PERNAB': 'PE', 'SAOLUIS': 'MA', 'TERESINA': 'PI'
        }

        # 3. Limpeza pesada na coluna ESTADO
        if 'ESTADO' in df_novo.columns:
            # Remove espaços, converte para maiúsculas e aplica o mapa
            df_novo['ESTADO'] = df_novo['ESTADO'].astype(str).str.strip().str.upper()
            df_novo['ESTADO'] = df_novo['ESTADO'].replace(mapa_estados)

        # Definir colunas desejadas em maiúsculas para dar match com a limpeza acima
        colunas_desejadas = ['ESTADO','COD', 'RAZAO', 'TOTAL_GASTO', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        
        # Filtrar apenas colunas que realmente existem para evitar erro de KeyError
        colunas_existentes = [c for c in colunas_desejadas if c in df_novo.columns]

        # 4. Lógica de Filtro baseada no Login
        df_filtrado = df_novo.copy()
        
        # Normalizamos a seleção do usuário para comparar com o dado limpo do Excel
        ref_estado = str(estado_selecionado).strip().upper()

        if ref_estado != 'TODOS' and ref_estado != 'DIRETORIA':
            df_filtrado = df_novo[df_novo['ESTADO'] == ref_estado]

        # 5. Exibição final
        if not df_filtrado.empty:
            df_exibicao = df_filtrado[colunas_existentes].copy()
            if 'TOTAL_GASTO' in df_exibicao.columns:
                df_exibicao['TOTAL_GASTO'] = df_exibicao['TOTAL_GASTO'].apply(formatar_moeda)
            
            st.dataframe(df_exibicao, use_container_width=True, hide_index=True)
        else:
            st.warning(f"Nenhum dado encontrado para o filtro: {ref_estado}")

    except Exception as e:
        st.error(f"Erro técnico ao processar Seção 7: {e}")




# =========================================================
# FLUXO DE EXECUÇÃO (CRÍTICO)
# =========================================================
# Este bloco deve estar no "top-level" do script, 
# logo antes do __name__ == "__main__"

if authentication_status:
    # Caso o login seja bem-sucedido, executa o app passando o contexto do usuário
    main(username, name)

elif authentication_status is False:
    # Caso a senha esteja errada
    st.error('Usuário ou senha incorretos')

elif authentication_status is None:
    # Estado inicial: aguardando o usuário preencher os campos
    st.warning('Por favor, insira suas credenciais para acessar o painel')

# O ponto de entrada padrão do Python
if __name__ == "__main__":
    # Aqui você pode colocar configurações globais que não dependem do login
    pass
