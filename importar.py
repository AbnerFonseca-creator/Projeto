import pandas as pd
import psycopg2
import psycopg2.extras as extras # Essencial para a inserção em lote
import sys
import os
import urllib.parse
from pandas.io import sql as pd_sql # Usaremos para gerar o SQL de CREATE TABLE

# --- Tenta importar as credenciais do config.py ---
try:
    from config import db_usuario, db_senha, db_host, db_porta, db_nome
except ImportError:
    sys.exit("ERRO: Arquivo 'config.py' não encontrado. Crie-o com suas credenciais.")

# --- 1. CONFIGURAÇÕES (DEVES ALTERAR ISTO) ---
# Define o arquivo de entrada (pode ser .xlsx ou .csv)
caminho_do_arquivo = "dados.xlsx"
# Define o nome da tabela que será criada/substituída no PostgreSQL
nome_da_tabela = "Projeto" 
# --- FIM DAS CONFIGURAÇÕES ---


def ler_arquivo_de_dados(caminho_arquivo):
    """
    (E) EXTRAÇÃO: Lê um arquivo de dados (Excel ou CSV) e retorna um DataFrame.
    """
    try:
        nome_base, extensao = os.path.splitext(caminho_arquivo)
        extensao = extensao.lower()

        print(f"A tentar ler o arquivo: {caminho_arquivo} (tipo: {extensao})")

        if extensao == '.xlsx':
            df = pd.read_excel(caminho_arquivo)
        elif extensao == '.csv':
            # Nota: O teu CSV pode usar ';' como separador. Se falhar, altera sep=',' para sep=';'
            df = pd.read_csv(caminho_arquivo, sep=',')
        else:
            sys.exit(f"Erro: Extensão '{extensao}' não suportada. Use .xlsx ou .csv.")

        if df.empty:
            sys.exit(f"O arquivo '{caminho_arquivo}' está vazio.")
            
        print(f"Arquivo lido com sucesso. {len(df)} linhas encontradas.")
        
        # Limpa os nomes das colunas (boa prática para SQL)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace('-', '_', regex=False)
        print("Nomes das colunas limpos (minúsculas, sem espaços).")
        
        return df
        
    except FileNotFoundError:
        sys.exit(f"Erro: O arquivo '{caminho_arquivo}' não foi encontrado.")
    except Exception as e:
        sys.exit(f"Erro ao ler o arquivo: {e}")


def conectar_ao_banco_psycopg2():
    """
    Cria e retorna uma conexão direta com o PostgreSQL usando psycopg2.
    """
    print("A tentar conectar ao PostgreSQL usando psycopg2...")
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_porta,
            user=db_usuario,
            password=db_senha,
            dbname=db_nome
        )
        print("Conexão com o PostgreSQL (psycopg2) estabelecida com sucesso!")
        return conn
    except Exception as e:
        sys.exit(f"Erro ao conectar ao PostgreSQL com psycopg2: {e}")


def preparar_banco_de_dados(conn, df, tabela):
    # ... (início da função) ...
    try:
        with conn.cursor() as cursor:
            # 1. Rotina de exclusão (DROP TABLE)
            # Adicionamos aspas ("{tabela}") para forçar o case-sensitive (ex: "Projeto")
            print(f'A executar \'DROP TABLE IF EXISTS "{tabela}"\'...')
            cursor.execute(f'DROP TABLE IF EXISTS "{tabela}";') # <--- CORRIGIDO
            
            # 2. Rotina de criação (CREATE TABLE)
            # O pandas (get_schema) já lida com as aspas corretamente
            create_sql = pd.io.sql.get_schema(df, tabela)
            
            # ... (resto da função) ...
            
            # (Opcional) Ajuste fino do SQL gerado pelo Pandas
            create_sql = create_sql.replace('TEXT', 'VARCHAR(255)').replace('real', 'DECIMAL')
            
            print(f"A executar 'CREATE TABLE {tabela}'...")
            cursor.execute(create_sql)
            
        # IMPORTANTE: Não fazemos commit aqui! 
        # O commit só acontece DEPOIS da inserção, para garantir que tudo (DROP, CREATE, INSERT)
        # faça parte da MESMA transação.
        print("Banco de dados preparado com sucesso (DROP/CREATE executado).")
        
    except Exception as e:
        print(f"Erro ao preparar o banco de dados: {e}")
        print("A executar ROLLBACK da preparação...")
        conn.rollback() # Desfaz o DROP/CREATE se algo der errado
        sys.exit("Script parado.")


def inserir_dados_em_lote(conn, df, tabela):
    """
    (L) CARGA: Insere os dados em lote com gestão de transação (COMMIT/ROLLBACK).
    """
    print(f"Iniciando inserção em lote para {len(df)} linhas...")
    
    # 1. Transforma o DataFrame numa lista de tuplos (formato que o psycopg2 prefere)
    tuplos = [tuple(x) for x in df.to_numpy()]
    
    # 2. Gera a string de colunas (ex: "col1, col2, col3")
    cols = ','.join(list(df.columns))
    
    # 3. Gera a string de valores (ex: "%s, %s, %s")
    vals = ','.join(['%s'] * len(df.columns))
    
    # 4. Cria a query SQL final
    query = f"INSERT INTO {tabela} ({cols}) VALUES ({vals})"
    
    # 5. GERENCIAMENTO DE TRANSAÇÃO (COMMIT/ROLLBACK)
    cursor = None # Inicia o cursor fora do try
    try:
        cursor = conn.cursor()
        
        # Esta é a inserção em lote (muito mais rápido que um 'for' loop)
        extras.execute_batch(cursor, query, tuplos)
        
        # Se chegámos aqui, a inserção funcionou.
        print("Inserção em lote concluída. A executar 'COMMIT'...")
        conn.commit() # Confirma a transação (salva tudo)
        
        print(f"Sucesso! {len(df)} linhas inseridas e 'COMMIT' realizado.")
        
    except (Exception, psycopg2.DatabaseError) as e:
        # Se algo der errado durante a inserção...
        print(f"ERRO DURANTE A INSERÇÃO: {e}")
        print("A executar 'ROLLBACK'... Nenhuma linha foi inserida.")
        if conn:
            conn.rollback() # Desfaz a transação (não salva nada)
    finally:
        if cursor:
            cursor.close() # Sempre fecha o cursor


# --- Execução Principal do Script ---
if __name__ == "__main__":
    print("Iniciando o script ETL (v2 com psycopg2 e transações)...")
    
    conn = None # Inicia a conexão como None
    try:
        # ETAPA 1: EXTRAÇÃO (Ler o arquivo .xlsx ou .csv)
        dataframe = ler_arquivo_de_dados(caminho_do_arquivo)
        
        # ETAPA 2: CONEXÃO (Conectar ao banco via psycopg2)
        conn = conectar_ao_banco_psycopg2()
        
        # ETAPA 3: PREPARAÇÃO (DROP/CREATE TABLE)
        preparar_banco_de_dados(conn, dataframe, nome_da_tabela)
        
        # ETAPA 4: CARGA (INSERT em lote com COMMIT/ROLLBACK)
        inserir_dados_em_lote(conn, dataframe, nome_da_tabela)
        
    except Exception as e:
        print(f"Um erro inesperado ocorreu na execução principal: {e}")
    finally:
        # ETAPA 5: FECHAMENTO
        if conn:
            conn.close() # Sempre fecha a conexão no final
            print("Conexão com o PostgreSQL fechada.")
            
    print("Script ETL finalizado.")