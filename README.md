# Pipeline ETL: Excel/CSV para PostgreSQL

Este projeto é um script de automação **ETL (Extract, Transform, Load)** desenvolvido em Python. Ele automatiza o processo de leitura de arquivos de dados corporativos (Excel ou CSV), realiza o tratamento e limpeza dos dados, e carrega-os de forma segura num banco de dados PostgreSQL.

## 🚀 Funcionalidades

* **Multi-formato:** Detecção e leitura automática de arquivos `.xlsx` e `.csv`.
* **Limpeza Inteligente:** Uso de **Regex** para sanitizar nomes de colunas (remove acentos, espaços e caracteres especiais automaticamente).
* **Segurança Transacional:** Implementação de **COMMIT** e **ROLLBACK**. Se ocorrer qualquer erro durante a inserção de 1.000 linhas, o script desfaz tudo para evitar dados corrompidos (Atomicidade).
* **Preparação Automática:** Recriação automática da estrutura da tabela (`DROP/CREATE`) baseada dinamicamente nas colunas do arquivo.

## 🛠️ Tecnologias Utilizadas

* **Python 3.x**
* **Pandas:** Para manipulação e leitura de DataFrames.
* **Psycopg2:** Para conexão robusta e execução de comandos SQL no PostgreSQL.
* **PostgreSQL:** Banco de dados relacional.

## ⚙️ Como Configurar

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/SEU-USUARIO/NOME-DO-REPOSITORIO.git](https://github.com/SEU-USUARIO/NOME-DO-REPOSITORIO.git)
    ```

2.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure as Credenciais:**
    Crie um arquivo `config.py` na raiz do projeto com as suas credenciais do banco (este arquivo não é enviado ao GitHub por segurança):
    ```python
    db_usuario = "seu_usuario"
    db_senha = "sua_senha"
    db_host = "localhost"
    db_porta = "5432"
    db_nome = "postgres"
    ```

4.  **Execute o Script:**
    Coloque o seu arquivo de dados na pasta e execute:
    ```bash
    python importar.py
    ```

---
Desenvolvido para fins de estudo e automação de processos de dados.

ESTE PROJETO ESTA EM DESENVOLVIMENTO E NAO ESTÁ COMPLETO, POR FAVOR LEVE ISSO EM CONTA