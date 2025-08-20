# Oficina de IA 

Este projeto utiliza os microdados do Censo Escolar da Educação Básica para apresentar como desenvolver um ambiente para análises de dados utilizando o Copilot Github, Python, DuckDB, entree outros componentes de software. 


## Prompt para a IA - ETL

Gere um script Python robusto para carregar os microdados do Censo Escolar (INEP) 2024 em um banco DuckDB, obedecendo ao dicionário de dados em Excel. 

ARQUIVOS (no diretório corrente):
- CSV: ./data/microdados_ed_basica_2024.csv  (separador “;”, primeira linha tem cabeçalhos)
- Dicionário: ./data/dicionario.xlsx  (colunas: "Nome da Variável", "Descrição da Variável", "Tipo de Dado", "Tamanho", "Categoria (Domínio)"). 

Detalhamento sobre as colunas do dicionário de dados:

- Nome da Variável: Nome da Coluna / Atributo a  ser identificado na tabela de vabco de dados;
- "Descrição da Variável:  Um comentário para descrever ou conceituar o atributo;
- Tipo de Dado: É o tipo de dado do atributo a ser atribuído à coluna da tabela do banco de dados;
- Tamanho:  Tamanho do atributo (Num significa numérico e Char significa Texto). Considere utilizar tipos numéricos longos;
- Categoria (Domínio): Domínio do campo quando aplicável.   



SAÍDA:
- Banco DuckDB: ./db/censo_escolar.duckdb
- Nome da tabela destino: censo


REQUISITOS FUNCIONAIS:
1) Ler o dicionário em ./data/dicionario.xlsx e construir um schema (nome_da_coluna -> tipo_DuckDB) seguindo as regras abaixo.
2) Criar (ou recriar com segurança) a tabela censo no DuckDB.
3) Carregar o CSV para a tabela usando DuckDB (sem carregar tudo em memória via pandas).
4) Registrar uma tabela “_meta” com o conteúdo do dicionário e o tipo final usado.
5) Validar a aderência: verificar colunas do CSV vs. dicionário (faltantes e extras) e tratar conforme regras.

MAPEAMENTO DE TIPOS (dicionário → DuckDB):
- "Tipo de Dados" = "Num":
  - Se "Tamanho" estiver no formato precisão,escala (ex.: "10,2" com vírgula), converter para DECIMAL(precisão, escala).
  - Se "Tamanho" for um inteiro (ex.: "9") → usar BIGINT quando precisão ≤ 18; caso contrário, DOUBLE.
  - Se "Tamanho" vazio/indefinido → DOUBLE (fallback seguro).
- "Tipo de Dados" = "Char":
  - Se "Tamanho" for inteiro → VARCHAR(Tamanho).
  - Caso contrário → TEXT.
- Padronizar nomes de colunas exatamente como no CSV (não transformar para snake_case). Trime espaços e remova quebras invisíveis do Excel.
- Permitir caracteres acentuados e cedilha em nomes (sem alterar o cabeçalho do CSV).

REGRAS DE CARGA:
- Usar DuckDB “COPY” ou “read_csv”/“read_csv_auto” em SQL, com:
  - DELIMITER ';'
  - HEADER TRUE
  - QUOTE '"'
  - ESCAPE '"'
  - NULLSTR ['', 'NA', 'NULL'] (converter strings vazias e marcadores comuns para NULL)
- Não carregar o CSV inteiro em pandas (o arquivo é grande). A criação da tabela deve ocorrer antes da carga para que o COPY faça os casts.
- Se existirem colunas no CSV que não estão no dicionário:
  - Logar WARN e criar essas colunas como TEXT (para não perder dados).
- Se existirem colunas no dicionário que não estão no CSV:
  - Logar WARN e criar a coluna na tabela; durante o COPY permanecerá NULL.
- Ativar paralelismo do DuckDB quando possível (ex.: PRAGMA threads = N).

VALIDAÇÕES E RELATÓRIO:
- Printar um resumo ao final:
  - total de linhas carregadas (SELECT COUNT(*)…)
  - nº de colunas
  - lista de colunas extras (CSV não previstas) e colunas ausentes (previstas não presentes)
- Salvar um arquivo de log ./log/carga_censo.log com tempo de execução, avisos e estatísticas.
- Garantir idempotência:
  - Se ./db/censo_escolar.duckdb existir, recriar apenas a tabela destino (DROP TABLE IF EXISTS microdados_inep_2024;)

ROBUSTEZ:
- Tratar encoding do CSV:
  - Tentar carga direta; se falhar por encoding, detectar encoding rapidamente (opcional) e instruir DuckDB a ler o byte stream corretamente (ou, em fallback, abrir com Python e gravar um arquivo temporário UTF-8 para COPY).
- Tratar campos numéricos com separador decimal vírgula no próprio COPY (ou CAST apropriado pós-leitura, se necessário).
- Garantir que a montagem do schema não crie listas desalinhadas (evitar o erro “All arrays must be of the same length”).

INTERFACE (argparse):
- --csv ./data/microdados_ed_basica_2024.csv
- --dict ./data/dicionario.xlsx
- --db ./db/censo_escolar.duckdb
- --table censo
- Valores padrão conforme acima.

ESTRUTURA SUGERIDA:
- main()
  - parse_args()
  - ler_dicionario_em_dataframe()
  - normalizar_colunas_dicionario()
  - inferir_tipo_duckdb(tipo_dado, tamanho) -> string
  - construir_schema(dict_coluna_tipo)
  - conectar_duckdb(db_path)
  - criar_tabela_destino(schema)
  - executar_copy(csv_path, opcoes)
  - criar_tabela_meta(df_dicionario_com_tipo_final)
  - validar_e_relatar()

ACEITAÇÃO:
- Ao rodar o script sem argumentos, criar ./db/enso_escolar.duckdb, carregar a tabela microdados_inep_2024, criar _meta e imprimir o resumo.
- Não lançar “All arrays must be of the same length”.
- Respeitar os tipos conforme mapeamento.
- Log ./log/carga_censo.log criado com avisos sobre colunas extras/ausentes.

Gere o código completo com comentários e mensagens de log claras.


