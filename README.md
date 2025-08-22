# Oficina de IA 

Este projeto utiliza os microdados do Censo Escolar da Educação Básica para apresentar como desenvolver um ambiente para análises de dados utilizando o Copilot Github, Python, DuckDB, entree outros componentes de software. 


## Prompt para a IA - ETL

### Construção de um scrypt ETL

Gere um script Python robusto para carregar os microdados do Censo Escolar (INEP) 2024 em um banco DuckDB, obedecendo ao dicionário de dados em Excel. Nome para o scrupt cde ETL: etl_censo.py. 

ARQUIVO DE ENTRADA:

CSV: ./data/microdados_utf8.csv  (separador “;”, primeira linha tem cabeçalhos com os nomes de cada campo). O Arquivo está codificado em UTF-8. 
Para mais informações sobre o arquivo, consulte a planilha em Excel ./data/dicionario.xlsx (aba microdados_unidade_coleta). Esta planilha contém as seguinte coluns com as seguintes descrições (trata-se de um dicionário de dados):  

- Coluna A (Nome da Variável): Refere-se ao nome do campo (coluna/atributo da tabela)
- Coluna B ("Descrição da Variável): Refere-se a descrição detalhada do que significa o campo;
- Coluna C (Tipo de Dado): É o tipo de dado do campo (O valor "Num" significa que o campo é do tipo numérico e "Char" significa que o campo é do tipo Texto)
- Coluna D (Tamanho):  É o tamanho do atributo . 
- Coluna E (Categoria): Domínio do campo quando aplicável.   


SAÍDA:
- Banco DuckDB: ./db/censo_escolar.duckdb
- Nome da tabela destino: censo

Considere aplicar o tipo de dados e tamanho em cada campo da tabela destino. 
Os tipos numéricos são predominantemente inteiros longo podendo assumir valores maiores que 50 milhões. 

REQUISITOS FUNCIONAIS:
1) Ler o dicionário em ./data/dicionario.xlsx e construir um schema (nome_da_coluna -> tipo_DuckDB).
2) Criar (ou recriar com segurança) a tabela censo no DuckDB.
3) Carregar o CSV para a tabela usando DuckDB (dado o volume de dados, considere fazer esta operação de forma que a memória RAM do computador não seja sacrificada).
4) Registrar uma tabela “_meta” com o conteúdo do dicionário e o tipo final usado.
5) Validar a aderência: verificar colunas do CSV vs. dicionário (faltantes e extras) e tratar de forma conveniente.

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

VALIDAÇÕES E RELATÓRIO:
- Printar um resumo ao final:
  - total de linhas carregadas (SELECT COUNT(*)…)
  - nº de colunas
  - lista de colunas extras (CSV não previstas) e colunas ausentes (previstas não presentes)
- Salvar um arquivo de log ./log/carga_censo.log com tempo de execução, avisos e estatísticas.
- Garantir idempotência:
  - Se ./db/censo_escolar.duckdb existir, recriar apenas a tabela destino (DROP TABLE IF EXISTS microdados_inep_2024;)





