import argparse
import pandas as pd
import duckdb
import os
import sys
import logging
import time

def parse_args():
    parser = argparse.ArgumentParser(description="Carga ETL Censo Escolar INEP 2024 para DuckDB")
    parser.add_argument('--csv', default='./data/microdados_ed_basica_2024.csv')
    parser.add_argument('--dict', default='./data/dicionario.xlsx')
    parser.add_argument('--db', default='./db/censo_escolar.duckdb')
    parser.add_argument('--table', default='censo')
    return parser.parse_args()

def setup_logging():
    os.makedirs('./log', exist_ok=True)
    logging.basicConfig(
        filename='./log/carga_censo.log',
        filemode='a',
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )

def ler_dicionario_em_dataframe(dict_path):
    df = pd.read_excel(dict_path, engine='openpyxl')
    df = df.rename(columns=lambda x: x.strip())
    df['Nome da Variável'] = df['Nome da Variável'].astype(str).str.strip()
    df['Tipo de Dado'] = df['Tipo de Dado'].astype(str).str.strip()
    df['Tamanho'] = df['Tamanho'].apply(lambda x: str(x).strip() if pd.notnull(x) else '')
    return df

def inferir_tipo_duckdb(tipo, tamanho):
    tipo = tipo.lower()
    if tipo == 'num':
        if ',' in tamanho:
            prec, esc = tamanho.split(',')
            return f'DECIMAL({prec.strip()},{esc.strip()})'
        elif tamanho.isdigit():
            prec = int(tamanho)
            if prec <= 18:
                return 'BIGINT'
            else:
                return 'DOUBLE'
        else:
            return 'DOUBLE'
    elif tipo == 'char':
        if tamanho.isdigit():
            return f'VARCHAR({tamanho})'
        else:
            return 'TEXT'
    else:
        return 'TEXT'

def construir_schema(df_dict, csv_cols):
    schema = {}
    for _, row in df_dict.iterrows():
        col = row['Nome da Variável']
        tipo = inferir_tipo_duckdb(row['Tipo de Dado'], row['Tamanho'])
        schema[col] = tipo
    # Colunas extras do CSV
    extras = [c for c in csv_cols if c not in schema]
    for col in extras:
        logging.warning(f'Coluna extra no CSV não prevista no dicionário: {col}')
        schema[col] = 'TEXT'
    # Colunas ausentes no CSV
    ausentes = [c for c in schema if c not in csv_cols]
    for col in ausentes:
        logging.warning(f'Coluna prevista no dicionário ausente no CSV: {col}')
    return schema, extras, ausentes

def conectar_duckdb(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return duckdb.connect(db_path)

def criar_tabela_destino(conn, table, schema):
    cols_def = ',\n  '.join([f'"{col}" {tipo}' for col, tipo in schema.items()])
    conn.execute(f'DROP TABLE IF EXISTS {table};')
    conn.execute(f'CREATE TABLE {table} (\n  {cols_def}\n);')

def executar_copy(conn, table, csv_path, schema):
    # DuckDB COPY options: DELIMITER, HEADER, QUOTE, ESCAPE, NULLSTR
        options = "DELIMITER ';', HEADER, QUOTE '\"', ESCAPE '\"', NULLSTR ['', 'NA', 'NULL']"
    cols = ','.join([f'"{col}"' for col in schema.keys()])
    try:
        copy_cmd = f"COPY {table} ({cols}) FROM '{csv_path}' ({options});"
        print(f'Comando COPY gerado:\n{copy_cmd}')
        conn.execute(copy_cmd)
    except Exception as e:
        logging.error(f'Erro na carga direta do CSV: {e}')
        # Fallback: tentar detectar encoding e converter para UTF-8
        import chardet
        with open(csv_path, 'rb') as f:
            result = chardet.detect(f.read(10000))
        encoding = result['encoding']
        logging.info(f'Detectado encoding: {encoding}')
        if encoding and encoding.lower() != 'utf-8':
            temp_csv = csv_path + '.utf8.csv'
            import codecs
            with codecs.open(csv_path, 'r', encoding) as src, open(temp_csv, 'w', encoding='utf-8') as dst:
                for line in src:
                    dst.write(line)
            conn.execute(f"COPY {table} ({cols}) FROM '{temp_csv}' ({options});")
            os.remove(temp_csv)
        else:
            raise

def criar_tabela_meta(conn, df_dict, schema, table='_meta'):
    df_dict = df_dict.copy()
    df_dict['Tipo DuckDB'] = df_dict.apply(lambda r: inferir_tipo_duckdb(r['Tipo de Dado'], r['Tamanho']), axis=1)
    conn.execute(f'DROP TABLE IF EXISTS {table};')
    conn.execute(f'CREATE TABLE {table} AS SELECT * FROM df_dict;')

def validar_e_relatar(conn, table, extras, ausentes):
    n_linhas = conn.execute(f'SELECT COUNT(*) FROM {table};').fetchone()[0]
    n_colunas = len(conn.execute(f"PRAGMA table_info('{table}');").fetchall())
    print(f'Resumo da carga:')
    print(f'- Total de linhas carregadas: {n_linhas}')
    print(f'- Número de colunas: {n_colunas}')
    print(f'- Colunas extras no CSV: {extras}')
    print(f'- Colunas ausentes no CSV: {ausentes}')
    logging.info(f'Total de linhas: {n_linhas}')
    logging.info(f'Número de colunas: {n_colunas}')
    logging.info(f'Colunas extras: {extras}')
    logging.info(f'Colunas ausentes: {ausentes}')

def main():
    args = parse_args()
    setup_logging()
    start = time.time()
    logging.info('Iniciando carga ETL Censo Escolar')
    df_dict = ler_dicionario_em_dataframe(args.dict)
    # Detectar encoding do CSV antes de abrir
    import csv
    import chardet
    with open(args.csv, 'rb') as f:
        result = chardet.detect(f.read(10000))
    encoding = result['encoding'] if result['encoding'] else 'utf-8'
    logging.info(f'Encoding detectado para CSV: {encoding}')
    try:
        with open(args.csv, encoding=encoding) as f:
            reader = csv.reader(f, delimiter=';')
            csv_cols = [col.strip() for col in next(reader)]
    except UnicodeDecodeError as e:
        logging.error(f'Erro de encoding ao ler cabeçalho do CSV: {e}')
        print(f'Erro de encoding ao ler cabeçalho do CSV. Tente abrir o arquivo e salvar como UTF-8.')
        sys.exit(1)
    schema, extras, ausentes = construir_schema(df_dict, csv_cols)
    conn = conectar_duckdb(args.db)
    conn.execute('PRAGMA threads = 4;')
    criar_tabela_destino(conn, args.table, schema)
    executar_copy(conn, args.table, args.csv, schema)
    criar_tabela_meta(conn, df_dict, schema)
    validar_e_relatar(conn, args.table, extras, ausentes)
    elapsed = time.time() - start
    logging.info(f'Tempo total de execução: {elapsed:.2f} segundos')
    print(f'Tempo total de execução: {elapsed:.2f} segundos')
    conn.close()

if __name__ == '__main__':
    main()
