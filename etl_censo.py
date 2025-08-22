import argparse
import pandas as pd
import duckdb
import os
import logging
import time

def parse_args():
    parser = argparse.ArgumentParser(description="ETL Censo Escolar INEP 2024 para DuckDB")
    parser.add_argument('--csv', default='./data/microdados_utf8.csv')
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
    df = pd.read_excel(dict_path, sheet_name='microdados_unidade_coleta', engine='openpyxl')
    df = df.rename(columns=lambda x: x.strip())
    
    # Limpar e filtrar dados inválidos
    df['Nome da Variável'] = df['Nome da Variável'].astype(str).str.strip()
    df['Tipo de Dado'] = df['Tipo de Dado'].astype(str).str.strip()
    df['Tamanho'] = df['Tamanho'].apply(lambda x: str(x).strip() if pd.notnull(x) else '')
    
    # Remover linhas com nomes de variáveis inválidos
    df = df[df['Nome da Variável'].notna()]  # Remove NaN
    df = df[df['Nome da Variável'] != 'nan']  # Remove string 'nan'
    df = df[df['Nome da Variável'] != '']     # Remove vazias
    df = df[df['Nome da Variável'].str.len() > 0]  # Remove strings vazias
    
    print(f"Dicionário carregado: {len(df)} variáveis válidas")
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
    
    # Criar dicionário de tipos baseado no excel para lookup
    dict_tipos = {}
    for _, row in df_dict.iterrows():
        col = row['Nome da Variável']
        tipo = inferir_tipo_duckdb(row['Tipo de Dado'], row['Tamanho'])
        dict_tipos[col] = tipo
    
    # Usar APENAS as colunas do CSV como base
    for col in csv_cols:
        if col in dict_tipos:
            schema[col] = dict_tipos[col]
        else:
            # Coluna existe no CSV mas não no dicionário - usar TEXT
            logging.warning(f'Coluna do CSV não encontrada no dicionário: {col} - usando TEXT')
            schema[col] = 'TEXT'
    
    # Verificar se há colunas no dicionário que não estão no CSV
    ausentes = [c for c in dict_tipos.keys() if c not in csv_cols]
    for col in ausentes:
        logging.warning(f'Coluna prevista no dicionário ausente no CSV: {col}')
    
    # Não há colunas "extras" pois estamos usando apenas as do CSV
    extras = []
    
    print(f"Schema construído: {len(schema)} colunas baseadas no CSV")
    return schema, extras, ausentes

def conectar_duckdb(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return duckdb.connect(db_path)

def criar_tabela_destino(conn, table, schema):
    cols_def = ',\n  '.join([f'"{col}" {tipo}' for col, tipo in schema.items()])
    conn.execute(f'DROP TABLE IF EXISTS {table};')
    conn.execute(f'CREATE TABLE {table} (\n  {cols_def}\n);')

def executar_copy(conn, table, csv_path, schema):
    # Abordagem simplificada: deixar DuckDB detectar automaticamente as colunas
    temp_table = f"{table}_temp"
    conn.execute(f'DROP TABLE IF EXISTS {temp_table};')
    
    # Ler CSV diretamente para tabela temporária (DuckDB detecta colunas automaticamente)
    copy_cmd = f"CREATE TABLE {temp_table} AS SELECT * FROM read_csv_auto('{csv_path}', delim=';', header=true, nullstr=['', 'NA', 'NULL']);"
    print(f'Comando CREATE TABLE gerado:\n{copy_cmd}')
    
    try:
        conn.execute(copy_cmd)
        
        # Verificar as colunas reais da tabela temporária
        temp_cols_info = conn.execute(f"PRAGMA table_info('{temp_table}');").fetchall()
        temp_cols = [row[1] for row in temp_cols_info]  # Nome da coluna está na posição 1
        print(f'Colunas detectadas na tabela temporária: {temp_cols[:10]}... (total: {len(temp_cols)})')
        
        # Verificar as colunas da tabela de destino
        dest_cols_info = conn.execute(f"PRAGMA table_info('{table}');").fetchall()
        dest_cols = [row[1] for row in dest_cols_info]
        print(f'Colunas na tabela de destino: {dest_cols[:10]}... (total: {len(dest_cols)})')
        
        # Fazer mapeamento apenas das colunas que existem em ambas as tabelas
        col_mappings = []
        for col in dest_cols:
            if col in temp_cols:
                if col in schema:
                    target_type = schema[col]
                    if target_type in ['BIGINT', 'DOUBLE']:
                        col_mappings.append(f'CAST("{col}" AS {target_type}) AS "{col}"')
                    else:
                        col_mappings.append(f'"{col}"')
                else:
                    col_mappings.append(f'"{col}"')
            else:
                # Coluna não existe na temp - usar NULL
                col_mappings.append(f'NULL AS "{col}"')
        
        if col_mappings:
            insert_cmd = f'INSERT INTO {table} SELECT {", ".join(col_mappings)} FROM {temp_table};'
            print(f'Executando INSERT com conversão de tipos...')
            conn.execute(insert_cmd)
        else:
            print('Nenhuma coluna compatível encontrada!')
        
        # Limpar tabela temporária
        conn.execute(f'DROP TABLE {temp_table};')
        
    except Exception as e:
        logging.error(f'Erro na carga do CSV: {e}')
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
    import csv
    with open(args.csv, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        raw_cols = next(reader)
        print("Cabeçalho lido do CSV:", raw_cols[:10], "... total:", len(raw_cols))
        
        # Filtro robusto para colunas válidas
        csv_cols = []
        for i, col in enumerate(raw_cols):
            col_str = str(col).strip()
            # Aceita apenas colunas que:
            # 1. Não são vazias
            # 2. Não são 'nan' (case insensitive)
            # 3. Não são apenas espaços
            # 4. Têm pelo menos 1 caractere alfanumérico
            if (col_str and 
                col_str.lower() != 'nan' and 
                col_str != '' and
                any(c.isalnum() for c in col_str)):
                csv_cols.append(col_str)
            else:
                print(f"Coluna inválida ignorada na posição {i}: '{col}' (tipo: {type(col)})")
        
        print(f"Colunas válidas encontradas: {len(csv_cols)}")
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
