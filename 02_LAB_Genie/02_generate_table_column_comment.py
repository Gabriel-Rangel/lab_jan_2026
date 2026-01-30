# Databricks notebook source
# MAGIC %md
# MAGIC # Geração Automática de Comentários de Colunas com AI_GEN
# MAGIC
# MAGIC Este notebook demonstra como utilizar a função **`AI_GEN`** do Databricks para **gerar automaticamente descrições e comentários de colunas** em tabelas do Unity Catalog.  
# MAGIC O objetivo é **acelerar o processo de documentação de dados**, garantindo maior clareza e padronização nos catálogos.
# MAGIC
# MAGIC ## Importante
# MAGIC - As descrições e comentários gerados aqui são **apenas exemplos** produzidos por inteligência artificial.  
# MAGIC - Os resultados devem ser **revisados e adaptados** pelo usuário antes de serem aplicados em um ambiente produtivo.  
# MAGIC
# MAGIC > **Atenção**: Os comentários gerados automaticamente devem ser considerados um **ponto de partida**. A curadoria manual é essencial para garantir qualidade e conformidade.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definição do Catálogo e Schema onde serão aplicados os comentários para todas as tabelas

# COMMAND ----------

catalog_name = "bbts"
schema_name = "inadimplencia"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerando comentários para cada tabela e coluna através da `AI_GEN`

# COMMAND ----------

def sanitize_text(text):
    """Remove aspas simples que podem quebrar o comando SQL"""
    if text:
        return text.replace("'", "")
    return ""

def generate_table_comment(table_name):
    prompt = f'''
    Descreva de forma objetiva a finalidade da tabela **{table_name}**, considerando o contexto de um banco digital.
    A descrição deve ser clara, útil para analistas de dados e limitada a no máximo 256 caracteres.
    Não inicie com "A tabela...", seja direto. Não utilize aspas no texto.
    Inclua apenas o comentário, sem explicações adicionais.
    '''
    try:
        # Chama a função AI
        result_df = spark.sql(f"SELECT ai_gen('{prompt}') AS comment")
        comment = result_df.collect()[0]['comment']
        return sanitize_text(comment)
    except Exception as e:
        return f"Descrição indisponível: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicando os comentários gerados em cada tabela e coluna

# COMMAND ----------

from pyspark.sql import Row

# 3.1 Buscar lista de tabelas
tables_query = f"""
SELECT table_name
FROM system.information_schema.tables
WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}'
"""
tables_df = spark.sql(tables_query)
table_results = []

print("--- Iniciando Documentação de Tabelas ---")

# 3.2 Iterar e aplicar comentários
for row in tables_df.collect():
    table_name = row.table_name
    print(f"Processando tabela: {table_name}...")
    
    # Gera comentário
    comment = generate_table_comment(table_name)
    
    # Aplica no Unity Catalog
    sql_stmt = f"COMMENT ON TABLE {catalog_name}.{schema_name}.{table_name} IS '{comment}'"
    spark.sql(sql_stmt)
    
    table_results.append(Row(
        object_type="Table",
        name=table_name,
        comment=comment,
        status="Success"
    ))

print("--- Tabelas Concluídas ---")

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, expr
from concurrent.futures import ThreadPoolExecutor

# 1. Definição da Query Base
columns_query = f"""
SELECT table_name, column_name, data_type
FROM system.information_schema.columns
WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}'
"""
columns_df = spark.sql(columns_query)

# 2. Construção do Prompt dentro do DataFrame (Vetorizado)
prompt_expr = concat(
    lit("Descreva de forma objetiva a finalidade da coluna "), col("column_name"),
    lit(" na tabela "), col("table_name"),
    lit(", considerando o contexto de um banco digital. Tipo de dado: "), col("data_type"),
    lit(". A descrição deve ser clara, útil para analistas, max 256 chars. "),
    lit("Não repita nome/tipo, sem aspas. Apenas o texto da descrição.")
)

df_with_prompt = columns_df.withColumn("prompt", prompt_expr)

# 3. Geração dos Comentários em Paralelo (Distributed Inference)
print("--- Gerando descrições com IA (Paralelo) ---")
df_result = df_with_prompt.withColumn("generated_comment", expr("ai_gen(prompt)"))

# 4. Coletar os resultados já processados
rows_to_update = df_result.select("table_name", "column_name", "generated_comment").collect()

print(f"--- Aplicando {len(rows_to_update)} alterações no Unity Catalog ---")

# 5. Aplicação do SQL (Threading no Driver)
def run_alter_command(row):
    try:
        # Sanitização básica
        safe_comment = row.generated_comment.replace("'", "")
        
        sql_stmt = f"""
        ALTER TABLE {catalog_name}.{schema_name}.{row.table_name}
        ALTER COLUMN {row.column_name} COMMENT '{safe_comment}'
        """
        spark.sql(sql_stmt)
        
        # Retorna objeto Row para o relatório final
        return Row(
            object_type="Column", 
            name=f"{row.table_name}.{row.column_name}", 
            comment=safe_comment, 
            status="Success"
        )
    except Exception as e:
        return Row(
            object_type="Column", 
            name=f"{row.table_name}.{row.column_name}", 
            comment=f"Error: {str(e)}", 
            status="Failed"
        )

column_results = []
# Executa 10 ALTER TABLE simultâneos
with ThreadPoolExecutor(max_workers=10) as executor:
    column_results = list(executor.map(run_alter_command, rows_to_update))

print("--- Colunas Concluídas ---")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Relatório Final

# COMMAND ----------

all_results = table_results + column_results
final_df = spark.createDataFrame(all_results)

display(final_df)