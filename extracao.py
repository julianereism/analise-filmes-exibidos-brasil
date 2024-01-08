import pandas as pd

class DataframeCreator:
  def __init__(self, csv_path):
    self.csv_path = csv_path

  def create_dataframe(self):
    #Lendo um arquivo csv com pandas
    try:
      dataframe = pd.read_csv(self.csv_path, encoding='utf-8', sep=',')
      return dataframe

    except FileNotFoundError:
      print(f"O arquivo não foi encontrado: {self.csv_path}")
      return None

    except pd.errors.EmptyDataError:
      print(f"O arquico está vazio: {self.csv_path}")
      return None

    except pd.errors.ParserError as e:
      print(f"Erro ao analisar o arquivo: {e}")
      return None

------------ 

      #Criar dataframe

csv_path = f'https://drive.google.com/uc?id=1EbowoS2EPIdPpZUSt6VZwLWSBLnw58Dk'

dataframe_creator = DataframeCreator(csv_path)
df = dataframe_creator.create_dataframe()

if df is not None:
  print("Dataframe criado com sucesso")
  print(df.head())
  print(df.shape)

-------------

import duckdb

class SQLExecutor:
    def __init__(self, df):
        self.df = df

    def execute_sql(self, sql_script):
        # Criar uma conexão DuckDB usando o DataFrame
        con = duckdb.connect(database=':memory:',read_only=False)

        # Registrar o DataFrame como uma tabela DuckDB
        con.register('table_dataframe', self.df)

        # Executar o script SQL na tabela registrada
        result_dataframe = con.execute(sql_script).fetchdf()

        return result_dataframe

--------------

        # Consulta SQL para selecionar colunas desejadas

sql_script = """
SELECT
    "Ano de exibição" as "Ano",
    "Gênero" as "Gênero",
    "País(es) produtor(es) da obra" as "País Produtor",
    "Nacionalidade da obra" as "Nacionalidade",
    "Empresa distribuidora" as "Distribuidora",
    "Origem da empresa distribuidora" as "Origem da Distribuidora",
    "Público no ano de exibição" as "Público",
    "Renda (R$) no ano de exibição" as "Bilheteria"
FROM
    table_dataframe
"""

sql_executor = SQLExecutor(df)
result_query = sql_executor.execute_sql(sql_script)

if result_query is not None:
  print(result_query.head())
  print(result_query.shape)


---------------

  class ParquetDataProcessor:
  def __init__(self, dataframe):
    self.dataframe = dataframe

  def save_to_parquet(self, output_file_path):
    try:
      self.dataframe.to_parquet(output_file_path, index=False)
      print(f"O Dataframe foi salvo em parquet: {output_file_path}")

    except Exception as e:
      print(f"Erro ao salvar o Dataframe em parquet: {e}")


---------------

     processor = ParquetDataProcessor(result_query)

processor.save_to_parquet('Filmes_Exibidos_2009_a_2019.parquet') 