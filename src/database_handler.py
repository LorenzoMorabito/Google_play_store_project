import pandas as pd
from sqlalchemy import create_engine

class SQLHandler:
    """Classe per gestire il caricamento dei dati in un database SQL."""

    def __init__(self, original_data, conn_string, schema):
        self.original_data = original_data
        self.conn_string = conn_string
        self.schema = schema
        # creazione dell' istanza 'engine' per il database
        self.engine = create_engine(self.conn_string)

    def load_original_data(self, table_name):
        try:
            self.original_data.to_sql(table_name, 
                                        self.engine, 
                                        schema=self.schema, 
                                        if_exists='replace', 
                                        index=False)
        except Exception as error:
            print(f"Error loading original data: {error}")


    def close_connection(self):
        self.engine.dispose()