import pandas as pd
import os


class DataIngressor:
    def __init__(self):
        # Inizializzazione della variabile 'data' che conterrà i dati caricati.
        self.data = None

    def load_file(self, path, format=None):
        """
        Carica un file in base al formato fornito o dedotto dall'estensione del file.
        
        Args:
            path (str): Percorso del file da caricare.
            format (str): Formato del file. Se non fornito, viene dedotto dall'estensione del file.
        """
        # Se il formato non viene fornito, lo deduciamo dall'estensione del file.
        if not format:
            _, path_ext = os.path.splitext(path)
            format = path_ext[1:]

        # A seconda del formato fornito, carichiamo i dati utilizzando le funzioni appropriate di pandas.
        if format == 'pickle':
            self.data = pd.read_pickle(path)
        elif format == 'csv':
            self.data = pd.read_csv(path)
        elif format == 'xlsx':
            self.data = pd.read_excel(path)
        else:
            print(f'Invalid file format {format}')

    def save_file(self, path, format='csv', index=False):
        """
        Salva i dati nella variabile 'data' in un file in base al formato fornito.
        
        Args:
            path (str): Percorso in cui salvare il file.
            format (str): Formato del file. Default è 'csv'.
            index (bool): Se salvare l'indice dei dati nel file. Default è False.
        """
        # A seconda del formato fornito, salviamo i dati utilizzando le funzioni appropriate di pandas.
        if format == 'pickle':
            self.data.to_pickle(path)
        elif format == 'csv':
            self.data.to_csv(path, index=index)
        elif format == 'xlsx':
            self.data.to_excel(path, index=index)
        else:
            raise ValueError(f'Not valid format {format}')

    def out_data(self):
        """
        Ritorna i dati completi salvati nella variabile 'data'.
        
        Returns:
            pd.DataFrame: Dati caricati.
        """
        return self.data

    def series_view(self, series):
        """
        Ritorna una serie di dati specifica dalla variabile 'data'.
        
        Args:
            series (str): Nome della serie da visualizzare.
            
        Returns:
            pd.Series: Serie di dati richiesta.
        """
        return self.data[series]