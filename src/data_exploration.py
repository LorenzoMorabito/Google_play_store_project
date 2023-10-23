import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class DataExploration:
    """Classe per esplorare e visualizzare la distribuzione dei dati."""
    
    def __init__(self, dataframe):
        """Inizializza la classe DataExploration.
        
        Args:
            dataframe (DataFrame): DataFrame da esplorare.
        """
        self.db = dataframe

    def distribution(self, columns, save_path_prefix=None):
        """Visualizza la distribuzione di specifiche colonne.
        
        Args:
            columns (list): Lista di colonne da esplorare.
            save_path_prefix (str, optional): Prefisso del percorso per salvare i grafici. Se None, i grafici non vengono salvati.
        """
        for col in columns:
            print(self.db[col].describe())
            
            if self.db[col].dtype == "object":
                # Calcolo dell'ordine in base alla frequenza                
                plt.figure()
                order = self.db[col].value_counts().index
                sns.countplot(data=self.db, x=self.db[col], order= order)
                plt.xticks(rotation=90)
                if save_path_prefix:
                    plt.savefig(f"{save_path_prefix}{col}_countplot.png")
                plt.show()

            elif self.db[col].dtype == "float64":
                plt.figure()
                plt.hist(self.db[col], bins=40)
                if save_path_prefix:
                    plt.savefig(f"{save_path_prefix}{col}_histplot.png")
                plt.show()
