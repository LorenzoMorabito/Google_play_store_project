import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class DataVisualizer:                                                 
    def __init__(self, dataframe, style="classic"):
        
        """Inizializza la classe DataVisualizer."""
        
        if not isinstance(dataframe, pd.DataFrame):
            raise ValueError("The provided data is not a DataFrame.")
        
        self.db = dataframe
        self.style = style

    def barplot(self, xb, yb, save_path=None):
        
        """Visualizza un barplot."""
        
        # Controllo esistenza colonne
        if xb not in self.db.columns or yb not in self.db.columns:
            raise ValueError(f"Columns {xb} or {yb} not found in the DataFrame.")
        
        plt.style.use(self.style)
        
        # Calcolo dell'ordine per il barplot
        order = self.db[[xb, yb]].groupby(yb).mean().sort_values(by=xb, ascending=False).index
        
        sns.barplot(data=self.db, x=self.db[xb], y=self.db[yb], order=order)
        plt.xticks(rotation=90)
        
        # Salvataggio grafico 
        if save_path:
            plt.savefig(save_path + f'correlation_between_{xb}_and_{yb}.png')
        plt.show()
        

    def correlation(self, columns=None, cmap="coolwarm", save_path=None):
        
        """Visualizza una heatmap di correlazione."""
        
        if columns is None:
            columns = [col for col in self.db.columns if self.db[col].dtype == "float"]
        
        plt.style.use(self.style)
        sns.heatmap(self.db[columns].corr(), annot=True, cmap=cmap)
        
        
        # Salvataggio grafico 
        if save_path:
            plt.savefig(save_path + f'correlation_between_{xb}_and_{yb}.png')
        plt.show()



