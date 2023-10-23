# Importazione delle librerie necessarie
import pandas as pd
from afinn import Afinn

class DataAnalyser:
    def __init__(self, dbreview):
        """
        Inizializza la classe DataAnalyser.
        """
        self.afin = None
        self.dbreview = dbreview

    def building_afinn(self, language="en"):
        """Crea un oggetto Afinn.
        
        Args:
            language (str): Lingua testi. Default: 'en'.
        """
        self.afin = Afinn(language=language)

    def add_word(self, terminipositivi_path, termininegativi_path):
        """Aggiunge parole personalizzate all'oggetto Afinn.
        
        Args:
            terminipositivi_path (str): Percorso del file con termini positivi.
            termininegativi_path (str): Percorso del file con termini negativi.
        
        Returns:
            dict: Dizionario aggiornato di Afinn.
        """
        terp = pd.read_excel(terminipositivi_path, header=None)
        dictpos = {x: 2 for x in terp[0]}
        
        tern = pd.read_excel(termininegativi_path, header=None)
        dictneg = {x: -2 for x in tern[0]}
        
        for word, score in dictpos.items():
            self.afin._dict[word] = score
        
        for word, score in dictneg.items():
            self.afin._dict[word] = score
            
        return self.afin._dict

    def sentiment_score(self):
        """Calcola il punteggio del sentimento per ogni recensione."""
        scores = [self.afin.score(x) for x in self.dbreview["Translated_Review"]]
        self.dbreview["scoresentiment"] = scores
        return self.dbreview

    def sentiment_update(self):
        """Aggrega i punteggi del sentimento e li unisce con il dataframe dboriginale."""
        sentiment_avg = self.dbreview[["App", "scoresentiment"]].groupby("App").mean()
        dboriginalemarged = self.dbreview.merge(sentiment_avg, on="App", how="inner")
        return dboriginalemarged
