import pandas as pd
import numpy as np


class DataProcessor:
    def __init__(self, dataframe=None):
        self.data = dataframe

    def clean_function(self):
        """
        Esegue una pulizia iniziale del dataframe:
        [Generale] rimuovendo duplicati
        [Object] rimozione caratteri speciali tranne lettere e numeri r'[^a-zA-Z0-9\s] .
        [Object] Casting delle variabili di tipo Object se all'interno delle variabili una % >= 95% sono scartteri numerici.
        
        """
        # Rimuovi le righe duplicate dal DataFrame
        self.data.drop_duplicates(inplace=True)
        
        # Itera su ogni colonna del DataFrame
        for series in self.data.columns:
            # Se il tipo di dato della colonna è 'object'
            if self.data[series].dtype == 'object':
                # Sostituisci i valori mancanti con la stringa 'mask'
                mask = self.data[series].isna()
                self.data.loc[mask, series] = 'mask'
                
                # Rimuovi i caratteri speciali dalla colonna
                self.data[series] = self.data[series].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
                
                # Calcola il 95° percentile della lunghezza della colonna
                percentile_value = 95
                soglia_value = int(np.percentile(self.data[series].dropna().index, percentile_value))
                
                # Se il numero di valori numerici nella colonna supera la soglia, converti la colonna in numerica
                if self.data[series].str.isnumeric().sum() > soglia_value:
                    self.data[series] = pd.to_numeric(self.data[series], errors='coerce')
                else:
                    pass
                # Ripristino dei valori nulli
                self.data.loc[mask, series] = None
    
    
    def converter(self, series, item1, item2, operation1={'pro': 1}, operation2={'pro': 1}):
        """ 
        Questa funzione effettua la conversione arrotondata alla seconda cifra decimale dei valori all'interno di una colonna
        
        Parameters:
        - series      = colonna del df 
        - item1       = 1 elemento della conversione
        - item2       = 2 elemento ""
        - operation 1 = operazione 1
        - operation 2 = operazione 2
        
        Returns:
        - None la funzione modifica i valori inplaces
        """
        # Dizionario di operazioni 
        op = {
            'add': lambda x, y: x + y,
            'sub': lambda x, y: x - y,
            'pro': lambda x, y: x * y,
            'div': lambda x, y: x / y
        }
        # estrapolo la coppia chiave valore da ogni dizionario e l'assegno a 2 variabili
        key1, num1 = list(operation1.items())[0]
        key2, num2 = list(operation2.items())[0]
        # Controllo che i parametri delle operazioni siano giuste          
        if key1 not in op or key2 not in op:
            raise ValueError('Operation not valid')
        # controllo che la colonna selezionata sia nel file 
        if series not in self.data.columns:
            raise ValueError('Columns not found')
            
        def conv(val):
            """ 
            Funzione di conversione. Questa funzione verra applicata alle colonne
            """
            # Convero il valore in stringa e lo assegno ad una variabile 
            val_str = str(val)
            # condizione: se item è all'interno della stringa
            if item1 in val_str:
                # applico la funzione lambda corrispondente all valore 
                return round(op[key1](float(val_str.replace(item1, '')), num1), 2)
                # lambda x, y: (x= (float(val_str.replace(item1, ''), y= num1)
            elif item2 in val_str:
                return round(op[key2](float(val_str.replace(item2, '')), num2), 2)
            else:
                return val
        #applico la funzione conv alla serie
        self.data[series] = self.data[series].apply(conv)
        
        
        
        
        
    def substitute(self, item, item_sub, scope='all', series=None):
        """ 
        Questa funzione sostituisce una serie di caratteri con un'altra serie di caratteri 
        all'interno di specificate colonne del DataFrame
        
        Parameters:
        - item:       lista di caratteri da sostituire
        - item_sub:   lista di caratteri con cui sostituire
                    Devono avere la stessa lunghezza
        - scope:      specifica dove effettuare la sostituzione. Valori accettati:
                    'all'  - su tutto il DataFrame
                    'some' - solo sulle colonne specificate in 'series'
        - series:     lista di nomi di colonne del DataFrame su cui effettuare la sostituzione 
                    (usato solo quando scope è 'some')
        
        Returns:
        - None: la funzione modifica il DataFrame inplace
        
        Raises:
        - ValueError: se c'è una discrepanza nella lunghezza tra 'item' e 'item_sub', 
                    se 'scope' non è valido, o se una colonna in 'series' non è nel DataFrame.
        """
        
        # Controllo della lunghezza degli elementi
        if len(item) != len(item_sub):
            raise ValueError('Mismatch in length between items and substitutes.')

        # Verifica che scope sia 'all' o 'some'
        if scope not in ['all', 'some']:
            raise ValueError(f'Invalid scope value: {scope}')

        # Determina le colonne su cui operare in base allo "scope" e alla "series" fornita
        if scope == 'all':
            columns_to_modify = self.data.columns
        else:  # scope è 'some'
            if not series:
                raise ValueError('For scope "some", a series must be provided.')
            
            # Verifica che tutte le colonne in 'series' siano nel DataFrame
            for col in series:
                if col not in self.data.columns:
                    raise ValueError(f'Column "{col}" not found in the DataFrame.')
            
            columns_to_modify = series

        # Applica le sostituzioni alle colonne selezionate
        for column in columns_to_modify:
            for obj, sub in zip(item, item_sub):
                self.data[column] = self.data[column].replace(obj, sub)
    
    
    
        
    def casting(self, series, tipe):
        """
        Effettua il casting di specifiche colonne del DataFrame.

        Parameters:
        - series : list
            Lista di colonne del DataFrame da modificare.
        - dtype : list
            Lista dei tipi di dati desiderati per ogni colonna (come stringhe).
            
        Returns:
        - None
            Modifica il DataFrame in-place.
        """
        # Definisco le varie funzioni per la conversione 
        def to_date(col):
            return pd.to_datetime(self.data[col])

        def to_object(col):
            return self.data[col].astype(object)

        def to_float(col):
            return self.data[col].astype(float)

        def to_int(col):
            return self.data[col].astype(int)
        
        # Creazione di un dizionario dove si fa riferimento alle funzioni 
        type_dit = {
            'date': to_date,
            'object': to_object,
            'float': to_float,
            'int': to_int
        }
        
        # Controllo della lungezza tra series e dtype
        if len(series) != len(tipe):
            raise ValueError('Length mismatch between series and tipe lists.')            
        
        for col, operation  in zip(series, tipe):
            
            # Controllo se le colonne inserite sono realmente nel dataframe
            if col not in self.data.columns:
                raise ValueError(f'Column "{col}" not found in the DataFrame.')
            
            # Controllo se il type desiderato e all'interno delle funzioni di casting 
            if operation not in type_dit.keys():
                raise ValueError(f'Error. Not find type: {operation}')
            
            # Effettuo l'operazione di casting in base al parametro passato e la relativa funzione 
            try:
                self.data[col] = type_dit[operation](col)
            except:
                raise ValueError(f'Error in casting column {col} to type {operation}')
                
                    
                    
                    
                    
                    
                    
    
    def imputator(self, series, value):
        """
        Imputa i valori mancanti in specifiche colonne del DataFrame.

        Parameters:
        - series : list
            Lista di colonne da imputare 
        - value : list di stringhe 
            Lista di strategie da usare per l'imputazione 
        """
        for ser, val in zip(series, value):
        
            # Verifica che la colonna esista nel DataFrame
            if ser not in self.data.columns:
                raise ValueError(f'Column "{ser}" not found in DataFrame')
            
            if val == 'mean':
                # controlla che la colonna sia numerica 
                if self.data[ser].dtype in (int, float):
                    val = self.data[ser].mean()
                else:
                    raise ValueError(f'Column "{ser}" is not of numeric')
                
            elif val == 'median':
                if self.data[ser].dtype in (int, float):
                    val = self.data[ser].median()
                else:
                    raise ValueError(f'Column "{ser}" is not of numeric')
                
            elif val == 'mode':
                val = self.data[ser].mode()[0]

            # Imputa con il parametro passato
            self.data[ser] = self.data[ser].fillna(val)
    
    
    
    def sort_data(self, column_order, ascending=True):
        """
        Ordina il DataFrame in base a specifiche colonne.

        Parameters:
        - column_order : list
            Colonna su cui ordinare il DataFrame.
        - ascending : bool or list of bool, default True
            Se True, ordina in modo crescente, altrimenti in modo decrescente.
            Può essere una lista di valori booleani se si ordinano più colonne.
        """
        # Se il valore di ascending e in singolo valore booleano 
        if isinstance(ascending, bool):
            # Converto ascending in una lista di valori lunga quanto n colonne 
            ascending = [ascending] * len(column_order)        
        
        if len(ascending) < len(column_order):
            i = len(column_order) - len(ascending)
            for n in range(i):
                place_order = ascending[0]
                ascending.append(place_order)
        
        # Controlla che ogni colonna in 'column_order' esista nel DataFrame
        for col in column_order:
            if col not in self.data.columns:
                raise ValueError(f"Column '{col}' not found in the DataFrame.")
        
        for item in ascending:
            if type(item) != bool:
                raise ValueError(f"Expected boolean values in 'ascending': {type(item)}.")
        
        # Ordina il DataFrame
        self.data.sort_values(by=column_order, ascending=ascending, inplace=True)
        
        
    def dropper_axis(self, item, axis= 0):
        """
        Elimina una colonna o una riga dal DataFrame in base al valore fornito e all'asse.
        
        Args:
            item: Nome della colonna o indice della riga da eliminare. 
            axis (int): 0 per eliminare righe, 1 per eliminare colonne.
            
        """
        if axis == 1:
            if item in self.data.columns:
                self.data.drop(columns=[item], inplace= True)
            else:
                raise ValueError(f"Column: {item} not found")
        elif axis == 0:
            if item in self.data.index:
                self.data.drop(index= item, inplace= True)
            else:
                raise ValueError(f"Column: {item} not found")
        else:
            raise ValueError("Invalid axis value. Use 0 for rows and 1 for columns.")