from unidecode import unidecode
import pandas as pd
import numpy as np


class handler_df:
    def __init__(self, path_csv):
        """Inicializador de la clase: lee el archivo csv en la ruta del
        argumento y lo guarda en un atributo como dataframe
        Args:
            path_csv: Ruta del archivo csv en formato string
        """
        self.path_csv = path_csv
        try:
            self._df = pd.read_csv(path_csv)
        except pd.errors.EmptyDataError as e:
            SystemExit(f"Error empty data in path {self.path_csv} \n" + str(e))
        self._df.columns = [unidecode(x.lower()) for x in self._df.columns]

    def select_columns(self, list_columns):
        """Selecciona las columnas que se desean conservar en el dataframe"""
        self._df = pd.DataFrame(self._df, columns=list_columns)

    def rename_columns(self, old_column, new_column):
        """Renombra una columna existente del dataframe"""
        if old_column in list(self._df.columns):
            self._df.rename(columns={old_column: new_column}, inplace=True)

    def get_df(self):
        """Devuelve el dataframe"""
        return self._df

    def join_columnA_to_columnB(self, colA, colB, i):
        """Unifica las columnas colA y colB en la columna colB
        WARNING: la columna colA NO se elimina del dataframe
        Args:
            colA: Nombre de la columna a unificar en la columna colB
            colB: Nombre de la columna B a la que se unifica la columna colA
        """
        if pd.notnull(self._df.loc[i, colA]) & pd.notnull(
                self._df.loc[i, colB]):
            data_A = str(self._df.loc[i, colA])
            data_B = str(self._df.loc[i, colB])
            self._df.loc[i, colB] = data_A + " " + data_B

    def numbers_to_string(self, list_column, i):
        """Estandarizar una columna que contiene numeros en formato string,
        por ejemplo: 1000 a '1000', '411-50020' a '41150202',
        si contiene un dato que no sea un numero se coloca NaN
        Args:
            list_column: lista de columnas
            i: numero de fila de la columna
        """
        for j in list_column:
            if isinstance(self._df.loc[i, j], str):
                try:
                    self._df.loc[i, j] = str(
                        int(self._df.loc[i, j].replace(" ", "")))
                except ValueError as e:
                    self._df.loc[i, j] = np.nan
                    continue

            elif isinstance(self._df.loc[i, j], int):
                self._df.loc[i, j] = str(self._df.loc[i, j])

            elif pd.notnull(self._df.loc[i, j]):
                try:
                    self._df.loc[i, j] = str(int(self._df.loc[i, j]))
                except ValueError as e:
                    self._df.loc[i, j] = np.nan
                    continue

            else:
                self._df.loc[i, j] = np.nan

    def format_email(self, column, i):
        """Verifica el formato correcto minimo de un email, no su existencia"""
        if pd.notnull(self._df.loc[i, column]):
            fractured = self._df.loc[i, column].split("@")
            if len(fractured) < 2:
                self._df.loc[i, column] = np.nan

    def format_cp_AR(self, column, i):
        """Verifica el formato correcto detallado de un codigo postal,
        no su existencia
        """
        cp = str(self._df.loc[i, column])
        if len(cp) == 4:
            try:
                int(cp)
            except ValueError as e:
                self._df.loc[i, column] = np.nan
                return
            self._df.loc[i, column] = cp

        elif len(cp) == 8:
            if isinstance(cp[0], str) and isinstance(cp[-3:], str):
                try:
                    int(cp[1:5])
                except ValueError as e:
                    self._df.loc[i, column] = np.nan
                    return
                self._df.loc[i, column] = cp
        else:
            self._df.loc[i, column] = np.nan

    def cleaned_sd(self, i, column):
        """"Elimina los campos "s/d" de una columna en una fila i particular"""
        if self._df.loc[i, column] == "s/d":
            self._df.loc[i, column] = np.nan

    def run_cleanup(self, list_nts, columnAjoin, columnBjoin, columnEmail,
                    columnCp, columnWeb):
        """ Ejecuta las funciones numbers_to_string, join_columnA_to_columnB,
        format_email, format_cp_AR, cleaned_sd sobre todos los datos en
        columnas particulares del df almacenado en el atributo _df
        Args:
            list_nts: columna de numeros en string
            columnAjoin: columna a unificar en la columnBjoin
            columnBjoin: columna donde se guardara la unificacion
            columnEmail: columna donde hay emails en formato string
            columnCp: columna donde hay codigos postales argentinos
            columnWeb: columnas donde hay paginas webs en string
        """
        for i in range(len(self._df)):
            self.numbers_to_string(list_nts, i)
            self.join_columnA_to_columnB(columnAjoin, columnBjoin, i)
            self.format_email(columnEmail, i)
            self.format_cp_AR(columnCp, i)
            self.cleaned_sd(i, columnWeb)
        self._df = self._df.drop(columns=[columnAjoin], axis=1)
