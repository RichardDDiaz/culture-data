from unidecode import unidecode
import pandas as pd
import numpy as np


class handler_df:
    def __init__(self, path_csv):
        self.path_csv = path_csv
        try:
            self._df = pd.read_csv(path_csv)
        except pd.errors.EmptyDataError as e:
            SystemExit(f"Error empty data in path {self.path_csv} \n" + str(e))
        self._df.columns = [unidecode(x.lower()) for x in self._df.columns]

    def select_columns(self, list_columns):
        self._df = pd.DataFrame(self._df, columns=list_columns)

    def rename_columns(self, old_column, new_column):
        if old_column in list(self._df.columns):
            self._df.rename(columns={old_column: new_column}, inplace=True)

    def get_df(self):
        return self._df

    def join_columnA_to_columnB(self, colA, colB, i):
        if pd.notnull(self._df.loc[i, colA]) & pd.notnull(
                self._df.loc[i, colB]):
            data_A = str(self._df.loc[i, colA])
            data_B = str(self._df.loc[i, colB])
            self._df.loc[i, colB] = data_A + " " + data_B

    def numbers_to_string(self, list_column, i):
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
        if pd.notnull(self._df.loc[i, column]):
            fractured = self._df.loc[i, column].split("@")
            if len(fractured) < 2:
                self._df.loc[i, column] = np.nan

    def format_cp_AR(self, column, i):
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
        if self._df.loc[i, column] == "s/d":
            self._df.loc[i, column] = np.nan

    def run_cleanup(self, list_nts, columnAjoin, columnBjoin, columnEmail,
                    columnCp, columnWeb):
        for i in range(len(self._df)):
            self.numbers_to_string(list_nts, i)
            self.join_columnA_to_columnB(columnAjoin, columnBjoin, i)
            self.format_email(columnEmail, i)
            self.format_cp_AR(columnCp, i)
            self.cleaned_sd(i, columnWeb)
        self._df = self._df.drop(columns=[columnAjoin], axis=1)
