from csv_download import csv_download
from handler_df import handler_df
from conexionDB import conexionDB
from decouple import config
import pandas as pd

"""Reformatea a 1 y 0 toda una columna de un df particular"""
def cleaning_column_1_0(df, culumn):
    for i in range(len(df)):
        if not pd.notnull(df.loc[i, culumn]):
            df.loc[i, culumn] = 0
        else:
            df.loc[i, culumn] = 1


if __name__ == "__main__":
    # crear las clases de csv_download para cada csv
    csv_museos = csv_download(
        config("URL_MUSEOS"),
        "Museos")
    csv_cines = csv_download(
        config("URL_CINES"),
        "Salas de Cine")
    csv_bibliotecas = csv_download(
        config("URL_BIBLIOTECAS_POPULARES"),
        "Bibliotecas Populares")

    # descargar los csv y obtener los paths de cada uno
    path_museos = csv_museos.download_explicit_date()
    path_cines = csv_cines.download_explicit_date()
    path_bibliotecas = csv_bibliotecas.download_explicit_date()

    # -----------------------------------------------------

    # crear clase para la importacion y limpieza de los DF
    handler_df_museos = handler_df(path_museos)
    handler_df_cines = handler_df(path_cines)
    handler_df_bibliotecas = handler_df(path_bibliotecas)

    # DataFrames crudos
    df_museos = handler_df_museos.get_df()
    df_cines = handler_df_cines.get_df()
    df_bibliotecas = handler_df_bibliotecas.get_df()

    # A continuacion se generaran 3 tablas con los datos de cada csv

    """
    TABLA: Se normalizaran toda la información de Museos, Salas de Cine y
    Bibliotecas Populares, para crear una única tabla que contenga:
        cod_localidad
        id_provincia
        id_departamento
        categoria
        provincia
        localidad
        nombre
        domicilio
        código_postal
        número_de_teléfono
        mail
        web
    """
    # columnas estandarizadas para los 3 csv
    select_columns = ["cod_loc", "idprovincia", "iddepartamento", "categoria",
                      "provincia", "localidad", "nombre", "domicilio", "cp",
                      "cod_area", "telefono", "mail", "web"]

    # limpiando el df museos
    handler_df_museos.rename_columns("direccion", "domicilio")
    handler_df_museos.select_columns(select_columns)
    handler_df_museos.run_cleanup(["telefono", "cod_area"], "cod_area",
                                  "telefono", "mail", "cp", "web")

    # limpiando el df cines
    handler_df_cines.rename_columns("direccion", "domicilio")
    handler_df_cines.select_columns(select_columns)
    handler_df_cines.run_cleanup(["telefono", "cod_area"], "cod_area",
                                 "telefono", "mail", "cp", "web")

    # limpiando el df bibliotecas
    handler_df_bibliotecas.rename_columns("cod_tel", "cod_area")
    handler_df_bibliotecas.select_columns(select_columns)
    handler_df_bibliotecas.run_cleanup(["telefono", "cod_area"], "cod_area",
                                       "telefono", "mail", "cp", "web")

    # Tabla 1: Se crea una tabla con todos los datos de los 3 csv
    df_tabla1 = pd.concat([handler_df_bibliotecas.get_df(),
                           handler_df_cines.get_df(),
                           handler_df_museos.get_df()])

    # Renombrando columnas
    df_tabla1.columns = [
        "cod_localidad",
        "id_provincia",
        "id_departamento",
        "categoria",
        "provincia",
        "localidad",
        "nombre",
        "domicilio",
        "codigo_postal",
        "numero_de_telefono",
        "mail",
        "web"]
    print(list(df_tabla1.columns))

    """
    TABLA: tablas con la siguiente informacion:
        Cantidad de registros totales por categoría
        Cantidad de registros totales por fuente
        Cantidad de registros por provincia y categoría

    Debido a que tenemos muchas fuentes, muchas provincias y 3 categorias,
    ingenuamente pareceria que se resolveria como una tabla de una sola fila
    con numeros, lo cual no tiene sentido si luego se lo exporta a una BD sql.
    Por eso, se crearan 3 tablas cada una que represente uno de los 3 puntos y
    luego se les aplicara un join entre dos de ella y la resultante se le
    hara un producto carteciano con la faltante, lo que quedaria como una tabla
    totalmente "desnormalizada" pero que tiene sentido a la hora de importarla
    a una base de datos relaciona.
    """

    # Cantidad de registros totales por categoría
    df_categoría = df_tabla1["categoria"].value_counts()
    df_categoría = pd.DataFrame(
        df_tabla1["categoria"].value_counts()).reset_index()
    df_categoría.columns = ["categoria", "cantidad_registros_categoria"]

    # Cantidad de registros totales por fuente
    df_fuentes = pd.concat([(df_museos.loc[:, ["fuente"]]),
                            (df_cines.loc[:, ["fuente"]]),
                            (df_bibliotecas.loc[:, ["fuente"]])])
    df_fuentes = pd.DataFrame(df_fuentes.value_counts()).reset_index()
    df_fuentes.columns = ["fuente", "cantidad_registros_fuente"]

    # Cantidad de registros por provincia y categoría
    df_provincia_categoria = pd.DataFrame({
        'cantidad_registros_prov_categ':
            df_tabla1.groupby(["provincia", "categoria"]).size()
    }).reset_index()

    # debido a que categoria y provincia_categoria tienen una columna
    # con el mismo proposito ("categoria"), se decidio usar un merge join
    df_tabla2 = df_categoría.merge(df_provincia_categoria, on="categoria")

    # aqui con fuentes se utilizaran cross join (producto cartesiano)
    df_tabla2 = df_tabla2.merge(df_fuentes, how='cross')

    """
    Tabla: Se procesara la información del csv cines para poder crear una tabla
    que contenga:
        Provincia
        Cantidad de pantallas
        Cantidad de butacas
        Cantidad de espacios INCAA

    Decision de diseño, normalizaremos la columna "espacio_incca" para que
    donde existan nan seran 0, analizando pedazos de la informacion de
    la columna se colocan strings afirmativos, lo que indicara finalmente que
    es esta columna un 1 es que si existen espacio incca y un 0 un no.
    """

    df_tabla3 = df_cines.loc[:, ["provincia", "pantallas", "butacas",
                                 "espacio_incaa"]]
    cleaning_column_1_0(df_tabla3, "espacio_incaa")
    df_tabla3 = df_tabla3.groupby(["provincia"]).agg(
        {"pantallas": "sum",
         "butacas": "sum",
         "espacio_incaa": "sum"}).reset_index()

    df_tabla3.columns = ["provincia", "cantidad_de_pantallas",
                         "cantidad_de_butacas", "cantidad_de_espacios_incaa"]

    # Se cargan los datos a una base  de datos postgresql con
    # sqlAlchemy

    conexionDB_postgres = conexionDB()
    # tabla1
    conexionDB_postgres.insert_update_table_with_csv(
        "centros_cultura", df_tabla1)
    # tabla2
    conexionDB_postgres.insert_update_table_with_csv("cantidades", df_tabla2)
    # tabla3
    conexionDB_postgres.insert_update_table_with_csv(
        "info_cines_provincia", df_tabla3)

    print("fin")
