from sqlalchemy import create_engine, exc
from sqlalchemy_utils import database_exists, create_database
from decouple import config
import pandas as pd
import logging


"""
Es necesario modificar las variables de entorno si usted desea conectarse
a una bd de su preferencia.
"""



logger = logging.getLogger(__name__)

class conexionDB:
    try:
        _USERNAME = config("POSTGRES_USER")
        _PASSWORD = config("POSTGRES_PASS")
        _HOST = config("POSTGRES_HOST")
        _PORT = config("POSTGRES_PORT")
        _DB = config("POSTGRES_DB")
    except ValueError as e:
        raise SystemExit(f"Error Variable Entorno: \n" + str(e))

    def __init__(self):
        """Inicializador de la clase: almacena como atributo la url de
        una bd postgresql y una engine para manipularla la misma.
        si la db no existe, la crea y ejecuta un script .sql en una
        ruta en particular
        """
        self.url = (f"postgresql://{self._USERNAME}:{self._PASSWORD}@"
                    f"{self._HOST}:{self._PORT}/{self._DB}")
        self.engine = None
        logger.debug(f"Conectando a la base de datos: {self.url}")
        try:
            if not database_exists(self.url):
                logger.info("La bd no existe, se procede a crearla")
                create_database(self.url)
                self.engine = create_engine(self.url)
                with open(config("PATH_SQL_DDL"), 'r') as ddl:
                    with self.engine.connect() as conn:
                        result = conn.execute(ddl.read())

            else:
                self.engine = create_engine(self.url)
                logger.debug("Conexion a la bd lista")
        except exc.DisconnectionError as e:
            logger.exception(str(e))
            raise SystemExit(e)
        except exc.SQLAlchemyError as e:
            logger.exception(str(e))
            raise SystemExit(e)
        except OSError as e:
            logger.exception(str(e))
            raise SystemExit(e)

    def get_engine(self):
        """Obtener el engine de la bd"""
        return self.engine

    def close_engine(self):
        """cerrar el engine de la bd"""
        self.engine.dispose()

    def insert_update_table_with_csv(self, table_name, path_csv):
        """Eliminar todos los datos de una tabla en un schema particular
        y almacenar los datos guardados en un csv.
        Args:
            table_name: nombre de la tabla done almacenar la nueva info
            path_csv: ruta de la ubicacion del archivo .csv"""
        logger.debug(f"insertando df en la tabla: {table_name}")
        try:
            # leer csv
            df = pd.read_csv(path_csv) if isinstance(
                path_csv, str) else path_csv
            # eliminar todas las columnas de la tabla
            self.engine.execute(
                f"DELETE FROM tablas_cultura.{table_name}")
            df.to_sql(
                table_name,
                self.get_engine(),
                schema="tablas_cultura",
                if_exists='append',
                index=False)
        except exc.SQLAlchemyError as e:
            logger.exception(str(e))
            raise SystemExit(e)
        except OSError as e:
            logger.exception(str(e))
            raise SystemExit(e)
        except pd.errors.EmptyDataError as e:
            logger.error(f"El archivo en path: {self.path_csv}"
                        "No existe o esta vacio")
            raise SystemExit(f"Error empty data in path {self.path_csv} \n" +
                             str(e))


if __name__ == "__main__":
    conexion = conexionDB()
