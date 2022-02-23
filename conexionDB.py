from sqlalchemy import create_engine, exc
from sqlalchemy_utils import database_exists, create_database
from decouple import config
import pandas as pd


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
        self.url = (f"postgresql://{self._USERNAME}:{self._PASSWORD}@"
                    f"{self._HOST}:{self._PORT}/{self._DB}")
        self.engine = None
        try:
            if not database_exists(self.url):
                create_database(self.url)
                self.engine = create_engine(self.url)
                with open(config("PATH_SQL_DDL"), 'r') as ddl:
                    with self.engine.connect() as conn:
                        result = conn.execute(ddl.read())

            else:
                self.engine = create_engine(self.url)
        except exc.DisconnectionError as e:
            raise SystemExit(e)
        except exc.SQLAlchemyError as e:
            raise SystemExit(e)
        except OSError as e:
            raise SystemExit(e)

    def get_engine(self):
        return self.engine

    def close_engine(self):
        self.engine.dispose()

    def insert_update_table_with_csv(self, table_name, path_csv):
        try:
            # leer csv
            df = pd.read_csv(path_csv) if isinstance(
                path_csv, str) else path_csv
            # eliminar todas las columnas de la tabla
            self.engine.execute(f"DELETE FROM tablas_cultura.{table_name}")
            df.to_sql(
                table_name,
                self.get_engine(),
                schema="tablas_cultura",
                if_exists='append',
                index=False)
        except exc.SQLAlchemyError as e:
            raise SystemExit(e)
        except OSError as e:
            raise SystemExit(e)
        except pd.errors.EmptyDataError as e:
            raise SystemExit(f"Error empty data in path {self.path_csv} \n" +
                             str(e))


if __name__ == "__main__":
    conexion = conexionDB()
