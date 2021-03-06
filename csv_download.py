from datetime import datetime
from pathlib import Path
import requests as req
import locale
import logging


locale.setlocale(locale.LC_ALL, 'es_ES.utf8')
logger = logging.getLogger(__name__)

class csv_download:
    def __init__(self, url, file_name):
        """Inicializador de la clase
        Args:
            url: url del archivo csv en string
            file_name: nombre del archivo csv en string
        ambos guardados en atributos
        """

        self.url = url
        self.file_name = file_name.lower().replace(" ", "_")

    def download_explicit_date(self):
        """Descarga el archivo csv con la fecha actual en una
           direccion especifica
        Returns:
            direccion del archivo csv en string
        """

        date = datetime.now()
        path_raw = "/".join([self.file_name, date.strftime('%Y') +
                            "-" + date.strftime('%B').lower()])
        full_name = self.file_name + "-" + date.strftime('%d-%m-%Y') + ".csv"
        try:
            path = Path(path_raw).mkdir(parents=True, exist_ok=True)
            file = req.get(self.url)
            file.raise_for_status()
            full_name = path_raw + "/" + full_name
            logger.info(f"Se abrio/creo el archivo: \n {full_name}, como wb")
            with open(full_name, "wb") as f:
                f.write(file.content)
        except req.exceptions.ConnectionError as e:
            logger.exception(f"Error de conexion: {self.url}")
            raise SystemExit(f"Error de conexion: {self.url} \n" +
                             str(e))
        except req.exceptions.Timeout as e:
            logger.exception(f"Error de tiempo de espera: {self.url}")
            raise SystemExit(f"Error de tiempo de espera: {self.url} \n" +
                             str(e))
        except req.exceptions.HTTPError as e:
            logger.exception(f"Error de HTTP: {self.url},\n HTTP: {e.code}")
            raise SystemExit("Error HTTP \n" + str(e))
        except req.exceptions.RequestException as e:
            logger.exception(f"Error de request: {self.url},\n request: {e}")
            raise SystemExit(str(e))
        except OSError as e:
            logger.exception(f"Error al abrir/crear:\n {full_name},\n OS: {e}")
            raise SystemExit("Error al crear el directorio \n" + str(e))

        logger.debug(f"Archivo {full_name} descargado en {path_raw}")
        return full_name
