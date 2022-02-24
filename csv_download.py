from datetime import datetime
from pathlib import Path
import requests as req
import locale


locale.setlocale(locale.LC_ALL, 'es_ES.utf8')


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
            with open(path_raw + "/" + full_name, "wb") as f:
                f.write(file.content)
        except req.exceptions.ConnectionError as e:
            raise SystemExit(f"Error de conexion: {self.url} \n" +
                             str(e))
        except req.exceptions.HTTPError as e:
            raise SystemExit("Error HTTP \n" + str(e))
        except req.exceptions.RequestException as e:
            raise SystemExit(str(e))
        except OSError as e:
            raise SystemExit("Error al crear el directorio \n" + str(e))

        return path_raw + "/" + full_name
