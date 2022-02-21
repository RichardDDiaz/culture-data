import requests as req
import locale
from datetime import datetime
from pathlib import Path

locale.setlocale(locale.LC_ALL, 'es_ES.utf8')


class csv_download:
    def __init__(self, url, file_name):
        self.url = url
        self.file_name = file_name.lower().replace(" ", "_")

    def download_explicit_date(self):
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
            raise SystemExit(e)
        except req.exceptions.HTTPError as e:
            raise SystemExit(e)
        except req.exceptions.RequestException as e:
            raise SystemExit(e)
        except OSError as e:
            raise SystemExit(f"Error al crear el directorio" + e.response.text)

        return path_raw + "/" + full_name
