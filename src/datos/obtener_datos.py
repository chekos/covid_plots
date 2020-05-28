import requests
from zipfile import ZipFile
import io
import pandas as pd
from datetime import datetime as dt
from pathlib import Path

ESTE_ARCHIVO = Path(__file__)
ESTA_CARPETA = ESTE_ARCHIVO.parent
DATOS = ESTA_CARPETA.joinpath("../../datos/")
DATOS_BRUTOS = DATOS.joinpath("brutos/")
DATOS_INTERINOS = DATOS.joinpath("interinos/")

# No hay datos antes del 12 de abril del 2020 en las bases federales
PRIMERA_FECHA = "04/12/2020"
ULTIMA_FECHA = dt.today().strftime("%m/%d/%Y")

fechas = pd.date_range(start=PRIMERA_FECHA, end=ULTIMA_FECHA)

BASE_URL = "http://187.191.75.115/gobmx/salud/datos_abiertos"
URL_HISTORICOS = BASE_URL + "/historicos/datos_abiertos_covid19_"
URL_DEL_DIA = BASE_URL + "/datos_abiertos_covid19.zip"

FECHAS_SUFIJO = fechas[:-1].strftime("%d.%m.%Y")

ARCHIVOS_DESCARGADOS = [path.name for path in DATOS_BRUTOS.glob("*COVID19MEXICO.csv")]

def checa_url(url: str) -> None:
    try:
        r = requests.get(url)
        return r
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        SystemExit(e)
        return None

def descarga_datos(fecha: str, url: str = URL_HISTORICOS, mas_recientes: bool = False) -> None:
    """Descarga datos historicos desde la URL base

    Parameters
    ----------
    fecha : str
        Fecha para buscar los datos
    url : str
        URL base con la cual construimos la URL de la que descargamos los datos.
    """
    if mas_recientes:
        URL_DATOS = url
    else:
        URL_DATOS = f"{url}{fecha}.zip"

    dia, mes, año = fecha.split(".")
    nombre_del_archivo = f"{año[-2:]}{mes}{dia}COVID19MEXICO.csv"
    # Extraer los datos si no los hemos descargado
    try:
        if nombre_del_archivo not in ARCHIVOS_DESCARGADOS:
            r = checa_url(URL_DATOS)
            print(f"Descargando archivo `{nombre_del_archivo}`.", end = "\n")
            archivo = ZipFile(io.BytesIO(r.content))
            archivo.extractall(DATOS_BRUTOS)
        else:
            print(f"El archivo {nombre_del_archivo} ya existe.", end = "\n",)
    except:
        print(f"El archivo de {fecha} no se pudo descargar.", end = "\n")


if __name__ == "__main__":
    # historicos
    for fecha in FECHAS_SUFIJO:
        descarga_datos(fecha)

    # los mas recientes
    descarga_datos(fecha = FECHAS_SUFIJO[-1], url=URL_DEL_DIA, mas_recientes=True)