import pandas as pd
from datetime import datetime as dt
from pathlib import Path

ESTE_ARCHIVO = Path(__file__)
ESTA_CARPETA = ESTE_ARCHIVO.parent
DATOS = ESTA_CARPETA.joinpath("../../datos/")
DATOS_BRUTOS = DATOS.joinpath("brutos/")
DATOS_INTERINOS = DATOS.joinpath("interinos/")
DATOS_PROCESADOS = DATOS.joinpath('procesados/')

# 1. Construye dataset de las bases de datos diarias
print("Leyendo los datos...")
dataframes = []
for base_de_datos in DATOS_BRUTOS.glob("200527COVID19MEXICO.csv"):
    _df = pd.read_csv(base_de_datos, encoding = 'latin1', low_memory=False)
    _df['FECHA_ARCHIVO'] = dt.strptime(base_de_datos.name[:6], '%y%m%d').date()
    dataframes.append(_df)

data = pd.concat(dataframes, axis = 0, ignore_index = False)

# 2. Procesar datos
## 2.1 Limpiar fechas
columnas_de_fechas = [col for col in data.columns if "FECHA_" in col]

for columna in columnas_de_fechas:
    # Fecha de defuncion 9999-99-99 se vuelve Not a Time (NaT)
    data[columna] = pd.to_datetime(data[columna], format = "%Y-%m-%d", errors = 'coerce', )

## 2.2 Agregacion a nivel id_registro (por persona)
grupos_id = data.groupby(["ID_REGISTRO"])

pacientes = grupos_id[['FECHA_SINTOMAS', 'FECHA_ARCHIVO', 'FECHA_INGRESO', 'FECHA_DEF']].min()

### Para obtener la fecha del primer evento en la historia de un paciente
def fechador(frame, columna: str, valor: int):
    """Extrae la fecha del primer evento en la historia de un paciente. Cada `frame` es la historia de un paciente. Busca el `valor` obtenido en la `columna` obtenida y devuelve la primera fecha en la que esa 

    Parameters
    ----------
    frame : pd.DataFrame
        Un objeto groupby de pandas
    columna : str
        Columna para buscar el `valor` sugerido.
    valor : int
        Valor de la `columna` que nos interesa.

    Returns
    -------
    pd.Timestamp
        Regresa la primera fecha en la que la `columna` tiene el `valor` sugerido.
    """
    sframe = frame.sort_values("FECHA_ARCHIVO")
    fframe = sframe[sframe[columna] == valor]
    if len(fframe) == 0:
        return None
    ind_s = sframe.index[0]
    ind_f = fframe.index[0]
    if (ind_s == ind_f):
        return pd.Series(sframe["FECHA_INGRESO"].min())
    else:
        return pd.Series(fframe.loc[ind_f, "FECHA_ARCHIVO"])
        

print("Procesando los datos, construyendo historias por paciente...")
print("Esto puede tardar un rato...")
print("Resultados...")
fecha_pos_series = grupos_id.apply(lambda x: fechador(x,"RESULTADO",1))
fecha_neg_series = grupos_id.apply(lambda x: fechador(x,"RESULTADO",2))
print("Tipo de paciente...")
fecha_hos_series = grupos_id.apply(lambda x: fechador(x,"TIPO_PACIENTE",1))
fecha_amb_series = grupos_id.apply(lambda x: fechador(x,"TIPO_PACIENTE",2))
print("Intubado...")
fecha_int_series = grupos_id.apply(lambda x: fechador(x,"INTUBADO",1))
fecha_noint_series = grupos_id.apply(lambda x: fechador(x,"INTUBADO",2))
fecha_noa_int_series = grupos_id.apply(lambda x: fechador(x,"INTUBADO",97))
fecha_ign_int_series = grupos_id.apply(lambda x: fechador(x,"INTUBADO",99))
print("Neumonia...")
fecha_neu_series = grupos_id.apply(lambda x: fechador(x,"NEUMONIA",1))
fecha_noneu_series = grupos_id.apply(lambda x: fechador(x,"NEUMONIA",2))
fecha_ign_neu_series = grupos_id.apply(lambda x: fechador(x,"NEUMONIA",99))
print("UCI...")
fecha_uci_series = grupos_id.apply(lambda x: fechador(x,"UCI",1))
fecha_nouci_series = grupos_id.apply(lambda x: fechador(x,"UCI",2))
fecha_noa_uci_series = grupos_id.apply(lambda x: fechador(x,"UCI",97))
fecha_ign_uci_series = grupos_id.apply(lambda x: fechador(x,"UCI",99))

pacientes["FECHA_POSITIVO"] = fecha_pos_series
pacientes["FECHA_NEGATIVO"] = fecha_neg_series
pacientes["FECHA_HOSPITALIZACION"] = fecha_hos_series
pacientes["FECHA_AMBULATORIO"] = fecha_amb_series
pacientes["FECHA_INTUBACION"] = fecha_int_series
pacientes["FECHA_NO_INTUBACION"] = fecha_noint_series
pacientes["FECHA_NO_APL_INTUBACION"] = fecha_noa_int_series
pacientes["FECHA_SE_IGN_INTUBACION"] = fecha_ign_int_series
pacientes["FECHA_NEUMONIA"] = fecha_neu_series
pacientes["FECHA_NO_NEUMONIA"] = fecha_noneu_series
pacientes["FECHA_SE_IGN_NEUMONIA"] = fecha_ign_neu_series
pacientes["FECHA_UCI"] = fecha_uci_series
pacientes["FECHA_NO_UCI"] = fecha_nouci_series
pacientes["FECHA_NOAPL_UCI"] = fecha_noa_uci_series
pacientes["FECHA_SE_IGN_UCI"] = fecha_ign_uci_series

### Limpiar nombres un poco
pacientes.rename(columns={"FECHA_ARCHIVO":"FECHA_APARICION"},inplace=True)

## 3. Exportar historias 
print("Guardando los datos en la carpeta de datos/procesados...")
HOY = dt.today().strftime("%d_%m_%Y")
pacientes.to_csv(DATOS_PROCESADOS / f"historia_pacientes_{HOY}.csv", encoding = 'utf-8')
