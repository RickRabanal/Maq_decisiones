import sys
import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr,coalesce,to_date,lower
from pyspark.conf import SparkConf
from datetime import datetime

from pyspark.sql import DataFrame
#from pyspark.sql import functions as F
from functools import reduce
from pyspark import StorageLevel

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

mongo_conn = sys.argv[1]
mongo_db = sys.argv[2]
coll_input=sys.argv[3]
coll_output=sys.argv[4]


#.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2") \
spark = SparkSession.builder.config("spark.mongodb.input.uri", mongo_conn)\
    .config("spark.mongodb.input.database", mongo_db)\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.output.uri", mongo_conn) \
    .config("spark.mongodb.output.database", mongo_db)\
    .getOrCreate()



df = spark.read.format("mongo").option("schema_infer_mode", "dynamic").option("collection",coll_input).load()
df.printSchema()



# Verificar si las columnas existen
if "fecha_disposicion" in df.columns:
    df = df.withColumn("fecha_disposicion", to_date(col("fecha_disposicion"), "yyyy-MM-dd"))

if "Transicionan" in df.columns:
    df = df.withColumn("Transicionan_1",col("Transicionan"))

if "contrato_id" in df.columns:
    df = df.withColumn("contrato_id", col("contrato_id").cast("string"))

# Obtener la ruta absoluta del archivo JSON
file_path_inicial = '/data/version_octubre_reglas_inicial_diarias.json'
file_path_goteo = '/data/version_octubre_reglas_goteo_v2.json'



df_inicial =df.filter(
    (
        ((col("dias_vencidos_inicial") > 0) & (col("dias_vencidos_inicial") <= 269)) 
        | (col("Sub_estrategia").isin("C033", "F037", "F021", "C034","F032","C035"))
    )     & 
    ~(col("status").isin("P"))     &
    ~(col("segmentacion").isin(["Sin segmento", "Sin Segmento"]))  # Asegurar lista
)


df_goteo = df.filter(
((col("dias_vencidos_inicial") <= 0) | (col("dias_vencidos_inicial").isNull())) & 
    (~col("Sub_estrategia").isin("C033", "F037", "F021", "C034","F032","C035") | col("Sub_estrategia").isNull()) &
    ~(col("status").isin("P")) & 
    ~(col("segmentacion").isin("Castigos", "Sin segmento", "Sin Segmento"))
).drop("regla", "Sub_estrategia", "Canal", "Transicionan", "Comentarios")

df_guarda = df.filter((col("segmentacion").isin( "Sin segmento", "Sin Segmento")) | (col("dias_vencidos_inicial") > 269) | (col("status").isin("P")) ).drop("regla", "Sub_estrategia", "Canal", "Transicionan", "Comentarios")

df_guarda.write.format("mongo").option("collection",coll_output).mode("overwrite").save()



def reglas(file_path):
    # Verificar si el archivo existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    # Leer las reglas desde un archivo JSON
    with open(file_path, 'r') as file:
        rules = json.load(file)
    return rules

regla_inicial=reglas(file_path_inicial)
regla_goteo=reglas(file_path_goteo)

def is_date(value):
    """Verifica si el valor tiene el formato de fecha yyyy-mm-dd."""
    #print(value)
    try:
        value_str = str(value)  
        datetime.strptime(value_str, "%Y-%m-%d")
        #print("Se paso a fecha")
        return True
    except ValueError:
        return False
    
# Función para construir la condición basada en las operaciones definidas
def build_condition(cond):
    field = cond["field"]
    operation = cond["operation"]
    try:
        values = cond["values"]
    except KeyError:
        # Si 'values' no existe, usa 'value'
        values = cond["value"]

    if field not in df.columns:
        #print(f"El campo '{field}' no existe en el DataFrame, se omite la condición.")
        return None
    
    # Ignorar la condición si los valores son [None]
    if isinstance(values, list) and values == [None]:
        return None
    
    # Dependiendo de la operación
    if operation == "in":
        #return col(field).isin(values)
        # Asegurarse de que 'values' es una lista con exactamente dos elementos
        if isinstance(values, list) and len(values) > 1:
            #values = values[0]
            return col(field).isin(values)
        elif isinstance(values, list) and len(values) == 1:
            return col(field).isin(values[0])  
        
    elif operation == "not in":
        if isinstance(values, list) and len(values) > 1:
            # Devolver True si el valor no está en la lista o si es NULL
            return (~col(field).isin(values)) | col(field).isNull()
        elif isinstance(values, list) and len(values) == 1:
            # Devolver True si el valor no es igual al único valor en la lista o si es NULL
            return (col(field) != values[0]) | col(field).isNull()
    

    elif operation == "between":
        # Asegurarse de que 'values' es una lista con exactamente dos elementos
        if isinstance(values, list) and len(values) == 2:
            return col(field).between(values[0], values[1])
        else:
            raise ValueError(f"Error en 'between': {field} requiere una lista de dos elementos, pero recibió {values}")
    elif operation == "=":
        if is_date(values[0]):
            comparison_value = lit(values[0]).cast("date")
        else:
            comparison_value = lit(values[0])
        return col(field) == comparison_value
    elif operation == ">":
        if is_date(values[0]):
            comparison_value = lit(values[0]).cast("date")
        else:
            comparison_value = lit(values[0])
        return col(field) > comparison_value
    elif operation == "<":
        if is_date(values[0]):
            comparison_value = lit(values[0]).cast("date")
        else:
            comparison_value = lit(values[0])
        return col(field) < comparison_value
    elif operation == ">=":
        if is_date(values[0]):
            comparison_value = lit(values[0]).cast("date")
        else:
            comparison_value = lit(values[0])
        return col(field) >= comparison_value
    elif operation == "<=":
        if is_date(values[0]):
            comparison_value = lit(values[0]).cast("date")
        else:
            comparison_value = lit(values[0])
        #print("Valores:", values)
        return col(field) <= comparison_value
    else:
        raise ValueError(f"Operación desconocida: {operation}")



def agregar_columna(df):
    # Verificar si la columna 'regla' no existe y crearla con valor por defecto (None)
    if 'regla' not in df.columns:
        df = df.withColumn('regla', lit(None))

    # Verificar si la columna 'Sub_estrategia' no existe y crearla con valor por defecto
    if 'Sub_estrategia' not in df.columns:
        df = df.withColumn('Sub_estrategia', lit(None))

    # Verificar si la columna 'Canal' no existe y crearla con valor por defecto
    if 'Canal' not in df.columns:
        df = df.withColumn('Canal', lit(None))

    # Verificar si la columna 'Comentarios' no existe y crearla con valor por defecto
    if 'Comentarios' not in df.columns:
        df = df.withColumn('Comentarios', lit(None))


    # Verificar si la columna 'Nivel_de_gestión' no existe y crearla con valor por defecto
    if 'Transicionan' not in df.columns:
        df = df.withColumn('Transicionan', lit(None))
    return df

from pyspark.sql import functions as F


def proceso(df,rules):
    for rule in rules["rules"]:
        condition = None  # Inicialización de la variable 'condition'
        skip_rule = False 
        # Construcción dinámica de la condición a partir de las reglas
        
        for cond in rule["conditions"]:
            
            # Verificar que el campo existe en el DataFrame antes de aplicar la condición
            #print("cond:", cond)
            #print("type of cond:", type(cond))
            if cond["field"] in df.columns:
                try:
                    current_condition = build_condition(cond)  # Construir la condición
                    
                except ValueError as e:
                    #print(f"Error en la regla {rule['rule']}: {str(e)}")
                    current_condition = None
            else:
                df = df.withColumn(
                    cond["field"],lit(None)
                    #coalesce(col(cond["field"]),lit(None))
                )
                try:
                    current_condition = build_condition(cond)  # Construir la condición
                except ValueError as e:
                    #print(f"Error en la regla {rule['rule']}: {str(e)}")
                    current_condition = None

            # Combinación de condiciones
            if current_condition is not None :
                if condition is None:
                    condition = current_condition  # Inicializar condition en la primera iteración
                else :
                    condition = condition & current_condition  # Combinar condiciones con AND
        # Si se encontró un campo inexistente, invalidar la regla completa
        if skip_rule:
            condition = None  # Aseguramos que la regla no sea aplicada


        # Aplicar las transformaciones solo si hay una condición válida
        if condition is not None :

            for output in rule["outputs"]:
                #print(output)
                # Si la columna no existe, crearla con valor None
                if output["field"] not in df.columns :
                    df = df.withColumn(output["field"], lit(None))
                # Aplicar la condición si es válida
                if isinstance(output["value"], list) :
                    # Si el valor es una lista válida, aplicamos el primer elemento
                    df = df.withColumn(output["field"], when(condition, lit(output["value"][0])).otherwise(col(output["field"])))
                else:
                    # Si el valor no es una lista, aplicamos directamente el valor
                    df = df.withColumn(output["field"], when((condition), lit(output["value"])).otherwise(col(output["field"])))

        else:
            print(f"No se aplicaron condiciones para la regla: {rule['rule']}")

    return df
df_inicial=proceso(df_inicial,regla_inicial)
df_inicial.write.format("mongo").option("collection",coll_output).mode("append").save()
print('TERMINA ','INICIAL')

df_goteo=proceso(df_goteo,regla_goteo)
df_goteo.write.format("mongo").option("collection",coll_output).mode("append").save()
#df_goteo.write.format("mongo").option("collection","core_map_goteo_diaria_081224").mode("overwrite").save()
print('TERMINA ','GOTEO')

spark.stop()
