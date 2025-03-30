import csv
csv.field_size_limit(2147483647)
import time
import tracemalloc
from DataStructures.Map.map_functions import next_prime


from DataStructures.Map import map_linear_probing as lp
from DataStructures.List import array_list as al
from DataStructures.Map import map_separate_chaining as scv

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos}
    catalogo = {
        "fuentes":lp(),
        "productos":lp(),
        "estados":lp(),
        "datos":al()
    }
    return catalogo


# Funciones para la carga de datos

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos
    start_time = get_time()
    f = open(filename, encoding='utf-8')
    lines = f.readlines()
    f.close()
    
    headers = lines[0].strip().split(",")
    if len(lines)>1:
        first_year = int(lines[1].strip().split(",")[headers.index("year_collection")])
        min_year = first_year
        max_year = first_year
    else:
        min_year = None
        max_year = None
        
    first_five = []  
    last_five = [] 
    total_records = 0
        
    for linea in lines[1:]:
        valores = linea.strip().split(",")
        record = {headers[i]: valores[i] for i in range(len(headers))}
        year = int(record["year_collection"])
        if year < min_year:
            min_year = year
        if year > max_year:
            max_year = year
            
        if total_records < 5:
            first_five.append(record)
        else:
            last_five.append(record)
            if len(last_five) > 5:
                last_five.pop(0)
        total_records += 1

    end_time = get_time()
    c_tiempo = delta_time(start_time,end_time) 
    report = {
        "execution_time": c_tiempo, 
        "total_records": total_records,  
        "min_year": min_year,  
        "max_year": max_year,  
        "first_five": first_five,  
        "last_five": last_five  
    }
    return report


# Funciones de consulta sobre el catálogo

def get_data(catalog, id):
    """
    Retorna un dato por su ID.
    """
    #TODO: Consulta en las Llamar la función del modelo para obtener un dato
    if id in catalog["datos"]:
        return catalog["datos"][id]
    else:
        return None


def req_1(catalog, anio):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    start_time = get_time()
    registro = catalog["datos"]
    
    filtro = [registro for valor in registro if valor["anio_collection"] == anio]
    if not filtro:
        return {"aviso": f"no se encontraron registros para el año {anio}"}
    
    ultimo_reg = filtro[0]
    for registro in filtro:
        if registro["load_date"] > ultimo_reg["load_date"]:
            ultimo_reg = registro
    end_time = get_time()
    c_tiempo= delta_time(start_time,end_time)
    
    report = {
        "execution_time": c_tiempo,
        "total_records": len(filtro),
        "last_record": {
            "year_collection": ultimo_reg["year_collection"],
            "load_date": ultimo_reg["load_date"],
            "source_type": ultimo_reg["source_type"],
            "frequency": ultimo_reg["frequency"],
            "department": ultimo_reg["department"],
            "product_type": ultimo_reg["product_type"],
            "unit": ultimo_reg["unit"],
            "value": ultimo_reg["value"]
        }
    }
    return report


def req_2(catalog, departamento, N):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    start_time = get_time()
    
    registros_filtrados = []
    for registro in catalog["datos"]:
        if registro["department"] == departamento:
            registros_filtrados.append(registro)
    
    for i in range(len(registros_filtrados)):
        for j in range(i + 1, len(registros_filtrados)):
            if registros_filtrados[i]["load_date"] < registros_filtrados[j]["load_date"]:
                registros_filtrados[i], registros_filtrados[j] = registros_filtrados[j], registros_filtrados[i]
    
    registros_seleccionados = registros_filtrados[:N]
    
    end_time = get_time()
    execution_time = delta_time(start_time, end_time)
    
    report = {
        "execution_time": execution_time,
        "total_records": len(registros_filtrados),
        "records": [
            {
                "year_collection": reg["year_collection"],
                "load_date": reg["load_date"],
                "source_type": reg["source_type"],
                "frequency": reg["frequency"],
                "department": reg["department"],
                "product_type": reg["product_type"],
                "unit": reg["unit"],
                "value": reg["value"]
            }
            for reg in registros_seleccionados
        ]
    }
    return report


def req_3(catalog, departamento, anio_inicial, anio_final):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    start_time = get_time()
    registros_filtrados = []
    for registro in catalog["datos"]:
        if registro["department"] == departamento and anio_inicial <= int(registro["year_collection"]) <= anio_final:
            registros_filtrados.append(registro)
    
    for i in range(len(registros_filtrados)):
        for j in range(i + 1, len(registros_filtrados)):
            if (registros_filtrados[i]["load_date"], registros_filtrados[i]["department"]) < (registros_filtrados[j]["load_date"], registros_filtrados[j]["department"]):
                registros_filtrados[i], registros_filtrados[j] = registros_filtrados[j], registros_filtrados[i]
    
    total_survey = sum(1 for reg in registros_filtrados if reg["source_type"] == "SURVEY")
    total_census = sum(1 for reg in registros_filtrados if reg["source_type"] == "CENSUS")
    
    if len(registros_filtrados) > 20:
        registros_filtrados = registros_filtrados[:5] + registros_filtrados[-5:]
    
    end_time = get_time()
    execution_time = delta_time(start_time, end_time)
    
    report = {
        "execution_time": execution_time,
        "total_records": len(registros_filtrados),
        "total_survey": total_survey,
        "total_census": total_census,
        "records": registros_filtrados
    }
    return report


def req_4(catalog, producto, anio_inicial, anio_final):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    start_time = get_time()
    registro = catalog["datos"]
    filtro = [registro for valor in registro if valor["product_type"] == producto and anio_inicial <= registro["collection_year"] <= anio_final]
    if not filtro:
        return None
    
    lp.shell_sort(filtro)
    total_registros = len(filtro)
    total_survey = sum(1 for record in filtro if record["source_type"] == "SURVEY")
    total_census = sum(1 for record in filtro if record["source_type"] == "CENSUS")
    
    if total_registros > 20:
        filtro = filtro[:5] + filtro[-5:]
    end_time = get_time()
    c_tiempo= delta_time(start_time,end_time)
    report = {
        "execution_time": c_tiempo,
        "total_records": total_registros,
        "total_survey": total_survey,
        "total_census": total_census,
        "records": [
            {
                "source_type": record["source_type"],
                "collection_year": record["collection_year"],
                "load_date": record["load_date"],
                "frequency": record["frequency"],
                "department": record["department"],
                "unit": record["unit"]
            }
            for record in filtro
        ]
    }
    
    return report
    pass


def req_5(catalog,categoria, anio_inicial, anio_final):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    start_time = get_time()
    registro = catalog["datos"]
    filtro = [record for record in registro if record["statistical_category"] == categoria and anio_inicial <= record["collection_year"] <= anio_final]
    if not filtro:
        return None
    lp.shell_sort(filtro)
    total_registros = len(filtro)
    total_survey = sum(1 for record in filtro if record["source_type"] == "SURVEY")
    total_census = sum(1 for record in filtro if record["source_type"] == "CENSUS")
    
    if total_registros > 20:
        filtro = filtro[:5] + filtro[-5:]
        
    end_time = get_time()
    c_tiempo= delta_time(start_time,end_time)
    
    report = {
        "execution_time": c_tiempo,
        "total_records": total_registros,
        "total_survey": total_survey,
        "total_census": total_census,
        "records": [
            {
                "source_type": record["source_type"],
                "collection_year": record["collection_year"],
                "load_date": record["load_date"],
                "frequency": record["frequency"],
                "department": record["department"],
                "unit": record["unit"],
                "product_type": record["product_type"]
            }
            for record in filtro
        ]
    }
    
    return report

def req_6(catalog, departamento, anio_inicial, anio_final):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    start_time = get_time()
    filtro = []
    registros = catalog["datos"]
    for registro in registros:
        anio_coleccion = int(registro["year_collection"]) 
        if registro["department"] == departamento and anio_inicial <= anio_coleccion <= anio_final:
            filtro.append(registro)
    map.shell_sort(filtro)
    total_registros = len(filtro)
    total_survey = sum(1 for registro in filtro if registro["source_type"] == "SURVEY")
    total_census = sum(1 for registro in filtro if registro["source_type"] == "CENSUS")
    if total_registros > 20:
        datos_filtrados = filtro[:5] + filtro[-5:] 
    else:
        datos_filtrados = filtro
    end_time = get_time()
    execution_time = delta_time(start_time,end_time)
    report = {
        "execution_time": execution_time,
        "total_records": total_registros,
        "survey_count": total_survey,
        "census_count": total_census,
        "filtered_data": datos_filtrados
    }
    return report
    
        
    
def req_7(catalog, departamento, anio_inicial, anio_final, ordenamiento):
    """
    Retorna el resultado del requerimiento 7
    """
    # TODO: Modificar el requerimiento 7
    


def req_8(catalog):
    """
    Retorna el resultado del requerimiento 8
    """
    # TODO: Modificar el requerimiento 8
    start_time = get_time()
    departamento_tiempos = {}
    
    for registro in catalog["datos"]:
        if registro["unit"] == "D":
            continue
        departamento = registro["department"]
        tiempo_diferencia = int(registro["load_date"][:4]) - int(registro["year_collection"])
        if departamento not in departamento_tiempos:
            departamento_tiempos[departamento] = []
        departamento_tiempos[departamento].append(tiempo_diferencia)
    
    estadisticas = []
    for departamento, tiempos in departamento_tiempos.items():
        promedio = sum(tiempos) / len(tiempos)
        estadisticas.append((departamento, promedio, len(tiempos), min(tiempos), max(tiempos)))
    
    ordenar_lista(estadisticas, order)
    seleccionados = estadisticas[:N]
    
    end_time = get_time()
    execution_time = delta_time(start_time, end_time)
    
    report = {
        "execution_time": execution_time,
        "total_departments": len(seleccionados),
        "departments": seleccionados
    }
    return report


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
