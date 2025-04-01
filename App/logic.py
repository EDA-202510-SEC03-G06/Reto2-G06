import csv
csv.field_size_limit(2147483647)
import time
import os
import tracemalloc
import datetime
from DataStructures.Map.map_functions import next_prime
from DataStructures.Map import map_linear_probing as lp
from DataStructures.List import array_list as al
from DataStructures.Map import map_separate_chaining as scv

def new_logic():
    """
    Crea el catálogo vacío con las estructuras necesarias.
    """
    catalogo = {
        "list_all_data": al.new_list(),
        "map_by_departments": lp.new_map(num_elements=60, load_factor=0.5),
        "map_by_commodity": lp.new_map(num_elements=80, load_factor=0.5)
    }
    return catalogo

# Funciones para la carga de datos

def load_data(catalogo, datos):

    inicio = time.time()

    registros_cargados = 0
    min_year = float('inf')
    max_year = float('-inf')

    for fila in datos:
        if fila['year_collection'].isdigit():
            fila['year_collection'] = int(fila['year_collection'])
        else:
            fila['year_collection'] = -1  

        if fila['year_collection'] > 0:
            if fila['year_collection'] < min_year:
                min_year = fila['year_collection']
            if fila['year_collection'] > max_year:
                max_year = fila['year_collection']

        es_valido = True
        for c in fila['value']:
            if not (c.isdigit() or c == ',' or c == '.'):
                es_valido = False
                break

        if fila['value'] != '' and es_valido:
            fila['value'] = float(fila['value'].replace(',', ''))
        else:
            fila['value'] = -1  

        partes_fecha = fila['load_time'].split(' ')
        if len(partes_fecha) == 2:
            fecha_partes = partes_fecha[0].split('-')
            hora_partes = partes_fecha[1].split(':')
            if len(fecha_partes) == 3 and len(hora_partes) == 3:
                fila['load_time'] = datetime.datetime(
                    int(fecha_partes[0]), int(fecha_partes[1]), int(fecha_partes[2]),
                    int(hora_partes[0]), int(hora_partes[1]), int(hora_partes[2])
                )
            else:
                fila['load_time'] = datetime.datetime.min  
        else:
            fila['load_time'] = datetime.datetime.min

        fila['location'] = fila['location'].replace(', ', ',').split(',')

        al.add_last(catalogo['list_all_data'], fila)
        registros_cargados += 1

        depto = fila['state_name']
        lista_depto = lp.get(catalogo['map_by_departments'], depto)
        if lista_depto is None:
            lista_depto = al.new_list()
            lp.put(catalogo['map_by_departments'], depto, lista_depto)
        al.add_last(lista_depto, fila)

        producto = fila['commodity']
        lista_prod = lp.get(catalogo['map_by_commodity'], producto)
        if lista_prod is None:
            lista_prod = al.new_list()
            lp.put(catalogo['map_by_commodity'], producto, lista_prod)
        al.add_last(lista_prod, fila)

    fin = time.time()  
    tiempo_ejecucion = fin - inicio

    print(f"\nTiempo de ejecución: {tiempo_ejecucion:.4f} segundos")
    print(f"Total de registros cargados: {registros_cargados}")
    print(f"Menor año de recolección: {min_year if min_year != float('inf') else 'N/A'}")
    print(f"Mayor año de recolección: {max_year if max_year != float('-inf') else 'N/A'}")
    
    

    return catalogo
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
    lp.shell_sort(filtro)
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
    start_time = get_time()
    registros = catalog["datos"]
    filtro = []
    ingresos_totales = []
    ingresos_anio = {}
    total_survey = 0
    total_census = 0
    registros_invalidos = 0
    for registro in registros:
        if registro["department"] == departamento:
            anio_coleccion = int(registro["year_collection"])
            ingreso = registro["income"]
            unidad_medida = registro["unit_measure"] 
            if anio_inicial <= anio_coleccion <= anio_final and "$" in unidad_medida:
                if ingreso.replace('.', '', 1).isdigit():
                    ingreso = float(ingreso)
                    filtro.append(registro) 
                    
                    if anio_coleccion not in ingresos_anio:
                        ingresos_anio[anio_coleccion] = {"total": 0, "cantidad": 0, "survey": 0, "census": 0}
                    ingresos_anio[anio_coleccion]["total"] += ingreso
                    ingresos_anio[anio_coleccion]["cantidad"] += 1
                    if registro["source_type"] == "SURVEY":
                        ingresos_anio[anio_coleccion]["survey"] += 1
                        total_survey += 1
                    elif registro["source_type"] == "CENSUS":
                        ingresos_anio[anio_coleccion]["census"] += 1
                        total_census += 1
                else:
                    registros_invalidos += 1
    for anio,datos in ingresos_anio.items():
        total = datos["total"]
        cantidad = datos["cantidad"]
        ingresos_totales.append((anio, total, cantidad))
    
    lp.shell_sort(ingresos_totales, ordenamiento)
    
    if ingresos_totales:
        if ordenamiento == "ASCENDENTE":
            anio_mayor = ingresos_totales[-1][0]
            anio_menor = ingresos_totales[0][0]
        else:
            anio_mayor = ingresos_totales[0][0]
            anio_menor = ingresos_totales[-1][0]
    else:
        anio_mayor = None
        anio_menor = None
        
    if len(ingresos_totales) > 15:
        ingresos_totales = ingresos_totales[:5] + ingresos_totales[-5:]  
    
    end_time = get_time()
    execution_time = delta_time(start_time, end_time) 
    
    report = {
        "execution_time": execution_time,
        "total_records": len(filtro),
        "survey_count": total_survey,
        "census_count": total_census,
        "invalid_records": registros_invalidos,
        "filtered_data": ingresos_totales,
        "anio_mayor": anio_mayor,
        "anio_menor": anio_menor
    }
    return report



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
