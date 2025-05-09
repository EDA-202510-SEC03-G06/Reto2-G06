import sys
default_limit = 1000
sys.setrecursionlimit(default_limit*10)
import time
import csv
from App import logic
from DataStructures.Map import map_linear_probing as lp
from DataStructures.List import array_list as al

def new_logic():
    """
        Se crea una instancia del controlador
    """
    #TODO: Llamar la función de la lógica donde se crean las estructuras de datos
    control = logic.new_logic()
    return control

def print_menu():
    print("Bienvenido")
    print("1- Cargar información")
    print("2- Ejecutar Requerimiento 1")
    print("3- Ejecutar Requerimiento 2")
    print("4- Ejecutar Requerimiento 3")
    print("5- Ejecutar Requerimiento 4")
    print("6- Ejecutar Requerimiento 5")
    print("7- Ejecutar Requerimiento 6")
    print("8- Ejecutar Requerimiento 7")
    print("9- Ejecutar Requerimiento 8 (Bono)")
    print("0- Salir")

def load_data(control):
    """
    Carga los datos
    """
    ruta = "Data/agricultural-80.csv"  
    contador = 0
    
    with open(ruta, encoding='utf-8') as archivo:
        lector = csv.DictReader(archivo)
        
        datos = []
        for fila in lector:
            datos.append(fila)
            contador += 1
            
    print(f"Se cargaron {contador} registros.")
    respuesta = logic.load_data(control, datos)       
    print("cargando datos...")
    return respuesta


def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    #TODO: Realizar la función para imprimir un elemento
    id = input("ingrese el ID del dato a consultar: ")
    result = logic.get_data(control,id)
    if result:
        print("dato encontrado: ")
        print(result)
    else:
        print("El ID no se encuentra en los datos.")
    return result

def print_req_1(control):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 1
    year = int(input("Ingrese el año de interés (YYYY): "))
    report = logic.req_1(control, year)
    if report:
        print("\n" + "="*60)
        print("Tiempo de ejecución:", report["execution_time"], "ms")
        print("Total de registros encontrados:", report["total_records"])
        print("Último registro encontrado:")
        for key, value in report["last_record"].items():
            print(f"{key}: {value}")
        print("\nPrimeros 5 registros:")
        for i, reg in enumerate(report["first_five"], 1):
            print(f"Registro {i}:")
            print(f"Año de recolección: {reg['year_collection']}")
            print(f"Fecha de carga: {reg['load_time']}")
            print(f"Departamento: {reg['state_name']}")
            print(f"Fuente: {reg['source_type']}")
            print(f"Unidad: {reg['unit']}")
            print(f"Valor: {reg['value']}")
            print("-" * 40)

        print("\nÚltimos 5 registros:")
        for i, reg in enumerate(report["last_five"], 1):
            print(f"Registro {i}:")
            print(f"Año de recolección: {reg['year_collection']}")
            print(f"Fecha de carga: {reg['load_time']}")
            print(f"Departamento: {reg['state_name']}")
            print(f"Fuente: {reg['source_type']}")
            print(f"Unidad: {reg['unit']}")
            print(f"Valor: {reg['value']}")
            print("-" * 40)
    pass 

def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    departamento = input("Ingrese el nombre del departamento: ")
    cantidad = int(input("Ingrese la cantidad de registros a mostrar: "))
    
    report = logic.req_2(control, departamento, cantidad)
    
    if report:
        print(f"\nTiempo de ejecución: {report['execution_time']} ms")
        print(f"Total de registros encontrados: {report['total_records']}")
        
        print("\nRegistros:")
        for record in report["last_N_records"]:
            print(f"\nAño de recopilación: {record['year_collection']}")
            print(f"Fecha de carga: {record['load_time']}")
            print(f"Tipo de fuente/origen: {record['source_type']}")
            print(f"Frecuencia de recopilación: {record['frequency']}")
            print(f"Nombre del departamento: {record['state_name']}")
            print(f"Tipo del producto: {record['commodity']}")
            print(f"Unidad de medición: {record['unit']}")
            print(f"Valor de la medición: {record['value']}")
    else:
        print("No se encontraron registros para los parámetros especificados.")

def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    departamento = input("Ingrese el nombre del departamento: ")
    anio_inicial = int(input("Ingrese el año inicial (YYYY): "))
    anio_final = int(input("Ingrese el año final (YYYY): "))
    
    report = logic.req_3(control, departamento, anio_inicial, anio_final)
    
    if report:
        print(f"\nTiempo de ejecución: {report['execution_time']} ms")
        print(f"Total de registros encontrados: {report['total_records']}")
       
        count_survey = sum(1 for record in report['last_N_records'] if record['source_type'] == 'SURVEY')
        count_census = sum(1 for record in report['last_N_records'] if record['source_type'] == 'CENSUS')
        
        print(f"Registros con fuente 'SURVEY': {count_survey}")
        print(f"Registros con fuente 'CENSUS': {count_census}")

        print("\nRegistros:")
        for record in report["last_N_records"]:
            print(f"\nTipo de fuente/origen: {record['source_type']}")
            print(f"Año de recopilación: {record['year_collection']}")
            print(f"Fecha de carga: {record['load_time']}")
            print(f"Frecuencia de recopilación: {record['frequency']}")
            print(f"Nombre del departamento: {record['state_name']}")
            print(f"Tipo del producto: {record['commodity']}")
            print(f"Unidad de medición: {record['unit']}")
            print(f"Valor de la medición: {record['value']}")
            print("-" * 50)  
    else:
        print("\nNo se encontraron registros para los parámetros especificados.")
        
def print_req_4(control):
    """
    Función que imprime la solución del Requerimiento 4 en consola
    """
    producto = input("Ingrese el tipo de producto (ej. HOGS, SHEEP): ").strip()
    anio_inicial = int(input("Ingrese el año inicial (YYYY): "))
    anio_final = int(input("Ingrese el año final (YYYY): "))
    
    report = logic.req_4(control, producto, anio_inicial, anio_final)
    
    if report:
        print("\n" + "="*60)
        print(f"Tiempo de ejecución: {report['execution_time']:.3f} ms")
        print(f"Total de registros encontrados: {report['total_records']}")
        print(f"Registros con fuente SURVEY: {report['total_survey']}")
        print(f"Registros con fuente CENSUS: {report['total_census']}")
        
        print("\n" + "-"*60)
        print(f"{'REGISTROS ENCONTRADOS':^60}")
        print("-"*60)
        
        for i, record in enumerate(report['records'], 1):
            print(f"\nRegistro #{i}:")
            print(f"• Tipo de fuente: {record['source_type']}")
            print(f"• Año de recopilación: {record['collection_year']}")
            print(f"• Fecha de carga: {record['load_date']}")
            print(f"• Frecuencia: {record['frequency']}")
            print(f"• Departamento: {record['department']}")
            print(f"• Unidad de medida: {record['unit']}")
            if i < len(report['records']):
                print("-"*40)
    else:
        print("\nNo se encontraron registros que coincidan con los criterios de búsqueda.")

def print_req_5(control):
    """
    Muestra los resultados del Requerimiento 5 de manera organizada.
    """  
    categoria = input("Ingrese la categoría estadística (ej. INVENTORY, SALES): ").strip()
    anio_inicial = int(input("Ingrese el año inicial (YYYY): "))
    anio_final = int(input("Ingrese el año final (YYYY): "))
    
    report = logic.req_5(control, categoria, anio_inicial, anio_final)
    
    if report:
        print("\n" + "="*60)
        print(f"• Tiempo de ejecución: {report['execution_time']:.3f} ms")
        print(f"• Total registros: {report['total_records']}")
        print(f"• Registros SURVEY: {report['total_survey']}")
        print(f"• Registros CENSUS: {report['total_census']}")
        
        print("\n" + "-"*60)
        print(f"{'REGISTROS ENCONTRADOS':^60}")
        print("-"*60)
        
        for i, record in enumerate(report['records'], 1):
            print(f"\n[Registro {i}]")
            print(f"  Fuente: {record['source_type']}")
            print(f"  Año recopilación: {record['collection_year']}")
            print(f"  Fecha carga: {record['load_date']}")
            print(f"  Frecuencia: {record['frequency']}")
            print(f"  Departamento: {record['department']}")
            print(f"  Unidad: {record['unit']}")
            print(f"  Producto: {record['product_type']}")
            print("-"*40 if i < len(report['records']) else "")
    else:
        print("\n¡No se encontraron registros para los criterios especificados!")
        print("Revise la categoría y el rango de años.")
    pass
def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    
    departamento = input("Ingrese el nombre del departamento: ").strip()
    fecha_inicial = input("Ingrese la fecha inicial (YYYY-MM-DD): ").strip()
    fecha_final = input("Ingrese la fecha final (YYYY-MM-DD): ").strip()

    report = logic.req_6(control, departamento, fecha_inicial, fecha_final)

    if report:
        print("\n" + "-" * 50)
        print(f"RESULTADOS PARA {departamento} ({fecha_inicial} - {fecha_final})".center(50))
        print("-" * 50)
        print(f"Tiempo de ejecución: {report['execution_time']:.2f} ms")
        print(f"Total registros encontrados: {report['total_records']}")
        print(f"Registros SURVEY: {report['survey_count']}")
        print(f"Registros CENSUS: {report['census_count']}")

        print("\nREGISTROS DESTACADOS:")
        for i, record in enumerate(report["filtered_data"], 1):
            print(f"\nRegistro #{i}:")
            print(f"  • Fuente: {record['source_type']}")
            print(f"  • Año recolección: {record['collection_year']}")
            print(f"  • Fecha carga: {record['load_date']}")
            print(f"  • Frecuencia: {record['frequency']}")
            print(f"  • Departamento: {record['department']}")
            print(f"  • Unidad: {record['unit']}")
            print(f"  • Producto: {record['product_type']}")
    else:
        print("\nNO se encontraron registros para los criterios ingresados.")
    print("\n" + "=" * 50 + "\n")
    
def print_req_7(control):
    """
        Función que imprime la solución del Requerimiento 7 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 7
    departamento = input("Ingrese el nombre del departamento: ").strip()
    anio_inicial = int(input("Ingrese el año inicial (YYYY): "))
    anio_final = int(input("Ingrese el año final (YYYY): "))
    ordenamiento = input("Ingrese orden (ASCENDENTE/DESCENDENTE): ").strip()

    report = logic.req_7(control, departamento, anio_inicial, anio_final, ordenamiento)

    if report:
        print("\n" + "-"*50)
        print(f"INGRESOS AGRÍCOLAS EN {departamento} ({anio_inicial}-{anio_final})".center(50))
        print("-"*50)
        print(f"Tiempo ejecución: {report['execution_time']:.2f} ms")
        print(f"Total registros válidos: {report['total_records']}")
        print(f"Registros SURVEY: {report['survey_count']}")
        print(f"Registros CENSUS: {report['census_count']}")
        print(f"Registros inválidos: {report['invalid_records']}")
        
        print("\nRESUMEN POR AÑO (orden {0}):".format(ordenamiento.lower()))
        for anio_data in report["filtered_data"]:
            print("\n" + "~"*30)
            print(f"AÑO: {anio_data[0]}")
            if anio_data[0] == report["anio_mayor"]:
                print("<< AÑO CON MAYOR INGRESO >>")
            elif anio_data[0] == report["anio_menor"]:
                print("<< AÑO CON MENOR INGRESO >>")
            print(f"Ingreso total: ${anio_data[1]:,.2f}")
            print(f"Total registros: {anio_data[2]}")
            print(f"  - Survey: {anio_data[3]}")
            print(f"  - Census: {anio_data[4]}")
    else:
        print("NO se encontraron registros con ingresos válidos para los criterios ingresados.")
    print("\n" + "="*50 + "\n")


def print_req_8(control):
    """
        Función que imprime la solución del Requerimiento 8 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 8
    N = int(input("Ingrese el número de departamentos a listar: "))
    order = input("Ingrese el tipo de ordenamiento (ASCENDENTE/DESCENDENTE): ").strip().upper()
    report = logic.req_8(control, N, order, catalog)
    
    if report:
        print("Tiempo de ejecución:", report["execution_time"], "ms")
        print("Total de departamentos analizados:", report["total_departments"])
        print("Tiempo promedio entre recopilación y carga de los registros:", report["avg_time"])
        print("Menor año de recopilación:", report["min_year"])
        print("Mayor año de recopilación:", report["max_year"])
        
        for departamento in report["departments"]:
            print(f"Departamento: {departamento[0]}")
            print(f"Tiempo promedio entre recopilación y carga: {sum(departamento[1])/len(departamento[1]):.2f}")
            print(f"Número de registros: {departamento[2]}")
            print(f"Menor año de recopilación: {departamento[3]}")
            print(f"Mayor año de recopilación: {departamento[4]}")
            print(f"Menor tiempo entre recopilación y carga: {departamento[5]}")
            print(f"Mayor tiempo entre recopilación y carga: {departamento[6]}")
            print(f"Total registros 'SURVEY': {departamento[7]}")
            print(f"Total registros 'CENSUS': {departamento[8]}")


# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 1:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 2:
            print_req_1(control)

        elif int(inputs) == 3:
            print_req_2(control)

        elif int(inputs) == 4:
            print_req_3(control)

        elif int(inputs) == 5:
            print_req_4(control)

        elif int(inputs) == 6:
            print_req_5(control)

        elif int(inputs) == 7:
            print_req_6(control)

        elif int(inputs) == 8:
            print_req_7(control)

        elif int(inputs) == 9:
            print_req_8(control)

        elif int(inputs) == 0:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
