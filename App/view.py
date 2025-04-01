import sys
default_limit = 1000
sys.setrecursionlimit(default_limit*10)
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
    ruta = "Data/agricultural-20.csv"  # Asegúrate de que esta ruta sea la correcta

    # Inicializa el contador de registros
    contador = 0
    
    # Abre el archivo CSV
    with open(ruta, encoding='utf-8') as archivo:
        lector = csv.DictReader(archivo)
        
        # Carga los datos y cuenta los registros
        datos = []
        for fila in lector:
            datos.append(fila)
            contador += 1  # Incrementa el contador por cada fila
            
    # Imprime la cantidad de registros cargados
    print(f"Se cargaron {contador} registros.")
    
    # Devuelve los datos cargados
    return datos


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
    year = input("Ingrese el año de interés (YYYY): ")
    report = logic.req_1(control, year)
    if report:
        print("Tiempo de ejecución:", report["execution_time"], "ms")
        print("Total de registros encontrados:", report["total_records"])
        print("Último registro encontrado:")
        for key, value in report["last_record"].items():
            print(f"{key}: {value}")

    pass


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    
    departamento = input("Ingrese el nombre del departamento: ")
    N = int(input("Ingrese la cantidad de registros a mostrar: "))
    report = req_2(control, departamento, N)
    
    if report:
        print("Tiempo de ejecución:", report["execution_time"], "ms")
        print("Total de registros encontrados:", report["total_records"])
        print("Registros:")
        for record in report["records"]:
            for key, value in record.items():
                print(f"{key}: {value}")
            print("-")
    else:
        print("No se encontraron registros para el departamento especificado.")

def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    departamento = input("Ingrese el nombre del departamento: ").strip().upper()
    anio_inicial = int(input("Ingrese el año inicial (YYYY): ").strip())
    anio_final = int(input("Ingrese el año final (YYYY): ").strip())

    report = logic.req_3(control, departamento, anio_inicial, anio_final)

    if report:
        print("\nTiempo de ejecución:", report["execution_time"], "ms")
        print("Total de registros encontrados:", report["total_records"])
        print("Registros con fuente SURVEY:", report["total_survey"])
        print("Registros con fuente CENSUS:", report["total_census"])

        if report["total_records"] == 0:
            print("\nNo se encontraron registros para los criterios ingresados.")
        else:
            print("\nRegistros encontrados:")
            for reg in report["records"]:
                print(f"{reg['source_type']} | {reg['year_collection']} | {reg['load_date']} | {reg['frequency']} | {reg['product_type']} | {reg['unit']}")



def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    producto = input("ingrese el tipo de producto: ")
    anio_inicial = int(input("ingrese el año iniciald: "))
    anio_final = int(input("ingrese el año final: "))
    
    report = logic.req_4(control, producto, anio_inicial, anio_final)
    
    if report:
        print("Execution time:", report["execution_time"], "ms")
        print("Total records found:", report["total_records"])
        print("Records with SURVEY source:", report["total_survey"])
        print("Records with CENSUS source:", report["total_census"])
        print("List of records:")
        for record in report["records"]:
            for key, value in record.items():
                print(f"{key}: {value}")
            print("-")
    else:
        print("No hay recursos encontrados segun los criterios dados.")
    pass


def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    categoria = input("ingrese el tipo de producto: ")
    anio_inicial = int(input("ingrese el año iniciald: "))
    anio_final = int(input("ingrese el año final: "))
    
    report = logic.req_5(control, categoria, anio_inicial, anio_final)
    
    if report:
        print("Execution time:", report["execution_time"], "ms")
        print("Total records found:", report["total_records"])
        print("Records with SURVEY source:", report["total_survey"])
        print("Records with CENSUS source:", report["total_census"])
        print("List of records:")
        for record in report["records"]:
            for key, value in record.items():
                print(f"{key}: {value}")
            print("-")
    else:
        print("No hay recursos encontrados segun los criterios dados.")
    pass


def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    departamento = input("Ingrese el nombre del departamento: ")
    anio_inicial = int(input("Ingrese el año inicial (YYYY): "))
    anio_final = int(input("Ingrese el año final (YYYY): "))

    report = logic.req_6(control, departamento, anio_inicial, anio_final)

    if report:
        print("Tiempo de la ejecución:", report["execution_time"], "ms")
        print("Total registros encontrados:", report["total_records"])
        print("Total registros con fuente SURVEY:", report["survey_count"])
        print("Total registros con fuente CENSUS:", report["census_count"])

        for record in report["records"]:
            for key, value in record.items():
                print(f"{key}: {value}")
            print("-")
    else:
        print("No se encontraron registros para los criterios ingresados.")    
    
    
def print_req_7(control):
    """
        Función que imprime la solución del Requerimiento 7 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 7
    pass


def print_req_8(control):
    """
        Función que imprime la solución del Requerimiento 8 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 8
    N = int(input("Ingrese el número de departamentos a listar: "))
    order = input("Ingrese el tipo de ordenamiento (ASCENDENTE/DESCENDENTE): ").strip().upper()
    report = req_8(control, N, order)
    
    if report:
        print("Tiempo de ejecución:", report["execution_time"], "ms")
        print("Total de departamentos analizados:", report["total_departments"])
        for departamento in report["departments"]:
            print(f"Departamento: {departamento[0]}")
            print(f"Tiempo promedio entre recopilación y carga: {departamento[1]}")
            print(f"Número de registros: {departamento[2]}")
            print(f"Menor año de recopilación: {departamento[3]}")
            print(f"Mayor año de recopilación: {departamento[4]}")
            print(f"Menor tiempo entre recopilación y carga: {departamento[5]}")
            print(f"Mayor tiempo entre recopilación y carga: {departamento[6]}")


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
