from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, DAILY
import os, sys
import harp
import numpy



# indicamos el bbox de la zona de descarga
footprint = geojson_to_wkt(read_geojson('bbox_spain.geojson'))

# parámetros de acceso a sentinelhub
api = SentinelAPI('s5pguest', 's5pguest', 'https://s5phub.copernicus.eu/dhus')


def cambiar_extension():
    """
    Función para cambiar la extensión de las escenas
    (de .zip a .nc)
    """

    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
      base_file, ext = os.path.splitext(filename)
      if ext == ".zip":
        os.rename(filename, base_file + ".nc")


def mosaico_escenas(nombre_nc):
    """
    Función para crear un mosaico de productos
    """

    # Buscamos en el directorio los productos S5P

    file_names = []
    products = [] # array con los productos a juntar

    print("Importar productos a HARP")


    i = 0
    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
      base_file, ext = os.path.splitext(filename)

      if ext == ".nc" and base_file.split('_')[0] == 'S5P':
        product_name = base_file + "_" + str(i)

        try:
            product_name = harp.import_product(base_file + ext,
                                                operations="latitude > -55 [degree_north]; latitude < 80 [degree_north]; tropospheric_NO2_column_number_density_validity > 75;bin_spatial(231,-55,0.5,721,-180,0.5)",
                                                post_operations="bin();squash(time, (latitude,longitude))")
            print("Producto " + base_file + ext + " importado")

            products.append(product_name)
        except:
            print ("Producto no importado")

        i = i + 1


    try:
        print("Ejecutando operación de unión")
        product_bin = harp.execute_operations(products, "", "bin()")
        #harp.export_product(product_bin, file_names[0].split('_')[8]+".nc")

        print("Exportar producto unido")
        harp.export_product(product_bin, str(nombre_nc)+".nc")

    except:
        print("No se ha podido realizar la unión")


    # Borramos los archivos originales, para no ocupar tanta memoria



    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
      base_file, ext = os.path.splitext(filename)

      if ext == ".nc" and base_file.split('_')[0] == 'S5P':
        os.remove(base_file + ext)




def descarga_datos(grupo_dias,parametros_acceso, bbox):
    """Función para la descarga de datos.

    Keyword arguments:
    grupo_dias -- se hará la media del grupo de dias definido
    parametros_acceso -- credenciales y url acceso al repositorio de imágenes
    bbox -- área de busqueda de las imagenes
    """


    dia_inicio = date(2019, 12, 23)  # fecha inicio
    dia_final = date(2020, 6, 29)   # fecha final

    numero_grupo = 1

    while dia_inicio <= dia_final:

        # se realizará una búsqueda por área, fecha y producto
        products = parametros_acceso.query(bbox,
                             date=(dia_inicio, dia_inicio + timedelta(grupo_dias)),
        		     		 producttype='L2__NO2___',
                             platformname='Sentinel-5')

        # mostramos por pantalla los productos encontrados
        #print(products)

        dia_inicio = dia_inicio + timedelta(grupo_dias)

        try:
            # iniciamos la descarga de los productos encontrados
            api.download_all(products)

            # una vez descargadas las imagenes, renombramos los archivos
            cambiar_extension()

            # realizamos el mosaico de escenas
            mosaico_escenas(dia_inicio - timedelta(grupo_dias))
            numero_grupo = numero_grupo + 1

        except:
            print("Error en la descarga y/o ejecución del proceso")


# llamamos la función para la descarga de datos
descarga_datos(7, api, footprint)

#cambiar_extension()
#mosaico_escenas('2020-6-8')
