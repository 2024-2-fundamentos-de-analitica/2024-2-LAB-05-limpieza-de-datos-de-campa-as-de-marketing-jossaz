"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import os
import glob
import zipfile


def open_data():
    # Ruta hacia input
    path = 'files/input'

    # Se abren todos los .zip con glob
    zip_files = glob.glob(f'{path}/*.zip')

    # Se itera sobre cada archivo .zip par abrir sus csv.
    for zfile in zip_files:

        # Se abre el archivo .zip
        with zipfile.ZipFile(zfile,'r') as handler:
            # Se toman solo los archivos .csv
            csv_files = [file for file in handler.namelist() if file.endswith('.csv')]

            # Se itera sobre cada archvo .csv de cada .zip
            for csv_file in csv_files:
                with handler.open(csv_file) as file:
                    df = pd.read_csv(file)
                    # Se limpian los datos de cliente y se crea el archivo
                    update_client(df)

                    # Se limpian los datos de campaign y se crea el archivo
                    update_campaign(df)

                    # Se limpian los datos de economics y se crea el archivo
                    update_economics(df)
                continue

def update_client(df):
    # Vamos a limpiar los datos y quedarnos con lo q nos interesa
    columnas = ['client_id','age','job','marital','education','credit_default','mortgage']
    df = df[columnas]

    # Job: Cambiamos . por '', y - por _
    df['job'] = df['job'].str.replace('.','')
    df['job'] = df['job'].str.replace('-','_')

    # Education: Cambiamos . por _, y unknown por pd.Na
    df['education'] = df['education'].str.replace('.','_')
    df['education'] = df['education'].replace('unknown',pd.NA)

    # credit_default: Yes a 1, y cualquier otro valor a 0
    df['credit_default'] = df['credit_default'].replace('yes',1)
    df['credit_default'] = df['credit_default'].replace('no',0)
    df['credit_default'] = df['credit_default'].replace('unknown',0)

    # mortgage: Yes a 1, y cualquier otro valor a 0
    df['mortgage'] = df['mortgage'].replace('yes',1)
    df['mortgage'] = df['mortgage'].replace('no',0)
    df['mortgage'] = df['mortgage'].replace('unknown',0)


    #
    # Ahora guardamos los datos
    #
    # Si no existe la ubicación de guarado, crearla
    ruta = 'files/output/client.csv'
    
    # Crea la carpeta output
    if not os.path.exists('files/output'):
        os.makedirs('files/output')
    
    # Verificamos si el archivo existe(para saber si incluir la cabecera o no)
    file_exists = os.path.exists(ruta)
    # Crea los archivos csv(modo a => append, no sobreescribe lo que haya)
    df.to_csv(ruta,mode = 'a', index=False, header=not file_exists)


def update_campaign(df):
    # Vamos a limpiar los datos y quedarnos con lo q nos interesa
    columnas = ['client_id','number_contacts','contact_duration','previous_campaign_contacts','previous_outcome',
                'campaign_outcome','last_contact_date']
    

    # previous_outcome: cambiar "success" por 1, y cualquier otro valor a 0
    df['previous_outcome'] = df['previous_outcome'].replace('success',1)
    df['previous_outcome'] = df['previous_outcome'].replace('nonexistent',0)
    df['previous_outcome'] = df['previous_outcome'].replace('failure',0)

    # - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    df['campaign_outcome'] = df['campaign_outcome'].replace('yes',1)
    df['campaign_outcome'] = df['campaign_outcome'].replace('no',0)

    # - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
    #     combinando los campos "day" y "month" con el año 2022
    df['last_contact_date'] = pd.to_datetime('2022-' + df['month'].astype(str) + '-' + df['day'].astype(str))


    df = df[columnas]


    #
    # Ahora guardamos los datos
    #
    # Si no existe la ubicación de guarado, crearla
    ruta = 'files/output/campaign.csv'
    # Crea la carpeta output
    if not os.path.exists('files/output'):
        os.makedirs('files/output')

    # Verificamos si el archivo existe(para saber si incluir la cabecera o no)
    file_exists = os.path.exists(ruta)
    # Crea los archivos csv(modo a => append, no sobreescribe lo que haya)
    df.to_csv(ruta,mode = 'a', index=False, header=not file_exists)



def update_economics(df):
    # Vamos a limpiar los datos y quedarnos con lo q nos interesa
    columnas = ['client_id','cons_price_idx','euribor_three_months']
    df = df[columnas]

    ruta = 'files/output/economics.csv'
    # Crea la carpeta output
    if not os.path.exists('files/output'):
        os.makedirs('files/output')
    
    # Verificamos si el archivo existe(para saber si incluir la cabecera o no)
    file_exists = os.path.exists(ruta)
    # Crea los archivos csv(modo a => append, no sobreescribe lo que haya)
    df.to_csv(ruta,mode = 'a', index=False, header=not file_exists)





def clean_campaign_data():
    path = 'files/output'
    if os.path.exists(path):
        archivos = glob.glob(f'{path}/*.csv')
        for archivo in archivos:
            os.remove(archivo)

        os.rmdir(path)

    open_data()
# """
# En esta tarea se le pide que limpie los datos de una campaña de
# marketing realizada por un banco, la cual tiene como fin la
# recolección de datos de clientes para ofrecerls un préstamo.

# La información recolectada se encuentra en la carpeta
# files/input/ en varios archivos csv.zip comprimidos para ahorrar
# espacio en disco.

# Usted debe procesar directamente los archivos comprimidos (sin
# descomprimirlos). Se desea partir la data en tres archivos csv
# (sin comprimir): client.csv, campaign.csv y economics.csv.
# Cada archivo debe tener las columnas indicadas.

# Los tres archivos generados se almacenarán en la carpeta files/output/.

# client.csv:
# - client_id
# - age
# - job: se debe cambiar el "." por "" y el "-" por "_"
# - marital
# - education: se debe cambiar "." por "_" y "unknown" por pd.NA
# - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
# - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

# campaign.csv:
# - client_id
# - number_contacts
# - contact_duration
# - previous_campaing_contacts
# - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
# - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
# - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
# combinando los campos "day" y "month" con el año 2022.

# economics.csv:
# - client_id
# - const_price_idx
# - eurobor_three_months



# """

# return


# # # # if __name__ == "__main__":
# # # #     clean_campaign_data()
