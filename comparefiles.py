import os
import difflib
import pandas as pd


def compare_files(file1, file2, output_file):
    """
    Compara dos archivos de texto utilizando difflib y guarda las diferencias en un archivo de salida solo si hay diferencias.
    
    Parámetros:
    - file1: Ruta al primer archivo a comparar.
    - file2: Ruta al segundo archivo a comparar.
    - output_file: Ruta al archivo de salida donde se guardarán las diferencias.
    
    Devuelve:
    - int: Número de líneas con diferencias.
    """
    # Leer el contenido de los archivos
    with open(file1, 'r', encoding='utf-8') as f1:
        text1 = f1.readlines()
    
    with open(file2, 'r', encoding='utf-8') as f2:
        text2 = f2.readlines()
    
    # Usar difflib para generar las diferencias
    diff = list(difflib.unified_diff(text1, text2, fromfile=file1, tofile=file2, n=0))
    
    # Filtrar las líneas que no empiezan con '---', '+++' o '@@'
    filtered_diff = [line for line in diff if not (line.startswith('---') or line.startswith('+++') or line.startswith('@@'))]
    
    # Contar el número de líneas de diferencias
    diff_lines = len(filtered_diff)

    # Solo guardar las diferencias si hay alguna
    if diff_lines > 0:
        with open(output_file, 'w', encoding='utf-8') as output:
            output.writelines(filtered_diff)
    
    return diff_lines


def compare_folders_and_save_diffs(folder1, folder2, diff_folder):
    """
    Compara los archivos de dos carpetas y guarda las diferencias en una tercera carpeta.
    Devuelve un dataframe con el nombre del fichero de diferencias y el número de líneas con diferencias.
    
    Parámetros:
    - folder1: Ruta a la primera carpeta.
    - folder2: Ruta a la segunda carpeta.
    - diff_folder: Ruta a la carpeta donde se guardarán los archivos de diferencias.
    
    Devuelve:
    - pandas.DataFrame: Un dataframe con las columnas 'diff_file' y 'diff_lines'.
    """
    # Crear la carpeta de diferencias si no existe
    if not os.path.exists(diff_folder):
        os.makedirs(diff_folder)
    
    # Obtener la lista de archivos de texto en la primera carpeta
    files1 = [f for f in os.listdir(folder1) if os.path.isfile(os.path.join(folder1, f))]

    # Lista para almacenar los resultados de las diferencias
    diffs_data = []
    
    # Comparar cada archivo en la primera carpeta con el correspondiente en la segunda carpeta
    for file_name in files1:
        file1_path = os.path.join(folder1, file_name)
        file2_path = os.path.join(folder2, file_name)

        # Verificar si el archivo también existe en la segunda carpeta
        if os.path.exists(file2_path):
            # Nombre del archivo de diferencias
            diff_file_name = f"diff_{os.path.splitext(file_name)[0]}.txt"
            diff_file_path = os.path.join(diff_folder, diff_file_name)
            
            # Llamar a la función de comparación y obtener el número de líneas con diferencias
            diff_lines = compare_files(file1_path, file2_path, diff_file_path)
            
            # Añadir al dataframe si hay diferencias
            if diff_lines > 0:
                diffs_data.append({
                    'diff_file': diff_file_name,
                    'diff_lines': diff_lines
                })
    
    # Crear un dataframe con los resultados
    diffs_df = pd.DataFrame(diffs_data, columns=['diff_file', 'diff_lines'])
    print(diffs_data)
    return diffs_df












