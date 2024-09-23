import os
import difflib
import pandas as pd
from openpyxl import Workbook
import re
from dumpdbinfo import format_header_cell, adjust_column_widths


def compare_files(file1, file2, output_file):
    """
    Compares two text files using difflib and saves the differences to an output file only if there are differences.

    Parameters:
    -----------
    - file1 : str
        Path to the first file to compare.
    - file2 : str
        Path to the second file to compare.
    - output_file : str
        Path to the output file where the differences will be saved.

    Returns:
    --------
    int
        The number of lines with differences.
    """
    # Read the content of the files
    with open(file1, 'r') as f1:
        text1 = f1.readlines()
    
    with open(file2, 'r') as f2:
        text2 = f2.readlines()
    
    # Use difflib to generate the differences
    diff = list(difflib.unified_diff(text1, text2, fromfile=file1, tofile=file2, n=0))
    
    # Filter out lines that start with '---', '+++' or '@@'
    filtered_diff = [line for line in diff if not (line.startswith('---') or line.startswith('+++') or line.startswith('@@'))]

    # Add a newline character to the end of each element in filtered_diff if it doesn't already have one, except for the last element
    for i in range(len(filtered_diff) - 1):  # Exclude the last element
        if not filtered_diff[i].endswith('\n'):
            filtered_diff[i] += '\n'
            
    # Count the number of lines with differences
    diff_lines = len(filtered_diff)

    # Only save the differences if there are any
    if diff_lines > 0:
        with open(output_file, 'w') as output:
            output.writelines(filtered_diff)
    
    return diff_lines


def compare_folders_and_save_diffs(folder1, folder2, diff_folder):
    """
    Compares the files in two folders and saves the differences in a third folder.
    Returns a DataFrame with the name of the diff file and the number of lines with differences.

    Parameters:
    -----------
    - folder1 : str
        Path to the first folder.
    - folder2 : str
        Path to the second folder.
    - diff_folder : str
        Path to the folder where the diff files will be saved.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame with columns 'file_name', 'diff_file', 'diff_lines', and 'file_exists'.
    """

    # Create the diff folder if it doesn't exist
    if not os.path.exists(diff_folder):
        os.makedirs(diff_folder)
    
    # Get the list of text files in the first folder
    files1 = [f for f in os.listdir(folder1) if os.path.isfile(os.path.join(folder1, f))]

    # List to store the diff results
    diffs_data = []
    
    # Compare each file in the first folder with the corresponding file in the second folder
    for file_name in files1:
        file1_path = os.path.join(folder1, file_name)
        file2_path = os.path.join(folder2, file_name)

        # Check if the file also exists in the second folder
        if os.path.exists(file2_path):
            # Name of the diff file
            diff_file_name = f"diff_{os.path.splitext(file_name)[0]}.txt"
            diff_file_path = os.path.join(diff_folder, diff_file_name)
            
            # Call the comparison function and get the number of lines with differences
            diff_lines = compare_files(file1_path, file2_path, diff_file_path)
            
            # Add the result to the DataFrame 
            if diff_lines > 0:
                diffs_data.append({
                    'file_name': file_name,
                    'diff_file': diff_file_path,
                    'diff_lines': diff_lines,
                    'file_exists': True
                })
            else:
                diffs_data.append({
                    'file_name': file_name,
                    'diff_file': '',
                    'diff_lines': 0,
                    'file_exists': True
                })
        else:
            diffs_data.append({
                'file_name': file_name,
                'diff_file': '',
                'diff_lines': 0,
                'file_exists': False
            })
    
    # Create a DataFrame with the results
    diffs_df = pd.DataFrame(diffs_data, columns=['file_name', 'diff_file', 'diff_lines', 'file_exists'])

    return diffs_df


def generate_excel_from_diffs(folder1, folder2, diff_folder):
    """
    Genera un archivo Excel con las diferencias encontradas entre los archivos de dos carpetas.
    
    Parámetros:
    - folder1: Ruta a la primera carpeta.
    - folder2: Ruta a la segunda carpeta.
    - diff_folder: Ruta a la carpeta donde se guardarán los archivos de diferencias y el archivo Excel.
    
    El archivo Excel se guarda en 'diff_folder' con el nombre 'diff.xlsx'.
    """
    # Llama a la función para comparar las carpetas y obtener el dataframe
    diffs_df = compare_folders_and_save_diffs(folder1, folder2, diff_folder)
    
    # Solo procedemos si hay diferencias
    if not diffs_df.empty:
        # Crear un nuevo Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Differences"

        # Escribir la cabecera en el archivo Excel
        header = ['table_name', 'file_name', 'diff_file', 'diff_lines', 'file_exists']
        ws.append(header)

        # Formatear las celdas de cabecera
        for col_num, column_title in enumerate(header, start=1):
            cell = ws.cell(row=1, column=col_num)
            format_header_cell(cell)  # Formatear la celda de cabecera
        
        # Iterar a través del DataFrame y extraer la información
        for index, row in diffs_df.iterrows():
            file = row['file_name']
            diff_file = row['diff_file']
            diff_lines = row['diff_lines']
            file_exists = row['file_exists']
            
            # Obtener el nombre de la tabla desde el nombre del fichero
            # table_name_match = re.search(r"diff_(.+?)__", diff_file)
            table_name_match = re.match(r"(.+?)__", file)
            table_name = table_name_match.group(1) if table_name_match else "Unknown"

            # Obtener solo el nombre del archivo para mostrar
            file_name = os.path.basename(diff_file)

            # Escribir la fila en el archivo Excel
            ws.append([table_name, file, diff_file, diff_lines, file_exists])

            # Configurar el hipervínculo en la celda 'diff_file'
            diff_file_cell = ws.cell(row=index + 2, column=3, value=file_name)  # +2 debido a la cabecera y la base 1 de openpyxl
            diff_file_cell.hyperlink = diff_file  # Establecer el hipervínculo a la ruta completa del archivo
            diff_file_cell.style = "Hyperlink"  # Estilo de hipervínculo para visualización


        # Inmovilizar la primera fila (cabecera)
        ws.freeze_panes = ws['A2']        
        
        # Auto-size columns 
        adjust_column_widths(ws)

        # Aplicar autofiltro a todas las columnas
        ws.auto_filter.ref = ws.dimensions

        # Guardar el archivo Excel en la carpeta diff_folder
        excel_path = os.path.join(diff_folder, 'diff.xlsx')
        wb.save(excel_path)











