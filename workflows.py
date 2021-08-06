from core.base import get_sheetnames_excel_file
from pandas import read_excel
from glob import glob
from os.path import isfile
from pathlib import Path


def combine_FMP_export_files(
        data_folder=None,
        output_file=None,
        sheet_identifier='-FuelHazard',
        verbose=True):
    """
    This function creates a single report from a collection of individual outputs from 
    the Fuel Monitoring Portal.
    
    USAGE:
    
    from workflows import combine_FMP_export_files
    
    combine_FMP_export_files(
        data_folder='location_of_my_files', 
        output_file='my_combined_file.csv'
    )
    
    
    :param data_folder: Location of export data output. 
                        The expectation is that files are of type .xlsx with multiple tabs.
    :param output_file: Name of file where all data will 
    :param sheet_identifier: The string identifier for each relevant tab.
    :param verbose: If you want to have status information displayed.
    
    :return: 
    """

    try:
        input_files = glob(f'{data_folder}/*xlsx')
    except Exception as e:
        print(f'Failed to explore directory: {data_folder}\n\n {e}')
        return 1

    for input_file in input_files:
        if verbose:
            print(f'reading in {input_file}')
        sheetnames = get_sheetnames_excel_file(input_file)
        for local_sheet in sheetnames:
            if sheet_identifier in local_sheet:
                if verbose:
                    print(f'opening the following tab: {local_sheet}')

                local_data = read_excel(
                    input_file, sheet_name=local_sheet, engine='openpyxl'
                )

                local_data['input_file'] = input_file
                local_data['sheet_name'] = local_sheet

                if isfile(output_file):
                    local_data.to_csv(
                        output_file, header=False, index=False, mode='a'
                    )
                else:
                    local_data.to_csv(output_file, index=False)