# Copyright (C) 2022-2023 Alex Codoreanu
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
"""
General Notes:

This program is designed to access a large collection of netcdf files in the directory identified by the netcdf_path
variable. The goal of this program is to identify all PM25 grid cells around each entry in the vineyard file name and
then extract and concatenate all relevant data into a single csv for each vineyard. The output file will be defined by
the Site_ID column entries.

The search perimeter around the lat/long vineyard location is defined by the lat_boundary_size and lon_boundary_size
variables and can be adjusted as desired.

This program is designed for ease of use and is optimised for human readability. IT IS NOT OPTIMISED FOR COMPUTE.
There are several opportunities for future optimisation:
    - if the associated PM25 lat/long values are always identical then the distances and ranking need to be computed
    only once, saved as a struct through the scope of the program and then used as a hash key to assign the relevant
    variables inside the output structure.
    - the distance calculation itself could be further improved, in particular the use of abs() will slow this down at
    scale.

HOW TO USE THIS PROGRAM:
    1. Identify your desired grid size and change the lat/long_boundary_size values. The default is set around a 3km
    square.

    2. Ensure that you have the full name, including unix readable path, for your vineyard_file_name variable. Ideally,
    you would have the vineyard file name inside the same directory as this program.

    3. Ensure that you have the full unix readable path for the netcdf_path variable. A suggested directory structure:
            project_directory/
                this_program.py
                vineyard_location.csv
                data_directory/

    4. Identify if the relevant PM25 netcdf files have a different file identifier. Currently, an example file has the
    following naming structure: 20220313_PM25plus_vtas.nc. This means that we can use the "PM25plus_vtas" string subset
    to identify the files of interest. If this is no longer the case, just change the value of the netcdf_identifier
    variable to something that works in your case.

    5. Once you've done the above, you can run this as you would regularly run a python file. Options are using SPyder
    or any other IDEs. You can also navigate to this directory in a terminal shell and simply run:
        python extract_individual_pm_series.py

    6. If everything runs smoothly then you will have new csv files inside your project_directory. A future improvement
    could be the addition of a specific output path variable.


For any questions reach out to alexandru.codoreanu@delwp.vic.gov.au.
"""
import os
import xarray as xr
from pandas import read_csv

# assuming 3km grid
lat_boundary_size = 0.035
lon_boundary_size = 0.035

# name of vineyard file
vineyard_file_name = 'vineyard_xy.csv'
vineyard_data = read_csv('vineyard_xy.csv')

# location of netcdf files
netcdf_path = 'test_data'
net_cdf_files = os.listdir(netcdf_path)
netcdf_identifier = 'PM25plus_vtas'

for net_cdf_file in net_cdf_files:
    if netcdf_identifier in net_cdf_file:
        print(f'opening {net_cdf_file}')

        net_cdf_data = xr.open_dataset(f'{netcdf_path}/{net_cdf_file}')
        pm25 = net_cdf_data['PM25'].to_dataframe()

        # extract date from net_cdf_file and add it to the data
        date = net_cdf_file.split('_')[0]
        pm25['date'] = date

        # extract the hour/location data from the index and add it to the data
        location_data = pm25.index.to_frame()
        pm25['hour'] = location_data['time'].tolist()
        pm25['lat'] = location_data['lat'].tolist()
        pm25['lon'] = location_data['lon'].tolist()
        pm25.reset_index(drop=True, inplace=True)

        # drop hour 24 entries as they are 0 values and correspond to hour 0 entry in the following day
        drop_index = pm25.loc[pm25.hour == 24].index.tolist()
        pm25.drop(drop_index, inplace=True)

        for vineyard_row in vineyard_data.index.tolist():
            local_vineyard_info = vineyard_data.loc[vineyard_row]
            output_file_name = f'{local_vineyard_info.Site_ID}_PM25_data.csv'

            local_lat = local_vineyard_info.Lat
            local_long = local_vineyard_info.Long

            local_data = pm25.loc[
                (pm25.lat > local_lat - lat_boundary_size) &
                (pm25.lat < local_lat + lat_boundary_size) &
                (pm25.lon > local_long - lon_boundary_size) &
                (pm25.lon < local_long + lon_boundary_size)
            ]

            # now, compute distance between vineyard location and each weather grid
            local_data['lat_diff_sq'] = local_data.lat.apply(lambda x: (abs(x) - abs(local_lat))**2)
            local_data['lon_diff_sq'] = local_data.lon.apply(lambda x: (abs(x) - abs(local_long))**2)
            local_data['distance'] = local_data['lat_diff_sq'] + local_data['lon_diff_sq']
            local_data['distance'] = local_data.distance.apply(lambda x: x**0.5)
            local_data.loc[:, 'rank'] = 0

            distances = sorted(local_data.distance.unique().tolist())

            for index, value in enumerate(distances):
                local_data.loc[local_data.distance == value, 'rank'] = index

            # now drop the distance and lat/long diff columns
            local_data.drop(columns=['distance', 'lat_diff_sq', 'lon_diff_sq'], inplace=True)

            # write the data to the output file
            if os.path.isfile(output_file_name):
                local_data.to_csv(output_file_name, header=None, index=False, mode='a')
            else:
                local_data.to_csv(output_file_name, index=False)

            del local_data, distances, local_lat, local_long

