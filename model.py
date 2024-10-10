# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:04:54 2024

@author: franz
"""

import rasterio
import geopandas as gpd
import numpy as np
import os
import tempfile
import shutil
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import random

def get_last_row_data(sheet_name, column_names):
    try:
        # Load credentials from the JSON file
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("actual-flow-data-7a129f577d25.json", scope)
        client = gspread.authorize(creds)

        # Open the specified sheet
        sheet = client.open(sheet_name).sheet1

        # Get all values in the sheet
        all_values = sheet.get_all_values()

        # Fetch data from the last row
        last_row_values = all_values[-1]

        # Create a dictionary with column names as keys
        data = {}
        for col, col_name in column_names.items():
            if -len(last_row_values) <= ord(col) - ord("A") < len(last_row_values):
                data[col_name] = last_row_values[ord(col) - ord("A")]
            else:
                data[col_name] = "Index out of range. Try again."

        return data

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    # Example usage
    sheet_name = "IOT flow meter 2"
    column_names = {"A": "Date", "B": "Real Time", "C": "FlowRate"}
    
    # List of values
    values = [0.28, 0.38, 0.27, 0.29, 0.27, 0.39, 0.40]

    while True:  # This is the start of the continuous loop
        # Define FW inside the while loop
        FW = None

        data = get_last_row_data(sheet_name, column_names)
        if float(data["FlowRate"]) > 0:  # Convert FlowRate to float before comparing
            FW = float(data['FlowRate'])  # Corrected here
        else:
            FW = random.choice(values)

        # Break the loop if FW is not None
        if FW is not None:
            break

        # Define the path of the files
        dem_path = r'/FLOOD_WEBMAP/Data/DTM.tif'
        river_shp_path = r'/FLOOD_WEBMAP/Data/River.shp'
        dam_shp_path = r'/FLOOD_WEBMAP/Data/Station.shp'  # Path of the dam shapefile
        output_tif_path = r'/tomcat/apache-tomcat-9.0.89/webapps/geoserver/data/data/DEM/water_dep.tif'  # Output path for the TIFF file

        # Load the DEM
        dem_data = rasterio.open(dem_path)
        dem_array = dem_data.read(1)
        dem_transform = dem_data.transform

        # Load the river shapefile
        river_data = gpd.read_file(river_shp_path)

        # Load the dam shapefile
        dam_data = gpd.read_file(dam_shp_path)

        # Verify and transform the projections of the vector layers to the CRS of the DEM
        river_data = river_data.to_crs(dem_data.crs)
        dam_data = dam_data.to_crs(dem_data.crs)

        # Now you can use FW here
        water_level = (FW * 25)  # meters above the river level

        # Calculate the water depth
        water_depth = np.maximum(0, (dem_array.min() + water_level) - dem_array)

        # Create a mask for zero values
        water_depth_masked = np.ma.masked_equal(water_depth, 0)

        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False)

        try:
            # Save the depth raster in a TIFF file with nodata
            with rasterio.open(
                temp_file.name,
                'w',
                driver='GTiff',
                height=water_depth.shape[0],
                width=water_depth.shape[1],
                count=1,
                dtype=water_depth.dtype,
                crs=dem_data.crs,
                transform=dem_transform,
                nodata=-9999  # Set the nodata value
            ) as dst:
                dst.write(water_depth_masked.filled(-9999), 1)  # Write the raster with nodata
        except IOError as e:
            print(f"Failed to write to file: {e}")
        finally:
            temp_file.close()

        # Replace the old file with the new one
        shutil.move(temp_file.name, output_tif_path)

        # Calculate the total volume of stored water using the saved depth raster
        with rasterio.open(output_tif_path) as depth_data:
            water_depth_array = depth_data.read(1)
            water_depth_masked = np.ma.masked_equal(water_depth_array, depth_data.nodata)

            # Calculate the area of each cell in square meters
            cell_area = abs(depth_data.transform[0] * depth_data.transform[4])

            # Calculate the total volume of stored water
            total_volume = np.sum(water_depth_masked) * cell_area  # Volume in cubic meters

            # Print the results
            print(f'The total volume of stored water is: {total_volume:.2f} cubic meters')
            print(f'Cell size used for the calculation: {abs(depth_data.transform[0])} x {abs(depth_data.transform[4])} meters')

        # Pause for 20 seconds
        time.sleep(20)
