################################################################################
# written by Francis Osei Tutu Afrifa, 2024.
################################################################################

### IMPORT NECESSARY PACKAGES ###
import pandas as pd
import geopandas as gp
import matplotlib.pyplot as plt
import cartopy.crs as crs
import cartopy.feature as cf
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER

def plot_map_of_visitor_count(CT_file, path_to_shapefile_CT, naics_name, county, save_file=False, save_directory=None):
    """
    CT_file: path
            A path of type str locating the Census Tract csv file to use.

    path_to_shapefile_CT: str
            Define the path to the Census Tracts Shapefile for the state or region of interest.

    naics_name: str
            A string of name of the NAICS code representing the business of interest. E.g. :'School'.

    county: str
            A string of name of the county of interest. E.g. :'Harris'.

    save_file: bool
            A boolean to determine if the plot should be saved as a file.

    save_directory: path
            A path of type str where the plot should be saved. Required if save_file is True.
    """
    file_ = pd.read_csv(CT_file)  # Read file with pandas
    
    # Select necessary columns to use
    file_.columns = ['date_range_start', 'date_range_end', 'TRACTCE', 'Home_TRACTCE',
                     'STATEFP', 'COUNTYFP', 'Visitor_Count', 'Home_LON', 'Home_LAT',
                     f'{naics_name}_LON', f'{naics_name}_LAT', 'Distance_Covered (km)']
    
    file_['TRACTCE'] = file_['TRACTCE'].astype(str)  # Convert series to type str

    gdf = gp.read_file(path_to_shapefile_CT)
    gdf_ = gdf.merge(file_[['date_range_start', 'date_range_end', 'TRACTCE', 'Home_TRACTCE', 'Visitor_Count']], on='TRACTCE')

    # Plot the choropleth map
    fig, ax = plt.subplots(1, 1, figsize=(12, 10), subplot_kw={'projection': crs.PlateCarree()})
    fig.patch.set_facecolor('xkcd:white')  # Set the background to white
    ax.add_feature(cf.BORDERS.with_scale('10m'), linewidth=2, edgecolor='black')
    ax.add_feature(cf.STATES.with_scale('10m'), linewidth=2, edgecolor='black', alpha=0.5)
    
    # Define color bar axes and add defined color bar
    cax = fig.add_axes([0.28, 0.02, 0.45, 0.04])
    gdf.boundary.plot(ax=ax, linewidth=1, color='k', facecolor="none", transform=crs.PlateCarree(), zorder=1, alpha=0.2)
    raw_visits_plot = gdf_.plot(column='Visitor_Count', cmap='jet', linewidth=1, ax=ax, edgecolor='0.8', vmin=0, vmax=20,
                                cax=cax, legend=True, legend_kwds={'orientation': 'horizontal', 'label': 'Number'}, zorder=10)
    
    # Set extent based on county location (adjust coordinates as needed)
    ax.set_extent([-95.95, -94.9, 29.45, 30.2])
    
    # Add grid lines
    #gl = ax.gridlines(draw_labels=True, alpha=0.6)
    gl = ax.gridlines(x_inline=False, alpha=0.6)
    gl.bottom_labels = True
    gl.left_labels = True
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER
    gl.xlabel_style = {'color': 'red', 'weight': 'bold', 'rotation': 45}
    gl.ylabel_style = {'color': 'green', 'weight': 'bold'}

    ax.set_title(f'Census Tract {naics_name} Visitors in {county} County ({file_["date_range_start"][0]} : {file_["date_range_end"][0]})', fontweight='bold')
    
    if save_file:
        if save_directory is None:
            raise ValueError("save_directory must be specified if save_file is True")
        save_path = f"{save_directory}/{file_['date_range_start'][0]} : {file_['date_range_end'][0]}visitor_count_map_{naics_name}_{county}.png"
        plt.savefig(save_path)
        print(f"Plot saved to {save_path}")
    else:
        plt.show()
