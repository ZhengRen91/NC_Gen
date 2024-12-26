import osmnx as ox
import geopandas as gpd

# download buildings roads and poi from osm using osmnx
def download_buildings():
    # download buildings from osm
    gdf = ox.features_from_place('Vienna, Austria', tags={'building': True})
    print(gdf.head())
    print(len(gdf))
    gdfnew = gdf[['geometry']]
    gdfnew.to_file(r'C:\zhgren\LivingCitiesPop\Data\osmdata\Vienna_buildings.gpkg', driver="GPKG")
    print('Done')

def downloadroads():
    G = ox.graph_from_place("Rome, Italy", network_type="drive")
    ox.save_graph_geopackage(G, filepath=r"C:\zhgren\ResilienceRoads\osmroads\Rome.gpkg")
    print('Done')


def download_POI():
    # download POI from osm
    gdf = ox.features_from_place('Vienna, Austria', tags={'amenity': True})
    print(gdf.head())
    print(len(gdf))
    gdfnew = gdf[['geometry', 'amenity']]
    gdfnew.to_file(r'C:\zhgren\LivingCitiesPop\Data\osmdata\Vienna_POI.gpkg', driver="GPKG")
    print('Done')


def download_adminbound():
    city = ox.geocode_to_gdf('London, UK')
    city.to_file(r"C:\zhgren\LivingCitiesPop\Data\LondonCase\LDadminbound.shp", driver="ESRI Shapefile")
    ax = ox.project_gdf(city).plot()
    plt.show()


def downloadroadswithinshp():
    boundshp = gpd.read_file(r"C:\zhgren\LivingCitiesPop\Data\LondonCase\LondonEnvelope.shp")
    boundary = boundshp['geometry'].iloc[0]
    G = ox.graph_from_polygon(boundary, network_type="drive")
    ox.save_graph_geopackage(G, filepath=r"C:\zhgren\LivingCitiesPop\Data\LondonCase\LondonStreets.gpkg")
    print('Done')


def download_POI_withinshp():
    # download POI from osm
    boundshp = gpd.read_file(r"C:\zhgren\LivingCitiesPop\Data\LondonCase\LondonEnvelope.shp")
    boundary = boundshp['geometry'].iloc[0]
    gdf = ox.features_from_polygon(boundary, tags={'amenity': True})
    print(gdf.head())
    print(len(gdf))
    gdfnew = gdf[['geometry', 'amenity']]
    gdfnew.to_file(r'C:\zhgren\LivingCitiesPop\Data\LondonCase\LondonPOI.gpkg', driver="GPKG")
    print('Done')