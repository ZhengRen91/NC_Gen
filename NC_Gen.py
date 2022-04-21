from scipy.spatial import Delaunay
from statistics import mean
from shapely.geometry import Polygon, Point,box
from shapely import ops
import datetime
import multiprocessing
from shapely import speedups
import fiona
import geopandas as gpd


def generate_nc(inurl, outurl):
    geodf = gpd.read_file(inurl, encoding='UTF-8')
    ndf = geodf['geometry']
    xcor = ndf.x
    ycor = ndf.y
    XY = list(zip(xcor, ycor))
    # print(XY)
    tin = Delaunay(XY)
    tricoords = coords_group(tin)
    tripolygons = create_polygons(tricoords)
    meanlength = get_mean(tripolygons)
    selected = select_polygons(meanlength, tripolygons)
    plist = list()
    dissolved = ops.unary_union(plist)
    nc = dissolved
    # Write a new Shapefile
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }
    with fiona.open(outurl, 'w', 'ESRI Shapefile',
                    schema) as c:
            c.write({
                'geometry': mapping(nc),
                'properties': {'id': 0},
            })
    print('\n')


def coords_group(tri):
    tript = tri.points
    triedge = tri.simplices
    coor_groups = [tript[x] for x in triedge]
    return coor_groups


def create_polygons(coor_groups):
    polygons = [Polygon(x) for x in coor_groups]
    return polygons


def select_polygons(mean, polygons):
    selected=[p for p in polygons if p.length<mean]
    return selected


def get_mean(polygons):
    meanlist = [m.length for m in polygons]
    meanvalue = mean(meanlist)
    return  meanvalue


if __name__ == '__main__':
    cpus = multiprocessing.cpu_count()
    print(cpus)
    speedups.enable()
    print(speedups.enabled)
    print(datetime.datetime.now())
    inputurl = r''
    outputurl = r''
    generate_nc(inputurl, outputurl)
    print(datetime.datetime.now())
