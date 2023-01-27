import json
import numpy as np
import pandas as pd
from scipy.spatial import Delaunay
from shapely.geometry import mapping
from shapely import ops
from statistics import mean
import datetime
import multiprocessing
from pyproj import Proj, transform
from rtree import index
from shapely import speedups
from itertools import accumulate
import geopandas
import os
import fiona
from shapely.geometry import shape
from itertools import groupby
from os import listdir
from shapely.geometry import Polygon, Point
import dask_geopandas
import dask


starttime = datetime.datetime.now()
dissall = []

def readxyfromcsv():
    df = pd.read_csv(r'C:\zhgren\Tweets\TweetWorld_1day\world_day1_date.csv', sep=',', header=None)
    print(df)
    xypair = df.as_matrix(columns=(2, 1))
    return xypair

def uniquexyfromcsv():
    df = pd.read_csv(r'C:\zhgren\Tweets\TweetWorld_1day\world_day1_date.csv', sep=',', header=None)
    print(len(df))
    udf = df.drop_duplicates([1,2])
    print(len(udf))
    from datetime import timedelta
    udf=udf.copy()
    t = pd.to_datetime(udf[3])-pd.to_timedelta(5,unit='h')
    udf[3] = t
    udf = udf.round(6)
    print(udf.head(5))
    udf.to_csv(r'C:\zhgren\Tweets\TweetWorld_1day\world_day1_sort.csv',header=None)

def dividetime():
    df = pd.read_csv(r'C:\zhgren\Tweets\TweetWorld_1day\world_day1_proj.csv', sep=',', header=None,nrows=2000)
    print(len(df))
    df[3] = pd.to_datetime(df[3])
    print(df[3])
    x=df.set_index(df[3]).groupby([pd.Grouper(freq='Min')]).size().to_frame(name='counts')

    interval=x['counts'].tolist()
    accumulist = list(accumulate(map(int, interval)))
    print(accumulist)

    # with open(r'C:\zhgren\Tweets\TweetWorld_1day\TimeindexMin.txt', 'w') as f:
    #     for item in accumulist:
    #         f.write("%s\n" % item)
    # print('done')

def sorttime():
    df = pd.read_csv(r'C:\zhgren\Tweets\TweetWorld_1day\world_day1_proj.csv', sep=',', header=None)
    print(len(df))
    df[3] = pd.to_datetime(df[3])
    df = df.sort_values(3)
    df.to_csv(r'C:\zhgren\Tweets\TweetWorld_1day\world_day1_sorttime.csv',header=None)
    print(len(df))

def projconversion(xypair):
    p1 = Proj(init='epsg:4326') #WGS84
    p2 = Proj(init='epsg:3857') #Web Mercator
    x,y = transform(p1,p2,xypair[:,0],xypair[:,1])
    newxypair = np.matrix([x, y]).transpose() # transpose the xy ndarray to the original form of the xypair
    print (newxypair)
    return newxypair

def readxyfromtxt():
    xypair = np.genfromtxt(r'C:\zhgren\BrightKite\xydata\XYList.txt', delimiter='\t')
    return xypair

def createdelaunay(pointsarray):
    tri = Delaunay(pointsarray)
    # plt.triplot(points[:,0], points[:,1], tri.simplices)
    # plt.plot(points[:,0], points[:,1], 'o')
    # plt.show()
    return tri

def coordgroup(tri):
    tript = tri.points
    triedge = tri.simplices
    coor_groups = [tript[x] for x in triedge]
    return coor_groups

def createpolygon(coor_groups):
    polygons = [Polygon(x) for x in coor_groups]
    return polygons

def calculatemean(polygons):
    perimeter = [polygons[i].length for i in range(0, len(polygons))];
    meanLength = mean(perimeter)
    return meanLength

def selectpolygons(mean, polygons):
    selected=[p for p in polygons if p.length<mean]
    return selected

def validationpolygon(selected):
    validated = [x for x in selected if x.is_valid is True]
    return validated

def aggregation():

    # List of non-overlapping polygons
    polygons = [
        Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
        Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
    ]

    # Populate R-tree index with bounds of polygons
    idx = index.Index()
    for pos, poly in enumerate(polygons):
        idx.insert(pos, poly.bounds)

    # Query a point to see which polygon it is in
    # using first Rtree index, then Shapely geometry's within
    point = Point(0.5, 0.2)
    poly_idx = [i for i in idx.intersection((point.coords[0]))
                if point.within(polygons[i])]
    for num, idx in enumerate(poly_idx, 1):
        print("%d:%d:%s" % (num, idx, polygons[idx]))

def readtwitter():
    df = pd.read_csv(r'C:\zhgren\Tweets\TweetWorld_1day\world_day1_sorttime.csv', sep=',', header=None,names=['id','uid','y','x','time'])
    #xypair = df.as_matrix(columns=(3, 2))
    df = df.round(decimals=4)
    return df

# data processing US buildings

def rename():
    filelist = [f for f in os.listdir(r'C:\zhgren\US_Buildings\OSMData')]
    print(filelist)
    newfilelist = [f[:len(f) - 16] for f in filelist]
    print(newfilelist)

    for x in range(2,len(filelist)):
        oldfile = os.path.join(r'C:\zhgren\US_Buildings\OSMData', filelist[x])
        print(oldfile)
        newfile = os.path.join(r'C:\zhgren\US_Buildings\OSMData', newfilelist[x])
        print(newfile)
        os.rename(oldfile, newfile)

def choosebuilding():
    filelist = [f for f in os.listdir(r'C:\zhgren\US_Buildings\OSMData')]
    print(filelist)
    for f in filelist:
        foldername = os.path.join(r'C:\zhgren\US_Buildings\OSMData', f)
        shplist = [s for s in os.listdir(foldername)]
        print(shplist)

        for s in shplist:
            if s[:26] == 'gis_osm_buildings_a_free_1':
                print('building')
                ext = s[26:]
                os.rename(os.path.join(foldername,s), os.path.join(foldername,f+ext))
            else:
                os.remove(os.path.join(foldername, s))

def movefiles():
    filelist = [f for f in os.listdir(r'C:\zhgren\US_Buildings\MicrosoftData')]
    print(filelist)
    for f in filelist:
        foldername = os.path.join(r'C:\zhgren\US_Buildings\MicrosoftData', f)
        shplist = [s for s in os.listdir(foldername)]
        print(shplist)
        for s in shplist:
            os.rename(os.path.join(foldername, s),os.path.join(r'C:\zhgren\US_Buildings\MicrosoftData',s))

def geojson2shp():
    filelist = [f for f in os.listdir(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\southmid')]
    for s in filelist:
        url = os.path.join(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\southmid', s)
        print(url)
        fname = s[:len(s) - 8] + '.shp'
        newname = os.path.join(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\southmidshp', fname)
        print(newname)
        df = geopandas.read_file(url)

        df.to_file(newname, driver='ESRI Shapefile')
        print(datetime.datetime.now()-starttime)

def geojson2shpbig():
    inputfile = r'C:\zhgren\CityCenterDetection\USdata\US_building2020\flo\Florida.geojson'
    shpname = r'C:\zhgren\CityCenterDetection\USdata\US_building2020\floshp\Florida2.shp'
    data = pd.read_table(inputfile, nrows=3263194, skiprows=4000004, header=None) #total 11542916line
    datalist = list()
    geomlist = list()
    for index, row in data.iterrows():
        jsdata = json.loads(row[0][:-1])
        datalist.append(jsdata)
        poly = Polygon(jsdata['geometry']['coordinates'][0])
        geomlist.append(poly)
    # print(geomlist)
    df = pd.DataFrame(datalist)
    gdf = geopandas.GeoDataFrame(df, geometry=geomlist)
    gdf.to_file(shpname, driver='ESRI Shapefile')
    print(datetime.datetime.now() - starttime)

def getbuildingpoint():
    filelist = [f for f in os.listdir(r'C:\zhgren\US_Buildings\MicrosoftData\gjson')]
    for f in filelist:
        url = os.path.join(r'C:\zhgren\US_Buildings\MicrosoftData\gjson', f)
        fname = f[:len(f) - 8] + '.csv'
        newname = os.path.join(r'C:\zhgren\US_Buildings\MSPT', fname)
        print(newname)
        df = geopandas.read_file(url)
        ptlist = df.centroid
        xlist = ptlist.x
        ylist = ptlist.y
        xydf = pd.DataFrame(data={"x": xlist, "y": ylist})
        xydf.to_csv(newname, sep=',', index=False)
        print(datetime.datetime.now() - starttime)

def getbuildingpoint2():
    shplist = [f for f in os.listdir(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\texshp')]
    for s in shplist:
        if(s[-4:]=='.shp'):
            url = os.path.join(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\texshp', s)
            sh = fiona.open(url)
            xlist = list()
            ylist = list()

            for g in sh:
                shpgeom = shape(g['geometry'])
                cpt = shpgeom.centroid
                xlist.append(cpt.x)
                ylist.append(cpt.y)

            cname = s[:len(s) - 4] + '.csv'
            newname = os.path.join(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\buildingcenter', cname)

            xydf = pd.DataFrame(data={"x": xlist, "y": ylist})
            xydf.to_csv(newname, sep=',', index=False)
            print(newname)

def readxyfromcsv2(url):
    df = pd.read_csv(url, sep=',')
    xypair = df.values
    return xypair

def projconversion2(xypair):
    p1 = Proj(init='epsg:4326') #WGS84
    p2 = Proj(init='epsg:5070') #NAD83 / Conus Albers
    # x, y = transform(p1, p2, xypair[:, 0], xypair[:, 1])
    # in case of Future syntax warning
    x, y = transform(p1, p2, xypair[:, 0], xypair[:, 1], always_xy=True)
    newxypair = np.matrix([x, y]).transpose() # transpose the xy ndarray to the original form of the xypair
    return newxypair

def batchproject():
    filelist = [f for f in os.listdir(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\test')]
    for pt in filelist:
        url = os.path.join(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\test', pt)
        xy = readxyfromcsv2(url)
        projxy = projconversion2(xy)
        xydf = pd.DataFrame(projxy, columns=['x', 'y'])
        newname =pt[:len(pt) - 4] + '_proj.csv'
        newurl = os.path.join(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\testproj', newname)
        xydf.to_csv(newurl, sep=',', index=False)
        print(newname)
        print(datetime.datetime.now()-starttime)

def mergecsv():
    files = [f for f in os.listdir(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_chunks\SW')]
    mergedf = pd.DataFrame()
    for f in files:
        url = os.path.join(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_chunks\SW', f)
        print(url)
        currentdf = pd.read_csv(url,header=0,sep=',')
        mergedf = pd.concat([mergedf,currentdf])
        print(f)
        print(len(mergedf))

    mergedf.to_csv(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_chunks\SW\SW_PT_chunk.csv', sep=',', index=False)

def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]

def fixTopoError(polygons):
    fixlist=[f.buffer(0) for f in polygons]
    return fixlist

def checknonull(geoms):
    nonull = []
    for g in geoms:
        if g is not None:
            nonull.append(g)
    return nonull



def readshp(files):
    alllist = []
    for f in files:
        with fiona.open(f, 'r') as shp:
            for feature in shp:
                geom = shape(feature['geometry'])
                alllist.append(geom)
    print(len(alllist))

    return alllist

def list_files(directory, extension):
    return (directory+'\\'+f for f in listdir(directory) if f.endswith('.' + extension))

def createNC():
    url = r'C:\zhgren\US_Buildings\MSPTChunks\M_chunk.csv'
    xy = readxyfromcsv2(url)
    print(len(xy))
    print(datetime.datetime.now() - starttime)

    tri = createdelaunay(xy)
    print(datetime.datetime.now() - starttime)
    tricoords = coordgroup(tri)
    tripolygons = createpolygon(tricoords)
    print(datetime.datetime.now() - starttime)
    meanlength = calculatemean(tripolygons)
    print(meanlength)
    selectedtri = selectpolygons(meanlength, tripolygons)
    print(len(selectedtri))
    print(datetime.datetime.now() - starttime)
    dispatch_jobs(selectedtri, 16)
    dissall = readshp(list_files(r'C:\zhgren\US_Buildings\MSNC', 'shp'))
    dissolved = ops.unary_union(dissall)
    print(len(dissolved))

    exteriorlist = list()
    for diss in dissolved:
        exter = Polygon(diss.exterior)
        exteriorlist.append(exter)
    print(datetime.datetime.now() - starttime)
    # Write a new Shapefile
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }
    with fiona.open(r'C:\zhgren\US_Buildings\MSNC\MidNC.shp', 'w', 'ESRI Shapefile',
                    schema) as c:
        for index, poly in enumerate(exteriorlist):
            c.write({
                'geometry': mapping(poly),
                'properties': {'id': index},
            })
    print(datetime.datetime.now() - starttime)
    print('\n')

def createNC2():
    url = r'D:\pythongeo\MSPTChunks\NE2_chunk.csv'
    xy = readxyfromcsv2(url)
    print(len(xy))
    print(datetime.datetime.now() - starttime)

    tri = createdelaunay(xy)
    print(datetime.datetime.now() - starttime)
    tricoords = coordgroup(tri)
    tripolygons = createpolygon(tricoords)
    print(datetime.datetime.now() - starttime)
    meanlength = 800
    print(meanlength)
    selectedtri = selectpolygons(meanlength, tripolygons)
    print(len(selectedtri))
    print(datetime.datetime.now() - starttime)

    dispatch_jobs(selectedtri, 18)

    dissall = readshp(list_files(r'D:\pythongeo\MSNC2', 'shp'))
    dissolved = ops.unary_union(dissall)
    print(len(dissolved))

    exteriorlist = list()
    for diss in dissolved:
        exter = Polygon(diss.exterior)
        exteriorlist.append(exter)
    print(datetime.datetime.now() - starttime)
    # Write a new Shapefile
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }
    with fiona.open(r'D:\pythongeo\MSNC2\NENC2.shp', 'w', 'ESRI Shapefile',
                    schema) as c:
        for index, poly in enumerate(exteriorlist):
            c.write({
                'geometry': mapping(poly),
                'properties': {'id': index},
            })
    print(datetime.datetime.now() - starttime)
    print('\n')

def missing_elements(L):
    start, end = L[0], L[-1]
    return sorted(set(range(start, end + 1)).difference(L))

def spatialjoin():
    ncurl = r'D:\pythongeo\USNC\USNC2.shp'
    with fiona.open(ncurl,'r',encoding='UTF-8') as nc:
        polygonlist = []
        for feature in nc:
            geom = shape(feature['geometry'])
            polygonlist.append(geom)
        print(len(polygonlist))
    print(datetime.datetime.now() - starttime)

    pturl = r'D:\pythongeo\MSPTChunks\NE2_chunk.csv'
    df = pd.read_csv(pturl,low_memory=False)

    ptlist = [Point(xy) for xy in zip(df.x, df.y)]
    ptcorlist = [xy for xy in zip(df.x, df.y)]

    print(ptcorlist[0])
    print(len(ptlist))
    print(datetime.datetime.now() - starttime)

    idx = index.Index()
    count = -1
    for q in polygonlist:
        count +=1
        idx.insert(count,q.bounds)

    matches=[]
    for i in range(len(ptlist)):
        temp = -1
        for j in idx.intersection(ptcorlist[i]):
            # use buffer intersect will be valid at last decimal of points, the least number of points is 3.
            #print(ptlist[i])
            if ptlist[i].buffer(0.000001).intersects(polygonlist[j]):
                temp = j
                matches.append(temp)
        print(i)

    matches.sort()
    samelist = list(set(matches))
    missinglist = missing_elements(samelist)
    print(missinglist)

    frq = [len(list(group)) for key, group in groupby(matches)]
    with open(r'D:\pythongeo\USNCJoinCount\NE2Count.txt', 'w') as f:
        for item in frq:
            f.write("%s\n" % item)
    print(datetime.datetime.now() - starttime)

def spatialjoinbatch():
    urllist = list_files(r'D:\pythongeo\StateNC3','shp')
    for url in urllist:

        statename = url[22:len(url)-4]
        print(statename)
        with fiona.open(url, 'r', encoding='UTF-8') as nc:
            polygonlist = []
            for feature in nc:
                geom = shape(feature['geometry'])
                polygonlist.append(geom)
            print(len(polygonlist))
        print(datetime.datetime.now() - starttime)
        statecsv = statename+'.csv'
        pturl = os.path.join(r'D:\pythongeo\MSPTProj\NE', statecsv)
        print(pturl)
        df = pd.read_csv(pturl, low_memory=False)

        ptlist = [Point(xy) for xy in zip(df.x, df.y)]
        ptcorlist = [xy for xy in zip(df.x, df.y)]

        print(ptcorlist[0])
        print(len(ptlist))
        print(datetime.datetime.now() - starttime)

        idx = index.Index()
        count = -1
        for q in polygonlist:
            count += 1
            idx.insert(count, q.bounds)

        matches = []
        for i in range(len(ptlist)):
            for j in idx.intersection(ptcorlist[i]):
                # use buffer intersect will be valid at last decimal of points, the least number of points is 3.
                if ptlist[i].buffer(0.000001).intersects(polygonlist[j]):
                    matches.append(j)
            print(i)

        matches.sort()
        frq = [len(list(group)) for key, group in groupby(matches)]
        txtfile =statename+'.txt'
        with open(os.path.join(r'D:\pythongeo\NCJoinCount', txtfile), 'w') as f:
            for item in frq:
                f.write("%s\n" % item)
        print(datetime.datetime.now() - starttime)

def spatialjoinbatch2():
    url = r'D:\pythongeo\USjoin\USNC_MergeH1JoinTweets.shp'

    with fiona.open(url, 'r', encoding='UTF-8') as nc:
        polygonlist = []
        for feature in nc:
            geom = shape(feature['geometry'])
            polygonlist.append(geom)
        print(len(polygonlist))
        print(datetime.datetime.now() - starttime)


    pturl = r'D:\pythongeo\USPT_Merge.csv'
    print(pturl)
    df = pd.read_csv(pturl, low_memory=False)

    ptlist = [Point(xy) for xy in zip(df.x, df.y)]
    ptcorlist = [xy for xy in zip(df.x, df.y)]

    print(ptcorlist[0])
    print(len(ptlist))
    print(datetime.datetime.now() - starttime)

    idx = index.Index()
    count = -1
    for q in polygonlist:
        count += 1
        idx.insert(count, q.bounds)

    matches = []
    for i in range(len(ptlist)):
        for j in idx.intersection(ptcorlist[i]):
            # use buffer intersect will be valid at last decimal of points, the least number of points is 3.
            if ptlist[i].buffer(0.000001).intersects(polygonlist[j]):
                matches.append(j)
        print(i)

    matches.sort()
    frq = [len(list(group)) for key, group in groupby(matches)]
    txtfile = 'USBuildingJoin'
    with open(os.path.join(r'D:\pythongeo\NCJoinCount', txtfile), 'w') as f:
        for item in frq:
            f.write("%s\n" % item)
    print(datetime.datetime.now() - starttime)

def readUSPT():
    df = pd.read_csv(r'D:\pythongeo\MSPTChunks\W_chunk.csv', sep=',', header=None,
                     names=['x', 'y'],skiprows=1)
    # xypair = df.as_matrix(columns=(3, 2))
    return df

def mergecsv2():
    files = [f for f in os.listdir(r'D:\pythongeo\MSPTProj\NE1')]
    mergedf = pd.DataFrame()
    for f in files:
        url = os.path.join(r'D:\pythongeo\MSPTProj\NE1', f)
        print(url)
        currentdf = pd.read_csv(url,header=0,sep=',')
        mergedf = pd.concat([mergedf,currentdf])
        print(f)
        print(len(mergedf))

    mergedf.to_csv(r'D:\pythongeo\MSPTProj\NE1\NE1_chunk.csv',sep=',', index=False)

def mergecsv3():
    files = [f for f in os.listdir(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_chunks\SE')]
    mergedf = pd.DataFrame()
    for f in files:
        url = os.path.join(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_chunks\SE', f)
        print(url)
        currentdf = pd.read_csv(url,header=0,sep=',')
        mergedf = pd.concat([mergedf,currentdf])
        print(f)
        print(len(mergedf))

    mergedf.to_csv(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_chunks\SE_PT_chunk.csv', sep=',', index=False)


def do_job(data_slice, idx, chunkdir):
    fixpoly = fixTopoError(data_slice)
    print(len(fixpoly))
    nonull = checknonull(fixpoly)
    print(len(nonull))

    diss = ops.unary_union(nonull)
    if diss.type is 'Polygon':
        dissall.append(diss)
    else:
        for p in diss:
            dissall.append(p)

    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }
    # Write a new Shapefile
    tmpname = 'chunk'+str(idx)+'.shp'
    tmpurl = os.path.join(chunkdir, tmpname)
    with fiona.open(tmpurl, 'w', 'ESRI Shapefile',
                    schema) as c:
        for index, poly in enumerate(dissall):
            c.write({
                'geometry': mapping(poly),
                'properties': {'id': index},
            })
    print(len(dissall))


def dispatch_jobs(data, job_number, chunkdir):

    total = len(data)
    chunk_size = int(total / job_number)
    slice = chunks(data, chunk_size)
    jobs = []
    idx = len(jobs)

    for i in slice:
        j = multiprocessing.Process(target=do_job, args=(i, idx, chunkdir))
        jobs.append(j)
        idx = len(jobs)
    for j in jobs:
        j.start()

    for k in jobs:
        k.join()

def createNC4():
    pturl = r'C:\zhgren\CityCenterDetection\twitter_2015_CUS_filtered\HOU_tweets.shp'
    chunkdir = r'C:\zhgren\CityCenterDetection\twitter_2015_CUS_filtered\tmpdata'
    outshp = r'C:\zhgren\CityCenterDetection\twitter_2015_CUS_filtered\HOU_tweetsNC.shp'
    gpd = geopandas.read_file(pturl)
    ptx = gpd.geometry.x
    pty = gpd.geometry.y
    xy = list(zip(ptx, pty))
    tri = createdelaunay(xy)
    print(datetime.datetime.now() - starttime)
    tricoords = coordgroup(tri)
    tripolygons = createpolygon(tricoords)
    print(datetime.datetime.now() - starttime)
    meanlength = calculatemean(tripolygons)
    print(meanlength)
    selectedtri = selectpolygons(meanlength, tripolygons)
    print(len(selectedtri))
    print(datetime.datetime.now() - starttime)
    dispatch_jobs(selectedtri, 4, chunkdir)
    dissall = readshp(list_files(chunkdir, 'shp'))
    dissolved = ops.unary_union(dissall)
    print(len(dissolved))
    exteriorlist = list()
    for diss in dissolved:
        exter = Polygon(diss.exterior)
        exteriorlist.append(exter)
    print(datetime.datetime.now() - starttime)
    # Write a new Shapefile
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }
    with fiona.open(outshp, 'w', 'ESRI Shapefile',
                    schema) as c:
        for index, poly in enumerate(exteriorlist):
            c.write({
                'geometry': mapping(poly),
                'properties': {'id': index},
            })
    print(datetime.datetime.now() - starttime)
    print('\n')

def createNC3():
    pturl = r'C:\zhgren\CityCenterDetection\USdata\US OSM data\CHI data\CH_roads_nodes.shp'
    chunkdir = r'C:\zhgren\CityCenterDetection\USdata\US OSM data\CHI data\tmp'
    outshp = r'C:\zhgren\CityCenterDetection\USdata\US OSM data\CHI data\CH_roads_HS.shp'
    gpd = geopandas.read_file(pturl)
    ptx = gpd.geometry.x
    pty = gpd.geometry.y
    xy = list(zip(ptx, pty))
    tri = createdelaunay(xy)
    print(datetime.datetime.now() - starttime)
    tricoords = coordgroup(tri)
    tripolygons = createpolygon(tricoords)
    print(datetime.datetime.now() - starttime)
    meanlength = calculatemean(tripolygons)
    print(meanlength)
    selectedtri = selectpolygons(meanlength, tripolygons)
    print(len(selectedtri))
    print(datetime.datetime.now() - starttime)
    dispatch_jobs(selectedtri, 4, chunkdir)
    dissall = readshp(list_files(chunkdir, 'shp'))
    dissolved = ops.unary_union(dissall)
    print(len(dissolved))
    exteriorlist = list()
    for diss in dissolved:
        exter = Polygon(diss.exterior)
        exteriorlist.append(exter)
    print(datetime.datetime.now() - starttime)
    # Write a new Shapefile
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }
    with fiona.open(outshp, 'w', 'ESRI Shapefile',
                    schema) as c:
        for index, poly in enumerate(exteriorlist):
            c.write({
                'geometry': mapping(poly),
                'properties': {'id': index},
            })
    print(datetime.datetime.now() - starttime)
    print('\n')

def selectptbyloc():
    print(datetime.datetime.now() - starttime)
    filedir = r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_shp\hou\HOUPT'
    filelist = [f for f in os.listdir(filedir)]
    for f in filelist:
        url = os.path.join(filedir, f)
        fname = f[:len(f) - 4] + '.geojson'
        newname = os.path.join(filedir, fname)
        print(newname)
        df = pd.read_csv(url)
        print(df.head())
        gdf1 = geopandas.GeoDataFrame(geometry=geopandas.points_from_xy(df.x, df.y, crs='epsg:5070'))
        gdf1.to_file(newname, driver='GeoJSON')
        bbox = (-12488, 690961, 120626, 825731)
        seleted = geopandas.read_file(newname, bbox=bbox)
        fname2 = f[:len(f) - 4] + '_selected.shp'
        newname2 = os.path.join(filedir, fname2)
        seleted.to_file(newname2, driver='ESRI Shapefile')
        print(newname2)

def innercenter():
    geopandas.options.use_pygeos = True
    url = r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_shp\la\LA_PT.shp'
    gpd = geopandas.read_file(url)
    ptx = gpd.geometry.x
    pty = gpd.geometry.y
    xy = list(zip(ptx, pty))
    tri = createdelaunay(xy)
    tricoords = coordgroup(tri)
    tripolygons = createpolygon(tricoords)
    print(len(tripolygons))
    meanlength1 = calculatemean(tripolygons)
    print(meanlength1)
    # selectedtri1 = selectpolygons(meanlength1, tripolygons)
    # print(len(selectedtri1))
    deselected = [p for p in tripolygons if p.length > meanlength1]
    print(len(deselected))
    largermean = calculatemean(deselected)
    print(largermean)
    selectedtri2 = selectpolygons(largermean,tripolygons)
    print(len(selectedtri2))
    print(datetime.datetime.now() - starttime)

    fixpoly = fixTopoError(selectedtri2)
    diss = ops.unary_union(fixpoly)
    if diss.type is 'Polygon':
        dissall.append(diss)
    else:
        for p in diss:
            dissall.append(p)

    exteriorlist = list()
    for dis in dissall:
        exter = Polygon(dis.exterior)
        exteriorlist.append(exter)
    print(datetime.datetime.now() - starttime)
    # Write a new Shapefile
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }
    with fiona.open(r'C:\zhgren\CityCenterDetection\USdata\US_building2020\building_center_shp\la\LA_HS1b.shp', 'w',
                    'ESRI Shapefile',
                    schema) as c:
        for index, poly in enumerate(exteriorlist):
            c.write({
                'geometry': mapping(poly),
                'properties': {'id': index},
            })
    print(datetime.datetime.now() - starttime)


if __name__ == '__main__':
    cpus = multiprocessing.cpu_count()
    print(cpus)
    speedups.enabled
    print(speedups.enabled)
    print(starttime)
    # selectptbyloc()
    createNC4()


