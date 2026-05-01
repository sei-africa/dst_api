import os
import re
import numpy as np
import pandas as pd
import geopandas as gpd
from .index_time import *
from app.scripts._global import GLOBAL_CONFIG

def read_user_csv_mpoints(csvfile, username):
    dir_users = GLOBAL_CONFIG['users_data']
    user_dir = os.path.join(dir_users, username, 'multipoints')
    csvpath = os.path.join(user_dir, csvfile)
    mpts = pd.read_csv(csvpath)
    new_names = dict(zip(mpts.columns.tolist(), ['loc', 'lon', 'lat']))
    mpts = mpts.rename(columns=new_names)
    mpts['lon'] = pd.to_numeric(mpts['lon'])
    mpts['lat'] = pd.to_numeric(mpts['lat'])
    return mpts

def format_list_mpoints_list(csvlist):
    # [[], [], ...]
    mpts = pd.DataFrame(csvlist, columns=['loc', 'lon', 'lat'])
    mpts['lon'] = pd.to_numeric(mpts['lon'])
    mpts['lat'] = pd.to_numeric(mpts['lat'])
    return mpts

def format_list_mpoints_dict(csvlist):
    # [{}, {}, ...]
    mpts = pd.DataFrame(csvlist)
    mpts['lon'] = pd.to_numeric(mpts['lon'])
    mpts['lat'] = pd.to_numeric(mpts['lat'])
    return mpts

def get_coords_dataset(xr_data):
    lon = xr_data['lon'].values
    lat = xr_data['lat'].values
    time = xr_data['time'].values
    return {'lon': lon, 'lat': lat, 'time': time}

def find_interval(x, v):
    mask = np.logical_or(x < v[0], x > v[-1])
    it = np.digitize(x, v)

    ms1 = []
    for i, s in enumerate(it):
        m = s + 1 > len(v)
        ms1 += [m]
        if not m:
            it[i] = s + (2 * x[i] > v[s - 1] + v[s])

    ms2 = it > len(v) - 1
    mask = np.logical_or(np.logical_or(mask, ms1), ms2)
    it = np.ma.masked_array(it, mask = mask)
    it.fill_value = 0
    return it - 1

def create_geom_mpoints_0(xr_coords, mpoints, padlon=0, padlat=0):
    px = int(padlon)
    py = int(padlat)
    lon = xr_coords['lon']
    lat = xr_coords['lat']
    nx = np.mean(np.diff(lon))
    ny = np.mean(np.diff(lat))

    voisin = []
    for row in mpoints.itertuples():
        xx = row.lon + nx * np.arange(-px, px + 1)
        yy = row.lat + ny * np.arange(-py, py + 1)
        x1, y1 = np.meshgrid(xx, yy)
        voisin += [pd.DataFrame({'lon': x1.flatten(), 'lat': y1.flatten()})]

    xlon = np.concatenate((np.array([lon[0] - nx/2]), lon + nx/2))
    xlat = np.concatenate((np.array([lat[0] - ny/2]), lat + ny/2))

    index = []
    for j in range(len(voisin)):
        ix = find_interval(voisin[j].lon, xlon)
        iy = find_interval(voisin[j].lat, xlat)
        mask = np.logical_or(np.ma.getmask(ix), np.ma.getmask(iy))
        ix = np.ma.masked_array(ix, mask = mask)
        iy = np.ma.masked_array(iy, mask = mask)
        index += [(ix, iy)]

    return index

def create_geom_mpoints(xr_coords, mpoints, padlon=0, padlat=0):
    px = int(padlon)
    py = int(padlat)
    lon = xr_coords['lon']
    lat = xr_coords['lat']
    nx = np.mean(np.diff(lon))
    ny = np.mean(np.diff(lat))

    voisin = []
    for row in mpoints.itertuples():
        xx = row.lon + nx * np.arange(-px, px + 1)
        yy = row.lat + ny * np.arange(-py, py + 1)
        x1, y1 = np.meshgrid(xx, yy)
        voisin += [pd.DataFrame({'lon': x1.flatten(), 'lat': y1.flatten()})]

    xlon = np.concatenate((np.array([lon[0] - nx/2]), lon + nx/2))
    xlat = np.concatenate((np.array([lat[0] - ny/2]), lat + ny/2))

    index = []
    for j in range(len(voisin)):
        ix = find_interval(voisin[j].lon, xlon)
        iy = find_interval(voisin[j].lat, xlat)
        mask = np.logical_or(np.ma.getmask(ix), np.ma.getmask(iy))
        ix = np.ma.masked_array(ix, mask = mask)
        iy = np.ma.masked_array(iy, mask = mask)
        ix = np.ma.getdata(ix[~ix.mask])
        iy = np.ma.getdata(iy[~iy.mask])
        index += [(ix, iy)]

    return index

def create_geom_mpoints_bbox(xr_coords, mpoints, padlon=0, padlat=0):
    px = int(padlon)
    py = int(padlat)
    lon = xr_coords['lon']
    lat = xr_coords['lat']
    nx = np.mean(np.diff(lon))
    ny = np.mean(np.diff(lat))

    index = []
    for row in mpoints.itertuples():
        xx = row.lon + nx * np.array([-px - 0.5, px + 0.5])
        yy = row.lat + ny * np.array([-py - 0.5, py + 0.5])
        ix = np.logical_and(lon >= xx[0], lon <= xx[1])
        iy = np.logical_and(lat >= yy[0], lat <= yy[1])
        ix = np.where(ix)[0]
        iy = np.where(iy)[0]
        if np.logical_or(len(ix) == 0, len(iy) == 0):
            ix = np.array([])
            iy = np.array([])
        index += [(ix, iy)]

    return index

def create_geom_polygons(xr_coords, shp, attr, poly):
    gpd_poly = shp[shp[attr] == poly].to_crs('EPSG:4326')
    return _create_geom_polygons_select(xr_coords, gpd_poly)

def create_geom_polygons_select(xr_coords, geom):
    geom.crs = 'EPSG:4326'
    return _create_geom_polygons_select(xr_coords, geom)

def _create_geom_polygons_select(xr_coords, geom):
    crds = _xrcoords_to_geoDataFrame(xr_coords)
    pts = gpd.sjoin(crds['gpdf'], geom, how='inner', predicate='within')
    pts = np.array(pts['id_points_extract'].tolist())
    iw = np.isin(crds['id'], pts)
    iw = iw.reshape(crds['shape'])
    return np.where(iw)

def _xrcoords_to_geoDataFrame(xr_coords):
    mlon, mlat = np.meshgrid(xr_coords['lon'], xr_coords['lat'])
    xy_shape = mlon.shape
    mlon = mlon.flatten()
    mlat = mlat.flatten()
    mid = np.arange(mlat.shape[0])
    gpd_grid = {'id_points_extract': mid, 'lon': mlon, 'lat': mlat}
    gpd_grid = pd.DataFrame(gpd_grid)
    geom_grid = gpd.points_from_xy(gpd_grid.lon, gpd_grid.lat)
    gpd_grid = gpd.GeoDataFrame(gpd_grid, geometry=geom_grid, crs='EPSG:4326')
    return {'shape': xy_shape, 'id': mid, 'gpdf': gpd_grid}

def json_coords_polygons(shp, attr, polys):
    obj = []
    for poly in polys:
        gpd_poly = shp[shp[attr] == poly].to_crs('EPSG:3857')
        ctr = gpd_poly.centroid.to_crs('EPSG:4326')
        if len(ctr) > 1:
            lon = round(np.mean(ctr.x.values).item(), 6)
            lat = round(np.mean(ctr.y.values).item(), 6)
        else:
            lon = round(ctr.x.values.item(), 6)
            lat = round(ctr.y.values.item(), 6)
        obj += [{'Name': poly, 'Longitude': lon, 'Latitude': lat}]
    return obj

def cdt_coords_polygons(shp, attr, polys):
    df = json_coords_polygons(shp, attr, polys)
    df = pd.DataFrame(df)
    df['Name'] = [re.sub(r'[^a-zA-Z0-9]', '', p) for p in df['Name']]
    col = df.columns
    df = df.transpose()
    df.insert(0, 'date', col)
    return df

def table_coords_polygons(shp, attr, polys):
    df = json_coords_polygons(shp, attr, polys)
    df = pd.DataFrame(df)
    df.columns = ['loc', 'lon', 'lat']
    return df

def get_bbox_latlon_index(xr_coords, bbox):
    ix = np.logical_and(xr_coords['lon'] >= bbox['minLon'],
                        xr_coords['lon'] <= bbox['maxLon'])
    iy = np.logical_and(xr_coords['lat'] >= bbox['minLat'],
                        xr_coords['lat'] <= bbox['maxLat'])
    ix = np.where(ix)[0]
    iy = np.where(iy)[0]
    if np.logical_or(len(ix) == 0, len(iy) == 0):
        return None
    return {'lon': ix, 'lat': iy}
