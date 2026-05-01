import os
import geopandas as gpd
from app.scripts._global import GLOBAL_CONFIG

def get_user_geojson(jsonfile, username):
    dir_users = GLOBAL_CONFIG['users_data']
    jsondir = os.path.join(dir_users, username, 'geojson')

    msg0 = 'You do not have a saved GeoJSON files on your account.'
    gjson = {'status': -1, 'message': msg0}

    if not os.path.exists(jsondir):
        return gjson

    if (jsonfile is None) or (jsonfile == ''):
        return gjson

    jsonpath = os.path.join(jsondir, jsonfile)
    return _get_geojson_polygons_points(jsonpath)

def read_geojson_file(jsonfile):
    if not os.path.exists(jsonfile):
        msg = f'File {os.path.basename(jsonfile)} not found.'
        return {'status': -1, 'message': msg}

    gjs = gpd.read_file(jsonfile)
    return geojson_polygons_points(gjs)

def geojson_polygons_points(geojson):
    if not _check_geojson_geom_type(geojson):
        msg = 'The geojson data does not contain a Polygons, MultiPolygons or Points.'
        return {'status': -1, 'message': msg}

    geojson = _filter_geojson_polygons_points(geojson)
    return {'status': 0, 'geojson': geojson}

def _get_geojson_polygons_points(jsonfile):
    gjs = read_geojson_file(jsonfile)
    if gjs['status'] == -1: return gjs

    geojson = gjs['geojson'].to_json(to_wgs84=True)
    fields = gjs['geojson'].columns.tolist()
    fields.remove('geometry')
    return {'status': 0, 'fields': fields, 'geojson': geojson}

def _filter_geojson_polygons_points(geojson):
    gt = list(geojson.geom_type)
    gt = [s.lower() for s in gt]
    pls = ['polygon' in s for s in gt]
    pts = ['point' in s for s in gt]
    irow = [x or y for x, y in zip(pls, pts)]
    return geojson[irow]

def _check_geojson_geom_type(geojson):
    gt = list(geojson.geom_type.unique())
    if len(gt) == 0:
        return False

    gt = [s.lower() for s in gt]
    pls = ['polygon' in s for s in gt]
    pts = ['point' in s for s in gt]
    check = [x or y for x, y in zip(pls, pts)]
    if any(check):
        return True
    else:
        return False

