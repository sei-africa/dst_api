import os
import json
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import rasterio.features as rsf
import odc.geo.xr
from .util import remove_duplicates_list
from app.scripts._global import GLOBAL_CONFIG

def get_list_polygons_app():
    shp_dir = GLOBAL_CONFIG['shp_dir']
    if not os.path.exists(shp_dir):
        msg = 'The folder <shp> containing the shapefiles not found.'
        return {'status': -1, 'message': msg}

    shp_files = [f for f in os.listdir(shp_dir) if f.endswith('.shp')]
    if len(shp_files) == 0:
        msg = 'No shapefiles found.'
        return {'status': -1, 'message': msg}

    return {'status': 0, 'shp': shp_files}

def get_shapefiles_path(shpfile, username=None):
    if username is None:
        shp_dir = GLOBAL_CONFIG['shp_dir']
        msg = 'The folder <shp> containing the shapefiles not found.'
    else:
        shp_dir = os.path.join(GLOBAL_CONFIG['users_data'], username, 'shapefiles')
        msg = 'You do not have a saved shapefiles on your account.'
        shpfile = f'{shpfile}.shp'

    if not os.path.exists(shp_dir): 
        return {'status': -1, 'message': msg}

    shp_path = os.path.join(shp_dir, shpfile)
    return {'status': 0, 'path': shp_path}

def _check_polygons_geom_type(shp):
    gt = list(shp.geom_type.unique())
    if len(gt) == 0:
        return False

    gt = [s.lower() for s in gt]
    if any('polygon' in s for s in gt):
        return True
    else:
        return False

def _filter_shapefile_polygons(shp):
    gt = list(shp.geom_type)
    gt = [s.lower() for s in gt]
    irow = ['polygon' in s for s in gt]
    return shp[irow]

def read_shapefiles(shp_file):
    shp_path = os.path.join(GLOBAL_CONFIG['shp_dir'], shp_file)
    if not os.path.exists(shp_path):
        msg = f'Shapefile {shp_file} not found.'
        return {'status': -1, 'message': msg}
    shp = gpd.read_file(shp_path)
    return {'status': 0, 'shp': shp}

def read_shapefile_polygons(shpfile):
    if not os.path.exists(shpfile):
        msg = f'Shapefile {os.path.basename(shpfile)} not found.'
        return {'status': -1, 'message': msg}

    shp = gpd.read_file(shpfile)
    if not _check_polygons_geom_type(shp):
        msg = 'The shapefiles do not contain Polygons or MultiPolygons.'
        return {'status': -1, 'message': msg}

    shp = _filter_shapefile_polygons(shp)
    return {'status': 0, 'shp': shp}

def _get_geojson_polygons(shpfile):
    shp_obj = read_shapefile_polygons(shpfile)
    if shp_obj['status'] == -1: return shp_obj

    geojson = shp_obj['shp'].to_json(to_wgs84=True)
    fields = shp_obj['shp'].columns.tolist()
    fields.remove('geometry')
    return {'status': 0, 'fields': fields, 'geojson': geojson}

def get_defaut_polygons(shpfile):
    shp_path = get_shapefiles_path(shpfile)
    if shp_path['status'] == -1: return shp_path

    return _get_geojson_polygons(shp_path['path'])

def get_user_polygons(shpfile, username):
    if (shpfile is None) or (shpfile == ''):
        msg = 'You do not have a saved shapefiles on your account.'
        return {'status': -1, 'message': msg}

    shp_path = get_shapefiles_path(shpfile, username)
    if shp_path['status'] == -1: return shp_path

    return _get_geojson_polygons(shp_path['path'])

def get_bbox_polygons(shp, attr_field=None):
    bbox = shp.bounds
    if attr_field is None:
        attr_col = shp.iloc[:, 0]
    else:
        attr_col = shp[attr_field]

    bbox_df = pd.concat([attr_col, bbox], axis=1)
    return bbox_df

def format_bbox_polygons(bbox_df, attr, poly):
    bbxp = bbox_df[bbox_df[attr] == poly]
    bbox = {'minLon': float(bbxp['minx'].iat[0]),
            'maxLon': float(bbxp['maxx'].iat[0]),
            'minLat': float(bbxp['miny'].iat[0]),
            'maxLat': float(bbxp['maxy'].iat[0])}
    return bbox

def extract_polygons_griddata(data, shp, attr, poly):
    gpd_poly = shp[shp[attr] == poly].to_crs('EPSG:4326')
    mlon, mlat = np.meshgrid(data['lon'], data['lat'])
    mlon = mlon.flatten()
    mlat = mlat.flatten()
    mid = np.arange(mlat.shape[0])
    gpd_grid = {'id': mid, 'lon': mlon, 'lat': mlat}
    gpd_grid = pd.DataFrame(gpd_grid)
    geom_grid = gpd.points_from_xy(gpd_grid.lon, gpd_grid.lat)
    gpd_grid = gpd.GeoDataFrame(gpd_grid, geometry=geom_grid, crs='EPSG:4326')
    pts = gpd.sjoin(gpd_grid, gpd_poly, how='inner', predicate='within')
    pts = np.array(pts['id'].tolist())
    iw = ~np.isin(mid, pts)
    iw = iw.reshape(data['data'].shape)
    mask = np.ma.getmask(data['data'])
    data['data'] = np.ma.masked_array(data['data'], mask = mask | iw)
    return data

def mask_polygons_xarray_dataArray(xr_ds, shp, attr, poly):
    gpd_poly = shp[shp[attr] == poly].to_crs('EPSG:4326')
    xr_ds = xr_ds.odc.assign_crs('EPSG:4326')
    mask = rsf.geometry_mask(gpd_poly.geometry,
            out_shape=xr_ds.odc.geobox.shape,
            transform=xr_ds.odc.geobox.affine,
            invert=True)
    mask = xr.DataArray(mask, dims=('lat', 'lon'))
    return xr_ds.where(mask)

def get_shapefiles_data(params):
    if params['shpSource'] == 'user':
        shp_file = get_shapefiles_path(params['shpFile'], params['user']['username'])
    else:
        shp_file = get_shapefiles_path(params['shpFile'])

    if shp_file['status'] == -1: return shp_file

    shp_obj = read_shapefile_polygons(shp_file['path'])
    if shp_obj['status'] == -1: shp_obj

    bbox_df = get_bbox_polygons(shp_obj['shp'], params['shpField'])

    if params['allPolygons']:
        npolys = bbox_df[params['shpField']].tolist()
        params['Poly'] = remove_duplicates_list(npolys)

    return {'status': 0, 'shp': shp_obj['shp'],
            'bbox': bbox_df, 'polys': params['Poly']}

