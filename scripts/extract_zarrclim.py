import json
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from .extract import *
from .util import remove_duplicates_list
from .zarrclim import get_zarr_clim
from .extract_clim import (_clim_format_gridded,
                           _clim_params_info,
                           _clim_multiple_columns)
from .shapefiles import (format_bbox_polygons,
                         mask_polygons_xarray_dataArray,
                         get_shapefiles_data)
from .geojson import get_user_geojson, geojson_polygons_points

def _extract_statistics(ds, params):
    if params['climFunction'] == 'percentile':
        qv = np.array(params['precentileValue'])/100
        ds = ds.sel(quantile=qv)
    elif params['climFunction'] == 'mean':
        ds = ds.sel(statistics='mean')
    elif params['climFunction'] == 'stdev':
        ds = ds.sel(statistics='stdev')
    elif params['climFunction'] == 'mean-stdev':
        ds = ds
    else:
        ds = None
    return ds

def _get_zarr_clim_data(params):
    if params['climFunction'] in ['mean', 'stdev']:
        climFunction = 'mean-stdev'
    else:
        climFunction = params['climFunction']

    data = get_zarr_clim(params['dataset'], params['temporalRes'],
                            params['variable'], climFunction)
    data = _extract_statistics(data, params)
    if data is None:
        msg = 'Unknown climFunction value'
        return {'status': -1, 'message': msg}
    return {'status': 0, 'data': data}

def zarrclim_gridded_data(params, dataset, bbox = None):
    ds_data = _get_zarr_clim_data(params)
    if ds_data['status'] == -1: return ds_data
    xr_data = ds_data['data']
    xr_coords = get_coords_dataset(xr_data)
    if bbox:
        sindex = get_bbox_latlon_index(xr_coords, bbox)
        if sindex is None:
            msg = 'Rectangle outside of data spatial range.'
            return {'status': -1, 'message': msg}
    if bbox:
        xr_ds = xr_data.isel(lat=sindex['lat'], lon=sindex['lon'])
    else:
        xr_ds = xr_data
    out_data = _clim_format_gridded(xr_ds, dataset)
    out_data['poly'] = None
    return {'status': 0, 'data': [out_data]}

def zarrclim_polygons_grid_data(params, dataset):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1: return shpObj

    if type(shpObj['polys']) is not list:
        shpObj['polys'] = [shpObj['polys']]

    ds_data = _get_zarr_clim_data(params)
    if ds_data['status'] == -1: return ds_data
    xr_data = ds_data['data']
    xr_coords = get_coords_dataset(xr_data)

    out_clim = []
    for poly in shpObj['polys']:
        bbox = format_bbox_polygons(shpObj['bbox'], params['shpField'], poly)
        sindex = get_bbox_latlon_index(xr_coords, bbox)
        if sindex is None:
            out_clim += [None]
            continue

        xr_ds = xr_data.isel(lat=sindex['lat'], lon=sindex['lon'])
        out_clim += [xr_ds]

    if all(l is None for l in out_clim):
        msg = 'All polygons are outside of data spatial range.'
        return {'status': -1, 'message': msg}

    index = [True if x else False for x in out_clim]
    out_clim = [x for x, b in zip(out_clim, index) if b]
    poly_name = [x for x, b in zip(shpObj['polys'], index) if b]

    out_data = []
    for i in range(len(out_clim)):
        out_tmp = _clim_format_gridded(out_clim[i], dataset)
        out_tmp['poly'] = poly_name[i]
        out_data += [out_tmp]

    return {'status': 0, 'data': out_data}

def zarrclim_polygons_point_data(params, dataset):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1: return shpObj

    if type(shpObj['polys']) is not list:
        shpObj['polys'] = [shpObj['polys']]

    ds_data = _get_zarr_clim_data(params)
    if ds_data['status'] == -1: return ds_data
    xr_data = ds_data['data']
    xr_coords = get_coords_dataset(xr_data)
    out_clim = []
    for poly in shpObj['polys']:
        sindex = create_geom_polygons(xr_coords, shpObj['shp'],
                                    params['shpField'], poly)
        if len(sindex[0]) == 0:
            out_clim += [None]
            continue
        ilo = xr.DataArray(sindex[1], dims='points')
        ila = xr.DataArray(sindex[0], dims='points')
        xr_ds = xr_data.isel(lat=ila, lon=ilo)
        xr_ds = xr_ds.mean(dim='points', skipna=True)
        out_clim += [xr_ds]

    if all(l is None for l in out_clim):
        msg = 'All polygons are outside of data spatial range.'
        return {'status': -1, 'message': msg}

    index = [True if x else False for x in out_clim]
    out_clim = [x for x, b in zip(out_clim, index) if b]
    poly_name = [x for x, b in zip(shpObj['polys'], index) if b]

    out = []
    for oc in out_clim:
        tmp = oc.fillna(dataset['missval'])
        tmp = pd.DataFrame(tmp[params['variable']].values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    date = out_clim[0]['date'].values.tolist()
    coords = table_coords_polygons(shpObj['shp'], params['shpField'], poly_name)

    return {'status': 0, 'data': out, 'dates': date,
            'coords': coords, 'missval': dataset['missval'],
            'mpars': _clim_multiple_columns(params)}

def zarrclim_retangle_point_data(params, dataset, bbox):
    ds_data = _get_zarr_clim_data(params)
    if ds_data['status'] == -1: return ds_data
    xr_data = ds_data['data']
    xr_coords = get_coords_dataset(xr_data)
    sindex = get_bbox_latlon_index(xr_coords, bbox)
    if sindex is None:
        msg = 'Rectangle outside of data spatial range.'
        return {'status': -1, 'message': msg}

    xr_ds = xr_data.isel(lat=sindex['lat'], lon=sindex['lon'])
    xr_ds = xr_ds.mean(dim=['lon', 'lat'], skipna=True)
    xr_ds = xr_ds.fillna(dataset['missval'])
    out = pd.DataFrame(xr_ds[params['variable']])
    out = [out.map(lambda x: f'{x:.2f}')]

    date = xr_ds['date'].values.tolist()
    clon = round((bbox['minLon'] + bbox['maxLon'])/2, 6)
    clat = round((bbox['minLat'] + bbox['maxLat'])/2, 6)
    coords = pd.DataFrame({'loc': ['Rectangle'], 'lon': [clon], 'lat': [clat]})

    return {'status': 0, 'data': out, 'dates': date,
            'coords': coords, 'missval': dataset['missval'],
            'mpars': _clim_multiple_columns(params)}

def zarrclim_multipoints_data(params, dataset):
    if params['pointsSource'] == 'user':
        csvdata = read_user_csv_mpoints(params['pointsFile'], params['user']['username'])
    else:
        csvdata = format_list_mpoints_dict(params['pointsList'])

    ds_data = _get_zarr_clim_data(params)
    if ds_data['status'] == -1: return ds_data
    xr_data = ds_data['data']
    xr_coords = get_coords_dataset(xr_data)
    sindex = create_geom_mpoints_bbox(xr_coords, csvdata, params['padLon'], params['padLat'])

    out_clim = []
    for s in sindex:
        if len(s[0]) == 0:
            out_clim += [None]
            continue

        xr_ds = xr_data.isel(lat=s[1], lon=s[0])
        xr_ds = xr_ds.mean(dim=['lon', 'lat'], skipna=True)
        out_clim += [xr_ds]

    if all(l is None for l in out_clim):
        msg = 'All points are outside of data spatial range.'
        return {'status': -1, 'message': msg}

    index = [True if x else False for x in out_clim]
    out_clim = [x for x, b in zip(out_clim, index) if b]
    csvdata = csvdata[index]

    date = out_clim[0]['date'].values.tolist()

    out = []
    for oc in out_clim:
        tmp = oc.fillna(dataset['missval'])
        tmp = pd.DataFrame(tmp[params['variable']].values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    return {'status': 0, 'data': out, 'dates': date,
            'coords': csvdata, 'missval': dataset['missval'],
            'mpars': _clim_multiple_columns(params)}

def zarrclim_geojson_data(params, dataset):
    if params['geojsonSource'] == 'user':
        json_data = get_user_geojson(params['geojsonFile'], params['user']['username'])
        if json_data['status'] == -1: return json_data
        json_data = json.loads(json_data['geojson'])
    else:
        json_data = params['geojsonData']

    geojson = gpd.GeoDataFrame.from_features(json_data['features'])
    geojson = geojson_polygons_points(geojson)
    if geojson['status'] == -1: return geojson

    geojson = geojson['geojson']
    geom_type = list(geojson.geom_type)

    pls = ['Polygon' in s for s in geom_type]
    if any(pls):
        geom_polygons = geojson[pls].reset_index(drop=True)
        poly_name = geom_polygons[params['geojsonField']].tolist()
        poly_name = [f'polygon_{n}' for n in poly_name]
        geom_polygons.insert(0, 'select_column', poly_name)
        poly_name = remove_duplicates_list(poly_name)

    pts = ['Point' in s for s in geom_type]
    if any(pts):
        geom_points = geojson[pts].reset_index(drop=True)
        point_name = geom_points[params['geojsonField']].tolist()
        point_name = [f'point_{n}' for n in point_name]
        geom_points.insert(0, 'select_column', point_name)

    if any(pls) and any(pts):
        geojson = pd.concat([geom_polygons, geom_points], axis=0)
        geojson = geojson.reset_index(drop=True)
        select_column = poly_name + point_name
    elif any(pls):
        geojson = geom_polygons
        select_column = poly_name
    elif any(pts):
        geojson = geom_points
        select_column = point_name
    else:
        msg = 'No polygons or points found.'
        return {'status': -1, 'message': msg}

    ds_data = _get_zarr_clim_data(params)
    if ds_data['status'] == -1: return ds_data
    xr_data = ds_data['data']
    xr_coords = get_coords_dataset(xr_data)

    out_clim = []
    out_coords = []
    for s in select_column:
        geom = geojson[geojson['select_column'] == s]
        geom_type = list(geom.geometry.geom_type)

        if 'Polygon' in geom_type or 'MultiPolygon' in geom_type:
            crds = geom.geometry.centroid
            info_coords = {
                'name': geom[params['geojsonField']].item(),
                'lon': round(crds.x.mean().item(), 6),
                'lat': round(crds.y.mean().item(), 6),
                'type': 'polygon'
            }

            sindex = create_geom_polygons_select(xr_coords, geom)
        elif 'Point' in geom_type:
            crds = geom.get_coordinates().reset_index(drop=True)
            info_coords = {
                'name': geom[params['geojsonField']].item(),
                'lon': round(crds.x.item(), 6),
                'lat': round(crds.y.item(), 6),
                'type': 'point'
            }
            ncrds = dict(zip(crds.columns.tolist(), ['lon', 'lat']))
            crds = crds.rename(columns=ncrds)

            sindex = create_geom_mpoints_bbox(xr_coords, crds, 0, 0)
            sindex = sindex[0]
        else:
            continue

        if len(sindex[0]) == 0:
            out_clim += [None]
            continue

        ilo = xr.DataArray(sindex[1], dims='points')
        ila = xr.DataArray(sindex[0], dims='points')
        xr_ds = xr_data.isel(lat=ila, lon=ilo)
        xr_ds = xr_ds.mean(dim='points', skipna=True)
        out_clim += [xr_ds]
        out_coords += [info_coords]

    if all(l is None for l in out_clim):
        msg = 'All features are outside of data spatial range.'
        return {'status': -1, 'message': msg}

    index = [True if x else False for x in out_clim]
    out_clim = [x for x, b in zip(out_clim, index) if b]
    out_coords = [x for x, b in zip(out_coords, index) if b]

    out = []
    for oc in out_clim:
        tmp = oc.fillna(dataset['missval'])
        tmp = pd.DataFrame(tmp[params['variable']].values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    date = out_clim[0]['date'].values.tolist()
    coords = pd.DataFrame(out_coords)
    coords.columns = ['loc', 'lon', 'lat', 'type']

    return {'status': 0, 'data': out, 'dates': date,
            'coords': coords, 'missval': dataset['missval'],
            'mpars': _clim_multiple_columns(params)}
