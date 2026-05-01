import re
import json
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from .shapefiles import *
from .extract import *
from .util import remove_duplicates_list
from .dates import format_output_date, get_ncinfo_date
from .index_time import get_index_dates_dataset
from .zarrdata import get_zarr_dataset
from .geojson import get_user_geojson, geojson_polygons_points
from .netcdf import get_netcdf_data, aggregate_netcdf_data, extract_netcdf_bbox

def extract_rectangular_grid_data(params, dataset, filename, bbox):
    if dataset['compute']:
        out = aggregate_netcdf_data(params, dataset, bbox)
        if out['status'] == -1: return out
        out = out['data']
    else:
        out = get_netcdf_data(params['dataset'], params['temporalRes'],
                              params['variable'], params['Date'], bbox)
        if out is None:
            msg = f'File {filename} not found.'
            return {'status': -1, 'message': msg}

    out['date'] = params['Date']
    out_data = [{'data': out, 'poly': None}]
    return {'status': 0, 'data': out_data}

def extract_polygons_grid_data(params, dataset, filename):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1: return shpObj

    multipolygons = False
    if type(shpObj['polys']) is list:
        if len(shpObj['polys']) > 1:
            multipolygons = True
        else:
            shpObj['polys'] = shpObj['polys'][0]

    if multipolygons:
        if dataset['compute']:
            out = aggregate_netcdf_data(params, dataset)
            if out['status'] == -1: return out
            out = out['data']
        else:
            out = get_netcdf_data(params['dataset'], params['temporalRes'],
                                  params['variable'], params['Date'])
            if out is None:
                msg = f'File {filename} not found.'
                return {'status': -1, 'message': msg}

        out_data = []
        for poly in shpObj['polys']:
            bbox = format_bbox_polygons(shpObj['bbox'], params['shpField'], poly)
            ret = extract_netcdf_bbox(out, bbox)
            ext = extract_polygons_griddata(ret, shpObj['shp'], params['shpField'], poly)
            # poly_name = re.sub(r'[^a-zA-Z0-9]', '', poly)
            # resfile = f'{poly_name}_{filename}'
            # ncdata += [{'data': ext, 'poly': poly, 'name': resfile}]
            ext['date'] = params['Date']
            out_data += [{'data': ext, 'poly': poly}]
    else:
        bbox = format_bbox_polygons(shpObj['bbox'], params['shpField'], shpObj['polys'])

        if dataset['compute']:
            out = aggregate_netcdf_data(params, dataset, bbox)
            if out['status'] == -1: return out
            out = out['data']
        else:
            out = get_netcdf_data(params['dataset'], params['temporalRes'],
                                  params['variable'], params['Date'], bbox)
            if out is None:
                msg = f'File {filename} not found.'
                return {'status': -1, 'message': msg}

        out = extract_polygons_griddata(out, shpObj['shp'], params['shpField'], shpObj['polys'])
        out['date'] = params['Date']
        out_data = [{'data': out, 'poly': shpObj['polys']}]

    return {'status': 0, 'data': out_data}

def extract_rectangle_point_data(params, dataset, bbox):
    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)
    sindex = get_bbox_latlon_index(xr_coords, bbox)
    if sindex is None:
        msg = 'Rectangle outside of data spatial range.'
        return {'status': -1, 'message': msg}

    period = format_output_date(params)

    if dataset['compute']:
        tindex = get_index_dates_dataset(xr_coords, params, dataset['input'], dataset['compute'])
        if tindex['status'] == -1: return tindex

        index = tindex['index']
        nl = tindex['length']
        dates = tindex['dates']
        minfrac = float(dataset['minfrac'])
        frac = [len(index[j])/nl[j] for j in range(len(index))]
        if all(x == 0 for x in frac):
            msg = f'No data found for the period {period}'
            return {'status': -1, 'message': msg}
        bfrac = [x < minfrac for x in frac]
        if all(bfrac):
            msg = f'Not enough {tindex["input_res"]} data to compute {params["temporalRes"]} data.'
            return {'status': -1, 'message': msg}
        index = [index[j] for j in range(len(bfrac)) if not bfrac[j]]
        nl = np.array([nl[j] for j in range(len(bfrac)) if not bfrac[j]])
        dates = [dates[j] for j in range(len(bfrac)) if not bfrac[j]]

        it_flat = [i.item() for it in index for i in it]
        it_len = [len(it) for it in index]
        it_fill = nl - it_len
        it_len = np.cumsum(it_len)

        xr_ds = xr_data[params['variable']].isel(time=it_flat, lat=sindex['lat'], lon=sindex['lon'])
        val = xr_ds.mean(dim=['lon', 'lat'], skipna=True)
        val = np.split(val.values, it_len)[:len(index)]
        val = [np.append(v, np.repeat(np.nan, it_fill[i])) for i, v in enumerate(val)]
        nna = np.array([np.count_nonzero(~np.isnan(x)) for x in val])

        if np.all(nna/nl < minfrac):
            val = np.repeat(dataset['missval'], len(index))
        else:
            if dataset['function'] == 'sum':
                try:
                    val = np.nansum(val, axis=1)
                except:
                    val = np.array([np.nansum(v) for v in val])
            elif dataset['function'] == 'mean':
                try:
                    val = np.nanmean(val, axis=1)
                except:
                    val = np.array([np.nanmean(v) for v in val])
            else:
                val = np.repeat(dataset['missval'], len(index))
    else:
        tindex = get_index_dates_dataset(xr_coords, params, params['temporalRes'], dataset['compute'])
        if tindex['status'] == -1: return tindex

        index = tindex['index']
        dates = tindex['dates']

        xr_ds = xr_data[params['variable']].isel(time=index, lat=sindex['lat'], lon=sindex['lon'])
        val = xr_ds.mean(dim=['lon', 'lat'], skipna=True)
        val = val.values
        val[np.isnan(val)] = dataset['missval']

    out = pd.DataFrame(val)
    out = out.map(lambda x: f'{x:.2f}')
    clon = round((bbox['minLon'] + bbox['maxLon'])/2, 6)
    clat = round((bbox['minLat'] + bbox['maxLat'])/2, 6)
    coords = pd.DataFrame({'loc': ['Rectangle'], 'lon': [clon], 'lat': [clat]})

    return {'status': 0, 'data': out, 'dates': dates, 'coords': coords}

def extract_polygons_points_data(params, dataset):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1: return shpObj

    if type(shpObj['polys']) is not list:
        shpObj['polys'] = [shpObj['polys']]

    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)
    sindex = []
    for poly in shpObj['polys']:
        sindex += [create_geom_polygons(xr_coords, shpObj['shp'],
                                        params['shpField'], poly)]

    ret = extract_polygons_points(params, dataset, xr_data, xr_coords, sindex)
    if ret['status'] == -1: return ret

    ret['coords'] = table_coords_polygons(shpObj['shp'], params['shpField'], shpObj['polys'])
    return ret

def extract_multipoints_data(params, dataset):
    if params['pointsSource'] == 'user':
        csvdata = read_user_csv_mpoints(params['pointsFile'], params['user']['username'])
    else:
        csvdata = format_list_mpoints_dict(params['pointsList'])

    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)
    # sindex = create_geom_mpoints(xr_coords, csvdata, params['padLon'], params['padLat'])
    sindex = create_geom_mpoints_bbox(xr_coords, csvdata, params['padLon'], params['padLat'])
    ret = extract_multipoints(params, dataset, xr_data, xr_coords, sindex)
    if ret['status'] == -1: return ret

    ret['coords'] = csvdata
    return ret

def extract_geojson_points_data(params, dataset):
    if params['geojsonSource'] == 'user':
        json_data = get_user_geojson(params['geojsonFile'], params['user']['username'])
        if json_data['status'] == -1: return json_data
        json_data = json.loads(json_data['geojson'])
    else:
        json_data = params['geojsonData']

    geojson = gpd.GeoDataFrame.from_features(json_data['features'])
    geojson = geojson_polygons_points(geojson)
    if geojson['status'] == -1: return geojson

    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    geojson = geojson['geojson']
    geom_type = list(geojson.geom_type)
    pls = ['Polygon' in s for s in geom_type]
    pts = ['Point' in s for s in geom_type]

    if any(pls):
        geom_polygons = geojson[pls]
        name = geom_polygons[params['geojsonField']].reset_index(drop=True)
        gt = pd.DataFrame(data={'type': ['polygon' for i in name]})
        crds = geom_polygons.centroid.get_coordinates().reset_index(drop=True)
        ncrds = dict(zip(crds.columns.tolist(), ['lon', 'lat']))
        crds = crds.rename(columns=ncrds)
        info_polygons = pd.concat([name, crds, gt], axis=1)

        npolys = name.tolist()
        params['Poly'] = remove_duplicates_list(npolys)
        # bbox_df = get_bbox_polygons(geom_polygons, params['geojsonField'])
        # npolys = bbox_df[params['geojsonField']].tolist()
        # params['Poly'] = remove_duplicates_list(npolys)

        geom_polygons.crs = 'EPSG:4326'
        sindex = []
        for poly in params['Poly']:
            sindex += [create_geom_polygons(xr_coords, geom_polygons,
                                            params['geojsonField'], poly)]
        ret = extract_polygons_points(params, dataset, xr_data, xr_coords, sindex)
        if ret['status'] == -1: return ret

        out_polygons = ret['data']
        dates_polygons = ret['dates']

    if any(pts):
        geom_points = geojson[pts]
        name = geom_points[params['geojsonField']].reset_index(drop=True)
        gt = pd.DataFrame(data={'type': ['point' for i in name]})
        crds = geom_points.get_coordinates().reset_index(drop=True)
        ncrds = dict(zip(crds.columns.tolist(), ['lon', 'lat']))
        crds = crds.rename(columns=ncrds)
        info_points = pd.concat([name, crds, gt], axis=1)

        sindex = create_geom_mpoints_bbox(xr_coords, info_points, 0, 0)
        ret = extract_multipoints(params, dataset, xr_data, xr_coords, sindex)
        if ret['status'] == -1: return ret

        out_points = ret['data']
        dates_points = ret['dates']

    if any(pls) and any(pts):
        out = pd.concat([out_polygons, out_points], axis=1)
        out = out.T.reset_index(drop=True).T
        crds = pd.concat([info_polygons, info_points], axis=0)
        crds = crds.reset_index(drop=True)
        dates = dates_points
    elif any(pls):
        out = out_polygons
        crds = info_polygons
        dates = dates_polygons
    elif any(pts):
        out = out_points
        crds = info_points
        dates = dates_points
    else:
        msg = 'No polygons or points found.'
        return {'status': -1, 'message': msg}

    crds['lon'] = crds['lon'].round(6)
    crds['lat'] = crds['lat'].round(6)
    crds.columns = ['loc', 'lon', 'lat', 'type']

    return {'status': 0, 'data': out, 'dates': dates, 'coords': crds}

def extract_polygons_points(params, dataset, xr_data, xr_coords, sindex):
    period = format_output_date(params)

    if dataset['compute']:
        tindex = get_index_dates_dataset(xr_coords, params, dataset['input'], dataset['compute'])
        if tindex['status'] == -1: return tindex

        index = tindex['index']
        nl = tindex['length']
        dates = tindex['dates']
        minfrac = float(dataset['minfrac'])

        frac = [len(index[j])/nl[j] for j in range(len(index))]
        if all(x == 0 for x in frac):
            msg = f'No data found for the period {period}'
            return {'status': -1, 'message': msg}

        bfrac = [x < minfrac for x in frac]
        if all(bfrac):
            msg = f'Not enough {tindex["input_res"]} data to compute {params["temporalRes"]} data.'
            return {'status': -1, 'message': msg}

        index = [index[j] for j in range(len(bfrac)) if not bfrac[j]]
        nl = np.array([nl[j] for j in range(len(bfrac)) if not bfrac[j]])
        dates = [dates[j] for j in range(len(bfrac)) if not bfrac[j]]

        it_flat = [i.item() for it in index for i in it]
        it_len = [len(it) for it in index]
        it_fill = nl - it_len
        it_len = np.cumsum(it_len)

        out = []
        for s in sindex:
            if len(s[0]) == 0:
                out += [np.repeat(dataset['missval'], len(index))]
                continue

            ix = xr.DataArray(s[1], dims='points')
            iy = xr.DataArray(s[0], dims='points')
            xr_ds = xr_data[params['variable']].isel(time=it_flat, lat=iy, lon=ix)
            val = xr_ds.mean(dim='points', skipna=True)
            val = np.split(val.values, it_len)[:len(index)]
            val = [np.append(v, np.repeat(np.nan, it_fill[i])) for i, v in enumerate(val)]
            nna = np.array([np.count_nonzero(~np.isnan(x)) for x in val])

            if np.all(nna/nl < minfrac):
                val = np.repeat(dataset['missval'], len(index))
            else:
                if dataset['function'] == 'sum':
                    try:
                        val = np.nansum(val, axis=1)
                    except:
                        val = np.array([np.nansum(v) for v in val])
                elif dataset['function'] == 'mean':
                    try:
                        val = np.nanmean(val, axis=1)
                    except:
                        val = np.array([np.nanmean(v) for v in val])
                else:
                    val = np.repeat(dataset['missval'], len(index))
            out += [val]
    else:
        tindex = get_index_dates_dataset(xr_coords, params, params['temporalRes'], dataset['compute'])
        if tindex['status'] == -1: return tindex

        index = tindex['index']
        dates = tindex['dates']

        out = []
        for s in sindex:
            if len(s[0]) == 0:
                val = np.repeat(dataset['missval'], len(index))
                out += [val]
                continue

            ix = xr.DataArray(s[1], dims='points')
            iy = xr.DataArray(s[0], dims='points')
            xr_ds = xr_data[params['variable']].isel(time=index, lat=iy, lon=ix)
            val = xr_ds.mean(dim='points', skipna=True)
            val = val.values
            val[np.isnan(val)] = dataset['missval']
            out += [val]

    out = pd.DataFrame(out).transpose()
    out = out.map(lambda x: f"{x:.2f}")
    return {'status': 0, 'data': out, 'dates': dates}

def extract_multipoints(params, dataset, xr_data, xr_coords, sindex):
    period = format_output_date(params)

    if dataset['compute']:
        tindex = get_index_dates_dataset(xr_coords, params, dataset['input'], dataset['compute'])
        if tindex['status'] == -1: return tindex

        index = tindex['index']
        nl = tindex['length']
        dates = tindex['dates']
        minfrac = float(dataset['minfrac'])

        frac = [len(index[j])/nl[j] for j in range(len(index))]
        if all(x == 0 for x in frac):
            msg = f'No data found for the period {period}'
            return {'status': -1, 'message': msg}

        bfrac = [x < minfrac for x in frac]
        if all(bfrac):
            msg = f'Not enough {tindex["input_res"]} data to compute {params["temporalRes"]} data.'
            return {'status': -1, 'message': msg}

        index = [index[j] for j in range(len(bfrac)) if not bfrac[j]]
        nl = np.array([nl[j] for j in range(len(bfrac)) if not bfrac[j]])
        dates = [dates[j] for j in range(len(bfrac)) if not bfrac[j]]

        it_flat = [i.item() for it in index for i in it]
        it_len = [len(it) for it in index]
        it_fill = nl - it_len
        it_len = np.cumsum(it_len)

        out = []
        for s in sindex:
            if len(s[0]) == 0:
                out += [np.repeat(dataset['missval'], len(index))]
                continue

            # ix = xr.DataArray(s[0], dims='points')
            # iy = xr.DataArray(s[1], dims='points')
            # xr_ds = xr_data[params['variable']].isel(time=it_flat, lat=iy, lon=ix)
            # val = xr_ds.mean(dim='points', skipna=True)
            # 
            xr_ds = xr_data[params['variable']].isel(time=it_flat, lat=s[1], lon=s[0])
            val = xr_ds.mean(dim=['lon', 'lat'], skipna=True)
            # 
            val = np.split(val.values, it_len)[:len(index)]
            val = [np.append(v, np.repeat(np.nan, it_fill[i])) for i, v in enumerate(val)]
            nna = np.array([np.count_nonzero(~np.isnan(x)) for x in val])

            if np.all(nna/nl < minfrac):
                val = np.repeat(dataset['missval'], len(index))
            else:
                if dataset['function'] == 'sum':
                    try:
                        val = np.nansum(val, axis=1)
                    except:
                        val = np.array([np.nansum(v) for v in val])
                elif dataset['function'] == 'mean':
                    try:
                        val = np.nanmean(val, axis=1)
                    except:
                        val = np.array([np.nanmean(v) for v in val])
                else:
                    val = np.repeat(dataset['missval'], len(index))
            out += [val]
    else:
        tindex = get_index_dates_dataset(xr_coords, params, params['temporalRes'], dataset['compute'])
        if tindex['status'] == -1: return tindex

        index = tindex['index']
        dates = tindex['dates']

        out = []
        for s in sindex:
            if len(s[0]) == 0:
                val = np.repeat(dataset['missval'], len(index))
                out += [val]
                continue

            # ix = xr.DataArray(s[0], dims='points')
            # iy = xr.DataArray(s[1], dims='points')
            # xr_ds = xr_data[params['variable']].isel(time=index, lat=iy, lon=ix)
            # val = xr_ds.mean(dim='points', skipna=True)
            # 
            xr_ds = xr_data[params['variable']].isel(time=index, lat=s[1], lon=s[0])
            val = xr_ds.mean(dim=['lon', 'lat'], skipna=True)
            # 
            val = val.values
            val[np.isnan(val)] = dataset['missval']
            out += [val]

    out = pd.DataFrame(out).transpose()
    out = out.map(lambda x: f"{x:.2f}")
    return {'status': 0, 'data': out, 'dates': dates}
