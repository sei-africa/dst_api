import json
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from .extract import *
from .zarrdata import get_zarr_dataset
from .util import remove_duplicates_list
from .index_clim import get_climatology_times_index
from .shapefiles import (format_bbox_polygons,
                         mask_polygons_xarray_dataArray,
                         get_shapefiles_data)
from .geojson import get_user_geojson, geojson_polygons_points
from .aggregate_data import aggregate_climatology, aggregate_timeSeries
from app.scripts._global import GLOBAL_CONFIG

def _clim_params_info(params):
    datasets = GLOBAL_CONFIG['datasets'][params['dataset']]
    dataset = datasets[params['temporalRes']]

    input_res = params['temporalRes']
    min_frac = 1.
    if dataset['compute']:
        info_var = dataset['netcdf'][params['variable']]
        input_res = info_var['input']
        min_frac = float(info_var['minfrac'])

    start_year = 1991
    if 'startYear' in params:
        start_year = int(params['startYear'])

    end_year = 2020
    if 'endYear' in params:
        end_year = int(params['endYear'])

    min_year = 30
    if 'minYear' in params:
        min_year = int(params['minYear'])

    seas_len = 3
    if params['temporalRes'] == 'seasonal':
        seas_len = int(params['seasLength'])

    day_win = 0
    if params['temporalRes'] == 'daily':
        day_win = int(params['daysWindow'])

    perc_value = 0.5
    if params['climFunction'] == 'percentile':
        perc = params['precentileValue']
        if type(perc) is list:
            perc_value = [float(p)/100 for p in perc]
        else:
            perc_value = float(perc)/100

    freqOper = '>='
    freqThres = 1.0
    if params['climFunction'] == 'frequency':
        freqOper = params['frequencyOper']
        freqThres = float(params['frequencyThres'])

    return {'input': input_res, 'output': params['temporalRes'],
            'startYear': start_year, 'endYear': end_year, 'minYear': min_year,
            'minFrac': min_frac, 'seasLen': seas_len,
            'dayWin': day_win, 'percValue': perc_value,
            'freqOper': freqOper, 'freqThres': freqThres}

def _clim_multiple_columns(params):
    ret = None
    if params['climFunction'] == 'percentile':
        if type(params['precentileValue']) is list:
            if len(params['precentileValue']) > 1:
                ret = {'name': 'Percentiles', 'values': params['precentileValue']}
    if params['climFunction'] == 'mean-stdev':
        ret = {'name': 'Statistics', 'values': ['mean', 'stdev']}
    return ret

def _clim_format_gridded(out_clim, dataset):
    lon = out_clim['lon'].values.tolist()
    lon = [round(x, 6) for x in lon]
    lat = out_clim['lat'].values.tolist()
    lat = [round(x, 6) for x in lat]
    date = out_clim['date'].values.tolist()
    time = out_clim['time'].values.tolist()

    name_dim4 = None
    dim4 = None
    ndims = dict(out_clim.sizes)
    if len(ndims) > 3:
        dims3 = ['lon', 'lat', 'time']
        exdim = [l for l in ndims.keys() if l not in dims3]
        name_dim4 = exdim[0]
        out_clim = out_clim.transpose(name_dim4, 'time', 'lat', 'lon')
        dim4 = out_clim[name_dim4].values.tolist()

    xdata = out_clim[dataset['varid']].values
    xdata = np.nan_to_num(xdata, nan=dataset['missval'])
    xdata = xdata.tolist()
    if len(ndims) > 3:
        xdata = [[[[round(l, 2) for l in z] for z in y] for y in x] for x in xdata]
    else:
        xdata = [[[round(z, 2) for z in y] for y in x] for x in xdata]

    return {'lon': lon, 'lat': lat, 'time': time,
            'date': date, 'data': xdata, 'ndims': ndims,
            'ndim4': name_dim4, 'dim4': dim4,
            'missval': dataset['missval']}


def _clim_create_miss_gridded(xr_ds, params, tclim):
    clim = xr_ds[params['variable']].isel(time=0)
    clim = clim.drop_vars('time')
    clim = clim.drop_attrs()
    if params['climFunction'] == 'mean-stdev':
        dim = {'statistics': np.array(['mean', 'stdev'])}
        clim = clim.expand_dims(dim=dim, axis=0)
        clim[:,:,:] = np.nan
    elif params['climFunction'] == 'percentile':
        if type(tclim['percValue']) is list:
            dim = {'quantile': np.array(tclim['percValue'])}
            clim = clim.expand_dims(dim=dim, axis=0)
            clim[:,:,:] = np.nan
        else:
            clim = clim.assign_coords(quantile=tclim['percValue'])
            clim[:,:] = np.nan
    else:
        clim[:,:] = np.nan

    return clim

def climatology_gridded_data(params, dataset, bbox=None,
                             keep_DataArray=False):
    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)
    if bbox:
        sindex = get_bbox_latlon_index(xr_coords, bbox)
        if sindex is None:
            msg = 'Rectangle outside of data spatial range.'
            return {'status': -1, 'message': msg}

    tclim = _clim_params_info(params)
    tindex = get_climatology_times_index(xr_data['time'].values,
                tclim['output'], tclim['input'],
                tclim['startYear'], tclim['endYear'], tclim['minYear'],
                params['fullYear'], params['climDate'],
                tclim['seasLen'], tclim['dayWin'], tclim['minFrac'])

    if tindex['status'] == -1: return tindex

    syear = [d['syear'] for d in tindex['index']]
    out_clim = []
    for t in tindex['syear']:
        if not t in syear:
            if bbox:
                xr_ds = xr_data.isel(lat=sindex['lat'], lon=sindex['lon'])
            else:
                xr_ds = xr_data

            clim = _clim_create_miss_gridded(xr_ds, params, tclim)
            out_clim += [clim]
            continue

        ix = tindex['index'][syear.index(t)]

        if tindex['aggregate']:
            aggr = []
            for i, v in enumerate(ix['index']):
                if bbox:
                    xr_ds = xr_data[params['variable']].isel(time=v, lat=sindex['lat'], lon=sindex['lon'])
                else:
                    xr_ds = xr_data[params['variable']].isel(time=v)
                aggr_len = ix['length'][i]
                aggr += [aggregate_timeSeries(xr_ds, dataset['function'],
                                              aggr_len, tclim['minFrac'])]

            y_dim = np.arange(0, len(aggr)).tolist()
            aggr = xr.concat(aggr, pd.Index(y_dim, name='y'))
        else:
            if bbox:
                aggr = xr_data[params['variable']].isel(time=ix['index'], lat=sindex['lat'], lon=sindex['lon'])
            else:
                aggr = xr_data[params['variable']].isel(time=ix['index'])
            aggr = aggr.rename({'time': 'y'})

        clim = aggregate_climatology(aggr, params['climFunction'],
                            tclim['minYear'], tclim['percValue'],
                            tclim['freqOper'], tclim['freqThres'])
        out_clim += [clim]

    out_clim = xr.concat(out_clim, pd.Index(tindex['syear'], name='time'))
    coords = {'time': tindex['syear']}
    xr_dates = xr.DataArray(tindex['dates'], name='date', coords=coords, dims='time')
    # xr_dates = xr.Dataset({'date': ('time', tindex['dates'])}, coords=coords)
    out_clim = xr.merge([out_clim, xr_dates])
    if keep_DataArray:
        return out_clim
    else:
        out_data = _clim_format_gridded(out_clim, dataset)
        out_data['poly'] = None
        return {'status': 0, 'data': [out_data]}

def climatology_polygons_grid_data(params, dataset):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1: return shpObj

    if type(shpObj['polys']) is not list:
        shpObj['polys'] = [shpObj['polys']]

    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    tclim = _clim_params_info(params)
    tindex = get_climatology_times_index(xr_data['time'].values,
                tclim['output'], tclim['input'],
                tclim['startYear'], tclim['endYear'], tclim['minYear'],
                params['fullYear'], params['climDate'],
                tclim['seasLen'], tclim['dayWin'], tclim['minFrac'])

    if tindex['status'] == -1: return tindex

    syear = [d['syear'] for d in tindex['index']]
    out_clim = []
    for poly in shpObj['polys']:
        bbox = format_bbox_polygons(shpObj['bbox'], params['shpField'], poly)
        sindex = get_bbox_latlon_index(xr_coords, bbox)

        if sindex is None:
            out_clim += [None]
            continue

        out_tmp = []
        for t in tindex['syear']:
            if not t in syear:
                xr_ds = xr_data.isel(lat=sindex['lat'], lon=sindex['lon'])
                clim = _clim_create_miss_gridded(xr_ds, params, tclim)
                out_tmp += [clim]
                continue

            ix = tindex['index'][syear.index(t)]

            if tindex['aggregate']:
                aggr = []
                for i, v in enumerate(ix['index']):
                    xr_ds = xr_data[params['variable']].isel(time=v, lat=sindex['lat'], lon=sindex['lon'])
                    aggr_len = ix['length'][i]
                    c_aggr = aggregate_timeSeries(xr_ds, dataset['function'], aggr_len, tclim['minFrac'])
                    c_aggr = mask_polygons_xarray_dataArray(c_aggr, shpObj['shp'], params['shpField'], poly)
                    aggr += [c_aggr]

                y_dim = np.arange(0, len(aggr)).tolist()
                aggr = xr.concat(aggr, pd.Index(y_dim, name='y'))
            else:
                aggr = xr_data[params['variable']].isel(time=ix['index'], lat=sindex['lat'], lon=sindex['lon'])
                aggr = aggr.rename({'time': 'y'})

            clim = aggregate_climatology(aggr, params['climFunction'],
                                tclim['minYear'], tclim['percValue'],
                                tclim['freqOper'], tclim['freqThres'])
            out_tmp += [clim]

        out_clim += [out_tmp]

    if all(l is None for l in out_clim):
        msg = 'All polygons are outside of data spatial range.'
        return {'status': -1, 'message': msg}

    index = [True if x else False for x in out_clim]
    out_clim = [x for x, b in zip(out_clim, index) if b]
    poly_name = [x for x, b in zip(shpObj['polys'], index) if b]

    coords = {'time': tindex['syear']}
    xr_dates = xr.DataArray(tindex['dates'], name='date', coords=coords, dims='time')

    out_data = []
    for i in range(len(out_clim)):
        out_tmp = xr.concat(out_clim[i], pd.Index(tindex['syear'], name='time'))
        out_tmp = xr.merge([out_tmp, xr_dates])
        out_tmp = _clim_format_gridded(out_tmp, dataset)
        out_tmp['poly'] = poly_name[i]
        out_data += [out_tmp]

    return {'status': 0, 'data': out_data}

def climatology_retangle_point_data(params, dataset, bbox):
    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)
    sindex = get_bbox_latlon_index(xr_coords, bbox)
    if sindex is None:
        msg = 'Rectangle outside of data spatial range.'
        return {'status': -1, 'message': msg}

    tclim = _clim_params_info(params)
    tindex = get_climatology_times_index(xr_data['time'].values,
                tclim['output'], tclim['input'],
                tclim['startYear'], tclim['endYear'], tclim['minYear'],
                params['fullYear'], params['climDate'],
                tclim['seasLen'], tclim['dayWin'], tclim['minFrac'])

    if tindex['status'] == -1: return tindex

    syear = [d['syear'] for d in tindex['index']]
    out_clim = []
    for t in tindex['syear']:
        if not t in syear:
            clim = xr.DataArray(data=np.nan, name=params['variable'])
            out_clim += [clim]
            continue

        ix = tindex['index'][syear.index(t)]

        if tindex['aggregate']:
            aggr = []
            for i, v in enumerate(ix['index']):
                xr_ds = xr_data[params['variable']].isel(time=v, lat=sindex['lat'], lon=sindex['lon'])
                xr_ds = xr_ds.mean(dim=['lon', 'lat'], skipna=True)
                aggr_len = ix['length'][i]
                aggr += [aggregate_timeSeries(xr_ds, dataset['function'],
                                              aggr_len, tclim['minFrac'])]

            y_dim = np.arange(0, len(aggr)).tolist()
            aggr = xr.concat(aggr, pd.Index(y_dim, name='y'))
        else:
            aggr = xr_data[params['variable']].isel(time=ix['index'], lat=sindex['lat'], lon=sindex['lon'])
            aggr = aggr.mean(dim=['lon', 'lat'], skipna=True)
            aggr = aggr.rename({'time': 'y'})

        aggr = aggregate_climatology(aggr, params['climFunction'],
                            tclim['minYear'], tclim['percValue'],
                            tclim['freqOper'], tclim['freqThres'])
        out_clim += [aggr]

    out_clim = xr.concat(out_clim, pd.Index(tindex['syear'], name='time'))
    out_clim = out_clim.fillna(dataset['missval'])

    out = pd.DataFrame(out_clim)
    out = [out.map(lambda x: f'{x:.2f}')]

    clon = round((bbox['minLon'] + bbox['maxLon'])/2, 6)
    clat = round((bbox['minLat'] + bbox['maxLat'])/2, 6)
    coords = pd.DataFrame({'loc': ['Rectangle'], 'lon': [clon], 'lat': [clat]})

    return {'status': 0, 'data': out, 'dates': tindex['dates'],
            'coords': coords, 'missval': dataset['missval'],
            'mpars': _clim_multiple_columns(params)}

def climatology_polygons_point_data(params, dataset):
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

    tclim = _clim_params_info(params)
    tindex = get_climatology_times_index(xr_data['time'].values,
                tclim['output'], tclim['input'],
                tclim['startYear'], tclim['endYear'], tclim['minYear'],
                params['fullYear'], params['climDate'],
                tclim['seasLen'], tclim['dayWin'], tclim['minFrac'])

    if tindex['status'] == -1: return tindex

    syear = [d['syear'] for d in tindex['index']]
    out_clim = []
    for s in sindex:
        if len(s[0]) == 0:
            clim = xr.DataArray(data=np.nan, name=params['variable'])
            clim = [clim for i in range(len(tindex['syear']))]
            out_clim += [clim]
            continue

        ilo = xr.DataArray(s[1], dims='points')
        ila = xr.DataArray(s[0], dims='points')

        out_tmp = []
        for t in tindex['syear']:
            if not t in syear:
                clim = xr.DataArray(data=np.nan, name=params['variable'])
                out_tmp += [clim]
                continue

            ix = tindex['index'][syear.index(t)]

            if tindex['aggregate']:
                aggr = []
                for i, v in enumerate(ix['index']):
                    xr_ds = xr_data[params['variable']].isel(time=v, lat=ila, lon=ilo)
                    xr_ds = xr_ds.mean(dim='points', skipna=True)
                    aggr_len = ix['length'][i]
                    aggr += [aggregate_timeSeries(xr_ds, dataset['function'],
                                                  aggr_len, tclim['minFrac'])]
                y_dim = np.arange(0, len(aggr)).tolist()
                aggr = xr.concat(aggr, pd.Index(y_dim, name='y'))
            else:
                aggr = xr_data[params['variable']].isel(time=ix['index'], lat=ila, lon=ilo)
                aggr = aggr.mean(dim='points', skipna=True)
                aggr = aggr.rename({'time': 'y'})

            aggr = aggregate_climatology(aggr, params['climFunction'],
                                tclim['minYear'], tclim['percValue'],
                                tclim['freqOper'], tclim['freqThres'])
            out_tmp += [aggr]

        out_clim += [out_tmp]

    out = []
    for oc in out_clim:
        tmp = xr.concat(oc, pd.Index(tindex['syear'], name='time'))
        tmp = tmp.fillna(dataset['missval'])
        tmp = pd.DataFrame(tmp.values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    coords = table_coords_polygons(shpObj['shp'], params['shpField'], shpObj['polys'])

    return {'status': 0, 'data': out, 'dates': tindex['dates'],
            'coords': coords, 'missval': dataset['missval'],
            'mpars': _clim_multiple_columns(params)}

def climatology_multipoints_data(params, dataset):
    if params['pointsSource'] == 'user':
        csvdata = read_user_csv_mpoints(params['pointsFile'], params['user']['username'])
    else:
        csvdata = format_list_mpoints_dict(params['pointsList'])

    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)
    sindex = create_geom_mpoints_bbox(xr_coords, csvdata, params['padLon'], params['padLat'])

    tclim = _clim_params_info(params)
    tindex = get_climatology_times_index(xr_data['time'].values,
                tclim['output'], tclim['input'],
                tclim['startYear'], tclim['endYear'], tclim['minYear'],
                params['fullYear'], params['climDate'],
                tclim['seasLen'], tclim['dayWin'], tclim['minFrac'])

    if tindex['status'] == -1: return tindex

    syear = [d['syear'] for d in tindex['index']]
    out_clim = []
    for s in sindex:
        if len(s[0]) == 0:
            clim = xr.DataArray(data=np.nan, name=params['variable'])
            clim = [clim for i in range(len(tindex['syear']))]
            out_clim += [clim]
            continue

        out_tmp = []
        for t in tindex['syear']:
            if not t in syear:
                clim = xr.DataArray(data=np.nan, name=params['variable'])
                out_tmp += [clim]
                continue

            ix = tindex['index'][syear.index(t)]

            if tindex['aggregate']:
                aggr = []
                for i, v in enumerate(ix['index']):
                    xr_ds = xr_data[params['variable']].isel(time=v, lat=s[1], lon=s[0])
                    xr_ds = xr_ds.mean(dim=['lon', 'lat'], skipna=True)
                    aggr_len = ix['length'][i]
                    aggr += [aggregate_timeSeries(xr_ds, dataset['function'],
                                                  aggr_len, tclim['minFrac'])]
                y_dim = np.arange(0, len(aggr)).tolist()
                aggr = xr.concat(aggr, pd.Index(y_dim, name='y'))
            else:
                aggr = xr_data[params['variable']].isel(time=ix['index'], lat=s[1], lon=s[0])
                aggr = aggr.mean(dim=['lon', 'lat'], skipna=True)
                aggr = aggr.rename({'time': 'y'})

            aggr = aggregate_climatology(aggr, params['climFunction'],
                                tclim['minYear'], tclim['percValue'],
                                tclim['freqOper'], tclim['freqThres'])
            out_tmp += [aggr]

        out_clim += [out_tmp]

    out = []
    for oc in out_clim:
        tmp = xr.concat(oc, pd.Index(tindex['syear'], name='time'))
        tmp = tmp.fillna(dataset['missval'])
        tmp = pd.DataFrame(tmp.values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    return {'status': 0, 'data': out, 'dates': tindex['dates'],
            'coords': csvdata, 'missval': dataset['missval'],
            'mpars': _clim_multiple_columns(params)}

def climatology_geojson_data(params, dataset):
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

    xr_data = get_zarr_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    tclim = _clim_params_info(params)
    tindex = get_climatology_times_index(xr_data['time'].values,
                tclim['output'], tclim['input'],
                tclim['startYear'], tclim['endYear'], tclim['minYear'],
                params['fullYear'], params['climDate'],
                tclim['seasLen'], tclim['dayWin'], tclim['minFrac'])

    if tindex['status'] == -1: return tindex

    syear = [d['syear'] for d in tindex['index']]
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
            clim = xr.DataArray(data=np.nan, name=params['variable'])
            clim = [clim for i in range(len(tindex['syear']))]
            out_clim += [clim]
            continue

        ilo = xr.DataArray(sindex[1], dims='points')
        ila = xr.DataArray(sindex[0], dims='points')

        out_tmp = []
        for t in tindex['syear']:
            if not t in syear:
                clim = xr.DataArray(data=np.nan, name=params['variable'])
                out_tmp += [clim]
                continue

            ix = tindex['index'][syear.index(t)]

            if tindex['aggregate']:
                aggr = []
                for i, v in enumerate(ix['index']):
                    # 11% of computation time
                    xr_ds = xr_data[params['variable']].isel(time=v, lat=ila, lon=ilo)
                    # 15%
                    xr_ds = xr_ds.mean(dim='points', skipna=True)
                    aggr_len = ix['length'][i]
                    # 74% 0.0085s
                    aggr += [aggregate_timeSeries(xr_ds, dataset['function'],
                                                  aggr_len, tclim['minFrac'])]

                y_dim = np.arange(0, len(aggr)).tolist()
                aggr = xr.concat(aggr, pd.Index(y_dim, name='y'))
            else:
                aggr = xr_data[params['variable']].isel(time=ix['index'], lat=ila, lon=ilo)
                aggr = aggr.mean(dim='points', skipna=True)
                aggr = aggr.rename({'time': 'y'})

            aggr = aggregate_climatology(aggr, params['climFunction'],
                                tclim['minYear'], tclim['percValue'],
                                tclim['freqOper'], tclim['freqThres'])
            out_tmp += [aggr]

        out_clim += [out_tmp]
        out_coords += [info_coords]

    out = []
    for oc in out_clim:
        tmp = xr.concat(oc, pd.Index(tindex['syear'], name='time'))
        tmp = tmp.fillna(dataset['missval'])
        tmp = pd.DataFrame(tmp.values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    coords = pd.DataFrame(out_coords)
    coords.columns = ['loc', 'lon', 'lat', 'type']

    return {'status': 0, 'data': out, 'dates': tindex['dates'],
            'coords': coords, 'missval': dataset['missval'],
            'mpars': _clim_multiple_columns(params)}
