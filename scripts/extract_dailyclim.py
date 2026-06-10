import json
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from .shapefiles import *
from .extract import *
from .util import remove_duplicates_list
from .index_dailydata import get_daily_index_season
from .aggregate_dailydata import aggregate_daily_analysis
from .zarrdata import get_zarr_daily_dataset
from .geojson import get_user_geojson, geojson_polygons_points
from .aggregate_data import aggregate_climatology

def climatology_gridded_dailydata(params, datainfo, bbox=None, keep_DataArray=False):
    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    if bbox:
        sindex = get_bbox_latlon_index(xr_coords, bbox)
        if sindex is None:
            msg = 'Rectangle outside of data spatial range.'
            return {'status': -1, 'message': msg}
        xr_ds = xr_data.isel(
            lat=sindex['lat'],
            lon=sindex['lon']
        )
    else:
        xr_ds = xr_data

    tclim = _clim_params_info(params)
    tindex = get_daily_index_season(
        xr_ds['time'].values,
        params['startMonth'],
        params['startDay'],
        params['endMonth'],
        params['endDay']
    )
    years = np.array([
        k for k in tindex['index'].keys()
    ])
    year1 = tclim['startYear']
    year2 = tclim['endYear']

    ds_data = []
    for year in range(year1, year2 + 1):
        if not year in years:
            continue
        index = tindex['index'][year]
        frac = tindex['length'][year]['frac']
        if frac < tclim['minFrac']:
            continue
        nb_seas = tindex['length'][year]['nb_seas']
        ds_data += [
            aggregate_daily_analysis(
                xr_ds, params, index, nb_seas
            )
        ]

    if len(ds_data) < tclim['minYear']:
        msg = 'Not enough data to compute the seasonal parameter'
        return {'status': -1, 'message': msg}

    y_dim = np.arange(0, len(ds_data)).tolist()
    ds_data = xr.concat(ds_data, pd.Index(y_dim, name='y'))

    clim = aggregate_climatology(
        ds_data,
        params['seasStats'],
        tclim['minYear'],
        proba_thres=tclim['probaThres'],
        proba_unit=tclim['probaUnit']
    )
    clim = clim.rename({
        list(clim.data_vars)[0]: datainfo['out_varid']
    })
    clim = clim.expand_dims(dim={'time': [1]})
    clim.attrs['date_values'] = _format_date_dailyclim(params)
    clim.attrs['date_dimension'] = 'time'

    if keep_DataArray:
        return clim
    else:
        out = _clim_format_gridded(clim, datainfo)
        out['poly'] = None
        return {'status': 0, 'data': [out]}

def climatology_polygons_grid_dailydata(params, datainfo):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1:
        return shpObj

    if type(shpObj['polys']) is not list:
        shpObj['polys'] = [shpObj['polys']]

    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    tclim = _clim_params_info(params)
    tindex = get_daily_index_season(
        xr_data['time'].values,
        params['startMonth'],
        params['startDay'],
        params['endMonth'],
        params['endDay']
    )
    years = np.array([
        k for k in tindex['index'].keys()
    ])
    year1 = tclim['startYear']
    year2 = tclim['endYear']

    out_clim = []
    for poly in shpObj['polys']:
        bbox = format_bbox_polygons(
            shpObj['bbox'], params['shpField'], poly
        )

        sindex = get_bbox_latlon_index(xr_coords, bbox)
        if sindex is None:
            out_clim += [None]
            continue

        xr_ds = xr_data.isel(
            lat=sindex['lat'],
            lon=sindex['lon']
        )
        xr_ds = mask_polygons_xarray_dataArray(
            xr_ds,
            shpObj['shp'],
            params['shpField'],
            poly
        )

        ds_data = []
        for year in range(year1, year2 + 1):
            if not year in years:
                continue
            index = tindex['index'][year]
            frac = tindex['length'][year]['frac']
            if frac < tclim['minFrac']:
                continue
            nb_seas = tindex['length'][year]['nb_seas']
            aggr = aggregate_daily_analysis(
                xr_ds, params, index, nb_seas
            )
            ds_data += [
                    aggregate_daily_analysis(
                        xr_ds, params, index, nb_seas
                    )
                ]

        if len(ds_data) < tclim['minYear']:
            out_clim += [None]
            continue

        y_dim = np.arange(0, len(ds_data)).tolist()
        ds_data = xr.concat(ds_data, pd.Index(y_dim, name='y'))

        clim = aggregate_climatology(
            ds_data,
            params['seasStats'],
            tclim['minYear'],
            proba_thres=tclim['probaThres'],
            proba_unit=tclim['probaUnit']
        )

        clim = clim.rename({
            list(clim.data_vars)[0]: datainfo['out_varid']
        })

        out_clim += [clim]

    if all(l is None for l in out_clim):
        msg1 = 'All polygons are outside of data spatial range.'
        msg2 = 'Or not enough number of years to compute climatology'
        return {'status': -1, 'message': f'{msg1} {msg2}'}

    index = [True if x else False for x in out_clim]
    out_clim = [x for x, b in zip(out_clim, index) if b]
    poly_name = [x for x, b in zip(shpObj['polys'], index) if b]

    out_data = []
    for i in range(len(out_clim)):
        out_tmp = out_clim[i].expand_dims(dim={'time': [1]})
        out_tmp.attrs['date_values'] = _format_date_dailyclim(params)
        out_tmp.attrs['date_dimension'] = 'time'
        out_tmp = _clim_format_gridded(out_tmp, datainfo)
        out_tmp['poly'] = poly_name[i]
        out_data += [out_tmp]

    return {'status': 0, 'data': out_data}

def climatology_rectangle_point_dailydata(params, datainfo, bbox):
    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    sindex = get_bbox_latlon_index(xr_coords, bbox)
    if sindex is None:
        msg = 'Rectangle outside of data spatial range.'
        return {'status': -1, 'message': msg}

    xr_ds = xr_data.isel(
        lat=sindex['lat'],
        lon=sindex['lon']
    )
    xr_ds = xr_ds.mean(
        dim=['lon', 'lat'],
        skipna=True
    )

    tclim = _clim_params_info(params)
    tindex = get_daily_index_season(
        xr_ds['time'].values,
        params['startMonth'],
        params['startDay'],
        params['endMonth'],
        params['endDay']
    )

    years = np.array([
        k for k in tindex['index'].keys()
    ])
    year1 = tclim['startYear']
    year2 = tclim['endYear']

    ds_data = []
    for year in range(year1, year2 + 1):
        if not year in years:
            continue
        index = tindex['index'][year]
        frac = tindex['length'][year]['frac']
        if frac < tclim['minFrac']:
            continue
        nb_seas = tindex['length'][year]['nb_seas']
        ds_data += [
            aggregate_daily_analysis(
                xr_ds, params, index, nb_seas
            )
        ]

    if len(ds_data) < tclim['minYear']:
        msg = 'Not enough data to compute the seasonal parameter'
        return {'status': -1, 'message': msg}

    y_dim = np.arange(0, len(ds_data)).tolist()
    ds_data = xr.concat(ds_data, pd.Index(y_dim, name='y'))

    clim = aggregate_climatology(
        ds_data,
        params['seasStats'],
        tclim['minYear'],
        proba_thres=tclim['probaThres'],
        proba_unit=tclim['probaUnit']
    )

    clim = clim.rename({
        list(clim.data_vars)[0]: datainfo['out_varid']
    })
    clim = clim.fillna(datainfo['missval'])
    clim = clim[datainfo['out_varid']].values
    out = clim.reshape(1, 1)
    out = pd.DataFrame(out)
    out = [out.map(lambda x: f'{x:.2f}')]

    clon = round((bbox['minLon'] + bbox['maxLon']) / 2, 6)
    clat = round((bbox['minLat'] + bbox['maxLat']) / 2, 6)
    coords = pd.DataFrame({'loc': ['Rectangle'], 'lon': [clon], 'lat': [clat]})

    return {
        'status': 0,
        'data': out,
        'dates': _format_date_dailyclim(params),
        'coords': coords,
        'missval': datainfo['missval'],
        'mpars': _clim_multiple_columns(params)
    }

def climatology_polygons_point_dailydata(params, datainfo):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1:
        return shpObj

    if type(shpObj['polys']) is not list:
        shpObj['polys'] = [shpObj['polys']]

    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    sindex = []
    for poly in shpObj['polys']:
        sindex += [
            create_geom_polygons(
                xr_coords,
                shpObj['shp'],
                params['shpField'],
                poly
            )
        ]

    tclim = _clim_params_info(params)
    tindex = get_daily_index_season(
        xr_data['time'].values,
        params['startMonth'],
        params['startDay'],
        params['endMonth'],
        params['endDay']
    )
    years = np.array([
        k for k in tindex['index'].keys()
    ])
    year1 = tclim['startYear']
    year2 = tclim['endYear']

    out_clim = []
    for s in sindex:
        if len(s[0]) == 0:
            clim = xr.DataArray(
                data=np.nan,
                name=datainfo['out_varid']
            )
            out_clim += [clim.to_dataset()]
            continue

        ilo = xr.DataArray(s[1], dims='points')
        ila = xr.DataArray(s[0], dims='points')
        xr_ds = xr_data.isel(lat=ila, lon=ilo)
        xr_ds = xr_ds.mean(dim='points', skipna=True)

        ds_data = []
        for year in range(year1, year2 + 1):
            if not year in years:
                continue
            index = tindex['index'][year]
            frac = tindex['length'][year]['frac']
            if frac < tclim['minFrac']:
                continue
            nb_seas = tindex['length'][year]['nb_seas']
            aggr = aggregate_daily_analysis(
                xr_ds, params, index, nb_seas
            )
            ds_data += [
                    aggregate_daily_analysis(
                        xr_ds, params, index, nb_seas
                    )
                ]

        if len(ds_data) < tclim['minYear']:
            clim = xr.DataArray(
                data=np.nan,
                name=datainfo['out_varid']
            )
            out_clim += [clim.to_dataset()]
            continue

        y_dim = np.arange(0, len(ds_data)).tolist()
        ds_data = xr.concat(ds_data, pd.Index(y_dim, name='y'))

        clim = aggregate_climatology(
            ds_data,
            params['seasStats'],
            tclim['minYear'],
            proba_thres=tclim['probaThres'],
            proba_unit=tclim['probaUnit']
        )
        clim = clim.rename({
            list(clim.data_vars)[0]: datainfo['out_varid']
        })

        out_clim += [clim]

    out = []
    for oc in out_clim:
        tmp = oc.expand_dims(dim={'time': [1]})
        tmp = tmp.fillna(datainfo['missval'])
        tmp = pd.DataFrame(tmp[datainfo['out_varid']].values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    coords = table_coords_polygons(
        shpObj['shp'],
        params['shpField'],
        shpObj['polys']
    )

    return {
            'status': 0,
            'data': out,
            'dates': _format_date_dailyclim(params),
            'coords': coords,
            'missval': datainfo['missval'],
            'mpars': _clim_multiple_columns(params)
        }

def climatology_multipoints_dailydata(params, datainfo):
    if params['pointsSource'] == 'user':
        csvdata = read_user_csv_mpoints(
            params['pointsFile'],
            params['user']['username']
        )
    else:
        csvdata = format_list_mpoints_dict(params['pointsList'])

    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    sindex = create_geom_mpoints_bbox(
        xr_coords,
        csvdata,
        params['padLon'],
        params['padLat']
    )

    tclim = _clim_params_info(params)
    tindex = get_daily_index_season(
        xr_data['time'].values,
        params['startMonth'],
        params['startDay'],
        params['endMonth'],
        params['endDay']
    )
    years = np.array([
        k for k in tindex['index'].keys()
    ])
    year1 = tclim['startYear']
    year2 = tclim['endYear']

    out_clim = []
    for s in sindex:
        if len(s[0]) == 0:
            clim = xr.DataArray(
                data=np.nan,
                name=datainfo['out_varid']
            )
            out_clim += [clim.to_dataset()]
            continue

        xr_ds = xr_data.isel(lat=s[1], lon=s[0])
        xr_ds = xr_ds.mean(dim=['lon', 'lat'], skipna=True)

        ds_data = []
        for year in range(year1, year2 + 1):
            if not year in years:
                continue
            index = tindex['index'][year]
            frac = tindex['length'][year]['frac']
            if frac < tclim['minFrac']:
                continue
            nb_seas = tindex['length'][year]['nb_seas']
            aggr = aggregate_daily_analysis(
                xr_ds, params, index, nb_seas
            )
            ds_data += [
                    aggregate_daily_analysis(
                        xr_ds, params, index, nb_seas
                    )
                ]

        if len(ds_data) < tclim['minYear']:
            clim = xr.DataArray(
                data=np.nan,
                name=datainfo['out_varid']
            )
            out_clim += [clim.to_dataset()]
            continue

        y_dim = np.arange(0, len(ds_data)).tolist()
        ds_data = xr.concat(ds_data, pd.Index(y_dim, name='y'))

        clim = aggregate_climatology(
            ds_data,
            params['seasStats'],
            tclim['minYear'],
            proba_thres=tclim['probaThres'],
            proba_unit=tclim['probaUnit']
        )
        clim = clim.rename({
            list(clim.data_vars)[0]: datainfo['out_varid']
        })

        out_clim += [clim]

    out = []
    for oc in out_clim:
        tmp = oc.expand_dims(dim={'time': [1]})
        tmp = tmp.fillna(datainfo['missval'])
        tmp = pd.DataFrame(tmp[datainfo['out_varid']].values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    return {
            'status': 0,
            'data': out,
            'dates': _format_date_dailyclim(params),
            'coords': csvdata,
            'missval': datainfo['missval'],
            'mpars': _clim_multiple_columns(params)
        }

def climatology_geojson_dailydata(params, datainfo):
    if params['geojsonSource'] == 'user':
        json_data = get_user_geojson(
            params['geojsonFile'],
            params['user']['username']
        )
        if json_data['status'] == -1: return json_data
        json_data = json.loads(json_data['geojson'])
    else:
        json_data = params['geojsonData']

    geojson = gpd.GeoDataFrame.from_features(json_data['features'])
    geojson = geojson_polygons_points(geojson)
    if geojson['status'] == -1:
        return geojson

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

    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    tclim = _clim_params_info(params)
    tindex = get_daily_index_season(
        xr_data['time'].values,
        params['startMonth'],
        params['startDay'],
        params['endMonth'],
        params['endDay']
    )
    years = np.array([
        k for k in tindex['index'].keys()
    ])
    year1 = tclim['startYear']
    year2 = tclim['endYear']

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
                'type': 'point',
            }
            ncrds = dict(zip(crds.columns.tolist(), ['lon', 'lat']))
            crds = crds.rename(columns=ncrds)

            sindex = create_geom_mpoints_bbox(xr_coords, crds, 0, 0)
            sindex = sindex[0]
        else:
            continue

        if len(sindex[0]) == 0:
            clim = xr.DataArray(
                data=np.nan,
                name=datainfo['out_varid']
            )
            out_clim += [clim.to_dataset()]
            continue

        ilo = xr.DataArray(sindex[1], dims='points')
        ila = xr.DataArray(sindex[0], dims='points')
        xr_ds = xr_data.isel(lat=ila, lon=ilo)
        xr_ds = xr_ds.mean(dim='points', skipna=True)

        ds_data = []
        for year in range(year1, year2 + 1):
            if not year in years:
                continue
            index = tindex['index'][year]
            frac = tindex['length'][year]['frac']
            if frac < tclim['minFrac']:
                continue
            nb_seas = tindex['length'][year]['nb_seas']
            aggr = aggregate_daily_analysis(
                xr_ds, params, index, nb_seas
            )
            ds_data += [
                    aggregate_daily_analysis(
                        xr_ds, params, index, nb_seas
                    )
                ]

        if len(ds_data) < tclim['minYear']:
            clim = xr.DataArray(
                data=np.nan,
                name=datainfo['out_varid']
            )
            out_clim += [clim.to_dataset()]
            continue

        y_dim = np.arange(0, len(ds_data)).tolist()
        ds_data = xr.concat(ds_data, pd.Index(y_dim, name='y'))

        clim = aggregate_climatology(
            ds_data,
            params['seasStats'],
            tclim['minYear'],
            proba_thres=tclim['probaThres'],
            proba_unit=tclim['probaUnit']
        )
        clim = clim.rename({
            list(clim.data_vars)[0]: datainfo['out_varid']
        })

        out_clim += [clim]
        out_coords += [info_coords]

    out = []
    for oc in out_clim:
        tmp = oc.expand_dims(dim={'time': [1]})
        tmp = tmp.fillna(datainfo['missval'])
        tmp = pd.DataFrame(tmp[datainfo['out_varid']].values)
        tmp = tmp.map(lambda x: f'{x:.2f}')
        out += [tmp]

    coords = pd.DataFrame(out_coords)
    coords.columns = ['loc', 'lon', 'lat', 'type']

    return {
            'status': 0,
            'data': out,
            'dates': _format_date_dailyclim(params),
            'coords': coords,
            'missval': datainfo['missval'],
            'mpars': _clim_multiple_columns(params)
        }

def _format_date_dailyclim(params):
    s_year = int(params['startYear'])
    e_year = int(params['endYear'])
    s_mon = int(params['startMonth'])
    s_day = int(params['startDay'])
    e_mon = int(params['endMonth'])
    e_day = int(params['endDay'])

    year = f'{s_year}-{e_year}'
    start = f'{s_mon:02d}-{s_day:02d}'
    end = f'{e_mon:02d}-{e_day:02d}'
    return f'{year}_{start}_{end}'

def _clim_format_gridded(clim, datainfo):
    lon = clim['lon'].values.tolist()
    lon = [round(x, 6) for x in lon]
    lat = clim['lat'].values.tolist()
    lat = [round(x, 6) for x in lat]

    date = clim.attrs['date_values']
    time = clim['time'].values.tolist()

    name_dim4 = None
    dim4 = None
    ndims = dict(clim.sizes)
    if len(ndims) > 3:
        dims3 = ['lon', 'lat', 'time']
        exdim = [l for l in ndims.keys() if l not in dims3]
        name_dim4 = exdim[0]
        clim = clim.transpose(name_dim4, 'time', 'lat', 'lon')
        dim4 = clim[name_dim4].values.tolist()

    xdata = clim[datainfo['out_varid']].values
    xdata = np.nan_to_num(xdata, nan=datainfo['missval'])
    xdata = xdata.tolist()
    if len(ndims) > 3:
        xdata = [[[[round(l, 2) for l in z] for z in y] for y in x] for x in xdata]
    else:
        xdata = [[[round(z, 2) for z in y] for y in x] for x in xdata]

    return {
            'lon': lon,
            'lat': lat,
            'time': time,
            'date': date,
            'data': xdata,
            'ndims': ndims,
            'ndim4': name_dim4,
            'dim4': dim4,
            'missval': datainfo['missval'],
        }

def _clim_params_info(params):
    min_frac = 1.0
    if 'minFrac' in params:
       min_frac = float(params['minFrac']) 

    start_year = 1991
    if 'startYear' in params:
        start_year = int(params['startYear'])

    end_year = 2020
    if 'endYear' in params:
        end_year = int(params['endYear'])

    min_year = 30
    if 'minYear' in params:
        min_year = int(params['minYear'])

    proba_thres = 20.0
    if params['seasStats'] == 'probExc':
        proba_thres = float(params['probaThres'])

    if params['seasStats'] == 'probNoExc':
        proba_thres = float(params['probaThres'])

    proba_unit = 'perc'
    if params['seasStats'] == 'probExc' or params['seasStats'] == 'probNoExc':
        proba_unit = params['probaUnit']

    return {
            'startYear': start_year,
            'endYear': end_year,
            'minYear': min_year,
            'minFrac': min_frac,
            'probaThres': proba_thres,
            'probaUnit': proba_unit
        }

def _clim_multiple_columns(params):
    ret = None
    if params['seasStats'] == 'percentile':
        if type(params['precentileValue']) is list:
            if len(params['precentileValue']) > 1:
                ret = {
                    'name': 'Percentiles',
                    'values': params['precentileValue']
                }
    if params['seasStats'] == 'mean-stdev':
        ret = {
            'name': 'Statistics',
            'values': ['mean', 'stdev']
        }
    if params['seasStats'] == 'trend':
        ret = {
            'name': 'Metrics',
            'values': [
                'slope',
                'std.slope',
                't-value.slope',
                'p-value.slope',
                'intercept',
                'std.intercept',
                't-value.intercept',
                'p-value.intercept',
                'R2',
                'sigma',
            ],
        }

    return ret
