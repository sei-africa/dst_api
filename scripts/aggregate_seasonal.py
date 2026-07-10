import numpy as np
import xarray as xr
import pandas as pd
from .dates import nbdays_of_month
from .zarrdata import get_zarr_dataset_timeres
from .extract import (
    get_coords_dataset,
    format_list_mpoints_dict,
    create_geom_mpoints_bbox,
    create_geom_polygons
)
from .shapefiles import get_shapefiles_data
from app.scripts._global import GLOBAL_CONFIG

def aggregate_seasonal_xrdata(params):
    ret_params = _get_varids_data(params)
    if ret_params['status'] == -1:
        return ret_params
    params = ret_params['params']

    xr_data = get_zarr_dataset_timeres(
        params, params['inputData']
    )

    if params['geomExtract'] == 'points':
        xr_ds = _aggregate_points_xrdata(xr_data, params)
    elif params['geomExtract'] == 'polygons':
        xr_ds = _aggregate_polygons_xrdata(xr_data, params)
    elif params['geomExtract'] == 'original':
        xr_ds = xr_data
    else:
        msg = f"Unknown 'geomExtract': {params['geomExtract']}"
        return {'status': -1, 'message': msg}

    tindex = _get_index_seasonal(
        xr_ds['time'].values,
        params['inputData'],
        params['seasStart'],
        params['seasLength']
    )

    years = np.array([
        k for k in tindex['index'].keys()
    ])

    ds_data = []
    yr_data = []
    for year in years:
        index = tindex['index'][year]
        frac = tindex['length'][year]['frac']
        if frac < params['minFrac']:
            continue
        nb_seas = tindex['length'][year]['nb_seas']
        ds_data += [
            _aggregate_seasonal_xrdata(
                xr_ds, params, index, nb_seas
            )
        ]
        yr_data += [year]

    yr_dim = pd.Index(np.array(yr_data), name='year')
    ds_data = xr.concat(ds_data, yr_dim)
    ds_data = ds_data.rename({
        list(ds_data.data_vars)[0]: 'seas_var'
    })

    return {'status': 0, 'data': ds_data}

def _aggregate_points_xrdata(xr_data, params):
    if params['pointsSource'] == 'user':
        csvdata = read_user_csv_mpoints(
            params['pointsFile'],
            params['user']['username']
        )
    else:
        csvdata = format_list_mpoints_dict(
            params['pointsList']
        )

    xr_coords = get_coords_dataset(xr_data)
    sindex = create_geom_mpoints_bbox(
        xr_coords,
        csvdata,
        params['padLon'],
        params['padLat']
    )

    xr_ds = []
    xr_crds = []
    for i, s in enumerate(sindex):
        if len(s[0]) == 0:
            continue
        tmp_ds = xr_data.isel(lat=s[1], lon=s[0])
        xr_crds += [{
            'name': csvdata.iloc[i, 0],
            'lon': csvdata.iloc[i, 1],
            'lat': csvdata.iloc[i, 2],
        }]
        tmp_ds = tmp_ds.mean(dim=['lon', 'lat'],  skipna=True) 
        xr_ds += [tmp_ds]

    return _combine_points_xrdata(xr_ds, xr_crds)

def _aggregate_polygons_xrdata(xr_data, params):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1: return shpObj

    if type(shpObj['polys']) is not list:
        shpObj['polys'] = [shpObj['polys']]

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

    xr_ds = []
    xr_crds = []
    for i, s in enumerate(sindex):
        if len(s[0]) == 0:
            continue
        ix = xr.DataArray(s[1], dims='points')
        iy = xr.DataArray(s[0], dims='points')
        tmp_ds = xr_data.isel(lat=iy, lon=ix)
        xr_crds += [{
            'name': shpObj['polys'][i],
            'lon': tmp_ds.lon.mean(dim='points').values,
            'lat': tmp_ds.lat.mean(dim='points').values,
        }]
        tmp_ds = tmp_ds.mean(dim='points', skipna=True)
        xr_ds += [tmp_ds]

    return _combine_points_xrdata(xr_ds, xr_crds)

def _combine_points_xrdata(xr_ds, xr_crds):
    new_dim = pd.Index(np.arange(len(xr_crds)), name='points')
    xr_ds = xr.concat(xr_ds, dim=new_dim)
    return xr_ds.assign_coords(
            name=('points', [c['name'] for c in xr_crds]),
            lon=('points', [c['lon'] for c in xr_crds]),
            lat=('points', [c['lat'] for c in xr_crds])
        )

def _get_index_seasonal(times, time_res, seasStart, seasLength):
    d = times.astype('datetime64[D]')
    years = d.astype('datetime64[Y]').astype(int) + 1970
    months = d.astype('datetime64[M]').astype(int) % 12 + 1
    days = (d - d.astype('datetime64[M]')).astype(int) + 1

    start_month = seasStart
    start_day = 1
    end_month = ((seasStart + seasLength) - 1) % 12
    if end_month == 0:
        end_month = 12
    end_day = f'2026-{end_month}'
    end_day = nbdays_of_month(end_day)

    same_year_season = (start_month, start_day) <= (end_month, end_day)

    if same_year_season:
        in_season = (
            ((months > start_month) | ((months == start_month) & (days >= start_day))) &
            ((months < end_month) | ((months == end_month) & (days <= end_day)))
        )
        season_year = years
    else:
        in_start_year = (
            (months > start_month) |
            ((months == start_month) & (days >= start_day))
        )

        in_end_year = (
            (months < end_month) |
            ((months == end_month) & (days <= end_day))
        )

        in_season = in_start_year | in_end_year
        season_year = np.where(in_start_year, years, years - 1)

    season_indices = {
        y: np.where(in_season & (season_year == y))[0]
        for y in np.unique(season_year[in_season])
    }

    season_days = {}
    for y in season_indices:
        start = pd.Timestamp(year=y, month=start_month, day=start_day)

        if same_year_season:
            end = pd.Timestamp(year=y, month=end_month, day=end_day)
        else:
            end = pd.Timestamp(year=y + 1, month=end_month, day=end_day)

        if time_res == 'daily':
            nb_seas = (end - start).days + 1
        elif time_res == 'dekadal':
            nb_seas = 3 * seasLength
        elif time_res == 'monthly':
            nb_seas = seasLength
        else:
            return None

        nb_days = len(season_indices[y])
        season_days[y] = {
            'nb_seas': nb_seas,
            'nb_days': nb_days,
            'frac': nb_days/nb_seas
        }

    return {
        'index': season_indices,
        'length': season_days
    }

def _aggregate_seasonal_xrdata(xr_data, params, index, nb_days):
    xr_ds = xr_data.isel(time=index)
    xr_nomiss = xr_ds.notnull().sum(dim='time')
    xr_frac = xr_nomiss / nb_days

    if params['variable'] == 'rainfall':
        if params['climVariable'] == 'precip':
            don = xr_ds.sum(dim='time', skipna=True)

    if params['variable'] == 'temperature':
        if params['climVariable'] == 'tmin':
            don = xr_ds.mean(dim='time', skipna=True)

        if params['climVariable'] == 'tmax':
            don = xr_ds.mean(dim='time', skipna=True)

        if params['climVariable'] == 'tmean':
            don = xr_ds.mean(dim='time', skipna=True)

    return don.where(xr_frac >= params['minFrac'], np.nan)

def _get_varids_data(params):
    params = params.copy()
    defvar = GLOBAL_CONFIG['datasets'][params['dataset']]['variables']
    if params['variable'] == 'temperature':
        if params['climVariable'] == 'tmin':
            params['varNames'] = [defvar['minimum_temperature']]
        elif params['climVariable'] == 'tmax':
            params['varNames'] = [defvar['maximum_temperature']]
        elif params['climVariable'] == 'tmean':
            params['varNames'] = [defvar['minimum_temperature'],
                                  defvar['maximum_temperature']]
        elif params['climVariable'] == 'trange':
            params['varNames'] = [defvar['minimum_temperature'],
                                  defvar['maximum_temperature']]
        else:
            msg = f"Unknown parameter <climVariable={params['climVariable']}>."
            return {'status': -1, 'message': msg}
    elif params['variable'] == 'rainfall':
        if params['climVariable'] == 'precip':
            params['varNames'] = [defvar['rainfall']]
        else:
            msg = f"Unknown parameter <climVariable={params['climVariable']}>."
            return {'status': -1, 'message': msg}
    else:
        msg = f"Unknown parameter <variable={params['variable']}>."
        return {'status': -1, 'message': msg}

    return {'status': 0, 'params': params}
