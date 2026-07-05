import numpy as np
import xarray as xr
import pandas as pd
from .dates import nbdays_of_month
from .zarrdata import get_zarr_dataset_timeres
from .extract import get_coords_dataset
from app.scripts._global import GLOBAL_CONFIG

def aggregate_seasonal_xrdata(params):
    """
    params
    timeSeries: true or false
    if timeSeries == true
        geomExtract: 'points'
        pointsSource: 'user', 'upload'
        if pointsSource == 'user'
            pointsFile: 'point_file_name.csv'
        if pointsSource == 'upload'
            pointsList: [{'loc': 'point', 'lon': 47, 'lat': -20}]
        padLon: int
        padLat: int
    inputData: 'daily', 'dekadal', 'monthly'
    seasStart: int
    seasLength: int
    minFrac: 0.95
    variable: 'rainfall', 'temperature'
    if variable == 'rainfall':
        climVariable: 'precip',
    if variable == 'temperature':
        climVariable: 'tmin', 'tmax', 'tmean'

    """
    params = params.copy()
    ret_params = _get_varids_data(params)
    if ret_params['status'] == -1:
        return ret_params
    params = ret_params['params']

    xr_data = get_zarr_dataset_timeres(
        params, params['inputData']
    )
    if params['timeSeries']:
        # xr_coords = get_coords_dataset(xr_data)
        # if params['pointsSource'] == 'user':
        #     csvdata = read_user_csv_mpoints(
        #         params['pointsFile'],
        #         params['user']['username']
        #     )
        # else:
        #     csvdata = format_list_mpoints_dict(
        #         params['pointsList']
        #     )

        # sindex = create_geom_mpoints_bbox(
        #     xr_coords,
        #     csvdata,
        #     params['padLon'],
        #     params['padLat']
        # )

        # sindex
        xr_ds = xr_data
    else:
        xr_ds = xr_data

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

def _aggregate_seasonal_xrdata(xr_ds, params, index, nb_days):
    xr_ds = xr_ds.isel(time=index)
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
    defvar = GLOBAL_CONFIG['datasets'][params['dataset']]['variables']
    if params['variable'] == 'temperature':
        if params['climVariable'] == 'tmin':
            params['varNames'] = [defvar['minimum_temperature']]
        elif params['climVariable'] == 'tmax':
            params['varNames'] = [defvar['maximum_temperature']]
        elif params['climVariable'] == 'tmean':
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
