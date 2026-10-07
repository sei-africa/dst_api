import json
import numpy as np
import pandas as pd
import xarray as xr

from app.scripts._global import GLOBAL_CONFIG
from app.scripts._cache import (
    cache,
    hash_pamars_spei,
    hash_distr_pamars_spei
)

from .zarrdata import get_zarr_dataset
from .dates import convert_strings_npdatetime64
from .shapefiles import (
    get_shapefiles_data,
    format_bbox_polygons,
    extract_polygons_griddata
)
from .netcdf import extract_netcdf_bbox

from .spei_functions import *
from .aggregate_dataarray import xr_aggregate_data
from .download_raw import download_rawdata
from .data_info import get_datasets_information

def get_spei_data(params):
    cache_key = hash_pamars_spei(params)
    cache.delete(cache_key)
    spei_data = cache.get(cache_key)

    if spei_data is None:
        if params['gridded']:
            distr_pars = get_spei_distribution_pars(params)
            if distr_pars['status'] == -1: return distr_pars
            try:
                spei_data = _spei_spatial_data(distr_pars['data'], params)
            except Exception as e:
                return {'status': -1, 'message': str(e)}
        else:
            try:
                spei_data = _spei_points_data(params)
            except Exception as e:
                return {'status': -1, 'message': str(e)}

        cache.set(cache_key, spei_data)

    return {'status': 0, 'data': spei_data}

def get_spei_distribution_pars(params):
    cache_key = hash_distr_pamars_spei(params)
    # cache.delete(cache_key)
    distr_pars = cache.get(cache_key)

    if distr_pars is None:
        try:
            distr_pars = _spei_distribution_pars(params)
        except Exception as e:
            return {'status': -1, 'message': str(e)}

        cache.set(cache_key, distr_pars)

    return {'status': 0, 'data': distr_pars}

def check_spei_cache_status(params):
    cache_key = hash_distr_pamars_spei(params)
    return {
        'status': 0,
        'cached': cache.has(cache_key)
    }

def clear_cache_spei_distribution_pars(
    analysis='spi',
    dataset='MON',
    temporalRes='dekadal',
    timeScale=1,
    **kwargs
):
    params = {
        'analysis': analysis,
        'dataset': dataset,
        'temporalRes': temporalRes,
        'timeScale': timeScale
    }
    if analysis == 'spei':
        params['distribution'] = kwargs.get(
            'distribution', 'llogistic'
        )
    else:
        params['distribution'] = kwargs.get('distribution', 'gamma')

    if 'variable' in kwargs:
        params['variable'] = kwargs['variable']
    else:
        if analysis == 'spei':
            params['variable'] = ['precip', 'et0']
        else:
            params['variable'] = ['precip']

    if temporalRes == 'seasonal':
        params['timeRes'] = 'monthly'

    cache_key = hash_distr_pamars_spei(params)
    cache.delete(cache_key)
    return 0

def _spei_spatial_data(distr_pars, params):
    precip, et0 = _get_spei_xr_data(params)
    spei_args = SPEI_Wrapper_setup(precip, params)
    if params['analysis'] == 'spei':
        data = precip - et0
    else:
        data = precip

    if spei_args['time_res'] == 'dekadal':
        this_date = convert_strings_npdatetime64(
            params['Date'],
            params['temporalRes'],
            sep = '-'
        ).astype('datetime64[ns]')
        xr_ds = data.sel(time=this_date)
    else:
        if spei_args['tscale'] == 1:
            this_date = convert_strings_npdatetime64(
                params['Date'],
                params['temporalRes'],
                sep = '-',
                mon_day=1
            ).astype('datetime64[ns]')
            xr_ds = data.sel(time=this_date)
        else:
            seas_dates = convert_strings_npdatetime64(
                params['Date'].split('_'),
                'monthly',
                sep = '-',
                mon_day=1
            ).astype('datetime64[ns]')
            xr_ds = data.sel(time=slice(seas_dates[0], seas_dates[1]))

    data_aggr = xr_ds.sum(
        dim='time',
        skipna=True,
        keepdims=True
    ).assign_coords(
        time=[xr_ds.time.values[-1]]
    )

    spei = SPEI_Computation_sp(
        data_aggr,
        distr_pars,
        spei_args['tscale'],
        spei_args['frequency'],
        spei_args['distribution'],
        spei_args['time_res'],
        params['analysis']
    )

    if params['geomExtract'] == 'original':
        return _spei_gridded_data(spei)

    if params['geomExtract'] == 'rectangle':
        bbox = {
            k: float(params[k])
            for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
        }
        spei = spei.sel(
            lon=slice(bbox['minLon'], bbox['maxLon']),
            lat=slice(bbox['minLat'], bbox['maxLat'])
        )
        return _spei_gridded_data(spei)

    if params['geomExtract'] == 'polygons':
        shpObj = get_shapefiles_data(params)
        if shpObj['status'] == -1: return shpObj

        multipolygons = False
        if type(shpObj['polys']) is list:
            if len(shpObj['polys']) > 1:
                multipolygons = True
            else:
                shpObj['polys'] = shpObj['polys'][0]

        np_spei = {
            'lon': spei['lon'].values,
            'lat': spei['lat'].values,
            'data': np.squeeze(spei.values)
        }
        info_spei = {
            'date': params['Date'],
            'varid': spei.name,
            'long_name': spei.attrs['long_name'],
            'units': spei.attrs['units']
        }

        if multipolygons:
            out_spei = []
            for poly in shpObj['polys']:
                bbox = format_bbox_polygons(
                    shpObj['bbox'],
                    params['shpField'],
                    poly
                )
                ret = extract_netcdf_bbox(np_spei, bbox)
                ext = extract_polygons_griddata(
                    ret,
                    shpObj['shp'],
                    params['shpField'],
                    poly
                )
                ext['poly'] = poly
                ext = ext | info_spei
                out_spei += [_np_spei_gridded_data(ext)]
        else:
            out = extract_polygons_griddata(
                np_spei,
                shpObj['shp'],
                params['shpField'],
                shpObj['polys']
            )
            out['poly'] = shpObj['polys']
            out = out | info_spei
            out_spei = _np_spei_gridded_data(out)

        return out_spei

def _spei_distribution_pars(params):
    precip, et0 = _get_spei_xr_data(params)
    spei_args = SPEI_Wrapper_setup(precip, params)
    if params['analysis'] == 'spei':
        data = precip - et0
    else:
        data = precip
    data_aggr = SPEI_Aggregate_data(
        data, spei_args['tscale']
    )
    return SPEI_Compute_params(
        data_aggr,
        spei_args['tscale'],
        spei_args['frequency'],
        spei_args['distribution'],
        min_non_na=5,
        n_jobs=params.get('n_jobs')
    )

def _spei_gridded_data(spei):
    out = {}
    if spei.attrs['time_resolution'] == 'monthly':
        out['Date'] = spei.time.dt.strftime('%Y-%m').values[0]

    if spei.attrs['time_resolution'] == 'dekadal':
        yymm = spei['time'].dt.strftime('%Y-%m').values[0]
        dekad = xr.where(
            spei['time'].dt.day <= 10, 1,
            xr.where(spei['time'].dt.day <= 20, 2, 3)
        )
        out['Date'] = f'{yymm}-{dekad.values[0]}'

    out['Latitude'] = spei['lat'].round(6).values.tolist()
    out['Longitude'] = spei['lon'].round(6).values.tolist()
    out['Dimensions'] = {
        'Latitude': spei.sizes['lat'],
        'Longitude': spei.sizes['lon']
    }

    miss = -9999.0
    out['Missing'] = miss
    out['Data'] = spei.fillna(miss).values.tolist()

    out['VariableVarId'] = spei.name
    out['VariableName'] = spei.attrs['long_name']
    out['VariableUnits'] = spei.attrs['units']
    return out

def _np_spei_gridded_data(np_spei):
    out = {}
    out['Date'] = np_spei['date']
    out['Latitude'] = np.round(np_spei['lat'], 6).tolist()
    out['Longitude'] = np.round(np_spei['lon'], 6).tolist()
    out['Dimensions'] = {
        'Latitude': len(np_spei['lat']),
        'Longitude': len(np_spei['lon'])
    }

    miss = -9999.0
    out['Missing'] = miss
    data_filled = np.ma.filled(np_spei['data'], miss)
    data_filled = np.nan_to_num(
        data_filled, nan=miss, posinf=miss, neginf=miss
    )
    out['Data'] = data_filled.tolist()

    out['VariableVarId'] = np_spei['varid']
    out['VariableName'] = np_spei['long_name']
    out['VariableUnits'] = np_spei['units']
    out['Name'] = np_spei['poly']
    return out

def _get_spei_xr_data(params):
    data_sets = GLOBAL_CONFIG['datasets'][params['dataset']]
    dset_vars = data_sets['variables']
    params_data = {
        k: params[k]
        for k in ['temporalRes', 'dataset']
    }
    if 'timeRes' in params:
        params_data['temporalRes'] = params['timeRes']

    params_precip = params_data.copy()
    # params_precip['variable'] = params['variable'][0]
    # var_precip = params_precip['variable']
    var_precip = dset_vars['rainfall']
    params_precip['variable'] = var_precip
    info_precip = data_sets[params_precip['temporalRes']]['netcdf']
    info_precip = info_precip[var_precip]
    precip = get_zarr_dataset(params_precip)
    precip_da = precip[var_precip]
    if info_precip['compute']:
        precip_da = xr_aggregate_data(
            precip_da,
            info_precip['function'],
            info_precip['input'],
            params_precip['temporalRes'],
            info_precip['minfrac']
        )

    et0_da = None
    if params['analysis'] == 'spei':
        params_et0 = params_data.copy()
        if 'reference_evapotranspiration' not in dset_vars:
            raise ValueError('No evapotranspiration data found.')
        # params_et0['variable'] = params['variable'][1]
        # var_et0 = params_et0['variable']
        var_et0 = dset_vars['reference_evapotranspiration']
        params_et0['variable'] = var_et0
        info_et0 = data_sets[params_et0['temporalRes']]['netcdf']
        info_et0 = info_et0[var_et0]
        et0 = get_zarr_dataset(params_et0)
        et0_da = et0[var_et0]
        if info_et0['compute']:
            et0_da = xr_aggregate_data(
                et0_da,
                info_et0['function'],
                info_et0['input'],
                params_et0['temporalRes'],
                info_et0['minfrac']
            )
        precip_da, et0_da = xr.align(
            precip_da, et0_da, join='inner'
        )

    return precip_da, et0_da

def _spei_points_data(params):
    dates_range = _format_spei_ts_dates_range(params)
    if dates_range is None:
        raise ValueError('Unknown temporal resolution')

    precip, et0 = _get_spei_ts_data(params)
    spei = SPEI_computation_wrapper(precip, params, et0)
    spei = spei.sel(time=slice(dates_range[0], dates_range[1]))

    out = {}
    if spei.attrs['time_resolution'] == 'monthly':
        out['Dates'] = spei.time.dt.strftime('%Y%m').values.tolist()

    if spei.attrs['time_resolution'] == 'dekadal':
        yymm = spei['time'].dt.strftime('%Y%m').values
        dekad = xr.where(
            spei['time'].dt.day <= 10, 1,
            xr.where(spei['time'].dt.day <= 20, 2, 3)
        ).values
        dek_dates = np.char.add(yymm.astype(str), dekad.astype(str))
        out['Dates'] = dek_dates.tolist()

    out['Data'] = [
        {
            'Name': str(point),
            'Longitude': float(spei.lon.isel(point=i).item()),
            'Latitude': float(spei.lat.isel(point=i).item()),
            'Values': np.where(
                np.isnan(values := spei.isel(point=i).values),
                -9999.0,
                values.round(3),
            ).tolist()
        }
        for i, point in enumerate(spei.point.values)
    ]
    out['VariableVarId'] = spei.name
    out['VariableName'] = spei.attrs['long_name']
    out['VariableUnits'] = spei.attrs['units']
    out['Missing'] = -9999.0

    return out

def _get_spei_ts_data(params):
    data_infos = get_datasets_information()
    data_infos = data_infos[params['dataset']]
    params_spei = params.copy()
    if params['temporalRes'] == 'seasonal':
        params_spei['temporalRes'] = 'monthly'

    if params_spei['geomExtract'] == 'polygons':
        if 'allPolygons' not in params_spei:
            params_spei['allPolygons'] = False

    params_spei['variable'] = params['variable'][0]
    data_infos = data_infos[params_spei['temporalRes']]
    data_infos = data_infos[params_spei['variable']]
    data_cov = data_infos['temporal_coverage']
    params_spei['startDate'] = data_cov['start']
    params_spei['endDate'] = data_cov['end']

    precip = _get_spei_ts_rawdata(params_spei)
    if precip['status'] != 0:
        raise ValueError(precip['message'])
    precip_da = precip['data']

    et0_da = None
    if params['analysis'] == 'spei':
        params_spei['variable'] = params['variable'][1]
        et0 = _get_spei_ts_rawdata(params_spei)
        if et0['status'] != 0:
            raise ValueError(et0['message'])
        et0_da = et0['data']
        precip_da, et0_da = xr.align(
            precip_da,
            et0_da,
            join='outer',
            fill_value=np.nan,
            exclude={'point'}
        )
    return precip_da, et0_da

def _get_spei_ts_rawdata(params):
    data_raw = download_rawdata(params)
    data_raw = json.loads(data_raw)
    if data_raw['status'] != 0: return data_raw
    data_raw = json.loads(data_raw['data'])

    data_out = []
    for x in data_raw['Data']:
        y = np.array(x['Values'])
        y[y == float(data_raw['Missing'])] = np.nan
        x['Values'] = y
        data_out += [x]

    dates = _format_spei_ts_dates(
        data_raw['Dates'], params['temporalRes']
    )

    da = xr.DataArray(
        np.stack([p['Values'] for p in data_out], axis=1),
        dims=('time', 'point'),
        coords={
            'time': np.asarray(dates, dtype='datetime64[ns]'),
            'point': [p['Name'] for p in data_out],
            'lon': ('point', [p['Longitude'] for p in data_out]),
            'lat': ('point', [p['Latitude'] for p in data_out]),
        },
        name=params['variable']
    )

    if 'Type' in data_out[0]:
        da.attrs['geometry_type'] = [p['Type'] for p in data_out]

    return {'status': 0, 'data': da}

def _format_spei_ts_dates(dates, time_res):
    if time_res == 'dekadal':
       return convert_strings_npdatetime64(
            dates, time_res, sep = ''
        ).astype('datetime64[ns]')

    if time_res == 'monthly':
        return convert_strings_npdatetime64(
            dates, time_res, sep = '', mon_day=1
        ).astype('datetime64[ns]')

def _format_spei_ts_dates_range(params):
    if params['temporalRes'] == 'dekadal':
        return convert_strings_npdatetime64(
            [params['startDate'], params['endDate']],
            params['temporalRes'],
            sep = '-'
        ).astype('datetime64[ns]')
    elif params['temporalRes'] == 'monthly':
        return convert_strings_npdatetime64(
            [params['startDate'], params['endDate']],
            params['temporalRes'],
            sep = '-',
            mon_day=1
        ).astype('datetime64[ns]')
    elif params['temporalRes'] == 'seasonal':
        start = f"{params['startDate']}-01"
        end = f"{params['endDate']}-12"
        return convert_strings_npdatetime64(
            [start, end],
            'monthly',
            sep = '-',
            mon_day=1
        ).astype('datetime64[ns]')
    else:
        return None
