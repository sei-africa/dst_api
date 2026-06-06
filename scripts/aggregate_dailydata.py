import numpy as np
import xarray as xr

def aggregate_daily_analysis(xr_ds, params, index, nb_days):
    xr_ds = xr_ds.isel(time=index)
    xr_nomiss = xr_ds.notnull().sum(dim='time')
    xr_frac = xr_nomiss / nb_days

    if params['variable'] == 'rainfall':
        if params['seasParams'] == 'TotRain':
            don = xr_ds.sum(dim='time', skipna=True)

        if params['seasParams'] == 'RainInt':
            wet = xr_ds >= params['defThres']
            swet = wet.sum(dim='time', skipna=True)
            don = xr_ds.where(wet)
            don = don.sum(dim='time', skipna=True)
            don = don / swet

        if params['seasParams'] == 'NumWD':
            wet = xr_ds >= params['defThres']
            don = wet.sum(dim='time', skipna=True)

        if params['seasParams'] == 'NumDD':
            dry = xr_ds < params['defThres']
            don = dry.sum(dim='time', skipna=True)

        if params['seasParams'] == 'NumWS':
            don = xr.apply_ufunc(
                count_spells_1d,
                xr_ds.chunk({'time': -1}),
                input_core_dims=[['time']],
                output_core_dims=[[]],
                vectorize=True,
                dask='parallelized',
                # output_dtypes=[np.int32],
                kwargs={
                    'spell': 'wet',
                    'thres': params['defThres'],
                    'spell_len': params['defSpell'],
                }
            )

        if params['seasParams'] == 'NumDS':
            don = xr.apply_ufunc(
                count_spells_1d,
                xr_ds.chunk({'time': -1}),
                input_core_dims=[['time']],
                output_core_dims=[[]],
                vectorize=True,
                dask='parallelized',
                kwargs={
                    'spell': 'dry',
                    'thres': params['defThres'],
                    'spell_len': params['defSpell'],
                }
            )

        if params['seasParams'] == 'LongDS':
            don = xr.apply_ufunc(
                longest_spells_1d,
                xr_ds.chunk({'time': -1}),
                input_core_dims=[['time']],
                output_core_dims=[[]],
                vectorize=True,
                dask='parallelized',
                kwargs={
                    'spell': 'dry',
                    'thres': params['defThres']
                }
            )

        if params['seasParams'] == 'LongWS':
            don = xr.apply_ufunc(
                longest_spells_1d,
                xr_ds.chunk({'time': -1}),
                input_core_dims=[['time']],
                output_core_dims=[[]],
                vectorize=True,
                dask='parallelized',
                kwargs={
                    'spell': 'wet',
                    'thres': params['defThres']
                }
            )

    if params['variable'] == 'temperature':
        if params['seasParams'] == 'MinTemp':
            don = xr_ds.mean(dim='time', skipna=True)

        if params['seasParams'] == 'MaxTemp':
            don = xr_ds.mean(dim='time', skipna=True)

        if params['seasParams'] == 'MeanTemp':
            don = xr_ds.mean(dim='time', skipna=True)

        if params['seasParams'] == 'NumCD':
            cold = xr_ds < params['defThres']
            don = cold.sum(dim='time', skipna=True)

        if params['seasParams'] == 'NumHD':
            hot = xr_ds >= params['defThres']
            don = hot.sum(dim='time', skipna=True)

        if params['seasParams'] == 'CDD':
            t_diff = xr_ds - params['defTempBase']
            don = xr.where(t_diff > 0, t_diff, 0)
            don = don.sum(dim='time', skipna=True)

        if params['seasParams'] == 'HDD':
            t_diff = params['defTempBase'] - xr_ds
            don = xr.where(t_diff > 0, t_diff, 0)
            don = don.sum(dim='time', skipna=True)

        if params['seasParams'] == 'GDD':
            t_diff = xr_ds - params['defTempBase']
            don = xr.where(t_diff > 0, t_diff, 0)
            don = don.sum(dim='time', skipna=True)

    return don.where(xr_frac >= params['minFrac'], np.nan)

def count_spells_1d(x, spell='dry', thres=1.0, spell_len=7):
    x = np.asarray(x)
    nb_spell = 0
    if spell == 'dry':
        cnt = (~np.isnan(x)) & (x < thres)
    elif spell == 'wet':
        cnt = (~np.isnan(x)) & (x >= thres)
    else:
        return nb_spell

    if np.all(~cnt):
        return nb_spell

    padded = np.r_[False, cnt, False]
    changes = np.diff(padded.astype(np.int8))
    starts = np.where(changes == 1)[0]
    ends = np.where(changes == -1)[0]
    lengths = ends - starts
    valid_lengths = lengths[lengths >= spell_len]
    nb_spell = np.sum(valid_lengths // spell_len)

    return nb_spell.item()

def longest_spells_1d(x, spell='dry', thres=1.0):
    x = np.asarray(x)
    long_spell = 0
    if spell == 'dry':
        cnt = (~np.isnan(x)) & (x < thres)
    elif spell == 'wet':
        cnt = (~np.isnan(x)) & (x >= thres)
    else:
        return long_spell

    if np.all(~cnt):
        return long_spell

    padded = np.r_[False, cnt, False]
    changes = np.diff(padded.astype(np.int8))
    starts = np.where(changes == 1)[0]
    ends = np.where(changes == -1)[0]
    lengths = ends - starts
    long_spell = np.max(lengths)

    return long_spell.item()
