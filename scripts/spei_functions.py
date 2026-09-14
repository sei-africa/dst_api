from __future__ import annotations
from typing import Mapping
from collections.abc import Mapping
import calendar
import numpy as np
import xarray as xr
from scipy import stats

def SPI_computation_wrapper(
    precip: xr.DataArray,
    params: Mapping[str, object]
) -> xr.DataArray:
    spi_args = SPEI_Wrapper_setup(precip, params)
    data = SPEI_Aggregate_data(
        precip, spi_args['tscale']
    )
    fitted = SPEI_Compute_params(
        data,
        spi_args['tscale'],
        spi_args['frequency'],
        spi_args['distribution'],
        min_non_na=5
    )
    spi = SPEI_Computation_ts(
        data,
        fitted,
        spi_args['tscale'],
        spi_args['frequency'],
        spi_args['distribution'],
        spi_args['time_res'],
        'spi'
    )
    start = spi_args['tscale'] - 1
    return spi.isel(time=slice(start, None))

def SPEI_computation_wrapper(
    precip: xr.DataArray,
    etp: xr.DataArray,
    params: Mapping[str, object]
) -> xr.DataArray:
    spei_args = SPEI_Wrapper_setup(precip, params)
    precip, etp = xr.align(precip, etp, join='inner')
    data = SPEI_Aggregate_data(
        precip - etp, spei_args['tscale']
    )
    fitted = SPEI_Compute_params(
        data,
        spei_args['tscale'],
        spei_args['frequency'],
        spei_args['distribution'],
        min_non_na=5
    )
    spei = SPEI_Computation_ts(
        data,
        fitted,
        spei_args['tscale'],
        spei_args['frequency'],
        spei_args['distribution'],
        spei_args['time_res'],
        'spei'
    )
    start = spei_args['tscale'] - 1
    return spei.isel(time=slice(start, None))

def SPEI_Aggregate_data(
    data: xr.DataArray,
    tscale: int = 1
) -> xr.DataArray:
    values = _dataarray_values(data)
    if tscale < 1:
        raise ValueError('tscale must be at least 1')
    if tscale == 1:
        return data.astype(float).copy(deep=True)

    out = np.full(values.shape, np.nan)
    for k in range(
        max(0, values.shape[0] - tscale + 1)
    ):
        out[k + tscale - 1] = np.nansum(
            values[k : k + tscale],
            axis=0
        )
    return xr.DataArray(
        out.reshape(data.shape),
        coords=data.coords,
        dims=data.dims,
        name=data.name,
        attrs=data.attrs
    )

def SPEI_Wrapper_setup(
    precip: xr.DataArray,
    params: Mapping[str, object]
) -> tuple[int, int]:
    # _dataarray_values(precip)
    tscale = int(params['timeScale'])
    if 'timeRes' in params:
        timeres = str(params['timeRes'])
    else:
        timeres = str(params['temporalRes'])

    if timeres not in ('monthly', 'dekadal'):
        raise ValueError("time_res must be 'monthly' or 'dekadal'")
    if timeres == 'dekadal' and tscale > 1:
        raise ValueError('Time scale must be 1 for dekadal data')

    nominal = 36 if timeres == 'dekadal' else 12
    frequency = min(nominal, max(0, precip.sizes['time'] - tscale + 1))
    distribution = str(params.get('distribution', 'gamma'))
    return {
        'tscale': tscale,
        'time_res': timeres,
        'distribution': distribution,
        'frequency': frequency
    }

def SPEI_Compute_params(
    data: xr.DataArray,
    tscale: int = 1,
    frequency: int = 12,
    distribution: str = 'gamma',
    min_non_na: int = 5
) -> np.ndarray:
    values = _dataarray_values(data)
    if frequency < 1:
        raise ValueError('frequency must be at least 1')
    params = np.full(
        (frequency, values.shape[1]),
        None,
        dtype=object
    )
    eligible = np.flatnonzero(
        np.sum(~np.isnan(values), axis=0) >= min_non_na
    )
    for k in range(frequency):
        rows = np.arange(
            k + tscale - 1,
            values.shape[0],
            frequency
        )
        if rows.size == 0:
            continue
        for j in eligible:
            sample = values[rows, j]
            sample = sample[~np.isnan(sample)]
            if sample.size < min_non_na:
                continue
            if distribution == 'zscore':
                params[k, j] = {
                    'mean': float(np.mean(sample)),
                    'sd': float(np.std(sample, ddof=1))
                }
                continue
            pzero = (
                float(np.mean(sample == 0))
                if distribution in ('gamma', 'peasron3')
                else None
            )
            fit_values = (
                sample[sample > 0]
                if distribution in ('gamma', 'peasron3')
                else sample
            )
            fitted = _fit_distribution(
                fit_values, distribution, min_non_na
            )
            if fitted is not None:
                if pzero is not None:
                    fitted['pzero'] = pzero
                params[k, j] = fitted
    return params

def SPEI_Computation_ts(
    data_aggr: xr.DataArray,
    distr_pars: np.ndarray,
    tscale: int = 1,
    frequency: int = 12,
    distribution: str = 'gamma',
    time_res: str = 'monthly',
    spei_type: str = 'spi'
) -> xr.DataArray:
    values_array = _dataarray_values(data_aggr)
    out = np.full(values_array.shape, np.nan)
    for k in range(frequency):
        rows = np.arange(
            k + tscale - 1,
            values_array.shape[0],
            frequency
        )
        for j in range(values_array.shape[1]):
            values = values_array[rows, j]
            valid = ~np.isnan(values)
            pars = distr_pars[k, j]
            if distribution == 'zscore':
                if pars is not None:
                    z = (values - pars['mean']) / pars['sd']
                    z[~np.isfinite(z)] = 0
                    out[rows, j] = z
                continue
            if pars is None or np.sum(valid) < 4:
                continue
            probability = _cdf_ts(values[valid], pars, distribution)
            if distribution in ('gamma', 'peasron3'):
                pzero = pars['pzero']
                probability = pzero + (1.0 - pzero) * probability
            z = stats.norm.ppf(probability)
            z[np.isneginf(z)] = -5
            z[np.isposinf(z)] = 5
            target = np.full(values.shape, np.nan)
            target[valid] = z
            out[rows, j] = target

    out[out > 5] = 5
    out[out < -5]  = -5
    attrs = dict(data_aggr.attrs)
    long_name = (
        'Standardized Precipitation Index'
        if spei_type == 'spi'
        else 'Standardized Precipitation Evapotranspiration Index'
    )
    attrs.update({
        'long_name': long_name,
        'units': '',
        'distribution': distribution,
        'time_scale': tscale,
        'time_resolution': time_res
    })
    return xr.DataArray(
        out.reshape(data_aggr.shape),
        coords=data_aggr.coords,
        dims=data_aggr.dims,
        name=spei_type,
        attrs=attrs
    )

def SPEI_Computation_sp(
    data_aggr: xr.DataArray,
    distr_pars: np.ndarray,
    tscale: int = 1,
    frequency: int = 12,
    distribution: str = 'gamma',
    time_res: str = 'monthly',
    spei_type: str = 'spi'
) -> xr.DataArray:
    values_array = _dataarray_values(data_aggr)
    out = np.full(values_array.shape, np.nan)
    index_pars = _season_index(data_aggr, time_res)

    for k in range(frequency):
        if k + 1 not in index_pars:
            continue
        rows = np.where(index_pars == k + 1)[0]
        values = values_array[rows, ]
        season_pars = distr_pars[k, ]

        keys_pars = next(x for x in season_pars if x is not None)
        season_pars = {
            key: np.array([
                x[key] if x is not None else np.nan
                for x in season_pars
            ])
            for key in keys_pars
        }

        if distribution == 'zscore':
            z = (
                values - season_pars['mean'][None, :]
            ) / season_pars['sd'][None, :]
            z[~np.isfinite(z)] = 0
            out[rows, :] = z
        else:
            probability = _cdf_sp(
                values, season_pars, distribution
            )
            if distribution in ('gamma', 'peasron3'):
                pzero = season_pars['pzero'][None, :]
                probability = pzero + (1.0 - pzero) * probability

            z = stats.norm.ppf(probability)
            z[np.isneginf(z)] = -5
            z[np.isposinf(z)] = 5
            out[rows, ] = z

    attrs = dict(data_aggr.attrs)
    long_name = (
        'Standardized Precipitation Index'
        if spei_type == 'spi'
        else 'Standardized Precipitation Evapotranspiration Index'
    )
    attrs.update({
        'long_name': long_name,
        'units': '',
        'distribution': distribution,
        'time_scale': tscale,
        'time_resolution': time_res
    })
    return xr.DataArray(
        out.reshape(data_aggr.shape),
        coords=data_aggr.coords,
        dims=data_aggr.dims,
        name=spei_type,
        attrs=attrs
    )

def SPEI_distribution_params(
    distr_pars: np.ndarray,
    template: xr.DataArray,
    parameter: str,
    period: int,
    time_res: str = 'monthly',
) -> xr.DataArray:
    """
    Extract one fitted parameter for one month/dekad as a spatial grid.
    ``period`` is 1-based: 1--12 for monthly data and 1--36 for dekadal
    data. For example, ``parameter="shape", period=1`` extracts the gamma
    shape parameter for January (or the first dekad when dekadal).
    """
    # _dataarray_values(template)
    if template.ndim != 3 or template.dims[1:] != ('lat', 'lon'):
        raise ValueError(
            "template dimensions must be ('time', 'lat', 'lon')"
        )
    if time_res not in ('monthly', 'dekadal'):
        raise ValueError(
            "time_res must be 'monthly' or 'dekadal'"
        )
    expected_periods = 12 if time_res == 'monthly' else 36
    if not 1 <= period <= expected_periods:
        raise ValueError(
            f'period must be between 1 and {expected_periods}'
        )
    if distr_pars.ndim != 2 or distr_pars.shape[0] < period:
        raise ValueError(
            'distr_pars does not contain the requested period'
        )

    spatial_shape = (
        template.sizes['lat'],
        template.sizes['lon']
    )
    if distr_pars.shape[1] != int(np.prod(spatial_shape)):
        raise ValueError(
            'distr_pars spatial size does not match the template grid'
        )

    values = np.full(distr_pars.shape[1], np.nan, dtype=float)
    for index, fitted in enumerate(distr_pars[period - 1]):
        if fitted is not None and parameter in fitted:
            values[index] = fitted[parameter]

    if time_res == 'monthly':
        period_label = calendar.month_name[period]
    else:
        month = (period - 1) // 3 + 1
        dekad = (period - 1) % 3 + 1
        period_label = f'{calendar.month_name[month]} dekad {dekad}'

    return xr.DataArray(
        values.reshape(spatial_shape),
        dims=('lat', 'lon'),
        coords={
            'lat': template['lat'],
            'lon': template['lon']
        },
        name=parameter,
        attrs={
            'long_name': f'{parameter} parameter for {period_label}'
        }
    )

def _dataarray_values(
    data: xr.DataArray
) -> np.ndarray:
    if not isinstance(data, xr.DataArray):
        raise TypeError('data must be an xarray.DataArray')
    if data.ndim < 2:
        raise ValueError(
            'data must have a time dimension and at least one spatial dimension'
        )
    if data.dims[0] != 'time':
        raise ValueError("the first DataArray dimension must be 'time'")
    values = np.asarray(data.values, dtype=float)
    return values.reshape(values.shape[0], -1)

def _season_index(
    data: xr.DataArray,
    time_res: str
) -> np.ndarray:
    if time_res == 'monthly':
        season = data.time.dt.month
    elif time_res == 'dekadal':
        dekad = xr.where(
            data.time.dt.day <= 10, 1,
            xr.where(data.time.dt.day <= 20, 2, 3)
        )
        season = (data.time.dt.month - 1) * 3 + dekad
    else:
        raise ValueError(
            "time_res must be 'monthly' or 'dekadal'"
        )
    return season.astype(np.int16).values

def _cdf_ts(
    values: np.ndarray,
    pars: Mapping[str, float],
    distribution: str
) -> np.ndarray:
    if distribution == 'gamma':
        return stats.gamma.cdf(
            values,
            pars['shape'],
            loc=pars['loc'],
            scale=pars['scale'])
    if distribution == 'peasron3':
        return stats.pearson3.cdf(
            values,
            pars['skew'],
            loc=pars['loc'],
            scale=pars['scale']
        )
    if distribution == 'llogistic':
        return stats.genlogistic.cdf(
            values,
            pars['shape'],
            loc=pars['loc'],
            scale=pars['scale']
        )
    raise ValueError(
        f'unsupported distribution: {distribution!r}'
    )

def _cdf_sp(
    values: np.ndarray,
    pars: Mapping[str, np.ndarray],
    distribution: str
) -> np.ndarray:
    if distribution == 'gamma':
        return stats.gamma.cdf(
            values,
            pars['shape'][None, :],
            loc=pars['loc'][None, :],
            scale=pars['scale'][None, :]
        )
    elif distribution == 'pearson3':
        return stats.pearson3.cdf(
            values,
            pars['skew'][None, :],
            loc=pars['loc'][None, :],
            scale=pars['scale'][None, :]
        )
    elif distribution == 'llogistic':
        return stats.genlogistic.cdf(
            values,
            pars['shape'][None, :],
            loc=pars['loc'][None, :],
            scale=pars['scale'][None, :]
        )
    else:
        raise ValueError(
            f'unsupported distribution: {distribution!r}'
        )

def _fit_distribution(
    x: np.ndarray,
    distribution: str,
    min_non_na: int
) -> dict[str, float] | None:
    x = x[np.isfinite(x)]
    if x.size < min_non_na:
        return None
    if np.unique(x).size == 1:
        runif = (
            np.random.default_rng(0)
            .uniform(0.1, 0.5, x.size)
        )
        x = x + runif
    try:
        if distribution == 'gamma':
            shape, loc, scale = stats.gamma.fit(x, floc=0)
            return {
                'shape': shape,
                'loc': loc,
                'scale': scale
            }
        if distribution == 'peasron3':
            skew, loc, scale = stats.pearson3.fit(x)
            return {
                'skew': skew,
                'loc': loc,
                'scale': scale
            }
        if distribution == 'llogistic':
            # lmomco's generalized logistic is represented by
            # SciPy's genlogistic.
            shape, loc, scale = stats.genlogistic.fit(x)
            return {
                'shape': shape,
                'loc': loc,
                'scale': scale
            }
    except (ValueError, FloatingPointError):
        return None
    raise ValueError(
        f'unsupported distribution: {distribution!r}'
    )
