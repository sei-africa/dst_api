from __future__ import annotations
from typing import Any, Mapping
import numpy as np
import xarray as xr
from scipy import stats

def spei_spatial_computation(
    data: xr.DataArray,
    distr_pars: xr.Dataset,
    tscale: int = 1,
    frequency: int | None = None,
    distribution: str = 'gamma',
    time_res: str = 'monthly',
    spei_type: str = 'spi'
) -> xr.DataArray:
    seasons = _season_index(data, time_res)
    first_name, second_name = (
        ('shape', 'scale')
        if distribution == 'gamma'
        else ('mean', 'sd')
    )
    spei = xr.apply_ufunc(
        _spei_1d,
        data,
        seasons,
        distr_pars[first_name],
        distr_pars[second_name],
        distr_pars['pzero'],
        input_core_dims=[
            ['time'], ['time'],
            ['season'], ['season'], ['season']
        ],
        output_core_dims=[['time']],
        vectorize=True,
        dask='parallelized',
        output_dtypes=[float],
        kwargs={'distribution': distribution},
        dask_gufunc_kwargs={'allow_rechunk': True},
    )
    spei = spei.transpose(*data.dims)
    spei = spei.assign_coords(data.coords).rename(spei_type)
    long_name = (
        'Standardized Precipitation Index'
        if spei_type == 'spi'
        else 'Standardized Precipitation Evapotranspiration Index'
    )
    spei.attrs.update(
        units='',
        long_name=long_name,
        distribution=distribution,
        time_scale=tscale,
        time_resolution=time_res
    )
    return spei

def spei_aggregate_data(
    data: xr.DataArray,
    tscale: int = 1
) -> xr.DataArray:
    if tscale < 1:
        raise ValueError('tscale must be at least 1')
    if tscale == 1:
        return data.copy(deep=False)
    tmp = (
        data
        .rolling(time=tscale, min_periods=1)
        .sum(skipna=True)
    )
    leading = xr.DataArray(
        np.arange(data.sizes['time']) < tscale - 1,
        dims='time',
        coords={'time': data.time}
    )
    return tmp.where(~leading, drop=True)

def spei_compute_params(
    data: xr.DataArray,
    tscale: int = 1,
    frequency: int | None = None,
    distribution: str = 'gamma',
    time_res: str = 'monthly',
    min_non_na: int = 5
) -> xr.Dataset:
    expected = 36 if time_res == 'dekadal' else 12
    frequency = (
        expected
        if frequency is None
        else int(frequency)
    )
    seasons = _season_index(data, time_res)
    shape, scale, pzero = xr.apply_ufunc(
        _params_1d, data, seasons,
        input_core_dims=[['time'], ['time']],
        output_core_dims=[
            ['season'], ['season'], ['season']
        ],
        vectorize=True,
        dask='parallelized',
        output_dtypes=[float, float, float],
        kwargs={
            'frequency': frequency,
            'distribution': distribution,
            'min_non_na': min_non_na
        },
        dask_gufunc_kwargs={
            'output_sizes': {'season': frequency},
            'allow_rechunk': True
        }
    )
    names = (
        ('shape', 'scale')
        if distribution == 'gamma'
        else ('mean', 'sd')
    )
    result = xr.Dataset({
        names[0]: shape,
        names[1]: scale,
        'pzero': pzero
    })
    result = result.assign_coords(
        season=np.arange(1, frequency + 1)
    )
    result.attrs.update(
        distribution=distribution,
        time_res=time_res,
        tscale=tscale
    )
    return result

def _season_index(
    data: xr.DataArray,
    time_res: str
) -> xr.DataArray:
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
    return season.astype(np.int16).rename('season_index')

def _spei_1d(
    values: np.ndarray,
    seasons: np.ndarray,
    first: np.ndarray,
    second: np.ndarray,
    pzero: np.ndarray,
    distribution: str
) -> np.ndarray:
    spei = np.full(values.shape, np.nan, dtype=float)
    for i, value in enumerate(values):
        if not np.isfinite(value):
            continue
        k = int(seasons[i]) - 1
        if k < 0 or k >= first.size or not np.isfinite(first[k]):
            continue
        if distribution == 'gamma':
            probability = stats.gamma.cdf(value, first[k], scale=second[k])
            probability = pzero[k] + (1 - pzero[k]) * probability
            spei[i] = stats.norm.ppf(probability)
        elif distribution == 'zscore':
            spei[i] = (value - first[k]) / second[k]
            if not np.isfinite(spei[i]):
                spei[i] = 0
        else:
            raise ValueError(
                "xarray implementation supports 'gamma' and 'zscore'"
            )
    spei[np.isneginf(spei)] = -5
    spei[np.isposinf(spei)] = 5
    spei[spei > 5] = 5
    spei[spei < -5] = -5
    return spei

def _params_1d(
    values: np.ndarray,
    seasons: np.ndarray,
    frequency: int,
    distribution: str,
    min_non_na: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    first = np.full(frequency, np.nan)
    second = np.full(frequency, np.nan)
    pzero = np.full(frequency, np.nan)
    for season in range(1, frequency + 1):
        x = values[seasons == season]
        x = x[np.isfinite(x)]
        if x.size < min_non_na:
            continue
        if distribution == 'gamma':
            pzero[season - 1] = np.mean(x == 0)
            fitted = _gamma_lmoments(x, min_non_na)
            if fitted is not None:
                first[season - 1], second[season - 1] = fitted
        elif distribution == 'zscore':
            first[season - 1] = np.mean(x)
            second[season - 1] = np.std(x, ddof=1)
        else:
            raise ValueError(
                "xarray implementation supports 'gamma' and 'zscore'"
            )
    return first, second, pzero

def _gamma_lmoments(
    values: np.ndarray,
    min_non_na: int
) -> tuple[float, float] | None:
    """
    Hosking gamma L-moment fit,
    equivalent to R function lmomco::pargam.
    """
    x = np.sort(
        values[np.isfinite(values) & (values > 0)]
    )
    n = x.size
    if n < min_non_na or n < 2:
        return None
    if np.unique(x).size == 1:
        x = np.sort(
            x + np.random.default_rng(0).uniform(0.1, 0.5, n)
        )
    l1 = float(np.mean(x))
    b1 = float(np.sum((np.arange(n) / (n - 1)) * x) / n)
    l2 = 2 * b1 - l1
    if not np.isfinite(l1) or not np.isfinite(l2) or l1 <= 0 or l2 <= 0 or l2 >= l1:
        return None
    tau = l2 / l1
    if tau < 0.5:
        z = np.pi * tau * tau
        shape = (1 - 0.3080 * z) / (z - 0.05812 * z**2 + 0.01765 * z**3)
    else:
        z = 1 - tau
        shape = z * (0.7213 - 0.5947 * z) / (1 - 2.1817 * z + 1.2113 * z**2)
    return float(shape), float(l1 / shape)
