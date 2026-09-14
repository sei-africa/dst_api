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
        # _params_1d, data, seasons,
        _params_nd, data, seasons,
        input_core_dims=[['time'], ['time']],
        output_core_dims=[
            ['season'], ['season'], ['season']
        ],
        # _params_nd handles a complete spatial chunk at once.  xarray's
        # vectorize=True calls a Python function once per grid cell, which is
        # prohibitively expensive for large rasters.
        vectorize=False,
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

def _params_nd(
    values: np.ndarray,
    seasons: np.ndarray,
    frequency: int,
    distribution: str,
    min_non_na: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Fit every series in an xarray/Dask spatial block in one call.
    Core dimensions are last, as required by ``apply_ufunc``.  Seasonal
    selection and the common L-moment calculations are vectorized over all
    cells in the block; only the rare constant-series correction needs a
    small loop.
    """
    values = np.asarray(values, dtype=float)
    seasons = np.asarray(seasons)
    outer_shape = values.shape[:-1]
    series = values.reshape(-1, values.shape[-1])
    first = np.full((series.shape[0], frequency), np.nan)
    second = np.full_like(first, np.nan)
    pzero = np.full_like(first, np.nan)

    if distribution not in ('gamma', 'zscore'):
        raise ValueError(
            "xarray implementation supports 'gamma' and 'zscore'"
        )

    for season in range(1, frequency + 1):
        seasonal = series[:, seasons == season]
        finite = np.isfinite(seasonal)
        count = finite.sum(axis=1)
        valid = count >= min_non_na
        if not np.any(valid):
            continue

        slot = season - 1
        if distribution == 'zscore':
            # Explicit sums avoid warnings from nanmean/nanstd for empty rows.
            total = np.where(finite, seasonal, 0.0).sum(axis=1)
            mean = np.divide(total, count, out=np.full_like(total, np.nan), where=count > 0)
            centered = np.where(finite, seasonal - mean[:, None], 0.0)
            variance = np.divide(
                (centered * centered).sum(axis=1), count - 1,
                out=np.full_like(total, np.nan), where=count > 1
            )
            first[valid, slot] = mean[valid]
            second[valid, slot] = np.sqrt(variance[valid])
            continue

        pzero[valid, slot] = (
            np.count_nonzero(finite & (seasonal == 0), axis=1)[valid]
            / count[valid]
        )
        positive = np.where(finite & (seasonal > 0), seasonal, np.nan)
        npositive = np.count_nonzero(np.isfinite(positive), axis=1)
        fit = valid & (npositive >= min_non_na) & (npositive >= 2)
        if not np.any(fit):
            continue

        ordered = np.sort(positive[fit], axis=1)
        n = npositive[fit]
        # Match _gamma_lmoments' deterministic correction for a constant row.
        constant = ordered[:, 0] == ordered[np.arange(ordered.shape[0]), n - 1]
        for size in np.unique(n[constant]):
            rows = constant & (n == size)
            jitter = np.sort(
                np.random.default_rng(0).uniform(0.1, 0.5, int(size))
            )
            ordered[rows, :size] += jitter

        rank = np.arange(ordered.shape[1])[None, :]
        present = rank < n[:, None]
        clean = np.where(present, ordered, 0.0)
        l1 = clean.sum(axis=1) / n
        weights = np.divide(
            rank, n[:, None] - 1,
            out=np.zeros_like(clean), where=present
        )
        b1 = (weights * clean).sum(axis=1) / n
        l2 = 2 * b1 - l1
        good = (
            np.isfinite(l1) & np.isfinite(l2)
            & (l1 > 0) & (l2 > 0) & (l2 < l1)
        )
        tau = np.divide(l2, l1, out=np.zeros_like(l1), where=good)
        shape = np.full_like(l1, np.nan)
        low = good & (tau < 0.5)
        z = np.pi * tau[low] ** 2
        shape[low] = (1 - 0.3080 * z) / (
            z - 0.05812 * z**2 + 0.01765 * z**3
        )
        high = good & ~low
        z = 1 - tau[high]
        shape[high] = z * (0.7213 - 0.5947 * z) / (
            1 - 2.1817 * z + 1.2113 * z**2
        )
        fit_rows = np.flatnonzero(fit)
        first[fit_rows[good], slot] = shape[good]
        second[fit_rows[good], slot] = l1[good] / shape[good]

    output_shape = outer_shape + (frequency,)
    return (
        first.reshape(output_shape),
        second.reshape(output_shape),
        pzero.reshape(output_shape)
    )

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
