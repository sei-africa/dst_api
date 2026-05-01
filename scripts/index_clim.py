import numpy as np
import pandas as pd
from datetime import datetime as dt
from .dates import convert_day_dekad, seq_dekads_of_year
from .util import split_list, remove_duplicates_list
from .index_time import get_index_dates_aggregate

def get_climatology_times_index(times, out_time_res, in_time_res,
                                start_year, end_year, min_year,
                                full_year=True, clim_date=None,
                                seas_len=3, day_win=0, min_frac=1):
    # times: date for in_time_res
    year = [str(np.datetime64(y, 'Y')) for y in times]
    year = split_list(year, year)
    if len(year) < min_year:
        msg = 'Not enough number of years to compute the climatology.'
        return {'status': -1, 'message': msg}

    if out_time_res == 'daily':
        out = daily_climatology_index(times, start_year, end_year, day_win)
    elif out_time_res == 'dekadal':
        out = dekadal_climatology_index(times, out_time_res, in_time_res,
                                        start_year, end_year)
    elif out_time_res == 'monthly':
        out = monthly_climatology_index(times, out_time_res, in_time_res,
                                        start_year, end_year)
    elif out_time_res == 'seasonal':
        out = seasonal_climatology_index(times, out_time_res, in_time_res,
                                         start_year, end_year, seas_len)
    elif out_time_res == 'annual':
        out = annual_climatology_index(times, out_time_res, in_time_res,
                                       start_year, end_year)
    else:
        return {'status': -1, 'message': 'Unknown output time resolution.'}

    if out['status'] == -1: return out

    if out['aggregate']:
        out_index = []
        for x in out['index']:
            y = x.copy()
            y['index'] = []
            y['length'] = []
            for i, j in enumerate(x['index']):
                l = x['length'][i]
                mf = len(j)/l
                if mf >= min_frac:
                    y['index'] += [j]
                    y['length'] += [l]

            if len(y['index']) >= min_year:
               out_index += [y]
    else:
        out_index = []
        for x in out['index']:
            minyr = min_year
            if out_time_res == 'daily':
                minyr = (2 * day_win + 1) * min_year

            if len(x['index']) >= minyr:
                out_index += [x]

    if len(out_index) == 0:
        msg = 'Not enough number of years'
        msg = f'{msg} for {in_time_res} data to compute {out_time_res} climatology.'
        return {'status': -1, 'message': msg}

    out['index'] = out_index
    if not full_year:
        out = filter_climatology_index(out, out_time_res, clim_date)

    return out

def filter_climatology_index(index, out_time_res, clim_date):
    if out_time_res == 'daily':
        dy = f'2025-{clim_date}'
        dy = dt.strptime(dy, '%Y-%m-%d')
        ix = int(dy.strftime('%j'))
    elif out_time_res == 'dekadal':
        dk = f'2025-{clim_date}'
        dk = dt.strptime(dk, '%Y-%m-%d')
        yr = dk.strftime('%Y')
        mo = dk.strftime('%m')
        dk = f'{yr}-{mo}-{dk.day}'
        dk_yr = seq_dekads_of_year(2025)
        ix = dk_yr.index(dk) + 1
    elif out_time_res == 'monthly':
        ix = int(clim_date)
    elif out_time_res == 'seasonal':
        ix = int(clim_date)
    else:
        return index

    it = index['syear'].index(ix)
    index['dates'] = [index['dates'][it]]
    d = index['syear'][it]
    index['syear'] = [d]
    index['index'] = [x for x in index['index'] if x['syear'] == d]
    if len(index['index']) == 0:
        msg = 'Unable to compute climatology for'
        msg = f'{msg} temporalRes={out_time_res}, climDate={clim_date}'
        return {'status': -1, 'message': msg}

    return index

def monthly_climatology_index(times, out_time_res, in_time_res, start_year, end_year):
    period = f'{start_year}-{end_year}'
    date1 = np.datetime64(f'{start_year}-01-01').astype('datetime64[ns]')
    date2 = np.datetime64(f'{end_year}-12-31T23:59:59').astype('datetime64[ns]')
    index = np.logical_and(times >= date1, times <= date2)
    times_clm = times[index]
    index = np.where(index)[0]

    aggregate = out_time_res != in_time_res

    if aggregate:
        start_date = str(np.datetime64(np.min(times_clm), 'M'))
        end_date = str(np.datetime64(np.max(times_clm), 'M'))
        idx = get_index_dates_aggregate(times_clm, out_time_res,
                                in_time_res, start_date, end_date)
        if idx['status'] == -1: return idx

        times_mon = [dt.strptime(m, '%Y%m').month for m in idx['dates']]
        ix_t = [index[i].tolist() for i in idx['index']]
        ix_o = split_list(ix_t, times_mon)
        aggr_len = split_list(idx['length'], times_mon)
    else:
        times_mon = [pd.Timestamp(m).month for m in times_clm]
        ix_o = split_list(index.tolist(), times_mon)
        aggr_len = np.repeat(None, len(ix_o)).tolist()

    ix_i = sorted(list(set(times_mon)))
    out_index = []
    for i, j in enumerate(ix_i):
        tmp = {'syear': j,
               'index': ix_o[i],
               'length': aggr_len[i]}
        out_index += [tmp]

    month = np.arange(1, 13).tolist()
    out_dates = []
    for i in month:
        out_dates += [f'{period}_{i:02}']

    return {'status': 0, 'time_res': out_time_res, 'aggregate': aggregate,
            'syear': month, 'dates': out_dates, 'index': out_index}

def dekadal_climatology_index(times, out_time_res, in_time_res, start_year, end_year):
    period = f'{start_year}-{end_year}'
    date1 = np.datetime64(f'{start_year}-01-01').astype('datetime64[ns]')
    date2 = np.datetime64(f'{end_year}-12-31T23:59:59').astype('datetime64[ns]')
    index = np.logical_and(times >= date1, times <= date2)
    times_clm = times[index]
    index = np.where(index)[0]

    dkoy = np.arange(1, 37)
    dekad = np.tile(np.array([6, 16, 26]), 12).tolist()
    month = np.repeat(np.arange(1, 13), 3).tolist()
    x_vtimes = [f'{dekad[i]}_{month[i]}' for i in range(len(dekad))]

    aggregate = out_time_res != in_time_res

    if aggregate:
        s = str(np.datetime64(np.min(times_clm), 'D'))
        start_date = convert_day_dekad(s)
        e = str(np.datetime64(np.max(times_clm), 'D'))
        end_date = convert_day_dekad(e)
        idx = get_index_dates_aggregate(times_clm, out_time_res,
                                in_time_res, start_date, end_date)
        if idx['status'] == -1: return idx

        times_idx = [dt.strptime(t, '%Y%m%d') for t in idx['dates']]
        times_dek = [(d.day - 1) * 10 + 6 for d in times_idx]
        times_mon = [m.month for m in times_idx]
        x_times = [f'{times_dek[i]}_{times_mon[i]}' for i in range(len(times_dek))]

        ix_m = [x_vtimes.index(x) if x in x_vtimes else None for x in x_times]
        times_doy = dkoy[ix_m].tolist()
        ix_t = [index[i].tolist() for i in idx['index']]
        ix_o = split_list(ix_t, times_doy)
        aggr_len = split_list(idx['length'], times_doy)
    else:
        # times_dek = [t.astype('datetime64[D]').item().day for t in times_clm]
        # times_mon = [(t.astype('datetime64[M]').astype(int) % 12 + 1).item() for t in times_clm]
        times_dek = [pd.Timestamp(t).day for t in times_clm]
        times_mon = [pd.Timestamp(t).month for t in times_clm]
        x_times = [f'{times_dek[i]}_{times_mon[i]}' for i in range(len(times_dek))]

        ix_m = [x_vtimes.index(x) if x in x_vtimes else None for x in x_times]
        times_doy = dkoy[ix_m].tolist()
        ix_o = split_list(index.tolist(), times_doy)
        aggr_len = np.repeat(None, len(ix_o)).tolist()

    ix_i = sorted(list(set(times_doy)))
    out_index = []
    for i, j in enumerate(ix_i):
        tmp = {'syear': j,
               'index': ix_o[i],
               'length': aggr_len[i]}
        out_index += [tmp]

    dekad = [int((dk - 6)/10 + 1) for dk in dekad]
    out_dates = []
    for i in range(len(dekad)):
        out_dates += [f'{period}_{month[i]:02}-{dekad[i]}']

    return {'status': 0, 'time_res': out_time_res, 'aggregate': aggregate,
            'syear': dkoy.tolist(), 'dates': out_dates, 'index': out_index}

def daily_climatology_index(times, start_year, end_year, day_win):
    period = f'{start_year}-{end_year}'
    date1 = np.datetime64(f'{start_year}-01-01').astype('datetime64[ns]')
    date2 = np.datetime64(f'{end_year}-12-31T23:59:59').astype('datetime64[ns]')
    index = np.logical_and(times >= date1, times <= date2)
    times_clm = times[index]
    index = np.where(index)[0]

    end_mon = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    days_mon = [np.arange(1, j + 1).tolist() for j in end_mon]
    days = [d for m in days_mon for d in m]
    month = np.repeat(np.arange(1, 13), end_mon).tolist()
    x_vtimes = [f'{days[i]}_{month[i]}' for i in range(len(days))]

    # times_day = [t.astype('datetime64[D]').item().day for t in times_clm]
    # times_mon = [(t.astype('datetime64[M]').astype(int) % 12 + 1).item() for t in times_clm]
    times_day = [pd.Timestamp(t).day for t in times_clm]
    times_mon = [pd.Timestamp(t).month for t in times_clm]
    x_times = [f'{times_day[i]}_{times_mon[i]}' for i in range(len(times_mon))]

    ix_m = [x_vtimes.index(x) if x in x_vtimes else None for x in x_times]

    doy = np.arange(1, 366).tolist()
    times_doy = [59 if i is None else doy[i] for i in ix_m]
    days_year = remove_duplicates_list(times_doy)
    times_doy = np.array(times_doy)
    nl = len(times_doy)

    ## sliding windows day_win
    ix_win = []
    for d in days_year:
        ix = np.where(times_doy == d)[0]
        ix = [i + np.arange(-day_win, day_win + 1) for i in ix]
        # ix = [i.item() for s in ix for i in s]
        ix = np.array(ix).flatten()

        ex_low = []
        if ix[0] < 0:
            t_diff = times_clm[0] - times[0]
            nbdays = t_diff.astype('timedelta64[D]').item().days
            # ex_low = [i for i in ix if i >= -nbdays and i < 0]
            ex_low = ix[np.logical_and(ix >= -nbdays, ix < 0)]

        ex_up = []
        if ix[-1] >= nl:
            t_diff = times[-1] - times_clm[-1]
            nbdays = t_diff.astype('timedelta64[D]').item().days
            # ex_up = [i - nl + 1 for i in ix if i < nl + nbdays and i >= nl]
            ex_up = ix[np.logical_and(ix < nl + nbdays, ix >= nl)] - nl + 1

        # ix = [i for i in ix if i >= 0 and i < nl]
        ix = ix[np.logical_and(ix >= 0, ix < nl)]
        io = index[ix]

        if len(ex_low) > 0:
            t0 = times[io[0]] - np.timedelta64(day_win, 'D')
            i0 = io[0] + ex_low
            i0 = [i for i in i0 if times[i] >= t0]
            if len(i0) > 0: io = np.concatenate((i0, io))

        if len(ex_up) > 0:
            t1 = times[io[-1]] + np.timedelta64(day_win, 'D')
            i1 = io[-1] + ex_up
            i1 = [i for i in i1 if times[i] <= t1]
            if len(i1) > 0: io = np.concatenate((io, i1))

        ix_win += [{'syear': d, 'index': io.tolist(), 'length': None}]

    out_dates = []
    for i in range(len(days)):
        out_dates += [f'{period}_{month[i]:02}-{days[i]:02}']

    return {'status': 0, 'time_res': 'daily', 'aggregate': False,
            'syear': doy, 'dates': out_dates, 'index': ix_win}

def annual_climatology_index(times, out_time_res, in_time_res, start_year, end_year):
    period = f'{start_year}-{end_year}'
    date1 = np.datetime64(f'{start_year}-01-01').astype('datetime64[ns]')
    date2 = np.datetime64(f'{end_year}-12-31T23:59:59').astype('datetime64[ns]')
    index = np.logical_and(times >= date1, times <= date2)
    times_clm = times[index]
    index = np.where(index)[0]

    aggregate = out_time_res != in_time_res

    if aggregate:
        idx = get_index_dates_aggregate(times_clm, out_time_res,
                                in_time_res, start_year, end_year)
        if idx['status'] == -1: return idx

        ix_o = [index[i].tolist() for i in idx['index']]
        aggr_len = idx['length']
    else:
        ix_o = index.tolist()
        aggr_len = None

    out_index = [{'syear': 1, 'index': ix_o, 'length': aggr_len}]

    return {'status': 0, 'time_res': out_time_res, 'aggregate': aggregate,
            'syear': [1], 'dates': [period], 'index': out_index}

def seasonal_climatology_index(times, out_time_res, in_time_res, start_year, end_year, seas_len):
    period = f'{start_year}-{end_year}'
    date1 = np.datetime64(f'{start_year}-01-01').astype('datetime64[ns]')
    date2 = np.datetime64(f'{end_year + 1}-12-31T23:59:59').astype('datetime64[ns]')
    index = np.logical_and(times >= date1, times <= date2)
    times_clm = times[index]
    index = np.where(index)[0]

    aggregate = out_time_res != in_time_res

    if aggregate:
        out_index = []
        for seas_mon in range(1, 13):
            idx = get_index_dates_aggregate(times_clm, out_time_res, in_time_res,
                                     start_year, end_year, seas_mon, seas_len)
            if idx['status'] == -1: continue

            ix_o = [index[i].tolist() for i in idx['index']]
            aggr_len = idx['length']
            out_index += [{'syear': seas_mon, 'index': ix_o, 'length': aggr_len}]
    else:
        return {'status': -1, 'message': 'No aggregation possible.'}

    season = np.arange(1, 13).tolist()
    out_dates = []
    for seas_mon in range(1, 13):
        mn = (seas_mon + seas_len - 1) % 12
        mn = 12 if mn == 0 else mn
        out_dates += [f'{period}_{seas_mon:02}-{mn:02}']

    return {'status': 0, 'time_res': out_time_res, 'aggregate': aggregate,
            'syear': season, 'dates': out_dates, 'index': out_index}

