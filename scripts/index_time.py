import numpy as np
from .dates import convert_strings_npdatetime64, aggregate_range_dates

def get_index_dates_series(times, time_res, start_date, end_date):
    date_range = [start_date, end_date]
    date_range = convert_strings_npdatetime64(date_range, time_res, '-')
    index = np.logical_and(times >= date_range[0], times <= date_range[1])
    index = np.where(index)[0]
    if len(index) == 0:
        period = f'{start_date}_{end_date}'
        msg = f'No data found for the period {period}'
        return {'status': -1, 'message': msg}

    dates = times[index]
    if time_res == 'daily':
        dates = [np.datetime_as_string(d, unit='D').item() for d in dates]
        dates = [d.replace('-', '') for d in dates]
    elif time_res == 'dekadal':
        dates = [np.datetime_as_string(d, unit='D').item() for d in dates]
        tmp = []
        for d in dates:
            dk = int(d.split('-')[2])
            dk = 1 if dk <= 10 else 3 if dk > 20 else 2
            ym = ''.join(d.split('-')[:2])
            tmp += [f'{ym}{dk}']
        dates = tmp
    elif time_res == 'monthly':
        # dates = [str(np.datetime64(d, 'M')) for d in dates]
        dates = [np.datetime_as_string(d, unit='M').item() for d in dates]
        dates = [d.replace('-', '') for d in dates]
    elif time_res == 'annual':
        # dates = [str(np.datetime64(d, 'Y').item().year) for d in dates]
        dates = [np.datetime_as_string(d, unit='Y').item() for d in dates]
    else:
        return {'status': -1, 'message': 'Unknown temporal resolution'}

    return {'status': 0, 'index': index, 'dates': dates, 'length': None}

def get_index_dates_aggregate(times, out_time_res, in_time_res,
                              start_date, end_date, seas_mon=1, seas_len=3):
    if not out_time_res in ['dekadal', 'monthly', 'seasonal', 'annual']:
        return {'status': -1, 'message': 'Unknown output temporal resolution'}

    seq_dates = aggregate_range_dates(out_time_res, in_time_res,
                        start_date, end_date, seas_mon, seas_len)
    if seq_dates is None:
        return {'status': -1, 'message': 'Unknown input temporal resolution'}

    if out_time_res == 'dekadal':
        dates = []
        for d in seq_dates:
            ymd = d[0].split('-')
            ym = ''.join(ymd[:2])
            dk = int(ymd[2])
            dk = 1 if dk < 11 else 3 if dk > 20 else 2
            dates += [f'{ym}{dk}']
    elif out_time_res == 'monthly':
        dates = [''.join(s[0].split('-')[:2]) for s in seq_dates]
    elif out_time_res == 'seasonal':
        dates = []
        for s in seq_dates:
            seas1 = '-'.join(s[0].split('-')[:2])
            seas2 = '-'.join(s[-1].split('-')[:2])
            dates += [f'{seas1}_{seas2}']
    else:
        # 'annual'
        dates = [''.join(s[0].split('-')[0]) for s in seq_dates]

    if in_time_res == 'dekadal':
        tmp = []
        for s in seq_dates:
            dek = []
            for d in s:
                ymd = d.split('-')
                ym = '-'.join(ymd[:2])
                dk = (int(ymd[2]) - 1) * 10 + 6
                dek += [f'{ym}-{dk:02}']
            tmp += [dek]
        seq_dates = tmp

    nl = [len(x) for x in seq_dates]
    seq_times = [[np.datetime64(d) for d in s] for s in seq_dates]
    # seq_times = [np.array(s, dtype='datetime64[ns]') for s in seq_dates]
    index = [np.where(np.isin(times, s))[0] for s in seq_times]

    ff = [len(i) for i in index]
    if all(f == 0 for f in ff):
        return {'status': -1, 'message': 'No data found to aggregate'}

    index = [s for i, s in enumerate(index) if ff[i] > 0]
    dates = [s for i, s in enumerate(dates) if ff[i] > 0]
    nl = [s for i, s in enumerate(nl) if ff[i] > 0]

    return {'status': 0, 'index': index, 'dates': dates, 'length': nl}

def get_index_dates_dataset(xr_coords, params, input_res, compute):
    if compute:
        if params['temporalRes'] == 'seasonal':
            seasS = params['seasStart']
            seasL = params['seasLength']
        else:
            seasS = None
            seasL = None

        out = get_index_dates_aggregate(xr_coords['time'],
                                    params['temporalRes'],
                                    input_res,
                                    params['startDate'],
                                    params['endDate'],
                                    seasS, seasL)
    else:
        out = get_index_dates_series(xr_coords['time'],
                               params['temporalRes'],
                               params['startDate'],
                               params['endDate'])

    out['input_res'] = input_res
    return out
