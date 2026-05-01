from datetime import datetime as dt
from datetime import timedelta
import numpy as np
import re
from .util import split_list

def nbdays_of_month(date):
    # date: string 'yyyy-mm-dd' or 'yyyy-mm'
    frmt = '%Y-%m' if date.count('-') == 1 else '%Y-%m-%d'
    date = dt.strptime(date, frmt)
    next_mon = date.replace(month = date.month % 12 + 1, day = 1)
    end_mon = next_mon - timedelta(days = 1)
    return end_mon.day

def nbdays_of_year(date):
    # date: string 'yyyy-mm-dd' or 'yyyy-mm' or 'yyyy'
    nb = date.count('-')
    if nb == 0:
        frmt = '%Y'
    elif nb == 1:
        frmt = '%Y-%m'
    else:
        frmt = '%Y-%m-%d'

    date = dt.strptime(date, frmt)
    start = date.replace(month = 1, day = 1)
    end = date.replace(year = date.year + 1, month = 1, day = 1)
    return (end - start).days

def add_months(month, length):
    # month: string 'yyyy-mm'
    # length: integer, month to add 
    yrmo = month.split('-')
    yr = int(yrmo[0])
    mo = int(yrmo[1])
    year = yr + (mo + length - 1) // 12
    mon = (mo + length - 1) % 12 + 1
    return f'{year}-{mon:02}'

def convert_day_dekad(date):
    # date: string 'yyyy-mm-dd'
    # out: string 'yyyy-mm-d'
    ymd = date.split('-')
    ym = '-'.join(ymd[:2])
    d = int(ymd[2])
    d = 1 if d < 11 else 3 if d > 20 else 2
    return f'{ym}-{d}'

def seq_days_of_dekad(dekad):
    # dekad: 'yyyy-mm-d' (d: 1, 2, 3)
    dk = dekad.split('-')
    if dk[2] == '1':
        days = [f'{d:02}' for d in range(1, 11)]
    elif dk[2] == '2':
        days = [d for d in range(11, 21)]
    else:
        nm = nbdays_of_month('-'.join(dk[:2]))
        days = [d for d in range(21, nm + 1)]

    return [f"{'-'.join(dk[:2])}-{d}" for d in days]

def seq_days_of_month(month):
    # month: string 'yyyy-mm'
    start = dt.strptime(month, '%Y-%m')
    nbday = nbdays_of_month(month)
    end = start.replace(day = nbday)
    days_range = range((end - start).days + 1)
    seq_days = [start + timedelta(days = d) for d in days_range]
    return [t.strftime('%Y-%m-%d') for t in seq_days]

def seq_dekads_of_month(month):
    # month: string 'yyyy-mm'
    return [f'{month}-{d}' for d in range(1, 4)]

def seq_days_of_year(year):
    # year: string 'yyyy'
    start = dt.strptime(str(year), '%Y')
    end = dt.strptime(str(int(year) + 1), '%Y')
    days_range = range((end - start).days)
    seq_days = [start + timedelta(days = d) for d in days_range]
    return [t.strftime('%Y-%m-%d') for t in seq_days]

def seq_dekads_of_year(year):
    # year: string 'yyyy'
    dekads = []
    for m in range(1, 13):
        for d in range(1, 4):
            dekads += [f'{year}-{m:02}-{d}']

    return dekads

def seq_months_of_year(year):
    # year: string 'yyyy'
    return [f'{year}-{m:02}' for m in range(1, 13)]

def seq_days_betwen_dates(start, end):
    # start, end: string 'yyyy-mm-dd' or 'yyyy-mm'
    sfrmt = '%Y-%m' if start.count('-') == 1 else '%Y-%m-%d'
    if end.count('-') == 1:
        nbd = nbdays_of_month(end)
        end = f'{end}-{nbd}'

    start = dt.strptime(start, sfrmt)
    end = dt.strptime(end, '%Y-%m-%d')
    days_range = range((end - start).days + 1)
    seq_days = [start + timedelta(days = d) for d in days_range]
    return [t.strftime('%Y-%m-%d') for t in seq_days]

def seq_days_betwen_dekads(start, end):
    # start, end: string 'yyyy-mm-d' (d: 1, 2, 3)
    l1 = start.split('-')
    s = l1[2]
    l1[2] = '01' if s == '1' else '11' if s == '2' else '21'

    l2 = end.split('-')
    nm = str(nbdays_of_month('-'.join(l2[:2])))
    e = l2[2]
    l2[2] = '10' if e == '1' else '20' if e == '2' else nm

    start = '-'.join(l1)
    end = '-'.join(l2)
    return seq_days_betwen_dates(start, end)

def seq_months_betwen_months(start, end):
    # start, end: string 'yyyy-mm'
    start = dt.strptime(start, '%Y-%m')
    end = dt.strptime(end, '%Y-%m')

    months = []
    for yr in range(start.year, end.year + 1, 1):
        mon1 = start.month if yr == start.year else 1
        mon2 = end.month if yr == end.year else 12
        for mo in range(mon1, mon2 + 1, 1):
            months += [f'{yr}-{mo:02}']

    return months

def seq_dekads_betwen_months(start, end):
    # start, end: string 'yyyy-mm'
    months = seq_months_betwen_months(start, end)

    dekads = []
    for mo in months:
        for d in range(1, 4):
            dekads += [f'{mo}-{d}']

    return dekads

def seq_dekads_betwen_dekads(start, end):
    # start, end: string 'yyyy-mm-d' (d: 1, 2, 3)
    l1 = start.split('-')
    l2 = end.split('-')
    mon1 = '-'.join(l1[:2])
    mon2 = '-'.join(l2[:2])
    dek1 = int(l1[2])
    dek2 = int(l2[2])
    months = seq_months_betwen_months(mon1, mon2)

    dekads = []
    for mo in months:
        dk1 = dek1 if mo == mon1 else 1
        dk2 = dek2 if mo == mon2 else 3
        for d in range(dk1, dk2 + 1, 1):
            dekads += [f'{mo}-{d}']

    return dekads

def get_ncinfo_date(time_res, date):
    origin = '1970-01-01'
    dorigin = dt.strptime(origin, '%Y-%m-%d')

    if time_res == 'daily':
        date = dt.strptime(date, '%Y-%m-%d')
        diff = date - dorigin
        ret = {'values': diff.days, 'units': f'days since {origin}'}
    elif time_res == 'dekadal':
        dk = int(date.split('-')[2])
        date = dt.strptime(date, '%Y-%m-%d')
        # date = date.replace(day = (dk - 1) * 10 + 1)
        date = date.replace(day = (dk - 1) * 10 + 6)
        diff = date - dorigin
        ret = {'values': diff.days, 'units': f'days since {origin}'}
    elif time_res == 'monthly':
        date = dt.strptime(date, '%Y-%m')
        month = date.month - dorigin.month
        year = date.year - dorigin.year
        diff = year * 12 + month
        ret = {'values': diff, 'units': f'months since {origin}'}
    elif time_res == 'seasonal':
        seas = date.split('_')
        m1 = dt.strptime(seas[0], '%Y-%m')
        m2 = dt.strptime(seas[1], '%Y-%m')
        month = m2.month - m1.month
        year = m2.year - m1.year
        mf = (year * 12 + month + 1) // 2 + 1
        yr = m1.year + (m1.month + mf - 1) // 12
        mo = (m1.month + mf - 1) % 12
        month = mo - dorigin.month
        year = yr - dorigin.year
        diff = year * 12 + month
        ret = {'values': diff, 'units': f'months since {origin}'}
    elif time_res == 'annual':
        date = dt.strptime(date, '%Y')
        month = date.month - dorigin.month
        year = date.year - dorigin.year
        diff = year * 12 + month
        ret = {'values': diff, 'units': f'months since {origin}'}
    else:
        ret = None

    return ret

def format_output_date(params):
    if params['temporalRes'] == 'seasonal':
        period = f"{params['startDate']}-{params['endDate']}"
        smon = int(params['seasStart'])
        slen = int(params['seasLength'])
        mn = (smon + slen - 1) % 12
        emon = 12 if mn == 0 else mn
        date = f'{period}_{smon:02}-{emon:02}'
    else:
        date = f"{params['startDate']}_{params['endDate']}"

    return date

def aggregate_seq_dates(out_res, in_res, date):
    # date: for out_res and out_res format
    seq_date = []
    if out_res == 'dekadal':
        if in_res == 'daily':
            seq_date = seq_days_of_dekad(date) 
        else:
            return None

    elif out_res == 'monthly':
        if in_res == 'daily':
            seq_date = seq_days_of_month(date)
        elif in_res == 'dekadal':
            seq_date = seq_dekads_of_month(date)
        else:
            return None

    elif out_res == 'annual':
        if in_res == 'daily':
            seq_date = seq_days_of_year(date)
        elif in_res == 'dekadal':
            seq_date = seq_dekads_of_year(date)
        elif in_res == 'monthly':
            seq_date = seq_months_of_year(date)
        else:
            return None

    elif out_res == 'seasonal':
        seas = date.split('_')
        if in_res == 'daily':
            seq_date = seq_days_betwen_dates(seas[0], seas[1])
        elif in_res == 'dekadal':
            seq_date = seq_dekads_betwen_months(seas[0], seas[1])
        elif in_res == 'monthly':
            seq_date = seq_months_betwen_months(seas[0], seas[1])
        else:
            return None

    return seq_date

def aggregate_range_dates(out_res, in_res,
                          start_date, end_date,
                          seas_mon=1, seas_len=3):
    # start_date, end_date: for out_res and out_res format
    seq_date = []
    if out_res == 'dekadal':
        if in_res == 'daily':
            seq_date = seq_days_betwen_dekads(start_date, end_date)
        else:
            return None

        fdek = [convert_day_dekad(t) for t in seq_date]
        seq_date = split_list(seq_date, fdek)

    elif out_res == 'monthly':
        if in_res == 'daily':
            seq_date = seq_days_betwen_dates(start_date, end_date)
        elif in_res == 'dekadal':
            seq_date = seq_dekads_betwen_months(start_date, end_date)
        else:
            return None

        fmon = ['-'.join(d.split('-')[:2]) for d in seq_date]
        seq_date = split_list(seq_date, fmon)

    elif out_res == 'annual':
        start = f'{start_date}-01'
        end = f'{end_date}-12'
        if in_res == 'daily':
            seq_date = seq_days_betwen_dates(start, end)
        elif in_res == 'dekadal':
            seq_date = seq_dekads_betwen_months(start, end)
        elif in_res == 'monthly':
            seq_date = seq_months_betwen_months(start, end)
        else:
            return None

        fyear = [d.split('-')[0] for d in seq_date]
        seq_date = split_list(seq_date, fyear)

    elif out_res == 'seasonal':
        seas_len = int(seas_len)
        seas_mon = int(seas_mon)
        start = int(start_date)
        end = int(end_date)
        starts = [f'{y}-{seas_mon}' for y in range(start, end + 1)]
        ends = [add_months(smon, seas_len - 1) for smon in starts]

        if in_res == 'daily':
            seq_date = [seq_days_betwen_dates(starts[i], ends[i]) for i in range(len(starts))]
        elif in_res == 'dekadal':
            seq_date = [seq_dekads_betwen_months(starts[i], ends[i]) for i in range(len(starts))]
        elif in_res == 'monthly':
            seq_date = [seq_months_betwen_months(starts[i], ends[i]) for i in range(len(starts))] 
        else:
            return None
    else:
        return None

    return seq_date

def convert_strings_npdatetime64(times, time_res, sep=''):
    if type(times) is not list:
        times = [times]

    dates = []
    for f in times:
        if time_res in ['daily', 'dekadal']:
            frmt = '%Y-%m-%d'
        elif time_res == 'monthly':
            frmt = '%Y-%m'
        elif time_res == 'annual':
            frmt = '%Y'
        else:
            continue

        frmt = frmt.replace('-', sep)
        d = dt.strptime(str(f), frmt)

        if time_res == 'dekadal':
            dk = int(d.strftime('%d'))
            # d = d.replace(day = (dk - 1) * 10 + 1)
            d = d.replace(day = (dk - 1) * 10 + 6)

        if time_res == 'monthly':
            d = d.replace(day = 16)

        # dates += [np.datetime64(d)]
        dates += [d]

    dates = np.array(dates, dtype='datetime64')
    return dates

def extract_ncfiles_datetime(nc_files, nc_format, time_res):
    if type(nc_files) is not list:
        nc_files = [nc_files]

    dates = []
    for f in nc_files:
        d = dt.strptime(f, nc_format)

        if time_res == 'dekadal':
            dk = int(d.strftime('%d'))
            # d = d.replace(day = (dk - 1) * 10 + 1)
            d = d.replace(day = (dk - 1) * 10 + 6)

        if time_res == 'monthly':
            d = d.replace(day = 16)

        # dates += [np.datetime64(d)]
        dates += [d]

    dates = np.array(dates, dtype='datetime64')
    return dates

def extract_filename_dates(nc_files, nc_format):
    if type(nc_files) is not list:
        nc_files = [nc_files]

    expr = re.finditer(r'%', nc_format)
    matches = list(expr)
    nl = len(matches)
    if nl == 0:
        return None

    frmt = nc_format
    for i in range(nl):
        ss = matches[i].start()
        se = matches[i].end() + 1
        pat = nc_format[ss:se]
        frmt = frmt.replace(pat, '%')

    frmt = frmt.split('%')
    frmt = [x for x in frmt if x != '']

    dates = []
    for d in nc_files:
        for f in frmt:
            d = d.replace(f, '')
        dates += [d]

    return dates
