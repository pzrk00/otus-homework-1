#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import gzip
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from string import Template
import yaml

config = {
    'REPORT_SIZE': 1000,
    'REPORT_DIR': './reports',
    'LOG_DIR': './log',
    'LOG_FILE': None,
    'ERROR_PERCENT': 32.5,
}


def get_config_filename():
    """
    check cli args for config filename
    :return: filename
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str)
    args = parser.parse_args()
    return args.config


def merge_config(internal_config, config_from_file):
    """
    merge internal & external config files
    :param internal_config:
    :param config_from_file:
    :return: merged config dict
    """
    result = dict()
    for k in internal_config.keys():
        if k in config_from_file.keys():
            if k == 'REPORT_SIZE':
                result[k] = int(config_from_file[k])
            elif k == 'ERROR_PERCENT':
                result[k] = float(config_from_file[k])
            else:
                result[k] = config_from_file[k]
        else:
            result[k] = internal_config[k]
    return result


def load_config(internal_config, cfg_filename):
    """
    load external config file(yaml) if exist
    :param internal_config:
    :param cfg_filename: external config file name
    :return: config dict
    """
    if cfg_filename:
        try:
            with open(cfg_filename, 'r', encoding='utf-8') as config_file:
                cfg = yaml.safe_load(config_file)
        except Exception as e:
            logging.exception(e)
            return None
        return merge_config(internal_config, cfg)
    else:
        return internal_config


def median(lst):
    """
    get median value
    :param lst: list of values
    :return: value of median
    """
    n = len(lst)
    if n < 1:
        return None
    elif n % 2 == 1:
        return sorted(lst)[n//2]
    else:
        return sum(sorted(lst)[n//2-1:n//2+1])/2.0


def check_float(v):
    """
    check parameter - is float
    :param v:
    :return: float if correct
    """
    try:
        return float(v)
    except ValueError:
        return None


def parse_line(line):
    """
    parse line

    simple split line by ' ', check [7] and [-1] values
    :param line:
    :return: url(str), duration(float)
    """
    value_list = line.strip().split(' ')
    if len(value_list) < 9:
        return None, None
    url = value_list[7]
    duration = check_float(value_list[-1])
    if duration is not None:
        return url, duration
    else:
        return None, None


def parse_log(fn, gz=False):
    """
    parse log file
    :param fn:
    :param gz: is .gz
    :return: sorted dict of grouped values, sum of all duration, lines count, error count
    """
    logging.info(f'parsing log file: "{fn}"')
    data = dict()
    all_time = 0
    count = 0
    error_count = 0
    try:
        opener = gzip.open if gz else open
        with opener(fn, 'rt', encoding='utf-8') as log_file:
            for line in log_file:
                count += 1
                url, req_time = parse_line(line)
                if url:
                    all_time += req_time
                    if url in data.keys():
                        data[url].append(req_time)
                    else:
                        data[url] = [req_time]
                else:
                    error_count += 1

                if count % 100000 == 0:
                    logging.info(f'{count:,} lines parsed, errors: {error_count}')
    except Exception as e:
        logging.error(e)
        return None, None, None, None
    return dict(sorted(data.items(), key=lambda k: sum(k[1]), reverse=True)), all_time, count, error_count


def calc_stat(data, all_count, all_time, report_size):
    """
    calculate stats data
    :param data: url + list of durations
    :param all_count: count of all request
    :param all_time: sum of all durations
    :param report_size: count of uris for report
    :return: json with data records
    """
    if data is None or len(data) == 0:
        return None
    i = 0
    d = list()
    max_size = report_size if len(data) > report_size else len(data)
    try:
        for r in data:
            count = len(data[r])
            count_perc = round(count * 100 / all_count, 3)
            time_sum = round(sum(data[r]), 3)
            time_max = round(max(data[r]), 3)
            time_avg = round(time_sum / count, 3)
            time_perc = round(time_sum * 100 / all_time, 3)
            time_med = round(median(data[r]), 3)

            d.append({'url': r, 'count': count, 'count_perc': count_perc, 'time_avg': time_avg,
                      'time_max': time_max, 'time_med': time_med, 'time_perc': time_perc, 'time_sum': time_sum})
            if i == max_size:
                break
            else:
                i += 1
        logging.info('statistics calculated')
        return json.dumps(d)
    except Exception as e:
        logging.error(e)
        return None


def create_report(data, rep_file, rep_path):
    """
    fill template and save it to report file
    :param data: json data
    :param rep_file: report file name
    :param rep_path: report path
    :return: none
    """
    rep_file = os.path.join(rep_path, rep_file)
    logging.info(rep_file)
    if data and len(data) > 0:
        try:
            with open('report.html', 'r') as f:
                template = Template(f.read())
                report_body = template.safe_substitute(table_json=data)
            with open(rep_file, 'w') as out:
                out.write(report_body)
        except Exception as e:
            logging.error(e)
            return
        logging.info(f'report created: "{rep_file}"')


def parse_filename_extension(extension):
    """
    try to parse log file extension
    :param extension:
    :return: list of date parts from file extension or None if file not correct log file
    """
    regex = 'log-(?P<dateandtime>\d{8})'
    x = re.search(regex, extension)
    try:
        st = x.group('dateandtime')
        datetime.strptime(st, '%Y%m%d')
        return st[0:4], st[4:6], st[6:8]
    except Exception:
        return None


def find_log_files(log_folder):
    """
    make dict: {date, log_file_name, report_file_name, is_gz: Boolean}
    :param log_folder: logs files folder
    :return: dict: {date, log_file_name, report_file_name, is_gz: Boolean}
    """
    if not Path(log_folder).is_dir():
        logging.error(f'"{log_folder}" is not a folder')
        return None

    result = list()
    for filename in os.listdir(log_folder):
        fname = os.path.basename(filename).split('.')
        if len(fname) > 1 and fname[0] == 'nginx-access-ui':
            date_parts = parse_filename_extension(fname[1])
            if date_parts:
                row = dict()
                row['rep_file'] = 'report-' + '.'.join(date_parts) + '.html'
                row['date'] = ''.join(date_parts)
                row['log_file'] = log_folder + filename
                row['gz'] = True if len(fname) == 3 and fname[2] == 'gz' else False
                result.append(row)
    result = sorted(result, key=lambda k: k['date'], reverse=True)
    return result[0] if len(result) > 0 else None


def main(internal_config):
    try:
        cfg = load_config(internal_config, get_config_filename())
        if not cfg:
            return
        logging.basicConfig(format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S',
                            level=logging.INFO, filename=cfg['LOG_FILE'])

        log_folder = os.path.join(cfg['LOG_DIR'], '')
        rep_folder = os.path.join(cfg['REPORT_DIR'], '')
        log_file = find_log_files(log_folder)

        if log_file is None:
            logging.info(f'the folder: {log_folder} has no files to process')
            return

        data, all_time, count, error_count = parse_log(log_file['log_file'], log_file['gz'])
        if data is None:
            return
        error_percent = (error_count / count) * 100
        logging.info(f'row count is: {count}, errors count is: {error_count}, '
                     f'errors percent is: {error_percent:.2f}')
        if error_percent > cfg['ERROR_PERCENT']:
            logging.info(f'error percent({error_percent:.2f}) '
                         f'exceeded specified value({cfg["ERROR_PERCENT"]:.2f}), cannot create report')
            return
        json_data = calc_stat(data, count, all_time, cfg['REPORT_SIZE'])
        create_report(json_data, log_file['rep_file'], rep_folder)
    except:
        logging.exception('unexpected error')


if __name__ == "__main__":
    main(config)
