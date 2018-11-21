#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import gzip
import json
import logging
import os
import pathlib
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
        return {**internal_config, **cfg}
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


def create_report(data, rep_file):
    """
    fill template and save it to report file
    :param data: json data
    :param rep_file: report file name
    :return: none
    """
    if not data:
        return
    logging.info(rep_file)
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


def find_log_file(log_path):
    """
    make dict: {date, log_file_name, report_file_name, is_gz: Boolean}
    :param log_path: logs files path
    :return: dict: {date, log_file_name, report_file_name, is_gz: Boolean}
    """
    if not Path(log_path).is_dir():
        logging.error(f'"{log_path}" is not a folder')
        return None

    result = {}
    for filename in os.listdir(log_path):
        regex = 'nginx-access.ui.log-(?P<date>\d{8})(?:(?P<gz>.gz$)?)'
        x = re.match(regex, filename)
        if not x:
            continue
        try:
            file_date = datetime.strptime(x.group('date'), '%Y%m%d')
            if not result or file_date > result['date']:
                result['log_file'] = os.path.join(log_path, filename)
                result['date'] = file_date
                result['gz'] = True if x.group('gz') else False
        except Exception:
            continue
    return result or None


def check_report(log_date, rep_path):
    """
    check if report already exists.
    :param log_date: log file date
    :param rep_path: report folder
    :return: None if report exists, report file name if not
    """
    report_date = datetime.strftime(log_date, '%Y.%m.%d')
    rep_file = os.path.join(rep_path, f'report-{report_date}.html')
    if os.path.isfile(rep_file):
        logging.info(f'{rep_file} already exists')
        return None
    pathlib.Path(rep_path).mkdir(parents=True, exist_ok=True)
    return rep_file


def main(internal_config):
    cfg = load_config(internal_config, get_config_filename())
    if not cfg:
        return
    logging.basicConfig(format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S',
                        level=logging.INFO, filename=cfg['LOG_FILE'])

    log_folder = cfg['LOG_DIR']
    rep_folder = cfg['REPORT_DIR']
    log_desc = find_log_file(log_folder)

    if not log_desc:
        logging.info(f'the folder: {log_folder} has no files to process')
        return

    rep_file_name = check_report(log_desc['date'], rep_folder)
    if not rep_file_name:
        return

    data, all_time, count, error_count = parse_log(log_desc['log_file'], log_desc['gz'])
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
    create_report(json_data, rep_file_name)


if __name__ == "__main__":
    try:
        main(config.copy())
    except:
        logging.exception('unexpected error')
