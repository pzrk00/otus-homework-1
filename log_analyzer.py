#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import gzip
import json
import logging
import os
import re
import sys
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


def load_yaml(filename):
    """
    load .yaml config file
    :param filename:
    :return: config dict
    """
    try:
        with open(filename, 'r', encoding='utf-8') as config_file:
            config_from_file = yaml.load(config_file)
            return config_from_file
    except Exception as e:
        logging.exception(e)
    return None


def check_int(v):
    """
    check parameter - is int
    :param v:
    :return: int if correct
    """
    try:
        return int(v)
    except ValueError as e:
        logging.exception(e)
        return None


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
                report_size = check_int(config_from_file[k])
                if report_size is None:
                    return None
                if report_size < 1:
                    logging.error(f'REPORT_SIZE parameter must be greater 1, config value is: {report_size}')
                    return None
                result[k] = report_size
            elif k == 'ERROR_PERCENT':
                err_percent = check_float(config_from_file[k])
                if err_percent is None:
                    return None
                if err_percent >= 100 or err_percent < 0:
                    logging.error(f'ERROR_PERCENT parameter must be greater 0 and less 100, '
                                  f'config value is: {err_percent}')
                    return None
                result[k] = err_percent
            else:
                result[k] = config_from_file[k]
        else:
            result[k] = internal_config[k]
    return result


def load_config(internal_config):
    """
    load external config file(yaml) if exist
    :return: config dict
    """
    cfg_filename = get_config_filename()
    if cfg_filename:
        cfg = load_yaml(cfg_filename)
        if cfg:
            return merge_config(internal_config, cfg)
        else:
            return None
    else:
        cfg_filename = os.path.splitext(os.path.basename(__file__))[0] + '.yaml'
        if Path(cfg_filename).is_file():
            cfg = load_yaml(cfg_filename)
            if cfg:
                return merge_config(internal_config, cfg)
            else:
                return None
        else:
            return config


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
        with open(fn, 'r', encoding='utf-8') if not gz else gzip.open(fn, 'rt', encoding='utf-8') as log_file:
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
    # all_count = len(data)

    max_size = report_size if len(data) > report_size else len(data)
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


def create_report(data, rep_file):
    """
    fill template and save it to report file
    :param data: json data
    :param rep_file: report file name
    :return: none
    """
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


def find_log_files(log_folder, rep_folder):
    """
    make dict: {date, log_file_name, report_file_name, is_gz: Boolean}
    :param log_folder: logs files folder
    :param rep_folder: reports files folder
    :return: dict: {date, log_file_name, report_file_name, is_gz: Boolean}
    """
    if not Path(log_folder).is_dir():
        logging.error(f'"{log_folder}" is not a folder')
        return None
    if not Path(rep_folder).is_dir():
        logging.error(f'"{rep_folder}" is not a folder')
        return None

    result = list()
    for filename in os.listdir(log_folder):
        fname = os.path.basename(filename).split('.')
        if len(fname) > 1 and fname[0] == 'nginx-access-ui':
            date_parts = parse_filename_extension(fname[1])
            if date_parts:
                rep_file_name = 'report-' + '.'.join(date_parts) + '.html'
                row = dict()
                row['date'] = ''.join(date_parts)
                row['log_file'] = log_folder + filename
                row['rep_file'] = rep_folder + rep_file_name
                row['gz'] = True if len(fname) == 3 and fname[2] == 'gz' else False
                result.append(row)
    result = sorted(result, key=lambda k: k['date'], reverse=True)
    if len(result) > 0 and not Path(result[0]['rep_file']).is_file():
        return result[0]
    else:
        return dict()


def main(internal_config):
    cfg = load_config(internal_config)
    if not cfg:
        return
    logging.basicConfig(format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S',
                        level=logging.INFO, filename=cfg['LOG_FILE'])
    logging.info(cfg)

    log_folder = cfg['LOG_DIR'] + '/'
    log_file = find_log_files(log_folder, cfg['REPORT_DIR'] + '/')

    if log_file is not None:
        if len(log_file) > 0:
            data, all_time, count, error_count = parse_log(log_file['log_file'], log_file['gz'])
            error_percent = (error_count / count) * 100
            logging.info(f'row count is: {count}, errors count is: {error_count}, '
                         f'errors percent is: {error_percent:.2f}')
            if error_percent > cfg['ERROR_PERCENT']:
                logging.info(f'error percent({error_percent:.2f}) '
                             f'exceeded specified value({cfg["ERROR_PERCENT"]:.2f}), cannot create report')
                return
            json_data = calc_stat(data, all_time, cfg['REPORT_SIZE'])
            create_report(json_data, log_file['rep_file'])
        else:
            logging.info(f'the folder: {log_folder} has no files to process')


if __name__ == "__main__":
    main(config)
