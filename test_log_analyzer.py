import json
import unittest

from homework.log_analyzer import load_config, config, median, parse_line, calc_stat


class SimpleTest(unittest.TestCase):
    def test_load_config(self):
        cfg = load_config(config)
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg['REPORT_SIZE'], 1000)
        self.assertEqual(cfg['ERROR_PERCENT'], 32.5)

    def test_median(self):
        list1 = [1, ]
        self.assertEqual(median(list1), 1)
        list2 = [1, 2, 3, 4]
        self.assertEqual(median(list2), 2.5)
        list3 = [1, 2, 3, 4, 5]
        self.assertEqual(median(list3), 3)

    def test_parse_line(self):
        line1 = '1.196.116.32 -  - [29/Jun/2017:03:50:22 +0300] "GET /api/v2/banner/25019354 HTTP/1.1" 200 927 ' \
                '"-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5" "-" ' \
                '"1498697422-2190034393-4708-9752759" "dc7161be3" 0.390'
        url, duration = parse_line(line1)
        self.assertEqual(url, '/api/v2/banner/25019354')
        self.assertEqual(duration, 0.390)

    def test_parse_line_bad(self):
        line1 = '1.99.174.176 3b81f63526fa8  - [29/Jun/2017:03:50:22 +0300] '
        line2 = '1.99.174.176 3b81f63526fa8  - [29/Jun/2017:03:50:22 +0300] ' \
                '"GET /api/1/photogenic_banners/list/?server_name=WIN7RB4 HTTP/1.1" ' \
                '200 12 "-" "Python-urllib/2.7" "-" "1498697422-32900793-4708-9752770" "-" 0.133 vvv'
        url, duration = parse_line(line1)
        self.assertEqual(url, None)
        url, duration = parse_line(line2)
        self.assertEqual(url, None)

    def test_calc_stat(self):
        data = {'url1': [7, 8, 9],
                'url2': [3, 4, 5],
                'url3': [1, 2, 3]}
        json_str = calc_stat(data, 9, 42, 3)
        data = json.loads(json_str)
        self.assertEqual(data[0]['time_avg'], 8.0)
        self.assertEqual(data[1]['time_avg'], 4.0)


if __name__ == '__main__':
    unittest.main()
