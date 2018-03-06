# coding:utf-8
"""
publicmodels_test.py
单元测试模块
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 25
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import unittest
import shutil

from pm_main import get_config
from pm_time import get_ymd_and_hm, is_cross_time, str2date, date_str2list, get_date_range


class TestMain(unittest.TestCase):
    """
    pm_main 测试
    """
    def setUp(self):
        # 创建配置文件
        with open('test_config.cfg', 'w') as f:
            f.write('[PATH]\n')
            f.write('PATH = /root/home/user')

    def tearDown(self):
        # 删除配置文件
        os.remove('test_config.cfg')

    def test_get_config(self):
        config = get_config('.', 'test_config.cfg')
        self.assertEqual(config['PATH']['PATH'], '/root/home/user')

        with self.assertRaises(ValueError):
            get_config('.', 'wrong.cfg')


class TestTime(unittest.TestCase):
    """
    pm_time 测试
    """
    def setUp(self):
        path = os.getcwd()
        test_path = os.path.join(path, 'test')
        dirpath1 = os.path.join(test_path, '2018')
        dirpath2 = os.path.join(test_path, '201801')
        dirpath3 = os.path.join(test_path, '20180101')
        dirpath4 = os.path.join(test_path, '201801010101')
        os.makedirs(dirpath1)
        os.makedirs(dirpath2)
        os.makedirs(dirpath3)
        os.makedirs(dirpath4)
        with open('test_20180101_1751.test', 'w') as f:
            f.write('[PATH]\n')
            f.write('PATH = /root/home/user')

    def tearDown(self):
        path = os.getcwd()
        test_path = os.path.join(path, 'test')
        shutil.rmtree(test_path)
        os.remove('test_20180101_1751.test')

    def test_get_ymd_and_hm(self):
        path = os.getcwd()
        test_path = os.path.join(path, 'test')
        filepath1 = os.path.join('.', 'test_20180101_1751.test')
        dirpath1 = os.path.join(test_path, '2018')
        dirpath2 = os.path.join(test_path, '201801')
        dirpath3 = os.path.join(test_path, '20180101')
        dirpath4 = os.path.join(test_path, '201801010101')

        self.assertEqual(get_ymd_and_hm(filepath1), ['20180101', '1751'])
        self.assertEqual(get_ymd_and_hm(dirpath1), ['2018', ''])
        self.assertEqual(get_ymd_and_hm(dirpath2), ['201801', ''])
        self.assertEqual(get_ymd_and_hm(dirpath3), ['20180101', ''])
        self.assertEqual(get_ymd_and_hm(dirpath4), ['20180101', '0101'])
