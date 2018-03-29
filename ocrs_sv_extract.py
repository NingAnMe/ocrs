# coding:utf-8
"""
ocrs_sv_extract.py
提取 OBC 文件的 SV 数据
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 24
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import numpy as np

import publicmodels as pm


def sv_extract(obc, probe_250m, probe_1000m):
    """
    提取 OBC 文件的 SV 数据
    :param obc: OBC 文件
    :param probe_250m: (list)250m 每个通道选取的探元 id
    :param probe_1000m: (list)1000m 每个通道选取的探元 id
    :return:
    """
    # 获取数据集
    setnames_obc = ['SV_1km', 'SV_250m_REFL']
    datasets_obc = pm.pm_h5py.read_dataset_hdf5(obc, setnames_obc)
    # 提取 SV_250m_REFL
    dataset_250m = []
    for i in xrange(0, 4):
        dataset = datasets_obc['SV_250m_REFL'][i]
        probe_count = 40
        probe_id = probe_250m[i]

        dataset_new = sv_dataset_extract(dataset, probe_count, probe_id)
        dataset_250m.append(dataset_new)
    # 提取 SV_1km
    dataset_1000m = []
    for i in xrange(0, 15):
        dataset = datasets_obc['SV_1km'][i]
        probe_count = 10
        probe_id = probe_1000m[i]

        dataset_new = sv_dataset_extract(dataset, probe_count, probe_id)
        dataset_1000m.append(dataset_new)

    # 将数据转换为 ndarray 类
    sv_250m = np.array(dataset_250m)
    sv_1000m = np.array(dataset_1000m)

    return sv_250m, sv_1000m


def sv_dataset_extract(dataset, probe_count, probe_id):
    """
    提取某个通道 SV 数据集的数据
    :param dataset: SV 数据集
    :param probe_count: (int) 探元数量
    :param probe_id: (int) 此通道对应的探元 id
    :return:
    """
    # 筛选探元号对应的行
    dataset_ext = pm.pm_calculate.extract_lines(dataset, probe_count, probe_id)
    # 计算 avg 和 std
    avg_std_list = pm.pm_calculate.rolling_calculate_avg_std(dataset_ext, 10)
    # 过滤有效值
    dataset_valid = pm.pm_calculate.filter_valid_value(dataset_ext,
                                                       avg_std_list, 2)
    # 计算均值
    dataset_avg = pm.pm_calculate.calculate_avg(dataset_valid)
    dataset_avg = np.array(dataset_avg).reshape(len(dataset_avg), 1)
    # 将行数扩大 10 倍
    dataset_avg = pm.pm_calculate.expand_dataset_line(dataset_avg, 10)
    # 对浮点数据数据进行四舍五入
    dataset_new = np.rint(dataset_avg)
    return dataset_new
