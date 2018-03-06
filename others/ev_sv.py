# coding:utf-8

import numpy as np
import publicmodels as pm

# 读取两个表的数据
path = ur'D:\nsmc\data\ocrs\FY3\FY3B\MERSI\L1\OBC\2010\20101118\FY3B_MERSI_GBAL_L1_20101118_2255_OBCXX_MS.HDF'
set_name1 = 'SV_250m_REFL'
set_name2 = 'SV_1km'
dataset_ev = pm.pm_h5py.read_dataset_hdf5(path, set_name1)
dataset_sv = pm.pm_h5py.read_dataset_hdf5(path, set_name2)
# 生成其中一个通道的数据
l = []
set1 = dataset_ev[2]
set2 = dataset_sv[2]
for k in xrange(0, 2000):
    l.extend([set1[k] + set2[k]])

np.savetxt('ev_sv_10_2255_2.txt', l, fmt='%d')
