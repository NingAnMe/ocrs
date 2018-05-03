# coding:utf-8

import os
import sys
import logging
from datetime import datetime

import h5py
import numpy as np

import publicmodels as pm
from publicmodels.pm_time import time_this, time_block
from ocrs_sv_extract import sv_extract

from DV.dv_img import dv_rgb


file_path = "/storage-space/disk3/Granule/out/2017/201710/20171012/20171012_0000_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_0000_1000M.HDF"

h5 = h5py.File(file_path, 'r')

print h5.keys()

h5.close()