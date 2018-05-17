import os
import sys
import calendar
from datetime import datetime
from multiprocessing import Pool, Lock

import numpy as np
from matplotlib.ticker import MultipleLocator

from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from numpy.lib.polynomial import polyfit
from numpy.ma.core import std, mean
from numpy.ma.extras import corrcoef

from DV import dv_pub_3d
from DV.dv_pub_3d import plt, mpl, mdates, Basemap
from DV.dv_pub_3d import bias_information, day_data_write, get_bias_data, get_cabr_data, set_tick_font
from DV.dv_pub_3d import FONT0, FONT_MONO, FONT1
from DM.SNO.dm_sno_cross_calc_map import RED, BLUE, EDGE_GRAY, ORG_NAME, mpatches
from PB.CSC.pb_csc_console import LogServer
from PB import pb_time, pb_io
from PB.pb_time import is_day_timestamp_and_lon

from plt_io import ReadHDF5, loadYamlCfg