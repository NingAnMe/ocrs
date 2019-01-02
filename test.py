# co ing=utf-8

import os
import re
import sys
import h5py
import numpy as np


def main():

    a = [[645.0, 1209.75], [1210.0, 1999.75],
         [2000.0, 2760.0], [645.0, 2760.0]]
    b = np.array(a)
    print b.shape

if __name__ == '__main__':

    main()
