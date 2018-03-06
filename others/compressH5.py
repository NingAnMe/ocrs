# coding: UTF-8
'''
Created on 2011-9-20

@author: zhangtao
'''
import h5py


def compressHdf5(fpath0, fpath1):
    '''
    压缩HDF5
    fpath0  输入路径
    fpath1  输出路径
    '''
    h5file0 = h5py.File(fpath0, 'r')
    h5file1 = h5py.File(fpath1, 'w')

    deepCopy(h5file0, h5file1)

    h5file0.close()
    h5file1.close()


def deepCopy(obj1, obj2):

    for key in obj1.keys():
        set1 = obj1.get(key)

        if type(set1).__name__ == "Group":
            set2 = obj2.create_group(key)
            deepCopy(set1, set2)
        else:
            set2 = obj2.create_dataset(key, dtype=set1.dtype, data=set1,
                                       compression='gzip', compression_opts=5,  # 压缩等级5
                                       shuffle=True)
            # 复制dataset属性
            for akey in set1.attrs.keys():
                set2.attrs[akey] = set1.attrs[akey]

    # 复制group属性
    for akey in obj1.attrs.keys():
        obj2.attrs[akey] = obj1.attrs[akey]


if __name__ == '__main__':
    in_file = u'D:/nsmc/data/ocrs/FY3/FY3B/MERSI/L1/OBC/2010/20101118/FY3B_MERSI_GBAL_L1_20101118_2255_OBCXX_MS.HDF'
    out_file = u'D:/nsmc/data/ocrs/FY3/OUT/OBC/2010/20101118/FY3B_MERSI_GBAL_L1_20101118_2255_OBCXX_MS.HDF'
    compressHdf5(in_file, out_file)
