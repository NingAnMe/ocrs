# -*- coding: utf-8 -*-

from array import array
import os
import sys
import time

from mpi4py import MPI


def main():

    # 初始化mpi接口
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    name = MPI.Get_processor_name()

    # 记录时间
    t_start = MPI.Wtime()

    if rank == 0:
        manager(comm, size)
    else:
        worker(comm, rank)

    t_end = MPI.Wtime()

    t_total = (t_end - t_start)

    if rank == 0:
        print 'all time : <<%2ld:%2ld:%2ld>>\n' % (t_total / 3600, (t_total % 3600) / 60, t_total % 60)
        sys.stdout.flush()


def manager(comm, size, args_list=None):

    # 读取分发列表
    if args_list is None:
        with open('filelist.txt') as fp:
            file_lines = fp.readlines()
    else:
        file_lines = args_list

    # 记录文件
    terminate = 0

    while terminate < size - 1:
        # 接收worker发来请求
        power = comm.recv(None)

        # 判断worker的状态是0则发送文件给worker
        if power[0] == 0 and len(file_lines) != 0:
            comm.send(file_lines[0], power[1])
#             print("I am manager sent the file : %s to the process :%d\n" %
#                   (file_lines[0], power[1]))
            file_lines.pop(0)

        # 文件发送完毕 告诉woker
        if len(file_lines) == 0:
            comm.send(0, power[1])

        # 判断worker状态是1 则是已经处理完成
        if power[0] == 1:
            terminate = terminate + 1


def worker(comm, rank):

    size = comm.Get_size()
    name = MPI.Get_processor_name()

    power = array('i', [0, 0])
    power[0] = 0
    power[1] = rank

    while True:
        # 告诉主控我是第几个进程
        comm.send(power, 0)
        # 从主控接收分发的消息
        cmd = comm.recv(None, 0)
        #
        if 0 != cmd:
            # print("I received the file %s from Manager.\n" % cmd)
            sys.stdout.write(
                "Hello, World I am process %d of %d on %s.\n"
                % (rank, size, name))
            print cmd
            os.system(cmd)
            sys.stdout.flush()

        # 告诉主控收到结束消息
        else:
            power[0] = 1
            comm.send(power, 0)
            break

if __name__ == '__main__':

    main()
