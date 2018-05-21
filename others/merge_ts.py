# -*-coding: utf-8 -*-
"""
合并 ts 文件
文件夹内文件的名字最好按顺序命名：01.ts 02.ts 03.ts
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 21
~~~~~~~~~~~~~~~~~~~
"""
import sys
import os


# def add(x):
#     """'x' is the amount of *.ts
#     This function should add all other .ts combined into '1.ts'
#     """
#     os.popen("touch all.ts")  # 创建一个空的文件
#     x = int(x)  # 原来文件的数量，在他这个程序里面，也是名字，比如 '1.ts'
#     i = 1
#     while i <= x:
#         command = "cat " + str(i) + ".ts " + ">> all.ts"
#         try:
#             os.popen(command)
#         except:  # 如果没有当前.ts, 打印出错误并跳过
#             print "There is no " + i + ".ts"
#         i += 1
#     return True
#
#
# if __name__ == '__main__':
#     num = sys.argv[1]  # num is the amount of *.ts files
#     add(num)

def run(in_path, out_file):
    """
    遍历文件夹下面的文件，进行合并
    :param out_file:
    :param in_path:
    :return:
    """
    # 将文件夹的文件遍历到一个列表
    tem_files = os.listdir(in_path)
    files = [x for x in tem_files if ".ts" in x]  # 简单的过滤一遍 ts 文件
    files.sort()  # 不知道顺序是不是重要，按名字排序一下
    for ts_file in files:
        command = "cat {} >> {}".format(ts_file, out_file)
        try:
            os.system(command)  # 相当于上面的 os.popen() ，不过既然没有使用输出的内容，用这个就行了
        except Exception as why:
            print why
            print "Error: {}".format(ts_file)
    print "Exit"


if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        u"""
        [参数1]： 待处理文件的文件夹
        [参数2]： 输出文件的文件名
        [样例]： python merge_ts.py /ts ts_merge.ts
        """
    if "-h" in args or "--help" in args or len(args) != 2:
        print help_info
        sys.exit(-1)
    else:
        IN_PATH = args[0]
        OUT_FILE = args[1]
        run(IN_PATH, OUT_FILE)
