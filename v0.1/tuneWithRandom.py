#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 11:00:14 2017

@author: dawnson
"""

'''
@功能：
用户提交submit接口：用户使用此接口时，默认采用系统的自动参数调优工具
用户传入参数：原生的hadoop提交Jar包命令，例如：hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort input output
@依赖：
需要安装python hdfs模块
安装命令 pip install hdfs
@要求：
v0.1的局限性：
（1）需要用户在提交任务时通过setjobname()方法，将jobname设置为函数名
如运行/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort input output 命令时，需要将Jobname设置为terasort
（2）只能针对相同name的job，因而具有局限性
'''

'''
@功能：调用hdfs模块，获取用户输入文件路径信息
@输入：为hdfs上的路径
@返回：指定输入文件夹中使用的字节数
@注意事项：需要配置成hdfs所在的地址,有用户权限问题
'''

from hdfs import InsecureClient
def getPathLength(spath,host = 'http://localhost:50070',user='root'):
    try:
        client = InsecureClient(host,user)
        return client.content(spath)['length']
    except:
        print "erros occurs when accsing HDFS,check for the host:",host," and the user:",user

'''
@功能：判断是否存在历史相同任务，如存在，则进行优化,并构建模型,否则，不进行优化
@实现：当前实现的方案为：通过函数名称是否相同来判断任务是否相同，即数据库检索操作
@要求：需要用户在提交job时设定job name 等于 主函数名称
'''

import trainModel
def createModel(funcname):
    # 构造sql语句
    sql = " select * from jobParameters right join jobsubmit on jobParameters.jobid = jobsubmit.jobid "
    sql = sql + "where jobsubmit.jobname= '" + str(funcname) + "'"
    x, y = trainModel.SQLTrainData(sql)
    if len(x) >1:  # 检索出结果
        et = trainModel.getModel(x, y)
        return et
    return None

'''
@功能：创建含有优化参数的命令
@输入：训练好的模型和数据量
@返回：运行的hadoop命令
'''
def optiConf(model,N,command):
    conf = optimize.optimizer('config.json', model, 100000.0, 0.95, N)
    commandconfs = ''
    commandconfs = commandconfs + ' -D mapreduce.input.fileinputformat.split.minsize=' + str(int(conf[0]))
    commandconfs = commandconfs + ' -D mapreduce.reduce.shuffle.parallelcopies=' + str(int(conf[1]))
    commandconfs = commandconfs + ' -D mapreduce.job.jvm.numtasks=' + str(int(conf[2]))
    commandconfs = commandconfs + ' -D mapreduce.task.io.sort.factor=' + str(int(conf[3]))
    commandconfs = commandconfs + ' -D mapreduce.task.io.sort.mb=' + str(int(conf[4]))
    commandconfs = commandconfs + ' -D mapreduce.reduce.shuffle.merge.percent=' + str(conf[5])
    commandconfs = commandconfs + ' -D mapreduce.reduce.input.buffer.percent=' + str(conf[6])
    commandconfs = commandconfs + ' -D mapreduce.map.sort.spill.percent=' + str(conf[7])
    commandconfs = commandconfs + ' -D mapreduce.job.reduces=' + str(int(conf[8]))
    commandconfs = commandconfs + ' -D mapreduce.tasktracker.reduce.tasks.maximum=' + str(int(conf[9]))
    commandconfs = commandconfs + ' -D mapreduce.reduce.shuffle.input.buffer.percent=' + str(conf[10])
    commandconfs = commandconfs + ' -D mapreduce.tasktracker.map.tasks.maximum=' + str(int(conf[11]))
    commandconfs = commandconfs + ' -D mapreduce.job.reduce.slowstart.completedmaps=' + str(conf[12])
    commandconfs = commandconfs + ' -D mapreduce.reduce.merge.inmem.threshold=' + str(int(conf[13]))
    commandconfs = commandconfs + ' -D mapreduce.reduce.shuffle.memory.limit.percent=' + str(conf[14])

    jar = command[3]
    funcname = command[4]
    inputpath = command[5]
    outputpath = command[6]

    jarcommand = "hadoop jar "
    jarcommand += str(jar)
    jarcommand += " "
    jarcommand += str(funcname)
    jarcommand += " "

    iocommand = str(inputpath)
    iocommand += " "
    iocommand += str(outputpath)

    return jarcommand + commandconfs + " " + iocommand

'''
@功能：当发现不能对任务进行调优时，就进行随机的参数配置
'''
import random
def randomConf(command):
    MapTasksMax = random.randrange(2, 203,20)  # 2 mapreduce.tasktracker.map.tasks.maximum  10 #there is no order here, but in optimize
    SplitSize = random.randrange(0, 300, 64)  # 0 mapreduce.input.fileinputformat.split.minsize 5
    SortMB = random.randrange(100, 1000, 50)  # 100 mapreduce.task.io.sort.mb 18
    SortPer = 0.05 * random.randint(0, 7) + 0.6  # 0.8 mapreduce.map.sort.spill.percent 8
    JVMReuse = random.randint(1, 10)  # 2 mapreduce.job.jvm.numtasks 10
    ReduceTasksMax = random.randrange(2, 30, 2)  # 2 mapreduce.tasktracker.reduce.tasks.maximum 14
    ShuffleMergePer = 0.05 * random.randint(0, 9) + 0.5  # 0.66 mapreduce.reduce.shuffle.merge.percent 10
    ReduceSlowstart = 0.05 * random.randint(0, 7)  # 0.05 mapreduce.job.reduce.slowstart.completedmaps 8
    ReduceInputPer = 0.05 * random.randint(0, 9)  # 0.0 mapreduce.reduce.input.buffer.percent 10
    inMenMergeThreshold = random.randrange(100, 2000, 50)  # 1000 mapreduce.reduce.merge.inmem.threshold 38
    ShuffleInputPer = 0.05 * random.randint(0, 9) + 0.5  # 0.7 mapreduce.reduce.shuffle.input.buffer.percent 10
    Factor = random.randrange(5, 51, 5)  # 10 mapreduce.task.io.sort.factor  10
    Parallelcopies = random.randrange(5, 51, 5)  # 5 mapreduce.reduce.shuffle.parallelcopies 10
    ReduceNum = random.randrange(5, 51, 5)  # 1 mapreduce.job.reduces 10
    ShufflelimitPer = 0.05 * random.randint(0, 9)  # 0.25 mapreduce.reduce.shuffle.memory.limit.percent 10
    confs = []
    confs.append(MapTasksMax)
    confs.append(SplitSize)
    confs.append(SortMB)
    confs.append(JVMReuse)
    confs.append(SortPer)
    confs.append(ReduceTasksMax)
    confs.append(ReduceSlowstart)
    confs.append(ShuffleMergePer)
    confs.append(ReduceInputPer)
    confs.append(ShuffleInputPer)
    confs.append(inMenMergeThreshold)
    confs.append(Factor)
    confs.append(Parallelcopies)
    confs.append(ReduceNum)
    confs.append(ShufflelimitPer)

    commandconfs = ''
    commandconfs = commandconfs + ' -D mapreduce.tasktracker.map.tasks.maximum=' + str(MapTasksMax)
    commandconfs = commandconfs + ' -D mapreduce.input.fileinputformat.split.minsize=' + str(SplitSize)
    commandconfs = commandconfs + ' -D mapreduce.task.io.sort.mb=' + str(SortMB)
    commandconfs = commandconfs + ' -D mapreduce.map.sort.spill.percent=' + str(SortPer)
    commandconfs = commandconfs + ' -D mapreduce.job.jvm.numtasks=' + str(JVMReuse)
    commandconfs = commandconfs + ' -D mapreduce.tasktracker.reduce.tasks.maximum=' + str(ReduceTasksMax)
    commandconfs = commandconfs + ' -D mapreduce.reduce.shuffle.merge.percent=' + str(ShuffleMergePer)
    commandconfs = commandconfs + ' -D mapreduce.job.reduce.slowstart.completedmaps=' + str(ReduceSlowstart)
    commandconfs = commandconfs + ' -D mapreduce.reduce.input.buffer.percent=' + str(ReduceInputPer)
    commandconfs = commandconfs + ' -D mapreduce.reduce.merge.inmem.threshold=' + str(inMenMergeThreshold)
    commandconfs = commandconfs + ' -D mapreduce.reduce.shuffle.input.buffer.percent=' + str(ShuffleInputPer)
    commandconfs = commandconfs + ' -D mapreduce.task.io.sort.factor=' + str(Factor)
    commandconfs = commandconfs + ' -D mapreduce.reduce.shuffle.parallelcopies=' + str(Parallelcopies)
    commandconfs = commandconfs + ' -D mapreduce.job.reduces=' + str(ReduceNum)
    commandconfs = commandconfs + ' -D mapreduce.reduce.shuffle.memory.limit.percent=' + str(ShufflelimitPer)

    jar = command[3]
    funcname = command[4]
    inputpath = command[5]
    outputpath = command[6]

    jarcommand = "hadoop jar "
    jarcommand += str(jar)
    jarcommand += " "
    jarcommand += str(funcname)
    jarcommand += " "

    iocommand = str(inputpath)
    iocommand += " "
    iocommand += str(outputpath)

    return  jarcommand + commandconfs + " " + iocommand

'''

@功能:用于解析用户提交的hadoop命令，例如 hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort
@接口要求：用户提交的格式为 hadoop jar jar包路径 函数名 输入目录  输出目录，中间用空格分开
@输入：解析过的用户输入的命令，数据量阈值大小
'''

def parseAndSubmit(command,threshold_size):
    try:
        funcname = command[4]
        inputpath = command[5]
        N = getPathLength(str(inputpath).strip())  # 计算输入N
        print "the input size bytes : ", N
        if int(N) < threshold_size:  # 如果数据量小于阈值，不进行优化
            randomcommand = randomConf(command)
            #print randomcommand
            print "do not tune for the inputsize is low"
            callcommand(randomcommand)
        else:
            model = createModel(str(funcname).strip())
            print "create model successfully"
            if model != None:#可以优化
                print "start tunning"
                tunecommand = optiConf(model,N,command)
                #print tunecommand
                callcommand(tunecommand)
            else:
                print "doesn't tune for the history data like this job is little"

    except:
        print "some erros occur and use random parameters to run"
        randomcommand = randomConf(command)
        #print randomcommand
        callcommand(randomcommand)

'''
@功能：将命令传入python命令行进行执行
@输入：命令行，如hadoop jar .....
'''
import commands
def callcommand(tunecommand):
    print "start hadoop job"
    print commands.getoutput(tunecommand)

import optimize
import sys
if __name__ == "__main__":
    parseAndSubmit(sys.argv,100000)
    