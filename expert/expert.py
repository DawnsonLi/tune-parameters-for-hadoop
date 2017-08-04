# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 15:38:03 2017

@author: dawnson

构建基本的参数设置规则，建立专家系统的知识库
功能：对已经运行完成一次的任务，对其参数进行调整
"""

'''
@调节参数名称：
io.sort.mb
@说明：
每一个map都会对应存在一个内存buffer（MapOutputBuffer），map会将已经产生的部分结果先写入到该buffer中，这个buffer默认是100MB大小，
map task对磁盘的操作就会变少，如果map tasks的瓶颈在磁盘上，这样调整就会大大提高map的计算性能。
@调整策略：
计算历史任务中：每个map任务的平均数据溢出大小为：map过程spill的记录个数*记录字节大小/map个数，即为调整量
@函数输入说明：
map_num:map任务的个数
spill_records:发生溢出的记录个数
recordsize:每条记录的字节大小
d:历史任务配置参数构成的字典
注：这里的recordsize由mapoutput的字节数和记录数计算
'''
def getIOSortMb(map_num, spill_records, recordsize, d):
    sortmb = float(d['mapreduce.task.io.sort.mb'])
    avgspillsize = spill_records * recordsize * 1.0 / map_num
    o = sortmb + avgspillsize
    if o > 1200:
	return 1200
    return o

'''
@调节参数名称：
io.sort.factor
@说明：
该参数默认为10。它表示当merge spill文件时，最多能有多少并行的stream向merge文件中写入。
当map的中间结果非常大，调大io.sort.factor，有利于减少merge次数，进而减少map对磁盘的读写频率
@函数参数说明：map输出的字节大小，和阈值，默认10G,d为参数字典
'''
def getSortFactor(mapOutputBytes, d ):
    threshold=10000000
    if mapOutputBytes > threshold:
      return int(d['mapreduce.task.io.sort.factor'])+1
    else:
        return d['mapreduce.task.io.sort.factor']

'''
@调节参数名称：
mapred.compress.map.output
@说明：
压缩的好处在于，通过压缩减少写入读出磁盘的数据量。对中间结果非常大，磁盘速度成为map执行瓶颈的job，尤其有用
@函数参数说明：map输出的字节大小，和阈值，默认1G
'''
def Compress(mapOutputBytes, d, threshold=10000000):
    if mapOutputBytes > threshold:
        return True
    else:
        return d['mapred.compress.map.output']

'''
@调节参数名称：
map个数
@说明：
map的数量通常是由hadoop集群的DFS块大小确定的，也就是输入文件的总块数，
正常的map数量的并行规模大致是每一个Node是10~100个，对于CPU消耗较小的作业可以设置Map数量为300个左右，
但是由于hadoop的没一个任务在初始化时需要一定的时间，因此比较合理的情况是每个map执行的时间至少超过1分钟。
具体的数据分片是这样的，InputFormat在默认情况下会根据hadoop集群的DFS块大小进行分片，每一个分片会由一个map任务来进行处理，
当然用户还是可以通过参数mapred.min.split.size参数在作业提交客户端进行自定义设置。
还有一个重要参数就是mapred.map.tasks，这个参数设置的map数量仅仅是一个提示，
只有当InputFormat决定了map任务的个数比mapred.map.tasks值小时才起作用。
当用户的map数量较小或者比本身自动分割的值还小时可以使用一个相对较大的默认值，从而提高整体hadoop集群的效率。
'''
'''
def getMapNum(d,nodenum,avgmaptime):
    if avgmaptime < 1000:#如果小于1分钟
        return d['mapreduce.job.reduces']
    return nodenum*10
'''
'''
@调节参数名称：
reduce个数
@说明：
reduce在运行时往往需要从相关map端复制数据到reduce节点来处理，因此相比于map任务。reduce节点资源是相对比较缺少的，同时相对运行较慢，正确的reduce任务的个数应该是0.95或者1.75 *（节点数 ×mapred.tasktracker.tasks.maximum参数值）。
如果任务数是节点个数的0.95倍，那么所有的reduce任务能够在 map任务的输出传输结束后同时开始运行。
如果任务数是节点个数的1.75倍，那么高速的节点会在完成他们第一批reduce任务计算之后开始计算第二批 reduce任务，这样的情况更有利于负载均衡。
同时需要注意增加reduce的数量虽然会增加系统的资源开销，但是可以改善负载匀衡，降低任务失败带来的负面影响。
'''

def getReduceNum(d, nodenum):
    return int(int(d['mapreduce.tasktracker.reduce.tasks.maximum']) * 0.95 * nodenum)

'''
@调节参数名称：
mapred.reduce.parallel.copies
@说明：
Reduce copy数据的线程数量，默认值是5
Reduce到每个完成的Map Task 拷贝数据（通过RPC调用），默认同时启动5个线程到map节点取数据。
这个配置还是很关键的，如果你的map输出数据很大，有时候会发现map早就100%了，reduce却在缓慢的变化，那就是copy数据太慢了，
比如5个线程copy 10G的数据，确实会很慢，这时就要调整这个参数，但是调整的太大，容易造成集群拥堵
'''
def getReduceParallelCopies(mapOutputBytes, d, threshold=10000000):
    if mapOutputBytes > threshold:
        return int(float(d['mapreduce.task.io.sort.factor'] )* 1.5)
    else:
        return d['mapreduce.task.io.sort.factor']

'''
@调节参数名称：
mapred.job.shuffle.input.buffer.percent(0.7)
@说明：
当指定了JVM的堆内存最大值以后，上面这个配置项就是Reduce用来存放从Map节点取过来的数据所用的内存占堆内存的比例，默认是0.7，既70%，通常这个比例是够了，
但是对于大数据的情况，这个比例还是小了一些，0.8-0.9之间比较合适。
'''
import random
def getShuffleInputBufferPercent(reduceInRecord, d, threshold=1000000):
    if reduceInRecord > threshold:
        return random.randrange(73, 90, 2)*0.01
    else:
        return d['mapreduce.reduce.shuffle.input.buffer.percent']

'''
@调节参数名称：
mapred.job.shuffle.merge.percent(默认值0.66)
@说明：
指的是从Map节点取数据过来，放到内存，当达到这个阈值之后，后台启动线程（通常是Linux native process）把内存中的数据merge sort，写到reduce节点的本地磁盘；
从实际经验来看，mapred.job.shuffle.merge.percent默认值偏小，完全可以设置到0.8左右
'''
def getShuffleMergerPercent():
    return random.randrange(67, 82, 2)*0.01


