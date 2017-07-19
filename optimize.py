# -*- coding:utf-8 -*-  
import random
import json
import math
import trainModel
import numpy as np
'''
功能：使用优化方法在空间中搜索最优参数
'''

'''
类名：sample
功能：读取参数配置文件config.json并进行解析
'''
class sample:
    def __init__(self,config_json):
        json_data = open(config_json)
        data = json.load(json_data)
        self.confjson = data["sample_standard_list"]
    def sampleconf(self):
        x_sample = []
        for conf in self.confjson:
            if conf['type'] == 'int':
                x = float(random.randrange(int(conf['low-bound']),int(conf['high-bound']),int(conf['interval'])))
                x_sample.append(x)
            elif conf['type'] == 'float':
                f_temp = float(conf['high-bound'])-float(conf['low-bound'])
                x = float(conf['high-bound']) + random.randint(0,int(f_temp / float(conf['interval'])))*float(conf['interval'])
                x_sample.append(x)
        x_sample.append(6000000000.0)
        return x_sample   
    '''
    功能：返回json文件中i号参数的步长
    '''
    def getstep(self,i):
        j = 0
        for conf in self.confjson:
            if j == i:
                return float(conf['interval'])
            j+=1
        return -1
    '''
    功能：返回json文件中i号参数的高边界值
    '''
    def get_high_bound(self,i):
        j = 0
        for conf in self.confjson:
            if j == i:
                return float(conf['high-bound'])
            j+=1
        return -1
    '''
    功能：返回json文件中i号参数的低边界值
    '''
    def get_low_bound(self,i):
        j = 0
        for conf in self.confjson:
            if j == i:
                return float(conf['low-bound'])
            j+=1
        return -1
    '''
    功能：返回配置的总数
    '''
    def getnum_of_conf(self):
        j = 0
        for conf in self.confjson:
            j += 1
        return j
'''
模拟退火优化算法
参数名称：config_json:配置参数,T为温度，cool为降低温度的比例(幅度)
'''
def optimizer(config_json,T,cool):
    s = sample(config_json)
    vec = s.sampleconf()#取一组配置参数
    conflen = s.getnum_of_conf()#获取配置参数的总数
    x,y = trainModel.getTrainData()
    et = trainModel.getModel(x,y)
    while T > 0.1:
        i = random.randint(0,conflen-1)#随机选择一个参数进行值的修改
        step = s.getstep(i)#获取选定参数的步长
        dis = random.randint(-1,1)*step#移动的距离
        vecb = vec #保存参数副本，用于修改某个参数
        vecb[i] += dis#将指定位置的参数进行修改
        if vecb[i] < s.get_low_bound(i):
            vecb[i] = s.get_low_bound(i)
            #print i,':out of index of low'
        elif vecb[i] > s.get_high_bound(i):
            vecb[i] = s.get_high_bound(i)
            #print i, ':out of index of high'
        lvec = []
        lvec.append(vec)
        lvecb = []
        lvecb.append(vecb)
       
        ea = et.predict(np.array(lvec))#返回预测时间
        eb = et.predict(np.array(lvecb))
        if (eb<ea or random.random()<pow(math.e,-(eb-ea)/T)):
            vec = vecb[:]
        T = T*cool
    return vec
conf = optimizer('config.json',100000.0,0.95)
print conf


Jarcommand = 'hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort'
IOPath = ' terainput60G tscoutput'
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
COMMAND1 = Jarcommand+commandconfs+IOPath
