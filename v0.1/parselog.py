# -*- coding:utf-8 -*-  
import requests
import MySQLdb 
'''
功能：获取并解析历史任务的详细信息，得到历史执行任务运行记录的详细信息
实现方式：调用hadoop history server API
备注：json解析格式与命名参考hadoop history server:
'''

#关注的参数
Wantedconf = ['mapreduce.tasktracker.map.tasks.maximum','mapreduce.input.fileinputformat.split.minsize',\
        'mapreduce.task.io.sort.mb','mapreduce.map.sort.spill.percent','mapreduce.job.jvm.numtasks',\
        'mapreduce.tasktracker.reduce.tasks.maximum','mapreduce.reduce.shuffle.merge.percent',\
        'mapreduce.job.reduce.slowstart.completedmaps','mapreduce.reduce.input.buffer.percent',\
        'mapreduce.reduce.merge.inmem.threshold','mapreduce.reduce.shuffle.input.buffer.percent',\
        'mapreduce.task.io.sort.factor','mapreduce.reduce.shuffle.parallelcopies','mapreduce.job.reduces',\
        'mapreduce.reduce.shuffle.memory.limit.percent']
#url和用户名、密码一定要符合权限
url = 'http://hit_history.viphk.ngrok.org/ws/v1/history/mapreduce/jobs/'
user = 'dbuser'
psw = 'dbcluster'

'''
@功能：按照jobid获取参数的配置信息
@输入：jobid
@返回：某个job的配置参数字典
'''
def getParameter(jobid):
    try:
        cs_url = url+str(jobid)+'/conf'#url为全局变量
        print cs_url
        r = requests.get(cs_url, auth=(user, psw))
        data = r.json()
        conf = data['conf']
        pro  = conf['property']
        parameter_dir = {}
        for item in pro:
            if item['name'] in  Wantedconf:
                value = item['value']
                parameter_dir[item['name']] = value
        print "get parameters ok"
        return parameter_dir
    except:
        print "network error when get parameters for jobid:",jobid
           
'''
@功能：按照jobid获取指定任务的输入数据大小
@输入：jobid
@返回：输入文件大小
'''
def getInput(jobid):
    try:
        cs_url = url+str(jobid)+'/counters'#url为全局变量
        print cs_url
        r = requests.get(cs_url, auth=(user, psw))
        data = r.json()
        jobCounters = data['jobCounters']
        counterGroup = jobCounters['counterGroup']
        for item in counterGroup:
            if item['counterGroupName'] == 'org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter':
                counter = item['counter']
                if counter != 'None':
                    print 'input size:', counter[0]['totalCounterValue']#因为部分文件counter有多个
                    return counter[0]['totalCounterValue']
    except:
         print "network error when get inputsize for jobid:",jobid
          
'''
@功能：访问job history server,返回jobid列表，以及jobid对应的执行时间字典
@输入：start:时间戳，用于进行api数据过滤
@返回：jobid列表和由jobid与job执行时间构成的列表
'''
def getJobs(start):
    try:
        cs_url = url[:len(url)-1]+'?startedTimeBegin='+str(start)#url为全局变量
        print cs_url
        print start
        print "_____________"        
        r = requests.get(cs_url, auth=(user, psw))
        data = r.json()
        if data == None:
            return None,None
        jobs = data['jobs']
        job = jobs['job']
        jobid_list = []
        jobid_excuteTime = {}
        for item in job:
            jobid_list.append(item['id'])
            startTime = int(item['startTime'])
            finishTime = int(item['finishTime'])
            excuteTime = finishTime - startTime
            jobid_excuteTime[item['id']] = excuteTime
        return jobid_list,jobid_excuteTime
    except:
        print "network error when getting jobs"
        
'''
@功能：将需要的信息存入数据库
'''
def saveParamters(jobid,excuteTime,parameters,inputsize):
    db = MySQLdb.connect("localhost","root","2345","hadoop" )
    sql = "insert into jobParameters(jobid,excuteTime,SplitSize,Parallelcopies ,JVMReuse ,Factor ,SortMB ,ShuffleMergePer,ReduceInputPer ,SortPer ,ReduceNum,ReduceTasksMax  ,ShuffleInputPer ,MapTasksMax ,ReduceSlowstart ,inMenMergeThreshold ,ShufflelimitPer,inputsize) values "
      
    l = []
    l.append(str(jobid))
    l.append(str(excuteTime))
    for p in parameters:
        l.append(float(p))
    l.append(inputsize)
    
    try:
        cursor = db.cursor()
        sql += str(tuple(l))
        cursor.execute(sql)
        db.commit()
    except:
        print "erro occurs when saving in mysql for jobid:",jobid
        db.rollback()
    db.close()   
'''
功能：判断某个job是否已经存在
'''
def jobExist(jobid):
    db = MySQLdb.connect("localhost","root","2345","hadoop" )
    sql = "select * from jobParameters where jobid = '"
    sql += str(jobid)
    sql += "'"
    
    try:
        cursor = db.cursor()
        data = cursor.execute(sql)
        db.commit()
    except:
        print "commit erro when justify if the job exist"
        db.rollback()
        db.close() 
        return True
    if data > 0:
        db.close()  
        print str(jobid)+" exist"
        return True#已经存在   
    else:   
        db.close()
        print str(jobid)+"not exist"
        return False
'''
功能：将取到的数据存入数据库参数表中
'''  
def GetsAndSave(start):
    try:
        jobid_list,jobid_excuteTime = getJobs(start)#获取全部jobid
        if jobid_list == None:
            return        
        print "all jobs:",jobid_list     
       
        for jobid in jobid_list:   
            if jobExist(jobid) != True:#不存在则插入数据库
                inputsize = getInput(jobid)#获取输入大小
                parameterdir = getParameter(jobid)#获取参数字典
                excuteTime = jobid_excuteTime[jobid]
                parameters = []
                for p in Wantedconf:
                    parameters.append(parameterdir[p])
                saveParamters(jobid,excuteTime,parameters,inputsize)
                
    except:
        print "cannot fetch data for network error"
    print "fetch and save successfully"

import time
from time import ctime, sleep   
start = 0
while True:
    try:
        GetsAndSave(start)
        print "at time %s" % ctime()
        sleep(10)#1小时取一回数据
        start =time.time()
    except:
        print "cannot fetch data normally，trys to connect latter"
        sleep(10)#过一分钟再次连接
