# -*- coding:utf-8 -*-
import requests

'''
功能：给出了基于经验的对刚刚运行的任务进行自动化优化的一种方案
'''
Wantedconf = ['mapreduce.tasktracker.map.tasks.maximum', 'mapreduce.input.fileinputformat.split.minsize', \
              'mapreduce.task.io.sort.mb', 'mapreduce.map.sort.spill.percent', 'mapreduce.job.jvm.numtasks', \
              'mapreduce.tasktracker.reduce.tasks.maximum', 'mapreduce.reduce.shuffle.merge.percent', \
              'mapreduce.job.reduce.slowstart.completedmaps', 'mapreduce.reduce.input.buffer.percent', \
              'mapreduce.reduce.merge.inmem.threshold', 'mapreduce.reduce.shuffle.input.buffer.percent', \
              'mapreduce.task.io.sort.factor', 'mapreduce.reduce.shuffle.parallelcopies', 'mapreduce.job.reduces', \
              'mapreduce.reduce.shuffle.memory.limit.percent']
# url和用户名、密码一定要符合权限
url = 'http://s18:19888/ws/v1/history/mapreduce/jobs/'
user = 'mesos'
psw = 'mesos106'

'''
功能：按照jobid获取参数的配置信息
输入：jobid
返回：某个job的配置参数字典
'''

def getParameter(jobid):
    try:
        cs_url = url + str(jobid) + '/conf'  # url为全局变量
        print cs_url
        r = requests.get(cs_url, auth=(user, psw))
        data = r.json()
        conf = data['conf']
        pro = conf['property']
        parameter_dir = {}
        for item in pro:
            if item['name'] in Wantedconf:
                value = item['value']
                parameter_dir[item['name']] = value
        print "get parameters ok"
        return parameter_dir
    except:
        print "network error when get parameters for jobid:", jobid

'''
功能：访问job history server,返回jobid列表，以及jobid对应的执行时间字典
'''
def getJobs():
    cs_url = url  # url为全局变量
    #print cs_url
    r = requests.get(cs_url, auth=(user, psw))
    data = r.json()
    #print data
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
    return jobid_list, jobid_excuteTime

def getCounters(jobid):
    try:
        cs_url = url + str(jobid) + '/counters'  # url为全局变量
        r = requests.get(cs_url, auth=(user, psw))
        data = r.json()
        jobCounters = data['jobCounters']
        counterGroup = jobCounters['counterGroup']
        d = {}
        for item in counterGroup:
            if item['counterGroupName'] == 'org.apache.hadoop.mapreduce.TaskCounter':
                counter = item['counter']
                if counter != 'None':
                    for c in counter:
                        if c['name'] == 'MAP_INPUT_RECORDS':
                            d['MAP_INPUT_RECORDS'] = c['totalCounterValue']
                        elif c['name'] == 'MAP_OUTPUT_RECORDS':
                            d['MAP_OUTPUT_RECORDS'] = c['totalCounterValue']
                        elif c['name'] == 'MAP_OUTPUT_BYTES':
                            d['MAP_OUTPUT_BYTES'] = c['totalCounterValue']
                        elif c['name'] == 'REDUCE_INPUT_RECORDS':
                            d['REDUCE_INPUT_RECORDS'] = c['totalCounterValue']
                        elif c['name'] == 'SPILLED_RECORDS':
                            d['SPILLED_RECORDS'] = c['totalCounterValue']
                        elif c['name'] == 'COMMITTED_HEAP_BYTES':
                            d['COMMITTED_HEAP_BYTES'] = c['totalCounterValue']
                        elif c['name'] == 'REDUCE_INPUT_RECORDS':
                            d['REDUCE_INPUT_RECORDS'] = c['totalCounterValue']
                        elif c['name'] == 'PHYSICAL_MEMORY_BYTES':
                            d['PHYSICAL_MEMORY_BYTES'] = c['totalCounterValue']
                            #print c['totalCounterValue'], 'total'
                            #print c['reduceCounterValue'], "reduce"
                            #print c['mapCounterValue'], "map"
    except:
        print "network error when get inputsize for jobid:", jobid
    return d

def getTime(jobid):
    cs_url = url + str(jobid)
    #print cs_url
    r = requests.get(cs_url, auth=(user, psw))
    data = r.json()
    d = {}
    job = data['job']
    avgreduce = job['avgReduceTime']
    avgmap = job['avgMapTime']
    avgShuffleTime = job['avgShuffleTime']
    avgMergeTime = job['avgMergeTime']
    mapsTotal = job['mapsTotal']
    d['avgReduceTime'] = avgreduce
    d['avgMapTime'] = avgmap
    d['avgShuffleTime'] = avgShuffleTime
    d['avgMergeTime'] = avgMergeTime
    d['mapsTotal'] = mapsTotal
    return d
def jobIsSuccessed(jobid):
     cs_url = url+str(jobid)
     r = requests.get(cs_url, auth=(user, psw))
     data = r.json()
     job = data['job']
     if job['state']!='SUCCEEDED':
         return False
     return True

def getNodeNum():
    '''
    r = requests.get('http://s18:8042/ws/v1/cluster/nodes', auth=(user, psw))
    data = r.json()
    nodes = data['nodes']
    node = nodes['node']
    #print node[0]
    return len(node)
    '''
    return 3
import expert
if __name__ == '__main__':
	
    print 'start calculating'
    jobid_list, jobid_excuteTime = getJobs()
    #print jobid_list
    job = jobid_list[-1]

    index = -1
    if jobIsSuccessed(job) == False:
	index -= 1
	job = jobid_list[index]
    conf = getParameter(job)
    counters = getCounters(job)
    print counters
    print "the configuration of last job:",conf
    time = getTime(job)
    nodenum = getNodeNum()
    recordsize = int(counters['MAP_OUTPUT_BYTES'] / counters['MAP_OUTPUT_RECORDS'])
    map_num = int(time['mapsTotal'])
    mapOutputBytes = int(counters['MAP_OUTPUT_BYTES'])
    spill_records = int(counters['SPILLED_RECORDS'])
    reduceInRecord = int(counters['REDUCE_INPUT_RECORDS'])

    iosortmb = int(expert.getIOSortMb(map_num, spill_records, recordsize,conf))
    
    #print mapOutputBytes,conf
    #print expert.getSortFactor(mapOutputBytes,conf)
    #print "**"

    sortfactors = expert.getSortFactor(mapOutputBytes,conf)
    reducenum = expert.getReduceNum(conf,nodenum)
    reducecopieds = expert.getReduceParallelCopies(mapOutputBytes,conf)
    sibp = expert.getShuffleInputBufferPercent(reduceInRecord,conf)
    smp = expert.getShuffleMergerPercent()

    '''
    print 'io.sort.tmb:',iosortmb
    print 'sort.factor:',sortfactors
    print 'mapreduce.reduce.num',reducenum
    print 'reduce.copies.num:',reducecopieds
    print 'shuffle input buffer percent:',sibp
    print 'shuffle merger percent:',smp
    '''
    print 'recommand you to use these settings:'
    p =  ' -D mapreduce.task.io.sort.mb='+str(iosortmb)
    p = p + ' -D mapreduce.task.io.sort.factor='+str(sortfactors)
    p = p + ' -D mapreduce.job.reduces='+str(reducenum)
    p = p + ' -D mapreduce.reduce.shuffle.parallelcopies='+str(reducecopieds)
    p = p + ' -D mapreduce.reduce.shuffle.input.buffer.percent='+str(sibp)
    #p = p + ' -D mapreduce.reduce.shuffle.memory.limit.percent='+str(smp)
    print p
    '''
    getIOSortMb(map_num, spill_records, recordsize, d)
    getSortFactor(mapOutputBytes, d, threshold=10000000)
    Compress(mapOutputBytes, d, threshold=10000000)
    getReduceNum(d, nodenum)
    getReduceParallelCopies(mapOutputBytes, d, threshold=10000000)
    getShuffleInputBufferPercent(reduceInRecord, d, threshold=1000000)
    getShuffleMergerPercent()
    
    '''


