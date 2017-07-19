#coding=utf-8
'''
功能：测试数据产生，返回运行脚本run.sh
通过为挑选出的12个参数，并针对几类任务，分别运行25条程序来收集数据
'''
import random
'''
功能：产生用户指定个数的命令
输入：num:用户指定生产命令的个数
     Jarcommand:提交hadoop时的命令
     IOpath：定义输入输出
返回：生成的命令字符串
'''
def generate_random_command(num,Jarcommand,IOPath):
    ConfCombine = []
    #每次产生25组随机数据
    for i in range(num):
        confs = []
        MapTasksMax = random.randrange(2,203,20) #2 mapreduce.tasktracker.map.tasks.maximum  10 #there is no order here, but in optimize
        SplitSize = random.randrange(0,300,64)  #0 mapreduce.input.fileinputformat.split.minsize 5
        SortMB = random.randrange(100,1000,50)#100 mapreduce.task.io.sort.mb 18
        SortPer = 0.05 * random.randint(0,7) + 0.6  # 0.8 mapreduce.map.sort.spill.percent 8
        JVMReuse = random.randint(1, 10) #2 mapreduce.job.jvm.numtasks 10
        ReduceTasksMax = random.randrange(2,30,2)#2 mapreduce.tasktracker.reduce.tasks.maximum 14
        ShuffleMergePer = 0.05*random.randint(0,9)+0.5#0.66 mapreduce.reduce.shuffle.merge.percent 10
        ReduceSlowstart = 0.05*random.randint(0,7)  # 0.05 mapreduce.job.reduce.slowstart.completedmaps 8
        ReduceInputPer = 0.05*random.randint(0,9)#0.0 mapreduce.reduce.input.buffer.percent 10
        inMenMergeThreshold = random.randrange(100,2000,50)#1000 mapreduce.reduce.merge.inmem.threshold 38
        ShuffleInputPer = 0.05*random.randint(0,9)+0.5#0.7 mapreduce.reduce.shuffle.input.buffer.percent 10
        #OutputCompress = random.randint(0,1) # false mapreduce.map.output.compress
        Factor = random.randrange(5,51,5)# 10 mapreduce.task.io.sort.factor  10
        Parallelcopies = random.randrange(5,51,5)#5 mapreduce.reduce.shuffle.parallelcopies 10
        ReduceNum = random.randrange(5,51,5)# 1 mapreduce.job.reduces 10
        ShufflelimitPer = 0.05*random.randint(0,9)# 0.25 mapreduce.reduce.shuffle.memory.limit.percent 10
        #HeapOpts = 64*random.randrange()   # 1024 mapred.child.java.opts
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
        
        if confs in ConfCombine:#防止随机产生相同的数据
            i -= 1
            continue
        ConfCombine.append(confs)
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
    
        return Jarcommand+commandconfs+IOPath+str(i+1)+'\n'#生成run.sh脚本 加数字的原因在于防止输出文件夹相同

f = open('run.sh','w')
Jarcommand = 'hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort'
IOPath = ' terainput120G ts4120output'#定义IO的输入输出，注意开头有空格
f.write(generate_random_command(25,Jarcommand,IOPath))
#另一组不同类型命令
Jarcommand = 'hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort'
IOPath = ' terainput30G ts430output'
f.write(generate_random_command(25,Jarcommand,IOPath))
#新一组工作
Jarcommand = 'hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort'
IOPath = ' terainput60G ts460output'
f.write(generate_random_command(25,Jarcommand,IOPath))
#又一组
Jarcommand = 'hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort'
IOPath = ' terainput90G ts490output'
f.write(generate_random_command(25,Jarcommand,IOPath))
f.close()
print 'Command generate SUCCESS！（100）'
