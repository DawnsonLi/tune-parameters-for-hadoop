use hadoop;
create table jobParameters(jobid char(100)  primary key,excuteTime char(100),SplitSize int,Parallelcopies int ,JVMReuse int ,Factor int ,SortMB int ,ShuffleMergePer float,ReduceInputPer float,SortPer float,ReduceNum int,ReduceTasksMax int ,ShuffleInputPer float,MapTasksMax int,ReduceSlowstart float,inMenMergeThreshold int ,ShufflelimitPer float,inputsize float,mapsMedian float,mapsStd float,reduceMedian float,reduceStd float);

