## 项目简介 <br>
实际使用分布式系统hadoop提交任务时，提交任务的参数对于任务的表现性能具有一定的影响，本项目研究如何自动化优化与配置这些参数 <br>
这里给出了两种解决思路：<br>
(1) 基于大量的历史数据，使用机器学习的方法进行参数自动化配置<br>
(2) 基于hadoop任务参数配置的经验规则进行参数自动化配置，主要针对仅运行过一次或较少次数的任务<br>
在hadoop自动化参数调优工作的基础上，对hive进行hadoop参数方面的调优工作 <br>
## 功能<br>
具有完整的数据收集、存储、分析、模型构建、空间搜索与优化模块<br>
拥有完备的信息，可以辅助用于hadoop历史任务的评价<br>
给出了明确的开源解决方案，用户接口简单清晰，十分利于使用<br>
给出了对hive应用hadoop mapreduce参数设置进行调优的实际方案<br>
通过对hadoop专家知识的编码，支持对任务一次运行后的优化<br>

## 部署环境<br>
1. 安装环境依赖<br>
(1)python hdfs包<br>
linux下安装示例：pip install hdfs<br>
(2)sklearn机器学习包<br>
linux下安装示例：pip install sklearn<br>
(3)python mysql包<br>
linux下安装示例：pip install mysql-python<br>
2. 开启hadoop历史服务器，nodemanager rest api（使用经验调优工具时要开启用于获取节点个数）。<br>
3. 查看hadoop端口配置文件，修改python文件中（tuneparameters.py及deamon.py）中历史服务器和nodemanager的端口和地址。<br>
4. 配置mysql用户权限。<br>
5. 创建mysql数据库和表结构，具体sql语句在sql文件夹下；创建数据库hadoop，创建表jobsubmit和表jobParameters。<br>
## 相关使用说明：<br>
根据实际情况选择运行tune-parameters.py、increase.py、deamon.py<br>
这四个文件相互独立，运行情况如下：<br>
(1)tune_parameters.py用于使用机器学习方法基于历史数据调优。<br>
(2)increase.py用于基于规则对刚刚运行完的任务进行调优。<br>
(3)deamon.py用于定时获取历史job相关信息。<br>

## 实现方法概述：<br>
在构建预测回归模型时，增加了特征NlogN，N为输入数据大小(因为hadoop存在多个排序过程)，单模型使用extratree回归,复合模型使用Stacking的方案。 <br>
参数空间搜索，使用模拟退火算法。<br>

## hive优化思路：<br>
通过对hive产生的hadoop job进行参数调优，优化hive任务。

## 其他问题：<br>
### 历史数据机器学习模型的冷启动
采用配置参数随机化积累历史数据
### 优化不同任务
当出现一个之前没有运行过的任务，可以使用基于规则经验的方法进行优化。那么如何运用基于历史数据的机器学习调优方法？<br>
定义了hadoop任务相似度量，度量内容包括：<br>
(1)map输入记录总量与输出记录总量比值(转化比)<br>
(2)map输出字节与输出记录总量比值（刻画记录特征）<br>
(3)每条记录花费的map时间(刻画map复杂度)<br>
(4)每条输入reduce的记录花费的reduce时间（刻画reduce复杂度）<br>


