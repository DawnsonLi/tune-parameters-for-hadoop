# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 11:00:14 2017

@author: dawnson
"""

'''
用户提交submit接口：用户使用此接口时，默认采用系统的自动参数调优工具
用户传入参数：原生的hadoop提交Jar包命令，例如：hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort input output
'''
'''
功能:用于解析用户提交的hadoop命令，例如 hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar terasort
接口要求：用户提交的格式为 hadoop jar jar包路径 函数名 输入目录  输出目录，中间用空格分开
'''
def parseHadoopJobCommand(command):
    print command
import sys  

if __name__ == "__main__":
    for arg in sys.argv:  
        print arg  
    print "over"