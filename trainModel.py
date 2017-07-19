# -*- coding:utf-8 -*-  
import MySQLdb 
import numpy as np
from sklearn.tree import ExtraTreeRegressor
'''
功能：独立的用于构建模型的模块
实现：从数据库中读取数据，使用一定的规则选取相应的数据，构建预测模型
'''

'''
功能：从数据库中获取数据，可以使用过滤条件filterstr，用于生成相应的SQL语句
'''
def getTrainData(filterstr = ''):
    db = MySQLdb.connect("localhost","root","2345","hadoop" )
    cursor = db.cursor()
    sql = 'select * from jobParameters'
    if filterstr!='':
        sql += 'where'
        sql += filterstr
    cursor.execute(sql)
    d = cursor.fetchall()#取出符合要求的数据 
    data =  np.array(d)
    level2Target = data[:,1]#终极预测目标
    level1feature = data[:,2:18]#第一层参数
    
    return level1feature,level2Target


'''
功能：构建模型,没有验证参数的环节
'''
from sklearn.externals import joblib
def getModel(x,y):
    et = ExtraTreeRegressor()
    et.fit(x,y)
    #joblib.dump(et,'./model/et')#保存模型
    return et


