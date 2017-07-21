# -*- coding:utf-8 -*-  
import MySQLdb 
import numpy as np
from sklearn.tree import ExtraTreeRegressor

'''
@功能：独立的用于构建模型的模块
@实现：从数据库中读取数据，使用一定的规则选取相应的数据，构建预测模型
'''

'''
@功能：从数据库中获取数据
@输入SQL语句
@返回:训练数据
'''
def SQLTrainData(sql):
    db = MySQLdb.connect("localhost","root","2345","hadoop" )
    try:
        cursor = db.cursor()
        cursor.execute(sql)
        d = cursor.fetchall()#取出符合要求的数据 
        data = np.array(d)
    except:
        print "erros occur when fetching data from the database"

    if len(data) < 1:#无返回结果
        return [],[]

    Target = data[:,1]#终极预测目标
    feature = data[:,2:18]#第一层属性
    inputsize = feature[:,-1]
    inputsize = inputsize.astype(np.float)    
    feature = np.column_stack((feature,inputsize*np.log(inputsize)))#增加特征N*logN
    return feature,Target

'''
功能：构建模型,没有验证参数的环节，单层模型
'''
from sklearn.externals import joblib
def getModel(x,y):
    et = ExtraTreeRegressor()
    et.fit(x,y)
    #joblib.dump(et,'./model/et')#保存模型
    return et
