# -*- coding:utf-8 -*-  
import MySQLdb 
import numpy as np
from sklearn.tree import ExtraTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.neighbors import NearestNeighbors
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
sql = ''
Features_list,Target_list = SQLTrainData(sql)
from sklearn import cross_validation
from sklearn.cross_validation import KFold

train, test,y_train, y_test= cross_validation.train_test_split(Features_list,Target_list,test_size=0.3, random_state=2017)
    
ntrain = train.shape[0]
ntest = test.shape[0]
SEED = 0 # for reproducibility
NFOLDS = 5 # set folds for out-of-fold prediction
kf = KFold(ntrain, n_folds= NFOLDS, random_state=SEED)

# Class to extend the Sklearn classifier
class SklearnHelper(object):
    def __init__(self, clf, seed=0, params=None):
        params['random_state'] = seed
        self.clf = clf(**params)

    def train(self, x_train, y_train):
        self.clf.fit(x_train, y_train)

    def predict(self, x):
        return self.clf.predict(x)
    
    def fit(self,x,y):
        return self.clf.fit(x,y)
    
    def feature_importances(self,x,y):
        print(self.clf.fit(x,y).feature_importances_)
        
def get_oof(clf, x_train, y_train, x_test):
    oof_train = np.zeros((ntrain,))
    oof_test = np.zeros((ntest,))
    oof_test_skf = np.empty((NFOLDS, ntest))
    for i, (train_index, test_index) in enumerate(kf):
        x_tr = x_train[train_index]
        y_tr = y_train[train_index]
#获取训练数据中的4折用于训练模型
        x_te = x_train[test_index]#剩余一折用来预测
        clf.train(x_tr, y_tr)
        oof_train[test_index] = clf.predict(x_te)#	训练数据的一折（剩余4折用于训练模型）#从而5次迭代后，对全部训练数据都进行了预测。
        oof_test_skf[i, :] = clf.predict(x_test)#	全部的测试数据
    oof_test[:] = oof_test_skf.mean(axis=0)#每个模型对测试数据预测了5次，取平均数
    return oof_train.reshape(-1, 1), oof_test.reshape(-1, 1)

et = ExtraTreeRegressor()
rr = RandomForestRegressor()
NN  = NearestNeighbors()
x_train = train
x_test = test
et_oof_train, et_oof_test = get_oof(et, x_train, y_train, x_test) # Extra Trees
rr_oof_train,rr_oof_test = get_oof(rr, x_train, y_train, x_test)
nn_oof_train,nn_oof_test = get_oof(NN,x_train,y_train,x_test)

x_train = np.concatenate(( et_oof_train, rr_oof_train, nn_oof_train), axis=1)
x_test = np.concatenate(( et_oof_test, rr_oof_test, nn_oof_test), axis=1)

gb = GradientBoostingRegressor().fit(x_train, y_train)
predictions = gb.predict(x_test)

