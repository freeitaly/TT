# encoding: UTF-8

'''
本文件中包含的是CTA模块的回测引擎，回测引擎的API和CTA引擎一致，
可以使用和实盘相同的代码进行回测。
'''
from __future__ import division

from datetime import datetime, timedelta
from collections import OrderedDict
from itertools import product
import pymongo
from pymongo.errors import ConnectionFailure
import os
from ctaBase import *
from ctaSetting import *
from vtConstant import *
from vtGateway import VtOrderData, VtTradeData
from vtFunction import loadMongoSetting


########################################################################
class BacktestingCalcEngine(object):
    """
    CTA回测并计算引擎（比回测引擎添加了数据库插入，并功能简化）
    函数接口和策略引擎保持一样，
    从而实现同一套代码从回测到实盘。
    """

    TICK_MODE = 'tick'
    BAR_MODE = 'bar'

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""

        # 引擎类型为回测
        self.engineType = ENGINETYPE_BACKTESTING

        # 回测相关
        self.strategy = None        # 回测策略
        self.mode = self.BAR_MODE   # 回测模式，默认为K线


        self.dbClient = None        # 数据库客户端
        self.dbCursor = None        # 数据库指针

        #self.historyData = []       # 历史数据的列表，回测用
        self.initData = []          # 初始化用的数据
        #self.backtestingData = []   # 回测用的数据

        self.dbName = ''            # 回测数据库名
        self.symbol = ''            # 回测集合名

        self.dataStartDate = None       # 回测数据开始日期，datetime对象
        self.dataEndDate = None         # 回测数据结束日期，datetime对象
        self.strategyStartDate = None   # 策略启动日期（即前面的数据用于初始化），datetime对象


        self.logList = []               # 日志记录

        # 当前最新数据，用于模拟成交用
        self.tick = None
        self.bar = None
        self.dt = None      # 最新的时间

    #----------------------------------------------------------------------
    def dbConnect(self):
        """连接MongoDB数据库"""

        # 读取数据库配置的方法，已转移至vtFunction

        if not self.dbClient:
            # 读取MongoDB的设置
            settingFileName = "VT_setting.json"
            settingFileName = os.path.dirname(os.getcwd()) + "/" + settingFileName
            host, port, replicaset, readPreference, database, userID, password = loadMongoSetting(settingFileName)
            try:
                # self.dbClient = pymongo.MongoClient(host+':'+str(port), replicaset=replicaset,readPreference=readPreference)
                # db = self.dbClient[database]
                # db.authenticate(userID, password)
                self.dbClient = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=500)
                print u'MongoDB连接成功'
            except ConnectionFailure:
                print u'MongoDB连接失败'
            except ValueError:
                print u'MongoDB连接配置字段错误，请检查'
    #----------------------------------------------------------------------
    def setStartDate(self, startDate='20100416', initDays=10):
        """设置回测的启动日期"""
        self.dataStartDate = datetime.strptime(startDate, '%Y%m%d')

        initTimeDelta = timedelta(initDays)
        self.strategyStartDate = self.dataStartDate + initTimeDelta

    #----------------------------------------------------------------------
    def setEndDate(self, endDate=''):
        """设置回测的结束日期"""
        if endDate:
            self.dataEndDate= datetime.strptime(endDate, '%Y%m%d')

    #----------------------------------------------------------------------
    def setBacktestingMode(self, mode):
        """设置回测模式"""
        self.mode = mode

    #----------------------------------------------------------------------
    def setDatabase(self, dbName, symbol):
        """设置历史数据所用的数据库"""
        self.dbName = dbName
        self.symbol = symbol

    #----------------------------------------------------------------------
    def loadHistoryData(self):
        """载入历史数据"""
        # host, port = loadMongoSetting()
        #
        # self.dbClient = pymongo.MongoClient(host, port)

        self.dbConnect()

        collection = self.dbClient[self.dbName][self.symbol]

        self.output(u'开始载入数据')

        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = CtaBarData
            func = self.newBar
        else:
            dataClass = CtaTickData
            func = self.newTick

        # 载入初始化需要用的数据
        flt = {'datetime':{'$gte':self.dataStartDate,
                           '$lt':self.strategyStartDate}}
        initCursor = collection.find(flt)

        # 将数据从查询指针中读取出，并生成列表
        for d in initCursor:
            data = dataClass()
            data.__dict__ = d
            self.initData.append(data)

        # 载入回测数据
        if not self.dataEndDate:
            flt = {'datetime':{'$gte':self.strategyStartDate}}   # 数据过滤条件
        else:
            flt = {'datetime':{'$gte':self.strategyStartDate,
                               '$lte':self.dataEndDate}}
        self.dbCursor = collection.find(flt)

        self.output(u'载入完成，数据量：%s' %(initCursor.count() + self.dbCursor.count()))

    #----------------------------------------------------------------------
    def runBacktesting(self):
        """运行回测"""
        # 载入历史数据
        self.loadHistoryData()

        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = CtaBarData
            func = self.newBar
        else:
            dataClass = CtaTickData
            func = self.newTick

        self.output(u'开始回测')

        self.strategy.inited = True
        self.strategy.onInit()
        self.output(u'策略初始化完成')

        self.strategy.trading = True
        self.strategy.onStart()
        self.output(u'策略启动完成')

        self.output(u'开始回放数据')

        for d in self.dbCursor:
            data = dataClass()
            data.__dict__ = d
            func(data)

        self.output(u'数据回放结束')

    #----------------------------------------------------------------------
    def newBar(self, bar):
        """新的K线"""
        self.bar = bar
        self.dt = bar.datetime
        self.strategy.onBar(bar)    # 推送K线到策略中

    #----------------------------------------------------------------------
    def newTick(self, tick):
        """新的Tick"""
        self.tick = tick
        self.dt = tick.datetime
        self.strategy.onTick(tick)

    #----------------------------------------------------------------------
    def initStrategy(self, strategyClass, setting=None):
        """
        初始化策略
        setting是策略的参数设置，如果使用类中写好的默认设置则可以不传该参数
        """
        self.strategy = strategyClass(self, setting)
        self.strategy.name = self.strategy.className
    #----------------------------------------------------------------------
    def insertData(self, dbName, collectionName, data):
        """插入数据到数据库（这里的data可以是CtaTickData或者CtaBarData）"""
        # print data.__dict__
        self.dbInsert(dbName, collectionName, data.__dict__)
    #----------------------------------------------------------------------
    def dbInsert(self, dbName, collectionName, d):
        """向MongoDB中插入数据，d是具体数据"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.insert(d)
    #----------------------------------------------------------------------
    def loadBar(self, dbName, collectionName, startDate):
        """直接返回初始化数据列表中的Bar"""
        return self.initData
    #----------------------------------------------------------------------
    def loadCursor(self, dbName, collectionName, todayDate, days):
        """返回数据库查询Cursor，startDate是datetime对象"""
        todayDate = datetime.strptime(todayDate, "%Y%m%d")
        startDate = todayDate - timedelta(days)

        d = {"$and":[{'datetime':{'$gte':startDate}},{'datetime':{'$lte':todayDate}}]}
        cursor = self.dbQuery(dbName, collectionName, d)
        # print startDate, todayDate
        return cursor
    #----------------------------------------------------------------------
    def dbQuery(self, dbName, collectionName, d):
        """从MongoDB中读取数据，d是查询要求，返回的是数据库查询的指针"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            cursor = collection.find(d)
            return cursor
        else:
            return None

    #----------------------------------------------------------------------
    def loadTick(self, dbName, collectionName, startDate):
        """直接返回初始化数据列表中的Tick"""
        return self.initData

    #----------------------------------------------------------------------
    def writeCtaLog(self, content):
        """记录日志"""
        log = str(self.dt) + ' ' + content
        self.logList.append(log)

    #----------------------------------------------------------------------
    def output(self, content):
        """输出内容"""
        print str(datetime.now()) + "\t" + content

    #----------------------------------------------------------------------

    #----------------------------------------------------------------------
    def putStrategyEvent(self, name):
        """发送策略更新事件，回测中忽略"""
        pass



#----------------------------------------------------------------------
def formatNumber(n):
    """格式化数字到字符串"""
    n = round(n, 2)         # 保留两位小数
    return format(n, ',')   # 加上千分符




if __name__ == '__main__':
    # 以下内容是一段回测脚本的演示，用户可以根据自己的需求修改
    # 建议使用ipython notebook或者spyder来做回测
    # 同样可以在命令模式下进行回测（一行一行输入运行）
    from ctaDemo import *

    # 创建回测引擎
    engine = BacktestingEngine()

    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # 设置滑点
    engine.setSlippage(0.2)     # 股指1跳

    # 设置回测用的数据起始日期
    engine.setStartDate('20100416')

    # 载入历史数据到引擎中
    engine.setDatabase(MINUTE_DB_NAME, 'IF0000')

    # 设置产品相关参数
    engine.setSlippage(0.2)     # 股指1跳
    engine.setRate(0.3/10000)   # 万0.3
    engine.setSize(300)         # 股指合约大小

    # 在引擎中创建策略对象
    engine.initStrategy(DoubleEmaDemo, {})

    # 开始跑回测
    engine.runBacktesting()

    # 显示回测结果
    # spyder或者ipython notebook中运行时，会弹出盈亏曲线图
    # 直接在cmd中回测则只会打印一些回测数值
    engine.showBacktestingResult()
