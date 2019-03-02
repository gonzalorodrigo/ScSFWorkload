import numpy as np


def cleanWallClocks(wc, req):
    wcN=[]
    reqN=[]
    for (w,r) in zip(wc, req):
        if (w>10 and r >10 ):
            wcN.append(w)
            reqN.append(r)
    return wcN, reqN


def calculateAccuracy(wc, req):
    taskTimeProp=[100*float(s)/float(w+1) for s,w in zip(wc, req)]
    return taskTimeProp
    

def normalize(varList, maxVal=None):
    if maxVal==None:
        maxVal=max(varList)
    maxVal=float(maxVal)
    return [float(x)/maxVal for x in varList]

def calculatePerson(varList):
   # print varList
    vec = np.array(varList, dtype=float)
    print vec.shape
   
    return np.corrcoef(vec)
    
def cleanTimeStamps(stamps, period=1):
    st0=stamps[0]
    return [float(st-st0)/float(period) for st in stamps]

def doKeyCalc(values, bins=100):
    hist, edges=np.histogram(values, bins=bins, normed=False)
    edges=edges[0:-1]
    cdf=np.cumsum(hist, dtype=float)
    cdf/=cdf[-1]
    return edges, hist, cdf


def genDateTag(elements):
    return "-".join([str(i) for i in elements])

def resKeys():
    return ["edges", "hist", "CDF"]

class ResultsStore:
    
    
    def __init__(self, hostnames=["host"]):
        self.resultsDic={}
        self.keyStore={}
        self.hostnames=hostnames
    
    def createResult(self, name, keys=["edges", "hist", "CDF"]):
        self.keyStore[name]=keys
        self.resultsDic[name]=ResultsStore.createBaseDic(keys, self.hostnames)
    
    def regResult(self,name, hostname, listR, dateKey="date"):
        if type(listR) is dict:
            self.regResultDic(name, hostname, listR, dateKey=dateKey)
        else:
            print 
            #print self.keyStore[name]
            #print listR
            for (key, res) in zip(self.keyStore[name], listR):
                self.resultsDic[name][key][hostname][dateKey]=res
    
    def regResultDic(self,name, hostname, dicData, dateKey="date"):
        #print "DICREgistered", name, dateKey, dicData
#        print listR
        for key in self.keyStore[name]:
            self.resultsDic[name][key][hostname][dateKey]=dicData[key]
            
            
    def getResult(self, name, key, flatten=False):
        if not flatten:
            return  self.resultsDic[name][key]
        else:
            dicD={}
            dicO=self.resultsDic[name][key]
            for host in dicO.keys():
                for date in dicO[host].keys():
                    dicD[host+" "+date]=dicO[host][date]
            return dicD
                

    def getResultHostDate(self, name, key, hostname, dateKey="date"):
        return  self.resultsDic[name][key][hostname][dateKey]
    
    def getResultHost(self, name, key, hostname, flatten=False):
        if not flatten:
            return  self.resultsDic[name][key][hostname]
        else:
            dicD={}
            dicO=resultsDic[name][key][hostname]
            for k in dicO.keys():
                dicD[hostname+" "+k]=dicO[k]
            return dicD
            
    
    def getKeys(self, name):
        return self.keyStore[name]
    @classmethod
    def createBaseDic(self, keys, hostnames):
        dic={}
        for k in keys:
            dic[k]={}
            for h in hostnames:
                dic[k][h]={}
        return dic
        
    