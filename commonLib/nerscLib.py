from collections import Counter
import gc
import operator
import os
import pickle
import socket
import sys
import time

from commonLib import nerscPlot as pLib
from commonLib import nerscRadar as nr
from commonLib import timeLib
from commonLib.DBManager import DB
from commonLib.TaskRecord import TaskRecord
import filemanager as fm
import numpy as np
import scipy as sc
import scipy.cluster.vq as vq


def groupValuesByKeys(keys, values, upper_edges_bins):
    groups_dic = {}
    current_key = float(-1)
    current_key_index = -1
#    print keys, values, upper_edges_bins
    
    for (key, value) in sorted(zip(keys, values)): #.sort(key=lambda x: x[0]):
#        print key
        while (current_key==-1 or
            float(key) > current_key and
            (current_key_index < len(upper_edges_bins)-1)):
            current_key_index += 1;
            current_key = float(upper_edges_bins [current_key_index])
            groups_dic[current_key] = []
        if current_key_index == len(upper_edges_bins):
            break
        groups_dic[current_key].append(value)
        
    while current_key_index < len(upper_edges_bins)-1:
        current_key_index += 1
        current_key = float(upper_edges_bins [current_key_index])
        groups_dic[current_key] = []
    return groups_dic

def doMedianOnDic(in_dic):
    out_dic = {}
    for (key, values) in in_dic.iteritems():
        if len(values) > 0:
            val = np.median(values)
            if (val!=val):
                val = -1
        else:
            val=-1
        out_dic[key] = val    
    return out_dic



def dumpRecordsMoab(taskRecords, fileRoute, reassignClusters=False, hostname=None, maxCount=20000):
    centroids=None
    if (reassignClusters):
        route=nr.getCentroidsRoute(hostname)
        centroids=nr.getCentroids(route)
        print "Centroids loaded: "+str(centroids.shape[0])
        
        
    
    i=0
    fw=fm.openWriteFile(fileRoute)
    buffer=""
    for task in taskRecords:
        line=task.toMoabFormat(reassignClusters, centroids)
        buffer+=line+"\n"
        if i==maxCount:
            fw.write(buffer)
            buffer=""
            i=0
        else:
            i+=1
    if (buffer!=""):
        fw.write(buffer)
    fw.close()
            
        

def getDBInfo(forceLocal=False):
    hostname=socket.gethostname()
    user = os.getenv("NERSCDB_USER", "root")
    password = os.getenv("NERSCDB_PASS", "")
    
    if forceLocal:
        return "localhost", user, password, "3306"
    if not user or not password:
        print("Database connection requires env vars NERSCDB_USER and "
         "NERSCDB_PASS to be set... exiting!")
        exit(-1)
    if "underdog" in hostname:
        return "127.0.0.1", user, password, "3306"
    return "127.0.0.1", user, password, "5050"
    

def getEpoch(year, month=1, day=1):
    date_time = str(day)+"."+str(month)+"."+str(year)+" 00:00:00"
    pattern = '%d.%m.%Y %H:%M:%S'
    epoch = int(time.mktime(time.strptime(date_time, pattern)))
    return epoch

def getDB(host="localhost", dbName="nerc", userName="nersc", password="nersc", \
          port="3306", useTunnel=False):
    print dbName, port
    d= DB(host, dbName, userName, password, port=port, useTunnel=useTunnel)
    #d.connect()
    return d

# Functions around the summary table    
def parseFromSQL(dbName="nersc", hostname="", userName="nersc", password="nersc", dbHost="localhost", dbPort="3306", year=-1, month=-1, day=-1, \
                 endYear=-1, endMonth=-1, endDay=-1, timeAdjust=0, orderingField=None, filtered=False):
    print "Loading Records"
    items=[]
    db=getDB(dbName=dbName, userName=userName, password=password, host=dbHost)

    m=1
    d=1
    if( month!=-1 and year!=-1):
        m=month
    if ( month!=-1 and day!=-1):
        d=day;
    condition="True"
    
    if (year!=-1):
        startEpoch=getEpoch(year, m, d)+timeAdjust
        condition="start>="+str(startEpoch)
        print "START:"+str(startEpoch)
   # print condition
   
    if (endYear!=-1):
        em=1
        ed=1
        if (endMonth!=-1):
            em=endMonth
        if (endDay!=-1):
            ed=endDay
        endEpoch=getEpoch(endYear, em, ed)
        condition+=" and start<="+str(endEpoch)
        print "STOP:"+str(endEpoch)
    if filtered:
        condition+=" and filtered=False"
        print "Filtered TASKS"
    to=timeLib.getTS()
    if (hostname==""):
        rows=db.getValuesDicList("summary", TaskRecord.getFields(), condition=condition, orderBy=orderingField)
    else:
        rows=db.getValuesDicList("summary", TaskRecord.getFields(), condition=condition+" and "+"hostname='"+hostname+"'", orderBy=orderingField)
    print "Time to Retrieve SQL Records:"+str(timeLib.getFinalT(to))
    print "Records Loaded"
    print "Parsing Records"
    to=timeLib.getTS()
    for row in rows:
        items.append(TaskRecord().parseSQL(row))
    print "Records parsed: "+str(len(items))
    print "Time to Parse SQL Records:"+str(timeLib.getFinalT(to))
    rows=None
    gc.collect()
    return items

def getTimeStamp(year, month, day, timeAdjust=0, shiftStart=0):
    m=1
    d=1
    if( month!=-1 and year!=-1):
        m=month
    if ( month!=-1 and day!=-1):
        d=day;
    startEpoch=getEpoch(year, m, d)+timeAdjust-shiftStart
    return startEpoch
    

def parseFromSQL_LowMem(dbName="nersc", hostname="", userName="nersc", password="nersc", dbHost="localhost", dbPort="3306", year=-1, month=-1, day=-1, startEpoch=-1,\
                 endYear=-1, endMonth=-1, endDay=-1, endEpoch=-1, timeAdjust=0, orderingField=None, filtered=False, fieldDate="start",
                 shiftStart=0, condition="True", useTunnel=False):
    print "Loading Records", dbPort
    items=[]
    db=getDB(dbName=dbName, userName=userName, password=password, host=dbHost, port=dbPort, useTunnel=useTunnel)
    significantStart=startEpoch
    
    if (startEpoch==-1):
        m=1
        d=1
        if( month!=-1 and year!=-1):
            m=month
        if ( month!=-1 and day!=-1):
            d=day;
        #condition="True"
        
        if (year!=-1):
            startEpoch=getEpoch(year, m, d)+timeAdjust-shiftStart
            significantStart=startEpoch+shiftStart
            
    
    if (startEpoch!=-1):
        condition+=" AND "+fieldDate+">="+str(startEpoch)
        print "START:"+str(startEpoch)
   # print condition
    if (endEpoch==-1):
        if (endYear!=-1):
            em=1
            ed=1
            if (endMonth!=-1):
                em=endMonth
            if (endDay!=-1):
                ed=endDay
            endEpoch=getEpoch(endYear, em, ed)+timeAdjust
    
    if endEpoch!=-1:
        
        condition+=" and "+fieldDate+"<="+str(endEpoch)
        print "STOP:"+str(endEpoch)
    if filtered:
        condition+=" and filtered=False"
        print "Filtered TASKS"
    to=timeLib.getTS()
    if (hostname==""):
        cur=db.getValuesDicList_LowMem("summary", TaskRecord.getFields(), condition=condition, orderBy=orderingField)
    else:
        cur=db.getValuesDicList_LowMem("summary", TaskRecord.getFields(), condition=condition+" and "+"hostname='"+hostname+"'", orderBy=orderingField)
    print "Time to Retrieve SQL Records:"+str(timeLib.getFinalT(to))
    print "Records Loaded"
    print "Parsing Records"
    
    to=timeLib.getTS()
    end=False
    maxCount=10000
    while not end:
        rows=[]
        count=1

        row=cur.fetchone()
        while row!=None:
            rows.append(row)
            if (count>=maxCount):
                break
            row=cur.fetchone()
       
            count+=1
        
        end=row==None
        for row in rows:
            items.append(TaskRecord().parseSQL(row))
        
            
        
   
    print "Records parsed: "+str(len(items))
    print "Time to Parse SQL Records:"+str(timeLib.getFinalT(to))
    rows=None
    
    db.close_LowMem(cur)
    gc.collect()
    return items, significantStart




def insertIntoDB(db, listRecords):
    print "inserting Records"

    for record in listRecords:
       # print "record", record
        db.insertValues("summary", record.keys(), record.values())

def insertIntoDBMany(db, listRecords):
    print "inserting Records"

    db.insertValuesMany("summary", listRecords)        
        
        
        

def readFileAndInsertDB(fileName, hostname, dbName="custom", moabFormat=False):
    print "Opening:"+fileName
    f=fm.openReadFile(fileName)
    lines=f.readlines()
    
    stepMax=1000
    
    dbHost, dbUser, dbPass, dbPort=getDBInfo()
    
    #db=getDB(dbName=dbName)
    
    db=getDB(host=dbHost, dbName=dbName, userName=dbUser, password=dbPass, port=dbPort)
    parsedRecords=[]
    for line in lines:
        record=TaskRecord()
        if record.parsePartialLog(hostname, line, moabFormat=moabFormat):
            parsedRecords.append(record.valuesDic)
            #print "New Record: "+record.valuesDic["stepid"]
        if (len(parsedRecords)==stepMax):
            #print "Dumping "+str(len(parsedRecords))+" records on DB "+dbName
            insertIntoDBMany(db, parsedRecords)
            parsedRecords=[]
    if len(parsedRecords)>0:
        #print "Dumping "+str(len(parsedRecords))+" records on DB "+dbName
        insertIntoDBMany(db, parsedRecords)
    print 
    print "Dump done"
    f.close()
            
            
                        
        
    


def GetTasksPerJobWithNoFailure(rows):
    jobCount={}
    for row in rows:
        jobName=row.getVal("jobname")
        stepid=row.getVal("stepid")
        status=row.getVal("status")
        if (status<0):
            jobCount[jobName]=-1
        else:
            print jobName
            
            if jobName in jobCount:
                if (jobCount[jobName]>0):
                    jobCount[jobName]+=1
            else:
                jobCount[jobName]=1
#    for jobName in jobCount.keys():
#        if (jobCount[jobName]>1):
#            print jobName, jobCount[jobName]

            
    return jobCount



def getBin(duration, globalEdges):
    #print globalEdges, duration
    i=0
    for edge in globalEdges:
        if (i>99):
            return 99
        if duration<edge:

            return i
        i+=1
    return i-1


def createFieldDic(dataFields):
    outputDic={}
    for field in dataFields:
        outputDic[field]=[]
    return outputDic


def createDicDic(dataFields):
    outputDic={}
    for field in dataFields:
        outputDic[field]={}
    return outputDic

def createAccDic(dataFields):
    outputDic={}
    for field in dataFields:
        outputLists[field]=0
    return outputDic
    

def getValueFromRow(row, field):
    value=None
    if field=="duration":
        value=row.duration()
    elif field=="totalcores":
        if (row.getVal("hostname")=="hopper" or row.getVal("hostname")=="edison"):
            value=row.getVal("numnodes")*24#row.getVal("cores_per_node")
        else:
            value=row.getVal("numnodes")*row.getVal("cores_per_node")
    elif field=="totaltime":
        value=row.getVal("numnodes")*row.getVal("cores_per_node")*row.duration()
    elif field=="waittime":
        value=row.waitTime()
    else:
        value=row.getVal(field)
        
        
    
    return value

def getSelectedDataFromRows(rows, dataFields, queueFields=[], accFields=[]):
    print "Starting Data Selection"
    to=timeLib.getTS()
    
    outputDic=createFieldDic(dataFields)
    temporaryAcc=createAccDic(accFields)
    outputAcc=createFieldDic(accFields)
    queueDic=createDicDic(queueFields)
    queueGDic=createDicDic(queueFields)
    queues={}
    queuesG={}
    
    count=0
#    while (len(rows)>0):
#        count+=1
#        if (count==100000):
#            gc.collect()
#        row=rows.pop(0)
    for row in rows:
        currentQueue=row.getVal("class")
        currentGQueue=row.getVal("classG")
        if (not currentQueue in queues.keys()):
            queues[currentQueue]=0
        if (not currentGQueue in queuesG.keys()):
            queuesG[currentGQueue]=0
        queues[currentQueue]+=1
        queuesG[currentGQueue]+=1
 
        
        for field in dataFields:
            value=getValueFromRow(row, field)
            outputDic[field].append(value)
            if field in accFields:
                temporaryAcc[field]+=value
                outputAcc[field].append(value)
        
        for field in queueFields:
      
            fieldDic=queueDic[field]
            if (not currentQueue in fieldDic.keys()):
                fieldDic[currentQueue]=[]
            fieldDic[currentQueue].append(getValueFromRow(row, field))
            
          
            fieldDic=queueGDic[field]
            if (not currentGQueue in fieldDic.keys()):
                fieldDic[currentGQueue]=[]
            fieldDic[currentGQueue].append(getValueFromRow(row, field))
            
            
#        currentQueue=row.getVal("class")
#        if (not currentQueue in queueDic.keys()):
#            queueDic[currentQueue]=createFieldDic(queueFields)
#        for field in queueFields:
#            queueDic[currentQueue][field].append(getValueFromRow(row, field))
        
#        currentQueue=row.getVal("classG")
#        if (not currentQueue in queueGDic.keys()):
#            queueGDic[currentQueue]=createFieldDic(queueFields)
#        for field in queueFields:
#            queueGDic[currentQueue][field].append(getValueFromRow(row, field))
    print "Time to extract information:"+str(timeLib.getFinalT(to))
    return len(rows), outputDic, outputAcc, queues, queueDic, queuesG, queueGDic
            
                
                
          


def getDataFromRows(rows, globalEdges):
    taskSizes=[]
    taskNodes=[]
    #taskCorePerNode=[]
    taskCores=[]
    cpuTime=[]
    wallClock=[]
    classes=[]
    waitTime=[]
    
    classTaskDuration={}
    classTaskCPUTime={}
    classCores={}
    classTaskCPUTimeDistro={}
    classWaitTimeDistro={}
    
    taskMemory=[]
    taskVMemory=[]
    classTaskMemory={}
    classTaskVMemory={}
    
  
    
    specialCPUTimeAcumulator=np.zeros(100, dtype=float)
    
    classOfPoint=[]
    
    for row in rows:
        taskSizes.append(row.duration())
        taskNodes.append(row.getVal("numnodes"))
        
        if row.getVal("hostname") in ["hopper", "edison"]:
            numCoresPerNode=24
        else:
            numCoresPerNode=row.getVal("cores_per_node")
        taskCores.append(row.getVal("numnodes")*numCoresPerNode)
        cpuTime.append(row.getVal("numnodes")*numCoresPerNode*row.duration())
        wallClock.append(row.getVal("wallclock"))
        classes.append(row.getVal("class"))
        waitTime.append(row.waitTime())
        taskMemory.append(row.getVal("memory"))
        taskVMemory.append(row.getVal("vmemory"))
        
        specialCPUTimeAcumulator[getBin(row.duration(), globalEdges)]+=row.getVal("numnodes")*numCoresPerNode*row.duration()
        
        c=row.getVal("class")
        if not c in classTaskDuration.keys():
            classTaskDuration[c]=[]
            classTaskCPUTime[c]=0
            classCores[c]=[]
            classTaskCPUTimeDistro[c]=[]
            classWaitTimeDistro[c]=[]
            classTaskMemory[c]=[]
            classTaskVMemory[c]=[]
            
        classTaskDuration[c].append(row.duration())
        classTaskCPUTime[c]+=row.duration()
        classCores[c].append(row.getVal("numnodes")*numCoresPerNode)
        classTaskCPUTimeDistro[c].append(row.duration()*row.getVal("numnodes")*numCoresPerNode)
        classWaitTimeDistro[c].append(row.waitTime())
        classTaskMemory[c].append(row.getVal("memory"))
        classTaskVMemory[c].append(row.getVal("vmemory"))
        #taskCorePerNode.append(row.getVal("cores_per_node"))
        classOfPoint=classes
    classes=Counter(classes)
    return taskSizes,taskNodes, taskCores, cpuTime, wallClock, classes, classTaskDuration, classTaskCPUTime, classCores, classTaskCPUTimeDistro, specialCPUTimeAcumulator, \
        waitTime, classWaitTimeDistro, taskMemory, taskVMemory, classTaskMemory, classTaskVMemory, classOfPoint 

    
def getCompareDics(dbName, hostname, dbHost, userName, password, year, month=1, endYear=-1, endMonth=-1):
    rows=parseFromSQL(dbName=dbName, hostname=hostname, dbHost=dbHost, userName=userName, \
                      password=password, \
                    year=year, month=month, endYear=endYear, endMonth=endMonth)

    taskSizes,taskNodes, taskCores, cpuTime, wallClock, classes, classTaskDuration, \
        classTaskCPUTime, classCores, classTaskCPUTimeDistro, specialCPUTimeAcumulator, \
        waitTime, classWaitTimeDistro,taskMemory, taskVMemory, \
        classTaskMemory, classTaskVMemory, classOfPoint =getDataFromRows(rows, "")
    return [wallClock, taskCores, taskMemory, classOfPoint], [classCores, classTaskCPUTimeDistro, classTaskDuration, classTaskMemory, classTaskVMemory]
def fuseDics(old, new, prefix):
    for key in new.keys():
        old[prefix+"-"+key]=new[key]
    return old
def fuseLists(old, new, prefix):
    newL=[]
    for l1, l2 in zip(old, new):
        if len(l2)>0 and type(l2[0])==str:
            l=l1+[]
            for e in l2:
                l.append(prefix+"-"+e) 
            newL.append(l)
        else:
            newL.append(l1+l2)
    return newL

def getFusedDics(dbName, hostnames, dbHost, userName, password, year, month=1, endYear=-1, endMonth=-1):
    allDics=""
    allLists=""
    for hostname in hostnames:
        newLists, newDics=getCompareDics(dbName=dbName, hostname=hostname, dbHost=dbHost, \
                                         userName=userName, password=password, \
                                        year=year, month=month, endYear=endYear, endMonth=endMonth)
        if allLists=="":
            allLists=newLists
        else:
            allLists=fuseLists(allLists, newLists, hostname[0:2])
        
        
        finalDics=[]
        prefix=hostname[0:2]
        if allDics=="":
            allDics=[]
            for i in range(len(newDics)):
                allDics.append({})
        for (old, new) in zip(allDics, newDics):
            finalDics.append(fuseDics(old, new, prefix))
        allDics=finalDics
        
    return allLists, allDics, allDics[0].keys()

def dumpClusters(fileName, centroids, associations, points, queuePerPoint, classDuration, classCores):
    np.save(fileName+"-cent", centroids)
    np.save(fileName+"-asso", associations)
    np.save(fileName+"-points", points)
    np.save(fileName+"-qasso", queuePerPoint)
    
    save_obj(fileName+"-qDura.dict", classDuration)
    save_obj(fileName+"-qCores.dict", classCores)
    
def loadClusters(fileName):
    ext=".npy"
    centroids=np.load(fileName+"-cent"+ext)
    associations=np.load(fileName+"-asso"+ext)
    points=np.load(fileName+"-points"+ext)
    queuePerPoint=np.load(fileName+"-qasso"+ext)
    
    classDuration=load_obj(fileName+"-qDura.dict")
    classCores=load_obj(fileName+"-qCores.dict")
    
    return centroids, associations, points,queuePerPoint, classDuration, classCores

def save_obj(name, obj):
    with open(name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name, 'r') as f:
        return pickle.load(f)

def getSeed(forced=None):
    if (forced==None):
        np.random.seed()
        return np.random.randint(0, sys.maxint)
    else:
        return forced
    

def initSeed(seed=None):
    np.random.seed(seed)
    
#print ge

#rows=parseFromSQL()

#taskSizes,taskNodes, taskCores, cpuTime, wallClock, classes, classTaskDuration, classTaskCPUTime, classCores, classTaskCPUTimeDistro, specialCPUTimeAcumulator, \
#    waitTime, classWaitTimeDistro =getDataFromRows(rows, ge)
#dir="Charts"
#pLib.paintHistogram("Task Duration distribution \N (#Jobs in 100 bins)", taskSizes, bins=100, dir=dir, graphFileName="DurationDistTask", labelX="Task Duration (s)", labelY="#Tasks", logScale=True, special=specialCPUTimeAcumulator)
#pLib.paintHistogram("Cores per Task distribution \N (#Jobs in 100 bins)", taskCores, bins=100, dir=dir, graphFileName="CoresDistTasks", labelX="#Cores/Task", labelY="#Tasks", logScale=True)
#pLib.paintHistogram("CPU TIME per Task distribution \N (#Jobs in 100 bins)", cpuTime, bins=100, dir=dir, graphFileName="CPUDistTasks", labelX="CPU Time(s)", labelY="#Tasks", logScale=True)
##print (classes)
#pLib.paintHistogram("WAIT TIME per Task distribution \N (#Jobs in 100 bins)", cpuTime, bins=100, dir=dir, graphFileName="WaitTimeTasks", labelX="Wait Time(s)", labelY="#Tasks", logScale=True)

#pLib.paintBars("Tasks per Class \N (#Jobs)",classes, dir=dir, graphFileName="TasksPerClass.png", labelX="Class", labelY="#Tasks", logScale=True)

#pLib.paintBoxPlot("Task Duration per Class", classTaskDuration, labelX="Class", labelY="Task Duration (s)", dir=dir, graphFileName="TaskDurationBoxPlotClass")
#pLib.paintBars("CPU Time per Class \N (#Jobs)",classTaskCPUTime, dir=dir, graphFileName="CPUTimePerClass", labelX="Class", labelY="CPUTime(s)", logScale=True)
#pLib.paintBoxPlot("#Cores per task in classes", classCores, labelX="Class", labelY="#cores", dir=dir, graphFileName="CoresPerTaskBoxPlotClass")
#pLib.paintBoxPlot("#CPUTime per task in classes", classTaskCPUTimeDistro, labelX="Class", labelY="CPU Time(s)", dir=dir, graphFileName="CPUTimePerTaskBoxPlotClass")

#pLib.paintBoxPlot("WaitTime per task in classes", classWaitTimeDistro, labelX="Class", labelY="Wait Time(s)", dir=dir, graphFileName="WaitTimePerTaskBoxPlotClass")


    
#pLib.showAll()
    
    

        
    