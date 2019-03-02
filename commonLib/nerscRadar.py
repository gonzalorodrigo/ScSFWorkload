from commonLib.nerscLib import *
import sys
import filemanager as fm
import numpy as np
from numpy.random import * 
from commonLib import nerscPlot
import scipy.cluster.vq as vq
#from scipy.spatial import Voronoi, voronoi_plot_2d
from scipy.stats import scoreatpercentile
from commonLib import timeLib

from commonLib import kmeans
import cmath

def toPolar(p, center):
    
    #print np.array(p), center
    p2=np.array(p)-center
    (r, phi)=cmath.polar(complex(p2[0], p2[1]))
    newP={}
    newP["polar"]=(r, phi)
    newP["cart"]=p
    return newP



def getPerimeterData(points, center):
    pPoints=[]
    maxR=0
    for p in points:
        p2=np.array(p)-center
        m=np.linalg.norm(p)
        maxR=max(maxR, m)
    return {"center":center, "r":maxR}
           
        

    
    
    
        

def calculateDistance(dataMatrix):
    colCount=dataMatrix.shape[1]
    rowCount=dataMatrix.shape[0]
    outArray=np.zeros((rowCount, rowCount, colCount),dtype=float)
    for i1 in range(rowCount):
        for i2 in range(0, rowCount):
            for j in range(colCount):
                outArray[i1, i2, j]= abs(dataMatrix[i1, j]-dataMatrix[i2,j])
    
    # In out array we have the between each queue for each variable
    return outArray

def areInBounds(dataMatrix, bound):
    rowCount=dataMatrix.shape[0]
    outArray=np.zeros((rowCount, rowCount))
    for i1 in range(rowCount):
        for i2 in range(0, rowCount):
            if (i1!=i2):
                if max(dataMatrix[i1, i2])<=bound:
                    outArray[i1, i2]=1
    return outArray

def makeGroups(dataMatrix):
    groups=[]
    rowCount=dataMatrix.shape[0]
    touched=np.zeros(rowCount)
    
    for i in range(rowCount):
        groupI=[i]
        for j in range(0, rowCount):
             if dataMatrix[i,j]==1:
                 groupI.append(j)
        groups.append(groupI)
#    print groups
    newGroups=[]
    for i in range(len(groups)):
        if (touched[i]==0):
            groupI=groups[i]
            ei=1
            while ei < len(groupI):
                relatedIndex=groupI[ei]
                groupD=groups[relatedIndex]
                for candidateToBeAdded in groupD:
                    touched[candidateToBeAdded]=1
                    if candidateToBeAdded not in groupI:
                        groupI.append(candidateToBeAdded)
                ei+=1
            newGroups.append(groupI)
#    print newGroups
    return newGroups


            

values=[[0.1, 0.2, 0.3], \
        [0.0, 0.4, 0.3], \
        [0.2, 0.1, 0.2], \
        [0.0, 0.6, 0.5]]
def getGroupsFromVars(values, maxDistance):

    #print values

    distances=calculateDistance(np.array(values))

    #for i in range(distances.shape[0]):
    #    print distances[i]

    bounds=areInBounds(distances, maxDistance)
    #print "BOUND MATRIX", maxDistance
    #print bounds

    groups=makeGroups(bounds)


    return groups

def changeIndexForLabel(groups, labels):
    newGroups=[]
    for g in groups:
        newG=[]
        for i in g:
            newG.append(labels[i])
        newGroups.append(newG)
    return newGroups
def formatGroups(groups):
    cad=""
    for group in groups:
        cad+=",".join(group)+"\n"
    return cad
        

def dicToArr(dicList, queueNames, logScale):
    #each row on DicList is one of the variables... they have to become a comlum.
    
    arr=[]
    for dic in dicList:
        subL=[]
        for queue in queueNames:
            if (logScale):
                subL.append(np.log(float(np.median(dic[queue]))))
            else:
                subL.append(float(np.median(dic[queue])))
        subL=np.array(subL, dtype=float)
        maxVal=max(subL)
        subL/=maxVal
        
        arr.append(subL)
    return np.transpose(np.array(arr, dtype=float))
    # here each 
    #return np.transpose(np.array(arr))
def doMultivarCompare(hostname, variableDictionaries, varNames, queueNames, maxDistance, logScale=False, dir="./"):

    print "Comparing Vars ("+",".join(varNames)+"), maxDistance="+str(maxDistance)
    data=dicToArr(variableDictionaries, queueNames, logScale)
    #print data.shape
    subName=""
    for word in queueNames:
        subName+=word[0]
    subName+="-"+str(len(varNames))
#    print queueNames
#    print varNames
#    print data
    groups=getGroupsFromVars(data, maxDistance)

    groupsNames=changeIndexForLabel(groups, queueNames)
#    print groups
#    print groupsNames

    nerscPlot.paintStarGraph(hostname+"-Taks on Queues Median Compare", data, varNames, queueNames, dir=dir, graphFileName="QueueStarCompare."+subName)
    return groupsNames
def isInList(list, e):
    for a in list:
        #print a
        #print e
        if np.array_equal(a,e):
            return True
    return False
def getRandomSamples(ar, number):
    size=len(ar)
    out=[]
    for i in range(number):
        index=int(np.random.randint(0, size))
        #print index
        val=np.array(ar[int(np.random.randint(0,size))])
        #print val
        while isInList(out, val):
            val=np.array(ar[int(np.random.randint(0,size))])
        out.append(val)
    #out=sorted(out)
    return np.array(out)
    
def whiten(inputPoints, normalized=True, weights=None):
    p=np.copy(inputPoints)
    if weights==None:
        weights=np.ones(inputPoints.shape[1], dtype=float)
    #print "Before Normalization", p
    if (normalized):
        for j in range(inputPoints.shape[1]):
            #print inputPoints[:,j]
            max=np.amax(inputPoints[:,j])
            min=np.amin(inputPoints[:,j])
            p[:,j]=(inputPoints[:,j]-min)/(max-min)*weights[j]
           # print (inputPoints[:,j]-min)/(max-min)*weights[j]
    #print "After Normalization", p
    return vq.whiten(p), p

def trimData(inputPoints, per):
    print "Shape", inputPoints.shape
    removeIndex=[]
    for j in range(inputPoints.shape[1]):
        column=inputPoints[:,j]
        #print "C", column.shape
        score=scoreatpercentile(column, per[j])
        #print "Score", j, score
        removeIndex+=np.where(column>score)[0].tolist()
        #print removeIndex
    #print removeIndex
    #for index in inputPoints:
    inputPoints=np.delete(inputPoints, removeIndex, axis=0)
    return inputPoints
    
        
    

def inverseWhiten(inputPoints, reference, normalizedReference, normalized=True, weights=None):
    #print "InputPoints", inputPoints
    if weights==None:
        weights=np.ones(inputPoints.shape[1], dtype=float)
    
    newPoints=np.copy(inputPoints)
    if (not normalized):
        normalizedReference=reference
    
    for j in range(normalizedReference.shape[1]):
        st=np.std(normalizedReference[:,j])
        newPoints[:,j]=newPoints[:,j]*st
#        for i in range(inputPoints.shape[0]):
#            newPoints[i,j]=inputPoints[i,j]*st
    #print "After DE-white", newPoints
    if (normalized):
        for j in range(reference.shape[1]):
            max=np.amax(reference[:,j])
        #    print max
            min=np.amin(reference[:,j])
        #    print min
            newPoints[:,j]=((newPoints[:,j]/weights[j])*(max-min))+min
    #print "After Denormalization", newPoints
    return newPoints
    
    
# this method uses the old centroids to generate the newsONes
def doKMeansSearchV2(wallClock, taskCores, numberOfRegions, numberOfRepeats, oneDimentionCV=False, \
                     cvBound=1.1, maxRegions=1000, normalized=False, weights=None, \
                         percentileCut=None, reduced=False, minSizeFound=None, numberOfTrials=10):
    found=False
    dataDic={}
    numberOfRegions=5
    searchedNumbers=[]
    partialFound=False
    dicotomicSearch=False
    min=0
    max=0
    maxRegionsAchieved=False
    currentNumberOfRegions=5
    numberOfRepeats=100
    
    inputPoints=np.transpose(np.array([wallClock, taskCores], dtype=float))
    #print "#Points before", inputPoints.shape[0]
    if percentileCut!=None:
        inputPoints=trimData(inputPoints, percentileCut)
    #print "#Points after", inputPoints.shape[0]
    #inputPointsWhiten=vq.whiten(inputPoints)
    inputPointsWhiten, inputPointsNormalized=whiten(inputPoints, normalized, weights)
    #print "usedPoints", inputPointsWhiten
    
    
    if reduced:
        numberOfTrials=20
    for i in range(numberOfTrials):
        if not found:
            numberOfRegions=currentNumberOfRegions
            repeats = numberOfRepeats
            inputCentroids=None
            if (maxRegionsAchieved):
                numberOfRegions=maxRegions

                
        while repeats > 0 and not found:
            if (inputCentroids!=None):
                numberOfRegions=len(inputCentroids)
            
                if (minSizeFound!=None):
                    if numberOfRegions>=minSizeFound:
                        print "We already found a better soultion before-Inner"
                        break
                    else:
                        print "This solution is still better that anything else found-Inner"
              
            
            print "Searching for the number of required regions. Testing: "+str(numberOfRegions)
            to=timeLib.getTS()
            centroids2, overalDistortion, points, association, dist = \
                doKMeansOnDataV2(inputPointsWhiten,numberOfRegions, False, inputCentroids=inputCentroids, reduced=reduced)
            print "Time to do one K-means process:"+str(timeLib.getFinalT(to))
            #print "First analysis, Wallclock"
            #print centroids2
            #print str(labels2)
            #print association

            #print "K-Means analysis done, obtained distortion: "+str(dist)
            if maxRegionsAchieved:
                found=True
          
            if (oneDimentionCV):
                found=withinBoundCV(centroids2, points, association, cvBound)
            else:
                meanList, stdevList, cvList= calculateCVForClusters(centroids2, points, association)
            #print "Centroid:", centroids2
                print "CV:", cvList
                outCentroids=[]
                found=True
                inputCentroids=np.empty((0,2))
                for cv, cPoint in zip(cvList, centroids2):
                    found = found and (cv <=cvBound)
                    if (cv>cvBound):
                        inputCentroids=np.vstack((inputCentroids, cPoint+cPoint*cv/2.0))
                        inputCentroids=np.vstack((inputCentroids, np.maximum(cPoint-cPoint*cv/2.0, np.zeros((1,2)))))
                        
                    else:
                        inputCentroids=np.vstack((inputCentroids, cPoint))
                #print "Before", centroids2
                #print "After", inputCentroids
                #inputCentroids=np.asarray(inputCentroids)
                #print centroids2
                #print outCentroids
                #if (not found):
                #    inputCentroids=np.vstack((centroids2,outCentroids))
                #print inputCentroids
            
            if maxRegionsAchieved:
                found=True
                    
            repeats-=1
        # it wasnt found
        
        if (found):
            print "Found Points!"
            dataDic[numberOfRegions]= [centroids2, overalDistortion, points, association, dist]
            break
    if (found):    
        centroids2, overalDistortion, points, association, dist=dataDic[numberOfRegions]
        print "in the end:"+str(len(dataDic.keys()))
    return numberOfRegions, inverseWhiten(centroids2, inputPoints, \
                    inputPointsNormalized, normalized, weights), overalDistortion, \
                    inputPoints, association, dist, found
    

def doKMeansSearch(wallClock, taskCores, numberOfRegions, numberOfRepeats, oneDimentionCV=False, cvBound=1.1, maxRegions=1000):
    found=False
    dataDic={}
    numberOfRegions=5
    searchedNumbers=[]
    partialFound=False
    dicotomicSearch=False
    min=0
    max=0
    maxRegionsAchieved=False
    
    while not found:
        repeats = numberOfRepeats
        
        if (maxRegionsAchieved):
            numberOfRegions=maxRegions
        searchedNumbers.append(numberOfRegions)
        while repeats > 0 and not found:
            
            print "Searchinf for the number of required regions. Testing: "+str(numberOfRegions)
            centroids2, overalDistortion, points, association, dist = doKMeansOnData([wallClock, taskCores],numberOfRegions, False)

            #print "First analysis, Wallclock"
            #print centroids2
            #print str(labels2)
            #print association

            #print "K-Means analysis done, obtained distortion: "+str(dist)
            if maxRegionsAchieved:
                found=True
          
            if (oneDimentionCV):
                found=withinBoundCV(centroids2, points, association, cvBound)
            else:
                meanList, stdevList, cvList= calculateCVForClusters(centroids2, points, association)
            #print "Centroid:", centroids2
                print "CV:", cvList
            
                found=True
                for cv in cvList:
                    found = found and (cv <cvBound)
            
            if maxRegionsAchieved:
                found=True
                    
            repeats-=1
        # it wasnt found
        
        if (found):
            dataDic[numberOfRegions]= [centroids2, overalDistortion, points, association, dist]
            if len(searchedNumbers)==1 or maxRegionsAchieved:
                found=True
            else:
                found=False
                if not dicotomicSearch:
                    dicotomicSearch=True
                    min=searchedNumbers[-2]
                    max=searchedNumbers[-1]
                else:
                    max=numberOfRegions
                
                numberOfRegions=int(np.floor(float(max-min)/2.0)+min)
                if (numberOfRegions==max):
                    found=True
        else:
            if not dicotomicSearch:
                print "incres # regions"
                numberOfRegions*=2
                if (numberOfRegions>maxRegions):
                    print "Max number of regions ("+str(numberOfRegions)+"/"+str(maxRegions)+") achieved"
                    maxRegionsAchieved=True
                    
            else:
                min=numberOfRegions
                numberOfRegions=int(np.floor(float(max-min)/2.0)+min)
                if (numberOfRegions==min):
                    found=True
                    numberOfRegions=max
                
    centroids2, overalDistortion, points, association, dist=dataDic[numberOfRegions]
    return numberOfRegions, centroids2, overalDistortion, points, association, dist
    


def doKMeansOnData(inputData, kGroups, doWhiten=True, inputCentroids=None):
    #inputData=np.array(listData)
    #inputData=np.transpose(inputData)
    #doWhiten=True
    if (doWhiten):
        inputData=vq.whiten(inputData)
    #print inputData
#    if (inputData.shape[1]<=2):
#        vor = Voronoi(inputData)
#        print len(vor.regions)
        #nerscPlot.paintVoronov("Voro", vor,  dir="./", graphFileName="VoroTest")
    book = getRandomSamples(inputData, kGroups)
    if (inputCentroids!=None):
        book=inputCentroids
    
    
    #np.array((inputData[0],inputData[2], inputData[5], inputData[20], inputData[25],  inputData[100],  inputData[120]))

    #centroids, labels=vq.kmeans2(inputData, kGroups)
    #return centroids, labels
    centroids, distortion=vq.kmeans(inputData, book)
    code, dist=vq.vq(inputData, centroids)
    return centroids, distortion, inputData, code, dist

def doKMeansOnDataV2(inputData, kGroups, doWhiten=True, inputCentroids=None, reduced=False):
    #return kmeans.doKMeansOnDataV2(inputData, kGroups, doWhiten, inputCentroids, reduced=reduced)
    
    
    #inputData=np.array(listData)
    #inputData=np.transpose(inputData)
    #doWhiten=True
    if (doWhiten):
        inputData=vq.whiten(inputData)
    #print inputData
#    if (inputData.shape[1]<=2):
#        vor = Voronoi(inputData)
#        print len(vor.regions)
        #nerscPlot.paintVoronov("Voro", vor,  dir="./", graphFileName="VoroTest")
    book = getRandomSamples(inputData, kGroups)
    if (inputCentroids!=None):
        book=inputCentroids
    
    
    #np.array((inputData[0],inputData[2], inputData[5], inputData[20], inputData[25],  inputData[100],  inputData[120]))

    #centroids, labels=vq.kmeans2(inputData, kGroups)
    #return centroids, labels
    print "calculating centroids"
    centroids, distortion=vq.kmeans(inputData, book)
    print "centroids calculate"
    print "getting the associated centroid for each point"
    code, dist=vq.vq(inputData, centroids)
    print "classification done"
    return centroids, distortion, inputData, code, dist


def getClusterAssociation(inputData, centroidFile):

    centroids=np.load(centroidFile)
    code, dist=vq.vq(inputData, centroids)
    
    return centroids, inputData, code

def getClusterOfPoint(taskSize, taskCores, centroids):
    point=np.array([[taskSize, taskCores]])
    #print point
    #print centroids
    code, dist=vq.vq(point, centroids)
    return code[0]
    
def getCentroidsRoute(hostname, centroidsDir="centroids/"):
    hostnameBase=""
    
    for h in ["edison", "hopper", "carver"]:
        if h in hostname:
            hostnameBase=h
            break
    centroidsRoute=centroidsDir+hostnameBase+"-cent.npy"
    return centroidsRoute

def getCentroids(route):
    return np.load(route)

def getClusterAssociationAutomatic(hostname,taskSizes, taskCores, centroidsRoute=None, centroidsDir="centroids/"):
    if (centroidsRoute==None):
        centroidsRoute=getCentroidsRoute(hostname, centroidsDir)
    
    inputPoints=np.transpose(np.array([taskSizes, taskCores]))
    centroids, inputPoints, assocList=getClusterAssociation(inputPoints, centroidsRoute)
    return centroids, inputPoints, assocLis
    
def divideListsByAssociation(dicLists, assocList):
     outDic={}
     index=0
     for key in dicLists.keys():
             outDic[key]={}
     for assoc in assocList:
        for key in dicLists.keys():
            if not str(assoc) in outDic[key].keys():
                outDic[key][str(assoc)]=[]

            outDic[key][str(assoc)].append(dicLists[key][index])

        index+=1
     return outDic


    
def doKMeansOnOneDim(cpu, groups, doWhiten=True):
    
    data=np.array([cpu])
    data=np.transpose(data)
    
    return doKMeansOnData(data, groups, doWhiten=doWhiten)
    
def doKMeansOnCPUCores(cpu, cores):
    
    data=np.array([cpu, cores])
    data=np.transpose(data)
    
    return doKMeansOnData(data, 5)

def doKMeansOnCPUCoresMem(cpu, cores, mem):
    data=np.array([cpu, cores, mem])
    data=np.transpose(data)
    
    return doKMeansOnData(data, 5)
    
def normDic(dic):
    newDic= {}
    for k in dic.keys():
        newDic[k]={}
        subDic=dic[k]
        c=0
        for k2 in subDic.keys():
            c+=subDic[k2]
       # print c
        c=float(c)
        if (c!=0):
            for k2 in subDic.keys():
                newDic[k][k2]=float(float(subDic[k2])/c)
        else:
            for k2 in subDic.keys():
                newDic[k][k2]=float(float(subDic[k2]))
    return newDic
    
def countRegionsInQueues(points, asociation, queuePerPoint):
    queues={}
    regions={}
    queues_ch={}
    
    
    
    queueKeys=sorted(list(set(queuePerPoint)))
    regionKeys=sorted(list(set(asociation)))
    #print "HEEEERE:", len(points), len(asociation), len(queuePerPoint)
    #print queueKeys
    #print regionKeys
    for q in queueKeys:
        queues[q]={}
        queues_ch[q]={}
        for r in regionKeys:
            queues[q][r]=0
            queues_ch[q][r]=0
    
    for q in regionKeys:
        regions[q]={}
        for r in queueKeys:
            regions[q][r]=0
    
    
    for point, asoc, q in zip(points, asociation, queuePerPoint):
        if not q in queues.keys():
            queues[q]={}
            queues_ch[q]={}
        if not asoc in queues[q].keys():
            queues[q][asoc]=0
            queues_ch[q][asoc]=0
        queues[q][asoc]+=1
        queues_ch[q][asoc]+=point[0]*point[1]
        
        if not asoc in regions.keys():
            regions[asoc]={}
        if not q in regions[asoc].keys():
            regions[asoc][q]=0
        regions[asoc][q]+=1
        
    return queues, regions, normDic(queues), normDic(regions), normDic(queues_ch)

def displayRegionsQueues(queuesN):
    cad=""
    for q in sorted(queuesN.keys()):
        cad+=str(q)
        theQ=queuesN[q]
        ok=False
        for r in sorted(theQ.keys()):
            if(theQ[r]>=0.5):
                ok=True
            cad+="|"+str(r)+"|%.3f" % theQ[r]
        if (ok):
            cad+="|Yes"
        else:
            cad+="|No"
        cad+="\n"
    return cad

def getClusters(centroids, points, association):
    clusters=[]
    for i in centroids:
        clusters.append([])
    for p, a in zip(points, association):
        clusters[a].append(p)
    return clusters
        

def calculateCVForClusters(centroids, points, association):
    clusters=getClusters(centroids, points, association)
    means=[]
    stdDevs=[]
    CVs=[]
    for c in clusters:
       
        arr=np.array(c)
        
        
        m=np.mean(arr)
        st=np.std(arr)
        #print m
        #print st
        #print st/m
        means.append(m)
        stdDevs.append(st)
        CVs.append(st/m)
    
    return means, stdDevs, CVs

def calculateCVForClusters1D(centroids, points, association):
    clusters=getClusters(centroids, points, association)
    means=[]
    stdDevs=[]
    CVs=[]
    for c in clusters:
       
        arr=np.array(c)
        dicM={}
        dicST={}
        dicCV={}
        for i in range(arr.shape[1]):
            m=np.mean(arr[:,i])
            st=np.std(arr[:,i])
            dicM[i]=m
            dicST[i]=st
            dicCV[i]=st/m
            
        #print m
        #print st
        #print st/m
        means.append(dicM)
        stdDevs.append(dicST)
        CVs.append(dicCV)
    
    return means, stdDevs, CVs
    
def withinBoundCV(centroids, points, association, bound):
    means, stDevs, CVs = calculateCVForClusters1D(centroids, points, association)
    for c in CVs:
        print c
        for k, CV in c.iteritems():
            print CV
            if CV>bound:
                return False
    return True
        
    
