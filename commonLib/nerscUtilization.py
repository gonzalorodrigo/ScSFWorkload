import bisect
import time

import numpy as np


def getCurrentActiveJobs(startEpoch, startNumbers, durationNumbers, coreNumbers, completionNumbers):
        
    preStartNumbers=[]
    preDurationNumbers=[]
    preCoreNumbers=[]

    for (start, duration, cores, completion) in zip(startNumbers, durationNumbers, coreNumbers, completionNumbers):
        if start<startEpoch and completion > startEpoch or True:
            #print start, startEpoch, completion

            preStartNumbers.append(start)
            preDurationNumbers.append(duration)
            preCoreNumbers.append(cores)
            
    return preStartNumbers, preDurationNumbers, preCoreNumbers
    

def getMaxCores(hostname):
    if "hopper" in hostname:
        return 153216
    if ("carver" in hostname):
        return 9984
    if ("edison" in hostname):
        return 133824 
    
def cutSamples(inTimeStamps, sampleUse, shiftValue):
    
    #timeStamps=[x-inTimeStamps[0] for x in inTimeStamps]
    timeStamps=inTimeStamps
    index=0
    for t in timeStamps:
        if t>=shiftValue:
            break
        index+=1
    return inTimeStamps[index:], sampleUse[index:]
    
    


class UtilizationEngine:

    
    
    def __init__(self):
        self.endingJobsTime=[]
        self.endingJobsUse=[]
    
        self.sampleTimeStamp=[]
        self.sampleUse=[]
        self.currentUse=0
        self._waste_stamps = None
        self._waste_deltas = None
    
    def getIntegralUsage(self, maxUse=None, shiftValue=None):
        #timeStamps=[x-self.sampleTimeStamp[0] for x in self.sampleTimeStamp]
        if len(self.sampleTimeStamp) < 2:
            raise ValueError("Integral usage cannot be processed with a single"
                             " time Step")
        timeStamps=self.sampleTimeStamp
        #print timeStamps
        maxTimeStamp=int(timeStamps[-1])
        lastTimeStamp=0
        accummSurface=0
        
        cutSampleUser=self.sampleUse
        if (shiftValue!=None and shiftValue!=0):
            timeStamps,cutSampleUser=cutSamples(timeStamps, cutSampleUser, shiftValue)
        if (maxUse==None):
            maxUse=int(max(cutSampleUser))
        ts=np.array(timeStamps[1:])-np.array(timeStamps[0:-1])
        u=np.transpose(np.array(cutSampleUser[0:-1]))
        accummSurface=np.dot(ts,u)
        print("Obtained Integrated Surface:", accummSurface)               
        #for ts, u in zip(timeStamps[1:],self.sampleUse[0:-1]):
        #    accummSurface+=long(ts)*long(u)
        timePeriod=timeStamps[-1]-timeStamps[0]
        print("Time period", timePeriod)
        targetSurface=maxUse*(timePeriod)
        print("Target surface", targetSurface)
       
        return float(accummSurface)/float((targetSurface))
        
        
        
    
    def changeUse(self, timeStamp, useDelta, doRegister=True):
        #print "Change USe", timeStamp, useDelta, self.currentUse
        self.currentUse+=useDelta
#        if self.currentUse > 153216:
#            print "over User:", self.currentUse, useDelta
        if doRegister:
            self.sampleTimeStamp.append(timeStamp)
            self.sampleUse.append(self.currentUse)
        
        if (self.currentUse<0):
            print("JODER")
        
    
    def procesEndingJobs(self, timeStamp, doRegister=True):
        i=0
        for time, use in zip(self.endingJobsTime, self.endingJobsUse):
            if  timeStamp is None or time<=timeStamp:
                #print "job dying", timeStamp
                self.changeUse(time, -use, doRegister=doRegister)
                i+=1
            else:
                break
        
        #print "removing", i
        self.endingJobsTime=self.endingJobsTime[i:]
        self.endingJobsUse=self.endingJobsUse[i:]
        return i
    
    def processStartingJob(self, timeStamp, duration, use, doRegister=True):
        #print "Job Starts", timeStamp, timeStamp+duration
        index=bisect.bisect_left(self.endingJobsTime, timeStamp+duration)
        self.endingJobsTime.insert(index, timeStamp+duration)
        self.endingJobsUse.insert(index, use)
        
        self.changeUse(timeStamp, use, doRegister=doRegister)
        
    def apply_waste_deltas(self, waste_stamps, waste_deltas, start_cut=None,
                         end_cut=None):
        #print ("apply_waste_deltas",  waste_stamps, waste_deltas, start_cut, 
        #       end_cut)
        if start_cut and waste_stamps:
            pos = bisect.bisect_left(waste_stamps, start_cut)
            waste_stamps=waste_stamps[pos:]
            waste_deltas=waste_deltas[pos:]
        if end_cut and waste_stamps:
            pos = bisect.bisect_right(waste_stamps, end_cut)
            waste_stamps=waste_stamps[:pos]
            waste_deltas=waste_deltas[:pos]
        #print ("apply_waste_deltas after",  waste_stamps, waste_deltas, start_cut, 
        #       end_cut)
        if waste_stamps:
            self.sampleTimeStamp, self.sampleUse = _apply_deltas_usage(
                     self.sampleTimeStamp, self.sampleUse,
                     waste_stamps, waste_deltas, neg=True)
        
        return self.sampleTimeStamp, self.sampleUse    
    
    def processUtilization(self, timeStamps, durations, resourceUse, 
                           startCut=None, endCut=None, 
                           preloadDone=False, doingPreload=False):
        self.sampleTimeStamp=[]
        self.sampleUse=[]
            
        if (not preloadDone):
            self.endingJobsTime=[]
            self.endingJobsUse=[]

            self.currentUse=0

        first_stamp_registered=False
        steps=float(len(timeStamps))
        step=0.0
        for time, jobDuration, jobUse in zip(timeStamps, durations, 
                                             resourceUse):
            if (startCut!=None and time<startCut):
                continue
            if preloadDone and not first_stamp_registered:
                if startCut!=time:
                    self.changeUse(startCut, 0, True)
                first_stamp_registered=True
            if (endCut!=None and time>=endCut):
                break
            if (time<1):
                continue
            percent=(step/steps*100)
            if (percent%5.0==0.0):
                print("Progress: "+str( percent)+"%")
                
            
            time=int(time)
            jobDuration=int(jobDuration)
            jobUse=int(jobUse)
            
            
            
            self.procesEndingJobs(time, doRegister=not doingPreload)
            self.processStartingJob(time, jobDuration, jobUse, 
                                    doRegister=not doingPreload)
            step+=1.0
        if not doingPreload:
            self.procesEndingJobs(endCut)
            if endCut is not None and self.sampleTimeStamp[-1]!=endCut:
                self.changeUse(endCut, 0, True)
        return self.sampleTimeStamp, self.sampleUse

def _apply_deltas_usage(stamps_list, usage_list, stamps, usage, neg=False):
    """Applies a list of usage deltas over a list of absolute usage values."""
    if neg: 
        usage = [-x for x in usage]
    for (st_1, st_2, us) in zip(stamps[:-1], stamps[1:], usage):
        pos_init = bisect.bisect_left(stamps_list, st_1)
        pos_end = bisect.bisect_left(stamps_list, st_2)
        if _create_point_between(pos_init, st_1, stamps_list, usage_list):
            pos_end+=1
        if _create_point_between(pos_end, st_2, stamps_list, usage_list):
            pos_end+=1
        for pos in range(pos_init, pos_end):
            usage_list[pos]+=us       
    usage_list[pos_end-1]+=usage[-1]            
    return stamps_list, usage_list 

def _create_point_between(pos, key, key_list, value_list):
    if (pos==(len(key_list)) or key!=key_list[pos]):
        prev=0
        if pos>0:
            prev=value_list[pos-1]
        key_list.insert(pos, key)
        value_list.insert(pos, prev)
        return True
    return False



