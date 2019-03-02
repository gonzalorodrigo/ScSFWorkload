import getopt
import sys

def getFileName(fileName, viewName):
    if (fileName=="auto"):
        fileName=viewName+"UtilizationPlot"
    return fileName
    
def getDir(dirBase, hostname):
    return dirBase+"-"+hostname
def getDirParams(dirBase, params):
    for par in params:
        if par!=-1:
            dirBase+="-"+str(par)
    return dirBase
def getViewName(name, hostname, startYear,startMonth, startDay, stopYear, stopMonth, stopDay):
    if (name=="auto"):
        viewName=hostname+"-"
        for a in [startYear,startMonth, startDay, stopYear, stopMonth, stopDay]:
            if a!="" and a!=-1:
                viewName+=str(a)+"-"
    else:
        viewName=name
    return viewName

def addScore(list, double=False):
    char="-"
    if double:
        char="--"
    
    return [char+x for x in list]
def addPost(list):
    
    char="="

    
    return [x+char for x in list]
def summaryParameters(outActions, outputValues, parameterActions):
    cad=""
    for key in list(outActions.keys()):
        if (cad!=""):
            cad+=", "
        cad+="--"+key+" "+str(outActions[key])
    for (key, val) in zip(parameterActions,outputValues):
        if (cad!=""):
            cad+=", "
        cad+=key+" "+str(val)
    return cad
        
        

# singleActions is a list of options that don't take parameters
# parameterActions is a list of options that will require an extra argument
# defaultParameters is a list of the default values for the parameter Actions
#  
def processParameters(argv, singleActions, singleLetters, parameterActions, parameterLetters, defaultParameters):
    try:
        allLetters=":".join(parameterLetters)
        if (allLetters!=""):
            allLetters+=":"
        allLetters+="".join(singleLetters)
        #print allLetters
        allWords=addPost(parameterActions)+singleActions
        #print allLetters, allWords
        
        opts, args = getopt.getopt(sys.argv[1:], allLetters, \
                                   allWords)
                    
    except getopt.GetoptError as err:
        # print help information and exit:
        print(str(err)) # will print something like "option -a not recognized"
        sys.exit(2)
    
    singleActions=addScore(singleActions, True)
    singleLetters=addScore(singleLetters)
    parameterActions=addScore(parameterActions, True)
    parameterLetters=addScore(parameterLetters)
    
    
    outActions= {}
    for key in singleActions:
        outActions[key[2:]]=False
    outputValues=defaultParameters
    
    #print opts
    #print args

    for o, a in opts:
        if o in singleActions or o in singleLetters:
            index=-1
            if o in singleActions:
                index=singleActions.index(o)
            elif o in singleLetters:
                index=singleLetters.index(o)
            nameAction=singleActions[index]
            outActions[nameAction[2:]]=True
        else:
            index=-1
            if o in parameterActions:
                index=parameterActions.index(o)
            elif o in parameterLetters:
                index=parameterLetters.index(o)
            #print "KK", index
            if (index==-1):
                print("unknown parameter: "+o)
                exit(2)
            else:
                outputValues[index]=type(outputValues[index])(a)
    print("Input Args: "+summaryParameters(outActions, outputValues, parameterActions))
    return outActions, outputValues
            