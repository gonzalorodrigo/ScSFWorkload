import datetime

def getTS():
    return datetime.datetime.now()
    

def getSpan(tf, to):
    return tf-to

def getText(span):
    return str(span.seconds)+"."+str(span.microseconds)

def getSpanT(tf, to):
    return getText(getSpan(tf, to))
    
def getFinalT(to):
    tf=getTS()
    return getSpanT(tf, to)