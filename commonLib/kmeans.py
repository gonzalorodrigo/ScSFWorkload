
#from sklearn import metrics
#from sklearn.cluster import KMeans
#from sklearn.cluster import MiniBatchKMeans
#from sklearn.datasets import load_digits
#from sklearn.decomposition import PCA
#from sklearn.preprocessing import scale
from sys import platform as _platform



import numpy as np
import scipy as sc
import scipy.cluster.vq as vq

from . import nerscRadar as nr
def doKMeansOnDataV2(inputData, kGroups, doWhiten=True, inputCentroids=None, reduced=False):
     

    
    
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
    book = nr.getRandomSamples(inputData, kGroups)
    
    if (inputCentroids!=None):
        book=inputCentroids
#    else:
#        book='k-means++'
    n_jobs=4
    if isItMac():
        n_jobs=1
   
    print("calculating centroids. Parallel Degree:"+str(n_jobs))     
#    km=KMeans(n_clusters=kGroups, init='k-means++', \
#                        n_init=10, max_iter=300, tol=0.0001,\
#                        precompute_distances=True, verbose=0,\
#                        random_state=None, copy_x=True, n_jobs=1)
    if (reduced):
        km=MiniBatchKMeans(n_clusters=kGroups, init=book)
    
    else:
        km=KMeans(n_clusters=kGroups, init=book, \
                        n_init=10, max_iter=300, tol=0.0001,\
                        precompute_distances=True, verbose=0,\
                        random_state=None, copy_x=True, n_jobs=n_jobs)
    km.fit(inputData)
    
    centroids=km.cluster_centers_
    distortion=[] 
    
    #np.array((inputData[0],inputData[2], inputData[5], inputData[20], inputData[25],  inputData[100],  inputData[120]))

    #centroids, labels=vq.kmeans2(inputData, kGroups)
    #return centroids, labels
    
    #centroids, distortion=vq.kmeans(inputData, book)
    print("centroids calculate")
    print("getting the associated centroid for each point")
    code, dist=vq.vq(inputData, centroids)
    print("classification done")
    return centroids, distortion, inputData, code, dist

def isItMac():


    return _platform == "darwin"
