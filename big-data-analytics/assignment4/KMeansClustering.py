"""
Assignment instructions:
Write a spark application in python to implement K-means algorithm to 
calculate K-means for the device location (each location has a latitude and longitude). 
Do not use K-means in MLib of Spark to solve the problem.

K-means algorithm:
1. Choose K random points as starting centers
2. Find all points closest to each center
3. Find the center (mean) of each cluster
4. If the centers changed by more than convergeDist (e.g.
convergeDist = 0.1), iterate again starting from step 2;
otherwise, terminate.

Input data format: data, manufacturer, deviceID, latitude, longitude
Example first 2 lines from input file: 
2014-03-15:10:10:20,Sorrento,8cc3b47e-bd01-4482-b500-28f2342679af,33.6894754264,-117.543308253
2014-03-15:10:10:20,MeeToo,ef8c7564-0a1a-4650-a655-c8bbd5f8f943,37.4321088904,-121.485029632
"""

from pyspark import SparkContext
import sys

#defining helper functions
def getPoint(line):
    #parse a line of input and return (latitude, longitude) tuple
    fields = line.strip().split(',')
    latitude = float(fields[3])
    longitude = float(fields[4])
    return (latitude, longitude)

def getSquaredDistance(p1, p2):
    #calculate squared Euclidean distance between two points
    return (p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2

def getClosestCenter(point, centers):
    #find the index of the closest center to the given point
    closestIndex = 0
    closestDistance = float('inf') #start with infinite distance
    for i, center in enumerate(centers):
        distance = getSquaredDistance(point, center)
        if distance < closestDistance:
            closestDistance = distance
            closestIndex = i
    return closestIndex

def addPoints(p1, p2):
    return (p1[0] + p2[0], p1[1] + p2[1])

def mergeClusterStats(sum, current):
    #merge two tuples during reduceByKey
    sumOfPoints = addPoints(sum[0], current[0])
    totalCount = sum[1] + current[1]
    return (sumOfPoints, totalCount)

def calcClusterMean(clusterIndex_stats):
    #divide summed points by count to get the mean center point
    #example input: 
    #clusterIndex_stats = (0, ((74.5, -242.5), 2))
    # clusterIndex_stats[0]       = 0               # the cluster index
    # clusterIndex_stats[1]       = ((74.5, -242.5), 2)
    # clusterIndex_stats[1][0]    = (74.5, -242.5)  # summed coords
    # clusterIndex_stats[1][0][0] = 74.5            # sum of latitudes
    # clusterIndex_stats[1][0][1] = -242.5          # sum of longitudes
    # clusterIndex_stats[1][1]    = 2               # point count
    clusterIndex = clusterIndex_stats[0]
    summedCoords = clusterIndex_stats[1][0]
    count = clusterIndex_stats[1][1]
    meanLat = summedCoords[0] / count
    meanLon = summedCoords[1] / count
    return (clusterIndex, (meanLat, meanLon))

if __name__ == '__main__':
    args = sys.argv
    if len(args) != 3:
        print >> sys.sterr, "Usage: KMeansClustering.py <input> <output>"
        exit(-1)
    
    sc = SparkContext(appName="KMeansClustering")
    input_rdd = sc.textFile(args[1]) #read the input
    
    #parse input and filter out (0, 0) points
    datapoints = input_rdd.map(getPoint).filter(lambda point: point != (0.0, 0.0)).persist()

    # Step 1: You can use the takeSample() to randomly take points as starting centers.
    # The function takeSample has three parameters:
    # withReplacement: Boolean, whether sampling is done with replacement
    # num: Int, the size of the returned sample
    # seed: Long, the seed for the random number generator
    # The function returns the sample of specified size in an array
    # eg. initialKPoints = datapoints.takeSample(false, 5, 34)
    K = 5
    convergeDist = 0.1
    kPoints = datapoints.takeSample(False, K, 34)

    tempDist = float('inf')
    while tempDist > convergeDist:
        # Step 2: Assign each point to the closest center
        # Map each point to (clusterIndex, (point, 1))
        closest = datapoints.map(lambda point: (getClosestCenter(point, kPoints), (point, 1)))
        #example output: (0, ((37.4, -121.4), 1)) cluster index, point coords, count

        # Step 3: Sum all points per cluster, then divide to get new centers
        cluster = closest.reduceByKey(mergeClusterStats)
        newPoints = cluster.map(calcClusterMean).collectAsMap()

        # Step 4: Check convergence and update centers
        tempDist = sum(
            getSquaredDistance(kPoints[i], newPoints[i])
            for i in range(K)
            if i in newPoints
        )
        for i in newPoints:
            kPoints[i] = newPoints[i]

    #convert final results to RDD and then save
    sc.parallelize(kPoints).saveAsTextFile(args[2])
    sc.stop()