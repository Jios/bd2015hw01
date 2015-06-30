#!/usr/bin/python

from pyspark import SparkContext, SparkConf
from dateutil.parser import parse
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt
import hashlib
import numpy as np


################################################################


#/Users/jianli/Desktop/bigdata_hw/
dataFilename     = "datasets/train_dataset.txt"
testDataFilename = "datasets/test_dataset.txt"

sample_size  = 1060
train_size   = 1000
test_size    = sample_size - train_size


# es confs
es_read_conf  = {"es.nodes" : "localhost",
                 "es.port" : "9200",
                 "es.resource" : "test/spam_email"} #"es.query": ""
es_write_conf = {"es.nodes" : "localhost",
                 "es.port" : "9200",
                 "es.resource" : "test/kmeans"} 


################################################################


def error(point, clf):
    """ Evaluate clustering by computing Within Set Sum of Squared Errors """

    center = clf.centers[clf.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

def hashStringToInt(string):
    """ hash string to int """

    # hobj = hashlib.md5(string.encode())
    # return int(h.hexdigest(), 16)
    return abs(hash(string)) % (10 ** 8)

################################################################

def parseESData(doc_dict):
    """ parse data from dictionary to tuple """

    if doc_dict.has_key("geoip") and doc_dict.has_key("timestamp"):
        # contain reply-to field
        reply_to = 1 if doc_dict.has_key("reply-to") else 0

        # send during daytime
        hour = parse(doc_dict["timestamp"]).hour
        daytime = 1 if (hour > 6 and hour <= 18) else 0
        
        # subject length
        subject_len = len(doc_dict["subject"]) if doc_dict.has_key("subject") else 0

        # hash: email domain name
        sender_email = doc_dict["from"]
        domain_hash  = hashStringToInt(sender_email.split("@")[1])

        # content-type: text or multipart
        isPlainText = -1
        if doc_dict.has_key("content-type"):
            ctype = doc_dict["content-type"].split("/")[0]
            if ctype == "text":
                isPlainText = 1
            else:
                isPlainText = 0

        # geo data
        geo_dict = doc_dict["geoip"]

        ## hash: ip 
        ip      = geo_dict["ip"]
        ip_hash = hashStringToInt(ip)

        ## geolocation
        lon = geo_dict["longitude"]
        lat = geo_dict["latitude"]
        ## city, country, and continent geoname ID's
        city_geoname_id = geo_dict["city_geoname_id"]       if geo_dict.has_key("city_geoname_id")      else 0
        ctr_geoname_id  = geo_dict["country_geoname_id"]    if geo_dict.has_key("country_geoname_id")   else 0
        cntn_geoname_id = geo_dict["continent_geoname_id"]  if geo_dict.has_key("continent_geoname_id") else 0

        # return (ip_hash, domain_hash, subject_len, reply_to, daytime) #, ctr_geoname_id, cntn_geoname_id)
        return (isPlainText, city_geoname_id, subject_len, reply_to, daytime)
    else:
        return None

################################################################

def getRecord(line_tuple):
    """ convert tuple to string """

    pattern = "%d " * len(line_tuple)
    pattern = pattern.strip(" ")
    return pattern % line_tuple

def getTrainData(filename):
    """ load dataset from text file """

    data = sc.textFile(filename) # pyspark.rdd.RDD
    return data

################################################################

def saveDataToFile(filename, train_list):
    """ retrieve data from elasticsearch """

    # doc = es_rdd.first()[1]
    fobj = open(filename, "w")
    for docID, doc_dict in train_list:
        line_tuple = parseESData(doc_dict)
        if line_tuple is not None:
            line = getRecord(line_tuple)
            fobj.write(line + "\n")
    fobj.close()

################################################################

def kmeans(k=2):
    """ kmeans """

    # Load and parse training data
    data = getTrainData(dataFilename)
    parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')])) # pyspark.rdd.PipelinedRDD

    # Build the model (cluster the data)
    #  KMeans.train(cls, data, k, maxIterations=100, runs=1, initializationMode="k-means||")
    clf = KMeans.train(parsedData, k, maxIterations=10, runs=10, initializationMode="random") # pyspark.mllib.clustering.KMeansModel

    WSSSE = parsedData.map(lambda point: error(point, clf)).reduce(lambda x, y: x + y) # float
    print("Within Set Sum of Squared Error = " + str(WSSSE))
    print "###  cluster centers  ###:"
    print clf.centers
    return clf


################################################################


if __name__ == "__main__":
    np.set_printoptions(precision=3)

    conf = SparkConf().setAppName("ESTest")
    sc   = SparkContext(conf=conf)

    # pyspark.rdd.RDD
    es_rdd = sc.newAPIHadoopRDD(inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat", \
                                keyClass         = "org.apache.hadoop.io.NullWritable", \
                                valueClass       = "org.elasticsearch.hadoop.mr.LinkedMapWritable", \
                                conf             = es_read_conf)
    
    # retrieve sample data from ES
    rdd_list   = es_rdd.take(sample_size)
    train_list = rdd_list[:train_size-1]
    test_list  = rdd_list[-test_size:]

    # save training data to text file
    saveDataToFile(dataFilename    , train_list)
    saveDataToFile(testDataFilename, test_list)
    
    # dt = sc.parallelize((67875870, 32, 21, 1861060, 6255147))  # pyspark.rdd.RDD

    # prepare sample data for k-means test
    testdata_list = []
    for docID, doc_dict in test_list:
        line_tuple = parseESData(doc_dict)
        if line_tuple is not None:
            testdata_list.append([docID, line_tuple])

    # k-means clustering
    for k in [2, 5, 7, 9]:
        print "\n###  k = %d  ###:" % k
        # training
        clf = kmeans(k)
        for docID, line_tuple in testdata_list:
            line = getRecord(line_tuple)
            # prediction
            c = clf.predict(line_tuple)
            print docID + " - " + str(c) + " - (" + line + ")"


