import os
import math
import sys
from pyspark import SparkConf, SparkContext
import pandas as pd
import csv
from operator import add
from pandas import DataFrame
from pyspark.sql import SQLContext
import time
start_time = time.time()

sc = SparkContext()
sQLContext = SQLContext(sc)


#reading files
#file_rosmap = "ROSMAP_RNASeq_entrez.csv"
#file_gene_cluster = "gene_cluster.csv"
file_rosmap = "hdfs://localhost:9000/user/Elizabeth/inputfiles/ROSMAP_RNASeq_entrez.csv"
file_gene_cluster = "hdfs://localhost:9000/user/Elizabeth/inputfiles/gene_cluster.csv"

#declaring RDD's
data_rosmap = sc.textFile(file_rosmap)
data_gene_cluster = sc.textFile(file_gene_cluster)

#split & format both files
rosmap_split = data_rosmap.map(lambda x: x.split(','))
gene_cluster_split = data_gene_cluster.map(lambda cluster: cluster.split(','))

#filter out the headers from both files and filter anything else we won't need
rosmap_header = rosmap_split.first()
gene_header = gene_cluster_split.first()
rosmap_split = rosmap_split.filter(lambda x: x != rosmap_header)
gene_cluster_split = gene_cluster_split.filter(lambda cluster: cluster != gene_header)

#for faster processing, we filter out everything except diagnosis = 1, 4, 5 from the RDD
rosmap_cluster = rosmap_split.filter(lambda map: map != rosmap_header and map[1] != 'NA' and map[1] != '2' and map[1] != '3' and map[1] != '6')

# we do not need any other species but Human, so filter everything else out
gene_cluster_split = gene_cluster_split.filter(lambda x: x[2] == 'Human')

# create individual clusters with entrez id labels
def create_clusters(cluster):
    all_clusters = [] # will contain <entrez id, list<patient id, cluster id, value>> pairs
    end = len(cluster)
    for i in range(2,end):
        all_clusters.append((rosmap_header[i], [cluster[0], float(cluster[1]), float(cluster[i])]))
    return all_clusters

# this RDD is in the form of [(cluster id, [patient id, diagnosis #, value])......]
rosmap_cluster = rosmap_cluster.flatMap(lambda cluster: create_clusters(cluster))
# testing
print("ROSMAP CLUSTERS")
print(rosmap_cluster.take(10))
#print(type(rosmap_cluster))

#create individual clusters with cluster id labels
entrez_cluster = gene_cluster_split.map(lambda cluster: (list(cluster[4].split(';')), cluster[0]))
def group(x):
    pair = [] # this will contain all <entrez id, cluster id> pairs
    entrez_list = x[0]
    for id in entrez_list:
        pair.append((id, x[1]))
    return pair
# this creates pairs of <entrez id, cluster id>
entrez_cluster = entrez_cluster.flatMap(lambda x: group(x))

print(' ')
print(' ')
print(' ')

print("GENE CLUSTERS")
print(entrez_cluster.take(10))
#print(type(entrez_cluster))

print(' ')
print(' ')
print(' ')

# we join the RDD's by entrez id to get the form <entrez id, (cluster id, [patient id, diagnosis #, value]))
joined = entrez_cluster.join(rosmap_cluster)
print(joined.take(5))

# define the keys for the new mapped RDD
def keys(x):
    cluster_id = x[1][0]
    patient_id = x[1][1][0]
    diagnosis = x[1][1][1]
    return (cluster_id, patient_id, diagnosis)
# define the values for the new mapped RDD
def values(x):
    values = x[1][1][2]
    return (values)

# new (key,value) format = [((cluster_id, patient_id, diagnosis), values).....]
joined = joined.map(lambda x: (keys(x), (values(x))))
# testing
print("MAPPED")
#print(joined.take(5))

# we reduce by key entrez id to get:
# [((cluster id, patient id, diagnosis #), value) ...]
joined = joined.reduceByKey(add)
print("REDUCED")
#print(joined.take(5))
#print(' ')
#print(' ')

print('T SCORES ')

def defined_map(x):
    ad_count = nci_count = 1
    cluster_id = x[0][0]
    patient_id = x[0][1]
    diagnosis = x[0][2]
    values = x[1]
    values2 = values*values;
    if (diagnosis == 4 or diagnosis == 5): # AD
        return (cluster_id, (ad_count, 0, values, 0, values2, 0, diagnosis))
    elif (diagnosis == 1): # NCI
        return (cluster_id, (0, nci_count, 0, values, 0, values2, diagnosis))

def t_score(x):
    cluster_id = x[0]
    ad_val = x[1][2]
    ad_count = x[1][0]
    ad2 = x[1][4]
    nci_val = x[1][3]
    nci_count = x[1][1]
    nci2 = x[1][5]
    # do value/count
    mean_ad = ad_val / ad_count
    mean_nci = nci_val / nci_count
    # get squares of ad and nci values to be used to compute the standard deviation squared
    ad_std2 = (ad2/ad_count) - (mean_ad * mean_ad)
    nci_std2 = (nci2/nci_count) - (mean_nci * mean_nci)
    std_ad = math.sqrt(ad_std2)
    std_nci = math.sqrt(nci_std2)
    diagnosis = x[1][6]
    # using the formula from the slides
    t_num = mean_ad - mean_nci # calculate the numerator
    t_den = math.sqrt((ad_std2/ad_count) + (nci_std2/nci_count)) #calculate the denominator
    t_score = t_num/t_den

    return (cluster_id, (t_score, mean_ad, mean_nci, std_ad, std_nci))


# TODO: remap to make patient id the key, then reduce to remove it

#def remap(x):
#    cluster_id = x[0][0]
#    patient_id = x[0][1]
#    diagnosis = x[0][2]
#    value = x[1]
#    return ((cluster_id, diagnosis), value)

#diagnosis_rdd = joined.map(lambda map: map[0][1])
#print(' ')
#print(' ')
#print(diagnosis_rdd.take(5))
#ad_rdd = diagnosis_rdd.filter(lambda map: map[0][2] == '4' and map[0][2] == '5')
#nci_rdd = diagnosis_rdd.filter(lambda map: map[0][2] == '1')
#print(' ')
#print(' ')
#print ('RDDs')
#print(' ')
#print(' NCI ')
#print(nci_rdd.take(5))
#print(' ')
#print(' AD ')
#print(ad_rdd.take(5))
#print(' ')
#print(' ')

# map to get form [(cluster id, (0, 1, 0, value, 0, value^2, diagnosis)) ...  for diagnosis # = 1
# (cluster id, (1, 0, value, 0, value^2, 0, diagnosis)) ... ] for diagnosis # = 4 or 5
joined = joined.map(lambda x: defined_map(x))
#testing
#print("AFTER MAP")
#print(joined.take(8))
#print(' ')
#print(' ')

# reduce to get form of [(cluster id, (ad count, nci count, ad val, nci val, ad val^2, nci val^2, diagnosis #) ... ]
joined = joined.reduceByKey(lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1], c1[2] + c2[2], c1[3] + c2[3], c1[4] + c2[4], c1[5] + c2[5], c1[6]))
#print("AFTER REDUCE")
#print(joined.take(8))
#print(' ')
#print(' ')

#NEW JOINED FORMAT: (cluster id, (ad_count, nci_count, ad_val, nci_val, ad2_val, nci2_val, diagnosis))
joined = joined.map(lambda x: t_score(x))
#print("AFTER MAP")
#print(joined.take(8))
#print(' ')
#print(' ')

# this gives us the form (t-score, cluster id, mean ad, mean nci, std ad, std nci) in descending order
joined = joined.map(lambda x: (x[1][0], (x[0], x[1][1], x[1][2], x[1][3], x[1][4]))).sortByKey(ascending=False)
#print(joined.take(5))
#print(' ')
#print(' ')


# this swaps the cluster id and t-score to be (cluster id, t-score, mean ad, mean nci, std ad, std nci)
joined = joined.map(lambda x: (x[1][0], (x[0], x[1][1], x[1][2], x[1][3], x[1][4])))
print("TOP 10 T-SCORES")
print(joined.take(10))
print(' ')
print(' ')

top10 = joined.take(10)
print('SAVING TO FILE')
df = pd.DataFrame(top10)
df.to_csv("results.csv")

print("--- %s seconds ---" % (time.time() - start_time))
