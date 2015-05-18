#!/bin/bash

spark-submit --master local[4] --jars jars/elasticsearch-hadoop-2.1.0.Beta2.jar es_pyspark.py > kmeans_results/results.txt