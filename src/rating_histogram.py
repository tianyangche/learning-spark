from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MyFirstSparkProgram")
sc = SparkContext(conf = conf)
# example: 1,112,3.5,1094785740
lines = sc.textFile("/Users/tianyangche/spark-learning/ml-20m/ratings.csv")
ratings = lines.map(lambda x:x.split(',')[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

# The output
# 0.5 239125
# 1.0 680732
# 1.5 279252
# 2.0 1430997
# 2.5 883398
# 3.0 4291193
# 3.5 2200156
# 4.0 5561926
# 4.5 1534824
# 5.0 2898660

