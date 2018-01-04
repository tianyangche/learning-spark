from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Tianyang's App")
sc = SparkContext(conf = conf)
lines = sc.textFile("text_file.txt")

wordCount = lines.filter(lambda x: len(x) > 0).flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x,y: x + y).map(lambda x: (x[1], x[0])).sortByKey(False)

wordCount.saveAsTextFile("/Users/tianyangche/Desktop/wordCount.txt")
