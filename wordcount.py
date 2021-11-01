from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
if __name__ == "__main__":
    filename = sys.argv[1]
    print("filename is ", filename)
    if len(sys.argv) != 2:
        print("Problem Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

wordsCount = 0
spark = SparkSession\
            .builder\
            .appName("PythonWordCount")\
            .getOrCreate()
lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
if filename.endswith('.txt'):
    splitit=' '
elif filename.endswith(".csv"):
    splitit=','
counts = lines.flatMap(lambda x: x.split(splitit))\
        .map(lambda x: (x, 1)) \
        .reduceByKey(add).sortBy(lambda a:-a[1])

output = counts.take(1000)
for(word, count) in output:
        wordsCount += 1
        print("%s : %i" % (word, count))
print("number of words", wordsCount)
spark.stop()
