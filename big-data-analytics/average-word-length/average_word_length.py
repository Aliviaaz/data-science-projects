# read any text input and computes the average length of all
# words that start with each character

from pyspark import SparkContext
import sys
import re

if __name__ == '__main__':
    args = sys.argv
    if len(args) != 3:
        print >> sys.sterr, "Usage: average_word_length.py <input> <output>"
        exit(-1)
    
    sc = SparkContext()
    # read the input
    input_rdd = sc.textFile(args[1])

    # split on one or more non-word chars
    words = input_rdd.flatMap(lambda line: re.split(r"\\W+", line)).persist()

    # extract the first character of each word
    # then emit the character as key and word length as value
    # and group results by each letter
    characters = words.filter(lambda word: len(word) > 0).map(lambda word: (word[:1], len(word))).groupByKey().persist()

    # calculate the sum of lengths for each character
    # and then calculate the average with sum / count
    # [(key, [L1, L2, L3]), (key2, [L1,...]) ...] is how the RDD characters is formatted
    character_lengths = characters.map(lambda fields: (fields[0], (sum(fields[1]) / len(fields[1])))).persist()

    # emit letter, average
    character_lengths.saveAsTextFile(args[2])
    sc.stop()