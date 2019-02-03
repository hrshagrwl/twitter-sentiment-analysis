from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    print(counts)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    words = {
        'positive': [],
        'negative': [],
        'neutral': []
    }

    for count in counts:
        for val in count:
            words[val[0]].append(val[1])

    print (words)
    words['x'] = range(1, len(words['positive']) + 1)

    plt.plot( 'x', 'positive', data=words, marker='', color='green', linewidth=2)
    plt.plot( 'x', 'negative', data=words, marker='', color='red', linewidth=2)
    # plt.plot( 'x', 'neutral', data=words, marker='', color='grey', linewidth=2)
    plt.show()

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    return set(line.strip() for line in open(filename))

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    def getPolarity(word):
        if word in pwords: 
            return ('positive', 1)
        elif word in nwords:
            return ('negative', 1)
        else:
            return ('neutral', 1)

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    runningCount = tweets.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: getPolarity(word)) \
                  .reduceByKey(lambda a, b: a+b)
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    runningCount.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
