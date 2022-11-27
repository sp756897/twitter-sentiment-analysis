from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import pymongo
import re
import json
from datetime import datetime
from pyspark.sql.functions import lit

server_name = "jdbc:mysql://localhost:3306"
database_name = "tweets"
jdbcurl = server_name + "/" + database_name
table_name = "tweets"
db_properties = {"user":"root", "password":"akshaysp"}

def preprocessing(lines):
    #words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = lines
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words

# Replace the uri string with your MongoDB deployment's connection string.
def write_to_mongo(message):
    #message = json.loads(message.decode("utf-8"))

    #json_df = json.loads(message.withColumn('date_as_key', lit(datetime.now().date())).toJSON().first())    
    print(message)
    data = {
        "text": message["text"],
        "processed_text": message["processed_text"],
        "subjectivity": message["subjectivity"],
        "polarity": message["polarity"],
        "sentiment": message["sentiment"]
    }
    
    conn_str = "mongodb+srv://akshaysp:akshaysp@cluster0.y38oj.mongodb.net/tes.tes?retryWrites=true&w=majority"
    # set a 5-second connection timeout
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    try:
        #print(client.server_info())
        db = client.tweets
        d = db["tweets"];
        d.insert_one(data);

    except Exception as e:
        print(e)


def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet


# Create a function to get the subjectifvity
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


# Create a function to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

    # read the tweet data from socket
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").load()

    mySchema = StructType([StructField("text", StringType(), True)])
    # Get only the "text" from the information we receive from Kafka. The text is the tweet produce by a user
    values = lines.select(from_json(lines.value.cast("string"), mySchema).alias("tweet"))

    df1 = values.select("tweet.*")
    clean_tweets = F.udf(cleanTweet, StringType())
    raw_tweets = df1.withColumn('processed_text', clean_tweets(col("text")))
    # udf_stripDQ = udf(stripDQ, StringType())

    subjectivity = F.udf(getSubjectivity, FloatType())
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())

    subjectivity_tweets = raw_tweets.withColumn('subjectivity', subjectivity(col("processed_text")))
    polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
    sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))
    
    query = sentiment_tweets\
    .writeStream\
    .outputMode("append")\
    .foreach(write_to_mongo)\
    .trigger(processingTime='60 seconds').start()\
    .awaitTermination()
    """
    query = words.writeStream.queryName("all_tweets")\
        .outputMode("append").format("csv")\
        .option("path", "./parc")\
        .option("header", True)\
        .option("checkpointLocation", "./check")\
        .trigger(processingTime='60 seconds').start()
    """
    query.awaitTermination()