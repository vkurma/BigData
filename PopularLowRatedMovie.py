from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def MapMovieNamesToIds():
    
    mapper = {}
    
    with open("ml-100k/u.item") as file:
        for line in file:
            data = line.split('|')
            mapper[int(data[0])] = data[1]
        
        return mapper
    
    
def ParseInput(line):
    data = line.split()
    return Row(movieID = int(data[1]), rating = float(data[2]))

if __name__ == "__main__":
    
    sparkSession = SparkSession.builder.appName("PopularMovies").getOrCreate()
    movieNames = MapMovieNamesToIds()
    
    lines = sparkSession.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    
    movies = lines.map(ParseInput)
    
    moviesDataSet = sparkSession.createDataFrame(movies)
    
    avgRatings = moviesDataSet.groupBy("movieID").avg("rating")
    
    movieCount = moviesDataSet.groupBy("movieID").count().filter("count > 9")
    
    lowRatedMovies = movieCount.join(avgRatings, "movieID")
    
    lowRatedMovies = lowRatedMovies.orderBy('avg(rating)')
    
    topTen = lowRatedMovies.take(10)
    
    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])
        
    sparkSession.stop()
    
    
    
    
    