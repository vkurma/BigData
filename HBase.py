from starbase import Connection

c = Connection("127.0.0.1", "8000")

print("Getting ratings table")
ratings = c.table('ratings')

if(ratings.exists()):
    print('Dropping ratings table')
    ratings.drop()

print("Creating ratings table")    
ratings.create('rating')

print("Opening data file")
ratingFile = open("E:/Hadoop Files/ml-100k/u.data", "r")

print("Creating batch")
# To add data in batch instead of data row by row.
batch = ratings.batch()

print("Adding data in batch")
for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    # add each row. Here each row key is userID, So in HBase(BigTable) each row
    # has userID as key and 'rating' as ColumnFamily. InSide 
    # rating movie family we have each movieID as column and rating as its value.
    batch.update(userID, {'rating':{movieID: rating}})
    
ratingFile.close()

print("commiting batch")
batch.commit(finalize = True)

print("Fetching data for user 33")
print(ratings.fetch("33"))