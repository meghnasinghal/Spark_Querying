# Databricks notebook source
def SortedPairKey(value):
    userA = value[0] # user A is the userid
    friends = value[1].split(",") # getting list of friends
    res = []
    for userB in friends: #itertaing over list of friends
        if userA!='' and userB!='': 
            key = sorted([userA, userB]) #creating sorted key pair of userA and userB
            res.append((tuple(key), set(friends))) # output key as tuple of sorted friends and value as set of friends for userA
    return res

if __name__=="__main__":
  content = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj.txt") #read the data file from the location
  mutualfriends = content.map(lambda x: x.split("\t")).flatMap(SortedPairKey).reduceByKey(lambda x1, x2: x1.intersection(x2)).sortByKey()
  result = mutualfriends.map(lambda x: (x[0][0] + " , " + x[0][1] + "      " + str(len(x[1])))).collect()
  # split the file on tab, then create sorted key pairs, then reduce the unique keys by finding the intersection(mutual friends) between lists x1 and x2, then sort them by keys and then map the values = userA, userB   count
  print(result)



# COMMAND ----------


