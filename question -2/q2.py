# Databricks notebook source
def SortedPairKey(value):
    userA = value[0] # user A is the userid
    friends = value[1].split(",") # getting list of friends
    res = [] #list
    for userB in friends: #itertaing over list of friends
        if userA!='' and userB!='': 
            key = sorted([userA, userB]) #creating sorted key pair of userA and userB
            res.append((tuple(key), set(friends))) # output key as tuple of sorted friends and value as set of friends for userA
    return res

if __name__=="__main__":
  content = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj.txt") #read the data file from the location
  mutualfriends = content.map(lambda x: x.split("\t")).flatMap(SortedPairKey).reduceByKey(lambda x1, x2: x1.intersection(x2)).sortByKey()
  #split the file based on tab and then make sorted pair of friends and then find the mutual friends by intersection 
  
  count_mutual = mutualfriends.map(lambda x: ((x[0][0], x[0][1]), len(x[1])))
  # find the count of mutual friends by mapping the user 
  
  sorted_count = count_mutual.sortBy(lambda x: x[1], False).collect()
  # sorted the count_mutual based on count and convert to list
  
  max_mutual_frnds = sorted_count[0][1]
  #get the max number of mutual friends
  
  maxFriendUsers = count_mutual.filter(lambda x : x[1] ==  max_mutual_frnds).map(lambda x : x[0][0] + "," + x[0][1] + "     "  + str(x[1])).collect()
  #filter the count based onn the max number of mutual friends
  
  print(maxFriendUsers)



# COMMAND ----------


