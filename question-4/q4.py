# Databricks notebook source
if __name__=="__main__":
  reviews = sc.textFile("/FileStore/tables/review.csv").map(lambda val: val.split("::"))
  #read the reviews file and split on ::
  
  reviewsSum = reviews.map(lambda val: (val[2], float(val[3]))).reduceByKey(lambda a, b : a + b)
  #map the business id and rating and reduce on business id such that value is the sum of the reviews for a business
  
  reviewsCount = reviews.map(lambda val: (val[2], 1)).reduceByKey(lambda a, b : a + b)
  #map the business id and 1 and reduce on business id such that value is the sum of the counts for a business
  
  reviewsAvgRating = reviewsSum.join(reviewsCount).distinct().map(lambda val : (val[0], val[1][0] / val[1][1]))
  #join the sum and count RDD and map to give business id and average rating
  
  business = sc.textFile("/FileStore/tables/business.csv").map(lambda val: val.split("::")).map(lambda val : (val[0], (val[1] , val[2])))
  # read the business file and spit on :: and then map such that business id is key and pair of address and category is value
  
  result = business.join(reviewsAvgRating).map(lambda val : (val[0], (val[1][0][0] , val[1][0][1] , val[1][1]))).sortBy(lambda val : val[1][2] , ascending = False).take(10)
  #jon the reviewsAvgRating and businessValue RDD and map such that business id is key and value are address, category and average rating and then sort on average rating in descending order and filter top 10 out of them
  # the join gives (business id, ((address, category), average rating)) as list result
  
  print(result)



# COMMAND ----------


