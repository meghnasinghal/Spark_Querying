# Databricks notebook source
if __name__=="__main__":
  business = sc.textFile("/FileStore/tables/business.csv").map(lambda val: val.split("::"))
  #read the business file and split on ::
  
  category = business.flatMap(lambda x: x[2][5:-1].replace(', ', ',').split(","))
  # map the values such that category is key split on , (took substring of category to remove '(List' )
  
  result = category.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).map(lambda x : (x[0].strip(), x[1])).sortBy(lambda x : x[1], ascending = False).take(10)
  #for each key map (category, 1) and then reduce to take the sum. Further mapping is done to trim the category and then sort based on the number of business and take the top 10
  
  print(result)

 

# COMMAND ----------


