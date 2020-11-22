# Databricks notebook source
if __name__=="__main__":
  business = sc.textFile("/FileStore/tables/business.csv").map(lambda val: val.split("::"))
  #read the file and split on  ::
  
  category = business.flatMap(lambda x: x[2][5:-1].replace(', ', ',').split(","))
  # map the values such that category is key split on , (took substring of category to remove '(List' )
  
  result = category.map(lambda x:(x, 1)).reduceByKey(lambda a, b: a + b).map(lambda x : (x[0].strip(), x[1])).collect()
  #for each key map (category, 1) and then reduce to take the sum. Further mapping is done to trim the category
  
  print(result)

 

# COMMAND ----------



# COMMAND ----------


