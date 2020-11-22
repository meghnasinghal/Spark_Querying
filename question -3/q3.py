# Databricks notebook source
if __name__=="__main__":
  businessValues = sc.textFile("/FileStore/tables/business.csv").map(lambda val: val.split("::")) 
  #getting data from data file and splitting on :: delimiter
  
  filteredBusiness = businessValues.filter(lambda x: "stanford" in x[1].lower()).map(lambda val:(val[0], val[2]))
  #filtering business based on stanford in address and then mapping the values such that business id is key and category as value
  
  reviews = sc.textFile("/FileStore/tables/review.csv").map(lambda val: val.split("::")).map(lambda val: (val[2], (val[1], val[3])))
  #reading the review file and splitting on :: and then mapping such that business id is key and user id and rating is value
  
  result = filteredBusiness.join(reviews).distinct().map(lambda val: (val[1][1][0], val[1][1][1])).collect()
  #joining the business and reviews and getting the user id and rating for the business
  
  print(result)


