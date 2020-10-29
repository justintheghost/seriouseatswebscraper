# Databricks notebook source
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import time
from datetime import datetime 
import numpy as np
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.column import *
#from pyspark.sql.types import IntegerType

# COMMAND ----------

# Azure and Spark configs
ACCOUNT_KEY="fs.azure.account.key.recipeanalysis2020.dfs.core.windows.net"
ACCOUNT_SECRET_SCOPE="kv-secret-scope-recipe"
ACCOUNT_KEY_NAME="blobstorage-kv"

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set(ACCOUNT_KEY, dbutils.secrets.get(scope = ACCOUNT_SECRET_SCOPE, key = ACCOUNT_KEY_NAME))

# Set up configs for Azure SQL DB
db_conn={}
jdbcHostname = "recipeanalysis.database.windows.net"
jdbcPort = 1433
jdbcUsername="wmp"
jdbcPassword=dbutils.secrets.get(scope = ACCOUNT_SECRET_SCOPE, key = "recipesqldb-kv")
jdbcDatabase = "recipe"

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

db_conn['azureSQL']={'jdbcUrl': jdbcUrl, 'connProperties': connectionProperties}

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC 
# MAGIC val ACCOUNT_SECRET_SCOPE = "kv-secret-scope-recipe"
# MAGIC val jdbcHostname = "recipeanalysis.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcUsername="wmp"
# MAGIC val jdbcPassword=dbutils.secrets.get(scope = ACCOUNT_SECRET_SCOPE, key = "recipesqldb-kv")
# MAGIC val jdbcDatabase = "recipe"
# MAGIC 
# MAGIC def updateSQL (query: String):Unit= {
# MAGIC   val config = Config(Map(
# MAGIC     "url"          -> jdbcHostname,
# MAGIC     "databaseName" -> jdbcDatabase,
# MAGIC     "user"         -> jdbcUsername,
# MAGIC     "password"     -> jdbcPassword,
# MAGIC     "queryCustom"  -> query
# MAGIC   ))
# MAGIC 
# MAGIC   sqlContext.sqlDBQuery(config)
# MAGIC }

# COMMAND ----------

# URL goes in, dictionary (JSON) and failed recipe_source_ids
def scrape_recipe(url_list, recipe_source_ids):
  df = pd.DataFrame(columns=['recipe_name','recipe_source_id','url','recipe_about_section','recipe_published_date','recipe_last_updated_date','recipe_ingredients','recipe_categories','data_pulled_date'])
  schema = StructType([StructField('recipe_name', StringType())
    , StructField('recipe_source_id',IntegerType())
    , StructField('url',StringType())
    , StructField('recipe_about_section',MapType(StringType(),StringType()))
    , StructField('recipe_published_date',StringType())
    , StructField('recipe_last_updated_date',StringType())
    , StructField('recipe_ingredients',ArrayType(MapType(StringType(),MapType(StringType(),StringType()))))
    , StructField('recipe_categories',ArrayType(StringType()))
    , StructField('data_pulled_date',StringType())
  ])
  failed_recipes = []
  
  # Set up iterator to go through recipe_source_ids array
  recipe_source_id_iter = 0
  for url in url_list:
    try:
      # Scrape site and capture specific data elements
      print("Requesting data from " + url)
      r = requests.get(url, headers={"User-Agent":"justin.kolpak@gmail.com (Ethical Web Scraper). Email me for details."})
      data = BeautifulSoup(r.text, 'html.parser')
      recipe_section = data.find("div", {"class":"recipe-ingredients"})
      ingredients_list = recipe_section.find_all("li",{"class":"ingredient"})
      recipe_name = data.find("h1", {"class":"recipe-title"}).get_text()
      recipe_source_id = int(recipe_source_ids[recipe_source_id_iter])

      # Capture the "recipe about" section as an dictionary; not all recipes contain the same information
      recipe_about_section = data.find("ul", {"class":"recipe-about"})
      recipe_about_section_key = recipe_about_section.find_all("span", {"class":"label"})
      recipe_about_section_value = recipe_about_section.find_all("span", {"class":"info"})
      recipe_about_dict = {}
      
      for i in range(0,len(recipe_about_section_key)):
          key = recipe_about_section_key[i].get_text()
          key = key.replace(":","")
          value = recipe_about_section_value[i].get_text()
          recipe_about_dict[key]=value
      
      # Get dates; if not updated, it won't show up in the HTML - error handling for this
      recipe_publish_section = data.find("div", {"class":"pubmod-date"})
      recipe_published_date = recipe_publish_section.find("span", {"class":"publish-date"}).get_text()
      if recipe_publish_section.find("span", {"class":"modified-date"}) is not None:
          recipe_last_updated_date = recipe_publish_section.find("span", {"class":"modified-date"}).get_text()
      else: 
          recipe_last_updated_date = "N/A"

      # Iterate through ingredients list and build an ingredient dictionary    
      recipe_ingredients = []
      ingred_seq = 1
      for i in ingredients_list:
          ingredient_name = i.get_text()
          if i.find("strong") is not None:
              ingredient_type = "Section Header"
          else:
              ingredient_type = "Ingredient"

          if i.find("a") is not None:
              ingredient_linked_recipe = i.a['href']
          else:
              ingredient_linked_recipe = None

          ingredient_dict = {"ingredient": {"ingredient_name": ingredient_name
            , "ingredient_type":ingredient_type
            , "ingredient_linked_recipe":ingredient_linked_recipe
            , "ingredient_sequence": ingred_seq}
            }

          ingred_seq = ingred_seq+1

          recipe_ingredients.append((ingredient_dict))
      
      # Get the different categories that are associated with the recipe
      recipe_categories = []
      category_section = data.find("div", {"class":"breadcrumbs__more"})
      category_list = category_section.find_all("li", {"class":"label label-category"})
      for last_category in category_list:
        if last_category.find("strong") is not None:
          recipe_categories.append(last_category.a['href'])
        else:
          pass
      
      # Build pandas DF one row at a time
      recipe_list = list(zip([recipe_name], [recipe_source_id],[url], [recipe_about_dict],[recipe_published_date],[recipe_last_updated_date],[recipe_ingredients],[recipe_categories],[str(datetime.now())]))
      recipe = pd.DataFrame(recipe_list, columns=['recipe_name','recipe_source_id','url','recipe_about_section','recipe_published_date','recipe_last_updated_date','recipe_ingredients','recipe_categories','data_pulled_date'])

      recipe['recipe_source_id'] = int(recipe['recipe_source_id'])
      print("Adding " + recipe_name + " to dataframe...")
      df = df.append(recipe)  

      print("Waiting 10 seconds until scraping next recipe...")
      time.sleep(10)

      recipe_source_id_iter = recipe_source_id_iter + 1
      
      spark_df = spark.createDataFrame(df, schema)
      
    except:
      spark_df = spark.createDataFrame(df, schema)
      print("Recipe scraping for " + url + " failed...")
      failed_recipes.append(recipe_source_ids[recipe_source_id_iter])
      
  return spark_df, failed_recipes

# COMMAND ----------

def write_to_blob(df):
  batch_id = spark.read.jdbc(url=db_conn['azureSQL']['jdbcUrl'], table="(SELECT COALESCE(MAX(batch_id),0) + 1 AS batch_id FROM etl.batch) a", properties=db_conn['azureSQL']['connProperties']).collect()[0][0]
  
  #recipe_name = str(df.select("recipe_name").collect()[0]["recipe_name"])
  df.coalesce(1).write.mode('overwrite').json("abfss://recipes@recipeanalysis2020.dfs.core.windows.net/temp/" + "recipe_batch-"+ str(batch_id) + ".json")
  print("Writing file to " + "abfss://recipes@recipeanalysis2020.dfs.core.windows.net/" + "recipe_batch-"+ str(batch_id) + ".json")
  return("abfss://recipes@recipeanalysis2020.dfs.core.windows.net/temp/" + "recipe_batch-"+ str(batch_id) + ".json", batch_id)

# COMMAND ----------

def rename_file(file_path, batch_id):
  print("file_path: " + file_path)
  write_path = "abfss://recipes@recipeanalysis2020.dfs.core.windows.net/"
  file_list = dbutils.fs.ls(file_path) #### List out all files in temp directory
  
  #grabs the folder with the data
  for i in file_list:
    if i.name.startswith("part-00000"): #### find your temp file name 
      file_name = i.name
  
  new_file_path = write_path+"recipe_batch-"+ str(batch_id) +".json"
  dbutils.fs.mv(file_path + "/" + file_name, new_file_path)
  print("Moving file to " + file_path + "/" + file_name, new_file_path)
  #removes the empty folder
  dbutils.fs.rm(file_path, recurse= True)
  print("Deleting folder " + file_path + "...")
  dbutils.fs.rm(write_path + "temp", recurse= True)
  print("Deleting file " + write_path + "temp...")
  return new_file_path

# COMMAND ----------

def write_to_batch(batch_id, file_name):
  batch_data = list(zip([batch_id], [file_name], [datetime.now()], "P"))
  batch_df = pd.DataFrame(batch_data, columns = ["batch_id","file_name","batch_datetime","batch_status_code"])
  batch_df_spark = spark.createDataFrame(batch_df)
  
  batch_df_spark.write.mode("append").jdbc(url=db_conn['azureSQL']['jdbcUrl'], table="etl.batch", properties=db_conn['azureSQL']['connProperties'])

  return True

# COMMAND ----------

def write_to_recipe_batch(recipe_source_ids, batch_id):
  next_recipe_source_batch_id = spark.read.jdbc(url=db_conn['azureSQL']['jdbcUrl'], table="(SELECT COALESCE(MAX(recipe_source_batch_id),0) + 1 AS batch_id FROM etl.recipe_source_by_batch) a", properties=db_conn['azureSQL']['connProperties']).collect()[0][0]
  recipe_source_batch_df = pd.DataFrame(recipe_source_ids, columns=["recipe_source_id"])
  recipe_source_batch_df['recipe_source_batch_id'] = recipe_source_batch_df.index + next_recipe_source_batch_id
  
  recipe_source_batch_df['batch_id'] = batch_id
  recipe_source_batch_df['recipe_source_batch_datetime'] = datetime.now()

  recipe_source_batch_df_spark = spark.createDataFrame(recipe_source_batch_df)
    
  recipe_source_batch_df_spark.write.mode("append").jdbc(url=db_conn['azureSQL']['jdbcUrl'], table="etl.recipe_source_by_batch", properties=db_conn['azureSQL']['connProperties'])

  return True

# COMMAND ----------

# Orchestration 
recipes_to_process = spark.read.jdbc(url=db_conn['azureSQL']['jdbcUrl'], table="(SELECT recipe_link, recipe_source_id FROM etl.recipe_source WHERE ready_to_process='Y') a", properties=db_conn['azureSQL']['connProperties'])

# Need to capture result of collect() as numpy array, then convert to regular array
url_array_np = np.array(recipes_to_process.select("recipe_link").collect())
url_array = []
for i in url_array_np:
    i = np.char.replace(i, "]", "")
    i = np.char.replace(i, "[", "")
    url_array.append(str(i[0]))

# Need to capture result of collect() as numpy array, then convert to regular array
recipe_source_ids_np = np.array(recipes_to_process.select("recipe_source_id").collect())
recipe_source_ids = []
for i in recipe_source_ids_np:
    recipe_source_ids.append(int(i))    
    
scrape_recipe_results= scrape_recipe(url_array, recipe_source_ids)

failed_recipe_source_ids = scrape_recipe_results[1]
failed_recipe_source_ids_sql = ""
if len(failed_recipe_source_ids) != 0:
    failed_recipe_source_ids_sql = str(failed_recipe_source_ids).replace("[","(").replace("]",")")
    if failed_recipe_source_ids_sql is None: 
      failed_recipe_source_ids_sql = "(0)"
else:
    pass
  
scrape_recipe_result_df = scrape_recipe_results[0]
print(scrape_recipe_result_df)
blob_file = write_to_blob(scrape_recipe_result_df)[0]
batch_id = write_to_blob(scrape_recipe_result_df)[1]
new_file_path = rename_file(blob_file, batch_id)

write_to_batch(batch_id, new_file_path)
write_to_recipe_batch(recipe_source_ids, batch_id)

# Prepare UPDATE statement for the recipes that failed; create temp table (only way to pass variables from Python to Scala)
update_sql_failed = "UPDATE etl.recipe_source SET ready_to_process='F' WHERE recipe_source_id IN " + failed_recipe_source_ids_sql
update_sql_failed_row = Row(update_sql_failed)
update_sql_failed_df = spark.createDataFrame([update_sql_failed_row], ['sql'])
update_sql_failed_df.registerTempTable("update_sql_failed")

# COMMAND ----------

# MAGIC %scala 
# MAGIC // Update recipe_source with the sources it just processed, changing ready_to_process to "C" for completed
# MAGIC val update_sql_query_complete = "UPDATE etl.recipe_source SET ready_to_process='C' WHERE recipe_source_id IN (SELECT recipe_source_id FROM etl.recipe_source WHERE ready_to_process='Y')"
# MAGIC 
# MAGIC updateSQL(update_sql_query_complete)
# MAGIC 
# MAGIC 
# MAGIC val failed_recipe_source_ids_sql = spark.sql("select * from update_sql_failed").first().getString(0)
# MAGIC updateSQL(failed_recipe_source_ids_sql)
# MAGIC 
# MAGIC print("Updating etl.recipe_source status code to complete...")

# COMMAND ----------

