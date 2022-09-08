# Prepare to use Pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Load data into spark
dt = spark.read.csv('/content/ws2_data.csv', header = True, inferSchema = True, )

# Check schema of data
dt.printSchema()

# Check summary of data
dt.summary().show()

# Convert data type from "string" to "timestamp"
from pyspark.sql import functions as f

dt_clean = dt.withColumn("timestamp",
                        f.to_timestamp(dt.timestamp, 'yyyy-MM-dd HH:mm:ss')
                        )

# Show country and we have to check by ourselves
dt_clean.select("Country").distinct().count() # get number of countries
dt_clean.select("Country").distinct().sort("Country").show( 58, False ) # False => don't abbreviate data

# Check data in the row that wrong
dt_clean.where(dt_clean['Country'] == 'Japane').show()

# Convert "Japane" to "Japan" in new column named "CountryUpdate"
from pyspark.sql.functions import when
dt_clean_country = dt_clean.withColumn("CountryUpdate", when(dt_clean['Country'] == 'Japane', 'Japan').otherwise(dt_clean['Country']))
dt_clean = dt_clean_country.drop("Country").withColumnRenamed('CountryUpdate', 'Country') # drop column "Country" and Renamecolumn from "CountryUpdate" to "Country"

#In this data User ID have to contain 8 character. 
#Therefore, we find wrong data uing Regex (all data - correct data) 
dt_correct_userid = dt_clean.filter(dt_clean["user_id"].rlike("^[a-z0-9]{8}$"))  # find correct data
dt_incorrect_userid = dt_clean.subtract(dt_correct_userid) # all data - correct data

#Clean data by convert "ca86d17200" to "ca86d172" 
dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean['user_id'] == 'ca86d17200', 'ca86d172').otherwise(dt_clean['user_id']))
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')

# Check null
dt_nulllist = dt_clean.select([ sum(col(colname).isNull().cast("int")).alias(colname) for colname in dt_clean.columns ])
dt_nulllist.show()

# Check data in the row that user ID is null
dt_clean.where( dt_clean.user_id.isNull() ).show()

# Convert null to "00000000"
dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean['user_id'].isNull(), '00000000').otherwise(dt_clean['user_id']))
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')

# Check outliers
# In this project, outliers is correct data. Therefore, we mustn't change it
dt_clean_pd = dt_clean.toPandas()
sns.boxplot(x = dt_clean_pd['price'])
dt_clean.where( dt_clean.price > 80 ).select("book_id").distinct().show()

# Export data as CSV
dt_clean.write.csv('Cleaned_data.csv', header = True)

