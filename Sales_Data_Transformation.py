# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load('/Volumes/workspace/default/salesvolume/BigMart_Sales.csv')
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ###  1.Select

# COMMAND ----------


# df1.select('Item_Identifier','Item_Type','Item_MRP','Item_Outlet_Sales').display()
# OR
df1.select(col('Item_Identifier'),col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Alias

# COMMAND ----------


df1.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter/Where

# COMMAND ----------


# 3.1 Filtering the data with fat content = Regular
df1.filter(col('Item_Fat_Content')=='Regular').display()
                                                                                                                                                                                                                                                                                                                                                                                                                                               

# COMMAND ----------

# 3.2 Slice the data with item type = soft drinks and weight < 10
df1.filter((col('Item_Type')== 'Soft Drinks') & (col('Item_Weight')<10)).display()     

# COMMAND ----------

# 3.3 Fetch the Data with Tier in (Tier 1 or Tier 2) and Outlet Size is Null
df1.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.withColumnRenamed
# MAGIC

# COMMAND ----------

df1.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-1
# MAGIC Creating a new column named flag and inserting the data as new for each item

# COMMAND ----------

df1.withColumn('flag',lit("new")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenarion-2
# MAGIC Multiplying the Item_Weight and Item_MRP 
# MAGIC and storing this multiplied data into new column named multiply

# COMMAND ----------

df1.withColumn('Multiply',col('Item_Weight') * col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario 3
# MAGIC Changing the data inside the columns like example 'Regular' to 'Reg' 
# MAGIC using "regexp_replace"

# COMMAND ----------

df1.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario 4
# MAGIC Changing the data inside the columns like example 'Low Fat' to 'LF' 
# MAGIC using "regexp_replace"

# COMMAND ----------

df1.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","LF")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario - 5
# MAGIC To Transform more than one data inside the same column or in the different Column
# MAGIC Then we should follow like this

# COMMAND ----------

df1.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","LF")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting
# MAGIC Converting the columns Datatype into Another Datatype

# COMMAND ----------

df1.withColumn('Item_Weight',col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SortBy/Orderby

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-1
# MAGIC Converting the data in the Item_Weight Column in the descending order

# COMMAND ----------

df1.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-2
# MAGIC Converting all the Data inside the Item_Visibility into the Ascending Order

# COMMAND ----------

df1.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-3
# MAGIC Converting the data in multiple columns using boolean values

# COMMAND ----------

df1.sort(['Item_Weight','Item_Visibility'],ascending=[0,0]).display()
# Here the ascending=[0,0] is sorting the data inside the columns in a=descending order
# ascending=[0]=Descending
# ascending=[1]=Ascending  
# means 0 = False
#       1 = True

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-5
# MAGIC Converting one colums into descending order and one into ascending order

# COMMAND ----------

df1.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).display()
# Here ascending=[0]=Descending i.e is the first column i.e 'Item_Weight'
# Here ascending=[1]=Ascending i.e is the second column i.e 'Item_Visibility'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit
# MAGIC This is used to return the specific number of records from the data
# MAGIC
# MAGIC

# COMMAND ----------

df1.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

# MAGIC %md
# MAGIC This is a method as same as in the SQL which is used to drop the columns or tables from the data

# COMMAND ----------

# MAGIC %md
# MAGIC As of now iam not running this method because the data will get dropped and we need to use the same data for the further uses

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intermediate Methods in Data Transformation Using PySpark
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Drop_Duplicates

# COMMAND ----------

df1.dropDuplicates().display 

# COMMAND ----------

# MAGIC %md
# MAGIC  Scenario-2 (Used to Drop th eduplicates under the Specified Column)

# COMMAND ----------

df1.drop_duplicates(subset=['Item_type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Union and UnionBy

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing the Special Data for this method in the form of code Using this PySpark

# COMMAND ----------

#This is the way to create the data using the Pyspark in Python 
data1=[('1','Nithin'),
       ('2','Sashi')]
schema1 = 'id String , name String'

df2 = spark.createDataFrame(data1,schema1)

data2=[('3','Rahul'),
       ('4','Jas')]
schema2 = 'id String , name String'

df3 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df2.display()

# COMMAND ----------

df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union

# COMMAND ----------

df2.union(df3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### To use Union by we need to Change the pattern of the data to see the difference

# COMMAND ----------

data1=[('Nithin','1'),
       ('Sashi','2')]
schema1 = 'id String , name String'
df2 = spark.createDataFrame(data1,schema1)

# COMMAND ----------

df2.union(df3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now Performing the Operation UnionBy
# MAGIC Now it should return the names as seperate and the id's as separate like above 

# COMMAND ----------

df2.unionByName(df3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Initcap()
# MAGIC This Function is used to convert the First letter in the each word in the column to the Upper Case

# COMMAND ----------

df1.select(initcap('Item_type')).display()
# Here It Converted the First letter of the each word in the Item_type column
# Like example we have Fruits and Vegetables in the Item_type and it is converted into Fruits And Vegetables 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Upper()

# COMMAND ----------

df1.select(upper('Item_type').alias('Upper_Item_Type')).display()
#It Converts the Each and Every letter in the specified column into the Upper Case

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Lower()

# COMMAND ----------

# df2.select(lower('Item_Fat_Content').alias('Lower_Fat_Content')).display()
#This Gives the data in the lowercase on the selected column 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Current_Date()

# COMMAND ----------

df1.withColumn('Current_Date',current_date()).display()
# This Displays the Current Date in the 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Date_Add()
# MAGIC This is used to add the number of days from the current_date into a nwe column

# COMMAND ----------

df1.withColumn('Current_Date',current_date())\
    .withColumn('week_after',date_add('current_date',7)).display()
    #This is Used to Add the Number of days and displaying the adding date
    #and i have imported the current date to compare the current_date and the week_after column

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Data_Sub
# MAGIC This is Used to see the previous week/month date by subtracting the number of days from the current_date

# COMMAND ----------

df1.withColumn('Current_Date',current_date())\
    .withColumn('week_before',date_sub('current_date',7)).display()
#This is Used to Subtract the Number of days and displaying the week_before date
#and i have imported the current date to compare the current_date and the week_before column

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.DateDiff
# MAGIC This is used to specify the number of days between the 2 dates

# COMMAND ----------

# df1.withColumn('Current_Date',current_date())\
#     .withColumn('week_after',date_add('current_date',7)).display()\
#     .withColumn('datediff',datediff('week_after','current_date')).display()
#This returns the number of days between the specified start date and the end date

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Date Format
# MAGIC This is used to change the format of the Date\
# MAGIC By Default it will show the date as yyyy-mm-dd\
# MAGIC But we can show the date format like how we wanted bu tin india we use the format like dd-mm-yyyy

# COMMAND ----------

df1.withColumn('current_date',date_format('current_date','dd-MM-yyyy')).display()
#It Displays the Current Date in the dd-MM-yyyy format

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling NULL's
# MAGIC These are used to hadling NULL's like\
# MAGIC Replacing the NULL Values with the other data or\
# MAGIC Deleting the Data which contains the NULL values

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-1
# MAGIC Dropping NULLS

# COMMAND ----------

df1.dropna('all').display()
#This is Used to Drop the Record inside the data only when all the records in the data (i.e specified column)containing nulls.
#This deletes the entire column/record inside the data by using this method 

# COMMAND ----------

df1.dropna('any').display()
#This deletes the record where it contains NULL Values inside the data
#For Example from the raw data in the 8th record (Item_Type) we are having the null value so it gonna delete the entire 8th record from the data

# COMMAND ----------

df1.dropna(subset=['Outlet_Size']).display()
#This is used to delete the record when containing the null values under the mentioned column
#Here we used the column called 'Outlet_size' so it gonna delete the entire record where the null values are present in the above mentioned column

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-2\
# MAGIC Filling the NULL Values With the required data like NA
# MAGIC

# COMMAND ----------

df1.fillna('NotAvailable').display()
#This returns the data by replacing with the above mentioned text in the place of having nulls in the entire Data

# COMMAND ----------

df1.fillna('NotAvailable',subset=['Outlet_size']).display()
#This will replace the text with the above mentioned text in the place of having nulls in the specified column  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split and Indexing
# MAGIC Split : This can be done only when there is a space between tht words\
# MAGIC Indexing : This is used to access the data from the splitted data to retrive the data this can be only done when there is only the data is splitted

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-1 \
# MAGIC     Splitting the data

# COMMAND ----------

df1.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()
#If we use this we can see that the record in the Outlet_size column is splitted into two values
#With these splitted values we can perform the Indexing Operations in the data

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-2\
# MAGIC Indexing

# COMMAND ----------

df1.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()
#This is the way of Accessing the data using the Indexing Method 
#This Indexing method is as same as in thr Programming Langauge
#As we see clearly in the Query that we have mentioned a number under the Square Brackets which is the index value for the splitted data
#We can see in the output that the records under the Outlet_Type column is the accessed value from the above query that is the [1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode Function
# MAGIC This Function is used to merge the splitted data into the normal string

# COMMAND ----------

df_exp=df1.withColumn('Outlet_Type',split('Outlet_Type',' '))
display(df_exp)
#To Use the Explode Function firstly the data should be in the form of splitted right
# So here we splitted the data
# Now we can perform the explode operation 
# As we can see the record in the Outlet_Type column is splitted into two values

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()
# This is the way to use the Explode Function which is used to merge the splitted data into the single column
# As we can see the output the data in the specified Column is now merged into the single column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Array_Contains
# MAGIC This is a method which is used to check wheather the data mentioned in the query is available in the specified array or not\
# MAGIC It will return the boolean value if the specified element is present in the array

# COMMAND ----------

df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()
# Here we created an seperate column to check whether the specified value is present in the specified column or not by displaying the record as True or false
# And in the Query the array_contains method uses the 2 columns one is to specifying the column and another is the searching word in the record
# and Then it compares the values and returns the value as True or False in the seperately created column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group By
# MAGIC This is as same as the function that we used in the SQL\
# MAGIC This is Used only when we want to aggregate the data

# COMMAND ----------

# MAGIC %md
# MAGIC Senario - 1\
# MAGIC Grouping the Item_Type column with the Item_MRP with each record

# COMMAND ----------

df1.groupBy('Item_Type').agg(sum('Item_MRP')).display()
# Here We used group by sunction on column called Item_Type and then we used the aggregation function called sum on the column called 'Item_MRP'
# Now the Item_Type is displayed according to the sum of the MRP of the Items from the data
# This is the use of the GroupBy 

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario - 2\
# MAGIC Grouping the Item_Type column with the Item_MRP with each record(But Using the Different Aggregation Function)
# MAGIC

# COMMAND ----------

df1.groupBy('Item_Type').agg(avg('Item_MRP')).display()
# NOw here we used another Aggregation Function called the Average Function which is used to find the average of the specified Column by using the groupBy column
# Now it displays the average MRP of the Item_Type

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario - 3\
# MAGIC Grouping the 3 Columns\
# MAGIC Item_Type and Item_MRP and Outlet_Size\
# MAGIC Here we use the 2 GroupBy columns called 'Item_Type' and "Outlrt_Size' and using aggregation function under 'Item_MRP' Column

# COMMAND ----------

df1.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()
# As we can see from the Query We applied the GroupBy on two columns and then we used the Aggregation Function called sum on the column called 'Item_MRP'
# Now it displays the sum of the MRP of the Item_Type according to the Outlet_Size

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario - 3\
# MAGIC Grouping the 3 Columns\
# MAGIC Item_Type and Item_MRP and Outlet_Size \
# MAGIC Here we use the 2 GroupBy columns called 'Item_Type' and "Outlrt_Size' and using two aggregation function under 'Item_MRP' Column  

# COMMAND ----------

df1.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'),avg('Item_MRP').alias('Average_MRP')).display()
# As we can see from the Query We applied the GroupBy on two columns and then we used the  2 Aggregation Function called sum on the column called 'Item_MRP' and the avg on the same column called 'Item_MRP'
# Now it displays the sum of the MRP of the Item_Type  and the Average of the Item_Type according to the Outlet_Size

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advance Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Collect List 
# MAGIC This is like alternative function of Group Concat Available in Mysql

# COMMAND ----------

#Now Before Performing this function Lets create a new DataFrame
data=[('user1','book1'),
      ('user1','book2'),
      ('user2','book2'),
      ('user2','book4'),
      ('user3','book1')]
schema='user string,book string'
df_book_users=spark.createDataFrame(data,schema)

df_book_users.display()
      


# COMMAND ----------

df_book_users.groupBy('user').agg(collect_list('book')).display()
# This groups by every User and creates a list holding all values associated with that particular User
#Here The User1 reads book1 and book2 then it creates a list where user 1 read what books same like for all the remaining users

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Pivot

# COMMAND ----------

df1.groupby('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()
#By Using This We come to know the example like
#What is the Average MRP of the Item_Type according to the Outlet_Size
#Example Like The average Price of the Diary Item_Type in the High Oulet_Size is 153.50917249999995

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.When Otherwise
# MAGIC This is like alternative function of Case Statements Available in Mysql

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-1

# COMMAND ----------

df1.withColumn('Veg_Flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg')).display()
#This Creates a new Column named Veg_Flag and gives the records Non-Veg if the Item_Type is Meat and Veg if the Item_Type is not Meat

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-2

# COMMAND ----------

# df1.withColumn('Veg_exp_Flag',when(((col('Veg_Flag')=='Veg') & (col('Item_MRP')<100)),'Veg_Inexpensive')\
#                              .when(((col('Veg_Flag')=='Non-Veg') & col('Item_MRP')<100),'Non-Veg_Inexpensive')\
#                              .otherwise('Non_Veg')).display()
#This Creates the Column named Veg_exp_Flag and gives the records Veg_Inexpensive if the Veg_Flag is Veg and Item_MRP is less than 100 and Non-Veg_Inexpensive if the Veg_Flag is Non-Veg and Item_MRP is less than 100 and Non_Veg

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.Joins
# MAGIC 1.Inner Join\
# MAGIC 2.Left Join\
# MAGIC 3.Right Join\
# MAGIC 4.Anti Join

# COMMAND ----------

#Now Before Performing this function Lets create a new DataFrame
dataj1=[('1','Gaur','d01'),
        ('2','Kit','d02'),
        ('3','Sam','d03'),
        ('4','Tim','d03'),
        ('5','Aman','d05'),
        ('6','Nad','d06')]
schemaj1='emp_id string,emp_name string,dept_id string'

df_emp=spark.createDataFrame(dataj1,schemaj1)

dataj2=[('d01','HR'),
        ('d02','Marketing'),
        ('d03','Accounts'),
        ('d04','IT'),
        ('d05','Finannce')]
schemaj2='dept_id string,department string'

df_dept=spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df_emp.display()
df_dept.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 4.1 Inner Join

# COMMAND ----------

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'inner').display()
#This is As same as the Normal Inner Join in the Mysql which is used to join the two tables based on the common column
#It Returns the Common Records on the both the tables

# COMMAND ----------

# MAGIC %md
# MAGIC 4.2 Left Join

# COMMAND ----------

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'left').display()
#This is As same as the Left join in the MYSQL which returns the all the values of the left side
#If the left Side record doesnot contain any missing values form above then it returns the null values

# COMMAND ----------

# MAGIC %md
# MAGIC 4.3 Right Join

# COMMAND ----------

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'right').display()
#This is As same as the Right join in the MYSQL which returns the all the values of the right side
#If the right Side record doesnot contain any missing values form above then it returns the null values

# COMMAND ----------

# MAGIC %md
# MAGIC 4.4 Anti Join\
# MAGIC It is used to Fetch the Records that are available on the dataframe 1\
# MAGIC Thats mean we want all the records which are not matched with the second dataframe
# MAGIC

# COMMAND ----------

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'anti').display()
#As We can See there is no Record matching in the Dtaframe 1 with the DataFrame 2
#Hence It is Returned in the Output

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.Window Functions
# MAGIC 1.Row Number()\
# MAGIC 2.Rank()\
# MAGIC 3.Dense_Rank()\
# MAGIC 4.Cumulative Sum()

# COMMAND ----------

# MAGIC %md
# MAGIC 5.1 Row_Number()\
# MAGIC It will Not think about the Duplicates in the dataframe and it keeps asigning the numbers in the Records

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.window import Window
df1.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()
#This Assigns The Number for each record in the Dataframe without any Partition means 
#It will not think about the duplicates it juts gives the numbering to each record

# COMMAND ----------

# MAGIC %md
# MAGIC 5.2 Rank()\
# MAGIC It Will Think About the Duplicates and assign the Values According to the Duplicates in the Records Avilable in the DataFrame
# MAGIC

# COMMAND ----------

df1.withColumn('rank',rank().over(Window.orderBy('Item_Identifier'))).display()
#This Rank Check about the Duplicates
#First It Checks the Specified Column wheather the records in the Specifed Column contains Duplicates or not
#If it Contains the Duplicates it will get the rank as same as the above rank in the rank column
#For Example the first record in the specified Column contains duplicates until the positio  upto 7 
#Hence it is retuned 6 1's in the rank column and it returns the 7 according to the original rank
#Here in this rank it gives the next rank according to the adding each duplicate position too

# COMMAND ----------

# MAGIC %md
# MAGIC 5.3 Dense Rank()\
# MAGIC This is as same as the Rank Function and the main difference is the in the rank function it counts the duplicate position and returns the next value\
# MAGIC And in this Dense Rank it retuns the next values according to the assigned dense rank Value

# COMMAND ----------

df1.withColumn('rank',rank().over(Window.orderBy('Item_Identifier')))\
    .withColumn('denserank',dense_rank().over(Window.orderBy(col('item_Identifier')))).display()
    #With this we can compaer the difference between the RAnk and the Dense rank Function in the Pyspark 
    #If you not even get the Clarity on this once run this shell and compare the 2 coulmns called rank and the denserank

# COMMAND ----------

# MAGIC %md
# MAGIC 5.4 Cumulative Sum()\
# MAGIC It is Used to calculate the Cumulative sum of the Specified Column \
# MAGIC Cumulative sum in the Sense like i will explain in the Query

# COMMAND ----------

df1.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()
#This is One of the sample Query which creates the column named cumsum and in that cumsum it will return the sum of MRP of the Item_Type
#But we want like adding one by one record mrp from the dataframe so we should do this

# COMMAND ----------

df1.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()
#See As we can see in the output forst it will retun the item_mrp of the first record and it will the added with the seconf=d record and the sum of 1st record and the 2nd record will be returned in the second record under the column called cumsum
#This is Got by using this "rowsBetween(Window.unboundedPreceding,Window.currentRow)"

# COMMAND ----------

df1.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()
#This Returns the Sum of the all the Item_MRP No matter according to the Item_Type
#This is Got by using this "rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
#In the Output as we can see there is value under the cumsum column for all the records
#This is because it is the sum of all the values of the Item_MRP Column no matter the type of the Item_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Functions 
# MAGIC This Includes Several Steps Let us discuss one by one\
# MAGIC This is not much Important just remember this like a topic no need to user these functions further

# COMMAND ----------

# MAGIC %md
# MAGIC Step-1\
# MAGIC Creating a Function

# COMMAND ----------

def my_fun(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC Step-2\
# MAGIC Creating a Function number for storing the function which is used to display\
# MAGIC This step is like a creating a variable and storing the created function inside the variable

# COMMAND ----------

my_udf=udf(my_fun)

# COMMAND ----------

# MAGIC %md
# MAGIC Step-3\
# MAGIC Using the Stpe 2 under the dataframe and using the function on the specified column

# COMMAND ----------

df1.withColumn('my_new_column',my_udf('Item_MRP')).display()
#This Creates the New column named my_new_column and in that column it will return the square of the Item_MRP Column
#The square of the Item_MRP is returned in the my_new_column through the user_defined_function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Sql
# MAGIC This is the one Where the user uses the Sql Langauge To deal with the data like performing operations on the Data\
# MAGIC There Will be no performance Behaviour using this pyspark or the sparksql\
# MAGIC In this Scenario we use to a temporary View to deal with this Spark Sql\
# MAGIC This Temporary View is a view which will be deleted after the session is closed or notebook is closed 

# COMMAND ----------

# MAGIC %md
# MAGIC Step-1\
# MAGIC Creating a Temporary View

# COMMAND ----------

# df1.createTempView('df_sql')
#We Cannot Run this Multiple Times because the temporary view is only created once and it will remain until the session closes
#If we run multiple ways it will throw an error like "Cannot create the temporary view `df_sql` because it already exists."

# COMMAND ----------

# MAGIC %md
# MAGIC Step-2\
# MAGIC Retriving the Data using the SQl Langauge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_sql;
# MAGIC -- This is the Query used to Retrive all The records form the above dataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_sql
# MAGIC where Item_Fat_Content = 'Low Fat';
# MAGIC -- This Returns the Data where the Item_Fat_Content is Low Fat

# COMMAND ----------

# MAGIC %md
# MAGIC This is all about performing the operations under the Temporary view Which is closed after the session ends \
# MAGIC If we want to continue the thorought the task then using this method to perfom operations using sql\
# MAGIC By using This Method we can Convert the Temporary View into the dataframe where it will not collapse after the session ends

# COMMAND ----------

df_sql_main=spark.sql("select * from df_sql where Item_Type='Fruits and Vegetables'")

# COMMAND ----------

df_sql_main.display()