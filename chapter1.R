ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE)
  try(sapply(pkg, require, character.only = TRUE), silent = TRUE)
}
packages <- c("foreach", "doParallel", 
              "boot", "sparklyr")
ipak(packages)

######
## If you wish to install Spark on your local system, 
## simply install the sparklyr package and call spark_install()


## Exercise 1
# Load sparklyr
library(sparklyr)

# Connect to your Spark cluster
spark_conn <- spark_connect(master = "local")

# Print the version of Spark
spark_version(sc = spark_conn)

# Disconnect from Spark
spark_disconnect(sc = spark_conn)


## Exercise 2
# Load dplyr
library(dplyr)

# Explore track_metadata structure
str(track_metadata)

# Connect to your Spark cluster
spark_conn <- spark_connect(master = "local")

# Copy track_metadata to Spark
track_metadata_tbl <- copy_to(spark_conn, track_metadata, overwrite = TRUE)

# List the data frames available in Spark
src_tbls(spark_conn)

# Disconnect from Spark
spark_disconnect(spark_conn)

## Exercise 3
# Big data, tiny tibble
# In the last exercise, when you copied the data to Spark, copy_to() returned a value. This return value is a special kind of tibble() that doesn't contain any data of its own. To explain this, you need to know a bit about the way that tidyverse packages store data. Tibbles are usually just a variant of data.frames that have a nicer print method. However, dplyr also allows them to store data from a remote data source, such as databases, and – as is the case here – Spark. For remote datasets, the tibble object simply stores a connection to the remote data. This will be discussed in more detail later, but the important point for now is that even though you have a big dataset, the size of the tibble object is small.
# 
# On the Spark side, the data is stored in a variable called a DataFrame. This is a more or less direct equivalent of R's data.frame variable type. (Though the column variable types are named slightly differently – for example numeric columns are called DoubleType columns.) Throughout the course, the term data frame will be used, unless clarification is needed between data.frame and DataFrame. Since these types are also analogous to database tables, sometimes the term table will also be used to describe this sort of rectangular data.
# 
# Calling tbl() with a Spark connection, and a string naming the Spark data frame will return the same tibble object that was returned when you used copy_to().
# 
# A useful tool that you will see in this exercise is the object_size() function from the pryr package. This shows you how much memory an object takes up.



# A Spark connection has been created for you as spark_conn. The track metadata for 1,000 tracks is stored in the Spark cluster in the table "track_metadata".
# 
# Link to the "track_metadata" table using tbl(). Assign the result to track_metadata_tbl.
# See how big the dataset is, using dim() on track_metadata_tbl.
# See how small the tibble is, using object_size() on track_metadata_tbl.

# Link to the track_metadata table in Spark
track_metadata_tbl <- tbl(spark_conn, "track_metadata")

# See how big the dataset is
dim(track_metadata_tbl)

# See how small the tibble is
object_size(track_metadata_tbl)






## Exercise 4

# Exploring the structure of tibbles
# If you try to print a tibble that describes data stored in Spark, some magic has to happen, since the tibble doesn't keep a copy of the data itself. The magic is that the print method uses your Spark connection, copies some of the contents back to R, and displays those values as though the data had been stored locally. As you saw earlier in the chapter, copying data is a slow operation, so by default, only 10 rows and as many columns will fit onscreen, are printed.
# 
# You can change the number of rows that are printed using the n argument to print(). You can also change the width of content to display using the width argument, which is specified as the number of characters (not the number of columns). A nice trick is to use width = Inf to print all the columns.
# 
# The str() function is typically used to display the structure of a variable. For data.frames, it gives a nice summary with the type and first few values of each column. For tibbles that have a remote data source however, str() doesn't know how to retrieve the data. That means that if you call str() on a tibble that contains data stored in Spark, you see a list containing a Spark connection object, and a few other bits and pieces.
# 
# If you want to see a summary of what each column contains in the dataset that the tibble refers to, you need to call glimpse() instead. Note that for remote data such as those stored in a Spark cluster datasets, the number of rows is a lie! In this case, glimpse() never claims that the data has more than 25 rows.


# Print 5 rows, all columns
print(track_metadata_tbl, n = 5, width = Inf)

# Examine structure of tibble
str(track_metadata_tbl)

# Examine structure of data
glimpse(track_metadata_tbl)




## Exercise 5
# Selecting columns
# The easiest way to manipulate data frames stored in Spark is to use dplyr syntax. Manipulating data frames using the dplyr syntax is covered in detail in the Data Manipulation in R with dplyr and Joining Data in R with dplyr courses, but you'll spend the next chapter and a half covering all the important points.
# 
# dplyr has five main actions that you can perform on a data frame. You can select columns, filter rows, arrange the order of rows, change columns or add new columns, and calculate summary statistics.
# 
# Let's start with selecting columns. This is done by calling select(), with a tibble, followed by the unquoted names of the columns you want to keep. dplyr functions are conventionally used with magrittr's pipe operator, %>%. To select the x, y, and z columns, you would write the following.
# 
# a_tibble %>%
#   select(x, y, z)
# Note that square bracket indexing is not currently supported in sparklyr. So you cannot do
# 
# a_tibble[, c("x", "y", "z")]


## Exercise 6

# Filtering rows
# As well as selecting columns, the other way to extract important parts of your dataset is to filter the rows. This is achieved using the filter() function. To use filter(), you pass it a tibble and some logical conditions. For example, to return only the rows where the values of column x are greater than zero and the values of y equal the values of z, you would use the following.
# 
# a_tibble %>%
#   filter(x > 0, y == z)
# Before you try the exercise, take heed of two warnings. Firstly, don't mistake dplyr's filter() function with the stats package's filter() function. Secondly, sparklyr converts your dplyr code into SQL database code before passing it to Spark. That means that only a limited number of filtering operations are currently supported. For example, you can't filter character rows using regular expressions with code like
# 
# a_tibble %>%
#   filter(grepl("a regex", x))
# The help page for translate_sql() describes the functionality that is available. You are OK to use comparison operators like >, !=, and %in%; arithmetic operators like +, ^, and %%; and logical operators like &, | and !. Many mathematical functions such as log(), abs(), round(), and sin() are also supported.
# 
# As before, square bracket indexing does not currently work.

# track_metadata_tbl has been pre-defined
glimpse(track_metadata_tbl)

# Manipulate the track metadata
track_metadata_tbl %>%
  # Select columns
  select(artist_name, release, title, year) %>%
  # Filter rows
  filter(year >= 1960 & year < 1970)


## Exercise 7

# Arranging rows
# Back in the days when music was stored on CDs, there was a perennial problem: how do you best order your CDs so you can find the ones you want? By order of artist? Chronologically? By genre?
#   
#   The arrange() function lets you reorder the rows of a tibble. It takes a tibble, followed by the unquoted names of columns. For example, to sort in ascending order of the values of column x, then (where there is a tie in x) by descending order of values of y, you would write the following.
# 
# a_tibble %>%
#   arrange(x, desc(y))
# Notice the use of desc() to enforce sorting by descending order. Also be aware that in sparklyr, the order() function, used for arranging the rows of data.frames does not work.

# track_metadata_tbl has been pre-defined
track_metadata_tbl

# Manipulate the track metadata
track_metadata_tbl %>%
  # Select columns
  select(artist_name, release, title, year) %>%
  # Filter rows
  filter(year >= 1960, year < 1970) %>%
  # Arrange rows
  arrange(artist_name, desc(year), title)




### Exercise 8
### mutate columns

# Mutating columns
# It may surprise you, but not all datasets start out perfectly clean! Often you have to fix values, or create new columns derived from your existing data. The process of changing or adding columns is called mutation in dplyr terminology, and is performed using mutate(). This function takes a tibble, and named arguments to update columns. The names of each of these arguments is the name of the columns to change or add, and the value is an expression explaining how to update it. For example, given a tibble with columns x and y, the following code would update x and create a new column z.
# 
# a_tibble %>%
#   mutate(
#     x = x + y,
#     z = log(x)  
#   )
# In case you hadn't got the message already that base-R functions don't work with Spark tibbles, you can't use within() or transform() for this purpose.


# # track_metadata_tbl has been pre-defined
# track_metadata_tbl
# 
# # Manipulate the track metadata
# track_metadata_tbl %>%
#   # Select columns
#   select(title, duration) %>%
#   # Mutate columns
#   mutate(duration_minutes = duration / 60)




## Exercise 9
## Summarizing columns

# Summarizing columns
# The mutate() function that you saw in the previous exercise takes columns as inputs, and returns a column. If you are calculating summary statistics such as the mean, maximum, or standard deviation, then you typically want to take columns as inputs but return a single value. This is achieved with the summarize() function.
# 
# a_tibble %>%
#   summarize(
#     mean_x       = mean(x),
#     sd_x_times_y = sd(x * y)
#   )
# Note that dplyr has a philosophy (passed on to sparklyr) of always keeping the data in tibbles. So the return value here is a tibble with one row, and one column for each summary statistic that was calculated.