##### chapter 2
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE)
  try(sapply(pkg, require, character.only = TRUE), silent = TRUE)
}
packages <- c("foreach", "doParallel", 
              "boot", "sparklyr", "stringr", "DBI")
ipak(packages)
#####
##### DBI is for the function dbGetQuery

##### select multiple columns with select function
##### computing and collecting
##### SQL 


## Exercise 1
# Mother's little helper (1)
# If your dataset has thousands of columns, and you want to select a lot of them, then typing the name of each column when you call select() can be very tedious. Fortunately, select() has some helper functions to make it easy to select multiple columns without typing much code.
# 
# These helpers include starts_with() and ends_with(), that match columns that start or end with a certain prefix or suffix respectively. Due to dplyr's special code evaluation techniques, these functions can only be called from inside a call to select(); they don't make sense on their own.


# track_metadata_tbl has been pre-defined
track_metadata_tbl

track_metadata_tbl %>%
  # Select columns starting with artist
  select(starts_with("artist"))

track_metadata_tbl %>%
  # Select columns ending with id
  select(ends_with("id"))


## Exercise 2
# Mother's little helper (2)
# A more general way of matching columns is to check if their names contain a value anywhere within them (rather than starting or ending with a value). As you may be able to guess, you can do this using a helper named contains().
# 
# Even more generally, you can match columns using regular expressions. Regular expressions ("regexes" for short) are a powerful language used for matching text. If you want to learn how to use regular expressions, take the String Manipulation in R with stringr course. For now, you only need to know three things.
# 
# a: A letter means "match that letter".
# .: A dot means "match any character, including letters, numbers, punctuation, etc.".
# ?: A question mark means "the previous character is optional".
# You can find columns that match a particular regex using the matches() select helper.

# track_metadata_tbl has been pre-defined
track_metadata_tbl

track_metadata_tbl %>%
  # Select columns containing ti
  select(contains("ti"))

track_metadata_tbl %>%
  # Select columns matching ti.?t
  select(matches("ti.?t"))

## Exercise 3
# Selecting unique rows
# If you have a categorical variable stored in a factor, it is often useful to know what the individual categories are; you do this with the levels() function. For a tibble, the more general concept is to find rows with unique data. Following the terminology from SQL, this is done using the distinct() function. You can use it directly on your dataset, so you find unique combinations of a particular set of columns. For example, to find the unique combinations of values in the x, y, and z columns, you would write the following.
# 
# a_tibble %>%
#   distinct(x, y, z)

## Exercise 4
# Common people
# The distinct() function showed you the unique values. It can also be useful to know how many of each value you have. The base-R function for this is table(); that isn't supported in sparklyr since it doesn't conform to the tidyverse philosophy of keeping everything in tibbles. Instead, you must use count(). To use it, pass the unquoted names of the columns. For example, to find the counts of distinct combinations of columns x, y, and z, you would type the following.
# 
# a_tibble %>%
#   count(x, y, z)
# The result is the same as
# 
# a_tibble %>%
#   distinct(x, y, z)
# … except that you get an extra column, n, that contains the counts.
# 
# A really nice use of count() is to get the most common values of something. To do this, you call count(), with the argument sort = TRUE which sorts the rows by descending values of the n column, then use top_n() to restrict the results to the top however-many values. (top_n() is similar to base-R's head(), but it works with remote datasets such as those in Spark.) For example, to get the top 20 most common combinations of the x, y, and z columns, use the following.
# 
# a_tibble %>%
#   count(x, y, z, sort = TRUE) %>%
#   top_n(20)

## Exercise 5
# Collecting data back from Spark
# In the exercise 'Exploring the structure of tibbles', back in Chapter 1, you saw that tibbles don't store a copy of the data. Instead, the data stays in Spark, and the tibble simply stores the details of what it would like to retrieve from Spark.
# 
# There are lots of reasons that you might want to move your data from Spark to R. You've already seen how some data is moved from Spark to R when you print it. You also need to collect your dataset if you want to plot it, or if you want to use a modeling technique that is not available in Spark. (After all, R has the widest selection of available models of any programming language.)
# 
# To collect your data: that is, to move it from Spark to R, you call collect().

# Jolly good! copy_to() moves your data from R to Spark; collect() goes in the opposite direction.


## Exercise 6
# Storing intermediate results
# As you saw in Chapter 1, copying data between R and Spark is a fundamentally slow task. That means that collecting the data, as you saw in the previous exercise, should only be done when you really need to.
# 
# The pipe operator is really nice for chaining together data manipulation commands, but in general, you can't do a whole analysis with everything chained together. For example, this is an awful practice, since you will never be able to debug your code.
# 
# final_results <- starting_data %>%
#   # 743 steps piped together
#   # ... %>%
#   collect()
# That gives a dilemma. You need to store the results of intermediate calculations, but you don't want to collect them because it is slow. The solution is to use compute() to compute the calculation, but store the results in a temporary data frame on Spark. Compute takes two arguments: a tibble, and a variable name for the Spark data frame that will store the results.
# 
# a_tibble %>%
#   # some calculations %>%
#   compute("intermediate_results")


## Exercise 7
# Groups: great for music, great for data
# A common analysis problem is how to calculate summary statistics for each group of data. For example, you might want to know your sales revenues by month, or by region. In R, the process of splitting up your data into groups, applying a summary statistic on each group, and combining the results into a single data structure, is known as "split-apply-combine". The concept is much older though: SQL has had the GROUP BY statement for decades. The term "map-reduce" is a similar concept, where "map" is very roughly analogous to the "split" and "apply" steps, and "reducing" is "combining". The dplyr/sparklyr approach is to use group_by() before you mutate() or summarize(). It takes the unquoted names of columns to group by. For example, to calculate the mean of column x, for each combination of values in columns grp1 and grp2, you would write the following.
# 
# a_tibble %>%
#   group_by(grp1, grp2) %>%
#   summarize(mean_x = mean(x))
# Note that the columns passed to group_by() should typically be categorical variables. For example, if you wanted to calculate the average weight of people relative to their height, it doesn't make sense to group by height, since everyone's height is unique. You could, however, use cut() to convert the heights into different categories, and calculate the mean weight for each category.

## Exercise 8
# Groups of mutants
# In addition to calculating summary statistics by group, you can mutate columns with group-specific values. For example, one technique to normalize values is to subtract the mean, then divide by the standard deviation. You could perform group-specific normalization using the following code.
# 
# a_tibble %>%
#   group_by(grp1, grp2) %>%
#   mutate(normalized_x = (x - mean(x)) / sd(x))

# track_metadata_tbl has been pre-defined
# track_metadata_tbl
# 
# track_metadata_tbl %>%
#   # Group by artist
#   group_by(artist_name) %>%
#   # Calc time since first release
#   mutate(time_since_first_release = year - min(year)) %>%
#   # Arrange by descending time since first release
#   arrange(time_since_first_release)


## Exercise 9
## 

# Advanced Selection II: The SQL
# As previously mentioned, when you use the dplyr interface, sparklyr converts your code into SQL before passing it to Spark. Most of the time, this is what you want. However, you can also write raw SQL to accomplish the same task. Most of the time, this is a silly idea since the code is harder to write and harder to debug. However, if you want your code to be portable – that is, used outside of R as well – then it may be useful. For example, a fairly common workflow is to use sparklyr to experiment with data processing, then switch to raw SQL in a production environment. By writing raw SQL to begin with, you can just copy and paste your queries when you move to production.
# 
# SQL queries are written as strings, and passed to dbGetQuery() from the DBI package. The pattern is as follows.
# 
# query <- "SELECT col1, col2 FROM some_data WHERE some_condition"
# a_data.frame <- dbGetQuery(spark_conn, query)
# Note that unlike the dplyr code you've written, dbGetQuery() will always execute the query and return the results to R immediately. If you want to delay returning the data, you can use dbSendQuery() to execute the query, then dbFetch() to return the results. That's more advanced usage, not covered here. Also note that DBI functions return data.frames rather than tibbles, since DBI is a lower-level package.
# 
# If you want to learn more about writing SQL code, take the Intro to SQL for Data Science course.




## Exercise 10
#
# Left joins
# As well as manipulating single data frames, sparklyr allows you to join two data frames together. A full treatment of how to join tables together using dplyr syntax is given in the Joining Data in R with dplyr course. For the rest of this chapter, you'll see some examples of how to do this using Spark.
# 
# A left join takes all the values from the first table, and looks for matches in the second table. If it finds a match, it adds the data from the second table; if not, it adds missing values. The principle is shown in this diagram.
# 
# A left join, explained using table of colors.
# 
# Left joins are a type of mutating join, since they simply add columns to the first table. To perform a left join with sparklyr, call left_join(), passing two tibbles and a character vector of columns to join on.
# 
# left_join(a_tibble, another_tibble, by = c("id_col1", "id_col2"))
# When you describe this join in words, the table names are reversed. This join would be written as "another_tibble is left joined to a_tibble".
# 
# This exercise introduces another Spark DataFrame containing terms that describe each artist. These range from rather general terms, like "pop", to more niche genres such as "swiss hip hop" and "mathgrindcore".


## Exercise 11
#
# Anti joins
# In the previous exercise, the joined dataset wasn't as big as you might have expected, since not all the artists had tags associated with them. Anti joins are really useful for finding problems with other joins.
# 
# An anti join returns the rows of the first table where it cannot find a match in the second table. The principle is shown in this diagram.
# 
# An anti join, explained using table of colors.
# 
# Anti joins are a type of filtering join, since they return the contents of the first table, but with their rows filtered depending upon the match conditions.
# 
# The syntax for an anti join is more or less the same as for a left join: simply swap left_join() for anti_join().
# 
# anti_join(a_tibble, another_tibble, by = c("id_col1", "id_col2"))

## Exercise 12
# 
# semi joins
# Semi joins
# Semi joins are the opposite of anti joins: an anti-anti join, if you like.
# 
# A semi join returns the rows of the first table where it can find a match in the second table. The principle is shown in this diagram.
# 
# A semi join, explained using table of colors.
# 
# The syntax is the same as for other join types; simply swap the other join function for semi_join()
# 
# semi_join(a_tibble, another_tibble, by = c("id_col1", "id_col2"))
# You may have spotted that the results of a semi join plus the results of an anti join give the orignial table. So, regardless of the table contents or how you join them, semi_join(A, B) plus anti_join(A, B) will return A (though maybe with the rows in a different order).