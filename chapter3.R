##### chapter 3
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE)
  try(sapply(pkg, require, character.only = TRUE), silent = TRUE)
}
packages <- c("foreach", "doParallel", 
              "boot", "sparklyr", "stringr", "DBI", "dplyr", "tidytext")
ipak(packages)

###### 
# Popcorn double feature
# The dplyr methods that you saw in the previous two chapters use Spark's SQL interface. That is, they convert your R code into SQL code before passing it to Spark. This is an excellent solution for basic data manipulation, but it runs into problems when you want to do more complicated processing. For example, you can calculate the mean of a column, but not the median. Here is the example from the 'Summarizing columns' exercise that you completed in Chapter 1.
# 
# track_metadata_tbl %>%
#   summarize(mean_duration = mean(duration)) #OK
# track_metadata_tbl %>%
#   summarize(median_duration = median(duration))
# sparklyr also has two "native" interfaces that will be discussed in the next two chapters. Native means that they call Java or Scala code to access Spark libraries directly, without any conversion to SQL. sparklyr supports the Spark DataFrame Application Programming Interface (API), with functions that have an sdf_ prefix. It also supports access to Spark's machine learning library, MLlib, with "feature transformation" functions that begin ft_, and "machine learning" functions that begin ml_.
# 
# One important philosophical difference between working with R and working with Spark is that Spark is much stricter about variable types than R. Most of the native functions want DoubleType inputs and return DoubleType outputs. DoubleType is Spark's equivalent of R's numeric vector type. sparklyr will handle converting numeric to DoubleType, but it is up to the user (that's you!) to convert logical or integer data into numeric data and back again.
# 
# Which of these statements is true?
# 
# sparklyr's dplyr methods convert code into Scala code before running it on Spark.



####### feature transformation


#######
## Exercise 1

# Transforming continuous variables to logical
# Logical variables are nice because it is often easier to think about things in "yes or no" terms rather than in numeric terms. For example, if someone asks you "Would you like a cup of tea?", a yes or no response is preferable to "There is a 0.73 chance of me wanting a cup of tea". This has real data science applications too. For example, a test for diabetes may return the glucose concentration in a patient's blood plasma as a number. What you really care about is "Does the patient have diabetes?", so you need to convert the number into a logical value, based upon some threshold.
# 
# In base-R, this is done fairly simply, using something like this:
# 
# threshold_mmol_per_l <- 7
# has_diabetes <- plasma_glucose_concentration > threshold_mmol_per_l
# All the sparklyr feature transformation functions have a similar user interface. The first three arguments are always a Spark tibble, a string naming the input column, and a string naming the output column. That is, they follow this pattern.
# 
# a_tibble %>%
#   ft_some_transformation("x", "y", some_other_args)
# The sparklyr way of converting a continuous variable into logical uses ft_binarizer(). The previous diabetes example can be rewritten as the following. Note that the threshold value should be a number, not a string refering to a column in the dataset.
# 
# diabetes_data %>%
#   ft_binarizer("plasma_glucose_concentration", "has_diabetes", threshold = threshold_mmol_per_l)
# In keeping with the Spark philosophy of using DoubleType everywhere, the output from ft_binarizer() isn't actually logical; it is numeric. This is the correct approach for letting you continue to work in Spark and perform other transformations, but if you want to process your data in R, you have to remember to explicitly convert the data to logical. The following is a common code pattern.
# 
# a_tibble %>%
#   ft_binarizer("x", "is_x_big", threshold = threshold) %>%
#   collect() %>%
#   mutate(is_x_big = as.logical(is_x_big))
# This exercise considers the appallingly named artist_hotttnesss field, which provides a measure of how much media buzz the artist had at the time the dataset was created. If you would like to learn more about drawing plots using the ggplot2 package, please take the Data Visualization with ggplot2 (Part 1) course.
# 
# 
# 
# 
#                                                                                                                                                                                                                                                                                                                                                                                   Converting R code into SQL code limits the number of supported computations.
#                                                                                                                                                                                                                                                                                                                                                                                   Most Spark MLlib modeling functions require DoubleType inputs and return DoubleType outputs.
#                                                                                                                                                                                                                                                                                                                                                                                   Most Spark MLlib modeling functions require IntegerType inputs and return BooleanType outputs.


# track_metadata_tbl has been pre-defined
track_metadata_tbl

hotttnesss <- track_metadata_tbl %>%
  # Select artist_hotttnesss
  select(artist_hotttnesss) %>%
  # Binarize to is_hottt_or_nottt
  ft_binarizer("artist_hotttnesss", "is_hottt_or_nottt", threshold = 0.5) %>%
  # Collect the result
  collect() %>%
  # Convert is_hottt_or_nottt to logical
  mutate(is_hottt_or_nottt = as.logical(is_hottt_or_nottt))

# Draw a barplot of is_hottt_or_nottt
ggplot(hotttnesss, aes(is_hottt_or_nottt)) +
  geom_bar()


###############
#### Exercise 2
###############
#### Transforming continuous variables into categorical 
################

# 
# Transforming continuous variables into categorical (1)
# A generalization of the previous idea is to have multiple thresholds; that is, you split a continuous variable into "buckets" (or "bins"), just like a histogram does. In base-R, you would use cut() for this task. For example, in a study on smoking habits, you could take the typical number of cigarettes smoked per day, and transform it into a factor.
# 
# smoking_status <- cut(
#   cigarettes_per_day,
#   breaks = c(0, 1, 10, 20, Inf),
#   labels = c("non", "light", "moderate", "heavy"),
#   right  = FALSE
# )
# The sparklyr equivalent of this is to use ft_bucketizer(). The code takes a similar format to ft_binarizer(), but this time you must pass a vector of cut points to the splits argument. Here is the same example rewritten in sparklyr style.
# 
# smoking_data %>%
#   ft_bucketizer("cigarettes_per_day", "smoking_status", splits = c(0, 1, 10, 20, Inf))
# There are several important things to note. You may have spotted that the breaks argument from cut() is the same as the splits argument from ft_bucketizer(). There is a slight difference in how values on the boundary are handled. In cut(), by default, the upper (right-hand) boundary is included in each bucket, but not the left. ft_bucketizer() includes the lower (left-hand) boundary in each bucket, but not the right. This means that it is equivalent to calling cut() with the argument right = FALSE.
# 
# One exception is that ft_bucketizer() includes values on both boundaries for the upper-most bucket. So ft_bucketizer() is also equivalent to setting include.lowest = TRUE when using cut().
# 
# The final thing to note is that whereas cut() returns a factor, ft_bucketizer() returns a numeric vector, with values in the first bucket returned as zero, values in the second bucket returned as one, values in the third bucket returned as two, and so on. If you want to work on the results in R, you need to explicitly convert to a factor. This is a common code pattern:
#   
#   a_tibble %>%
#   ft_bucketizer("x", "x_buckets", splits = splits) %>%
#   collect() %>%
#   mutate(x_buckets = factor(x_buckets, labels = labels)



# # track_metadata_tbl, decades, decade_labels have been pre-defined
# track_metadata_tbl
# decades
# decade_labels
# 
# hotttnesss_over_time <- track_metadata_tbl %>%
#   # Select artist_hotttnesss and year
#   select(artist_hotttnesss, year) %>%
#   # Convert year to numeric
#   mutate(year = as.numeric(year)) %>%
#   # Bucketize year to decade using decades vector
#   ft_bucketizer("year", "decade", splits = decades) %>%
#   # Collect the result
#   collect() %>%
#   # Convert decade to factor using decade_labels
#   mutate(decade = factor(decade))
# 
# # Draw a boxplot of artist_hotttnesss by decade
# ggplot(hotttnesss_over_time, aes(decade, artist_hotttnesss)) +
#   geom_boxplot()  


# # track_metadata_tbl, decades, decade_labels have been pre-defined
# track_metadata_tbl
# decades
# decade_labels
# 
# hotttnesss_over_time <- track_metadata_tbl %>%
#   # Select artist_hotttnesss and year
#   select(artist_hotttnesss, year) %>%
#   # Convert year to numeric
#   mutate(year = as.numeric(year)) %>%
#   # Bucketize year to decade using decades vector
#   ft_bucketizer("year", "decade", splits = decades) %>%
#   # Collect the result
#   collect() %>%
#   # Convert decade to factor using decade_labels
#   mutate(decade = factor(decade, labels = decade_labels))
# 
# # Draw a boxplot of artist_hotttnesss by decade
# ggplot(hotttnesss_over_time, aes(decade, artist_hotttnesss)) +
#   geom_boxplot()  



################
### Exercise 3
################

# Transforming continuous variables into categorical (2)
# A special case of the previous transformation is to cut a continuous variable into buckets where the buckets are defined by quantiles of the variable. A common use of this transformation is to analyze survey responses or review scores. If you ask people to rate something from one to five stars, often the median response won't be three stars. In this case, it can be useful to split their scores up by quantile. For example, you can make five quintile groups by splitting at the 0th, 20th, 40th, 60th, 80th, and 100th percentiles.
# 
# The base-R way of doing this is cut() + quantile(). The sparklyr equivalent uses the ft_quantile_discretizer() transformation. This takes an n.buckets argument, which determines the number of buckets. The base-R and sparklyr ways of calculating this are shown together. As before, right = FALSE and include.lowest are set.
# 
# survey_response_group <- cut(
#   survey_score,
#   breaks = quantile(survey_score, c(0, 0.25, 0.5, 0.75, 1)),
#   labels = c("hate it", "dislike it", "like it", "love it"),
#   right  = FALSE,
#   include.lowest = TRUE
# )
# survey_data %>%
#   ft_quantile_discretizer("survey_score", "survey_response_group", n.buckets = 4)
# As with ft_bucketizer(), the resulting bins are numbers, counting from zero. If you want to work with them in R, explicitly convert to a factor.

# # track_metadata_tbl, duration_labels have been pre-defined
# track_metadata_tbl
# duration_labels
# 
# familiarity_by_duration <- track_metadata_tbl %>%
#   # Select duration and artist_familiarity
#   select(duration, artist_familiarity) %>%
#   # Bucketize duration
#   ft_quantile_discretizer("duration", "duration_bin", n.buckets = 5) %>%
#   # Collect the result
#   collect() %>%
#   # Convert duration bin to factor
#   mutate(duration_bin = factor(duration_bin, labels = duration_labels))
# 
# # Draw a boxplot of artist_familiarity by duration_bin
# ggplot(familiarity_by_duration, aes(duration_bin, artist_familiarity)) +
#   geom_boxplot() 




########### Exercise 4
#### tokenizer()
#### unnest()

# # track_metadata_tbl has been pre-defined
# track_metadata_tbl
# 
# title_text <- track_metadata_tbl %>%
#   # Select artist_name, title
#   select(artist_name, title) %>%
#   # Tokenize title to words
#   ft_tokenizer("title", "word") %>%
#   # Collect the result
#   collect() %>%
#   # Flatten the word column 
#   mutate(word = lapply(word, as.character)) %>% 
#   # Unnest the list column
#   unnest()



############# Exercise 5
#############
############# More than words
############# tokenization
# More than words: tokenization (2)
# The tidytext package lets you analyze text data using "tidyverse" packages such as dplyr and sparklyr. How to do sentiment analysis is beyond the scope of this course; you can see more in the Sentiment Analysis and Sentiment Analysis: The Tidy Way courses. This exercise is designed to give you a quick taste of how to do it on Spark.
# 
# Sentiment analysis essentially lets you assign a score or emotion to each word. For example, in the AFINN lexicon, the word "outstanding" has a score of +5, since it is almost always used in a positive context. "grace" is a slightly positive word, and has a score of +1. "fraud" is usually used in a negative context, and has a score of -4. The AFINN scores dataset is returned by get_sentiments("afinn"). For convenience, the unnested word data and the sentiment lexicon have been copied to Spark.
# 
# Typically, you want to compare the sentiment of several groups of data. To do this, the code pattern is as follows.
# 
# text_data %>%
#   inner_join(sentiments, by = "word") %>%
#   group_by(some_group) %>%
#   summarize(positivity = sum(score))
# An inner join takes all the values from the first table, and looks for matches in the second table. If it finds a match, it adds the data from the second table. Unlike a left join, it will drop any rows where it doesn't find a match. The principle is shown in this diagram.
# 
# An inner join, explained using table of colors.
# 
# Like left joins, inner joins are a type of mutating join, since they add columns to the first table. See if you can guess which function to use for inner joins, and how to use it. (Hint: the usage is really similar to left_join(), anti_join(), and semi_join()!)


# AFINN score for sentiment analysis
# 
# title_text_tbl, afinn_sentiments_tbl have been pre-defined
title_text_tbl
afinn_sentiments_tbl

sentimental_artists <- title_text_tbl %>%
  # Inner join with sentiments on word field
  inner_join(afinn_sentiments_tbl, by = "word") %>%
  # Group by artist
  group_by(artist_name) %>%
  # Summarize to get positivity
  summarise(positivity = sum(score))

sentimental_artists %>%
  # Arrange by ascending positivity
  arrange(positivity) %>%
  # Get top 5
  top_n(5)

sentimental_artists %>%
  # Arrange by descending positivity
  arrange(desc(positivity)) %>%
  # Get top 5
  top_n(5)





#####################
#####################
####### Exercise 6
#####################
#####################
#### tokenization
#### ft_regex_tokenizer()
#### 

# track_metadata_tbl has been pre-defined
# track_metadata_tbl
# 
# track_metadata_tbl %>%
#   # Select artist_mbid column
#   select(artist_mbid) %>%
#   # Split it by hyphens
#   ft_regex_tokenizer("artist_mbid", "artist_mbid_chunks", "-")




#######################
#### Exercise 7
#######################
#######################

# Sorting vs. arranging
# So far in this chapter, you've explored some feature transformation functions from Spark's MLlib. sparklyr also provides access to some functions making use of the Spark DataFrame API.
# 
# The dplyr way of sorting a tibble is to use arrange(). You can also sort tibbles using Spark's DataFrame API using sdf_sort(). This function takes a character vector of columns to sort on, and currently only sorting in ascending order is supported.
# 
# For example, to sort by column x, then (in the event of ties) by column y, then by column z, the following code compares the dplyr and Spark DataFrame approaches.
# 
# a_tibble %>%
#   arrange(x, y, z)
# a_tibble %>%
#   sdf_sort(c("x", "y", "z"))
# To see which method is faster, try using both arrange(), and sdf_sort(). You can see how long your code takes to run by wrapping it in microbenchmark(), from the package of the same name.
# 
# microbenchmark({
#   # your code
# })
# You can learn more about profiling the speed of your code in the Writing Efficient R Code course.


# # track_metadata_tbl has been pre-defined
# track_metadata_tbl
# 
# # Compare timings of arrange() and sdf_sort()
# microbenchmark(
#   arranged = track_metadata_tbl %>%
#     # Arrange by year, then artist_name, then release, then title
#     arrange(year, artist_name, release, title) %>%
#     # Collect the result
#     collect(),
#   sorted = track_metadata_tbl %>%
#     # Sort by year, then artist_name, then release, then title
#     sdf_sort(c("year", "artist_name", "release", "title")) %>%
#     # Collect the result
#     collect(),
#   times = 5
# )





##############################
############ Exercise 8
##############################
##### Exploring spark data types


# 
# Exploring Spark data types
# 
# You've already seen (back in Chapter 1) src_tbls() for listing the DataFrames on Spark that sparklyr can see. You've also seen glimpse() for exploring the columns of a tibble on the R side.
# 
# sparklyr has a function named sdf_schema() for exploring the columns of a tibble on the R side. It's easy to call; and a little painful to deal with the return value.
# 
# sdf_schema(a_tibble)
# 
# The return value is a list, and each element is a list with two elements, containing the name and data type of each column. The exercise shows a data transformation to more easily view the data types.
# 
# Here is a comparison of how R data types map to Spark data types. Other data types are not currently supported by sparklyr.


# # track_metadata_tbl has been pre-defined
# track_metadata_tbl
# 
# # Get the schema
# (schema <- sdf_schema(track_metadata_tbl))
# 
# # Transform the schema
# schema %>%
#   lapply(function(x) do.call(data_frame, x)) %>%
#   bind_rows()




####################################
####################################
################# Exercise 9
####################################
####################################

# Shrinking the data by sampling
# 
# When you are working with a big dataset, you typically don't really need to work with all of it all the time. Particularly at the start of your project, while you are experimenting wildly with what you want to do, you can often iterate more quickly by working on a smaller subset of the data. sdf_sample() provides a convenient way to do this. It takes a tibble, and the fraction of rows to return. In this case, you want to sample without replacement. To get a random sample of one tenth of your dataset, you would use the following code.
# 
# a_tibble %>%
#   sdf_sample(fraction = 0.1, replacement = FALSE)
# 
# Since the results of the sampling are random, and you will likely want to reuse the shrunken dataset, it is common to use compute() to store the results as another Spark data frame.
# 
# a_tibble %>%
#   sdf_sample(<some args>) %>%
#   compute("sample_dataset")
# 
# To make the results reproducible, you can also set a random number seed via the seed argument. Doing this means that you get the same random dataset every time you run your code. It doesn't matter which number you use for the seed; just choose your favorite positive integer.


# Use sdf_sample() to sample a subet 
# 

# # track_metadata_tbl has been pre-defined
# track_metadata_tbl
# 
# track_metadata_tbl %>%
#   # Sample the data without replacement
#   sdf_sample(fraction = 0.01, replacement = FALSE, seed = 20000229) %>%
#   # Compute the result
#   compute("sample_track_metadata")




###############################
###############################
###### Exercise 10
###############################
###############################
## Training / testing partitions

# Training/testing partitions
# 
# Most of the time, when you run a predictive model, you need to fit the model on one subset of your data (the "training" set), then test the model predictions against the rest of your data (the "testing" set).
# 
# sdf_partition() provides a way of partitioning your data frame into training and testing sets. Its usage is as follows.
# 
# a_tibble %>%
#   sdf_partition(training = 0.7, testing = 0.3)
# 
# There are two things to note about the usage. Firstly, if the partition values don't add up to one, they will be scaled so that they do. So if you passed training = 0.35 and testing = 0.15, you'd get double what you asked for. Secondly, you can use any set names that you like, and partition the data into more than two sets. So the following is also valid.
# 
# a_tibble %>%
#   sdf_partition(a = 0.1, b = 0.2, c = 0.3, d = 0.4)
# 
# The return value is a list of tibbles. you can access each one using the usual list indexing operators.
# 
# partitioned$a
# partitioned[["b"]]


###
# track_metadata_tbl has been pre-defined
track_metadata_tbl

partitioned <- track_metadata_tbl %>%
  # Partition into training and testing sets
  sdf_partition(training = 0.7, testing = 0.3) # partitioned is a of tbl, tbl_spark class

# Get the dimensions of the training set
dim(partitioned$training)

# Get the dimensions of the testing set
dim(partitioned$testing)