#######################################################
## Chapter 4, gradient boosted trees and random forests
#######################################################
#######################################################
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE)
  try(sapply(pkg, require, character.only = TRUE), silent = TRUE)
}
packages <- c("foreach", "doParallel", 
              "boot", "sparklyr", "stringr", "DBI", "dplyr", "tidytext")
ipak(packages)

#
# Machine learning functions
# 
# In the last chapter, you saw some of the feature transformation functionality of Spark MLlib. If that library were a meal, the feature transformations would be a starter; the main course is a sumptuous selection of machine learning modeling functions! These functions all have names beginning with ml_, and have a similar signature. They take a tibble, a string naming the response variable, a character vector naming features (input variables), and possibly some other model-specific arguments.
# 
# a_tibble %>%
#   ml_some_model("response", c("a_feature", "another_feature"), some_other_args)
# 
# Supported machine learning functions include linear regression and its variants, tree-based models (ml_decision_tree(), and a few others. You can see the list of all the machine learning functions using ls().
#                                                                                                     
#                                                                                                     ls("package:sparklyr", pattern = "^ml")
#                                                                                                     
#                                                                                                     What arguments do all the machine learning model functions take?

# list all ml related functions
# ls("package:sparklyr", pattern = "^ml")

##### Spark MLib
#####


##########################################################
#### Exercise 1
##########################################################



# (Hey you) What's that sound?

# Songs start out as an analogue thing: their sound is really a load of vibrations of air. In order to analyze a song, you need to turn it into some meaningful numbers. Tracks in the Million Song Dataset have twelve timbre measurements taken at regular time intervals throughout the song. (Timbre is a measure of the perceived quality of a sound; you can use it to distinguish voices from string instruments from percussion instruments, for example.)
# 
# In this chapter, you are going to try and predict the year a track was released, based upon its timbre. That is, you are going to use these timbre measurements to generate features for the models. (Recall that feature is machine learning terminology for an input variable in a model. They are often called explanatory variables in statistics.)
# 
# The timbre data takes the form of a matrix, with rows representing the time points, and columns representing the different timbre measurements. Thus all the timbre matrices have twelve columns, but the number of rows differs from song to song. The mean of each column estimates the average of a timbre measurement over the whole song. These can be used to generate twelve features for the model.


# colMeans()




###############################
######### Exercise 2
###############################
###############################

# 
# Working with parquet files
# 
# CSV files are great for saving the contents of rectangular data objects (like R data.frames and Spark DataFrames) to disk. The problem is that they are really slow to read and write, making them unusable for large datasets. Parquet files provide a higher performance alternative. As well as being used for Spark data, parquet files can be used with other tools in the Hadoop ecosystem, like Shark, Impala, Hive, and Pig.
# 
# Technically speaking, parquet file is a misnomer. When you store data in parquet format, you actually get a whole directory worth of files. The data is split across multiple .parquet files, allowing it to be easily stored on multiple machines, and there are some metadata files too, describing the contents of each column.
# 
# sparklyr can import parquet files using spark_read_parquet(). This function takes a Spark connection, a string naming the Spark DataFrame that should be created, and a path to the parquet directory. Note that this function will import the data directly into Spark, which is typically faster than importing the data into R, then using copy_to() to copy the data from R to Spark.
# 
# spark_read_parquet(sc, "a_dataset", "path/to/parquet/dir")


# Parquest files can be used with other tools in the Hadoop ecosystem, like shark,
# impala hive and pig

# the data is split across multiple .parquet files

# can import parquet files using spark_read_parquet()
# this function takes a spark connection
# a string naming the Spark DataFrame that should be created
# and apath to the parquet directory
# 

# parquet_dir has been pre-defined
parquet_dir

# List the files in the parquet dir
filenames <- dir(parquet_dir, full.names = TRUE)

# Show the filenames and their sizes
data_frame(
  filename = basename(filenames),
  size_bytes = file.size(filenames)
)

# Import the data into Spark
timbre_tbl <- spark_read_parquet(spark_conn, "timbre", parquet_dir)


#############################
################ Exercise 3
#############################
# Come together
# 
# The features to the models you are about to run are contained in the timbre dataset, but the response – the year – is contained in the track_metadata dataset. Before you run the model, you are going to have to join these two datasets together. In this case, there is a one to one matching of rows in the two datasets, so you need an inner join.
# 
# There is one more data cleaning task you need to do. The year column contains integers, but Spark modeling functions require real numbers. You need to convert the year column to numeric.



###############################
################# Exercise 4
################################

# Partitioning data with a group effect
# 
# Before you can run any models, you need to partition your data into training and testing sets. There's a complication with this dataset, which means you can't just call sdf_partition(). The complication is that each track by a single artist ought to appear in the same set; your model will appear more accurate than it really is if tracks by an artist are used to train the model then appear in the testing set.
# 
# The trick to dealing with this is to partition only the artist IDs, then inner join those partitioned IDs to the original dataset. Note that artist_id is more reliable than artist_name for partitioning, since some artists use variations on their name between tracks. For example, Duke Ellington sometimes has an artist name of "Duke Ellington", but other times has an artist name of "Duke Ellington & His Orchestra", or one of several spelling variants.


# track_data_tbl has been pre-defined
# track_data_tbl
# 
# training_testing_artist_ids <- track_data_tbl %>%
#   # Select the artist ID
#   select(artist_id) %>%
#   # Get distinct rows
#   distinct() %>%
#   # Partition into training/testing sets
#   sdf_partition(training = 0.7, testing = 0.3)
# 
# track_data_to_model_tbl <- track_data_tbl %>%
#   # Inner join to training partition
#   inner_join(training_testing_artist_ids$training, by = "artist_id" )
# 
# track_data_to_predict_tbl <- track_data_tbl %>%
#   # Inner join to testing partition
#   inner_join(training_testing_artist_ids$training, by = "artist_id")




##########################
#### Exercie 5
##########################
#### Gradient boosted trees
##########################
# 
# # track_data_tbl has been pre-defined
# track_data_to_model_tbl
# 
# feature_colnames <- track_data_to_model_tbl %>%
#   # Get the column names
#   colnames() %>%
#   # Limit to the timbre columns
#   str_subset(fixed("timbre"))
# 
# gradient_boosted_trees_model <- track_data_to_model_tbl %>%
#   # Run the gradient boosted trees model
#   ml_gradient_boosted_trees("year", feature_colnames)




#############################
#############################
#### Exercise 6
#############################
#############################

# Gradient boosted trees: prediction
# 
# Once you've run your model, then the next step is to make a prediction with it. sparklyr contains methods for the predict() function from base-R. This means that you can make predictions from Spark models with the same syntax as you would use for predicting a linear regression. predict() takes two arguments: a model, and some testing data.
# 
# predict(a_model, testing_data)
# 
# A common use case is to compare the predicted responses with the actual responses, which you can draw plots of in R. The code pattern for preparing this data is as follows. Note that currently adding a prediction column has to be done locally, so you must collect the results first.
# 
# predicted_vs_actual <- testing_data %>%
#   select(response) %>%
#   collect() %>%
#   mutate(predicted_response = predict(a_model, testing_data))

# training, testing sets & model are pre-defined
# track_data_to_model_tbl
# track_data_to_predict_tbl
# gradient_boosted_trees_model
# 
# responses <- track_data_to_predict_tbl %>%
#   # Select the year column
#   select(year) %>%
#   # Collect the results
#   collect() %>%
#   # Add in the predictions
#   mutate(
#     predicted_year = predict(gradient_boosted_trees_model,
#                              track_data_to_predict_tbl
#     )
#   )






######################
######################
#### Exercise 7
######################
######################

# Gradient boosted trees: visualization
# 
# Now you have your model predictions, you might wonder "are they any good?". There are many plots that you can draw to diagnose the accuracy of your predictions; here you'll take a look at two common plots. Firstly, it's nice to draw a scatterplot of the predicted response versus the actual response, to see how they compare. Secondly, the residuals ought to be somewhere close to a normal distribution, so it's useful to draw a density plot of the residuals. The plots will look something like these.



# # responses has been pre-defined
# responses
# 
# # Draw a scatterplot of predicted vs. actual
# ggplot(responses, aes(actual, predicted)) +
#   # Add the points
#   geom_point(alpha = 0.1) +
#   # Add a line at actual = predicted
#   geom_abline(slope = 1, intercept = 0)
# 
# residuals <- responses %>%
#   # Transmute response data to residuals
#   transmute(residual = predicted - actual)
# 
# # Draw a density plot of residuals
# ggplot(residuals, aes(residual)) +
#   # Add a density curve
#   geom_density() +
#   # Add a vertical line through zero
#   geom_vline(xintercept = 0)





#############################
#############################
############ Exercise 8
#############################
#############################

# random forest

# Random Forest: modeling
# 
# Like gradient boosted trees, random forests are another form of ensemble model. That is, they use lots of simpler models (decision trees, again) and combine them to make a single better model. Rather than running the same model iteratively, random forests run lots of separate models in parallel, each on a randomly chosen subset of the data, with a randomly chosen subset of features. Then the final decision tree makes predictions by aggregating the results from the individual models.
# 
# sparklyr's random forest function is called ml_random_forest(). Its usage is exactly the same as ml_gradient_boosted_trees() (see the first exercise of this chapter for a reminder on syntax).

# track_data_to_model_tbl has been pre-defined
# track_data_to_model_tbl
# 
# # Get the timbre columns
# feature_colnames <- track_data_to_model_tbl %>% colnames() %>% str_subset(fixed("timbre"))
# 
# # Run the random forest model
# random_forest_model <- track_data_to_model_tbl %>% ml_random_forest("year", feature_colnames)


# training, testing sets & model are pre-defined
# track_data_to_model_tbl
# track_data_to_predict_tbl
# random_forest_model
# 
# # Create a response vs. actual dataset
# responses <- track_data_to_predict_tbl %>% select(year) %>% collect() %>% 
#   mutate(predicted_year = predict(random_forest_model, track_data_to_predict_tbl) )




##############################
##############################
############## Exercise 9
##############################
##############################

# both_responses has been pre-defined
# both_responses
# 
# # Draw a scatterplot of predicted vs. actual
# ggplot(both_responses, aes(actual, predicted, color = model)) +
#   # Add a smoothed line
#   geom_smooth() +
#   # Add a line at actual = predicted
#   geom_abline(intercept = 0, slope = 1)
# 
# # Create a tibble of residuals
# residuals <- both_responses %>% mutate(residual = predicted - actual)
# 
# # Draw a density plot of residuals
# ggplot(residuals, aes(residual, color = model)) +
#   # Add a density curve
#   geom_density() +
#   # Add a vertical line through zero
#   geom_vline(xintercept = 0)




############################
############################
#### Exercise 10
############################
############################

# Comparing model performance
# 
# Plotting gives you a nice feel for where the model performs well, and where it doesn't. Sometimes it is nice to have a statistic that gives you a score for the model. This way you can quantify how good a model is, and make comparisons across lots of models. A common statistic is the root mean square error (sometimes abbreviated to "RMSE"), which simply squares the residuals, then takes the mean, then the square root. A small RMSE score for a given dataset implies a better prediction. (By default, you can't compare between different datasets, only different models on the same dataset. Sometimes it is possible to normalize the datasets to provide a comparison between them.)
# 
# Here you'll compare the gradient boosted trees and random forest models.