################
################
# expand.grid in sparklyr
################
################

# load packages
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE)
  try(sapply(pkg, require, character.only = TRUE), silent = TRUE)
}
packages <- c("foreach", "doParallel", 
              "boot", "sparklyr")
ipak(packages)

# install the spark
## If you wish to install Spark on your local system, 
## simply install the sparklyr package and call spark_install()

# build the connection
# Connect to your local Spark cluster
spark_conn <- spark_connect(master = "local")

# create the data 
x <- rnorm(10^5)
y <- rnorm(10^5)
df <- data.frame(x,y)

# Copy track_metadata to Spark
track_metadata_tbl <- copy_to(spark_conn, df, overwrite = TRUE)

