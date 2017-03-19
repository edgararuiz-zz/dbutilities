type_conversion <- function(x, db) {
    new_type <- NULL
    if (db == "hive") {
        new_type <- x
        if (x == "integer")
            new_type <- "int"
        if (x == "numeric")
            new_type <- "double"
        if (x == "character")
            new_type <- "string"
        if (x == "POSIXct")
            new_type <- "timestamp"
        if (new_type == x)
            new_type <- NULL
    }

    if (db == "sparklyr") {
        #new_type <- x
        #if (x == "POSIXct")
        new_type <- "character"
    }


    return(new_type)
}

#' Use a sample CSV file to create a Hive table or pass the 'columns' argument to spark_read_csv
#'
#' @export
#' @param sample_file The path to a sample CSV file that will be used to determine the column types.
#' @param db The type of connection or database. Possible values: 'hive', 'sparklyr'.
#' @param sample_size The number of the top rows that will be sampled to determine the class. Defaults to 5.
#' @param dir_location 'hive' only - Passes the location of the directory where the data files are.
#' @param table_name 'hive' only - Passes the name of the table. Defaults to 'default'.
#' @details
#' This technique is meant to cut down the time of reading CSV files into the Spark context. It does that by
#' either passing the column names and types in spark_read_csv or by using SQL to create the table
#' @examples
#' \dontrun{
#' #Libraries needed for this example
#' library(tidyverse)
#' library(sparklyr)
#' library(dbutilities)
#' library(nycflights13)
#'
#' #Creating a local spark context
#' conf <- spark_config()
#' conf$`sparklyr.shell.driver-memory` <- "16G"
#' sc <- spark_connect(master = "local",
#'                     version = "2.1.0",
#'                     config = conf)
#'
#' #Using flights from nycflights13 for example
#' data("flights")
#' flights
#'
#' #Creating a csv file out of the flights table
#' if(!dir.exists("csv"))dir.create("csv")
#' write_csv(flights, "csv/flights.csv")
#'
#' #Mapping the CSV file (Hive)
#' create_sql <- db_map_csv(sample_file = "csv/flights.csv",
#'                          dir_location = file.path(getwd(), "csv"),
#'                          db = "hive",
#'                          table_name = "sql_flights")
#'
#' #Run resulting SQL command to create the table
#' DBI::dbGetQuery(sc, create_sql)
#'
#' #Mapping the CSV file (sparklyr)
#' flights_columns <- db_map_csv(sample_file = "csv/flights.csv")
#'
#' #Use spark_read_csv with the infer_schema argument set to FALSE
#' flights_noinfer <- spark_read_csv(sc,
#'                                   name = "noinfer_flights",
#'                                   path = "csv/",
#'                                   infer_schema = FALSE,
#'                                   columns = flights_columns)
#'
#' spark_disconnect(sc)
#' }

db_map_csv <- function(sample_file, db = "sparklyr", sample_size = 5, dir_location = NULL, table_name = NULL, ...) {

    dir_location <- ifelse(is.null(dir_location), dirname(sample_file), dir_location )
    top_rows <- suppressMessages(readr::read_csv(sample_file, n_max = sample_size))


    column_types <- purrr::map(top_rows, function(x) class(x)[1])

    column_names <- stringr::str_to_lower(colnames(top_rows))
    column_names <- stringr::str_replace(column_names, " ", "_")
    fields <- purrr::map(1:length(column_names), function(x) paste(column_names[x], type_conversion(column_types[x],
        db = db)))

    string_fields <- paste0(fields, collapse = ",")

    create_table <- NULL

    if (db == "hive") {

        table_name <- ifelse(is.null(table_name), " default ", table_name)
        start_sql <- paste0("CREATE EXTERNAL TABLE ", table_name ," (")
        end_sql <- ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
        location_sql <- paste0(" LOCATION '", dir_location  ,"'")

        create_table <- paste0(start_sql, string_fields, end_sql, location_sql)
    }

    if (db == "sparklyr") {
        create_table <- list()
        fields <- purrr::map(1:length(column_names), function(x) column_names[x])
        types <- purrr::map(1:length(column_names), function(x) type_conversion(column_types[[x]], db = db))

        create_table <- as.data.frame(fields, stringsAsFactors = FALSE)
        colnames(create_table) <- create_table[1, ]

        create_table[1, ] <- paste0(types)
        create_table <- as.list(create_table)
    }

    return(create_table)

}
