type_conversion <- function(x, db){
  new_type <- NULL
  if(db == "hive"){
    new_type <- x
    if(x == "integer")new_type<-"int"
    if(x == "numeric")new_type<-"double"
    if(x == "character")new_type<-"string"
    if(x == "POSIXct")new_type<-"timestamp"
    if(new_type==x)new_type<-NULL
  }

  if(db == "sparklyr"){
    new_type <- x
    if(x == "POSIXct")new_type<-"character"
  }


  return(new_type)
}

#' db_histogram
#'
#' @export
#' @param filename A CSV file
#' @param db The type of connection or database. Possible values: 'hive', 'sparklyr'
#'
#' @details
#'
#' Utility to build the CREATE TABLE SQL for a Hive table or the columns list for sparklyr based on the top 5
#' rows in the CSV file
#'

db_map_csv  <- function(filename, db = "sparklyr", ...){

  top_rows <- suppressMessages(readr::read_csv(filename, n_max = 5))


  column_types <-purrr::map(top_rows, function(x)class(x)[1])

  column_names <- stringr::str_to_lower(colnames(top_rows))
  column_names <- stringr::str_replace(column_names, " ", "_")
  fields <-  purrr::map(1:length(column_names),
                        function(x)paste(column_names[x], type_conversion(column_types[x], db = db)))

  string_fields <- paste0(fields, collapse = ",")

  create_table <- NULL

  if(db == "hive"){
    start_sql <- "CREATE EXTERNAL TABLE taxi ("
    end_sql <- ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' TBLPROPERTIES('skip.header.line.count'='2')"

    create_table <- paste0(start_sql, string_fields, end_sql)
  }

  if(db == "sparklyr"){
    create_table <- list()
    fields <-  purrr::map(1:length(column_names),
                          function(x)column_names[x])

    types <-  purrr::map(1:length(column_names),
                         function(x)type_conversion(column_types[[x]], db = db))

    create_table <- as.data.frame(fields, stringsAsFactors = FALSE)
    colnames(create_table) <- create_table[1,]
    create_table[1,] <- paste0(types)
    create_table <- as.list(create_table)
  }

  return(create_table)

}
