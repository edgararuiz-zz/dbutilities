#' db_histogram
#'
#' @export
#' @param data Data frame or Spark table
#' @param bins Number of bins for the Histogram. Defaults to 30.
#' @param output Sets the type of output, defaults to 'data'. Possible values: 'data', 'ggplot'
#'
#' @details
#'
#' Pushes the computation of the bins to the server and returns a data frame with the specified number of bins
db_histogram <- function(data,bins = 30, output = "data", ...) {
  args <- list(...)

  x_name <- colnames(data)[1]

  data_prep <- data %>%
    select_(x_field = x_name) %>%
    filter(!is.na(x_field)) %>%
    mutate(x_field = as.double(x_field))

  s <- data_prep %>%
    summarise(max_x = max(x_field), min_x = min(x_field)) %>%
    mutate(bin_value = (max_x - min_x) / bins) %>%
    collect()

  new_bins <- as.numeric(c((0:(bins - 1) * s$bin_value) + s$min_x, s$max_x))

  if(class(data)[1]=="tbl_spark"){
    plot_table <- data_prep %>%
      ft_bucketizer(input.col = "x_field", output.col = "key_bin", splits = new_bins) %>%
      group_by(key_bin) %>%
      tally() %>%
      collect()
  }


  all_bins <- data.frame(
    key_bin = 0:(bins - 1),
    bin = 1:bins,
    bin_ceiling = head(new_bins, -1)
  )

  plot_table <- plot_table %>%
    full_join(all_bins, by="key_bin") %>%
    arrange(key_bin) %>%
    mutate(n = ifelse(!is.na(n), n, 0)) %>%
    select( count = n,   bin_values =  bin_ceiling)

  if(output == "ggplot"){
    plot_table <- plot_table %>%
      ggplot() +
      geom_bar(aes(bin_values, count), stat = "identity") +
      labs(x = x_name)
  }

  return(plot_table)
}

