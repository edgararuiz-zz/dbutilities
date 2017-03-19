#' Computes the bins of the histogram in the server
#'
#' @export
#' @param data Data frame or Spark table
#' @param bins Number of bins for the Histogram. Defaults to 30.
#' @param output Sets the type of output, defaults to 'data'. Possible values: 'data', 'ggplot'
#'
#' @details
#'
#' The function will use the first column in the data set.  Using the dplyr::select statement
#' prior to calling db_histogram will ease adding or removing a plotting step.  The 'data' value
#' for the 'db' argument will return a data frame instead of a plot.
#'
#' @examples
#' \dontrun{
#' # This will return a 'ggplot' of a 30 bin histogram of the 'distance' field
#' flights %>%
#' filter(arr_delay < 100) %>%
#'   select(distance) %>%
#'   db_histogram()
#' }
#'
#'
db_histogram <- function(data, bins = 30, output = "ggplot") {

    x_name <- colnames(data)[1]

    data_prep <- dplyr::select_(data, x_field = x_name)
    data_prep <- dplyr::filter(data_prep, !is.na(x_field))
    data_prep <- dplyr::mutate(data_prep, x_field = as.double(x_field))

    s <- dplyr::summarise(data_prep, max_x = max(x_field), min_x = min(x_field))
    s <- dplyr::mutate(s, bin_value = (max_x - min_x)/bins)
    s <- dplyr::collect(s)

    new_bins <- as.numeric(c((0:(bins - 1) * s$bin_value) + s$min_x, s$max_x))

    if (class(data)[1] == "tbl_spark") {
        plot_table <- sparklyr::ft_bucketizer(data_prep, input.col = "x_field", output.col = "key_bin", splits = new_bins)
        plot_table <- dplyr::group_by(plot_table, key_bin)
        plot_table <- dplyr::tally(plot_table)
        plot_table <- dplyr::collect(plot_table)
    }


    all_bins <- data.frame(key_bin = 0:(bins - 1), bin = 1:bins, bin_ceiling = head(new_bins, -1))

    plot_table <- dplyr::full_join(plot_table, all_bins, by = "key_bin")
    plot_table <- dplyr::arrange(plot_table, key_bin)
    plot_table <- dplyr::mutate(plot_table, n = ifelse(!is.na(n), n, 0))
    plot_table <- dplyr::select(plot_table, count = n, bin_values = bin_ceiling)

    if (output == "ggplot") {
      plot_table <- ggplot2::ggplot(data = plot_table) +
        ggplot2::geom_bar(ggplot2::aes(bin_values, count), stat = "identity") +
        ggplot2::labs(x = x_name)
    }

    return(plot_table)
}

