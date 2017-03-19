#' db_raster
#'
#' @export
#' @param data Data frame or Spark table
#' @param resolution Number of squares to return to plot as raster. Defaults to 300.
#' @param output Sets the type of output, defaults to 'data'. Possible values: 'data', 'ggplot'
#'
#' @details
#'
#' Pushes the computation of the raster squares to the server and returns a data frame
db_raster <- function(data, resolution = 300, output = "data", ...) {
    args <- list(...)
    
    x_field <- colnames(data)[1]
    y_field <- colnames(data)[2]
    
    data_prep <- dplyr::select_(data, x = x_field, y = y_field)
    data_prep <- dplyr::filter(data_prep, !is.na(x), !is.na(y))
    
    s <- dplyr::summarise(data_prep, max_x = max(x), max_y = max(y), min_x = min(x), min_y = min(y))
    s <- dplyr::mutate(s, rng_x = max_x - min_x, rng_y = max_y - min_y, resolution = resolution)
    
    s <- dplyr::collect(s)
    
    counts <- dplyr::mutate(data_prep, res_x = round((x - s$min_x)/s$rng_x * resolution, 0), res_y = round((y - 
        s$min_y)/s$rng_y * resolution, 0))
    counts <- dplyr::count(counts, res_x, res_y) %>% counts <- dplyr::collect(counts)
    
    if (output == "ggplot") {
        counts <- ggplot2::ggplot(data = counts) + ggplot2::geom_raster(aes(res_x, res_y, fill = n)) + ggplot2::labs(x = x_field, 
            y = y_field) + ggplot2::scale_fill_continuous(name = "Frequency")
    }
    
    return(counts)
}
