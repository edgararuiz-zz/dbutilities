#export
db_raster <- function(data,resolution = 300, output = "data", ...) {
  args <- list(...)

  x_field <- colnames(data)[1]
  y_field <- colnames(data)[2]

  data_prep <- data %>%
    dplyr::select_(x = x_field, y = y_field) %>%
    dplyr::filter(!is.na(x), !is.na(y))

  s <- data_prep %>%
    dplyr::summarise(max_x = max(x),
                     max_y = max(y),
                     min_x = min(x),
                     min_y = min(y)) %>%
    dplyr::mutate(rng_x = max_x - min_x,
                  rng_y = max_y - min_y,
                  resolution = resolution) %>%
    collect

  counts <- data_prep %>%
    dplyr::mutate(res_x = round((x - s$min_x) / s$rng_x * resolution, 0),
                  res_y = round((y - s$min_y) / s$rng_y * resolution, 0)) %>%
    dplyr::count(res_x, res_y) %>%
    collect

  if(output == "ggplot"){
    counts <- counts %>%
      ggplot() +
      geom_raster(aes(res_x, res_y, fill = n)) +
      labs(x = x_field, y = y_field) +
      scale_fill_continuous(name = "Frequency")
  }

  return(counts)
}
