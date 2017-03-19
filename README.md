dbutilties
================

A set of time and cost (import/compute) saving functions as tools:

-   **db\_histogram** - Calculates the bins of a Histogram inside the database. And by default returns a data frame with the values. When the **output** argument is changed to **ggplot** the function will return a geom\_bar ggplot, which meant for a quick plot view.

-   **db\_raster** - Groups and aggregates the values of two variables so as to allow a large dataset be visualized. When the **output** argument is changed to **ggplot** the function will return a geom\_bar ggplot, which meant for a quick plot view.
