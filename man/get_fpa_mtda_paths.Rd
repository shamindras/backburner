% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fpa-fod-transform.R
\name{get_fpa_mtda_paths}
\alias{get_fpa_mtda_paths}
\title{Provides a helper dataframe to list all of the extracted FPA FOD files}
\usage{
get_fpa_mtda_paths(ds_source = "fpa_fod", dl_date)
}
\arguments{
\item{ds_source}{(character) : name of the data source, default to "fpa"}

\item{dl_date}{(date) : Date in which the file were downloaded. This is
going to look for a folder in the `data/{ds_source}/` named
as this date}
}
\value{
A tibble with filenames
}
\description{
Provides a helper dataframe to list all of the extracted FPA FOD files
}
