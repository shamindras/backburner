% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/etl-utils.R
\name{setup_global_variables}
\alias{setup_global_variables}
\title{Setup the global variables for the various extract functions}
\usage{
setup_global_variables(
  dat_flag,
  data_dl_method = "libcurl",
  metadata_dirname = "metadata"
)
}
\arguments{
\item{dat_flag}{(character) : name of the data - will appear as data folder
name when data are saved}

\item{data_dl_method}{(character) : "libcurl" (default) data download method}

\item{metadata_dirname}{(character) : "metadata_dirname" (default) metadata
folder name}
}
\description{
Setup the global variables for the various extract functions
}
