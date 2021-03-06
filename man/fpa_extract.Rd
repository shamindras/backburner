% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fpa-fod-extract.R
\name{fpa_extract}
\alias{fpa_extract}
\title{Wrapper for FPA data extraction pipeline.
This function will setup global variables, gather information and urls on
data and metadata and download the data.}
\usage{
fpa_extract(
  data_folder = "data",
  dat_flag = "fpa_fod",
  data_dl_method = "libcurl",
  remove = FALSE,
  url = NULL,
  metadata_dirname = "metadata"
)
}
\arguments{
\item{data_folder}{(character) : "data" (default) data folder name}

\item{dat_flag}{(character) : "fpa_fod" (default) name of the data -
will appear as data folder name when data are
saved}

\item{data_dl_method}{(character) : "libcurl" (default) data download method}

\item{remove}{(logical) : FALSE (default) remove .zip file after extraction}

\item{url}{(character) : NULL (default) data url - the default one is encoded
in the function body}

\item{metadata_dirname}{(character) : "metadata" (default) metadata
folder name}
}
\description{
Wrapper for FPA data extraction pipeline.
This function will setup global variables, gather information and urls on
data and metadata and download the data.
}
