% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/etl-utils.R
\name{dl_extract_file}
\alias{dl_extract_file}
\title{Download file from specified url into specified path. If the directory
of the destination path is not specified then the function will create the
directory. Also gives the user the option to extract the contents of the
downloaded file}
\usage{
dl_extract_file(furl, outpath, outdir, extr = TRUE, remove = FALSE)
}
\arguments{
\item{furl}{(character) : URL for the required file download}

\item{outpath}{(character) : Full destination path for the specified
file download}

\item{extr}{(logical) : TRUE (default) to extract contents of download else
specify FALSE}

\item{remove}{(logical) : FALSE (default) to remove the .zip file once it is
extracted.}
}
\description{
Download file from specified url into specified path. If the directory
of the destination path is not specified then the function will create the
directory. Also gives the user the option to extract the contents of the
downloaded file
}
