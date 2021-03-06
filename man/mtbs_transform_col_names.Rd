% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/mtbs-transform.R
\name{mtbs_transform_col_names}
\alias{mtbs_transform_col_names}
\title{Setup the column names for MTBS.
There are two sets of names because there are two dataframes being returned.
These have to match the data dictionary names}
\usage{
mtbs_transform_col_names(mtbs_type)
}
\arguments{
\item{mtbs_type}{(character) : This specifies the type of MTBS data we want
to import and takes 2 values \code{mtbs_perimeter_data, mtbs_fod_pts_data}}
}
\value{
(list): two character vectors, one with the original column names
                and the other one with the transformed column names
}
\description{
Setup the column names for MTBS.
There are two sets of names because there are two dataframes being returned.
These have to match the data dictionary names
}
