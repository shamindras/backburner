% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fire-base-180d-sql-qry-utils.R
\name{gen_query_fire_base_ghcnd_sfeat_180d}
\alias{gen_query_fire_base_ghcnd_sfeat_180d}
\title{Generate single feature GHCN-D lagged aggregated time summaries mapped to
individual MTBS fires by specified spatial degrees over a 180 day lag period
for each fire}
\usage{
gen_query_fire_base_ghcnd_sfeat_180d(
  fire_base_start_date = base::as.Date("2000-01-01"),
  max_lag_fire_base_days = 180,
  spat_deg_rad = 0.5,
  summary_type = "AVG",
  sfeat,
  out_qry_dir,
  out_tbl_name_pfx,
  fire_base_tbl_name
)
}
\arguments{
\item{fire_base_start_date}{(date) : The minimum date to filter MTBS fire data, we
will i.e. dataset will only contain MTBS fires after this date}

\item{max_lag_fire_base_days}{(integer) : The number of days for GHCN-D weather
data to lag behind the \code{fire_base_start_date}}

\item{spat_deg_rad}{(double) : The spatial degree radius to consider around
each fire when joining weather data (default value is 0.5)}

\item{summary_type}{(character) : The \code{SQL} summary function to apply
to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")}

\item{sfeat}{(character) : The GHCN-D feature to summarize for each given
fire. Aggregated and lagged versions of each feature are produced}

\item{out_qry_dir}{(character) : The directory to save the query. A full
file path will be generated as part of the output}
}
\value{
(tibble) : A \code{tibble} with the query string, file path to store
the query, and a check of whether the filepath exists
}
\description{
Generate single feature GHCN-D lagged aggregated time summaries mapped to
individual MTBS fires by specified spatial degrees over a 180 day lag period
for each fire
}
\examples{
\dontrun{
library("tidyverse")
gen_query_fire_base_ghcnd_sfeat_180d(fire_base_start_date = base::as.Date("2000-01-01"),
                                max_lag_fire_base_days = 180,
                                spat_deg_rad = 0.5,
                                summary_type = "AVG",
                                sfeat = "TOBS",
                                out_qry_dir = ".",
                                out_tbl_name_pfx = "mtbs",
                                fire_base_tbl_name = "mtbs_base_01")
}
}
