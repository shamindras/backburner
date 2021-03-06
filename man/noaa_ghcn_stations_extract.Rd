% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ghcn-stations-extract.R
\name{noaa_ghcn_stations_extract}
\alias{noaa_ghcn_stations_extract}
\title{This is a simply wrapper function to extract \code{noaa ghcn}
station, states, and countries metadata as individual tibbles.
This uses the \code{rnoaa} package directly to do this.}
\usage{
noaa_ghcn_stations_extract()
}
\value{
This will be list with 3 separate tibbles named
\code{ghcnd_stations, ghcnd_states, ghcnd_countries}
}
\description{
This is a simply wrapper function to extract \code{noaa ghcn}
station, states, and countries metadata as individual tibbles.
This uses the \code{rnoaa} package directly to do this.
}
\examples{
\dontrun{
ghcnd_stations_metadata <- backburner::noaa_ghcn_stations_extract()
}

}
