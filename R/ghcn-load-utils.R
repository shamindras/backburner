#' Get the unique United States station from our \code{ghcn stations} table
#'
#' @param conn (connection) : A connection to the PostGIS database containing
#' the \code{ghcn_stations} table
#'
#' @return (tibble): A tibble of unique United States GHCN stations with
#' the associated station id, elevation, location fields
#'
#' @export
get_ghcn_stations_us_unq <- function(conn){

    # Get the GHCN station data from our PostgreSQL server
    ghcn_stations <- sf::st_read(conn, query = "SELECT * FROM ghcn_stations")
    ghcn_stations_us_crs <- ghcn_stations %>% sf::st_crs(x = .)

    # Filter to GHCN US Only
    ghcn_stations_us <- ghcn_stations %>%
                            dplyr::filter(country_name == "United States") %>%
                            dplyr::select(id, elevation, location) %>%
                            dplyr::arrange(id, elevation)

    # Convert sf to tibble, for efficiency of joins.
    # It appears if this conversion is done after the previous operations
    # then it is faster, than as part of the %>%
    ghcn_stations_us <- ghcn_stations_us %>% tibble::as_tibble(x = .)

    # Just create an arbitrary row number for every combination of
    # station id and elevation, and then just pick the first record
    # for each station id, this way we get a unique (arbitrary) selection
    # of ghcn station id, elevation, and location
    ghcn_stations_us_unq_df <- ghcn_stations_us %>%
                                    dplyr::arrange(id, elevation) %>%
                                    dplyr::group_by(id) %>%
                                    dplyr::mutate(rownum = row_number()) %>%
                                    dplyr::filter(rownum == min(rownum)) %>%
                                    dplyr::select(-rownum) %>%
                                    dplyr::ungroup()

    out_list <- list("ghcn_stations_us_unq_df" = ghcn_stations_us_unq_df,
                     "ghcn_stations_us_crs" = ghcn_stations_us_crs)
    base::return(out_list)
}

#' Read in GHCN-D data for a given year and filter to US stations for loading
#'
#' @param ghcn_yr (integer) : The year in which the GHCN-D data is to be
#' downloaded
#' @param ghcn_stations_us_unq (list) : The output list of GHCN-D stations from
#' running \code{backburner::get_ghcn_stations_us_unq()}
#' @param ghcn_dl_date (date) : Date of extraction for the GHCN-D year file
#' @param ghcn_elem_selvars (character) : The GHCN-D element values that are
#' selected in the final data
#'
#' @return (tibble) : Cleaned and final transformed GHCN-D data for
#' loading to our database
#' @export
ghcn_load_us_stations <- function(ghcn_yr,
                                  ghcn_dl_date,
                                  ghcn_stations_us_unq,
                                  ghcn_elem_selvars){

    # Get US station data
    ghcn_stations_us_unq_df <- ghcn_stations_us_unq$ghcn_stations_us_unq_df
    ghcn_stations_us_crs <- ghcn_stations_us_unq$ghcn_stations_us_crs

    # Get unique US station IDs, useful for filtering
    ghcn_stations_us_unq_id <- ghcn_stations_us_unq_df %>% dplyr::select(id)

    # Let's get the GHCN data for the required year
    # Only run this once for each year, in a single day
    # since output is stored in the dir named after the date
    backburner::noaa_ghcn_extract(year_periods = ghcn_yr)

    # Pick out the first and only year
    ghcn_yr_dat <- backburner::ghcn_transform(dl_date = ghcn_dl_date,
                                              ghcn_yr)[[1]]

    # Convert the variables we want to select to a tibble
    # We will filter these element values out using this tibble
    ghcn_elem_selvars_df <- ghcn_elem_selvars %>%
                                tibble::enframe(x = ., name = NULL,
                                                value = "element")

    # Let's do subsetting and pivoting
    ghcn_yr_dat <- ghcn_yr_dat %>%
                        # Filter only GHCN-D elements that we require
                        dplyr::inner_join(x = .,
                                          y = ghcn_elem_selvars_df,
                                          by = "element") %>%
                        # Filter only to US Stations
                        dplyr::inner_join(x = .,
                                          y = ghcn_stations_us_unq_id,
                                          by = "id")

    # Filter out poor quality data based on q flag and m flag
    ghcn_yr_dat <- ghcn_yr_dat %>%
                        dplyr::filter(is.na(q_flag) & is.na(m_flag)) %>%
                        dplyr::select(-q_flag, -m_flag, -s_flag, -obs_tm)

    # Get the mean daily recorded value by element for each station id
    ghcn_yr_dat <- ghcn_yr_dat %>%
                        dplyr::group_by(id, record_dt, element) %>%
                        dplyr::summarise(data_val =
                                             mean(data_val, na.rm = TRUE)) %>%
                        dplyr::ungroup()

    # Pivot to wider ghcn format
    ghcn_yr_dat <- ghcn_yr_dat %>%
                        tidyr::pivot_wider(data = ., names_from = element,
                                           values_from = data_val)

    # Join on station elevation and location
    ghcn_yr_dat <- ghcn_yr_dat %>%
                        dplyr::inner_join(x = .,
                                          y = ghcn_stations_us_unq_df,
                                          by = "id") %>%
                        dplyr::select(id, record_dt, elevation,
                                      location, dplyr::everything())

    # Add in sf object details for the station
    ghcn_yr_dat <- ghcn_yr_dat %>%
                        sf::st_as_sf(x = .) %>%
                        sf::st_set_crs(x = ., value = ghcn_stations_us_crs)

    base::return(ghcn_yr_dat)
}
