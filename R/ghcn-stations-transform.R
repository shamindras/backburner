#' Transformed GHCN-Daily Stations file. This is metadata for the GHCN-daily
#' file. It contains additional info on US/Canada states and country
#' information for each station.
#'
#' @examples
#' \dontrun{
#' ghcnd_stations_tform <- backburner::get_transform_ghcnd_stations()
#' }
#'
#' @return (sf) : Transformed sf data frame of GHCN stations metadata
#' @export
get_transform_ghcnd_stations <- function() {

    # First extract the station, states, and countries metadata
    ghcnd_stations_metadata <- backburner::noaa_ghcn_stations_extract()

    # Download the ghcn stations, states, countries table
    ghcnd_stations <- ghcnd_stations_metadata[["ghcnd_stations"]]
    ghcnd_states <- ghcnd_stations_metadata[["ghcnd_states"]]
    ghcnd_countries <- ghcnd_stations_metadata[["ghcnd_countries"]]

    ## A helper function. Below, we apply st_point to every row's latitude and
    ## longitude to get an SF object. But st_point takes one argument with
    ## both coordinates, not two arguments with individual coordinates, so
    ## provide this helper function to adjust.
    make_point <- function(x, y) {
        sf::st_point(c(x, y))
    }

    # Do cleaning of ghcn station table to US/Canada state and country tables
    ghcnd_stations <- ghcnd_stations %>%
        dplyr::mutate(id_fips_code =
                          stringr::str_sub(string = id, start = 1,
                                           end = 2),
                      id_network_code =
                          stringr::str_sub(string = id, start = 3,
                                           end = 3),
                      id_station_numsys =
                          stringr::str_sub(string = id, start = 4,
                                           end = 12),
                      location = sf::st_sfc(purrr::pmap(list(longitude, latitude),
                                                        make_point))) %>%
        dplyr::rename(state_code = state) %>%
        dplyr::left_join(x = ., y = ghcnd_states,
                         by = c("state_code")) %>%
        dplyr::left_join(x = ., y = ghcnd_countries,
                         by = c("id_fips_code")) %>%
        dplyr::select(dplyr::starts_with("id"),
                      elevation, state_code, state_name, country_name,
                      name, dplyr::everything()) %>%
        dplyr::select(-longitude, -latitude) %>%
        janitor::clean_names() %>%
        sf::st_sf()

    ## GHCN documentation just specifies locations as latitude and longitude in
    ## degrees, and does not specify the CRS. Assume that they mean WGS 84, EPSG
    ## 4326.
    sf::st_crs(ghcnd_stations) <- 4326

    return(ghcnd_stations)
}
