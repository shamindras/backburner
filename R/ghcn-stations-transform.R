#' Transformed GHCN-Daily Stations file. This is metadata for the GHCN-daily
#' file. It contains additional info on US/Canada states and country
#' information for each station
#'
#' @examples
#' \dontrun{
#' ghcnd_stations_tform <- backburner::get_transform_ghcnd_stations()
#' }
#'
#' @return (tibble) : Transformed tibble of GHCN stations metadata
#' @export
get_transform_ghcnd_stations <- function(){

    # First extract the station, states, and countries metadata
    ghcnd_stations_metadata <- backburner::noaa_ghcn_stations_extract()

    # Download the ghcn stations, states, countries table
    ghcnd_stations <- ghcnd_stations_metadata[["ghcnd_stations"]]
    ghcnd_states <- ghcnd_stations_metadata[["ghcnd_states"]]
    ghcnd_countries <- ghcnd_stations_metadata[["ghcnd_countries"]]

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
                                           end = 12)) %>%
        dplyr::rename(state_code = state) %>%
        dplyr::left_join(x = ., y = ghcnd_states,
                         by = c("state_code")) %>%
        dplyr::left_join(x = ., y = ghcnd_countries,
                         by = c("id_fips_code")) %>%
        dplyr::select(dplyr::starts_with("id"),
                      latitude, longitude, elevation,
                      state_code, state_name, country_name,
                      name,
                      dplyr::everything()) %>%
        janitor::clean_names()

    base::return(ghcnd_stations)
}
