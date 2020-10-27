#' This is a simply wrapper function to extract \code{noaa ghcn}
#' station, states, and countries metadata as individual tibbles.
#' This uses the \code{rnoaa} package directly to do this.
#'
#' @return This will be list with 3 separate tibbles named
#' \code{ghcnd_stations, ghcnd_states, ghcnd_countries}
#'
#' @examples
#' \dontrun{
#' ghcnd_stations_metadata <- backburner::noaa_ghcn_stations_extract()
#' }
#'
#' @export
noaa_ghcn_stations_extract <- function(){

    # Download the ghcn stations, states, countries table directly
    ghcnd_stations  <- rnoaa::ghcnd_stations() %>% tibble::as_tibble(x = .)
    ghcnd_states    <- rnoaa::ghcnd_states() %>% tibble::as_tibble(x = .)
    ghcnd_countries <- rnoaa::ghcnd_countries() %>% tibble::as_tibble(x = .)

    # Do renaming of columns for ghcn country table
    ghcnd_countries <- ghcnd_countries %>%
                            dplyr::rename(id_fips_code = code,
                                          country_name = name)

    # Do renaming of columns for ghcn state table
    ghcnd_states    <- ghcnd_states %>%
                            dplyr::rename(state_code = code,
                                          state_name = name)

    # Return output as a list of tibbles
    out_ghcnd       <- list("ghcnd_stations" = ghcnd_stations,
                            "ghcnd_states" = ghcnd_states,
                            "ghcnd_countries" = ghcnd_countries)

    base::return(out_ghcnd)
}
