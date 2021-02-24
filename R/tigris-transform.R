#' Calculates areas of tigris state, county, and military geometry (polygons)
#' in units of square meters, square kilometers, square miles. It appends
#' these columns onto the input tigris tibble
#' (of either state, county, or military).
#'
#' @param tigris_tbl (sf) : Processed tigris \code{sf} \code{tibble} already
#' projected to CRS 4326
#'
#' @return (tibble): The input tigris \code{sf} \code{tibble} with
#' 3 additional columns area calculations appended to it.
#' @export
tigris_calc_areas <- function(tigris_tbl){
    tigris_tbl_area <-
        tigris_tbl %>%
        dplyr::mutate(area_sq_meters = sf::st_area(geometry),
                      area_sq_kilometers =
                          units::set_units(x = area_sq_meters,
                                           value = km^2),
                      area_sq_miles = area_sq_kilometers * 0.38610215854781257,
                      area_acres = area_sq_kilometers * 247.10538147) %>%
        # Remove the units attribute from converted columns
        dplyr::mutate(dplyr::across(dplyr::contains("area_sq"),
                                    as.numeric),
                      area_acres = as.numeric(x = area_acres)) %>%
        dplyr::relocate(dplyr::contains("area_sq"),
                        area_acres,
                        .before = geometry)
    base::return(tigris_tbl_area)
}

#' Transforms the TIGER/Line Files and Shapefiles for US States, US Counties,
#' and US military lines for a given year, and resolution, in preparation
#' for loading/modeling purposes
#'
#' @param tig_res (character) : The resolution of the cartographic boundary
#' file (if \code{cb == TRUE}). Defaults to '500k'; options include
#' '5m' (1:5 million) and '20m' (1:20 million). Default is '5m'
#' @param tig_year (integer) : The year for which you want to download the
#' @param tig_cb (logical) : If cb is set to \code{TRUE}, download a
#' generalized (1:500k) states file. Defaults to \code{FALSE} (the most
#' detailed TIGER/Line file)
#' @param tig_crs (integer) : The Coordinate Reference System (CRS) system with
#' default value of 4326
#'
#' @return (list) : A list of shapefiles with cleaned up column names for
#' US States, US Counties, and US military lines for a given year, and
#' resolution, transformed for loading/modeling purposes.
#' @export
#'
#'@examples
#'\dontrun{
#'tigris_out <- tigris_transform(tig_res = '5m', tig_year = 2019,
#'                               tig_cb = FALSE, tig_crs = 4326)
#'}
tigris_transform <- function(tig_res = '5m', tig_year = 2019, tig_cb = FALSE,
                             tig_crs = 4326){

    # Get basic extraction of tigris shapefiles
    tigris_ext <- tigris_extract(tig_res = tig_res, tig_year = tig_year,
                                 tig_cb = tig_cb, tig_crs = tig_crs)

    tigris_states    <- tigris_ext$tigris_states %>%
                            dplyr::mutate(geoid   = as.numeric(geoid),
                                          region   = as.numeric(region),
                                          division = as.numeric(division),
                                          aland    = as.numeric(aland),
                                          awater   = as.numeric(awater),
                                          statens  = as.numeric(statens),
                                          intptlon = as.numeric(intptlon),
                                          intptlat = as.numeric(intptlat)) %>%
                            tibble::as_tibble(x = .) %>%
                            dplyr::rowwise(data = .) %>%
                            dplyr::mutate(intpt = sf::st_sfc(sf::st_point(
                                cbind(intptlon, intptlat)),
                                crs = tig_crs)) %>%
                            dplyr::ungroup(x = .) %>%
                            sf::st_as_sf(x = .) %>%
                            sf::st_transform(x = ., crs = tig_crs)

    tigris_counties  <- tigris_ext$tigris_counties %>%
                            dplyr::mutate(geoid   = as.numeric(geoid),
                                          aland    = as.numeric(aland),
                                          awater   = as.numeric(awater),
                                          intptlon = as.numeric(intptlon),
                                          intptlat = as.numeric(intptlat)) %>%
                            tibble::as_tibble(x = .) %>%
                            dplyr::rowwise(data = .) %>%
                            dplyr::mutate(intpt =
                                              sf::st_sfc(sf::st_point(
                                                  cbind(intptlon, intptlat)),
                                                  crs = tig_crs)) %>%
                            dplyr::ungroup(x = .) %>%
                            sf::st_as_sf(x = .) %>%
                            sf::st_transform(x = ., crs = tig_crs)

    tigris_military  <- tigris_ext$tigris_military %>%
                            dplyr::mutate(aland    = as.numeric(aland),
                                          awater   = as.numeric(awater),
                                          intptlon = as.numeric(intptlon),
                                          intptlat = as.numeric(intptlat)) %>%
                            tibble::as_tibble(x = .) %>%
                            dplyr::rowwise(data = .) %>%
                            dplyr::mutate(intpt =
                                              sf::st_sfc(sf::st_point(
                                                  cbind(intptlon, intptlat)),
                                                  crs = tig_crs)) %>%
                            dplyr::ungroup(x = .) %>%
                            sf::st_as_sf(x = .) %>%
                            sf::st_transform(x = ., crs = tig_crs)

    # Obtain list of transformed shapefiles
    out_tig_nms <- c("tigris_states", "tigris_counties", "tigris_military")
    out_tig_sfs <- base::mget(out_tig_nms)
    out_tig_sfs_area <-
        out_tig_sfs %>%
            purrr::map(.x = .,
                       .f = ~backburner::tigris_calc_areas(tigris_tbl = .x))
    base::return(out_tig_sfs_area)
}
