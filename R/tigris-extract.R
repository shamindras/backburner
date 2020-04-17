#' Extracts the TIGER/Line Files and Shapefiles for US States, US Counties,
#' and US military lines for a given year, and resolution, in preparation
#' for transform purposes
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
#' resolution, with cleaned column names, ready for modeling transformations.
#' @export
#'
#'@examples
#'\dontrun{
#'tigris_out <- tigris_extract(tig_res = '5m', tig_year = 2019,
#'                             tig_cb = FALSE, tig_crs = 4326)
#'}
tigris_extract <- function(tig_res = '5m', tig_year = 2019, tig_cb = FALSE,
                           tig_crs = 4326){

    tigris_states    <- tigris::states(cb = tig_cb, resolution = tig_res,
                                       year = tig_year)
    tigris_counties  <- tigris::counties(cb = tig_cb, resolution = tig_res,
                                         year = tig_year)
    tigris_military  <- tigris::military(year = tig_year)

    out_tig_nms <- c("tigris_states", "tigris_counties", "tigris_military")
    out_tig_sps <- base::mget(out_tig_nms)
    out_tig_sfs <-
        out_tig_sps %>%
        purrr::map(.x = .,
                   .f = ~backburner::sp_transform_sf(sp = .x,
                                                     crs = tig_crs))
    base::return(out_tig_sfs)
}
