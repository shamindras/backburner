#' Setup the column names for NOAA SWDI
#'
#' @return (array): character array with the variable names
#' @export
noaa_swdi_transform_col_names <- function(){
    TFORM_NOAA_SWDI_DAILY_COLNAMES <- c("record_dt",
                                        "geo_lon",
                                        "geo_lat",
                                        "tot_count")
    return(TFORM_NOAA_SWDI_DAILY_COLNAMES)
}


#' Provides a helper dataframe to list all of the files extracted for NOAA SWDI
#' for a specified year of download
#'
#' @param ds_source (character) : name of the data source, default to "noaa_swdi"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param yr (integer) : The specifics NOAA SWDI year CSV we want to
#' transform
#' @param noaa_swdi_ind_tiles (integer) : A value in \code{{0, 1}}, with 1
#' indicating that tiles data is to be downloaded and extracted,
#' and 0 indicating non-tiles data is to be downloaded and extracted. Currently
#' only a value of 1 i.e. tiles is supported.
#' @param noaa_swdi_type (character) : Specifying the type of NOAA-SWDI data to
#' download and extract. Can be one of the following values:
#' \code{hail, mda, meso, nldn, plsr, structure, tvs, warn}
#'
#' @return A tibble with filenames
#' @export
get_noaa_swdi_mtda_paths <- function(ds_source = 'noaa_swdi',
                                     dl_date,
                                     yr,
                                     noaa_swdi_ind_tiles,
                                     noaa_swdi_type){

    # NOTE: Only tiles transform is supported currently
    assertthat::assert_that(noaa_swdi_ind_tiles == 1)

    tiles_path <- dplyr::if_else(noaa_swdi_ind_tiles == 1, "tiles", "no_tiles")

    # Define key directories and metadata file paths
    outpath_mtda <- here::here("data", ds_source, base::format(dl_date,
                                                               "%Y%m%d"),
                               tiles_path,
                               noaa_swdi_type,
                               stringr::str_c(ds_source,
                                              "metadata.csv",
                                              sep = "_"))
    outpath_mtda_all <- here::here("data", ds_source, base::format(dl_date,
                                                                   "%Y%m%d"),
                                   tiles_path,
                                   noaa_swdi_type,
                                   stringr::str_c(ds_source,
                                                  "metadata_all.csv",
                                                  sep = "_"))
    outdir_mtda <- outpath_mtda %>% fs::path_dir(path = .)
    outdir_mtda_all <- outpath_mtda_all %>% fs::path_dir(path = .)
    outdir_mtda_yr_dir <- fs::path_join(parts = c(outdir_mtda, yr))

    outdir_mtda_yr_files <- outdir_mtda_yr_dir %>%
        # TODO: Check if we can set recurse = FALSE and only specify the
        #       unzipped directory once we extract to a specified path
        fs::dir_ls(glob = "*", recurse = TRUE) %>%
        tibble::enframe(x = ., name = NULL, value = "fpath") %>%
        dplyr::mutate(fextn = fs::path_ext(fpath),
                      fname = fs::path_file(fpath),
                      # Get different shape file type indicators
                      # https://www.earthdatascience.org/courses/earth-analytics/spatial-data-r/shapefile-structure/
                      ind_shp =
                          backburner::ind_re_match(string = fextn,
                                                   pattern = "shp$"),
                      ind_prj =
                          backburner::ind_re_match(string = fextn,
                                                   pattern = "prj$"),
                      ind_shx =
                          backburner::ind_re_match(string = fextn,
                                                   pattern = "shx$"),
                      ind_dbf =
                          backburner::ind_re_match(string = fextn,
                                                   pattern = "dbf$"),
                      ind_sqlite =
                          backburner::ind_re_match(string = fextn,
                                                   pattern = "sqlite$"),
                      ind_csv =
                          backburner::ind_re_match(string = fextn,
                                                   pattern = "csv$"),
                      year = yr)

    base::return(outdir_mtda_yr_files)
}


#' Transform the NOAA SWDI file
#'
#' TODO: Need to add assertions to check the dimension of the column names and
#'       also the values. Somehow need to parse in the metadata from the web
#'       and get the field values in an automated manner.
#'
#' @param fpath (character) : The path to the shapefile we want to transform
#' @param new_colnames (character) : New list of colnames we want to set for
#' our transformed output shapefile. If \code{NULL} the column names will get
#' converted to lower case and spaces replaced by underscores via the
#' \code{janitor} package
#'
#' @return (sf object) : transformed shapefile
#' @export
get_transform_noaa_swdi <- function(fpath, new_colnames){

    # Read the NOAA SWDI file efficiently using vroom
    csv_df <- vroom::vroom(file = fpath,
                           skip = 3, # We need to skip 3 rows for the SWDI tiles datasets
                           delim = ",",
                           col_names = FALSE,
                           col_types = "cddd")

    csv_df <- csv_df %>%
                magrittr::set_colnames(x = ., value = new_colnames) %>%
                dplyr::mutate(record_dt = lubridate::ymd(record_dt))

    # Convert to sf object (for geo lat/long pairs) under standard projection
    csv_sf <-  sf::st_as_sf(csv_df,
                            coords = c("geo_lon", "geo_lat"),
                            crs = 4326,
                            agr = "constant") %>%
                dplyr::select(record_dt, geometry, tot_count)

    base::return(csv_sf)
}


#' Wrapper for NOAA SWDI transformation pipeline.
#'
#' @param ds_source (character) : data names. Default to "noaa_swdi"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param yrs (array): numerical array with the years which needs to be transformed
#' @return (list) : A list of transformed shapefiles, in which the names is the year
#'
#' @export
noaa_swdi_transform <- function(dl_date,
                                yrs,
                                ds_source = "noaa_swdi",
                                noaa_swdi_ind_tiles = 1,
                                noaa_swdi_type){

    # NOTE: Only tiles transform is supported currently
    assertthat::assert_that(noaa_swdi_ind_tiles == 1)

    tform_columns <- noaa_swdi_transform_col_names()

    outdir_mtda_yr_files <- yrs %>%
        purrr::map_df(.x = ., .f =
                          ~get_noaa_swdi_mtda_paths(
                              ds_source = ds_source,
                              dl_date = dl_date,
                              yr = .x,
                              noaa_swdi_ind_tiles = noaa_swdi_ind_tiles,
                              noaa_swdi_type = noaa_swdi_type)) %>%
        dplyr::arrange(year)

    out_mtda_fpaths <- outdir_mtda_yr_files %>%
        dplyr::filter(ind_csv == 1) %>%
        dplyr::select(fpath) %>%
        base::unlist(x = ., use.names = FALSE) %>%
        base::as.list(x = .)

    # Created a repeated list of column names
    # TODO: May be a way to do this much more elegantly with `purrr::pmap`
    out_mtda_colnames <- purrr::map(.x = 1:length(out_mtda_fpaths),
                                    function(x) {tform_columns})

    outdir_mtda_yr_out <- purrr::map2(.x = out_mtda_fpaths, .y = out_mtda_colnames,
                                      .f = ~get_transform_noaa_swdi(fpath = .x,
                                                                    new_colnames = .y))
    names(outdir_mtda_yr_out) <- yrs

    return(outdir_mtda_yr_out)
}
