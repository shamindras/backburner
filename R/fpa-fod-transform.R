#' Provides a helper dataframe to list all of the extracted FPA FOD files
#'
#' @param ds_source (character) : name of the data source, default to "fpa"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @return A tibble with filenames
#' @export
get_fpa_mtda_paths <- function(ds_source = 'fpa_fod',
                               dl_date){

    # This the download prefix for the FPA FOD files e.g. the GPKG format
    # of the FPA FOD database is named
    # "https://www.fs.usda.gov/rds/archive/products/RDS-2013-0009.4/RDS-2013-0009.4_GPKG.zip" i.e. the "RDS-2013-0009.4" is the common prefix, which we hardcode
    # here
    FPA_FOD_PFIX <- "RDS-2013-0009.4"

    # The extracted FPA FOD files have the following prefix e.g. the GPKG format
    # of the FPA FOD database is extracted with filename "FPA_FOD_20170508.gpkg"
    # i.e. the ""FPA_FOD_20170508"" is the common prefix, which we hardcode
    FPA_FOD_DL_PFIX <- "FPA_FOD_20170508"

    # Define key directories and metadata file paths
    outpath_mtda <- here::here("data", ds_source, base::format(dl_date,
                                                               "%Y%m%d"),
                               stringr::str_c(ds_source,
                                              "metadata.csv",
                                              sep = "_"))
    outpath_mtda_all <- here::here("data", ds_source, base::format(dl_date,
                                                                   "%Y%m%d"),
                                   stringr::str_c(ds_source,
                                                  "metadata_all.csv",
                                                  sep = "_"))
    # outdir_mtda <- outpath_mtda %>% fs::path_dir(path = .)
    # outdir_mtda_all <- outpath_mtda_all %>% fs::path_dir(path = .)
    # outdir_mtda_yr_dir <- fs::path_join(parts = c(outdir_mtda, fpa_fod_type))

    outdir_mtda_yr_files <- outpath_mtda_all %>%
        vroom::vroom(file = ., delim = ",", col_names = TRUE) %>%
        dplyr::rowwise(data = .) %>%
        dplyr::mutate(fextn = fs::path_ext(fname),
                      # fname = fs::path_file(fpath),
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
                      ind_zip =
                          backburner::ind_re_match(string = fextn,
                                                   pattern = "zip$"),
                      fpa_type = stringr::str_replace(
                          string = fs::path_ext_remove(path = fname),
                          pattern = stringr::str_c(FPA_FOD_PFIX, "_"),
                          replacement = ""),
                      fpa_full_fpath =
                          fs::path_join(c(outdir,
                                          "Data",
                                          glue::glue("{FPA_FOD_DL_PFIX}.{stringr::str_to_lower(fpa_type)}"))))
    base::return(outdir_mtda_yr_files)
}


#' Transform the FPA FOD fires file
#'
#' @param fpath (character) : The path to the FPA_FOD GPKG file we want
#' to transform. The column names will get converted to lower case and spaces
#' replaced by underscores via the \code{janitor} package
#'
#' @return (sf object) : transformed shapefile
#' @export
get_transform_fpa <- function(fpath){

    # connect to db
    con <- RSQLite::dbConnect(drv = RSQLite::SQLite(),
                              dbname = fpath)

    # Download the fires table into a tibble
    fires <- RSQLite::dbGetQuery(conn = con, statement = "SELECT * FROM Fires")
    fires <- fires %>%
                janitor::clean_names(dat = .) %>%
                tibble::as_tibble(x = .) %>%
                dplyr::select(-shape) %>%
                dplyr::mutate(cont_date = as.Date(x = cont_date),
                              discovery_date = as.Date(x = discovery_date)) %>%
                dplyr::rename(mtbs_fire_id = mtbs_id,
                              us_state = state) %>%
                sf::st_as_sf(x = ., coords = c("longitude", "latitude"),
                             crs = 4326, agr = "constant")
    # In dplyr calculating differences in dates causes posix issues, so this
    # is done outside of the dplyr pipeline for the duration calculation
    fires_sf <- fires
    fires_sf$fire_duration <- base::as.double(fires_sf$cont_date -
                                              fires_sf$discovery_date)

    # Close the connection to the database
    RSQLite::dbDisconnect(conn =  con)

    base::return(fires_sf)
}


#' Wrapper for FPA FOD transformation pipeline.
#'
#' @param ds_source (character) : data names. Default to "fpa"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param fpa_fod_type (character) : The specifics FPA FOD data format.
#' Currently only supported option is "GPKG", which imports the geopackage
#' data format
#' @return (list) : A list of transformed shapefiles, in which the names is the year
#'
#' @export
fpa_transform <- function(dl_date,
                          ds_source = "fpa_fod",
                          fpa_fod_type = "GPKG"){

    # NOTE: Only tiles transform is supported currently
    assertthat::assert_that(fpa_fod_type == "GPKG",
                            msg = glue::glue('fpa_fod_type = "GPKG" is supported
                                             currently fpa_fod_type is
                                             {fpa_fod_type}'))

    outdir_mtda_yr_files <- get_fpa_mtda_paths(
                              ds_source = ds_source,
                              dl_date = dl_date)

    # For FPA_FOD, this is a single path, since we only import one data source
    # type at a time e.g. "GPKG" etc
    out_mtda_fpaths <- outdir_mtda_yr_files %>%
        dplyr::filter(ind_zip == 1, fpa_type == fpa_fod_type) %>%
        dplyr::select(fpa_full_fpath) %>%
        base::unlist(x = ., use.names = FALSE)
    print(out_mtda_fpaths)

    outdir_mtda_yr_out <- get_transform_fpa(fpath = out_mtda_fpaths)
    names(outdir_mtda_yr_out) <- glue::glue("{ds_source}_{stringr::str_to_lower(string = fpa_fod_type)}")

    return(outdir_mtda_yr_out)
}
