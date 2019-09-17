#' Setup the column names for GHCN.
#'
#' @return (array): character array with the variable names
#' @export
ghcn_transform_col_names <- function(){
    TFORM_GHCN_DAILY_COLNAMES <- c("id",
                                   "record_dt",
                                   "element",
                                   "data_val",
                                   "m_flag",
                                   "q_flag",
                                   "s_flag",
                                   "obs_tm")
    return(TFORM_GHCN_DAILY_COLNAMES)
}


#' Provides a helper dataframe to list all of the files extracted for GHCN
#' for a specified year of download
#'
#' @param ds_source (character) : name of the data source, default to "noaa_ghcn_daily"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param yr (integer) : The specifics GHCN year shapefile we want to
#' transform
#'
#' @return A tibble with filenames
#' @export
get_ghcn_mtda_paths <- function(dl_date, yr, ds_source = 'noaa_ghcn_daily'){

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


#' Transform the GHCN file
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
get_transform_ghcn <- function(fpath, new_colnames){

    # Read the ghcn file efficiently using vroom
    csv_df <- vroom::vroom(file = fpath, delim = ",", col_names = FALSE,
                           col_types = "cicdcccc")

    csv_df <- csv_df %>%
        magrittr::set_colnames(x = ., value = new_colnames) %>%
        dplyr::mutate(record_dt = lubridate::ymd(record_dt))

    base::return(csv_df)
}


#' Wrapper for GHCN transformation pipeline.
#'
#' @param ds_source (character) : data names. Default to "noaa_ghcn_daily"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param yrs (array): numerical array with the years which needs to be transformed
#' @return (list) : A list of transformed shapefiles, in which the names is the year
#'
#' @export
ghcn_transform <- function(dl_date, yrs, ds_source = "noaa_ghcn_daily"){

    tform_columns <- ghcn_transform_col_names()

    outdir_mtda_yr_files <- yrs %>%
        purrr::map_df(.x = ., .f =
                          ~get_ghcn_mtda_paths(
                              ds_source = ds_source,
                              dl_date = dl_date,
                              yr = .x)) %>%
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
                                      .f = ~get_transform_ghcn(fpath = .x,
                                                               new_colnames = .y))
    names(outdir_mtda_yr_out) <- yrs

    return(outdir_mtda_yr_out)
}

