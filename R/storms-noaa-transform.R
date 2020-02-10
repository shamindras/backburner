#' Provides a helper dataframe to list all of the files extracted for NOAA STORMEVENTS
#' for a specified year of download
#'
#' @param ds_source (character) : name of the data source, default to "noaa_stormevents"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param yr (integer) : The specifics NOAA STORMEVENTS year shapefile we want to
#' transform
#'
#' @return A tibble with filenames
#' @export
get_noaa_stormevents_mtda_paths <- function(dl_date, yr, ds_source = 'noaa_stormevents'){

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
                      noaa_type = stringr::str_extract(string = fpath,
                                                       pattern = "(details|locations|fatalities)"),
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


#' Get data dictionaries for column names given .xlxs file
#'
#' @param ddict_xlsx_path : .xlxs file path
#' @param cols_rng : Column range in each of the excel sheets name
#'
#' @return List with column names in the different .xlxs pages
#' @export
get_ddict_noaa_stormevents <- function(ddict_xlsx_path, cols_rng){
    dim_noaa_stormevents_details <- backburner::get_ddict(
        ddict_xlsx_path = ddict_xlsx_path,
        sheet_nm = "dim_noaa_storms_ev_details",
        cols_rng = cols_rng)

    dim_noaa_stormevents_location <- backburner::get_ddict(
        ddict_xlsx_path = ddict_xlsx_path,
        sheet_nm = "dim_noaa_storms_ev_locations",
        cols_rng = cols_rng)

    dim_noaa_stormevents_fatalities <- backburner::get_ddict(
        ddict_xlsx_path = ddict_xlsx_path,
        sheet_nm = "dim_noaa_storms_ev_fatalaties",
        cols_rng = cols_rng)

    out_list <- list(dim_noaa_stormevents_details,
                     dim_noaa_stormevents_location,
                     dim_noaa_stormevents_fatalities)

    names(out_list) <- c("details", "locations", "fatalities")

    base::return(out_list)
}


#' Set Shapefile column names given dictionary names fetched from .xlsx
#' data dictionary file.
#'
#' @param fpath : shapefile path
#' @param year : year the shapefile is referred to
#' @param ddict_type : dictionary type - this needs to be corresponding to one
#'                     of the data dictionaries retrieved in the .xlsx
#' @param ddict_xlsx_path : path of the data dictionary .xlsx
#' @param cols_rng : column ranges to extract from the .xlsx
#'
#' @return Shapefile dataframe
#' @export
get_noaa_stormevents_new_cnames <- function(fpath, year, noaa_type,
                                            ddict_xlsx_path, cols_rng){

    # Obtain all dictionaries of geomac variable name mappings
    ddict_noaa <- get_ddict_noaa_stormevents(ddict_xlsx_path = ddict_xlsx_path,
                                             cols_rng = cols_rng)

    # Get specific dictionary mapping for required shapefile
    noaa_dict <- ddict_noaa[[noaa_type]]

    csv_df <- vroom::vroom(file = fpath, delim = ",", col_names = TRUE)

    # Get raw column names and convert to lower case
    orig_colnames_lwr <- base::colnames(x = csv_df) %>%
        stringr::str_to_lower(string = .) %>%
        tibble::enframe(x = ., name = NULL, value = "orig_varname")

    # Join on the new column name from our dictionary
    new_colnames_df <- orig_colnames_lwr %>%
        dplyr::left_join(x = ., y = noaa_dict,
                         by = "orig_varname")

    new_colnames <- new_colnames_df %>%
        dplyr::select(new_varname) %>%
        base::unlist() %>%
        base::as.vector()

    # Set the new column names for our imported data frame
    base::colnames(csv_df) <- new_colnames

    # Return the new imported shapefile, with revised column names
    base::return(csv_df)
}



#' Identify NOOA type from file name
#'
#' @export
noaa_stormevents_identify <- function(path,
                                      types = c('details', 'locations',
                                                'fatalities')){
    for (type in types){
        if (stringr::str_detect(path, type)){
            return(type)
        }
    }
    return(NA)
}


#' Wrapper for NOAA STORMEVENTS transformation pipeline.
#'
#' @param ds_source (character) : data names. Default to "noaa_stormevents"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param yrs (array): numerical array with the years which needs to be transformed
#' @return (list) : A list of transformed shapefiles, in which the names is the year
#'
#' @export
noaa_stormevents_transform <- function(dl_date, yrs, ds_source = "noaa_stormevents"){

    NOAA_DDICT_XLSX_PATH <- here::here("data", "Fire-Prediction-Data-Dictionary.xlsx")
    NOAA_DDICT_COLS_RNG <- "A:E"

    outdir_mtda_yr_files <- yrs %>%
        purrr::map_df(.x = ., .f =
                          ~get_noaa_stormevents_mtda_paths(
                              ds_source = ds_source,
                              dl_date = dl_date,
                              yr = .x)) %>%
        dplyr::arrange(year)

    out_mtda_fpaths <- outdir_mtda_yr_files %>%
        dplyr::filter(ind_csv == 1) %>%
        dplyr::mutate(ddict_xlsx_path = NOAA_DDICT_XLSX_PATH,
                      cols_rng = NOAA_DDICT_COLS_RNG) %>%
        dplyr::select(fpath, year, noaa_type, ddict_xlsx_path, cols_rng)

    nooa_stormevents_tform_details <- out_mtda_fpaths %>%
        dplyr::filter(noaa_type == 'details') %>%
        purrr::pmap(.l = ., .f = get_noaa_stormevents_new_cnames) %>%
        purrr::map(.f = noaa_stormevents_details_transform)
    names(nooa_stormevents_tform_details) <- yrs

    nooa_stormevents_tform_loc <- out_mtda_fpaths %>%
        dplyr::filter(noaa_type == 'locations') %>%
        purrr::pmap(.l = ., .f = get_noaa_stormevents_new_cnames)
    names(nooa_stormevents_tform_loc) <- yrs

    nooa_stormevents_tform_fatalities <- out_mtda_fpaths %>%
        dplyr::filter(noaa_type == 'fatalities') %>%
        purrr::pmap(.l = ., .f = get_noaa_stormevents_new_cnames) %>%
        purrr::map(.f = noaa_stormevents_fatalities_transform)
    names(nooa_stormevents_tform_fatalities) <- yrs

    # Now set the names and figure out what happens when there are multiple years

    nooa_stormevents_tform <- list(
        nooa_stormevents_tform_details,
        nooa_stormevents_tform_loc,
        nooa_stormevents_tform_fatalities
    )

    names(nooa_stormevents_tform) <- c('details', 'locations', 'fatalities')
    return(nooa_stormevents_tform)
}


#' Convert NOAA timestamps to R datetimes.
#'
#' NOAA inconveniently uses a weird timestamp format in different time zones,
#' and R's strptime can't accept vectorized input with one time zone per element
#' (instead of one fixed time zone). So we have to manually loop.
noaa_stormevents_parse_datetime <- function(datetimes, tzs, format) {
    assertthat::assert_that(length(datetimes) == length(tzs))

    # .POSIXct takes the bare list of doubles from unlist and marks its class as POSIXct
    .POSIXct(unlist(purrr::map(seq_along(datetimes),
                               function(ii) {
                                   as.POSIXct(strptime(datetimes[ii], format, tz = tzs[ii]))
                               })))
}

noaa_stormevents_pointify <- function(lats, lons) {
    assertthat::assert_that(length(lats) == length(lons))

    sf::st_sfc(purrr::map(seq_along(lats),
                          function(ii) {
                              sf::st_point(c(lons[ii], lats[ii]))
                          }))
}

noaa_stormevents_details_transform <- function(details) {
    date_format <- "%d-%b-%y %H:%M:%S"

    details <- details %>%
        mutate(begin_date = noaa_stormevents_parse_datetime(beg_dtt, cz_tzone, date_format),
               end_date = noaa_stormevents_parse_datetime(end_dtt, cz_tzone, date_format),
               begin_loc = noaa_stormevents_pointify(begin_lat, begin_lon),
               end_loc = noaa_stormevents_pointify(end_lat, end_lon)) %>%
        select(-begin_ym, -begin_day, -begin_time, -end_ym, -end_day, -end_time,
               -beg_dtt, -end_dtt, -year, -name_mth, -begin_lat, -begin_lon,
               -end_lat, -end_lon) %>%
        sf::st_sf()

    # The data documentation does not specify the coordinate reference system, but
    # it is latitude/longitude, so we assume they're using WGS84 like sensible
    # people.
    st::st_crs(details) <- 4326

    return(details)
}

noaa_stormevents_fatality_date <- function(timestamp) {
    as.Date(strptime(timestamp, format = "%m/%d/%Y %H:%M:%S"))
}

noaa_stormevents_fatalities_transform <- function(fatalities) {
    fatalities <- fatalities %>%
        mutate(fatality_date = noaa_stormevents_fatality_date(fatality_dtt)) %>%
        select(-year_month, -evnt_ym, -fatality_day, -fatality_time, -fatality_dtt)
}
