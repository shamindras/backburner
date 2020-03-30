#' Provides a helper dataframe to list all of the files extracted for geomac
#' for a specified year of download
#'
#' @param ds_source (character) :
#' @param dl_date (date) :
#' @param yr (integer) : The specifics geomac year shapefile we want to
#' transform
#'
#' @return A tibble of metadata paths
#' @export
get_geomac_mtda_paths <- function(ds_source, dl_date, yr){

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
                          ind_re_match(string = fextn,
                                       pattern = "shp$"),
                      ind_prj =
                          ind_re_match(string = fextn,
                                       pattern = "prj$"),
                      ind_shx =
                          ind_re_match(string = fextn,
                                       pattern = "shx$"),
                      ind_dbf =
                          ind_re_match(string = fextn,
                                       pattern = "dbf$"),
                      ind_sqlite =
                          ind_re_match(string = fextn,
                                       pattern = "sqlite$"),
                      ind_perims = stringr::str_detect(string = fname,
                                                       pattern =
                                                           stringr::fixed('perim',
                                                                          ignore_case = TRUE)),
                      ind_sit_reps = stringr::str_detect(string = fname,
                                                         pattern =
                                                             stringr::fixed('sit_rep',
                                                                            ignore_case = TRUE)),
                      year = yr)

    base::return(outdir_mtda_yr_files)
}

#' Transform the geomac file
#'
#' @param fpath (character) : The path to the shapefile we want to transform
#' @param new_colnames (character) : New list of colnames we want to set for our
#'     transformed output shapefile. If \code{NULL} the column names will get
#'     converted to lower case and spaces replaced by underscores via the
#'     \code{janitor} package
#' @param target_srid (numeric): Transform the geometries into this coordinate
#'     system, specified by SRID (default: 4326, GPS latitude/longitude)
#'
#' @return (sf object) : transformed shapefile
#' @export
get_transform_geomac <- function(fpath, exp_orig_colnames, new_colnames, target_srid = 4326){
    shp_df <- sf::read_sf(fpath) %>% sf::st_transform(target_srid)
    readin_colnames <- base::colnames(x = shp_df)

    # Check if the colnames that we read in from the required dataframe
    # match those that we expect from our global variables list
    base::stopifnot(assertthat::are_equal(x = readin_colnames,
                                          y = exp_orig_colnames))

    # Set the colnames manually or automatically convert them to lower case
    if(!is.null(new_colnames)){
        shp_df <- sf::read_sf(fpath) %>%
            magrittr::set_colnames(x = ., value = new_colnames)
    } else {
        shp_df <- sf::read_sf(fpath) %>%
            janitor::clean_names(dat = ., case = "lower")
    }

    base::return(shp_df)
}


#' Get data dictionaries for column names given .xlxs file
#'
#' @param ddict_xlsx_path : .xlxs file path
#' @param cols_rng : Column range in each of the excel sheets name
#'
#' @return List with column names in the different .xlxs pages
#' @export
get_ddict_geomac <- function(ddict_xlsx_path, cols_rng){
    dim_geomac_perims_pre15_xl <- backburner::get_ddict(
        ddict_xlsx_path = ddict_xlsx_path,
        sheet_nm = "dim_geomac_perims_pre15",
        cols_rng = cols_rng)

    dim_geomac_perims_pst15_xl <- backburner::get_ddict(
        ddict_xlsx_path = ddict_xlsx_path,
        sheet_nm = "dim_geomac_perims_pst15",
        cols_rng = cols_rng)

    dim_geomac_sitreps_pre15_xl <- backburner::get_ddict(
        ddict_xlsx_path = ddict_xlsx_path,
        sheet_nm = "dim_geomac_sitreps_pre15",
        cols_rng = cols_rng)

    dim_geomac_sitreps_pst15_xl <- backburner::get_ddict(
        ddict_xlsx_path = ddict_xlsx_path,
        sheet_nm = "dim_geomac_sitreps_pst15",
        cols_rng = cols_rng)
    out_list <- list(dim_geomac_perims_pre15_xl,
                     dim_geomac_perims_pst15_xl,
                     dim_geomac_sitreps_pre15_xl,
                     dim_geomac_sitreps_pst15_xl)

    names(out_list) <- c("dim_geomac_perims_pre15", "dim_geomac_perims_pst15",
                         "dim_geomac_sitreps_pre15", "dim_geomac_sitreps_pst15")

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
get_geomac_new_cnames <- function(fpath, year, ddict_type,
                                  ddict_xlsx_path, cols_rng){

    # Obtain all dictionaries of geomac variable name mappings
    ddict_geomac <- get_ddict_geomac(ddict_xlsx_path = ddict_xlsx_path,
                                     cols_rng = cols_rng)

    # Get specific dictionary mapping for required shapefile
    geomac_dict <- ddict_geomac[[ddict_type]]

    # Read in shapefile
    shp_df <- sf::read_sf(fpath)

    # Get raw column names and convert to lower case
    orig_colnames_lwr <- base::colnames(x = shp_df) %>%
        stringr::str_to_lower(string = .) %>%
        tibble::enframe(x = ., name = NULL, value = "orig_varname")

    # Join on the new column name from our dictionary
    new_colnames_df <- orig_colnames_lwr %>%
        dplyr::left_join(x = ., y = geomac_dict,
                         by = "orig_varname")
    # TODO: Check that there are no NAs here

    new_colnames <- new_colnames_df %>%
        dplyr::select(new_varname) %>%
        base::unlist() %>%
        base::as.vector()

    # Set the new column names for our imported data frame
    base::colnames(shp_df) <- new_colnames

    # Return the new imported shapefile, with revised column names
    base::return(shp_df)
}


#' Format GEOMAC shapefiles given path
#'
#' @param shapefiles_path (character) : Path of the shapefile to be transformed
#' @param geomac_type : Can be either `geomac_fire_perimeters` or
#'                      `geomac_situation_report`. Different types filter
#'                      on different columns for the creation of the shapefile
#'
#' @return A shapefile object
#' @export
geomac_format_shapefiles <- function(shapefiles_path, geomac_type){

    # ND: Creating an IF statement over here for geomac_type

    if (geomac_type == 'geomac_fire_perimeters'){
        outdir_mtda_yr_perims <- shapefiles_path %>%
            dplyr::filter(ind_perims) %>%
            dplyr::select(fpath, year, ddict_type,
                          ddict_xlsx_path, cols_rng)

        geomac_tform <- outdir_mtda_yr_perims %>%
            purrr::pmap(.l = ., .f = get_geomac_new_cnames)
    } else if (geomac_type == 'geomac_situation_report'){
        outdir_mtda_yr_sit_reps <- shapefiles_path %>%
            dplyr::filter(ind_sit_reps) %>%
            dplyr::select(fpath, year, ddict_type,
                          ddict_xlsx_path, cols_rng)

        geomac_tform <- outdir_mtda_yr_sit_reps %>%
            purrr::pmap(.l = ., .f = get_geomac_new_cnames)
    }
    base::return(geomac_tform)
}


#' Determine Sheet Prefix for Firepred Data Dictionary
#'
#' Before 11/7/2019, both fire perimeters and incident reports displayed
#' two sets of variable, depending on whether the data were recorded before
#' or after 2015.
#' As of 11/7/2019, fire perimeters have been updated so that the variable
#' names are consistent across years now. However, incident reports still
#' presents two different variable names depending on whether the year
#' was before or after 2015.
#' This function looks at which sets of variable names to look at based on
#' whether it's a perimeter or incident report file, which year the file is
#' and whether we are dealing with data before 11//7/2019 changes.
#' @param ind_perims (logical): Boolean indicator. If TRUE, the file is a perimeter file,
#'                              otherwise it is an incident report.
#' @param yrs (numeric): Vector with years which data need to be transformed
#' @param pre_pst_split_ind (logical): Boolean indicator. If TRUE, then variable names for perimeters
#'                                     change before and after 2015 (as before 11/7/2019 changes),
#'                                     so that the `*_pre15` sheets are used in the Firepred
#'                                     data dictionary for before 2015 and the `*_pst15' for the
#'                                     years afterwards. If FALSE (default since 11/7/2019 changes),
#'                                     the `*_pst15' sheets are used across the years for perimeters.
#' @return A vector of `pre15` or `pst15` according to which Data Dictionary sheet should be considered.
#' @export
derive_year_sheet_ind <- function(ind_perims, year, pre_pst_split_ind){
    ind_var <- base::ifelse(ind_perims & !pre_pst_split_ind, TRUE, FALSE)
    out <- base::ifelse(ind_var,
                        'pst15',
                        dplyr::case_when(
                            year >=2007 & year <= 2015 ~ "pre15",
                            year > 2015 ~ "pst15"))
    return(out)
}



#' Full Pipeline for Transforming GEOMAC Shapefiles
#'
#' This function will transform both geomac files, i.e.
#' \code{geomac_fire_perimeters, geomac_situation_report}.
#' @param ds_source (character) : data names. Default to "geomac"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param yrs (numeric): Vector with years which data need to be transformed
#' @param pre_pst_split_ind (logical): Boolean indicator. If TRUE, then variable names for perimeters
#'                                     change before and after 2015 (as before 11/7/2019 changes),
#'                                     so that the `*_pre15` sheets are used in the Firepred
#'                                     data dictionary for before 2015 and the `*_pst15' for the
#'                                     years afterwards. If FALSE (default since 11/7/2019 changes),
#'                                     the `*_pst15' sheets are used across the years for perimeters.
#' @return A list with two shapefiles for loading, in which the key is the
#'         mtbs_type
#' @export
geomac_transform <- function(dl_date, yrs, ds_source = "geomac", pre_pst_split_ind=FALSE){

    GEOMAC_DDICT_XLSX_PATH <- here::here("data", "Fire-Prediction-Data-Dictionary.xlsx")
    GEOMAC_DDICT_COLS_RNG <- "A:E"

    # Identify perimeter and situation reports shapefiles
    outdir_mtda_yr_shp <- yrs %>%
        purrr::map_df(.x = ., .f =
                          ~get_geomac_mtda_paths(
                              ds_source = ds_source,
                              dl_date = dl_date,
                              yr = .x)) %>%
        dplyr::filter(ind_shp == 1, ind_perims | ind_sit_reps) %>%
        dplyr::arrange(year) %>%
        dplyr::mutate(geomac_type =
                          dplyr::if_else(ind_perims,
                                         "perims",
                                         "sitreps"),
                      pre_pst_type = derive_year_sheet_ind(
                          ind_perims, year, pre_pst_split_ind),
#                           base::ifelse(ind_perims,
#                           base::ifelse(pre_pst_split_ind,
#                                             dplyr::case_when(
#                                               year >=2007 & year <= 2015 ~ "pre15",
#                                               year > 2015 ~ "pst15"),
#                                             'pst15'),
#                           base::ifelse(pre_pst_split_ind,
#                                         dplyr::case_when(
#                                             year >=2007 & year <= 2015 ~ "pre15",
#                                             year > 2015 ~ "pst15"),
#                                         dplyr::case_when(
#                                             year >=2007 & year <= 2015 ~ "pre15",
#                                             year > 2015 ~ "pst15"))),
                      ddict_type =
                          stringr::str_c("dim_geomac",
                                         geomac_type,
                                         pre_pst_type,
                                         sep = "_"),
                      ddict_xlsx_path = GEOMAC_DDICT_XLSX_PATH,
                      cols_rng = GEOMAC_DDICT_COLS_RNG) %>%
        dplyr::select(fpath, year, ddict_type,
                      ddict_xlsx_path, cols_rng,
                      ind_perims, ind_sit_reps)

    # Return shapefiles
    sources <- c('geomac_fire_perimeters', 'geomac_situation_report')
    names(sources) <- sources

    geomac_shapefiles <- purrr::map(sources,
                                    function(geomac_type) {
                                        shapefile_out <- geomac_format_shapefiles(
                                            shapefiles_path = outdir_mtda_yr_shp,
                                            geomac_type = geomac_type
                                        )
                                        return(shapefile_out)
                                    })

    base::return(geomac_shapefiles)
}
