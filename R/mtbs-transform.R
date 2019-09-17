#' Setup the column names for MTBS.
#' There are two sets of names because there are two dataframes being returned.
#' These have to match the data dictionary names
#'
#' @param mtbs_type (character) : This specifies the type of MTBS data we want
#' to import and takes 2 values \code{mtbs_perimeter_data, mtbs_fod_pts_data}
#'
#' @return (list): two character vectors, one with the original column names
#'                 and the other one with the transformed column names
#' @export
mtbs_transform_col_names <- function(mtbs_type){
    if (mtbs_type == 'mtbs_perimeter_data'){
        ORIG_COLNAMES <- c("Fire_ID",
                           "Fire_Name",
                           "Year",
                           "StartMonth",
                           "StartDay",
                           "Fire_Type",
                           "Acres",
                           "geometry")

        TFORM_COLNAMES <- c("fire_id",
                            "fire_name",
                            "start_year",
                            "start_mth",
                            "start_day",
                            "fire_type",
                            "acres_burned",
                            "geometry")

    } else if (mtbs_type == 'mtbs_fod_pts_data') {
        ORIG_COLNAMES <- c("Fire_ID",
                           "Fire_Name",
                           "Asmnt_Type",
                           "Pre_ID",
                           "Post_ID",
                           "Fire_Type",
                           "ND_T",
                           "IG_T",
                           "Low_T",
                           "Mod_T",
                           "High_T",
                           "Ig_Date",
                           "Lat",
                           "Long",
                           "Acres",
                           "geometry")

        TFORM_COLNAMES <- c("fire_id",
                            "fire_name",
                            "asmnt_type",
                            "pre_id",
                            "post_id",
                            "fire_type",
                            "nd_t",
                            "ig_t",
                            "low_t",
                            "mod_t",
                            "high_t",
                            "ig_date",
                            "geo_lat",
                            "geo_long",
                            "acres_burned",
                            "geometry")
    } else {
        stop("Wrong MTBS data type. It can either be `mtbs_perimeter_data` or`mtbs_fod_pts_data`")
    }
    return(list("ORIG_COLNAMES" = ORIG_COLNAMES,
                "TFORM_COLNAMES" = TFORM_COLNAMES))
}


#' Provides a helper dataframe to list all of the files extracted for MTBS
#' for a specified year of download
#'
#' @param ds_source (character) : data names. Default to "mtbs"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param mtbs_type (character) : This specifies the type of MTBS data we want
#' to import and takes 2 values \code{mtbs_perimeter_data, mtbs_fod_pts_data}
#'
#' @return A tibble of filenames
#'
#'@examples
#'\dontrun{
#' get_mtbs_mtda_paths(ds_source = "mtbs",
#'                     dl_date = base::as.Date("2019-07-06"),
#'                     mtbs_type = "mtbs_perimeter_data")
#'}
#' @export
get_mtbs_mtda_paths <- function(dl_date, mtbs_type, ds_source = "mtbs"){

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
    outdir_mtda_yr_dir <- fs::path_join(parts = c(outdir_mtda, mtbs_type))

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
                      ind_sit_reps = stringr::str_detect(string = fname,
                                                         pattern =
                                                             stringr::fixed('sit_rep',
                                                                            ignore_case = TRUE)),
                      ind_perims =
                          backburner::ind_re_match(string = fname,
                                                   pattern = "mtbs_perims_DD"),
                      ind_fod_pts =
                          backburner::ind_re_match(string = fname,
                                                   pattern = "mtbs_fod_pts_DD"))

    base::return(outdir_mtda_yr_files)
}


#' Transform the MTBS file
#'
#' @param fpath (character) : The path to the shapefile we want to transform
#' @param new_colnames (character) : New list of colnames we want to set for
#' our transformed output shapefile. If \code{NULL} the column names will get
#' converted to lower case and spaces replaced by underscores via the
#' \code{janitor} package
#'
#' @return (sf object) : transformed shapefile
#' @export
get_transform_mtbs <- function(fpath, exp_orig_colnames, new_colnames){
    shp_df <- sf::read_sf(fpath)
    readin_colnames <- base::colnames(x = shp_df)

    # Check if the colnames that we read in from the required dataframe
    # match those that we expect from our global variables list
    assert_that(all(readin_colnames == exp_orig_colnames))

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


#' Format Specific MTBS Shapefile
#'
#' @param ds_source (character) : data names. Default to "mtbs"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @param mtbs_type (character) : This specifies the type of MTBS data we want
#' to import and takes 2 values \code{mtbs_perimeter_data, mtbs_fod_pts_data}
#' @param orig_colnames (array) : Name of the columns in the original shapefile
#' @param tform_colnames (array) : Name of the columns in the transformed shapefile
#'
#' @return A sf data frame containing the shapefile contents
#' @examples
#'\dontrun{
#' list_colnames <- mtbs_transform_col_names(mtbs_type = "mtbs_perimeter_data")
#' format_transform_mtbs(ds_source = "mtbs",
#'                     dl_date = base::as.Date("2019-07-06"),
#'                     mtbs_type = "mtbs_perimeter_data",
#'                     orig_colnames = list_colnames$ORIG_COLNAMES,
#'                     tform_colnames = list_colnames$TFORM_COLNAMES)
#'}
#' @export
format_transform_mtbs <- function(dl_date, mtbs_type, orig_colnames,
                                  tform_colnames, ds_source = "mtbs") {

    # ND: I am creating a bad `IF` statement here but I am not sure how to deal
    # with it in a nicer way, as dplyr gets annoying when you need to filter one
    # column non dynamically and the other dynamically
    if (mtbs_type == 'mtbs_perimeter_data'){
        out_mtda_fpath <- get_mtbs_mtda_paths(ds_source = ds_source, dl_date = dl_date,
                                              mtbs_type = mtbs_type) %>%
            dplyr::filter(ind_shp, ind_perims) %>%
            dplyr::select(fpath) %>%
            base::unlist(x = ., use.names = FALSE)
    } else if (mtbs_type == 'mtbs_fod_pts_data'){
        out_mtda_fpath <- get_mtbs_mtda_paths(ds_source = "mtbs", dl_date = dl_date,
                                              mtbs_type = mtbs_type) %>%
            dplyr::filter(ind_shp, ind_fod_pts) %>%
            dplyr::select(fpath) %>%
            base::unlist(x = ., use.names = FALSE)
    }

    return(get_transform_mtbs(fpath = out_mtda_fpath,
                              exp_orig_colnames = orig_colnames,
                              new_colnames = tform_colnames))
}



#' Wrapper for MTBS transformation pipeline.
#' This function will transform both mtbs files, i.e.
#' \code{mtbs_perimeter_data, mtbs_fod_pts_data}.
#' @param ds_source (character) : data names. Default to "mtbs"
#' @param dl_date (date) : Date in which the file were downloaded. This is
#'                         going to look for a folder in the `data/{ds_source}/` named
#'                         as this date
#' @return A list with two shapefiles for loading, in which the key is the
#'         mtbs_type
#'
#' @export
mtbs_transform <- function(dl_date, ds_source = "mtbs"){

    sources <- c('mtbs_perimeter_data', 'mtbs_fod_pts_data')
    names(sources) <- sources

    mtbs_shapefiles <- purrr::map(sources,
                                  function(mtbs_type) {
                                      list_colnames <- mtbs_transform_col_names(mtbs_type = mtbs_type)
                                      shapefile_out <- format_transform_mtbs(ds_source = ds_source,
                                                                             dl_date = dl_date,
                                                                             mtbs_type = mtbs_type,
                                                                             orig_colnames = list_colnames$ORIG_COLNAMES,
                                                                             tform_colnames = list_colnames$TFORM_COLNAMES)
                                      return(shapefile_out)
                                  })

    return(mtbs_shapefiles)
}

