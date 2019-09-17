#' Setup the global variables for MTBS data
#'
#' @param dat_flag (character) : "mtbs" (default) name of the data - will appear
#'                               as data folder name when data are saved
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata" (default) metadata
#'                                       folder name
#' @param url (character) : NULL (default) data url - the default one is encoded
#'                          in the function body
#'
#' @export
mtbs_setup_variables <- function(dat_flag = 'mtbs', data_dl_method = 'libcurl',
                                 metadata_dirname = 'metadata', url = NULL){
    backburner::setup_global_variables(dat_flag = dat_flag, data_dl_method = data_dl_method,
                                       metadata_dirname = metadata_dirname)
    if (is.null(url)){
        url <- "https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/MTBS_Fire/data/composite_data/"
    }
    MAIN_MTBS_DL_URL <<- stringr::str_c(url, sep = "")
    MTBS_DL_URL_SFX <<- c("fod_pt_shapefile/mtbs_fod_pts_data.zip",
                          "burned_area_extent_shapefile/mtbs_perimeter_data.zip")
    RE_PTRN_ARCHIVE <<- "(zip|gz)$" # generic archive matching pattern
}

#' Extracting information on the download URLs for MTBS functions
#'
#' @export
mtbs_read_data <- function(){
    mtbs_mtda <- tibble::tibble(fname = base::basename(MTBS_DL_URL_SFX),
                                furl = stringr::str_c(MAIN_MTBS_DL_URL,
                                                      MTBS_DL_URL_SFX,
                                                      sep = ""),
                                ind_zip = backburner::ind_re_match(string = fname,
                                                                   pattern = RE_PTRN_ARCHIVE),
                                # In this case metadata column is the same
                                # as zip indicator. Duplicated to have consistent
                                # metadata column names
                                ind_mtda = as.integer(!ind_zip),
                                dl_datetime = dl_datetime,
                                dl_date = dl_date,
                                dl_sysname = dl_sysname,
                                dat_src_flag = DAT_SRC_FLAG)
    return(mtbs_mtda)
}

#' Download full data and metadata information for MTBS data
#' @param mtbs_mtda (tibble) : MTBS url and data information tibble
#'
#' @export
mtbs_metadata <- function(mtbs_mtda){
    mtbs_all <-
        mtbs_mtda %>%
        dplyr::mutate(.data = .,
                      outpath =
                          dplyr::if_else(ind_zip == 1,
                                         here::here("data",
                                                    DAT_SRC_FLAG,
                                                    base::format(dl_date,
                                                                 "%Y%m%d"),
                                                    fname),
                                         here::here("data",
                                                    DAT_SRC_FLAG,
                                                    base::format(dl_date,
                                                                 "%Y%m%d"),
                                                    GLOBAL_MTDA_DIRNAME,
                                                    fname)),
                      outdir = fs::path_ext_remove(path = outpath)) %>%
        dplyr::select(fname, furl,
                      ind_zip,
                      dl_datetime, dl_date, dl_sysname,
                      dat_src_flag, outpath, ind_mtda, outdir)
    return(mtbs_all)
}

#' Download data for MTBS data
#' @param noaa_all (tibble) : Data and Metadata info
#' @param data_folder (character) : "data" (default) data folder name
#' @param remove (logical) : FALSE (default) remove .zip file after extraction
#'
#' @export
mtbs_data_download <- function(mtbs_all, data_folder = 'data', remove = FALSE){
    outpath_mtda <- here::here(data_folder, DAT_SRC_FLAG, base::format(dl_date,
                                                                       "%Y%m%d"),
                               stringr::str_c(DAT_SRC_FLAG, "metadata.csv",
                                              sep = "_"))
    outpath_mtda_all <- here::here(data_folder, DAT_SRC_FLAG, base::format(dl_date,
                                                                           "%Y%m%d"),
                                   stringr::str_c(DAT_SRC_FLAG, "metadata_all.csv",
                                                  sep = "_"))
    outdir_mtda <- outpath_mtda %>% fs::path_dir(path = .)
    outdir_mtda_all <- outpath_mtda_all %>% fs::path_dir(path = .)
    fs::dir_create(path = outdir_mtda)

    mtbs_all %T>%
        readr::write_csv(x = ., path = outpath_mtda_all, col_names = TRUE) %>%
        dplyr::filter(!ind_mtda) %>%
        dplyr::select(furl, outpath, ind_zip, outdir) %>%
        dplyr::mutate(extr = base::as.logical(ind_zip),
                      remove = base::as.logical(remove)) %>%
        dplyr::select(-ind_zip) %T>%
        readr::write_csv(x = ., path = outpath_mtda, col_names = TRUE) %>%
        purrr::pwalk(backburner::dl_extract_file)
}

#' Wrapper for MTBS data extraction pipeline.
#' This function will setup global variables, gather information and urls on
#' data and metadata and download the data.
#' @param dat_flag (character) : "mtbs" (default) name of the data -
#'                               will appear as data folder name when data are
#'                               saved
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata" (default) metadata
#'                                       folder name
#' @param url (character) : NULL (default) data url - the default one is encoded
#'                          in the function body
#' @param data_folder (character) : "data" (default) data folder name
#' @param remove (logical) : FALSE (default) remove .zip file after extraction
#'
#' @export
mtbs_extract <- function(dat_flag = 'mtbs', data_dl_method = 'libcurl',
                         url = NULL, remove = FALSE, data_folder = 'data',
                         metadata_dirname = 'metadata'){
    mtbs_setup_variables(dat_flag = dat_flag,
                         data_dl_method = data_dl_method,
                         url = url, metadata_dirname = metadata_dirname)
    mtbs_mtda <- mtbs_read_data()
    mtbs_all <- mtbs_metadata(mtbs_mtda = mtbs_mtda)
    mtbs_data_download(mtbs_all = mtbs_all, remove = remove,
                       data_folder = data_folder)
}
