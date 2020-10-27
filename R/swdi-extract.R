#' Setup the global variables for NOAA SWDI data
#'
#' @param dat_flag (character) : "noaa_swdi" (default) name of the data -
#'                               will appear as data folder name when data are
#'                               saved
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata" (default) metadata
#'                                       folder name
#' @param url (character) : NULL (default) data url - the default one is encoded
#'                          in the function body
#'
#' @export
noaa_swdi_setup_variables <- function(dat_flag = 'noaa_swdi',
                                      data_dl_method = 'libcurl',
                                      metadata_dirname = 'metadata',
                                      url = NULL){
    backburner::setup_global_variables(dat_flag = dat_flag, data_dl_method = data_dl_method,
                                       metadata_dirname = metadata_dirname)

    RE_PTRN_ARCHIVE <<- "(zip|gz)$" # generic archive matching pattern
    RE_PTRN_YEAR <<- "\\d{4}"
    RE_PTRN_CSV_ONLY <<- "csv$"
    RE_PTRN_TILES <<- "tiles"
    RE_PTRN_SWDI_TYPE_TILES <<- "([:alpha:]|\\-)+[:alpha:]+"
    RE_PTRN_SWDI_TYPE <<- "^[:alpha:]+"

    if (is.null(url)){
        url <- "ftp://ftp.ncdc.noaa.gov/pub/data/swdi/database-csv/v2/"
    }
    noaa_url <<- stringr::str_c(url, sep = "")
}


#' Extracting information on the download URLs for NOAA SWDI data
#'
#' @export
noaa_swdi_read_data <- function(){

    # Getting URL Tibble
    hand <- curl::new_handle()
    gg <- curl::curl_fetch_memory(url = noaa_url, handle = hand)
    gg <- rawToChar(gg$content)

    noaa_urls <- readr::read_delim(file = gg, delim = "\\s+", col_names = FALSE) %>%
        tibble::as_tibble() %>%
        tidyr::separate(data = ., col = X1,
                        into = c("rw", "group", "ftp", "code", "size",
                                 "month", "dom", "time", "fname"),
                        sep = "\\s+") %>%
        dplyr::mutate(mod_year = base::as.integer(
            format(dl_date, "%Y"))) %>%
        tidyr::unite(data = ., col = mod_datetime, month, dom, mod_year,
                     time, sep = " ") %>%
        # Modify to a datetime format using lubridate
        # Setting the locale to "en_US.UTF-8" is important here
        dplyr::mutate(year_data =
                          base::as.integer(
                              stringr::str_extract(
                                  string = fname,
                                  pattern = RE_PTRN_YEAR)),
                      ind_tiles =
                          base::as.integer(
                              stringr::str_detect(string = fname,
                                                  pattern = RE_PTRN_TILES)),
                      swdi_type_tiles =
                          stringr::str_extract(
                              string = fname,
                              pattern = RE_PTRN_SWDI_TYPE_TILES),
                      swdi_type =
                          stringr::str_extract(
                              string = fname,
                              pattern = RE_PTRN_SWDI_TYPE),
                      mod_datetime =
                          lubridate::mdy_hm(mod_datetime,
                                            locale = "en_US.UTF-8"),
                      size_mb = base::as.double(size),
                      ind_zip =
                          base::as.integer(
                              stringr::str_detect(string = fname,
                                                  pattern = RE_PTRN_ARCHIVE)),
                      # In this case metadata column is the same
                      # as zip indicator. Duplicated to have consistent
                      # metadata column names
                      ind_mtda = base::as.integer(!ind_zip),
                      ind_csv_only =
                          base::as.integer(
                              stringr::str_detect(string = fname,
                                                  pattern = RE_PTRN_CSV_ONLY)),
                      dl_datetime = dl_datetime,
                      dl_date = dl_date,
                      dl_user = dl_user,
                      dl_sysname = dl_sysname,
                      dat_src_flag = DAT_SRC_FLAG) %>%
        dplyr::select(-c("rw", "group", "ftp"))

    return(noaa_urls)
}


#' Download full data and metadata information for NOAA SWDI data
#' @param noaa_urls (tibble) : NOAA SWDI URLs tibble
#' @param noaa_swdi_ind_tiles (integer) : A value in \code{{0, 1}}, with 1
#' indicating that tiles data is to be downloaded and extracted,
#' and 0 indicating non-tiles data is to be downloaded and extracted
#'
#' @export
noaa_swdi_metadata <- function(noaa_urls, noaa_swdi_ind_tiles){
    tiles_path <- dplyr::if_else(noaa_swdi_ind_tiles == 1, "tiles", "no_tiles")
    noaa_all <- noaa_urls %>%
        dplyr::mutate(furl = stringr::str_c(noaa_url,
                                            fname, sep = ""),
                      outpath =
                          dplyr::if_else(!base::is.na(year_data),
                                         here::here("data",
                                                    DAT_SRC_FLAG,
                                                    base::format(dl_date,
                                                                 "%Y%m%d"),
                                                    tiles_path,
                                                    swdi_type,
                                                    year_data,
                                                    fname),
                                         here::here("data",
                                                    DAT_SRC_FLAG,
                                                    base::format(dl_date,
                                                                 "%Y%m%d"),
                                                    tiles_path,
                                                    swdi_type,
                                                    GLOBAL_MTDA_DIRNAME,
                                                    fname)),
                      ind_mtda = as.integer(base::is.na(year_data)),
                      outdir = fs::path_dir(path = outpath)) %>%
        # Reorder dataset
        dplyr::select(fname, furl, year_data, size_mb, mod_datetime,
                      ind_zip, ind_tiles, swdi_type_tiles, swdi_type,
                      ind_mtda, ind_csv_only,
                      dl_datetime, dl_date, dl_sysname,
                      dat_src_flag, outpath, ind_mtda, outdir)

    return(noaa_all)
}


#' Download data for NOAA SWDI data
#' @param noaa_all (tibble) : Data and Metadata info
#' @param noaa_swdi_ind_tiles (integer) : A value in \code{{0, 1}}, with 1
#' indicating that tiles data is to be downloaded and extracted,
#' and 0 indicating non-tiles data is to be downloaded and extracted
#' @param noaa_swdi_type (character) : Specifying the type of NOAA-SWDI data to
#' download and extract. Can be one of the following values:
#' \code{hail, mda, meso, nldn, plsr, structure, tvs, warn}
#' @param year_periods (numeric) : 1800:1800 (default) array of years to be
#'                                 downloaded
#' @param data_folder (character) : "data" (default) data folder name
#' @param remove (logical) : FALSE (default) remove .zip file after extraction
#'
#' @export
noaa_swdi_data_download <- function(noaa_all,
                                    noaa_swdi_ind_tiles,
                                    noaa_swdi_type,
                                    year_periods = 1995:1995,
                                    data_folder = 'data', remove = FALSE){

    tiles_path <- dplyr::if_else(noaa_swdi_ind_tiles == 1, "tiles", "no_tiles")
    outpath_mtda <- here::here(data_folder, DAT_SRC_FLAG, base::format(dl_date,
                                                                       "%Y%m%d"),
                               tiles_path,
                               noaa_swdi_type,
                               stringr::str_c(DAT_SRC_FLAG,
                                              "metadata.csv",
                                              sep = "_"))
    outpath_mtda_all <- here::here(data_folder, DAT_SRC_FLAG, base::format(dl_date,
                                                                           "%Y%m%d"),
                                   tiles_path,
                                   noaa_swdi_type,
                                   stringr::str_c(DAT_SRC_FLAG,
                                                  "metadata_all.csv",
                                                  sep = "_"))
    outdir_mtda <- outpath_mtda %>% fs::path_dir(path = .)
    outdir_mtda_all <- outpath_mtda_all %>% fs::path_dir(path = .)
    fs::dir_create(path = outdir_mtda)

    # I've got a problem here as apparently R archive does not really
    # work with gzip
    # https://github.com/omnisci/omniscidb/blob/master/Archive/Archive.h LINE 75
    # https://github.com/libarchive/libarchive/issues/586

    noaa_all %T>%
        readr::write_csv(x = ., file = outpath_mtda_all, col_names = TRUE) %>%
        dplyr::filter(year_data %in% year_periods) %>%
        dplyr::filter(ind_tiles == noaa_swdi_ind_tiles,
                      swdi_type == noaa_swdi_type) %>%
        dplyr::select(furl, outpath, ind_zip, outdir) %>%
        dplyr::mutate(extr = base::as.logical(ind_zip),
                      remove = base::as.logical(remove)) %>%
        dplyr::select(-ind_zip) %T>%
        readr::write_csv(x = ., file = outpath_mtda, col_names = TRUE) %>%
        purrr::pwalk(backburner::dl_extract_file)
}

#' Wrapper for NOAA GCHN data extraction pipeline.
#' This function will setup global variables, gather information and urls on
#' data and metadata and download the data.
#' @param dat_flag (character) : "noaa_swdi_daily" (default) name of the data -
#'                               will appear as data folder name when data are
#'                               saved
#' @param noaa_swdi_ind_tiles (integer) : A value in \code{{0, 1}}, with 1
#' indicating that tiles data is to be downloaded and extracted,
#' and 0 indicating non-tiles data is to be downloaded and extracted
#' @param noaa_swdi_type (character) : Specifying the type of NOAA-SWDI data to
#' download and extract. Can be one of the following values:
#' \code{hail, mda, meso, nldn, plsr, structure, tvs, warn}
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata" (default) metadata
#'                                       folder name
#' @param url (character) : NULL (default) data url - the default one is encoded
#'                          in the function body
#' @param year_periods (numeric) : 1800:1800 (default) array of years to be
#'                                 downloaded
#' @param data_folder (character) : "data" (default) data folder name
#' @param remove (logical) : FALSE (default) remove .zip file after extraction
#'
#' @examples
#' \dontrun{
#' noaa_swdi_extract(year_periods = 1995:1996,
#'                   noaa_swdi_ind_tiles = 1,
#'                   noaa_swdi_type = "hail")
#' }
#' @export
noaa_swdi_extract <- function(year_periods = 1995:1995,
                              noaa_swdi_ind_tiles,
                              noaa_swdi_type,
                              data_folder = 'data',
                              dat_flag = 'noaa_swdi', remove = FALSE,
                              data_dl_method = 'libcurl', url = NULL,
                              metadata_dirname = 'metadata'){
    noaa_swdi_setup_variables(dat_flag = dat_flag,
                              data_dl_method = data_dl_method,
                              url = url, metadata_dirname=metadata_dirname)
    noaa_urls <- noaa_swdi_read_data()
    noaa_all <- noaa_swdi_metadata(noaa_urls = noaa_urls,
                                   noaa_swdi_ind_tiles = noaa_swdi_ind_tiles)
    noaa_swdi_data_download(noaa_all = noaa_all,
                            noaa_swdi_ind_tiles = noaa_swdi_ind_tiles,
                            noaa_swdi_type = noaa_swdi_type,
                            year_periods = year_periods,
                            data_folder = data_folder, remove = remove)
}
