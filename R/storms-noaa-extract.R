#' Setup the global variables for NOOA STORMEVENTS CSV data
#'
#' @param dat_flag (character) : "noaa_stormevents" (default) name of the data -
#'                               will appear as data folder name when data are
#'                               saved
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata" (default) metadata
#'                                       folder name
#' @param url (character) : NULL (default) data url - the default one is encoded
#'                          in the function body
#'
#' @export
noaa_stormevents_setup_variables <- function(dat_flag = 'noaa_stormevents',
                                             data_dl_method = 'libcurl',
                                             metadata_dirname = 'metadata',
                                             url = NULL){
    backburner::setup_global_variables(dat_flag = dat_flag, data_dl_method = data_dl_method,
                                       metadata_dirname = metadata_dirname)

    RE_PTRN_ARCHIVE <<- "(zip|gz)$" # generic archive matching pattern
    RE_PTRN_YEAR <<- "^\\d{1,4}"
    RE_PTRN_CSV_ONLY <<- "csv$"

    # Storm event specific regex patterns
    RE_PTRN_YEAR_STORMS <<- "\\d{4}"
    RE_PTRN_YMD_STORMS <<- "\\d{8}"
    RE_PTRN_STORMS_TYPE <<- "(details|fatalities|locations)"

    if (is.null(url)){
        url <- "ftp://ftp.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
    }
    noaa_url <<- stringr::str_c(url, sep = "")
}


#' Extracting information on the download URLs for NOOA STORMEVENTS CSV data
#'
#' @export
noaa_stormevents_read_data <- function(){

    # Getting URL Tibble
    hand <- curl::new_handle()
    gg <- curl::curl_fetch_memory(url = noaa_url, handle = hand)
    gg <- rawToChar(gg$content)

    noaa_urls <- readr::read_delim(file = gg, delim = "\\s+", col_names = FALSE, skip = 4,
                                   skip_empty_rows = TRUE) %>%
        tibble::as_tibble() %>%
        tidyr::separate(data = ., col = X1,
                        into = c("rw", "group", "ftp", "code", "size",
                                 "month", "dom", "year", "fname"),
                        sep = "\\s+") %>%
        dplyr::mutate(mod_datetime = base::strftime(dl_datetime, format="%H:%M:%S"),
                      # NOTE: If we can't parse the year correctly for a particular
                      # file we just assign it the download year. Not ideal, but
                      # better than parsing incorrectly
                      year = ifelse(stringr::str_detect(string = year,
                                                        pattern = "\\:"),
                                    format(dl_date, "%Y"), year)) %>%
        tidyr::unite(data = ., col = mod_datetime, month, dom, year,
                     mod_datetime, sep = " ", remove = FALSE) %>%
        # Modify to a datetime format using lubridate
        # Setting the locale to "en_US.UTF-8" is important here
        dplyr::mutate(year_data =
                          base::as.integer(
                              stringr::str_extract(
                                  string = fname,
                                  pattern = RE_PTRN_YEAR_STORMS)),
                      date_data =
                          lubridate::ymd(stringr::str_extract(
                              string = fname,
                              pattern = RE_PTRN_YMD_STORMS)),
                      storm_type = stringr::str_extract(string = fname,
                                                        pattern = RE_PTRN_STORMS_TYPE),
                      mod_datetime =
                          lubridate::mdy_hms(mod_datetime,
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
        dplyr::select(-c("rw", "group", "ftp")) %>%
        dplyr::arrange(.data = ., year_data, storm_type)

    return(noaa_urls)
}


#' Download full data and metadata information for NOOA STORMEVENTS CSV data
#' @param noaa_urls (tibble) : NOOA STORMEVENTS CSV URLs tibble
#'
#' @export
noaa_stormevents_metadata<- function(noaa_urls){
    noaa_all <- noaa_urls %>%
        dplyr::mutate(furl = stringr::str_c(noaa_url,
                                            fname, sep = ""),
                      outpath =
                          dplyr::if_else(!base::is.na(year_data),
                                         here::here("data",
                                                    DAT_SRC_FLAG,
                                                    base::format(dl_date,
                                                                 "%Y%m%d"),
                                                    year_data,
                                                    fname),
                                         here::here("data",
                                                    DAT_SRC_FLAG,
                                                    base::format(dl_date,
                                                                 "%Y%m%d"),
                                                    GLOBAL_MTDA_DIRNAME,
                                                    fname)),
                      ind_mtda = as.integer(base::is.na(year_data)),
                      outdir = fs::path_dir(path = outpath)) %>%
        # Reorder dataset
        dplyr::select(fname, furl, year_data, size_mb, mod_datetime,
                      ind_zip, ind_mtda, ind_csv_only,
                      dl_datetime, dl_date, dl_sysname,
                      dat_src_flag, outpath, ind_mtda, outdir)

    return(noaa_all)
}


#' Download data for NOOA STORMEVENTS CSV data
#' @param noaa_all (tibble) : Data and Metadata info
#' @param year_periods (numeric) : 1800:1800 (default) array of years to be
#'                                 downloaded
#' @param data_folder (character) : "data" (default) data folder name
#' @param remove (logical) : FALSE (default) remove .zip file after extraction
#'
#' @export
noaa_stormevents_data_download <- function(noaa_all, year_periods = 1800:1800,
                                           data_folder = 'data', remove = FALSE){

    outpath_mtda <- here::here(data_folder, DAT_SRC_FLAG, base::format(dl_date,
                                                                       "%Y%m%d"),
                               stringr::str_c(DAT_SRC_FLAG,
                                              "metadata.csv",
                                              sep = "_"))
    outpath_mtda_all <- here::here(data_folder, DAT_SRC_FLAG, base::format(dl_date,
                                                                           "%Y%m%d"),
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
        dplyr::select(furl, outpath, ind_zip, outdir) %>%
        dplyr::mutate(extr = base::as.logical(ind_zip),
                      remove = base::as.logical(remove)) %>%
        dplyr::select(-ind_zip) %T>%
        readr::write_csv(x = ., file = outpath_mtda, col_names = TRUE) %>%
        purrr::pwalk(dl_extract_file)
}

#' Wrapper for NOOA STORMEVENTS CSV data extraction pipeline.
#' This function will setup global variables, gather information and urls on
#' data and metadata and download the data.
#' @param dat_flag (character) : "noaa_stormevents" (default) name of the data -
#'                               will appear as data folder name when data are
#'                               saved
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata" (default) metadata
#'                                       folder name
#' @param url (character) : NULL (default) data url - the default one is encoded
#'                          in the function body
#' @param year_periods (numeric) : 1950:1950 (default) array of years to be
#'                                 downloaded
#' @param data_folder (character) : "data" (default) data folder name
#' @param remove (logical) : FALSE (default) remove .zip file after extraction
#'
#' @examples
#' \dontrun{
#' noaa_stormevents_extract(year_periods = 1800:1801)
#' }
#' @export
noaa_stormevents_extract <- function(year_periods = 1950:1950, data_folder = 'data',
                                     dat_flag = 'noaa_stormevents', remove = FALSE,
                                     data_dl_method = 'libcurl', url = NULL,
                                     metadata_dirname = 'metadata'){
    noaa_stormevents_setup_variables(dat_flag = dat_flag,
                                     data_dl_method = data_dl_method,
                                     url = url, metadata_dirname=metadata_dirname)
    noaa_urls <- noaa_stormevents_read_data()
    noaa_all <- noaa_stormevents_metadata(noaa_urls = noaa_urls)
    noaa_stormevents_data_download(noaa_all = noaa_all, year_periods = year_periods,
                                   data_folder = data_folder, remove = remove)
}

