#' Setup the global variables for GEOMAC data
#'
#' @param dat_flag (character) : "geomac" (default) name of the data - will
#'                               appear as data folder name when data are saved
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata" (default) metadata
#'                                       folder name
#' @param url (character) : NULL (default) data url - the default one is encoded
#'                          in the function body
#'
#' @export
geomac_setup_variables <- function(dat_flag = 'geomac',
                                   data_dl_method = 'libcurl',
                                   metadata_dirname = 'metadata',
                                   url = NULL){
    backburner::setup_global_variables(dat_flag = dat_flag, data_dl_method = data_dl_method,
                                       metadata_dirname = metadata_dirname)

    if (is.null(url)){
        url <- "https://rmgsc.cr.usgs.gov/outgoing/GeoMAC/historic_fire_data/"
    }
    geomac_fire_url <<- stringr::str_c(url, sep = "")

    # GLOBAL locale
    GLOBAL_LOCALE <<- "en_US.UTF-8"
    GLOBAL_MTDA_DIRNAME <<- "metadata" # Metadata download directory name

    # GLOBAL Regex patterns
    RE_PTRN_GEOMAC_HTML <<- stringr::str_c("[0-9]{1,2}\\",
                                           "/[0-9]{1,2}\\",
                                           "/[0-9]{4}",
                                           "\\s+\\d+\\:\\d+",
                                           "\\s+(A|P)M",
                                           "\\s+\\d+",
                                           sep = "")
    RE_PTRN_GEOMAC_TYPE <<- "(sit_rep_pts|perimeters)" # geomac core data classes
    RE_PTRN_GDB <<- "gdb" # geomac specific geospatial database
    RE_PTRN_ARCHIVE <<- "(zip|gz)$" # generic archive matching pattern
    RE_PTRN_YEAR <<- "^\\d{1,4}"
    RE_PTRN_CSV_ONLY <<- "csv$"

    # Constants
    SIZE_CONV_BYTES_TO_MB <<- 1/1000000
}

#' Extracting information on the metadata for GEOMAC data
#'
#' @export
geomac_read_data <- function(){
    geomac_fire_html <- xml2::read_html(x = geomac_fire_url)

    geomac_urls <- geomac_fire_html %>%
        rvest::html_nodes("a") %>%
        rvest::html_attr("href") %>%
        tibble::enframe(x = .) %>%
        dplyr::slice(.data = ., -1) # Remove reference to generic
    # "[To Parent Directory]" link

    # Now get the important metadata atrributes associated with each file
    geomac_mtda <- geomac_fire_html %>%
        rvest::html_nodes("pre") %>%
        rvest::html_text() %>%
        stringr::str_extract_all(string = .,
                                 # Split-up long regex pattern
                                 pattern = RE_PTRN_GEOMAC_HTML) %>%
        base::unlist() %>%
        base::as.character() %>%
        tibble::enframe(x = ., name = NULL) %>%
        # Split on multiple spaces
        tidyr::separate(data = ., col = "value",
                        into = c("date", "time", "am_pm", "size"),
                        sep = "\\s+") %>%
        tidyr::unite(data = ., col = mod_datetime, date, time, am_pm,
                     sep = " ") %>%
        # Modify to a datetime format using lubridate
        # Setting the locale to "en_US.UTF-8" is important here
        dplyr::mutate(mod_datetime =
                          lubridate::mdy_hm(mod_datetime,
                                            locale = GLOBAL_LOCALE),
                      size_mb =
                          base::as.double(size)*SIZE_CONV_BYTES_TO_MB)

    geomac_urls <- dplyr::bind_cols(geomac_mtda, geomac_urls)
    return(geomac_urls)
}

#' Download full data and metadata information for GEOMAC data
#' @param geomac_urls (tibble) : geomac data information
#'
#' @export
geomac_metadata <- function(geomac_urls){
    geomac_all <-  geomac_urls %>%
        dplyr::mutate(fname = base::basename(path = value),
                      furl = stringr::str_c(geomac_fire_url,
                                            fname, sep = "")) %>%
        dplyr::select(-value) %>%
        # Reorder dataset
        dplyr::select(fname, furl, size_mb, mod_datetime)

    ## Additional Metadata variables --------------------------------------------
    # Add more metadata variables
    geomac_all <- geomac_all %>%
        dplyr::mutate(year_data =
                          base::as.integer(
                              stringr::str_extract(
                                  string = fname,
                                  pattern = "^\\d{1,4}")),
                      ind_zip =
                          ind_re_match(string = fname,
                                       pattern = RE_PTRN_ARCHIVE),
                      ind_gdb =
                          ind_re_match(string = fname,
                                       pattern = RE_PTRN_GDB),
                      dl_datetime = dl_datetime,
                      dl_date = dl_date,
                      dl_sysname = dl_sysname,
                      dat_src_flag = DAT_SRC_FLAG)

    geomac_all <- geomac_all %>%
        dplyr::mutate(outpath =
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
                      outdir = fs::path_ext_remove(path = outpath)) %>%
        dplyr::arrange(year_data)
}

#' Download data for geomac data
#' @param noaa_all (tibble) : Data and Metadata info
#' @param year_periods (numeric) : 1800:1800 (default) array of years to be
#'                                 downloaded
#' @param data_folder (character) : "data" (default) data folder name
#' @param remove (logical) : FALSE (default) remove .zip file after extraction
#'
#' @export
geomac_data_download <- function(geomac_all, year_periods = 2016:2018,
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

    geomac_all %T>%
        readr::write_csv(x = ., file = outpath_mtda_all, col_names = TRUE) %>%
        dplyr::filter(year_data %in% year_periods,
                      !ind_gdb, !ind_mtda) %>%
        dplyr::select(furl, outpath, ind_zip, outdir) %>%
        dplyr::mutate(extr = base::as.logical(ind_zip),
                      remove = base::as.logical(remove)) %>%
        dplyr::select(-ind_zip) %T>%
        readr::write_csv(x = ., file = outpath_mtda, col_names = TRUE) %>%
        purrr::pwalk(dl_extract_file)
}

#' Wrapper for GEOMAC data extraction pipeline.
#' This function will setup global variables, gather information and urls on
#' data and metadata and download the data.
#' @param dat_flag (character) : "geomac" (default) name of the data -
#'                               will appear as data folder name when data are
#'                               saved
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
#' @export
geomac_extract <- function(year_periods = 2016:2018, data_folder = 'data',
                           dat_flag = 'geomac', data_dl_method = 'libcurl',
                           url = NULL, remove = FALSE,
                           metadata_dirname = 'metadata'){
    geomac_setup_variables(dat_flag = dat_flag,
                           data_dl_method = data_dl_method,
                           url = url, metadata_dirname = metadata_dirname)
    geomac_urls <- geomac_read_data()
    geomac_all <- geomac_metadata(geomac_urls = geomac_urls)
    geomac_data_download(geomac_all = geomac_all, year_periods = year_periods,
                         data_folder = data_folder, remove = remove)
}
