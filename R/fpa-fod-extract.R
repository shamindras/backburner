#' Setup the global variables for FPA data
#'
#' @param dat_flag (character) : "fpa_fod" (default) name of the data - will
#'                               appear as data folder name when data are saved
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata" (default) metadata
#'                                       folder name
#' @param url (character) : NULL (default) data url - the default one is encoded
#'                          in the function body
#'
#' @export
fpa_setup_variables <- function(dat_flag = 'fpa_fod',
                                data_dl_method = 'libcurl',
                                metadata_dirname = 'metadata',
                                url = NULL){
    backburner::setup_global_variables(dat_flag = dat_flag, data_dl_method = data_dl_method,
                                       metadata_dirname = metadata_dirname)
    RE_PTRN_GDB <<- "gdb"

    if (is.null(url)){
        url <- "https://www.fs.usda.gov/rds/archive/Product/RDS-2013-0009.4/"
    }
    fpa_fod_fire_url <<- stringr::str_c(url, sep = "")
}

#' Extracting information on the download URLs for FPA data
#'
#' @export
fpa_read_data <- function(){
    fpa_fod_fire_html <- xml2::read_html(x = fpa_fod_fire_url)

    mod_datetime <- fpa_fod_fire_html %>%
        rvest::html_nodes("div") %>%
        rvest::html_nodes("dl") %>%
        rvest::html_nodes("dd") %>%
        rvest::html_text() %>%
        .[1] %>% # The first sentence should contain the date info
        stringr::str_extract(string = ., pattern = "20\\d{6}") %>%
        lubridate::as_datetime(x = .)

    # Extract sizes of files by parsing individually
    # We need to get the zip filenames separately, since they are in a
    # "hidden id" field in the html
    fpa_fod_fire_sz_fnames <- fpa_fod_fire_html %>%
                                rvest::html_nodes("em") %>%
                                rvest::html_nodes("div") %>%
                                rvest::html_attr("id") %>%
                                tibble::enframe(x = ., value = "fname_src")

    fpa_fod_fire_sz <- fpa_fod_fire_html %>%
        rvest::html_nodes("em") %>%
        rvest::html_text() %>%
        tibble::enframe(x = ., value = "size_text") %>%
        dplyr::slice(.data = ., -1) %>%
        dplyr::select(size_text) %>%
        # Bind the filenames separately
        dplyr::bind_cols(fpa_fod_fire_sz_fnames) %>%
        dplyr::mutate(.data = .,
                      # The size for these files is in MB
                      size_mb =
                          stringr::str_extract(string =
                                                   size_text,
                                               pattern = "\\d+") %>%
                          base::as.double(x = .),
                      ind_zip =
                          base::as.integer(
                              stringr::str_detect(
                                  string = fname_src, # Check zip name from source
                                  pattern = stringr::fixed(
                                      pattern = "zip",
                                      ignore_case = TRUE))))


    fpa_fod_urls <- fpa_fod_fire_html %>%
        rvest::html_nodes("a") %>%
        rvest::html_attr("href") %>%
        tibble::enframe(x = .) %>%
        dplyr::select(-name) %>%
        dplyr::filter(.data = .,
                      stringr::str_detect(string = value,
                                          pattern = "RDS-2013-0009.4[:graph:]+\\.zip")) %>%
        dplyr::bind_cols(fpa_fod_fire_sz) %>%
        dplyr::rename(furl = value) %>%
        dplyr::mutate(furl = stringr::str_c("https://www.fs.usda.gov", furl),
                      fname = stringr::str_replace(string = fname_src,
                                                   pattern = "checksum_",
                                                   replacement = ""),
                      # fname = base::dirname(furl) %>%
                      #     base::basename(path = .) %>%
                      #     stringr::str_c(.,
                      #                    dplyr::if_else(
                      #                        base::as.logical(ind_zip),
                      #                        ".zip",
                      #                        ".html"),
                      #                    sep = ""),
                      ind_gdb =
                          ind_re_match(string = fname,
                                       pattern = RE_PTRN_GDB),
                      # In this case metadata column is the same
                      # as zip indicator. Duplicated to have consistent
                      # metadata column names
                      ind_mtda = as.integer(!ind_zip),
                      mod_datetime = mod_datetime,
                      dl_datetime = dl_datetime,
                      dl_date = dl_date,
                      dl_sysname = dl_sysname,
                      dat_src_flag = DAT_SRC_FLAG)

    return(fpa_fod_urls)
}

#' Download full data and metadata information for FPA data
#' @param fpa_fod_urls (tibble) : FPA URLs tibble
#'
#' @export
fpa_metadata <- function(fpa_fod_urls){
    fpa_fod_all <-
        fpa_fod_urls %>%
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
        dplyr::select(fname, furl, size_mb, mod_datetime,
                      ind_zip, ind_gdb,
                      dl_datetime, dl_date, dl_sysname,
                      dat_src_flag, outpath, ind_mtda, outdir)
    return(fpa_fod_all)
}

#' Download data for FPA data
#' @param noaa_all (tibble) : Data and Metadata info
#' @param data_folder (character) : "data" (default) data folder name
#' @param remove (logical) : FALSE (default) remove .zip file after extraction
#'
#' @export
fpa_data_download <- function(fpa_fod_all, data_folder = 'data',
                              remove=FALSE){
    outpath_mtda <- here::here(data_folder, DAT_SRC_FLAG, base::format(dl_date,
                                                                       "%Y%m%d"),
                               stringr::str_c(DAT_SRC_FLAG,"metadata.csv",
                                              sep = "_"))
    outpath_mtda_all <- here::here(data_folder, DAT_SRC_FLAG, base::format(dl_date,
                                                                           "%Y%m%d"),
                                   stringr::str_c(DAT_SRC_FLAG,"metadata_all.csv",
                                                  sep = "_"))
    outdir_mtda <- outpath_mtda %>% fs::path_dir(path = .)
    outdir_mtda_all <- outpath_mtda_all %>% fs::path_dir(path = .)
    fs::dir_create(path = outdir_mtda)

    fpa_fod_all %T>%
        readr::write_csv(x = ., path = outpath_mtda_all, col_names = TRUE) %>%
        dplyr::select(furl, outpath, ind_zip, outdir) %>%
        dplyr::mutate(extr = base::as.logical(ind_zip),
                      remove = base::as.logical(remove)) %>%
        dplyr::select(-ind_zip) %T>%
        readr::write_csv(x = ., path = outpath_mtda, col_names = TRUE) %>%
        purrr::pwalk(dl_extract_file)
}

#' Wrapper for FPA data extraction pipeline.
#' This function will setup global variables, gather information and urls on
#' data and metadata and download the data.
#' @param dat_flag (character) : "fpa_fod" (default) name of the data -
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
fpa_extract <- function(data_folder = 'data', dat_flag = 'fpa_fod',
                        data_dl_method = 'libcurl', remove = FALSE, url = NULL,
                        metadata_dirname = 'metadata'){
    fpa_setup_variables(dat_flag = dat_flag,
                        data_dl_method = data_dl_method,
                        url = url, metadata_dirname = metadata_dirname)
    fpa_fod_urls <- fpa_read_data()
    fpa_fod_all <- fpa_metadata(fpa_fod_urls = fpa_fod_urls)
    fpa_data_download(fpa_fod_all = fpa_fod_all, data_folder = data_folder,
                      remove = remove)
}
