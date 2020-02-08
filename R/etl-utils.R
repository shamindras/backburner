#' Indicator function to return a \code{{0, 1}} integer value if there is a
#' valid regex match for the given input string
#'
#' @param string (character) : Input string to match regex against
#' (must be a single element vector)
#' @param pattern (character) : regex string pattern
#' @export
ind_re_match <- function(string, pattern){
    ind_match = base::as.integer(
        stringr::str_detect(string = string,
                            pattern = pattern))
    base::return(ind_match)
}

#' Download file from specified url into specified path. If the directory
#' of the destination path is not specified then the function will create the
#' directory. Also gives the user the option to extract the contents of the
#' downloaded file
#'
#' @param furl (character) : URL for the required file download
#' @param outpath (character) : Full destination path for the specified
#'                              file download
#' @param extr (logical) : TRUE (default) to extract contents of download else
#'                         specify FALSE
#' @param remove (logical) : FALSE (default) to remove the .zip file once it is
#'                           extracted.
#'
#' @export
dl_extract_file <- function(furl, outpath, outdir, extr = TRUE, remove = FALSE){
    # Archive regex pattern
    #re_ptrn_arch <- "(zip|gz)$"
    re_ptrn_arch <- "(zip)$"
    re_ptrn_arch_gz <- "(gz)$"

    # Check if outpath is archive
    is_arch <- outpath %>%
        fs::path_ext(path = .) %>%
        stringr::str_detect(string = ., pattern = re_ptrn_arch)
    is_gz <- outpath %>%
        fs::path_ext(path = .) %>%
        stringr::str_detect(string = ., pattern = re_ptrn_arch_gz)

    #outdir <- outpath %>% fs::path_dir(path = .)
    # is_dir_exists <- outdir %>%  dir_exists(path = .) %>% base::unname()

    print(c(outpath, outdir))

    # Create directory - will silently ignore if the directory already exists
    fs::dir_create(path = outdir)
    download.file(url = furl, destfile = outpath, method = DAT_DL_METHOD)
    # Extract file if user allows
    if(extr && is_arch){
        archive::archive_extract(archive = outpath, dir = outdir)
        if (remove){
            file.remove(outpath)
        }
    } else if (extr && is_gz) {
        outpath_gz <- gsub(".gz", "", outpath)
        R.utils::gunzip(filename = outpath, destname = outpath_gz,
                        remove = remove)
    }
}

#' Setup the global variables for the various extract functions
#'
#' @param dat_flag (character) : name of the data - will appear as data folder
#'                               name when data are saved
#' @param data_dl_method (character) : "libcurl" (default) data download method
#' @param metadata_dirname (character) : "metadata_dirname" (default) metadata
#'                                       folder name
#'
#' @export
setup_global_variables <- function(dat_flag, data_dl_method = 'libcurl',
                                   metadata_dirname = 'metadata'){
    # Data source flag - this will be used to flag our metadata file
    DAT_SRC_FLAG <<- dat_flag
    GLOBAL_MTDA_DIRNAME <<- 'metadata'

    # Download method to use
    DAT_DL_METHOD <<- data_dl_method

    #Other general global variables
    dl_datetime <<- base::Sys.time()
    dl_date <<- base::as.Date(dl_datetime)
    dl_user <<- base::Sys.info()["user"]
    dl_sysname <<- base::Sys.info()["sysname"]
}


#' Get data dictionary names from single Excel sheet
#'
#' @param ddict_xlsx_path : .xlsx file path
#' @param sheet_nm : sheet number
#' @param cols_rng : column range in the sheet to be selected
#'
#' @return tibble with data dictionary names
#' @export
get_ddict <- function(ddict_xlsx_path, sheet_nm, cols_rng){
    # Read in specified sheet and range
    ddict <- readxl::read_excel(path = ddict_xlsx_path,
                                sheet = sheet_nm,
                                range = readxl::cell_cols(cols_rng))
    # Remove complete NA rows
    ddict <- ddict %>%
        dplyr::filter_all(any_vars(!is.na(.))) %>%
        dplyr::select(orig_varname, new_varname)
}