#' Generate single feature GHCN-D lagged aggregated time summaries mapped to
#' individual MTBS fires by specified spatial degrees over a 180 day lag period
#' for each fire
#'
#' @param fire_base_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_fire_base_days (integer) : The number of days for GHCN-D weather
#' data to lag behind the \code{fire_base_start_date}
#' @param spat_deg_rad (double) : The spatial degree radius to consider around
#' each fire when joining weather data (default value is 0.5)
#' @param summary_type (character) : The \code{SQL} summary function to apply
#' to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")
#' @param sfeat (character) : The GHCN-D feature to summarize for each given
#' fire. Aggregated and lagged versions of each feature are produced
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#'
#' @return (tibble) : A \code{tibble} with the query string, file path to store
#' the query, and a check of whether the filepath exists
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' gen_query_fire_base_ghcnd_sfeat_180d(fire_base_start_date = base::as.Date("2000-01-01"),
#'                                 max_lag_fire_base_days = 180,
#'                                 spat_deg_rad = 0.5,
#'                                 summary_type = "AVG",
#'                                 sfeat = "TOBS",
#'                                 out_qry_dir = ".",
#'                                 out_tbl_name_pfx = "mtbs",
#'                                 fire_base_tbl_name = "mtbs_base_01")
#' }
gen_query_fire_base_ghcnd_sfeat_180d <- function(fire_base_start_date = base::as.Date("2000-01-01"),
                                            max_lag_fire_base_days = 180,
                                            spat_deg_rad = 0.5,
                                            summary_type = "AVG",
                                            sfeat,
                                            out_qry_dir,
                                            out_tbl_name_pfx,
                                            fire_base_tbl_name){

    # Key transformations for building query string ----------------------------
    # Feature type
    sfeat_lowcase <- stringr::str_to_lower(string = sfeat)

    # Spatial Degrees
    spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
    spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                                  pattern = "\\.",
                                                  replacement = "")

    # SQL Summar Type e.g. AVG
    summary_type_lowcase <- stringr::str_to_lower(string = summary_type)

    # Our SQL table name i.e. mtbs_ghcnd_tobs_s05_t180
    # for TOBS feature, 0.5 spatial degrees, 180 days
    out_tbl_name <- glue::glue("{out_tbl_name_pfx}",
                               "ghcnd",
                               "{sfeat_lowcase}",
                               "{summary_type_lowcase}",
                               "s{spat_deg_rad_str_nums}",
                               "t{max_lag_fire_base_days}",
                               .sep = "_")

    # Query name should be separated by "-" instead of underscores
    out_qry_name <- stringr::str_replace_all(string = out_tbl_name,
                                             pattern = "_",
                                             replacement = "-")

    # Input validation ---------------------------------------------------------
    assertthat::assert_that(stringr::str_length(string = spat_deg_rad_str) == 3,
                            msg = glue::glue("spat_deg_rad must be of char length 3 e.g. 0.5 or 1.0,
                                         it is currently {spat_deg_rad}"))
    assertthat::assert_that(stringr::str_detect(string = spat_deg_rad_str,
                                                pattern = "\\d\\.\\d"),
                            msg = glue::glue("spat_deg_rad  must be 1 dec. pl. e.g. 0.5 or 1.0,
                                         it is currently {spat_deg_rad}"))
    assertthat::assert_that(max_lag_fire_base_days >= 180,
                            msg = glue::glue("max_lag_fire_base_days must be >= 180,
                                         it is currently {max_lag_fire_base_days}"))
    assertthat::assert_that(fire_base_start_date <= base::as.Date("2000-01-01"),
                            msg = glue::glue("fire_base_start_date must be before {base::as.Date('2000-01-01')},
                                         it is currently {fire_base_start_date}"))
    assertthat::assert_that(summary_type == stringr::str_to_upper(string = summary_type),
                            msg = glue::glue("summary_type must be in upper case,
                                         it is currently {summary_type}"))
    assertthat::assert_that(sfeat == stringr::str_to_upper(string = sfeat),
                            msg = glue::glue("sfeat must be in upper case,
                                         it is currently {sfeat}"))
    assertthat::assert_that(base::is.character(x = out_qry_dir) &&
                                fs::dir_exists(path = out_qry_dir),
                            msg = glue::glue("out_qry_dir must be a valid path,
                                         it is currently {out_qry_dir}"))

    out_qry_path <- fs::path_join(parts = c(out_qry_dir,
                                            glue::glue("{out_qry_name}.sql")))

    out_qry_str <-
        glue::glue(
            "/* Start query to create {out_tbl_name} ---------------------------------*/
DROP TABLE IF EXISTS {out_tbl_name};

CREATE TABLE {out_tbl_name} AS
(SELECT fbase.fire_id,
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN fbase.fire_start_date::date - integer '3'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't3d', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN fbase.fire_start_date::date - integer '7'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't1w', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN fbase.fire_start_date::date - integer '14'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't2w', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN fbase.fire_start_date::date - integer '21'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't3w', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN fbase.fire_start_date::date - integer '30'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't1m', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN fbase.fire_start_date::date - integer '60'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't2m', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN fbase.fire_start_date::date - integer '90'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't3m', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN fbase.fire_start_date::date - integer '180'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't6m', .sep = '_'))}
FROM {fire_base_tbl_name} AS fbase
LEFT JOIN ghcnd_observations_{sfeat_lowcase} AS ghobs
    ON (ghobs.record_dt BETWEEN (fbase.fire_start_date::date - integer {glue::single_quote(max_lag_fire_base_days)})
                           AND (fbase.fire_start_date::date - integer '1')
        AND ST_DWithin(fbase.fire_centroid, ghobs.location, {spat_deg_rad}))
GROUP BY fbase.fire_id);

/* Index this table on fire_id for the final LEFT JOIN of all features*/
CREATE INDEX {out_tbl_name}_fire_id ON {out_tbl_name} USING HASH (fire_id);

/* End query to create {out_tbl_name} ----------------------------------*/")

    out_val <- tibble::tibble(qry_str = out_qry_str, qry_path = out_qry_path)

    base::return(out_val)
}

#' Wrapper function to generate a single combined or separate \code{SQL} files
#' for individual feature GHCN-D lagged aggregated time summaries mapped to
#' individual MTBS fires by specified spatial degrees over a 180 day lag period
#' for each fire
#'
#' @param fire_base_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_fire_base_days (integer) : The number of days for GHCN-D weather
#' data to lag behind the \code{fire_base_start_date}
#' @param spat_deg_rad (double) : The spatial degree radius to consider around
#' each fire when joining weather data (default value is 0.5)
#' @param summary_type (character) : The \code{SQL} summary function to apply
#' to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")
#' @param sfeat_types (character) : The GHCN-D features to summarize for each given
#' fire. Aggregated and lagged versions of each feature are produced e.g.
#' \code{c("TOBS", "TMIN")} for generating the code for 2 features
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#' @param ind_comb_qry (logical) : If \code{TRUE} then produces a single
#' combined \code{sql} query for all features combined. If \code{FALSE}
#' then produces a separate \code{sql} queries for an individual GHCN-D feature
#'
#' @return (tibble) : A single or separate \code{SQL} files for single
#' GHCN features joined to MTBS fire_ids by specified spatial and temporal
#' parameters
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' wrap_gen_query_fire_base_ghcnd_sfeat_180d(fire_base_start_date =
#'                                          base::as.Date("2000-01-01"),
#'                                      max_lag_fire_base_days = 180,
#'                                      spat_deg_rad = 0.5,
#'                                      summary_type = "AVG",
#'                                      sfeat_types = c("TOBS", "TMIN", "TMAX"),
#'                                      out_qry_dir = ".",
#'                                      out_tbl_name_pfx = "mtbs",
#'                                      fire_base_tbl_name = "mtbs_base_01",
#'                                      ind_comb_qry = TRUE)
#' }
wrap_gen_query_fire_base_ghcnd_sfeat_180d <- function(fire_base_start_date =
                                                     base::as.Date("2000-01-01"),
                                                 max_lag_fire_base_days = 180,
                                                 spat_deg_rad = 0.5,
                                                 summary_type = "AVG",
                                                 sfeat_types,
                                                 out_qry_dir,
                                                 out_tbl_name_pfx,
                                                 fire_base_tbl_name,
                                                 ind_comb_qry = TRUE){

    # Key transformations for building query string ----------------------------
    # Feature type
    sfeat_lowcase <- stringr::str_to_lower(string = sfeat_types)

    # Spatial Degrees
    spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
    spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                                  pattern = "\\.",
                                                  replacement = "")

    # SQL Summar Type e.g. AVG
    summary_type_lowcase <- stringr::str_to_lower(string = summary_type)

    if(ind_comb_qry){
        # Write combined query as a single file ------------------------------------

        # Our SQL table name i.e. mtbs_ghcnd_tobs_s05_t180
        # for TOBS feature, 0.5 spatial degrees, 180 days
        out_gen_qry_name <- stringr::str_c(glue::glue("{out_tbl_name_pfx}",
                                                      "ghcnd",
                                                      "{summary_type_lowcase}",
                                                      "s{spat_deg_rad_str_nums}",
                                                      "t{max_lag_fire_base_days}",
                                                      .sep = "-"),
                                           ".sql")

        # Query name should be separated by "-" instead of underscores
        out_gen_qry_name <- stringr::str_replace_all(string = out_gen_qry_name,
                                                 pattern = "_",
                                                 replacement = "-")

        out_gen_qry_path <- fs::path_join(parts = c(out_qry_dir, out_gen_qry_name))
        out_gen_qry_path

        sfeat_types %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_fire_base_ghcnd_sfeat_180d(fire_base_start_date =
                                                                                 fire_base_start_date,
                                                                             max_lag_fire_base_days = max_lag_fire_base_days,
                                                                             spat_deg_rad = spat_deg_rad,
                                                                             summary_type = summary_type,
                                                                             sfeat = .x,
                                                                             out_qry_dir = out_qry_dir,
                                                                             out_tbl_name_pfx = out_tbl_name_pfx,
                                                                             fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::pull(.data = ., var = qry_str) %>%
            glue::glue_collapse(x = ., sep = "\n\n") %>%
            readr::write_lines(x = ., path = out_gen_qry_path)
    } else{
        # Write queries for each feature to individual files -----------------------
        sfeat_types %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_fire_base_ghcnd_sfeat_180d(fire_base_start_date =
                                                                                 fire_base_start_date,
                                                                             max_lag_fire_base_days = max_lag_fire_base_days,
                                                                             spat_deg_rad = spat_deg_rad,
                                                                             summary_type = summary_type,
                                                                             sfeat = .x,
                                                                             out_qry_dir = out_qry_dir,
                                                                             out_tbl_name_pfx = out_tbl_name_pfx,
                                                                             fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::rename(x = qry_str, path = qry_path) %>%
            dplyr::rowwise(data = .) %>%
            purrr::pwalk(.l = ., .f = ~readr::write_lines(x = .x, path = .y))
    }
}


#' Generate single feature NOAA SWDI lagged aggregated time summaries mapped to
#' individual MTBS fires by specified spatial degrees over a 180 day lag period
#' for each fire
#'
#' @param fire_base_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_fire_base_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{fire_base_start_date}
#' @param spat_deg_rad (double) : The spatial degree radius to consider around
#' each fire when joining weather data (default value is 0.5)
#' @param summary_type (character) : The \code{SQL} summary function to apply
#' to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")
#' @param sfeat (character) : The NOAA SWDI feature to summarize for each given
#' fire. Aggregated and lagged versions of each feature are produced e.g.
#' \code{"nldn", "structure", "hail", "tvs"}
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#'
#' @return (tibble) : A \code{tibble} with the query string, file path to store
#' the query, and a check of whether the filepath exists
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' gen_query_fire_base_swdi_sfeat_180d(fire_base_start_date = base::as.Date("2000-01-01"),
#'                                max_lag_fire_base_days = 180,
#'                                spat_deg_rad = 0.5,
#'                                summary_type = "AVG",
#'                                sfeat = "nldn",
#'                                out_qry_dir = ".",
#'                                out_tbl_name_pfx = "mtbs",
#'                                fire_base_tbl_name = "mtbs_base_01")
#' }
gen_query_fire_base_swdi_sfeat_180d <- function(fire_base_start_date = base::as.Date("2000-01-01"),
                                           max_lag_fire_base_days = 180,
                                           spat_deg_rad = 0.5,
                                           summary_type = "AVG",
                                           sfeat,
                                           out_qry_dir,
                                           out_tbl_name_pfx,
                                           fire_base_tbl_name){

    # Key transformations for building query string ----------------------------
    # Feature type
    sfeat_lowcase <- stringr::str_to_lower(string = sfeat)

    # Spatial Degrees
    spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
    spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                                  pattern = "\\.",
                                                  replacement = "")

    # SQL Summar Type e.g. AVG
    summary_type_lowcase <- stringr::str_to_lower(string = summary_type)

    # Our SQL table name i.e. mtbs_ghcnd_tobs_s05_t180
    # for TOBS feature, 0.5 spatial degrees, 180 days
    out_tbl_name <- glue::glue("{out_tbl_name_pfx}",
                               "{sfeat_lowcase}",
                               "{summary_type_lowcase}",
                               "s{spat_deg_rad_str_nums}",
                               "t{max_lag_fire_base_days}",
                               .sep = "_")

    # Query name should be separated by "-" instead of underscores
    out_qry_name <- stringr::str_replace_all(string = out_tbl_name,
                                             pattern = "_",
                                             replacement = "-")

    # Input validation ---------------------------------------------------------
    assertthat::assert_that(stringr::str_length(string = spat_deg_rad_str) == 3,
                            msg = glue::glue("spat_deg_rad must be of char length 3 e.g. 0.5 or 1.0,
                                         it is currently {spat_deg_rad}"))
    assertthat::assert_that(stringr::str_detect(string = spat_deg_rad_str,
                                                pattern = "\\d\\.\\d"),
                            msg = glue::glue("spat_deg_rad  must be 1 dec. pl. e.g. 0.5 or 1.0,
                                         it is currently {spat_deg_rad}"))
    assertthat::assert_that(max_lag_fire_base_days >= 180,
                            msg = glue::glue("max_lag_fire_base_days must be >= 180,
                                         it is currently {max_lag_fire_base_days}"))
    assertthat::assert_that(fire_base_start_date <= base::as.Date("2000-01-01"),
                            msg = glue::glue("fire_base_start_date must be before {base::as.Date('2000-01-01')},
                                         it is currently {fire_base_start_date}"))
    assertthat::assert_that(summary_type == stringr::str_to_upper(string = summary_type),
                            msg = glue::glue("summary_type must be in upper case,
                                         it is currently {summary_type}"))
    assertthat::assert_that(base::is.character(x = out_qry_dir) &&
                                fs::dir_exists(path = out_qry_dir),
                            msg = glue::glue("out_qry_dir must be a valid path,
                                         it is currently {out_qry_dir}"))

    out_qry_path <- fs::path_join(parts = c(out_qry_dir,
                                            glue::glue("{out_qry_name}.sql")))

    out_qry_str <-
        glue::glue(
            "/* Start query to create {out_tbl_name} ---------------------------------*/
DROP TABLE IF EXISTS {out_tbl_name};

CREATE TABLE {out_tbl_name} AS
(SELECT fbase.fire_id,
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN fbase.fire_start_date::date - integer '3'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't3d', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN fbase.fire_start_date::date - integer '7'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't1w', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN fbase.fire_start_date::date - integer '14'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't2w', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN fbase.fire_start_date::date - integer '21'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't3w', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN fbase.fire_start_date::date - integer '30'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't1m', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN fbase.fire_start_date::date - integer '60'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't2m', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN fbase.fire_start_date::date - integer '90'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't3m', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN fbase.fire_start_date::date - integer '180'
                                                      AND fbase.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't6m', .sep = '_'))}
FROM {fire_base_tbl_name} AS fbase
LEFT JOIN noaa_swdi_{sfeat} AS {sfeat}
    ON ({sfeat}.record_dt BETWEEN (fbase.fire_start_date::date - integer {glue::single_quote(max_lag_fire_base_days)})
                           AND (fbase.fire_start_date::date - integer '1')
        AND ST_DWithin(fbase.fire_centroid, {sfeat}.geometry, {spat_deg_rad}))
WHERE fbase.fire_start_date::date >= {glue::single_quote(
      format(fire_base_start_date, '%d-%m-%Y'))}::date
GROUP BY fbase.fire_id);

/* Index this table on fire_id for the final LEFT JOIN of all features*/
CREATE INDEX {out_tbl_name}_fire_id ON {out_tbl_name} USING HASH (fire_id);

/* End query to create {out_tbl_name} ----------------------------------*/")

    out_val <- tibble::tibble(qry_str = out_qry_str, qry_path = out_qry_path)

    base::return(out_val)
}


#' Wrapper function to generate a single combined or separate \code{SQL} files
#' for individual feature NOAA SWDI lagged aggregated time summaries mapped to
#' individual MTBS fires by specified spatial degrees over a 180 day lag period
#' for each fire
#'
#' @param fire_base_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_fire_base_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{fire_base_start_date}
#' @param spat_deg_rad (double) : The spatial degree radius to consider around
#' each fire when joining weather data (default value is 0.5)
#' @param summary_type (character) : The \code{SQL} summary function to apply
#' to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")
#' @param sfeat_types (character) : The NOAA SWDI features to summarize for each
#' given fire. Aggregated and lagged versions of each feature are produced e.g.
#' \code{c("nldn", "structure", "hail", "tvs")} for generating the code for 2
#' features
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#' @param ind_comb_qry (logical) : If \code{TRUE} then produces a single
#' combined \code{sql} query for all features combined. If \code{FALSE}
#' then produces a separate \code{sql} queries for individual GHCN-D feature
#'
#' @return (tibble) : A single or separate \code{SQL} files for single
#' GHCN features joined to MTBS fire_ids by specified spatial and temporal
#' parameters
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' wrap_gen_query_fire_base_swdi_sfeat_180d(fire_base_start_date =
#'                                       base::as.Date("2000-01-01"),
#'                                     max_lag_fire_base_days = 180,
#'                                     spat_deg_rad = 0.5,
#'                                     summary_type = "AVG",
#'                                     sfeat_types = c("nldn", "hail", "tvs", "structure"),
#'                                     out_qry_dir = ".",
#'                                     out_tbl_name_pfx = "mtbs",
#'                                     fire_base_tbl_name = "mtbs_base_01",
#'                                     ind_comb_qry = TRUE)
#' }
wrap_gen_query_fire_base_swdi_sfeat_180d <- function(fire_base_start_date =
                                                    base::as.Date("2000-01-01"),
                                                max_lag_fire_base_days = 180,
                                                spat_deg_rad = 0.5,
                                                summary_type = "AVG",
                                                sfeat_types,
                                                out_qry_dir,
                                                out_tbl_name_pfx,
                                                fire_base_tbl_name,
                                                ind_comb_qry = TRUE){

    # Key transformations for building query string ----------------------------
    # Feature type
    sfeat_lowcase <- stringr::str_to_lower(string = sfeat_types)

    # Spatial Degrees
    spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
    spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                                  pattern = "\\.",
                                                  replacement = "")

    # SQL Summar Type e.g. AVG
    summary_type_lowcase <- stringr::str_to_lower(string = summary_type)

    if(ind_comb_qry){
        # Write combined query as a single file ------------------------------------

        # Our SQL table name i.e. mtbs_nldn_avg_s05_t180
        # for TOBS feature, 0.5 spatial degrees, 180 days
        out_gen_qry_name <- stringr::str_c(glue::glue("{out_tbl_name_pfx}",
                                                      "swdi",
                                                      "{summary_type_lowcase}",
                                                      "s{spat_deg_rad_str_nums}",
                                                      "t{max_lag_fire_base_days}",
                                                      .sep = "-"),
                                           ".sql")

        # Query name should be separated by "-" instead of underscores
        out_gen_qry_name <- stringr::str_replace_all(string = out_gen_qry_name,
                                                     pattern = "_",
                                                     replacement = "-")

        out_gen_qry_path <- fs::path_join(parts = c(out_qry_dir, out_gen_qry_name))
        print(out_gen_qry_path)

        sfeat_types %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_fire_base_swdi_sfeat_180d(fire_base_start_date =
                                                                                fire_base_start_date,
                                                                            max_lag_fire_base_days = max_lag_fire_base_days,
                                                                            spat_deg_rad = spat_deg_rad,
                                                                            summary_type = summary_type,
                                                                            sfeat = .x,
                                                                            out_qry_dir = out_qry_dir,
                                                                            out_tbl_name_pfx = out_tbl_name_pfx,
                                                                            fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::pull(.data = ., var = qry_str) %>%
            glue::glue_collapse(x = ., sep = "\n\n") %>%
            readr::write_lines(x = ., path = out_gen_qry_path)
    } else{
        # Write queries for each feature to individual files -----------------------
        sfeat_types %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_fire_base_swdi_sfeat_180d(fire_base_start_date =
                                                                                fire_base_start_date,
                                                                            max_lag_fire_base_days = max_lag_fire_base_days,
                                                                            spat_deg_rad = spat_deg_rad,
                                                                            summary_type = summary_type,
                                                                            sfeat = .x,
                                                                            out_qry_dir = out_qry_dir,
                                                                            out_tbl_name_pfx = out_tbl_name_pfx,
                                                                            fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::rename(x = qry_str, path = qry_path) %>%
            dplyr::rowwise(data = .) %>%
            purrr::pwalk(.l = ., .f = ~readr::write_lines(x = .x, path = .y))
    }
}


#' Generate all fire features joined in the mtbs table for modeling the ground
#' process and fire size. This joins all of the lagged aggregated time summaries
#' mapped to individual MTBS fires by specified spatial degrees over a 180 day
#' lag period for each fire
#'
#' @param fire_base_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_fire_base_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{fire_base_start_date}
#' @param spat_deg_rad (double) : The spatial degree radius to consider around
#' each fire when joining weather data (default value is 0.5)
#' @param summary_type (character) : The \code{SQL} summary function to apply
#' to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#'
#' @return (tibble) : A \code{tibble} with the query string, file path to store
#' the query, and a check of whether the filepath exists
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' gen_query_fire_base_full_feat_180d(fire_base_start_date = base::as.Date("2000-01-01"),
#'                                max_lag_fire_base_days = 180,
#'                                spat_deg_rad = 0.5,
#'                                summary_type = "AVG",
#'                                out_qry_dir = ".",
#'                                out_tbl_name_pfx = "mtbs",
#'                                fire_base_tbl_name = "mtbs_base_01")
#' }
gen_query_fire_base_full_feat_180d <- function(fire_base_start_date =
                                              base::as.Date("2000-01-01"),
                                          max_lag_fire_base_days = 180,
                                          spat_deg_rad = 0.5,
                                          summary_type = "AVG",
                                          out_qry_dir,
                                          out_tbl_name_pfx,
                                          fire_base_tbl_name){

    # Key transformations for building query string ----------------------------

    # Spatial Degrees
    spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
    spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                                  pattern = "\\.",
                                                  replacement = "")

    # SQL Summar Type e.g. AVG
    summary_type_lowcase <- stringr::str_to_lower(string = summary_type)

    # Our SQL table name i.e. mtbs_ghcnd_tobs_s05_t180
    # for TOBS feature, 0.5 spatial degrees, 180 days
    out_tbl_name <- glue::glue("{out_tbl_name_pfx}",
                               "full_feat",
                               "{summary_type_lowcase}",
                               "s{spat_deg_rad_str_nums}",
                               "t{max_lag_fire_base_days}",
                               .sep = "_")

    # Query name should be separated by "-" instead of underscores
    out_qry_name <- stringr::str_replace_all(string = out_tbl_name,
                                             pattern = "_",
                                             replacement = "-")

    # Input validation ---------------------------------------------------------
    assertthat::assert_that(stringr::str_length(string = spat_deg_rad_str) == 3,
                            msg = glue::glue("spat_deg_rad must be of char length 3 e.g. 0.5 or 1.0,
                                         it is currently {spat_deg_rad}"))
    assertthat::assert_that(stringr::str_detect(string = spat_deg_rad_str,
                                                pattern = "\\d\\.\\d"),
                            msg = glue::glue("spat_deg_rad  must be 1 dec. pl. e.g. 0.5 or 1.0,
                                         it is currently {spat_deg_rad}"))
    assertthat::assert_that(max_lag_fire_base_days >= 180,
                            msg = glue::glue("max_lag_fire_base_days must be >= 180,
                                         it is currently {max_lag_fire_base_days}"))
    assertthat::assert_that(fire_base_start_date <= base::as.Date("2000-01-01"),
                            msg = glue::glue("fire_base_start_date must be before {base::as.Date('2000-01-01')},
                                         it is currently {fire_base_start_date}"))
    assertthat::assert_that(summary_type == stringr::str_to_upper(string = summary_type),
                            msg = glue::glue("summary_type must be in upper case,
                                         it is currently {summary_type}"))
    assertthat::assert_that(base::is.character(x = out_qry_dir) &&
                                fs::dir_exists(path = out_qry_dir),
                            msg = glue::glue("out_qry_dir must be a valid path,
                                         it is currently {out_qry_dir}"))

    out_qry_path <- fs::path_join(parts = c(out_qry_dir,
                                            glue::glue("{out_qry_name}.sql")))

    out_qry_str <-
        glue::glue(
            "/* Start query to create {out_tbl_name} ---------------------------------*/
DROP TABLE IF EXISTS {out_tbl_name};

CREATE TABLE {out_tbl_name} AS

(SELECT fbase.fire_id,
        fbase.fire_name,
        fbase.geomac_uniq_fire,
        fbase.geomac_incd_name,
        fbase.fire_start_date_day_name,
        fbase.fire_start_date,
        fbase.fire_start_day,
        fbase.fire_start_month,
        fbase.fire_start_year,
        fbase.state_name,
        fbase.state_code,
        fbase.state_area_acres,
        fbase.county_name,
        fbase.county_code,
        fbase.county_area_acres,
        fbase.fire_start_season,
        fbase.fire_start_season_numeric,
        fbase.fire_end_season,
        fbase.fire_end_season_numeric,
        fbase.fire_area_acres_burned,
        fbase.fire_area_acres_burned_log,
        fbase.fire_area_sq_meters_burned,
        fbase.fire_area_sq_kilometers_burned,
        fbase.fire_area_sq_miles_burned,
        fbase.fire_duration_days,
        fbase.fire_perimeter,
        fbase.fire_centroid,
        fbase.fire_centroid_longitude,
        fbase.fire_centroid_latitude,
        fbase.fire_end_centroid,
        fbase.grid_name,
        fbase.grid_code,
        fbase.grid_area_sq_meters,
        fbase.grid_area_acres,
        fbase.grid_area_sq_kilometers,
        fbase.grid_area_sq_miles,
        fbase.grid_cell_type,
        fbase.mtbs_fire_type,
        fbase.mtbs_fire_asmnt_type,
      	prcp.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3d,
      	prcp.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t1w,
      	prcp.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t2w,
      	prcp.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3w,
      	prcp.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t1m,
      	prcp.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t2m,
      	prcp.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3m,
      	prcp.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t6m,
      	snow.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3d,
      	snow.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t1w,
      	snow.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t2w,
      	snow.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3w,
      	snow.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t1m,
      	snow.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t2m,
      	snow.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3m,
      	snow.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t6m,
      	snwd.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3d,
      	snwd.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t1w,
      	snwd.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t2w,
      	snwd.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3w,
      	snwd.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t1m,
      	snwd.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t2m,
      	snwd.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3m,
      	snwd.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t6m,
      	tavg.{summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t3d,
      	tavg.{summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t1w,
      	tavg.{summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t2w,
      	tavg.{summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t3w,
      	tavg.{summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t1m,
      	tavg.{summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t2m,
      	tavg.{summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t3m,
      	tavg.{summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_tavg_s{spat_deg_rad_str_nums}_t6m,
      	tmax.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3d,
      	tmax.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t1w,
      	tmax.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t2w,
      	tmax.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3w,
      	tmax.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t1m,
      	tmax.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t2m,
      	tmax.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3m,
      	tmax.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t6m,
      	tmin.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3d,
      	tmin.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t1w,
      	tmin.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t2w,
      	tmin.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3w,
      	tmin.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t1m,
      	tmin.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t2m,
      	tmin.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3m,
      	tmin.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t6m,
      	tobs.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3d,
      	tobs.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t1w,
      	tobs.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t2w,
      	tobs.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3w,
      	tobs.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t1m,
      	tobs.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t2m,
      	tobs.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3m,
      	tobs.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t6m,
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3d,
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t1w,
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t2w,
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3w,
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t1m,
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t2m,
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3m,
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t6m,
      	hail.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3d,
      	hail.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t1w,
      	hail.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t2w,
      	hail.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3w,
      	hail.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t1m,
      	hail.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t2m,
      	hail.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3m,
      	hail.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t6m,
      	tvs.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3d,
      	tvs.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t1w,
      	tvs.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t2w,
      	tvs.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3w,
      	tvs.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t1m,
      	tvs.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t2m,
      	tvs.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3m,
      	tvs.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t6m,
      	structure.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3d,
      	structure.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t1w,
      	structure.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t2w,
      	structure.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3w,
      	structure.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t1m,
      	structure.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t2m,
      	structure.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3m,
      	structure.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t6m
FROM {fire_base_tbl_name} AS fbase
LEFT JOIN {out_tbl_name_pfx}_ghcnd_prcp_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS prcp
	ON fbase.fire_id = prcp.fire_id
LEFT JOIN {out_tbl_name_pfx}_ghcnd_snow_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS snow
	ON fbase.fire_id = snow.fire_id
LEFT JOIN {out_tbl_name_pfx}_ghcnd_snwd_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS snwd
	ON fbase.fire_id = snwd.fire_id
LEFT JOIN {out_tbl_name_pfx}_ghcnd_t{summary_type_lowcase}_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS tavg
	ON fbase.fire_id = tavg.fire_id
LEFT JOIN {out_tbl_name_pfx}_ghcnd_tmax_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS tmax
	ON fbase.fire_id = tmax.fire_id
LEFT JOIN {out_tbl_name_pfx}_ghcnd_tmin_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS tmin
	ON fbase.fire_id = tmin.fire_id
LEFT JOIN {out_tbl_name_pfx}_ghcnd_tobs_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS tobs
	ON fbase.fire_id = tobs.fire_id
LEFT JOIN {out_tbl_name_pfx}_nldn_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS nldn
	ON fbase.fire_id = nldn.fire_id
LEFT JOIN {out_tbl_name_pfx}_hail_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS hail
	ON fbase.fire_id = hail.fire_id
LEFT JOIN {out_tbl_name_pfx}_tvs_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS tvs
	ON fbase.fire_id = tvs.fire_id
LEFT JOIN {out_tbl_name_pfx}_structure_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS structure
	ON fbase.fire_id = structure.fire_id
WHERE fbase.fire_start_date::date >= {glue::single_quote(
      format(fire_base_start_date, '%d-%m-%Y'))}::date
);

/* Index this table on fire_id for the final LEFT JOIN of all features*/
CREATE INDEX {out_tbl_name}_fire_id ON {out_tbl_name} USING HASH (fire_id);

/* End query to create {out_tbl_name} ----------------------------------*/")

    out_val <- tibble::tibble(qry_str = out_qry_str, qry_path = out_qry_path)

    base::return(out_val)
}


#' Wrapper function to generate a single combined or separate \code{SQL} files
#' for all fire features joined in the mtbs table for modeling the ground
#' process and fire size. This joins all of the lagged aggregated time summaries
#' mapped to individual MTBS fires by specified spatial degrees over a 180 day
#' lag period for each fire
#'
#' @param fire_base_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_fire_base_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{fire_base_start_date}
#' @param spat_deg_rad (double) : The spatial degree radius to consider around
#' each fire when joining weather data (default value is 0.5)
#' @param summary_type (character) : The \code{SQL} summary function to apply
#' to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#' @param ind_comb_qry (logical) : If \code{TRUE} then produces a single
#' combined \code{sql} query for all features combined. If \code{FALSE}
#' then produces a separate \code{sql} queries for individual GHCN-D feature
#'
#' @return (tibble) : A single or separate \code{SQL} files for single
#' GHCN features joined to MTBS fire_ids by specified spatial and temporal
#' parameters
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' wrap_gen_query_fire_base_full_feat_180d(fire_base_start_date =
#'                                       base::as.Date("2000-01-01"),
#'                                     max_lag_fire_base_days = 180,
#'                                     spat_deg_rad = 0.5,
#'                                     summary_type = "AVG",
#'                                     out_qry_dir = ".",
#'                                     out_tbl_name_pfx = "mtbs",
#'                                     fire_base_tbl_name = "mtbs_base_01",
#'                                     ind_comb_qry = TRUE)
#' }
wrap_gen_query_fire_base_full_feat_180d <- function(fire_base_start_date =
                                                   base::as.Date("2000-01-01"),
                                               max_lag_fire_base_days = 180,
                                               spat_deg_rad = 0.5,
                                               summary_type = "AVG",
                                               out_qry_dir,
                                               out_tbl_name_pfx,
                                               fire_base_tbl_name,
                                               ind_comb_qry = TRUE){

    # Key transformations for building query string ----------------------------

    # Spatial Degrees
    spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
    spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                                  pattern = "\\.",
                                                  replacement = "")

    # SQL Summar Type e.g. AVG
    summary_type_lowcase <- stringr::str_to_lower(string = summary_type)

    if(ind_comb_qry){
        # Write combined query as a single file ------------------------------------

        # Our SQL table name i.e. mtbs_nldn_avg_s05_t180
        # for TOBS feature, 0.5 spatial degrees, 180 days
        out_gen_qry_name <- stringr::str_c(glue::glue("{out_tbl_name_pfx}",
                                                      "full-feat",
                                                      "{summary_type_lowcase}",
                                                      "s{spat_deg_rad_str_nums}",
                                                      "t{max_lag_fire_base_days}",
                                                      .sep = "-"),
                                           ".sql")

        # Query name should be separated by "-" instead of underscores
        out_gen_qry_name <- stringr::str_replace_all(string = out_gen_qry_name,
                                                     pattern = "_",
                                                     replacement = "-")

        out_gen_qry_path <- fs::path_join(parts = c(out_qry_dir, out_gen_qry_name))
        print(out_gen_qry_path)

        summary_type %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_fire_base_full_feat_180d(fire_base_start_date =
                                                                               fire_base_start_date,
                                                                           max_lag_fire_base_days = max_lag_fire_base_days,
                                                                           spat_deg_rad = spat_deg_rad,
                                                                           summary_type = .x,
                                                                           out_qry_dir = out_qry_dir,
                                                                           out_tbl_name_pfx = out_tbl_name_pfx,
                                                                           fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::pull(.data = ., var = qry_str) %>%
            glue::glue_collapse(x = ., sep = "\n\n") %>%
            readr::write_lines(x = ., path = out_gen_qry_path)
    } else{
        # Write queries for each feature to individual files -----------------------
        summary_type %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_fire_base_full_feat_180d(fire_base_start_date =
                                                                               fire_base_start_date,
                                                                           max_lag_fire_base_days = max_lag_fire_base_days,
                                                                           spat_deg_rad = spat_deg_rad,
                                                                           summary_type = .x,
                                                                           out_qry_dir = out_qry_dir,
                                                                           out_tbl_name_pfx = out_tbl_name_pfx,
                                                                           fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::rename(x = qry_str, path = qry_path) %>%
            dplyr::rowwise(data = .) %>%
            purrr::pwalk(.l = ., .f = ~readr::write_lines(x = .x, path = .y))
    }
}

#' Generate all the transformed fire features joined in the mtbs table for
#' modeling the ground process and fire size. This joins all of the lagged
#' aggregated time summaries mapped to individual MTBS fires by specified
#' spatial degrees over a 180 day lag period for each fire.
#'
#' @param fire_base_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_fire_base_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{fire_base_start_date}
#' @param spat_deg_rad (double) : The spatial degree radius to consider around
#' each fire when joining weather data (default value is 0.5)
#' @param summary_type (character) : The \code{SQL} summary function to apply
#' to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#'
#' @return (tibble) : A \code{tibble} with the query string, file path to store
#' the query, and a check of whether the filepath exists
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' gen_query_fire_base_full_feat__tform_180d(fire_base_start_date = base::as.Date("2000-01-01"),
#'                                      max_lag_fire_base_days = 180,
#'                                      spat_deg_rad = 0.5,
#'                                      summary_type = "AVG",
#'                                      out_qry_dir = ".",
#'                                      out_tbl_name_pfx = "mtbs",
#'                                      fire_base_tbl_name = "mtbs_base_01")
#' }
gen_query_fire_base_full_feat_tform_180d <- function(fire_base_start_date =
                                                    base::as.Date("2000-01-01"),
                                                max_lag_fire_base_days = 180,
                                                spat_deg_rad = 0.5,
                                                summary_type = "AVG",
                                                out_qry_dir,
                                                out_tbl_name_pfx,
                                                fire_base_tbl_name){

    # Key transformations for building query string ----------------------------

    # Spatial Degrees
    spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
    spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                                  pattern = "\\.",
                                                  replacement = "")

    # SQL Summar Type e.g. AVG
    summary_type_lowcase <- stringr::str_to_lower(string = summary_type)

    # Our SQL table name i.e. mtbs_ghcnd_tobs_s05_t180
    # for TOBS feature, 0.5 spatial degrees, 180 days
    out_tbl_name <- glue::glue("{out_tbl_name_pfx}",
                               "full_feat_tform", # This is the transformed table
                               "{summary_type_lowcase}",
                               "s{spat_deg_rad_str_nums}",
                               "t{max_lag_fire_base_days}",
                               .sep = "_")

    # Query name should be separated by "-" instead of underscores
    out_qry_name <- stringr::str_replace_all(string = out_tbl_name,
                                             pattern = "_",
                                             replacement = "-")

    # Input validation ---------------------------------------------------------
    assertthat::assert_that(stringr::str_length(string = spat_deg_rad_str) == 3,
                            msg = glue::glue("spat_deg_rad must be of char length 3 e.g. 0.5 or 1.0,
                                         it is currently {spat_deg_rad}"))
    assertthat::assert_that(stringr::str_detect(string = spat_deg_rad_str,
                                                pattern = "\\d\\.\\d"),
                            msg = glue::glue("spat_deg_rad  must be 1 dec. pl. e.g. 0.5 or 1.0,
                                         it is currently {spat_deg_rad}"))
    assertthat::assert_that(max_lag_fire_base_days >= 180,
                            msg = glue::glue("max_lag_fire_base_days must be >= 180,
                                         it is currently {max_lag_fire_base_days}"))
    assertthat::assert_that(fire_base_start_date <= base::as.Date("2000-01-01"),
                            msg = glue::glue("fire_base_start_date must be before {base::as.Date('2000-01-01')},
                                         it is currently {fire_base_start_date}"))
    assertthat::assert_that(summary_type == stringr::str_to_upper(string = summary_type),
                            msg = glue::glue("summary_type must be in upper case,
                                         it is currently {summary_type}"))
    assertthat::assert_that(base::is.character(x = out_qry_dir) &&
                                fs::dir_exists(path = out_qry_dir),
                            msg = glue::glue("out_qry_dir must be a valid path,
                                         it is currently {out_qry_dir}"))

    out_qry_path <- fs::path_join(parts = c(out_qry_dir,
                                            glue::glue("{out_qry_name}.sql")))

    out_qry_str <-
        glue::glue(
            "/* Start query to create {out_tbl_name} ---------------------------------*/
DROP TABLE IF EXISTS {out_tbl_name};

CREATE TABLE {out_tbl_name} AS

(SELECT agg_data.fire_id,
        agg_data.fire_name,
        agg_data.geomac_uniq_fire,
        agg_data.geomac_incd_name,
        agg_data.fire_start_date_day_name,
        agg_data.fire_start_date,
        agg_data.fire_start_day,
        agg_data.fire_start_month,
        agg_data.fire_start_year,
        agg_data.state_name,
        agg_data.state_code,
        agg_data.state_area_acres,
        agg_data.county_name,
        agg_data.county_code,
        agg_data.county_area_acres,
        agg_data.fire_start_season,
        agg_data.fire_start_season_numeric,
        agg_data.fire_end_season,
        agg_data.fire_end_season_numeric,
        agg_data.fire_area_acres_burned,
        agg_data.fire_area_acres_burned_log,
        agg_data.fire_area_sq_meters_burned,
        agg_data.fire_area_sq_kilometers_burned,
        agg_data.fire_area_sq_miles_burned,
        agg_data.fire_duration_days,
        agg_data.fire_perimeter,
        agg_data.fire_centroid,
        agg_data.fire_centroid_longitude,
        agg_data.fire_centroid_latitude,
        agg_data.fire_end_centroid,
        agg_data.grid_name,
        agg_data.grid_code,
        agg_data.grid_area_sq_meters,
        agg_data.grid_area_sq_kilometers,
        agg_data.grid_area_acres,
        agg_data.grid_area_sq_miles,
        agg_data.grid_cell_type,
        agg_data.mtbs_fire_type,
        agg_data.mtbs_fire_asmnt_type,
       COALESCE(agg_data.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3d, 0) AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3d,
       COALESCE(agg_data.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t1w, 0) AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t1w,
       COALESCE(agg_data.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t2w, 0) AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t2w,
       COALESCE(agg_data.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3w, 0) AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3w,
       COALESCE(agg_data.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t1m, 0) AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t1m,
       COALESCE(agg_data.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t2m, 0) AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t2m,
       COALESCE(agg_data.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3m, 0) AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t3m,
       COALESCE(agg_data.{summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t6m, 0) AS {summary_type_lowcase}_prcp_s{spat_deg_rad_str_nums}_t6m,
       COALESCE(agg_data.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3d, 0) AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3d,
       COALESCE(agg_data.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t1w, 0) AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t1w,
       COALESCE(agg_data.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t2w, 0) AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t2w,
       COALESCE(agg_data.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3w, 0) AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3w,
       COALESCE(agg_data.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t1m, 0) AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t1m,
       COALESCE(agg_data.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t2m, 0) AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t2m,
       COALESCE(agg_data.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3m, 0) AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t3m,
       COALESCE(agg_data.{summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t6m, 0) AS {summary_type_lowcase}_snow_s{spat_deg_rad_str_nums}_t6m,
       COALESCE(agg_data.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3d, 0) AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3d,
       COALESCE(agg_data.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t1w, 0) AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t1w,
       COALESCE(agg_data.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t2w, 0) AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t2w,
       COALESCE(agg_data.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3w, 0) AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3w,
       COALESCE(agg_data.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t1m, 0) AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t1m,
       COALESCE(agg_data.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t2m, 0) AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t2m,
       COALESCE(agg_data.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3m, 0) AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t3m,
       COALESCE(agg_data.{summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t6m, 0) AS {summary_type_lowcase}_snwd_s{spat_deg_rad_str_nums}_t6m,
       agg_data.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3d,
       agg_data.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t1w,
       agg_data.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t2w,
       agg_data.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3w,
       agg_data.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t1m,
       agg_data.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t2m,
       agg_data.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t3m,
       agg_data.{summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_tmax_s{spat_deg_rad_str_nums}_t6m,
       agg_data.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3d,
       agg_data.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t1w,
       agg_data.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t2w,
       agg_data.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3w,
       agg_data.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t1m,
       agg_data.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t2m,
       agg_data.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t3m,
       agg_data.{summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_tmin_s{spat_deg_rad_str_nums}_t6m,
       agg_data.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3d AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3d,
       agg_data.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t1w AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t1w,
       agg_data.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t2w AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t2w,
       agg_data.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3w AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3w,
       agg_data.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t1m AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t1m,
       agg_data.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t2m AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t2m,
       agg_data.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3m AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t3m,
       agg_data.{summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_tobs_s{spat_deg_rad_str_nums}_t6m,
       COALESCE(agg_data.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3d, 0) AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3d,
       COALESCE(agg_data.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t1w, 0) AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t1w,
       COALESCE(agg_data.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t2w, 0) AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t2w,
       COALESCE(agg_data.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3w, 0) AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3w,
       COALESCE(agg_data.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t1m, 0) AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t1m,
       COALESCE(agg_data.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t2m, 0) AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t2m,
       COALESCE(agg_data.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3m, 0) AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t3m,
       COALESCE(agg_data.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t6m, 0) AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t6m,
       COALESCE(agg_data.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3d, 0) AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3d,
       COALESCE(agg_data.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t1w, 0) AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t1w,
       COALESCE(agg_data.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t2w, 0) AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t2w,
       COALESCE(agg_data.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3w, 0) AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3w,
       COALESCE(agg_data.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t1m, 0) AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t1m,
       COALESCE(agg_data.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t2m, 0) AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t2m,
       COALESCE(agg_data.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3m, 0) AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t3m,
       COALESCE(agg_data.{summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t6m, 0) AS {summary_type_lowcase}_hail_count_s{spat_deg_rad_str_nums}_t6m,
       COALESCE(agg_data.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3d, 0) AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3d,
       COALESCE(agg_data.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t1w, 0) AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t1w,
       COALESCE(agg_data.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t2w, 0) AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t2w,
       COALESCE(agg_data.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3w, 0) AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3w,
       COALESCE(agg_data.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t1m, 0) AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t1m,
       COALESCE(agg_data.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t2m, 0) AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t2m,
       COALESCE(agg_data.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3m, 0) AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t3m,
       COALESCE(agg_data.{summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t6m, 0) AS {summary_type_lowcase}_tvs_count_s{spat_deg_rad_str_nums}_t6m,
       COALESCE(agg_data.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3d, 0) AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3d,
       COALESCE(agg_data.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t1w, 0) AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t1w,
       COALESCE(agg_data.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t2w, 0) AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t2w,
       COALESCE(agg_data.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3w, 0) AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3w,
       COALESCE(agg_data.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t1m, 0) AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t1m,
       COALESCE(agg_data.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t2m, 0) AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t2m,
       COALESCE(agg_data.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3m, 0) AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t3m,
       COALESCE(agg_data.{summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t6m, 0) AS {summary_type_lowcase}_structure_count_s{spat_deg_rad_str_nums}_t6m
FROM {fire_base_tbl_name}_full_feat_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_fire_base_days} AS agg_data
);

/* Index this table on fire_id for the final LEFT JOIN of all features*/
CREATE INDEX {out_tbl_name}_fire_id ON {out_tbl_name} USING HASH (fire_id);
CREATE INDEX {out_tbl_name}_perimeter ON {out_tbl_name} USING GIST (fire_perimeter);
CREATE INDEX {out_tbl_name}_start_date ON {out_tbl_name} (fire_start_date);

/* End query to create {out_tbl_name} ----------------------------------*/")

    out_val <- tibble::tibble(qry_str = out_qry_str, qry_path = out_qry_path)

    base::return(out_val)
}

#' Wrapper function to generate a single combined or separate \code{SQL} files
#' for all transformed fire features joined in the mtbs table for modeling the
#' ground process and fire size. This joins all of the lagged aggregated
#' time summaries mapped to individual MTBS fires by specified spatial degrees
#' over a 180 day lag period for each fire
#'
#' @param fire_base_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_fire_base_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{fire_base_start_date}
#' @param spat_deg_rad (double) : The spatial degree radius to consider around
#' each fire when joining weather data (default value is 0.5)
#' @param summary_type (character) : The \code{SQL} summary function to apply
#' to the GHCN-D weather data e.g. "AVG", "MEDIAN" (default value is "AVG")
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#' @param ind_comb_qry (logical) : If \code{TRUE} then produces a single
#' combined \code{sql} query for all features combined. If \code{FALSE}
#' then produces a separate \code{sql} queries for individual GHCN-D feature
#'
#' @return (tibble) : A single or separate \code{SQL} files for single
#' GHCN features joined to MTBS fire_ids by specified spatial and temporal
#' parameters
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' wrap_gen_query_fire_base_full_feat_tform_180d(fire_base_start_date =
#'                                            base::as.Date("2000-01-01"),
#'                                          max_lag_fire_base_days = 180,
#'                                          spat_deg_rad = 0.5,
#'                                          summary_type = "AVG",
#'                                          out_qry_dir = ".",
#'                                          out_tbl_name_pfx = "mtbs",
#'                                          fire_base_tbl_name = "mtbs_base_01",
#'                                          ind_comb_qry = TRUE)
#' }
wrap_gen_query_fire_base_full_feat_tform_180d <- function(fire_base_start_date =
                                                         base::as.Date("2000-01-01"),
                                                     max_lag_fire_base_days = 180,
                                                     spat_deg_rad = 0.5,
                                                     summary_type = "AVG",
                                                     out_qry_dir,
                                                     out_tbl_name_pfx,
                                                     fire_base_tbl_name,
                                                     ind_comb_qry = TRUE){

    # Key transformations for building query string ----------------------------

    # Spatial Degrees
    spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
    spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                                  pattern = "\\.",
                                                  replacement = "")

    # SQL Summar Type e.g. AVG
    summary_type_lowcase <- stringr::str_to_lower(string = summary_type)

    if(ind_comb_qry){
        # Write combined query as a single file ------------------------------------

        # Our SQL table name i.e. mtbs_nldn_avg_s05_t180
        # for TOBS feature, 0.5 spatial degrees, 180 days
        out_gen_qry_name <- stringr::str_c(glue::glue("{out_tbl_name_pfx}",
                                                      "full-feat-tform", # The transformed dataset in the pipeline
                                                      "{summary_type_lowcase}",
                                                      "s{spat_deg_rad_str_nums}",
                                                      "t{max_lag_fire_base_days}",
                                                      .sep = "-"),
                                           ".sql")

        # Query name should be separated by "-" instead of underscores
        out_gen_qry_name <- stringr::str_replace_all(string = out_gen_qry_name,
                                                     pattern = "_",
                                                     replacement = "-")

        out_gen_qry_path <- fs::path_join(parts = c(out_qry_dir, out_gen_qry_name))
        print(out_gen_qry_path)

        summary_type %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_fire_base_full_feat_tform_180d(fire_base_start_date =
                                                                                     fire_base_start_date,
                                                                                 max_lag_fire_base_days = max_lag_fire_base_days,
                                                                                 spat_deg_rad = spat_deg_rad,
                                                                                 summary_type = .x,
                                                                                 out_qry_dir = out_qry_dir,
                                                                                 out_tbl_name_pfx = out_tbl_name_pfx,
                                                                                 fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::pull(.data = ., var = qry_str) %>%
            glue::glue_collapse(x = ., sep = "\n\n") %>%
            readr::write_lines(x = ., path = out_gen_qry_path)
    } else{
        # Write queries for each feature to individual files -----------------------
        summary_type %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_fire_base_full_feat_tform_180d(fire_base_start_date =
                                                                                     fire_base_start_date,
                                                                                 max_lag_fire_base_days = max_lag_fire_base_days,
                                                                                 spat_deg_rad = spat_deg_rad,
                                                                                 summary_type = .x,
                                                                                 out_qry_dir = out_qry_dir,
                                                                                 out_tbl_name_pfx = out_tbl_name_pfx,
                                                                                 fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::rename(x = qry_str, path = qry_path) %>%
            dplyr::rowwise(data = .) %>%
            purrr::pwalk(.l = ., .f = ~readr::write_lines(x = .x, path = .y))
    }
}
