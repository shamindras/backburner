#' Generate single feature GHCN-D lagged aggregated time summaries mapped to
#' individual MTBS fires by specified spatial degrees over a 180 day lag period
#' for each fire
#'
#' @param mtbs_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_mtbs_days (integer) : The number of days for GHCN-D weather
#' data to lag behind the \code{mtbs_start_date}
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
#' gen_query_mtbs_ghcnd_sfeat_180d(mtbs_start_date = base::as.Date("2000-01-01"),
#'                                 max_lag_mtbs_days = 180,
#'                                 spat_deg_rad = 0.5,
#'                                 summary_type = "AVG",
#'                                 sfeat = "TOBS",
#'                                 out_qry_dir = ".")
#' }
gen_query_mtbs_ghcnd_sfeat_180d <- function(mtbs_start_date = base::as.Date("2000-01-01"),
                                            max_lag_mtbs_days = 180,
                                            spat_deg_rad = 0.5,
                                            summary_type = "AVG",
                                            sfeat,
                                            out_qry_dir){

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
    out_tbl_name <- glue::glue("mtbs_ghcnd",
                               "{sfeat_lowcase}",
                               "{summary_type_lowcase}",
                               "s{spat_deg_rad_str_nums}",
                               "t{max_lag_mtbs_days}",
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
    assertthat::assert_that(max_lag_mtbs_days >= 180,
                            msg = glue::glue("max_lag_mtbs_days must be >= 180,
                                         it is currently {max_lag_mtbs_days}"))
    assertthat::assert_that(mtbs_start_date <= base::as.Date("2000-01-01"),
                            msg = glue::glue("mtbs_start_date must be before {base::as.Date('2000-01-01')},
                                         it is currently {mtbs_start_date}"))
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
(SELECT mtbsf.fire_id,
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN mtbsf.fire_start_date::date - integer '3'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't3d', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN mtbsf.fire_start_date::date - integer '7'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't1w', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN mtbsf.fire_start_date::date - integer '14'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't2w', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN mtbsf.fire_start_date::date - integer '21'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't3w', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN mtbsf.fire_start_date::date - integer '30'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't1m', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN mtbsf.fire_start_date::date - integer '60'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't2m', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN mtbsf.fire_start_date::date - integer '90'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't3m', .sep = '_'))},
    {summary_type}(ghobs.{glue::double_quote(sfeat)}) FILTER (WHERE ghobs.record_dt BETWEEN mtbsf.fire_start_date::date - integer '180'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 's{spat_deg_rad_str_nums}', 't6m', .sep = '_'))}
FROM (SELECT mpfb.fire_id,
             mpfb.fire_start_date,
             mpfb.fire_centroid
      FROM mtbs_perims_fod_pts_base as mpfb
      WHERE mpfb.fire_start_date::date >= {glue::single_quote(
      format(mtbs_start_date, '%d-%m-%Y'))}::date) AS mtbsf
LEFT JOIN ghcnd_observations AS ghobs
    ON (ghobs.record_dt BETWEEN (mtbsf.fire_start_date::date - integer {glue::single_quote(max_lag_mtbs_days)})
                           AND (mtbsf.fire_start_date::date - integer '1')
        AND ST_DWithin(mtbsf.fire_centroid, ghobs.location, {spat_deg_rad}))
GROUP BY mtbsf.fire_id);

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
#' @param mtbs_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_mtbs_days (integer) : The number of days for GHCN-D weather
#' data to lag behind the \code{mtbs_start_date}
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
#' wrap_gen_query_mtbs_ghcnd_sfeat_180d(mtbs_start_date =
#'                                          base::as.Date("2000-01-01"),
#'                                      max_lag_mtbs_days = 180,
#'                                      spat_deg_rad = 0.5,
#'                                      summary_type = "AVG",
#'                                      sfeat_types = c("TOBS", "TMIN", "TMAX"),
#'                                      out_qry_dir = ".",
#'                                      ind_comb_qry = TRUE)
#' }
wrap_gen_query_mtbs_ghcnd_sfeat_180d <- function(mtbs_start_date =
                                                     base::as.Date("2000-01-01"),
                                                 max_lag_mtbs_days = 180,
                                                 spat_deg_rad = 0.5,
                                                 summary_type = "AVG",
                                                 sfeat_types,
                                                 out_qry_dir,
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
        out_gen_qry_name <- stringr::str_c(glue::glue("mtbs-ghcnd",
                                                      "{summary_type_lowcase}",
                                                      "s{spat_deg_rad_str_nums}",
                                                      "t{max_lag_mtbs_days}",
                                                      .sep = "-"),
                                           ".sql")

        out_gen_qry_path <- fs::path_join(parts = c(out_qry_dir, out_gen_qry_name))
        out_gen_qry_path

        sfeat_types %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_mtbs_ghcnd_sfeat_180d(mtbs_start_date =
                                                                                 mtbs_start_date,
                                                                             max_lag_mtbs_days = max_lag_mtbs_days,
                                                                             spat_deg_rad = spat_deg_rad,
                                                                             summary_type = summary_type,
                                                                             sfeat = .x,
                                                                             out_qry_dir = out_qry_dir)) %>%
            dplyr::pull(.data = ., var = qry_str) %>%
            glue::glue_collapse(x = ., sep = "\n\n") %>%
            readr::write_lines(x = ., path = out_gen_qry_path)
    } else{
        # Write queries for each feature to individual files -----------------------
        sfeat_types %>%
            purrr::map_dfr(.x = .,
                           .f = ~backburner::gen_query_mtbs_ghcnd_sfeat_180d(mtbs_start_date =
                                                                                 mtbs_start_date,
                                                                             max_lag_mtbs_days = max_lag_mtbs_days,
                                                                             spat_deg_rad = spat_deg_rad,
                                                                             summary_type = summary_type,
                                                                             sfeat = .x,
                                                                             out_qry_dir = out_qry_dir)) %>%
            dplyr::rename(x = qry_str, path = qry_path) %>%
            dplyr::rowwise(data = .) %>%
            purrr::pwalk(.l = ., .f = ~readr::write_lines(x = .x, path = .y))
    }
}


#' Generate single feature NOAA SWDI lagged aggregated time summaries mapped to
#' individual MTBS fires by specified spatial degrees over a 180 day lag period
#' for each fire
#'
#' @param mtbs_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_mtbs_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{mtbs_start_date}
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
#' gen_query_mtbs_swdi_sfeat_180d(mtbs_start_date = base::as.Date("2000-01-01"),
#'                                max_lag_mtbs_days = 180,
#'                                spat_deg_rad = 0.5,
#'                                summary_type = "AVG",
#'                                sfeat = "nldn",
#'                                out_qry_dir = ".")
#' }
gen_query_mtbs_swdi_sfeat_180d <- function(mtbs_start_date = base::as.Date("2000-01-01"),
                                           max_lag_mtbs_days = 180,
                                           spat_deg_rad = 0.5,
                                           summary_type = "AVG",
                                           sfeat,
                                           out_qry_dir){

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
  out_tbl_name <- glue::glue("mtbs",
                             "{sfeat_lowcase}",
                             "{summary_type_lowcase}",
                             "s{spat_deg_rad_str_nums}",
                             "t{max_lag_mtbs_days}",
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
  assertthat::assert_that(max_lag_mtbs_days >= 180,
                          msg = glue::glue("max_lag_mtbs_days must be >= 180,
                                         it is currently {max_lag_mtbs_days}"))
  assertthat::assert_that(mtbs_start_date <= base::as.Date("2000-01-01"),
                          msg = glue::glue("mtbs_start_date must be before {base::as.Date('2000-01-01')},
                                         it is currently {mtbs_start_date}"))
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
(SELECT mtbsf.fire_id,
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN mtbsf.fire_start_date::date - integer '3'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't3d', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN mtbsf.fire_start_date::date - integer '7'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't1w', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN mtbsf.fire_start_date::date - integer '14'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't2w', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN mtbsf.fire_start_date::date - integer '21'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't3w', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN mtbsf.fire_start_date::date - integer '30'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't1m', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN mtbsf.fire_start_date::date - integer '60'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't2m', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN mtbsf.fire_start_date::date - integer '90'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't3m', .sep = '_'))},
    {summary_type}({sfeat}.tot_count) FILTER (WHERE {sfeat}.record_dt BETWEEN mtbsf.fire_start_date::date - integer '180'
                                                      AND mtbsf.fire_start_date::date - integer '1') AS {glue::double_quote(
                                                      glue::glue({summary_type_lowcase}, {sfeat_lowcase}, 'count', 's{spat_deg_rad_str_nums}', 't6m', .sep = '_'))}
FROM (SELECT mpfb.fire_id,
             mpfb.fire_start_date,
             mpfb.fire_centroid
      FROM mtbs_perims_fod_pts_base as mpfb
      WHERE mpfb.fire_start_date::date >= {glue::single_quote(
      format(mtbs_start_date, '%d-%m-%Y'))}::date) AS mtbsf
LEFT JOIN noaa_swdi_{sfeat} AS {sfeat}
    ON ({sfeat}.record_dt BETWEEN (mtbsf.fire_start_date::date - integer {glue::single_quote(max_lag_mtbs_days)})
                           AND (mtbsf.fire_start_date::date - integer '1')
        AND ST_DWithin(mtbsf.fire_centroid, {sfeat}.geometry, {spat_deg_rad}))
GROUP BY mtbsf.fire_id);

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
#' @param mtbs_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_mtbs_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{mtbs_start_date}
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
#' wrap_gen_query_mtbs_swdi_sfeat_180d(mtbs_start_date =
#'                                       base::as.Date("2000-01-01"),
#'                                     max_lag_mtbs_days = 180,
#'                                     spat_deg_rad = 0.5,
#'                                     summary_type = "AVG",
#'                                     sfeat_types = c("nldn", "hail", "tvs", "structure"),
#'                                     out_qry_dir = ".",
#'                                     ind_comb_qry = TRUE)
#' }
wrap_gen_query_mtbs_swdi_sfeat_180d <- function(mtbs_start_date =
                                                  base::as.Date("2000-01-01"),
                                                max_lag_mtbs_days = 180,
                                                spat_deg_rad = 0.5,
                                                summary_type = "AVG",
                                                sfeat_types,
                                                out_qry_dir,
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
    out_gen_qry_name <- stringr::str_c(glue::glue("mtbs",
                                                  "swdi",
                                                  "{summary_type_lowcase}",
                                                  "s{spat_deg_rad_str_nums}",
                                                  "t{max_lag_mtbs_days}",
                                                  .sep = "-"),
                                       ".sql")

    out_gen_qry_path <- fs::path_join(parts = c(out_qry_dir, out_gen_qry_name))
    print(out_gen_qry_path)

    sfeat_types %>%
      purrr::map_dfr(.x = .,
                     .f = ~backburner::gen_query_mtbs_swdi_sfeat_180d(mtbs_start_date =
                                                                         mtbs_start_date,
                                                                       max_lag_mtbs_days = max_lag_mtbs_days,
                                                                       spat_deg_rad = spat_deg_rad,
                                                                       summary_type = summary_type,
                                                                       sfeat = .x,
                                                                       out_qry_dir = out_qry_dir)) %>%
      dplyr::pull(.data = ., var = qry_str) %>%
      glue::glue_collapse(x = ., sep = "\n\n") %>%
      readr::write_lines(x = ., path = out_gen_qry_path)
  } else{
    # Write queries for each feature to individual files -----------------------
    sfeat_types %>%
      purrr::map_dfr(.x = .,
                     .f = ~backburner::gen_query_mtbs_swdi_sfeat_180d(mtbs_start_date =
                                                                         mtbs_start_date,
                                                                       max_lag_mtbs_days = max_lag_mtbs_days,
                                                                       spat_deg_rad = spat_deg_rad,
                                                                       summary_type = summary_type,
                                                                       sfeat = .x,
                                                                       out_qry_dir = out_qry_dir)) %>%
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
#' @param mtbs_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_mtbs_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{mtbs_start_date}
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
#' gen_query_mtbs_full_feat_180d(mtbs_start_date = base::as.Date("2000-01-01"),
#'                                max_lag_mtbs_days = 180,
#'                                spat_deg_rad = 0.5,
#'                                summary_type = "AVG",
#'                                out_qry_dir = ".")
#' }
gen_query_mtbs_full_feat_180d <- function(mtbs_start_date =
                                          base::as.Date("2000-01-01"),
                                          max_lag_mtbs_days = 180,
                                          spat_deg_rad = 0.5,
                                          summary_type = "AVG",
                                          out_qry_dir){

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
  out_tbl_name <- glue::glue("mtbs",
                             "full_feat",
                             "{summary_type_lowcase}",
                             "s{spat_deg_rad_str_nums}",
                             "t{max_lag_mtbs_days}",
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
  assertthat::assert_that(max_lag_mtbs_days >= 180,
                          msg = glue::glue("max_lag_mtbs_days must be >= 180,
                                         it is currently {max_lag_mtbs_days}"))
  assertthat::assert_that(mtbs_start_date <= base::as.Date("2000-01-01"),
                          msg = glue::glue("mtbs_start_date must be before {base::as.Date('2000-01-01')},
                                         it is currently {mtbs_start_date}"))
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

(SELECT mtbs.fire_id AS fire_id,
        mtbs.fire_name AS fire_name,
      	mtbs.fire_start_date::date AS fire_start_date,
      	mtbs.fire_acres_burned AS area_burned_acres,
      	ST_X(mtbs.fire_centroid)::float AS fire_longitude,
      	ST_Y(mtbs.fire_centroid)::float AS fire_latitude,
        mtbs.fire_perimeter as fire_perimeter,
        mtbs.fire_type AS mtbs_fire_type,
        mtbs.fire_asmnt_type as mtbs_fire_asmnt_type,
        mtbs.fire_start_season as fire_start_season,
        mtbs.start_day as fire_start_day,
        mtbs.start_month as fire_start_month,
        mtbs.start_year as fire_start_year,
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
      	nldn.{summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t6m AS {summary_type_lowcase}_nldn_count_s{spat_deg_rad_str_nums}_t6m
FROM (SELECT mpfb.*
      FROM mtbs_perims_fod_pts_base mpfb
      WHERE mpfb.fire_start_date::date >= {glue::single_quote(
      format(mtbs_start_date, '%d-%m-%Y'))}::date) AS mtbs
LEFT JOIN mtbs_ghcnd_prcp_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_mtbs_days} AS prcp
	ON mtbs.fire_id = prcp.fire_id
LEFT JOIN mtbs_ghcnd_snow_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_mtbs_days} AS snow
	ON mtbs.fire_id = snow.fire_id
LEFT JOIN mtbs_ghcnd_snwd_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_mtbs_days} AS snwd
	ON mtbs.fire_id = snwd.fire_id
LEFT JOIN mtbs_ghcnd_t{summary_type_lowcase}_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_mtbs_days} AS tavg
	ON mtbs.fire_id = tavg.fire_id
LEFT JOIN mtbs_ghcnd_tmax_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_mtbs_days} AS tmax
	ON mtbs.fire_id = tmax.fire_id
LEFT JOIN mtbs_ghcnd_tmin_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_mtbs_days} AS tmin
	ON mtbs.fire_id = tmin.fire_id
LEFT JOIN mtbs_ghcnd_tobs_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_mtbs_days} AS tobs
	ON mtbs.fire_id = tobs.fire_id
LEFT JOIN mtbs_nldn_{summary_type_lowcase}_s{spat_deg_rad_str_nums}_t{max_lag_mtbs_days} AS nldn
	ON mtbs.fire_id = nldn.fire_id
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
#' @param mtbs_start_date (date) : The minimum date to filter MTBS fire data, we
#' will i.e. dataset will only contain MTBS fires after this date
#' @param max_lag_mtbs_days (integer) : The number of days for NOAA SWDI weather
#' data to lag behind the \code{mtbs_start_date}
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
#' wrap_gen_query_mtbs_full_feat_180d(mtbs_start_date =
#'                                       base::as.Date("2000-01-01"),
#'                                     max_lag_mtbs_days = 180,
#'                                     spat_deg_rad = 0.5,
#'                                     summary_type = "AVG",
#'                                     out_qry_dir = ".",
#'                                     ind_comb_qry = TRUE)
#' }
wrap_gen_query_mtbs_full_feat_180d <- function(mtbs_start_date =
                                                 base::as.Date("2000-01-01"),
                                               max_lag_mtbs_days = 180,
                                               spat_deg_rad = 0.5,
                                               summary_type = "AVG",
                                               out_qry_dir,
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
    out_gen_qry_name <- stringr::str_c(glue::glue("mtbs",
                                                  "full-feat",
                                                  "{summary_type_lowcase}",
                                                  "s{spat_deg_rad_str_nums}",
                                                  "t{max_lag_mtbs_days}",
                                                  .sep = "-"),
                                       ".sql")

    out_gen_qry_path <- fs::path_join(parts = c(out_qry_dir, out_gen_qry_name))
    print(out_gen_qry_path)

    summary_type %>%
      purrr::map_dfr(.x = .,
                     .f = ~backburner::gen_query_mtbs_full_feat_180d(mtbs_start_date =
                                                                        mtbs_start_date,
                                                                      max_lag_mtbs_days = max_lag_mtbs_days,
                                                                      spat_deg_rad = spat_deg_rad,
                                                                      summary_type = .x,
                                                                      out_qry_dir = out_qry_dir)) %>%
      dplyr::pull(.data = ., var = qry_str) %>%
      glue::glue_collapse(x = ., sep = "\n\n") %>%
      readr::write_lines(x = ., path = out_gen_qry_path)
  } else{
    # Write queries for each feature to individual files -----------------------
    summary_type %>%
      purrr::map_dfr(.x = .,
                     .f = ~backburner::gen_query_mtbs_full_feat_180d(mtbs_start_date =
                                                                        mtbs_start_date,
                                                                      max_lag_mtbs_days = max_lag_mtbs_days,
                                                                      spat_deg_rad = spat_deg_rad,
                                                                      summary_type = .x,
                                                                      out_qry_dir = out_qry_dir)) %>%
      dplyr::rename(x = qry_str, path = qry_path) %>%
      dplyr::rowwise(data = .) %>%
      purrr::pwalk(.l = ., .f = ~readr::write_lines(x = .x, path = .y))
  }
}
