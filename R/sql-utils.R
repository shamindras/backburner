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
                               "s{spat_deg_rad_str_nums}",
                               "t{max_lag_mtbs_days}",
                               .sep = "_")

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
                            msg = glue::glue("summary_type must be in upper case,
                                         it is currently {sfeat}"))
    assertthat::assert_that(base::is.character(x = out_qry_dir) &&
                                fs::dir_exists(path = out_qry_dir),
                            msg = glue::glue("out_qry_dir must be a valid path,
                                         it is currently {out_qry_dir}"))

    out_qry_path <- fs::path_join(parts = c(out_qry_dir,
                                            glue::glue("{out_tbl_name}.sql")))

    out_qry_str <-
        glue::glue("DROP TABLE IF EXISTS {out_tbl_name};

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
CREATE INDEX {out_tbl_name}_fire_id ON {out_tbl_name} USING HASH (fire_id);")

    out_val <- tibble::tibble(qry_str = out_qry_str, qry_path = out_qry_path)

    base::return(out_val)
}


mtbs_start_date = base::as.Date("2000-01-01")
max_lag_mtbs_days = 180
spat_deg_rad = 0.5
summary_type = "AVG"
sfeat = "TOBS"
out_qry_dir <- "."

sfeat_lowcase <- stringr::str_to_lower(string = sfeat)
spat_deg_rad_str <- base::format(base::round(spat_deg_rad, 2), nsmall = 1)
spat_deg_rad_str_nums <- stringr::str_replace(string = spat_deg_rad_str,
                                              pattern = "\\.",
                                              replacement = "")
summary_type_lowcase <- stringr::str_to_lower(string = summary_type)
summary_type_lowcase

out_tbl_name <- glue::glue("mtbs_ghcnd",
                           "{sfeat_lowcase}",
                           "s{spat_deg_rad_str_nums}",
                           "t{max_lag_mtbs_days}",
                           .sep = "_")
out_tbl_name

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
                        msg = glue::glue("summary_type must be in upper case,
                                         it is currently {sfeat}"))
assertthat::assert_that(base::is.character(x = out_qry_dir) && fs::dir_exists(path = out_qry_dir),
                        msg = glue::glue("out_qry_dir must be a valid path,
                                         it is currently {out_qry_dir}"))

out_qry_str <-
    glue::glue("DROP TABLE IF EXISTS {out_tbl_name};

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
CREATE INDEX {out_tbl_name}_fire_id ON {out_tbl_name} USING HASH (fire_id);")
out_qry_str

out_qry_path <- fs::path_join(parts = c(out_qry_dir,
                                        glue::glue("{out_tbl_name}.sql")))
tibble::tibble(qry_str = out_qry_str, qry_path = out_qry_path)
