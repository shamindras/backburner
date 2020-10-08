#' Generate SQL code for separate indexed NOAA GHCN tables for each feature
#' e.g. PRCP for precipitation
#'
#' @param sfeat (character) : The NOAA GHCN feature to create a separate table
#' for e.g \code{"PRCP", "SNOW", "SNWD", "TMAX", "TMIN", "TOBS", "TAVG"}
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#' @param out_tbl_name_pfx (character) : The default table name prefix to take.
#' This will define prefix the name of the output table and also the output
#' query file name. In this case a suitable default value is
#' "ghcnd_observations"
#' @param fire_base_tbl_name  (character) : The table name for the GHCN-D
#' observations table. In this case a suitable default value is
#' "ghcnd_observations"
#'
#' @return (tibble) : A \code{tibble} with the query string, file path to store
#' the query, and a check of whether the filepath exists
#' @export
#'
#' @examples
#' \dontrun{
#' library("tidyverse")
#' gen_query_ghcn_base_sfeat(sfeat = "PRCP",
#'                           out_qry_dir = ".",
#'                           out_tbl_name_pfx = "ghcnd_observations",
#'                           fire_base_tbl_name = "ghcnd_observations")
#' }
gen_query_ghcn_base_sfeat <- function(sfeat,
                                      out_qry_dir,
                                      out_tbl_name_pfx,
                                      fire_base_tbl_name){

    # Key transformations for building query string ----------------------------
    # Feature type
    sfeat_lowcase <- stringr::str_to_lower(string = sfeat)
    sfeat_uppcase <- stringr::str_to_upper(string = sfeat)

    # Our SQL table name i.e. mtbs_ghcnd_tobs_s05_t180
    # for TOBS feature, 0.5 spatial degrees, 180 days
    out_tbl_name <- glue::glue("{out_tbl_name_pfx}",
                               "{sfeat_lowcase}",
                               .sep = "_")

    # Query name should be separated by "-" instead of underscores
    out_qry_name <- stringr::str_replace_all(string = out_tbl_name,
                                             pattern = "_",
                                             replacement = "-")

    # Input validation ---------------------------------------------------------
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
(SELECT ghobs.record_dt,
        ghobs.{glue::double_quote(sfeat_uppcase)},
        ST_SetSRID(ghobs.location, 4326) AS location
 FROM {fire_base_tbl_name} ghobs
 WHERE ghobs.{glue::double_quote(sfeat_uppcase)} IS NOT NULL);

/* Index this table on location and record_dt */
CREATE INDEX {out_tbl_name}_location ON {out_tbl_name} USING GIST (location);
CREATE INDEX {out_tbl_name}_dt ON {out_tbl_name} (record_dt);

/* End query to create {out_tbl_name} ----------------------------------*/")

    out_val <- tibble::tibble(qry_str = out_qry_str, qry_path = out_qry_path)

    base::return(out_val)
}

#' Wrapper function to generate a single combined or separate \code{SQL} files
#' for individual feature NOAA SWDI lagged aggregated time summaries mapped to
#' individual MTBS fires by specified spatial degrees over a 180 day lag period
#' for each fire
#'
#' @param sfeat (character) : The NOAA GHCN features to create a separate table
#' for e.g \code{c("PRCP", "SNOW", "SNWD", "TMAX", "TMIN", "TOBS", "TAVG")}.
#' In this wrapper the user should pass in a vector of all the features that
#' are contained in the GHCN-D table. This will generate combined or separate
#' queries to generate an individual table for each such GHCN-D feature
#' @param out_qry_dir (character) : The directory to save the query. A full
#' file path will be generated as part of the output
#' @param out_tbl_name_pfx (character) : The default table name prefix to take.
#' This will define prefix the name of the output table and also the output
#' query file name. In this case a suitable default value is
#' "ghcnd_observations"
#' @param fire_base_tbl_name  (character) : The table name for the GHCN-D
#' observations table. In this case a suitable default value is
#' "ghcnd_observations"
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
#' wrap_gen_query_ghcn_base_sfeat(sfeat = c("PRCP", "SNOW", "SNWD", "TMAX", "TMIN", "TOBS", "TAVG"),
#'                                out_qry_dir = ".",
#'                                out_tbl_name_pfx = "ghcnd_observations",
#'                                fire_base_tbl_name = "ghcnd_observations",
#'                                ind_comb_qry = TRUE)
#' }
wrap_gen_query_ghcn_base_sfeat <- function(sfeat_types,
                                           out_qry_dir,
                                           out_tbl_name_pfx,
                                           fire_base_tbl_name,
                                           ind_comb_qry = TRUE){

    # Key transformations for building query string ----------------------------
    # Feature type
    sfeat_lowcase <- stringr::str_to_lower(string = sfeat_types)

    if(ind_comb_qry){
        # Write combined query as a single file ------------------------------------

        # Our SQL table name i.e. mtbs_nldn_avg_s05_t180
        # for TOBS feature, 0.5 spatial degrees, 180 days
        out_gen_qry_name <- stringr::str_c(glue::glue("{out_tbl_name_pfx}",
                                                      "all-feat",
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
                           .f = ~backburner::gen_query_ghcn_base_sfeat(
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
                           .f = ~backburner::gen_query_ghcn_base_sfeat(sfeat = .x,
                                                                       out_qry_dir = out_qry_dir,
                                                                       out_tbl_name_pfx = out_tbl_name_pfx,
                                                                       fire_base_tbl_name = fire_base_tbl_name)) %>%
            dplyr::rename(x = qry_str, path = qry_path) %>%
            dplyr::rowwise(data = .) %>%
            purrr::pwalk(.l = ., .f = ~readr::write_lines(x = .x, path = .y))
    }
}
