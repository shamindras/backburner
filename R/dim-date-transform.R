#' Creates a Date Dimension table i.e. \code{dim_date}. This is useful to
#' get date attributes for a sequence of dates as specified by the user. This
#' table can be used for joining attributes by date for existing tables and
#' also for subsetting tables by date where this is faster by joining rather
#' than by filtering.
#'
#' @param all_dates_start (date) : The starting date value for our date
#' dimension table. The default value is \code{as.Date("1981-01-01")}.
#' @param all_dates_end (date) : The ending date value for our date
#' dimension table. The default value is \code{as.Date("2025-01-01")}.
#'
#' @return (tibble) : The \code{dim_date} tibble to be uploaded to \code{postGIS}
#' for our purposes
#' @export
#'
#' @examples
#' \dontrun{
#' # Input start and end dates
#' ALL_DATES_START <- as.Date("1981-01-01")
#' ALL_DATES_END <- as.Date("2025-01-01")
#' out_dim_date <- get_transform_dim_date(all_dates_start = ALL_DATES_START,
#'                                        all_dates_end = ALL_DATES_END)
#'}
get_transform_dim_date <- function(all_dates_start = as.Date("1981-01-01"),
                                   all_dates_end = Sys.Date()){
    # Create sequence of dates
    all_dates <- base::seq.Date(from = all_dates_start,
                                to = all_dates_end,
                                by = "day")

    # Create dimension table with date attributes for all dates
    out_dim_date <- all_dates %>%
        tibble::enframe(x = ., value = "date", name = NULL) %>%
        dplyr::mutate(
            date_year       = base::as.integer(lubridate::year(x = date)),
            date_doy        = base::as.integer(lubridate::yday(x = date)),
            date_month      = base::as.integer(lubridate::month(x = date)),
            date_month_name = lubridate::month(x = date,
                                               label = TRUE,
                                               abbr = FALSE),
            date_month_name_abb = lubridate::month(x = date, label = TRUE, abbr = TRUE),
            date_day            = lubridate::day(x = date),
            date_day_name       = lubridate::wday(x = date,
                                                  label = TRUE,
                                                  abbr = FALSE),
            date_day_name_abb = lubridate::wday(x = date, label = TRUE, abbr = TRUE),
            date_week         = base::as.integer(lubridate::week(x = date)),
            date_isoweek      = base::as.integer(lubridate::isoweek(x = date)),
            date_us_season    = dplyr::case_when(
                date_month %in% 10:12 ~ "Fall",
                date_month %in% 1:3 ~ "Winter",
                date_month %in% 4:6 ~ "Spring",
                date_month %in% 7:9 ~ "Summer",
                TRUE ~ "err_us_season"
            ),
            date_cal_qtr      = base::as.integer(lubridate::quarter(x = date,
                                                                    with_year = FALSE,
                                                                    fiscal_start = 1)),
            date_cal_year_qtr = lubridate::quarter(x = date,
                                                   with_year = TRUE,
                                                   fiscal_start = 1),
            date_doq = base::as.integer(lubridate::qday(x = date)),
            date_fom = lubridate::floor_date(x = date, unit = "month"),
            date_lom = lubridate::ceiling_date(x = date, unit = "month") - 1,
            date_foy = lubridate::floor_date(x = date, unit = "year"),
            date_loy = lubridate::ceiling_date(x = date, unit = "year") - 1
        )
    base::return(out_dim_date)
}
