# R: philgeps_awards.R
# Output: data/procurement/philgeps_awards.csv (award_id, agency, vendor, amount, date, philgeps_ref, flags)

library(dplyr)
library(readr)
library(stringr)
library(rvest)
library(janitor)
library(purrr)

dir.create("data/procurement", recursive = TRUE, showWarnings = FALSE)

# --------- (A) Example public mirror scraper (replace with real agency portals) ----------
get_agency_awards <- function(url) {
  pg <- try(read_html(url), silent = TRUE)
  if (inherits(pg, "try-error")) return(tibble())
  tabs <- html_nodes(pg, "table")
  if (!length(tabs)) return(tibble())

  df <- tabs[[1]] |>
    html_table(fill = TRUE) |>
    clean_names()

  # Heuristic column mapping
  nm <- names(df)
  agency_col <- nm[which(nm %in% c("agency","procuring_entity"))[1]]
  vendor_col <- nm[which(nm %in% c("supplier","vendor","awardee"))[1]]
  amount_col <- nm[which(nm %in% c("amount","contract_amount","abc"))[1]]
  date_col   <- nm[which(nm %in% c("date","award_date","posting_date"))[1]]

  out <- tibble(
    agency = if (!is.na(agency_col)) df[[agency_col]] else NA_character_,
    vendor = if (!is.na(vendor_col)) df[[vendor_col]] else NA_character_,
    amount = suppressWarnings(as.numeric(gsub("[^0-9.]", "", as.character(df[[amount_col]])))),
    date   = suppressWarnings(as.Date(df[[date_col]])),
    philgeps_ref = NA_character_
  ) |>
    mutate(
      award_id = paste0("aw-", digest::digest(paste(agency, vendor, amount, date, na.rm = TRUE), algo = "sha256")),
      flags = NA_character_
    )

  distinct(out)
}

# Example mirrors (replace with actual public URLs you can scrape safely)
AGENCY_URLS <- c(
  # "https://example.gov.ph/procurement/awards",
  # add more agency award-listing pages here
)

scrape_agencies <- function() {
  bind_rows(purrr::map(AGENCY_URLS, get_agency_awards))
}

# --------- (B) Fallback: normalize a raw CSV dropped into data/input/ ---------
normalize_fallback <- function(path = "data/input/philgeps_awards_raw.csv") {
  if (!file.exists(path)) return(tibble())
  raw <- suppressMessages(read_csv(path, show_col_types = FALSE)) |> clean_names()

  nm <- names(raw)
  agency_col <- nm[which(nm %in% c("agency","procuring_entity"))[1]]
  vendor_col <- nm[which(nm %in% c("supplier","vendor","awardee","winning_bidder"))[1]]
  amount_col <- nm[which(nm %in% c("amount","contract_amount","award_amount","contract_price"))[1]]
  date_col   <- nm[which(nm %in% c("date","award_date","notice_date","posting_date"))[1]]
  ref_col    <- nm[which(nm %in% c("philgeps_ref","reference_no","ref_no"))[1]]

  out <- tibble(
    agency = if (!is.na(agency_col)) raw[[agency_col]] else NA_character_,
    vendor = if (!is.na(vendor_col)) raw[[vendor_col]] else NA_character_,
    amount = suppressWarnings(as.numeric(gsub("[^0-9.]", "", as.character(raw[[amount_col]])))),
    date   = suppressWarnings(as.Date(raw[[date_col]])),
    philgeps_ref = if (!is.na(ref_col)) as.character(raw[[ref_col]]) else NA_character_
  ) |>
    mutate(
      award_id = paste0("aw-", digest::digest(paste(agency, vendor, amount, date, philgeps_ref, na.rm = TRUE), algo = "sha256")),
      flags = NA_character_
    )

  distinct(out)
}

run_philgeps_awards <- function() {
  agency_df <- scrape_agencies()
  fb_df     <- normalize_fallback()
  out <- bind_rows(agency_df, fb_df) |>
    filter(!is.na(vendor), !is.na(agency)) |>
    mutate(date = as.Date(date)) |>
    distinct(award_id, .keep_all = TRUE)

  readr::write_csv(out, "data/procurement/philgeps_awards.csv")
  message("Saved: data/procurement/philgeps_awards.csv (", nrow(out), " rows)")
  invisible(out)
}

# Execute if called directly by Actions
if (sys.nframe() == 0) run_philgeps_awards()
