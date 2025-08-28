# R: philgeps_awards.R
# Output: data/procurement/philgeps_awards.csv (award_id, agency, vendor, amount, date, philgeps_ref, flags)

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(rvest)
  library(janitor)
  library(purrr)
  library(digest)
})

dir.create("data/procurement", recursive = TRUE, showWarnings = FALSE)

# -- ensure a stable schema so downstream steps never crash -------------------
ensure_award_schema <- function(df) {
  if (is.null(df) || !nrow(df)) {
    df <- tibble(
      award_id = character(),
      agency = character(),
      vendor = character(),
      amount = numeric(),
      date = as.Date(character()),
      philgeps_ref = character(),
      flags = character()
    )
  }
  # add missing columns with NA
  need <- c("award_id","agency","vendor","amount","date","philgeps_ref","flags")
  miss <- setdiff(need, names(df))
  if (length(miss)) {
    for (m in miss) df[[m]] <- NA
  }

  # coerce types lightly
  df <- df |>
    mutate(
      agency = as.character(agency),
      vendor = as.character(vendor),
      amount = suppressWarnings(as.numeric(amount)),
      date   = suppressWarnings(as.Date(date)),
      philgeps_ref = as.character(philgeps_ref),
      flags  = as.character(flags),
      award_id = as.character(award_id)
    )

  # generate award_id if missing
  df <- df |>
    mutate(
      award_id = ifelse(
        is.na(award_id) | award_id == "",
        paste0("aw-",
               digest(paste(agency %||% "", vendor %||% "",
                            amount %||% "", as.character(date) %||% "",
                            philgeps_ref %||% ""), algo = "sha256")),
        award_id
      )
    )
  df
}

`%||%` <- function(a, b) if (is.null(a)) b else a

# --------- (A) Example public mirror scraper (replace with real agency portals) ----------
get_agency_awards <- function(url) {
  pg <- try(read_html(url), silent = TRUE)
  if (inherits(pg, "try-error")) return(tibble())

  tabs <- html_nodes(pg, "table")
  if (!length(tabs)) return(tibble())

  df <- tabs[[1]] |>
    html_table(fill = TRUE) |>
    clean_names()

  nm <- names(df)
  agency_col <- nm[which(nm %in% c("agency","procuring_entity"))[1]]
  vendor_col <- nm[which(nm %in% c("supplier","vendor","awardee"))[1]]
  amount_col <- nm[which(nm %in% c("amount","contract_amount","abc","contract_price"))[1]]
  date_col   <- nm[which(nm %in% c("date","award_date","posting_date","notice_date"))[1]]

  out <- tibble(
    agency = if (!is.na(agency_col)) df[[agency_col]] else NA_character_,
    vendor = if (!is.na(vendor_col)) df[[vendor_col]] else NA_character_,
    amount = suppressWarnings(as.numeric(gsub("[^0-9.]", "", as.character(if (!is.na(amount_col)) df[[amount_col]] else NA)))),
    date   = suppressWarnings(as.Date(if (!is.na(date_col)) df[[date_col]] else NA))
  ) |>
    mutate(
      philgeps_ref = NA_character_,
      flags = NA_character_
    )

  ensure_award_schema(out)
}

# List any public agency pages you have permission to crawl
AGENCY_URLS <- c(
  # "https://example.gov.ph/procurement/awards"
)

scrape_agencies <- function() {
  if (!length(AGENCY_URLS)) return(tibble())
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
  ref_col    <- nm[which(nm %in% c("philgeps_ref","reference_no","ref_no","reference"))[1]]

  out <- tibble(
    agency = if (!is.na(agency_col)) raw[[agency_col]] else NA_character_,
    vendor = if (!is.na(vendor_col)) raw[[vendor_col]] else NA_character_,
    amount = suppressWarnings(as.numeric(gsub("[^0-9.]", "", as.character(if (!is.na(amount_col)) raw[[amount_col]] else NA)))),
    date   = suppressWarnings(as.Date(if (!is.na(date_col)) raw[[date_col]] else NA)),
    philgeps_ref = if (!is.na(ref_col)) as.character(raw[[ref_col]]) else NA_character_,
    flags  = NA_character_
  )

  ensure_award_schema(out)
}

run_philgeps_awards <- function() {
  agency_df <- ensure_award_schema(scrape_agencies())
  fb_df     <- ensure_award_schema(normalize_fallback())

  out <- bind_rows(agency_df, fb_df) |>
    # safe filter: columns exist due to ensure_award_schema()
    filter(!is.na(vendor) & vendor != "", !is.na(agency) & agency != "") |>
    mutate(date = suppressWarnings(as.Date(date))) |>
    distinct(award_id, .keep_all = TRUE)

  # if empty, still write a header so downstream steps do not fail
  if (!nrow(out)) {
    out <- out %>% slice(0)
  }
  write_csv(out, "data/procurement/philgeps_awards.csv")
  message("Saved: data/procurement/philgeps_awards.csv (", nrow(out), " rows)")
  invisible(out)
}

# Execute if called directly by Actions
if (sys.nframe() == 0) run_philgeps_awards()
