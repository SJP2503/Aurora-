# R: dbm_releases.R
# Output: data/dbm/releases.csv (agency, amount, date, release_type, ref, release_id)

library(dplyr)
library(readr)
library(stringr)
library(janitor)
library(lubridate)
library(digest)

dir.create("data/dbm", recursive = TRUE, showWarnings = FALSE)

# Normalizes a raw DBM releases CSV you drop at data/input/dbm_releases_raw.csv
normalize_dbm <- function(path = "data/input/dbm_releases_raw.csv") {
  if (!file.exists(path)) return(tibble())
  raw <- suppressMessages(read_csv(path, show_col_types = FALSE)) |> clean_names()

  nm <- names(raw)
  agency <- nm[which(nm %in% c("agency","department","entity","procuring_entity"))[1]]
  amount <- nm[which(nm %in% c("amount","release_amount","obligation","allotment"))[1]]
  dt     <- nm[which(nm %in% c("date","release_date","posting_date"))[1]]
  rtype  <- nm[which(nm %in% c("release_type","type","uacs_type"))[1]]
  ref    <- nm[which(nm %in% c("ref","reference","doc","url","source"))[1]]

  out <- tibble(
    agency = if (!is.na(agency)) raw[[agency]] else NA_character_,
    amount = suppressWarnings(as.numeric(gsub("[^0-9.]", "", as.character(raw[[amount]])))),
    date   = suppressWarnings(as.Date(raw[[dt]])),
    release_type = if (!is.na(rtype)) raw[[rtype]] else NA_character_,
    ref    = if (!is.na(ref)) as.character(raw[[ref]]) else NA_character_
  ) |>
    mutate(
      agency = str_squish(as.character(agency)),
      release_id = paste0("dbm-", digest(paste(agency, amount, date, release_type, ref), algo = "sha256"))
    ) |>
    filter(!is.na(agency), !is.na(amount), !is.na(date))

  distinct(out)
}

run_dbm_releases <- function() {
  rel <- normalize_dbm()
  write_csv(rel, "data/dbm/releases.csv")
  message("Saved: data/dbm/releases.csv (", nrow(rel), " rows)")
  invisible(rel)
}

if (sys.nframe() == 0) run_dbm_releases()
