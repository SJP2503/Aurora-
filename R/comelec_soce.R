# R: comelec_soce.R
# Output: data/comelec/soce.csv (contributor, recipient, amount, filed_year, ref)

library(dplyr)
library(readr)
library(stringr)
library(janitor)
library(lubridate)

dir.create("data/comelec", recursive = TRUE, showWarnings = FALSE)

normalize_soce <- function(path = "data/input/comelec_soce_raw.csv") {
  if (!file.exists(path)) return(tibble())
  raw <- suppressMessages(read_csv(path, show_col_types = FALSE)) |> clean_names()

  nm <- names(raw)
  contr <- nm[which(nm %in% c("contributor","donor","contributor_name"))[1]]
  recip <- nm[which(nm %in% c("recipient","candidate","candidate_party"))[1]]
  amt   <- nm[which(nm %in% c("amount","amount_donated","value"))[1]]
  year  <- nm[which(nm %in% c("year","filed_year","election_year"))[1]]
  ref   <- nm[which(nm %in% c("ref","reference","doc","source"))[1]]

  out <- tibble(
    contributor = if (!is.na(contr)) raw[[contr]] else NA_character_,
    recipient   = if (!is.na(recip)) raw[[recip]] else NA_character_,
    amount      = suppressWarnings(as.numeric(gsub("[^0-9.]", "", as.character(raw[[amt]])))),
    filed_year  = suppressWarnings(as.integer(raw[[year]])),
    ref         = if (!is.na(ref)) as.character(raw[[ref]]) else NA_character_
  ) |>
    mutate(
      contributor = str_squish(as.character(contributor)),
      recipient   = str_squish(as.character(recipient)),
      filed_year  = dplyr::coalesce(filed_year, year(Sys.Date()))
    ) |>
    filter(!is.na(contributor), !is.na(recipient), !is.na(amount))

  distinct(out)
}

run_comelec_soce <- function() {
  soce <- normalize_soce()
  write_csv(soce, "data/comelec/soce.csv")
  message("Saved: data/comelec/soce.csv (", nrow(soce), " rows)")
  invisible(soce)
}

if (sys.nframe() == 0) run_comelec_soce()
