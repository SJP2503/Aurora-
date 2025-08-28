# R: fetch_saln_metadata.R
# Build SALN metadata JSON files for each legislator into saln_meta/

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(jsonlite)
  library(stringr)
})

`%||%` <- function(a, b) if (is.null(a)) b else a

create_saln_metadata <- function(df, chamber) {
  dir.create("saln_meta", showWarnings = FALSE, recursive = TRUE)
  df |>
    mutate(
      saln_id = paste0(str_replace_all(tolower(name), "[^a-z0-9]", ""), "_", chamber),
      saln_status = "unknown",
      saln_last_checked = as.character(Sys.Date()),
      saln_notes = NA_character_
    ) |>
    rowwise() |>
    do({
      meta <- list(
        saln_id = .$saln_id,
        name = .$name,
        chamber = chamber,
        party = .$party %||% NA,
        district = .$district_info %||% NA,
        official_profile_url = .$official_profile_url %||% NA,
        saln_status = .$saln_status,
        saln_last_checked = .$saln_last_checked,
        saln_notes = .$saln_notes
      )
      path <- file.path("saln_meta", paste0(.$saln_id, ".json"))
      write_json(meta, path, pretty = TRUE, auto_unbox = TRUE)
      tibble(path = path)
    })
}

main <- function() {
  if (!file.exists("data/philippines_house_members.csv") ||
      !file.exists("data/philippines_senators.csv")) {
    message("Legislator CSVs not found. Run fetch_ph_legislators.R first.")
    return(invisible(NULL))
  }
  house  <- read_csv("data/philippines_house_members.csv", show_col_types = FALSE)
  senate <- read_csv("data/philippines_senators.csv", show_col_types = FALSE)
  create_saln_metadata(house, "house")
  create_saln_metadata(senate, "senate")
  message("SALN metadata written to saln_meta/")
}

if (sys.nframe() == 0) main()
