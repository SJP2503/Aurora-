# Scrape PH House & Senate from Wikipedia and save CSVs to data/
library(rvest)
library(dplyr)
library(stringr)
library(janitor)
library(readr)

get_house_members <- function() {
  url <- "https://en.wikipedia.org/wiki/List_of_current_members_of_the_House_of_Representatives_of_the_Philippines"
  page <- read_html(url)
  tabs <- page |> html_nodes("table.wikitable")
  if (length(tabs) == 0) stop("No House wikitable found.")
  df <- tabs[[1]] |> html_table(fill = TRUE) |> clean_names()

  df |>
    rename_with(~ str_replace_all(., "^district$", "district_info")) |>
    mutate(
      name           = str_squish(as.character(name)),
      party          = if ("party" %in% names(.)) str_squish(as.character(party)) else NA_character_,
      district_info  = if ("district_info" %in% names(.)) str_squish(as.character(district_info)) else NA_character_,
      chamber        = "house",
      source         = "wikipedia",
      official_profile_url = NA_character_,
      last_updated   = Sys.Date()
    )
}

get_senators <- function() {
  url <- "https://en.wikipedia.org/wiki/List_of_current_senators_of_the_Philippines"
  page <- read_html(url)
  tabs <- page |> html_nodes("table.wikitable")
  if (length(tabs) == 0) stop("No Senate wikitable found.")
  df <- tabs[[1]] |> html_table(fill = TRUE) |> clean_names()

  nm <- names(df)
  name_col  <- nm[which(nm %in% c("senator","name"))[1]]
  party_col <- nm[which(nm %in% c("party","political_party"))[1]]

  df |>
    rename(name = !!name_col, party = !!party_col) |>
    mutate(
      name           = str_squish(as.character(name)),
      party          = str_squish(as.character(party)),
      district_info  = NA_character_,
      chamber        = "senate",
      source         = "wikipedia",
      official_profile_url = NA_character_,
      last_updated   = Sys.Date()
    )
}

save_legislators <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  write_csv(df, path)
  message("Saved: ", path)
}

main <- function() {
  house  <- get_house_members()
  senate <- get_senators()
  save_legislators(house,  "data/philippines_house_members.csv")
  save_legislators(senate, "data/philippines_senators.csv")
}

main()
