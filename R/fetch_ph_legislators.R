# Scrape PH House & Senate from Wikipedia and save CSVs to data/
library(rvest)
library(dplyr)
library(stringr)
library(janitor)
library(readr)
library(tibble)

safe_pick <- function(nm, choices) {
  hit <- nm[nm %in% choices]
  if (length(hit)) hit[1] else NA_character_
}

get_house_members <- function() {
  url <- "https://en.wikipedia.org/wiki/List_of_current_members_of_the_House_of_Representatives_of_the_Philippines"
  page <- read_html(url)
  tabs <- page |> html_nodes("table.wikitable")
  if (length(tabs) == 0L) stop("No House wikitable found: ", url, call. = FALSE)

  df <- tabs[[1]] |> html_table(fill = TRUE) |> clean_names()
  nm <- names(df)

  name_col  <- safe_pick(nm, c("name","representative","member","incumbent"))
  party_col <- safe_pick(nm, c("party","political_party"))
  dist_col  <- safe_pick(nm, c("district","district_info","constituency"))

  tibble(
    name          = str_squish(as.character(df[[name_col]])),
    party         = if (!is.na(party_col))  str_squish(as.character(df[[party_col]])) else NA_character_,
    district_info = if (!is.na(dist_col))   str_squish(as.character(df[[dist_col]]))  else NA_character_,
    chamber       = "house",
    source        = "wikipedia",
    official_profile_url = NA_character_,
    last_updated  = Sys.Date()
  ) |> distinct()
}

get_senators <- function() {
  url <- "https://en.wikipedia.org/wiki/List_of_current_senators_of_the_Philippines"
  page <- read_html(url)
  tabs <- page |> html_nodes("table.wikitable")
  if (length(tabs) == 0L) stop("No Senate wikitable found: ", url, call. = FALSE)

  df <- tabs[[1]] |> html_table(fill = TRUE) |> clean_names()
  nm <- names(df)

  name_col  <- safe_pick(nm, c("name","senator","member"))
  party_col <- safe_pick(nm, c("party","political_party"))

  tibble(
    name          = str_squish(as.character(df[[name_col]])),
    party         = if (!is.na(party_col)) str_squish(as.character(df[[party_col]])) else NA_character_,
    district_info = NA_character_,
    chamber       = "senate",
    source        = "wikipedia",
    official_profile_url = NA_character_,
    last_updated  = Sys.Date()
  ) |> distinct()
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
      - name: Session info on failure
        if: failure()
        run: |
          Rscript -e 'sessionInfo()'
          echo "--- repo tree ---"
          ls -R

