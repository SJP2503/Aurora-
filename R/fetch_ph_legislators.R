# Scrape PH House & Senate from Wikipedia and save CSVs to data/
suppressPackageStartupMessages({
  library(rvest)
  library(dplyr)
  library(stringr)
  library(janitor)
  library(readr)
  library(tibble)
})

# ---------- helpers ----------
name_keywords  <- c("name","member","representative","incumbent","legislator")
party_keywords <- c("party","political_party","partylist","party_list")
dist_keywords  <- c("district","district_info","constituency","representation")

score_table <- function(df) {
  nm <- tolower(names(df))
  # bonus for having a likely name column and enough rows
  has_name <- any(nm %in% name_keywords)
  as.integer(has_name) * 2L + nrow(df)
}

pick_best_table <- function(tables) {
  if (!length(tables)) return(NULL)
  dfs <- lapply(tables, function(tb) try(html_table(tb, fill = TRUE), silent = TRUE))
  dfs <- dfs[vapply(dfs, inherits, logical(1), what = "data.frame")]
  if (!length(dfs)) return(NULL)
  dfs <- lapply(dfs, clean_names)
  # score & choose
  scores <- vapply(dfs, score_table, integer(1))
  dfs[[ which.max(scores) ]]
}

safe_pick_col <- function(df, candidates, default_first = TRUE) {
  nm <- tolower(names(df))
  hit <- which(nm %in% candidates)
  if (length(hit)) return(hit[1])

  # Heuristic: pick the column that looks most like person names
  name_like <- function(v) {
    # two words of letters (allow punctuation)
    mean(grepl("\\b[A-Za-z][A-Za-z.'-]+\\s+[A-Za-z][A-Za-z.'-]+\\b", as.character(v)))
  }
  ratios <- vapply(df, name_like, numeric(1))
  if (max(ratios, na.rm = TRUE) >= 0.2) return(which.max(ratios))

  if (default_first) return(1L)  # fall back to the first column
  NA_integer_
}

# ---------- house ----------
get_house_members <- function() {
  url <- "https://en.wikipedia.org/wiki/List_of_current_members_of_the_House_of_Representatives_of_the_Philippines"
  page <- read_html(url)
  best <- pick_best_table(html_nodes(page, "table.wikitable"))
  if (is.null(best)) stop("No House wikitable found at: ", url)

  # choose columns robustly
  name_ix  <- safe_pick_col(best, name_keywords,  default_first = TRUE)
  party_ix <- {
    nm <- tolower(names(best))
    hit <- which(nm %in% party_keywords)
    if (length(hit)) hit[1] else NA_integer_
  }
  dist_ix  <- {
    nm <- tolower(names(best))
    hit <- which(nm %in% dist_keywords)
    if (length(hit)) hit[1] else NA_integer_
  }

  tibble(
    name          = str_squish(as.character(best[[name_ix]])),
    party         = if (!is.na(party_ix))  str_squish(as.character(best[[party_ix]])) else NA_character_,
    district_info = if (!is.na(dist_ix))   str_squish(as.character(best[[dist_ix]]))  else NA_character_,
    chamber       = "house",
    source        = "wikipedia",
    official_profile_url = NA_character_,
    last_updated  = Sys.Date()
  ) |>
    filter(!is.na(name), name != "") |>
    distinct()
}

# ---------- senate ----------
get_senators <- function() {
  url <- "https://en.wikipedia.org/wiki/List_of_current_senators_of_the_Philippines"
  page <- read_html(url)
  best <- pick_best_table(html_nodes(page, "table.wikitable"))
  if (is.null(best)) stop("No Senate wikitable found at: ", url)

  nm <- tolower(names(best))
  # name column by keywords or heuristic
  name_ix  <- {
    hit <- which(nm %in% c("name","senator","member"))
    if (length(hit)) hit[1] else safe_pick_col(best, character(0), default_first = TRUE)
  }
  party_ix <- {
    hit <- which(nm %in% party_keywords)
    if (length(hit)) hit[1] else NA_integer_
  }

  tibble(
    name          = str_squish(as.character(best[[name_ix]])),
    party         = if (!is.na(party_ix)) str_squish(as.character(best[[party_ix]])) else NA_character_,
    district_info = NA_character_,
    chamber       = "senate",
    source        = "wikipedia",
    official_profile_url = NA_character_,
    last_updated  = Sys.Date()
  ) |>
    filter(!is.na(name), name != "") |>
    distinct()
}

save_legislators <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  write_csv(df, path)
  message("Saved: ", path, " (", nrow(df), " rows)")
}

# ---------- main with fail-safe ----------
main <- function() {
  ok <- TRUE
  house <- senate <- tibble()

  tryCatch({
    house  <- get_house_members()
    senate <- get_senators()
  }, error = function(e) {
    message("WARNING: fetch_ph_legislators failed: ", conditionMessage(e))
    ok <<- FALSE
  })

  save_legislators(house,  "data/philippines_house_members.csv")
  save_legislators(senate, "data/philippines_senators.csv")

  if (!ok) {
    message("Fetch completed with warnings; wrote what was available.")
  }
}

main()
