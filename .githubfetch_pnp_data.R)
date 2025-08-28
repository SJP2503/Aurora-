# R Script: fetch_pnp_data.R
# Purpose: Scrape selected public PNP datasets (directory, most-wanted, regional officers, crime stats PDFs) 
# Output: CSVs under data/pnp/

library(rvest)
library(dplyr)
library(stringr)
library(readr)
library(janitor)
library(purrr)

safe_read_html <- purrr::safely(read_html)

# --- 1) PNP Directory (phones/emails/names if available) ---
get_pnp_directory <- function(url = "https://pnp.gov.ph/pnp-directory-2/") {
  pg <- safe_read_html(url)$result
  if (is.null(pg)) return(tibble())

  # Heuristic: pull tables and definition lists if present
  tabs <- html_nodes(pg, "table")
  out <- list()
  if (length(tabs) > 0) {
    out <- tabs |> map(~ .x |> html_table(fill = TRUE) |> clean_names())
  }
  df <- bind_rows(out, .id = "table_id") |> mutate(source = url, last_updated = Sys.Date())
  df
}

# --- 2) PNP Most Wanted (national page) ---
get_pnp_most_wanted <- function(url = "https://pnp.gov.ph/most-wanted-persons-with-reward/") {
  pg <- safe_read_html(url)$result
  if (is.null(pg)) return(tibble())

  # Try to extract images/cards and any tables
  cards <- html_nodes(pg, ".elementor-widget-container, table")
  # Parse text blocks
  txt <- cards |> html_text2() |> str_squish()
  tibble(text = txt, source = url, last_updated = Sys.Date()) |> filter(nchar(text) > 0)
}

# --- 3) Regional Key Officers (example pages vary; allow a vector of URLs) ---
get_regional_key_officers <- function(urls = c(
  "https://pro1.pnp.gov.ph/key-officers/",
  "https://pro6.pnp.gov.ph/about-us/police-regional-office-6-key-officers/",
  "https://pro11.pnp.gov.ph/command/"
)) {
  map_dfr(urls, function(u) {
    pg <- safe_read_html(u)$result
    if (is.null(pg)) return(tibble())
    # Extract any tables and lists
    tbls <- html_nodes(pg, "table")
    tdf <- if (length(tbls)) map_dfr(tbls, ~ html_table(.x, fill = TRUE) |> clean_names(), .id = "table_id") else tibble()
    # Fallback: scrape list items
    lis <- html_nodes(pg, "li") |> html_text2() |> str_squish()
    ldf <- if (length(lis)) tibble(list_item = lis) else tibble()
    bind_rows(
      tdf |> mutate(kind = "table"),
      ldf |> mutate(kind = "list")
    ) |> mutate(region_source = u, last_updated = Sys.Date())
  })
}

# --- 4) Crime Statistics PDFs (example: SPD/NCRPO monthly PDF) ---
# Provide known PDF URLs or index pages where links can be discovered.
get_crime_stats_spd <- function(index_url = "https://spd.ncrpo.pnp.gov.ph/crime-data-statistics/") {
  pg <- safe_read_html(index_url)$result
  if (is.null(pg)) return(tibble())
  links <- html_nodes(pg, "a")
  hrefs <- html_attr(links, "href")
  pdfs <- hrefs[grepl("\\\.pdf$", hrefs, ignore.case = TRUE)]
  tibble(pdf_url = unique(pdfs), source = index_url, last_updated = Sys.Date())
}

# --- Save helpers ---
save_csv <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  write_csv(df, path)
  message("Saved: ", path)
}

main <- function() {
  dir.create("data/pnp", recursive = TRUE, showWarnings = FALSE)

  pnp_dir <- get_pnp_directory()
  save_csv(pnp_dir, "data/pnp/pnp_directory.csv")

  pnp_mwp <- get_pnp_most_wanted()
  save_csv(pnp_mwp, "data/pnp/pnp_most_wanted_raw.csv")

  key_off <- get_regional_key_officers()
  save_csv(key_off, "data/pnp/pnp_regional_key_officers.csv")

  spd_pdfs <- get_crime_stats_spd()
  save_csv(spd_pdfs, "data/pnp/pnp_spd_crime_stats_pdfs.csv")
}

# Execute
main()
