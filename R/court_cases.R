# R: court_cases.R
# Output: data/courts/case_events.csv (case_event_id, court, case_no, stage, outcome, date, ref, url)

library(dplyr)
library(readr)
library(stringr)
library(rvest)
library(janitor)
library(httr)
library(digest)
library(lubridate)
library(purrr)

dir.create("data/courts", recursive = TRUE, showWarnings = FALSE)

# TODO: put the actual public listing URLs you want here
COURT_INDEXES <- tibble::tibble(
  court = c(
    # "Ombudsman", "Sandiganbayan", "SC", "CA"
  ),
  url = c(
    # "https://<ombudsman decisions listing>",
    # "https://sb.judiciary.gov.ph/decisions",   # example placeholder
    # "https://sc.judiciary.gov.ph/decisions",
    # "https://ca.judiciary.gov.ph/decisions"
  )
)

polite_get <- function(url) {
  ua <- user_agent("AuroraDataBot/1.0 (contact: ops@example.com)")
  for (i in 1:3) {
    resp <- try(GET(url, ua), silent = TRUE)
    if (inherits(resp, "response") && status_code(resp) < 400) {
      return(read_html(content(resp, "raw")))
    }
    Sys.sleep(2)
  }
  NULL
}

extract_cases <- function(court, index_url) {
  pg <- polite_get(index_url); if (is.null(pg)) return(tibble())
  links <- html_nodes(pg, "a")
  hrefs <- html_attr(links, "href")
  texts <- html_text2(links)

  df <- tibble(url = hrefs, text = texts) |>
    filter(!is.na(url)) |>
    mutate(url = ifelse(startsWith(url, "/"), paste0(index_url, url), url)) |>
    distinct(url, .keep_all = TRUE) |>
    filter(grepl("\\.pdf($|\\?)", url, ignore.case = TRUE) |
           grepl("case|decision|resolution|docket|g.r.|crim|admin", text, ignore.case = TRUE))

  tibble(
    case_event_id = paste0("case-", digest(url, algo = "sha256")),
    court = court,
    case_no = str_extract(text, "(G\\.?R\\.?\\s*No\\.?\\s*[^,;\\s]+|SB-\\w+\\d+|CA-\\w+\\d+|OMB-\\w+\\d+)"),
    stage = NA_character_,
    outcome = NA_character_,
    date = suppressWarnings(as.Date(str_extract(text, "\\b\\d{4}-\\d{2}-\\d{2}\\b"))),
    ref = str_squish(text),
    url = url
  )
}

run_court_cases <- function() {
  if (!nrow(COURT_INDEXES)) { write_csv(tibble(), "data/courts/case_events.csv"); return(invisible(tibble())) }
  out <- bind_rows(pmap(COURT_INDEXES, extract_cases))
  write_csv(out, "data/courts/case_events.csv")
  message("Saved: data/courts/case_events.csv (", nrow(out), " rows)")
  invisible(out)
}

if (sys.nframe() == 0) run_court_cases()
