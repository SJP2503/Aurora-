# R: coa_findings.R
# Output: data/coa/coa_findings.csv (finding_id, entity_id, kind, ref, severity, doc_hash, url, detected_at)

library(dplyr)
library(readr)
library(stringr)
library(rvest)
library(janitor)
library(digest)

dir.create("data/coa", recursive = TRUE, showWarnings = FALSE)

# Configure known COA public listing pages for AAR/NDs (examples to be replaced with real links)
COA_INDEXES <- c(
  # "https://www.coa.gov.ph/reports/annual-audit-reports",   # sample placeholder
  # "https://www.coa.gov.ph/nd-listings"                     # sample placeholder
)

polite_get <- function(url) {
  ua <- httr::user_agent("AuroraDataBot/1.0 (contact: ops@example.com)")
  for (i in 1:3) {
    resp <- try(httr::GET(url, ua), silent = TRUE)
    if (inherits(resp, "response") && httr::status_code(resp) < 400) {
      return(read_html(httr::content(resp, "raw")))
    }
    Sys.sleep(2)
  }
  NULL
}

extract_links <- function(index_url) {
  pg <- polite_get(index_url)
  if (is.null(pg)) return(tibble())

  links <- html_nodes(pg, "a")
  hrefs <- html_attr(links, "href")
  texts <- html_text2(links)
  df <- tibble(url = hrefs, text = texts) |>
    filter(!is.na(url)) |>
    mutate(url = ifelse(startsWith(url, "/"), paste0(index_url, url), url)) |>
    distinct(url, .keep_all = TRUE)

  # crude filter: keep only PDFs or likely report pages
  df |> filter(grepl("\\.pdf($|\\?)", url, ignore.case = TRUE) | grepl("report|audit|nd|notice", url, ignore.case = TRUE))
}

fingerprint_url <- function(url) {
  # We hash the URL string; if you download binaries, hash raw bytes instead.
  paste0("sha256:", digest(url, algo = "sha256"))
}

normalize_finding <- function(row) {
  tx <- tolower(row$text)
  kind <- dplyr::case_when(
    grepl("notice of disallowance|\\bnd\\b", tx) ~ "ND_issued",
    grepl("annual audit report|aar", tx)        ~ "AAR_adverse",
    TRUE ~ "report"
  )
  severity <- ifelse(kind == "ND_issued", "high", "medium")

  tibble(
    finding_id = paste0("coa-", digest::digest(row$url, algo = "sha256")),
    entity_id  = NA_character_,         # to be resolved by MEG later
    kind       = kind,
    ref        = str_squish(row$text),
    severity   = severity,
    doc_hash   = fingerprint_url(row$url),
    url        = row$url,
    detected_at = Sys.Date()
  )
}

run_coa_findings <- function() {
  out <- bind_rows(purrr::map(COA_INDEXES, extract_links))
  if (!nrow(out)) {
    message("No COA links scraped (update COA_INDEXES).")
    readr::write_csv(tibble(), "data/coa/coa_findings.csv"); return(invisible(tibble()))
  }
  norm <- bind_rows(purrr::pmap(out, normalize_finding))
  readr::write_csv(norm, "data/coa/coa_findings.csv")
  message("Saved: data/coa/coa_findings.csv (", nrow(norm), " rows)")
  invisible(norm)
}

# Execute if called directly by Actions
if (sys.nframe() == 0) run_coa_findings()
