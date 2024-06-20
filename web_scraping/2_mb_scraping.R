#___________________________________________________________________________####
#   Web Scraping: Scrape Data on German Military Bases                      ####

# This script scrapes all military base information for each military unit
# saved in the CSV file `web_scraping/mb_info_urls.csv`

library(tidyverse)
library(rvest)
library(foreach)
library(doParallel)

files <- list(
  input = list(
    info_urls = "web_scraping/mb_info_urls.csv"
  ),
  output = list(
    military_bases = "web_scraping/military_bases/mb"
  )
)

# Import info URLs
info_urls <- files$input$info_urls %>% 
  read_csv2(show_col_types = F) %>% 
  arrange(url_id)


#___________________________________________________________________________####
#   Loop Through Info URLs and Download HTMLs                               ####

download_loop <- foreach(
  idx = list.files("web_scraping/htmls_info/", pattern = "\\.html$") %>% 
    str_remove("\\.html$") %>% 
    as.numeric() %>% 
    setdiff(info_urls$url_id, .),
  .packages = c("tidyverse", "rvest")
) %do% {
  x <- filter(info_urls, url_id == idx)
  
  download.file(
    x$url,
    str_c("web_scraping/htmls_info/", x$url_id, ".html"), 
    quiet = T
  )
  
  T
}


#___________________________________________________________________________####
#   Loop Through Info URLs                                                  ####

# Note: If many URLs are accessed, reading the URLs may throw an exception,
# thereby interrupting the loop. To prevent this, only a subset of URLs are
# processed simultaneously - bit by bit. If an exception was thrown, the same 
# start/end indexes are tried again sometime later.

start_index <- 1
end_index <- 2000

# Add start and end index to the output file name
files$output$military_bases <- files$output$military_bases %>% 
  str_c("_", start_index, "-", end_index, ".csv")

# Initialize parallel backend; use only a fraction of available cores to prevent
# 503 errors
num_cores <- detectCores()/4
cl <- makeCluster(num_cores)
registerDoParallel(cl)

df_mb <- foreach(
  url = info_urls$url[start_index:end_index],
  .combine = bind_rows,
  .packages = c("tidyverse", "rvest")
) %dopar% {
  html <- read_html(url)
  
  df <- html %>% 
    html_elements("label") %>% 
    html_text2() %>% 
    str_trim() %>% 
    as_tibble() %>% 
    separate_longer_delim(value, delim = "\n") %>% 
    separate_wider_delim(
      value, 
      delim = ": ", 
      names_sep = "__", 
      too_few = "align_end"
    ) %>% 
    filter(!is.na(value__1)) %>% 
    mutate(across(everything(), str_trim)) %>% 
    pivot_wider(names_from = value__1, values_from = value__2)
  
  df$unit <- html %>% 
    html_elements(".card-description") %>% 
    html_text2() %>% 
    str_trim()
  
  df$url <- url
  
  df
}

# Stop the parallel backend
stopCluster(cl)


#___________________________________________________________________________####
#   Processing                                                              ####

# Join unit IDs and add current date
df_mb <- df_mb %>%
  left_join(
    select(info_urls, unit_id, url),
    by = "url"
  )

df_mb$access_date <- Sys.Date()

# In some instances, `value` from the column separation is separated into
# 3 instead of 2 columns. This is only the case when some additional information
# on either the base name or the address is given. For these scraped data,
# the code below concatenates the additional information with the corresponding
# column

if ("value__3" %in% names(df_mb)) {
  df_mb <- df_mb %>% 
    mutate(
      Anschrift = ifelse(
        !is.na(value__3) & 
          str_detect(Anschrift, "\\(") & 
          !str_detect(Anschrift, "\\)"),
        str_c(Anschrift, value__3, sep = " "),
        Anschrift
      ),
      Liegenschaftsbezeichnung = ifelse(
        !is.na(value__3) & 
          str_detect(Liegenschaftsbezeichnung, "\\(") & 
          !str_detect(Liegenschaftsbezeichnung, "\\)"),
        str_c(Liegenschaftsbezeichnung, value__3, sep = " "),
        Liegenschaftsbezeichnung
      )
    ) %>% 
    select(-value__3)
}

# Rename columns and add identifier and date
df_mb <- df_mb %>% 
  rename(
    gemeinde = `Politische Gemeinde`,
    base_name = Liegenschaftsbezeichnung,
    address = Anschrift,
    coordinate = Koordinate,
    formation_date = Aufstellung,
    disband_date = Auflösung,
    reloc_date = Verlegung,
    literature = Literatur
  ) %>% 
  relocate(
    unit_id,
    unit,
    base_name,
    gemeinde,
    address,
    coordinate,
    formation_date,
    disband_date,
    reloc_date,
    literature,
    url,
    access_date
  )


#___________________________________________________________________________####
#   Export                                                                  ####

df_mb %>% 
  write_csv2(files$output$military_bases)
