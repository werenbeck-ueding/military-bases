#___________________________________________________________________________####
#   Web Scraping: Info URLs of German Military Bases                        ####

# This script scrapes all Bundeswehr (German Army) military units from
# https://www.deutsche-militaerstandorte-nach1945.de/. These all contain
# information when and where they were stationed. From this, it is possible
# to infer which military bases closed/opened when and where.

library(tidyverse)
library(rvest)
library(foreach)
library(doParallel)

files <- list(
  output = list(
    info_urls = "web_scraping/mb_info_urls.csv"
  )
)

base_url <- "https://www.deutsche-militaerstandorte-nach1945.de/"


mb_bw_table_rows <- base_url %>% 
  str_c("view_standorte.cfm?art=1") %>% 
  read_html() %>% 
  html_elements(".card-description") %>% 
  html_text2() %>% 
  str_extract("[:digit:]{1,}") %>% 
  as.numeric()


#___________________________________________________________________________####
#   Get all Info URLs                                                       ####

# Store all table URLs that lead to the table showing all Bundeswehr military
# bases in a vector.

# Each page of the table shows 100 entries. Since the URL indicates a simple
# "start row" filter for the connected database, we can simply create an integer
# vector of start rows to loop through.
mb_bw_table_rows <- base_url %>% 
  str_c("view_standorte.cfm?art=1") %>% 
  read_html() %>% 
  html_elements(".card-description") %>% 
  html_text2() %>% 
  str_extract("[:digit:]{1,}") %>% 
  as.numeric()

mb_bw_table_urls <- seq(1, mb_bw_table_rows, by = 100) %>% 
  str_c(
    base_url,
    "view_standorte.cfm?art=1&startrow=", .,
    "&aktiv_page=1&sort_id=2&sort_ud=1"
  )


# Iterate through all table URLs and save each html locally (prevents connection
# timeout issues)
foreach(
  table_url_idx = seq_along(mb_bw_table_urls),
  .packages = c("tidyverse", "rvest")
) %do% {
  download.file(
    mb_bw_table_urls[table_url_idx], 
    str_c("web_scraping/htmls_table/", table_url_idx, ".html"),
    quiet = T
  )
  
  T
}


# Initialize parallel backend; use only a fraction of available cores to prevent
# 503 errors
num_cores <- detectCores() - 1
cl <- makeCluster(num_cores)
registerDoParallel(cl)


# Iterate through all table HTMLs and store the URLs leading to the details page 
# of the corresponding table entry along with object information in a data frame
mb_bw_tables <- foreach(
  html_file = list.files("web_scraping/htmls_table/", full.names = T),
  .combine = bind_rows,
  .packages = c("tidyverse", "rvest")
) %do% {
  html <- read_html(html_file)
  
  html_table <- html %>% 
    html_table() %>% 
    .[[1]] %>% 
    select(Dienststelle, Kurzbez., Ort, Bezeichnung)
  
  html_table$url <- html %>% 
    html_elements(".btn-outline-primary") %>% 
    html_attr("href") %>% 
    str_c(base_url, .)
  
  html_table
}


mb_bw_tables <- mb_bw_tables %>% 
  mutate(
    url_id = 1:n(),
    access_date = Sys.Date()
  ) %>% 
  select(
    url_id, unit = Kurzbez., base_name = Bezeichnung, city = Ort, url, 
    access_date
  )


#___________________________________________________________________________####
#   Export                                                                  ####

mb_bw_tables %>% 
  write_csv2(files$output$info_urls)
