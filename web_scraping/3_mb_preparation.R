#___________________________________________________________________________####
#   Web Scraping: Construct Table with Military Bases                       ####

library(tidyverse)
library(rvest)
library(foreach)
library(doParallel)

files <- list(
  input = list(
    html_path = "web_scraping/htmls_info/"
  ),
  output = list(
    mb_bundeswehr = "data/processed/mb_bundeswehr.csv"
  )
)


#___________________________________________________________________________####
#   Read / Pre-Process HTMLs                                                ####

html_files <- files$input$html_path %>% 
  list.files(pattern = "\\.html$", full.names = T)

# Initialize parallel backend
num_cores <- detectCores() - 1
cl <- makeCluster(num_cores)
registerDoParallel(cl)

df_mb_bundeswehr <- foreach(
  html_file = html_files,
  .combine = bind_rows,
  .packages = c("tidyverse", "rvest")
) %dopar% {
  html <- read_html(html_file)
  
  df <- html %>% 
    html_elements("label") %>% 
    html_text2() %>% 
    str_trim() %>% 
    as_tibble()
  
  df$unit <- html %>% 
    html_elements(".card-description") %>% 
    html_text2() %>% 
    str_trim()
  
  df$url_id <- html_file %>% 
    str_remove_all("[:alpha:]") %>% 
    str_remove_all("[:punct:]") %>% 
    as.numeric()
  
  df$html_file <- html_file
  
  df %>% 
    relocate(url_id, unit, value)
}

# Stop the parallel backend
stopCluster(cl)


#___________________________________________________________________________####
#   Tidy Military Units Data                                                ####

# Bring the data into a tidy structure with the following information per unit:
# - Politische Gemeinde
# - Liegenschaftsbezeichnung
# - Anschrift
# - Koordinate
# - Aufstellung
# - Auflösung
# - Verlegung
# - Literatur
# - Notiz
# - Stationierung (mind.)
df_bundeswehr_units <- df_mb_bundeswehr %>% 
  separate_longer_delim(value, delim = "\n") %>% 
  filter(value != "") %>% 
  mutate(value = str_trim(value)) %>% 
  separate(
    value, 
    into = c("key", "value"), 
    sep = ":",
    extra = "merge",
    fill = "left"
  ) %>% 
  mutate(
    key = ifelse(is.na(key), "Notiz", key),
    value = ifelse(
      !key %in% c(
        "Politische Gemeinde", "Liegenschaftsbezeichnung", "Anschrift",
        "Koordinate", "Aufstellung", "Notiz", "Auflösung", "Literatur",
        "Verlegung"
      ),
      str_c(key, ": ", value),
      value
    ),
    key = ifelse(
      !key %in% c(
        "Politische Gemeinde", "Liegenschaftsbezeichnung", "Anschrift",
        "Koordinate", "Aufstellung", "Notiz", "Auflösung", "Literatur",
        "Verlegung"
      ),
      "Notiz",
      key
    ),
    value = str_trim(value),
    key = ifelse(
      str_starts(value, "Am Standort mindestens"),
      "Stationierung (mind.)",
      key
    ),
    value = ifelse(
      key == "Stationierung (mind.)", 
      value %>% str_remove_all("Am Standort mindestens ") %>% str_remove("\\.$"),
      value
    )
  ) %>% 
  # Collapse notes
  summarise(
    value = str_flatten(value, collapse = " |\n"),
    .by = -value
  ) %>% 
  pivot_wider(names_from = key, values_from = value) %>% 
  relocate(
    url_id, unit, `Politische Gemeinde`, Liegenschaftsbezeichnung,
    Anschrift, Koordinate, Aufstellung, Verlegung, Auflösung, 
    `Stationierung (mind.)`, Notiz, Literatur
  )


# Merge a temporary military base name ID to the military units data set to
# be able to merge the real military base name back to the units once they
# are processed

df_bundeswehr_units <- df_bundeswehr_units %>% 
  select(Liegenschaftsbezeichnung, Koordinate) %>% 
  distinct() %>% 
  mutate(temp_mb_id = 1:n()) %>% 
  right_join(
    df_bundeswehr_units,
    by = c("Liegenschaftsbezeichnung", "Koordinate")
  )


#___________________________________________________________________________####
#   Military Bases                                                          ####

# Create data frame containing aliases of military base names. If the 
# coordinates match perfectly or the bases are very near to each other, these
# are presumably the same bases.

# Some locations are directly removed from the data set because these do not
# constitute military bases, such as diplomacies or government agencies that
# have soldiers working there.

# Anything containing "Dienstgebäude" (or misspellings "Dienstgeäbude"/
# "Dienstgeäude") is a service building such as the Bundeswehr career centers

# Storage:
# - Außenlager
# - Betriebsstoffaußenlager, Betriebsstofflager
# - [Gg]erätelager, Geräteaußenlager
# - Materialaußenlager, Materiallager
# - Munitionsaußenlager, Munitionsbehelfslager, Munitionslager
# - Sanitätsmaterialaußenlager, Sanitätsmateriallager
# - Sondermunitionslager
# - Sonderwaffenlager

# Depots:
# - Betriebsstoffdepot
# - Bundeswehrdepot, Bw-Depot
# - Depot
# - Ehem. Alabama-Depot?
# - Fernmeldedepot
# - Gerätedepot, Gerätehauptdepot
# - Hauptdepot
# - Heeresdepot
# - Korpsdepot
# - Luftwaffendepot, Luftwaffenmaterialdepot, Luftwaffenmunitionsdepot,
#   Luftwaffenmunitionshauptdepot, Lw-Depot
# - Marine Depot, Marinedepot, Marinematerialdepot, Marinemunitionsdepot
# - Materialdepot, Materialhauptdepot
# - MunDepot, Munitionsdepot, Munitionshauptdepot
# - Reservedepot
# - Sanitätsdepot, Sanitätshauptdepot
# - Teildepot
# - Virginia-Depot?

# TODO
# Brückenstellen im Datensatz lassen? Heutzutage vmtl. nicht mehr relevant

# TODO
# Was machen wir mit "Lager"? Das müssen nicht zwangsläufig Materialdepots o. ä.
# sein. In der NS-Zeit wurden diese Lager eingerichtet und danach teils von der
# Bundeswehr weitergenutzt, wenn auch nur kurz und vmtl. sporadisch. Es gibt
# allerdings auch Lager [für] [Üü]bende Truppe[n] - vielleicht frühe Truppen-
# übungsplätze? Diese Orte hatten aber vermutlich auch Kasernen...

# TODO
# Könnte man frühere NS-Truppenlager als hypothetische Controls nehmen? Die 
# sollten ja ähnliche räumliche "Amenities" bieten (in strategischer Hinsicht).
# Damit ließe sich die räumliche Endogenität vielleicht lösen.

# TODO
# Was zählen wir zu den Militärstandort, was nicht? Wollen wir bspw. Bundeswehr-
# krankenhäuser einbeziehen, oder nicht? Theoretisch sollten die auch zur 
# lokalen Wirtschaft beitragen, wäre aber kein trennscharfes Treatment ggü.
# "richtigen" Stützpunkten (Kasernen). Gleiche Überlegung zu Lehreinrichtungen
# der Bundeswehr (z. B. Bundeswehrfachschulen, Akademien etc.).

# TODO
# Mobilmachungsstützpunkte (MobStp) entfernen? Sollten eigentlich keine Relevanz
# haben. Heutzutage sollten sie außerdem größtenteils in oder direkt an Kasernen
# gelegen sein.

# TODO 
# Separater Datensatz nur mit Truppenübungsplätzen? TrÜPl bieten lediglich
# ein Disamenity, da hier kaum Arbeiten verrichtet werden (stimmt das?) und 
# nur der Verkehr durch Militärtransporte behindert wird.

# TODO
# Kreiswehrersatzamt (KWEA) entfernen

# TODO
# Liegenschaften außerhalb Deutschlands entfernen, allerdings erst nachdem ein
# Paneldatensatz der einzelnen Liegenschaften erstellt wurde, d. h. 
# unterschiedliche Koordinaten konsolidiert und Missings gefüllt wurden.

df_mb_raw <- df_bundeswehr_units %>% 
  select(
    temp_mb_id, 
    mb_name = Liegenschaftsbezeichnung,
    coords = Koordinate
  ) %>% 
  distinct() %>% 
  mutate(
    mb_note = str_extract(mb_name, "(?<=\\().*(?=\\))"),
    mb_name = str_remove(mb_name, "\\(.*"),
    .before = coords
  ) %>% 
  arrange(mb_name)

df_mb_raw %>% 
  arrange(mb_name) %>% 
  filter(
    str_detect(mb_name, "Bundesministerium", negate = T),
    str_detect(mb_name, "Bundesamt", negate = T),
    str_detect(mb_name, "Bundesinstitut", negate = T),
    str_detect(mb_name, "Bundesanstalt", negate = T),
    str_detect(mb_name, "Bundesbahndirektion", negate = T),
    str_detect(mb_name, "Botschaft", negate = T),
    str_detect(mb_name, "Materialamt", negate = T),
    str_detect(mb_name, "Logistikamt", negate = T),
  ) %>% 
  mutate(
    # Military training grounds
    training_area = ifelse(
      str_detect(mb_name, "TrÜbPl") | str_detect(mb_name, "Truppenübungsplatz"),
      1, 0
    ),
    # Mobilisation points for military reserve
    mobilisation_point = ifelse(str_detect(mb_name, "MobStp"), 1, 0),
    # Locations used for storage / as depot
    storage = ifelse(
      str_detect(mb_name, "[Aa]ußenlager") |
        str_detect(mb_name, "Betriebsstofflager") |
        str_detect(mb_name, "[Gg]erätelager") |
        str_detect(mb_name, "Materiallager") |
        str_detect(mb_name, "Munitionslager") |
        str_detect(mb_name, "Munitionsbehelfslager") |
        str_detect(mb_name, "Sanitätsmateriallager") |
        str_detect(mb_name, "Sondermunitionslager") |
        str_detect(mb_name, "Sonderwaffenlager"),
      1, 0
    ),
    depot = ifelse(str_detect(mb_name, "[Dd]epot"), 1, 0)
  )




df_mb_raw %>% 
  filter(str_detect(mb_name, "Bundes"), str_starts(mb_name, "Botschaft", negate = T)) %>% 
  select(mb_name) %>% 
  distinct() %>% 
  View()





df_mb_raw %>% 
  filter(str_detect(mb_name, "[TrÜbPl|Truppenübungsplatz]"))


filter(
  str_detect(mb_name, "[Ll]ager") & 
    str_detect(mb_name, "[Kk]aserne", negate = T)
) %>% 
  View()









library(sf)

sf_krs <- "data/raw/administrative_borders/VG5000_KRS.shp" %>% 
  st_read()

df_mb_raw %>% 
  filter(
    str_starts(mb_name, "Dienstge", negate = T) & 
      str_starts(mb_name, "Botschaft", negate = T) &
      str_starts(mb_name, "KWEA", negate = T) & # Kreiswehrersatzamt
      str_detect(mb_name, "MobStp", negate = T) & # Mobilmachungsstützpunkt
      str_detect(mb_name, "Bundeswehrkrankenhaus", negate = T) &
      str_detect(mb_name, "Bundeswehrlazarett", negate = T) &
      str_detect(mb_name, "Bundeswehrfachschule", negate = T) &
      str_starts(mb_name, "Bundes", negate = T) &
      str_detect(mb_name, "[Aa]ußenlager", negate = T) &
      str_detect(mb_name, "[Aa]ußenstelle", negate = T) &
      str_detect(mb_name, "[Dd]epot", negate = T)
  ) %>% 
  arrange(mb_name) %>% 
  # Convert to sf object to filter objects outside Germany
  separate(coords, c("lat", "lon"), ", ", convert = T) %>% 
  st_as_sf(coords = c("lon", "lat"), crs = "WGS84", na.fail = F, remove = F) %>% 
  st_transform(crs = st_crs(sf_krs)) %>% 
  st_filter(sf_krs, .predicate = st_covered_by) %>% 
  View()



