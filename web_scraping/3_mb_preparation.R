#___________________________________________________________________________####
#   Web Scraping: Construct Table with Military Bases                       ####

library(tidyverse)
library(rvest)
library(foreach)
library(doParallel)
library(sf)
library(stringdist)

files <- list(
  input = list(
    html_path = "web_scraping/htmls_info/",
    krs = "data/raw/administrative_borders/VG5000_KRS.shp",
    gem = "data/raw/administrative_borders/VG5000_GEM.shp"
  ),
  output = list(
    mb_bundeswehr = "data/processed/mb_bundeswehr.csv"
  )
)

# Use UTM coordinate reference system
utm_crs <- "EPSG:4258"

sf_krs <- files$input$krs %>% 
  st_read() %>% 
  st_transform(crs = utm_crs) %>% 
  select(krs_id = AGS, krs_name = GEN)

sf_gem <- files$input$gem %>% 
  st_read() %>% 
  st_transform(crs = utm_crs) %>% 
  select(gem_id = AGS, gem_name = GEN) %>% 
  st_make_valid()


#___________________________________________________________________________####
#   Read / Pre-Process HTMLs                                                ####

html_files <- files$input$html_path %>% 
  list.files(pattern = "\\.html$", full.names = T)

# Initialize parallel backend
num_cores <- detectCores() - 1
cl <- makeCluster(num_cores)
registerDoParallel(cl)

df_html <- foreach(
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
df_bundeswehr_units <- df_html %>% 
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
  ) %>% 
  rename(
    municipality = `Politische Gemeinde`,
    mb_name = Liegenschaftsbezeichnung,
    address = Anschrift,
    coords = Koordinate
  ) %>% 
  # Convert to sf object to filter objects outside Germany, i. e. coordinates 
  # are given but no county could be joined
  separate(coords, c("lat", "lon"), ", ", convert = T, remove = F) %>% 
  st_as_sf(coords = c("lon", "lat"), crs = "WGS84", na.fail = F, remove = F) %>% 
  st_transform(crs = utm_crs) %>%
  st_join(sf_krs) %>% 
  filter(!(is.na(krs_id) & !st_is_empty(.))) %>% 
  # Join current municipality
  st_join(sf_gem) %>% 
  st_drop_geometry() %>% 
  # The provided municipality names are inconsistent and therefore replaced with
  # the current municipality based on coordinates. If coordinates are missing,
  # the municipality is inserted in the joined current municipality name if it
  # is found in the official data. The remaining missings are manually replaced.
  # Afterwards, municipality and county IDs are joined.
  mutate(
    across(where(is.character), ~ str_replace_all(., "nicht bekannt", "")),
    municipality = ifelse(municipality == "", gem_name, municipality),
    gem_id = ifelse(
      is.na(gem_name) & municipality %in% sf_gem$gem_name,
      map_chr(
        municipality, 
        ~ {
          out <- sf_gem$gem_id[sf_gem$gem_name == .]
          
          ifelse(length(out) == 1, out, NA)
        }
      ),
      gem_id
    ),
    gem_name = ifelse(
      is.na(gem_name) & municipality %in% sf_gem$gem_name,
      municipality, 
      gem_name
    )
  ) %>% 
  mutate(
    gem_id = case_when(
      municipality == "Altensalzwedel" & is.na(gem_name) ~ 
        "15081026", # Apenburg-Winterfeld
      municipality == "Altensalzwedel" & is.na(gem_name) ~ 
        "15081026", # Apenburg-Winterfeld
      municipality == "Frankenberg/Sachsen" & is.na(gem_name) ~ 
        "14522150", # Frankenberg/Sa.
      municipality == "Velen-Ramsdorf" & is.na(gem_name) ~ 
        "05554064", # Velen
      municipality == "Halver-Schwenke" & is.na(gem_name) ~ 
        "05962012", # Halver
      municipality == "Porta Westfalica-Nammen" & is.na(gem_name) ~ 
        "05770032", # Porta Westfalica
      municipality == "Gerolstein-Gees" & is.na(gem_name) ~ 
        "07233026", # Gerolstein
      municipality ==  "Koblenz-Bubenheim"  & is.na(gem_name) ~ 
        "07111000", # Koblenz
      municipality ==  "Stavenhagen/Basepohl"  & is.na(gem_name) ~ 
        "13071142", # Stavenhagen
      str_starts(municipality, "Bannesdorf")  & is.na(gem_name) ~ 
        "01055046", # Fehmarn
      municipality == "Büchel" & str_detect(mb_name, "NATO-Flugplatz") ~ 
        "07135018", # Büchel in Rhineland-Palatine
      municipality == "Roth" & str_detect(mb_name, "Otto-Lilienthal-Kaserne") ~ 
      "09576143", # Roth
      T ~ gem_id
    )
  ) %>% 
  # Filter remaining location at "Moskau, Russische Förderation" (Moscow, RU)
  filter(municipality != "Moskau, Russische Förderation") %>% 
  select(-gem_name, -krs_id, -krs_name) %>% 
  left_join(st_drop_geometry(sf_gem), by = "gem_id") %>% 
  mutate(krs_id = str_sub(gem_id, 1, 5)) %>% 
  left_join(st_drop_geometry(sf_krs), by = "krs_id") %>% 
  # Collect notes provided in the names (in parentheses) in a separate column
  mutate(
    mb_note = str_extract(mb_name, "(?<=\\().*(?=\\))"),
    mb_name = str_remove(mb_name, "\\(.*"),
    .before = coords
  )


# To reduce inconsistencies in the naming convention, the following steps are
# taken:
# - Anything that is a storage or depot is renamed to "Material-/
#   Munitionsdepot <`gem_name`>"
df_temp <- df_bundeswehr_units %>% 
  mutate(
    mb_name = mb_name %>% 
      str_remove("[Ee]hem.") %>% 
      str_remove("^[Ee]hemalige") %>% 
      str_remove("^[Ee]hemaliger") %>% 
      str_remove("^[Ee]hemaliges"),
    new_mb_name = case_when(
      # Consistent storage/depot names
      str_detect(mb_name, "[Aa]ußenlager") |
        str_detect(mb_name, "Betriebsstofflager") |
        str_detect(mb_name, "[Gg]erätelager") |
        str_detect(mb_name, "Materiallager") |
        str_detect(mb_name, "Munitionslager") |
        str_detect(mb_name, "Munitionsbehelfslager") |
        str_detect(mb_name, "Munitionsversorgungszentrum") |
        str_detect(mb_name, "Sanitätsmateriallager") |
        str_detect(mb_name, "Sondermunitionslager") |
        str_detect(mb_name, "Sonderwaffenlager") |
        str_detect(mb_name, "[Dd]epot") ~ 
        str_c("Material-/Munitionslager", gem_name, sep = " "),
      # Consistent "Lager" names
      str_starts(mb_name, "Lager") ~ str_c("Lager", gem_name, sep = " "),
      # Hospitals
      str_detect(mb_name, "Bundeswehrkrankenhaus") |
        str_detect(mb_name, "Bundeswehrlazarett") ~ 
        str_c("Bundeswehrkrankenhaus", gem_name, sep = " "),
      # Kreiswehrersatzämter
      str_detect(mb_name, "KWEA") |
        str_detect(mb_name, "Kreiswehrersatzamt") ~ 
        str_c("KWEA", gem_name, sep = " "),
      # Career centres of the Bundeswehr
      str_detect(unit, "KarrC") & str_detect(mb_name, "Dienstgebäude") ~
        str_c("Karrierecenter", gem_name, sep = " "),
      T ~ mb_name
      #.default = ""
    ),
    .after = mb_name
  )


#_______________________________________________________________________________
# TODO
#_______________________________________________________________________________

# Truppenübungsplätze
# - Truppenübungsplatz konsequent mit TrÜbPl abkürzen
# - Umbenennen in "TrÜbPl <gem_name>", aber nur wenn es pro Gemeinde immer nur 
#   einen TrÜbPl gibt, wovon ich ausgehe
# - Es gibt auch Standortübungsplätze. Die sollten aber eigentlich immer direkt
#   an der Kaserne sein und somit eher zum Kasernengelände gehören. Ähnlich zu
#   den Kasernen mit Heeresflugplatz bietet es sich hier vermutlich an, einen
#   Indikator zu basteln für die Präsenz eines Standortübungsplatzes (kann eine
#   Art Proxy für die Größe des Standorts sein). Müsste mal geprüft werden, was
#   Sinn ergibt, bspw. durch Berechnen durchschnittliche Distanz zur nächsten
#   Kaserne.
# - Sonderübungsplätze (z. B. für Pioniere) sollten eigentlich auch einfach nur
#   TrÜbPl sein (prüfen)
# - Übungsgelände gleichzusetzen mit TrÜbPl?

# Truppenlager
# - Recherche: Sind Truppenlager zwangsläufig "Lager"? Sollte es sich dabei um
#   Truppenlager aus der NS-Zeit handeln, die in den ersten Jahren der BRD
#   weiter unterhalten wurden, sollte einheitlich Truppenlager für den Namen
#   verwendet werden
# - Alle Lager in "Truppenlager <gem_name>" umbenennen? Was wäre hier sinnvoll

# Mobilmachungsstützpunkte (MobStp)
# - Umbenennen in "MobStp <mb_name ohne MobStp>"
# - Häufig bei Kasernen ist MobStp hinter dem Kasernennamen als ", MobStp". Hier
#   die Endung entfernen und das MobStp voransetzen
# - Teilweise finden sich noch weitere Namensbestandteile hinter MobStp, wenn es
#   am Ende des Namens steht. Hier prüfen, ob es Sinn ergibt, die immer zu 
#   streichen

# Kasernennamen
# - Auf korrekte Rechtschreibung / einheitliche Bezeichnung prüfen
# - Namensänderungen wie z. B. ", Gebäude 15"
# - Teilweise ist nur "Kaserne" angegeben. Über Zeitraum und Gemeinde sollte 
#   sich der Kasernenname herausfinden lassen
# - Teilweise sind Kasernen auch direkt z. B. Heeresflugplätze. Ich würde gerade
#   annehmen, dass man die Bezeichnung des Heeresflugplatzes streicht und nur
#   den Kasernennamen behält. Vielleicht müsste man später noch einen Indikator
#   einbauen, dass die Kaserne über einen FLugplatz verfügt.

# Fliegerhorste / Flugplätze
# - Einheitliche Bezeichnung als "Fliegerhorst <gem_name>"
# - Aufpassen, z. B. gibt es eine Fliegerhorstkirche. Ich nehme mal an, dass
#   damit kein Fliegerhorst gemeint ist
# - Flugplätze analog handhaben
# - Teilweise findet sich die Bezeichnung "Militärflugplatz". Die auch in Flug-
#   platz umbenennen

# Koordinaten vereinheitlichen
# In dem Datensatz besteht das Problem, dass die Koordinaten trotz gleicher
# Liegenschaft teilweise voneinander abweichen. Bei gleicher Liegenschaft
# sollte hier vermutlich ein gewichtetes Mittel der angegebenen Koordinaten
# verwendet werden. Eine weitere Schwierigkeit mit dem Datensatz ist, dass sich
# Kasernenbezeichnungen über die Jahre geändert haben. Um dennoch ein Datensatz
# mit Anfangs-/Endjahr einer Liegenschaft erstellen zu können, müsste man die
# Beobachtungen irgendwie räumlich clustern und so zusammenfassen. Als Name der
# Liegenschaft sollte dann der neueste verwendet werden. Aliases (ältere Namen)
# sollten wir dennoch in einer Spalte mitführen. Eventuell könnte man so auch
# nahe Munitions-/Materiallager einer Kaserne zuordnen.

# Anmerkungen für später
# - StOV steht für Standortverwaltung. Heute sollten das eigentlich Bundeswehr
#   dienstleistungszentren sein (muss geprüft werden). Für Konsistenz der 
#   Namensgebung entsprechend umbenennen?
# - Sind Standortmunitionsniederlagen gleichzusetzen mit Material-/Munitions-
#   lager?
# - Wie gehen wir mit Standortschießanlagen um? Vermutlich einfach umbenennen
#   in "Standortschießanlage <gem_name>" oder aber Kasernenname
# - Truppendienstgerichte sollten selten genug sein, dass "Truppendienstgericht
#   <gem_name>" ausreicht
# - Wie handhaben wir "Truppenunterkunft"?
# - Truppenübungsplatzkommandatur sollte irrelevant sein, Truppenübungsplatz
#   sollte ausreichend sein
# - Was ist "WBBeklA" und andere Ablürzungen? Vielleicht Glossar erstellen






# Nur ein kleiner Code, um sich alle einzigartigen Liegenschaftsnamen 
# anzuschauen
df_temp %>% 
  summarize(n_obs = n(), .by = c(new_mb_name, gem_name, coords)) %>% 
  distinct() %>% 
  arrange(new_mb_name) %>% 
  View()

















#___________________________________####
#   ALTER UNWICHTIGER CODE          ####
#___________________________________####




separate(
  address, c("street", "city"), ", ", remove = F, fill = "right"
) %>% 
  mutate(
    across(c(street, city), ~ str_replace(., "nicht bekannt", "")),
    plz = str_extract(city, "[:digit:]{5,5}"),
    city = str_trim(str_remove_all(city, "[:digit:]"))
  )
  
  df_bundeswehr_units %>% 
    select(address, street, plz, city) %>% 
    distinct() %>% 
    View()


df_bundeswehr_units$address[1:5]


df_bundeswehr_units %>% 
  filter(if_any(where(is.character), ~ str_detect(., "nicht bekannt"))) %>% 
  View()


stringdistmatrix(
  df_bundeswehr_units$municipality[1:50],
  sf_gem$gem_name[1:50],
  method = "lv"
) %>% 
  apply()


df_bundeswehr_units %>% 
  select(municipality, coords, gem_name, krs_name) %>% 
  arrange(municipality) %>% 
  distinct() %>% 
  filter(is.na(coords)) %>% 
  View()
  mutate(
    municipality = ifelse(municipality == "", gem_name, municipality),
    gem_name = ifelse(
      is.na(gem_name) & municipality %in% sf_gem$gem_name, 
      municipality, 
      NA
    )
  ) %>% 
  # Clean municipality names
  mutate(
    municipality = str_replace_all(municipality, " - ", "-"),
    municipality = case_when(
      municipality == "Altenstadt" & krs_name == "Weilheim-Schongau" ~
        "Altenstadt (Oberbayern)"
      municipality %in% c("Altenstadt bei Schongau", "Altenstadt/Schongau") ~
        "Altenstadt (Oberbayern)"
    )
  )








df_bundeswehr_units %>% 
  #select(municipality, coords) %>% 
  #arrange(municipality) %>% 
  #distinct() %>% 
  separate(coords, c("lat", "lon"), ", ", convert = T) %>% 
  st_as_sf(coords = c("lon", "lat"), crs = "WGS84", na.fail = F, remove = F) %>% 
  st_transform(crs = utm_crs) %>% 
  st_join(sf_krs) %>% 
  filter(!(is.na(krs_id) & !st_is_empty(.))) %>% 
  st_join(sf_gem)







# Merge a temporary military base name ID to the military units data set to
# be able to merge the real military base name back to the units once they
# are processed

df_bundeswehr_units <- df_bundeswehr_units %>% 
  select(`Politische Gemeinde`, Liegenschaftsbezeichnung, Koordinate) %>% 
  distinct() %>% 
  arrange(`Politische Gemeinde`, Liegenschaftsbezeichnung) %>% 
  mutate(temp_mb_id = 1:n()) %>% 
  right_join(
    df_bundeswehr_units,
    by = c("Politische Gemeinde", "Liegenschaftsbezeichnung", "Koordinate")
  ) %>% 
  arrange(`Politische Gemeinde`, Liegenschaftsbezeichnung)


#___________________________________________________________________________####
#   Data Cleaning                                                           ####

# The `df_bundeswehr_units` data set implicitly contains all military locations
# of the Bundeswehr. However, an identifier for single locations is missing
# and names and coordinates are not always consistent. To make these consistent,
# Levenshtein and Euclidean distance matrices are calculated. These are then
# used to define whether two observations belong to the same location or not.

# First collect are distinct combinations of municipality, location name and 
# coordinates
df_temp <- df_bundeswehr_units %>% 
  select(
    municipality = `Politische Gemeinde`,
    temp_mb_id,
    mb_name = Liegenschaftsbezeichnung,
    coords = Koordinate
  ) %>% 
  summarize(
    n_obs = n(),
    .by = c(municipality, temp_mb_id, mb_name, coords)
  ) %>% 
  # Collect notes provided in the names (in parentheses) in a separate column
  mutate(
    municipality_note = str_extract(municipality, "(?<=\\().*(?=\\))"),
    municipality = str_remove(municipality, "\\(.*"),
    mb_note = str_extract(mb_name, "(?<=\\().*(?=\\))"),
    mb_name = str_remove(mb_name, "\\(.*"),
    .before = coords
  )


# Convert to sf object to filter objects outside Germany, i. e. coordinates are
# given but no county could be joined
sf_temp <- df_temp %>% 
  separate(coords, c("lat", "lon"), ", ", convert = T) %>% 
  st_as_sf(coords = c("lon", "lat"), crs = "WGS84", na.fail = F, remove = F) %>% 
  st_transform(crs = utm_crs) %>% 
  st_join(sf_krs) %>% 
  filter(!(is.na(krs_id) & !st_is_empty(.))) %>% 
  st_join(sf_gem)


##__________________________________________________________________________####
##  Levenshtein Distance                                                    ####

# To reduce inconsistencies in the naming convention, the following steps are
# taken:
# - Anything that is a storage or depot is renamed to "Material-/
#   Munitionsdepot <`municipality`>"


sf_temp %>% 
  # Clean municipality names
  mutate(
    municipality = str_replace_all(municipality, " - ", "-"),
    municipality = case_when(
      municipality == "Altenstadt" & krs_name == "Weilheim-Schongau" ~
        "Altenstadt (Oberbayern)"
      municipality %in% c("Altenstadt bei Schongau", "Altenstadt/Schongau") ~
        "Altenstadt (Oberbayern)"
    )
  ) %>%
  mutate(
    new_mb_name = case_when(
      # Consistent storage/depot names
      str_detect(mb_name, "[Aa]ußenlager") |
        str_detect(mb_name, "Betriebsstofflager") |
        str_detect(mb_name, "[Gg]erätelager") |
        str_detect(mb_name, "Materiallager") |
        str_detect(mb_name, "Munitionslager") |
        str_detect(mb_name, "Munitionsbehelfslager") |
        str_detect(mb_name, "Munitionsversorgungszentrum") |
        str_detect(mb_name, "Sanitätsmateriallager") |
        str_detect(mb_name, "Sondermunitionslager") |
        str_detect(mb_name, "Sonderwaffenlager") |
        str_detect(mb_name, "[Dd]epot") ~ 
        str_c("Material-/Munitionslager", municipality, sep = " "),
      # Consistent "Lager" names
      str_detect(mb_name, "^Lager") ~ str_c("Lager", municipality, sep = ""),
      .default = ""
    ),
    .after = mb_name
  ) %>% 
  View()


string_dist_matrix <- sf_temp$mb_name %>% 
  stringdistmatrix(., ., method = "lv")


##__________________________________________________________________________####
##  Euclidean Distances                                                     ####

eucl_dist_matrix <- sf_temp %>% 
  st_distance()




#___________________________________________________________________________####
#   Levensthein Distances                                                   ####

# Group location names using their Levensthein distances

# TODO How to pre-process names before the distances are calculated, e. g.
# if MStP or else is in the name?



string_dist_mat <- stringdistmatrix(
  df_temp$mb_name,
  df_temp$mb_name,
  method = "lv",
  useNames = "strings"
)


string_dist_mat[1:5, 1:5]


#___________________________________________________________________________####
#   Euclidean Distances                                                     ####

# TODO How to handle missing coordinates? Look them up manually?


#___________________________________________________________________________####
#   Military Bases                                                          ####

# The scraped data contains time and space information on military units, 
# implicitly giving the locations of military locations. However, names are
# not always consistent, neither are the coordinates. To create a data frame
# with only military locations, data points are first cluster by their distance.
# Observations near to each other (with similar location names) are presumably
# stationed in the same base. Then - to not lose observations w/o coordinates -
# location names are clustered using string similarity.

# Luckily, coordinates are missing for only 166 out of 29,750 observations.

# Create data set containing only locations (w/o time dimension) to assess
# which names correspond to the same location
df_temp <- df_bundeswehr_units %>% 
  select(
    municipality = `Politische Gemeinde`,
    temp_mb_id,
    mb_name = Liegenschaftsbezeichnung,
    coords = Koordinate
  ) %>% 
  distinct() %>% 
  mutate(
    mb_note = str_extract(mb_name, "(?<=\\().*(?=\\))"),
    mb_name = str_remove(mb_name, "\\(.*"),
    .before = coords
  )

df_temp %>% 
  filter(is.na(coords)) %>% 
  View()


zoomerjoin::jaccard_string_group(
  df_temp$mb_name
)













  filter(!is.na(Koordinate)) %>% 
  select(temp_mb_id, Liegenschaftsbezeichnung, Koordinate) %>% 
  distinct() %>% 
  separate(Koordinate, c("lat", "lon"), ", ", convert = T) %>% 
  st_as_sf(coords = c("lon", "lat"), crs = "WGS84", na.fail = T, remove = F) %>% 
  st_transform(crs = utm_crs) %>% 
  st_filter(st_union(sf_krs), .predicate = st_covered_by) %>% 
  st_join(sf_krs)




sf_temp$Liegenschaftsbezeichnung



dist_matrix <- sf_temp %>% 
  st_distance()


dist_matrix %>% 
  as.numeric() %>% 
  { . < 2500 } %>% 
  matrix(
    nrow = nrow(dist_matrix),
    dimnames = list(
      sf_temp$Liegenschaftsbezeichnung,
      sf_temp$Liegenschaftsbezeichnung
    )
  ) %>% 
  igraph::graph_from_adjacency_matrix() %>% 
  plot()



dist_matrix[1:10, 1:10]


matrix(as.numeric(as.numeric(dist_matrix)) < 2500, nrow = nrow(dist_matrix)) %>% 
  igraph::graph_from_adjacency_matrix() %>% 
  plot()



# Create data frame containing aliases of military base names. If the 
# coordinates match perfectly or the bases are very near to each other, these
# are presumably the same bases.

# Some locations are directly removed from the data set because these do not
# constitute military bases, such as embassies or government agencies that
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



