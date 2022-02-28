library(dplyr)
library(readr)
library(rlang)
library(stringr)
library(tidyr)
# extract study cohort from analytics.claims.me
dat <- dbGetQuery(con, "select distinct member_street_address,
                  member_city_name_of_residence,
                  member_zip_code,
                  reporting_entity_name,
                  member_number,year,month from
                  analytics.claims.me where
                  member_state_or_province='DE' AND month='05'")
#clean adresses in dat
sample <- dat%>%
  mutate(short=substr(member_zip_code,1,5)) %>%
  mutate(add=paste(member_street_address,
                   member_city_name_of_residence,
                   "DE",
                   short,
                   sep=", ")) %>%
  mutate(full=tolower(add)) %>%
  filter(!str_detect(full, 'po|p.o.')) %>%
  mutate(full=gsub(" apt",", apt", full),
         full=gsub("unit",", unit", full),
         full=gsub("rm",", rm", full),
         full=str_trim(full),
         full=gsub("[,,]",",",full),
         full=gsub("[.]","",full),
         full=gsub("[#]","",full)) %>%
  select(full,
         reporting_entity_name,
         member_number,year,
  ) %>%
  distinct()


sample2<-sample %>% mutate(priority_address=NA) %>% 
  group_by(member_number,reporting_entity_name,year) %>% 
  filter(row_number()==1) %>% ungroup() %>%
  group_by(member_number,year) %>% 
  mutate(priority_address=
           ifelse(any(str_detect(reporting_entity_name,'CVS HEALTH')),
                  full[reporting_entity_name=='CVS HEALTH'],
                  ifelse(any(str_detect(reporting_entity_name,'DMMA - DPCI')),
                         full[reporting_entity_name=='DMMA - DPCI'], 
                         ifelse(any(str_detect(reporting_entity_name,'DMMA UnitedHealthCare')),
                                full[reporting_entity_name=='DMMA UnitedHealthCare'], 
                                ifelse(any(str_detect(reporting_entity_name,'DELAWARE MEDICAID ASSISTANCE')),
                                       full[reporting_entity_name=='DELAWARE MEDICAID ASSISTANCE'],
                                       ifelse(any(str_detect(full,'^[0-9]')),full[str_detect(full,'^[0-9]')][1],
                                              full[1])))))) %>%
  select(member_number,year,reporting_entity_name,priority_address) %>%
  distinct()

write.csv(sample2,'parallael_fetch/data/meOrig.csv',row.names=FALSE)
write.csv(unique(sample2$priority_address),'parallael_fetch/data/meAddr.csv',row.names=FALSE)
