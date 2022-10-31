#########################################################################
# First stage of FCQ admin setup/course audit sequence
# L:\mgt\FCQ\R_Code\FCQ_Audit01.R - Vince Darcangelo, 11/05/21
#########################################################################

# TOC (order of operations)
# Intro:    Run ciwpass.R to load ciw credentials        (ciwpass.r)
# Prologue: Update semester variables and load libraries (~ 2 mins)
# Part I:   Connect and pull data from ciw               (~ 7 mins)
# Part II:  Format data and prep for course audits       (< 1 mins)
# Part III: Create course audit columns and exceptions   (fcq_audit02.r)
# Part IV:  Set up administration dates                  (fcq_audit03.r)
# Part V:   Generate audit files                         (fcq_audit04.r)

#########################################################################
#'*Intro: Run ciwpass.R to load ciw credentials*
#########################################################################

# start.time <- Sys.time()
# end.time <- Sys.time()
# time.taken <- end.time - start.time
# time.taken

#########################################################################
#'*Prologue: Update semester variables and load libraries*
#########################################################################

# update each semester
term_cd <- "2227"
minStuEnrl <- 3
setwd("L:\\mgt\\FCQ\\CourseAudit")

# semEndDt = 05/31/YYYY for spring, 08/31/YYYY for summer, 12/31/YYYY for fall
semEndDt <- as.Date("12/31/2022", format = "%m/%d/%Y")
semEndDt <- format(semEndDt, "%m/%d/%Y")

# load libraries
library("DBI")
library("ROracle")
library("tidyverse")
library("sqldf")
library("data.table")
library("lubridate")
library("formattable")

#########################################################################
#'*Part I: Connect and pull data from ciw*
#########################################################################

# connect to Oracle database
drv <- dbDriver("Oracle")
connection_string <- '(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)
  (HOST=ciw.prod.cu.edu)(PORT=1525))(CONNECT_DATA=(SERVICE_NAME=CIW)))'
con <- dbConnect(drv, username = getOption("databaseuid"), 
  password = getOption("databasepword"), dbname = connection_string)

# pull from PS_D_CLASS as c (~15 secs)
classtbl <- dbGetQuery(con,
  "SELECT SESSION_SID, CLASS_SID, TERM_CD, INSTITUTION_CD, CAMPUS_CD, SESSION_CD, ACAD_GRP_CD, ACAD_GRP_LD, ACAD_ORG_CD, ACAD_ORG_LD, SBJCT_CD, SBJCT_LD, CATALOG_NBR, CLASS_SECTION_CD, CLASS_NUM, CRSE_CD, CRSE_OFFER_NUM, ASSOCIATED_CLASS, GRADE_BASIS_CD, CLASS_STAT, CLASS_TYPE, ENRL_TOT, ENRL_CAP, ROOM_CAP_REQUEST, CRSE_LD, LOC_ID, SSR_COMP_CD, SSR_COMP_SD, INSTRCTN_MODE_CD, INSTRCTN_MODE_LD, COMBINED_SECTION, CLASS_START_DT, CLASS_END_DT, DATA_ORIGIN
  FROM PS_D_CLASS"
)

c <- classtbl %>%
  filter(TERM_CD == term_cd &
    INSTITUTION_CD != "CUSPG" & 
    DATA_ORIGIN == 'S' &
    CLASS_STAT != "X") %>%
  select(-DATA_ORIGIN)

# exclusions
cx <- c %>%
# limit to 4-digit catalog numbers: 
  # NCLL, NCEG exceptions per Kadie Goodman, Joanne Addison
  filter(nchar(CATALOG_NBR) == 4 | SBJCT_CD %in% c("NCLL", "NCEG")) %>%
  filter(!(ACAD_GRP_CD %in% c('ZOTH', 'CONC', 'DENT')) &
    !(SBJCT_CD %in% c('STDY', 'STY', 'CAND')) &
    !(SBJCT_CD %in% c('CLDR', 'DTSA')) &
    !(SBJCT_CD %in% c('PHSL', 'MEDS', 'PRMD', 'PHMD', 'PALC')) &
# exclude org codes per requests from AMC and Boulder Registrar courses
    ACAD_ORG_CD != 'B-REGR' &
    !(ACAD_ORG_CD %in% c('D-DDSC', 'D-ANES', 'D-BIOS', 'D-CBHS', 'D-DERM', 'D-PHTR', 'D-PCSU', 'D-CCOH', 'D-EHOH', 'D-EMED', 'D-EPID', 'D-FMMD', 'D-NEUR', 'D-OBGY', 'D-OPHT', 'D-ORTH', 'D-OTOL', 'D-PHAS', 'D-PSCH', 'D-PATH', 'D-SURG', 'D-PHRD', 'D-HSMP', 'D-RAON', 'D-RADI', 'D-PEDS', 'D-NSUR', 'D-MEDI', 'D-PHNT', 'D-PUBH', 'D-PUNC', 'D-NURS'))) %>%
  filter(!(SBJCT_CD == 'IDPT' & !(CATALOG_NBR %in% c('7806', '7810')))) %>%
# exclude by campus: CEPS applied music and AMC 'RSC' courses
  filter(!(CAMPUS_CD == 'CEPS' & SESSION_CD == 'BM9') &
    !(CAMPUS_CD == 'AMC' & SSR_COMP_CD == 'RSC') &
    !(ACAD_GRP_CD == 'NOCR' & SBJCT_CD != 'NCLL') &
# exclude by institution/subject_cd: DN-MILR, BD-CSVC, MC-CLSC
    !(INSTITUTION_CD == 'CUBLD' & SBJCT_CD == 'CSVC') &
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'MILR') &
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'CLSC') &
# exclude classes that dept didn't cancel correctly
    CLASS_NUM != 0 &
# exclude DN-MATH Gxx and 501 sections
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'MATH' & str_detect(CLASS_SECTION_CD, '^G')) &
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'MATH' & CLASS_SECTION_CD == '501'))

# pull from PS_F_CLASS_MTG_PAT as cmtg (~15 secs)
mtgtbl <- dbGetQuery(con,
  "SELECT TERM_CD, SESSION_SID, CLASS_NUM, CLASS_SECTION_CD, SCHED_PRINT_INSTR, GRADE_RSTR_ACCESS, CLASS_MTG_NUM, PERSON_SID, INSTRCTR_ASGN_NUM, INSTRCTR_ROLE_SID, FCLTY_SID, MEETING_TIME_START, MEETING_TIME_END, STND_MTG_PAT, START_DT, END_DT, DATA_ORIGIN
  FROM PS_F_CLASS_MTG_PAT"
)

cmtg <- mtgtbl %>%
  filter(TERM_CD == term_cd &
    PERSON_SID != 2147483646 & 
    DATA_ORIGIN == 'S') %>%
  select(-c(TERM_CD, DATA_ORIGIN))

cmtg2 <- cmtg %>%
  group_by(CLASS_NUM) %>%
  filter(CLASS_MTG_NUM == max(CLASS_MTG_NUM))

# join cx & cmtg dataframes
j1 <- left_join(cx, cmtg2, by = "CLASS_NUM")

############# error check
# identify crse date mismatches
mismatch <- j1 %>%
  filter(END_DT != CLASS_END_DT) %>%
  select(INSTITUTION_CD, SBJCT_CD, CATALOG_NBR, CLASS_START_DT, CLASS_END_DT, END_DT)

write.csv(mismatch, "mismatch.csv", row.names = FALSE)
#############

j2 <- j1 %>%
  group_by(CLASS_NUM) %>% 
  summarise(CLASS_MTG_NUM = min(CLASS_MTG_NUM))
j3 <- left_join(j1, j2, by = c("CLASS_NUM", "CLASS_MTG_NUM"))

# pull from PS_D_FCLTY as fac (~1 sec)
factbl <- dbGetQuery(con,
  "SELECT FCLTY_SID, FCLTY_ID, FCLTY_LD, DATA_ORIGIN
    FROM PS_D_FCLTY"
)

fac <- factbl %>%
  filter(DATA_ORIGIN == 'S') %>%
  select(-DATA_ORIGIN)

j4 <- left_join(j3, fac, by = "FCLTY_SID")

# pull from PS_D_INSTRCTR_ROLE as irole (~1 sec)
irole <- dbGetQuery(con,
  "SELECT INSTRCTR_ROLE_SID, INSTRCTR_ROLE_CD
    FROM PS_D_INSTRCTR_ROLE"
)

j5 <- left_join(j4, irole, by = "INSTRCTR_ROLE_SID")

# Pull from PS_D_PERSON as pers (~1 min)
perstbl <- dbGetQuery(con,
  "SELECT PERSON_SID, PERSON_ID, PRF_PRI_NAME, PRF_PRI_LAST_NAME, PRF_PRI_FIRST_NAME, PRF_PRI_MIDDLE_NAME
  FROM PS_D_PERSON"
)

pers <- perstbl %>%
  filter(PERSON_SID != '2147483646')

colnames(pers) <- c("PERSON_SID", "instrPersonID", "instrNm_src", "instrLastNm", "instrFirstNm", "instrMiddleNm")

j6 <- left_join(j5, pers, by = "PERSON_SID")

# remove duplicates
# create dupkey combining inst and class numbers
j6$dupkey <- paste0(j6$PERSON_SID, j6$CLASS_NUM)

# create jsub doc by filtering for duplicate dupkeys
jsub <- j6 %>%
  group_by(dupkey) %>%
  filter(n() >= 2) %>%
  ungroup

# subset duplicates to pick up "Y" values
jsub2 <- subset(jsub, SCHED_PRINT_INSTR == "Y")

# quality check -- this should produce 0 obs unless dept entered instr >1x
jsub3 <- jsub2 %>%
  group_by(dupkey) %>%
  filter(n() >= 2) %>%
  ungroup

# in the case when jsub3 > 0, run this
jsub2x <- jsub2 %>%
  group_by(dupkey) %>%
  filter(INSTRCTR_ASGN_NUM == min(INSTRCTR_ASGN_NUM))

# use a filter to remove the duplicates from j6
j6fil <- j6 %>% 
  anti_join(jsub)

# rbind j6fil and jsub2 to restore filtered dups
j6x <- rbind(j6fil, jsub2x)

# pull from PS_D_PERSON_EMAIL as em (~20 secs)
em <- dbGetQuery(con,
  "SELECT PERSON_ID, PREF_EMAIL, BLD_EMAIL, CONT_ED_EMAIL, DEN_EMAIL
  FROM PS_D_PERSON_EMAIL"
)

colnames(em) <- c("instrPersonID", "PREF_EMAIL", "BLD_EMAIL", "CONT_ED_EMAIL", "DEN_EMAIL")

j7 <- left_join(j6x, em, by = "instrPersonID")

# pull from PS_D_PERSON_ATTR as cid (~30 secs)
cid <- dbGetQuery(con, 
  "SELECT PERSON_ID, CONSTITUENT_ID
  FROM PS_D_PERSON_ATTR"
)

colnames(cid) <- c("instrPersonID", "instrConstituentID")

j8 <- left_join(j7, cid, by = "instrPersonID")

# pull from PS_CU_D_EXT_SYSTEM as hr (~12 secs)
hrtbl <- dbGetQuery(con,
  "SELECT EXTERNAL_SYSTEM, PERSON_SID, EXTERNAL_SYSTEM_ID
  FROM PS_CU_D_EXT_SYSTEM"
)

hr0 <- hrtbl %>%
  filter(EXTERNAL_SYSTEM == 'HR' & PERSON_SID != '2147483646') %>%
  select(-EXTERNAL_SYSTEM)

hr <- unique(hr0)
colnames(hr) <- c("PERSON_SID", "instrEmplid")

j9 <- left_join(j8, hr, by = "PERSON_SID")

# pull from PS_CU_D_CLASS_ATTR as cattr (~6 secs)
cattrtbl <- dbGetQuery(con,
  "SELECT CRSE_ATTR_CD, DATA_ORIGIN, CLASS_SID, CRSE_ATTR_VALUE_CD
  FROM PS_CU_D_CLASS_ATTR"
)

cattr <- cattrtbl %>%
  filter(CRSE_ATTR_CD == "COMB" & DATA_ORIGIN == "S") %>%
  select(CLASS_SID, CRSE_ATTR_VALUE_CD) %>%
  mutate(combStat = case_when(
    CRSE_ATTR_VALUE_CD == "SPONSOR" ~ "S",
    CRSE_ATTR_VALUE_CD == "NON-SPONSR" ~ "N",
    CRSE_ATTR_VALUE_CD == TRUE ~ "-"
))

j10 <- left_join(j9, cattr, by = "CLASS_SID")

# pull from PS_CU_D_SCTN_CMBND as cmbsec (~1 sec)
sectbl <- dbGetQuery(con,
  "SELECT INSTITUTION_CD, TERM_CD, SESSION_CD, CLASS_NUM, SCTN_CMBND_CD, SCTN_CMBND_LD, SCTN_CMBND_EN_TOT, DATA_ORIGIN
  FROM PS_CU_D_SCTN_CMBND"
)

cmbsec <- sectbl %>%
  filter(TERM_CD == term_cd & DATA_ORIGIN == "S") %>%
  select(-DATA_ORIGIN)

j11 <- left_join(j10, cmbsec, by = c("TERM_CD", "INSTITUTION_CD", "SESSION_CD", "CLASS_NUM"))

# pull from PS_F_CLASS_ENRLMT as enr (~2 mins)
enrtbl <- dbGetQuery(con,
  "SELECT ENRLMT_STAT_SID, ENRLMT_DROP_DT_SID, DATA_ORIGIN, CLASS_SID
  FROM PS_F_CLASS_ENRLMT"
)

enr <- enrtbl %>%
  filter(ENRLMT_STAT_SID == '3' & ENRLMT_DROP_DT_SID == 19000101 
         & DATA_ORIGIN == "S") %>%
  select(CLASS_SID)

enr <- enr %>% count(CLASS_SID)
colnames(enr) <- c("CLASS_SID", "totEnrl")

j12 <- left_join(j11, enr, by = "CLASS_SID")

#########################################################################
#'*Part II: Format data and prep for course audits'*
#########################################################################

# extract times and dates
j12$mtgStartTm <- format(as.POSIXct(j12$MEETING_TIME_START, 
  format="%Y:%m:%d %H:%M:%S"), "%H:%M")
j12$mtgStartDt <- format(as.POSIXct(j12$START_DT, 
  format="%Y:%m:%d %H:%M:%S"), "%m/%d/%Y")
j12$mtgEndTm <- format(as.POSIXct(j12$MEETING_TIME_END, 
  format="%Y:%m:%d %H:%M:%S"), "%H:%M")
j12$mtgEndDt <- format(as.POSIXct(j12$END_DT, 
  format="%Y:%m:%d %H:%M:%S"), "%m/%d/%Y")

# Separate, format and rejoin times and dates from CLASS_START_DT and CLASS_END_DT
j12$cst <- format(as.POSIXct(j12$CLASS_START_DT, 
  format="%Y:%m:%d %H:%M:%S"), "%H:%M:%S")
j12$csd <- format(as.POSIXct(j12$CLASS_START_DT, 
  format="%Y:%m:%d %H:%M:%S"), "%d%b%y")
j12$cet <- format(as.POSIXct(j12$CLASS_END_DT, 
  format="%Y:%m:%d %H:%M:%S"), "%H:%M:%S")
j12$ced <- format(as.POSIXct(j12$CLASS_END_DT, 
  format="%Y:%m:%d %H:%M:%S"), "%d%b%y")
j12$fcqStDt <- format(as.POSIXct(j12$csd, format="%d%b%y"), "%m/%d/%Y")
j12$fcqEnDt <- format(as.POSIXct(j12$ced, format="%d%b%y"), "%m/%d/%Y")
j12$CLASS_START_DT <- toupper(paste(j12$csd, j12$cst, sep = ":"))
j12$CLASS_END_DT <- toupper(paste(j12$ced, j12$cet, sep = ":"))

# Convert NA values to blanks
j12$FCLTY_ID[is.na(j12$FCLTY_ID)] <- ""
j12$FCLTY_LD[is.na(j12$FCLTY_LD)] <- ""
j12$combStat[is.na(j12$combStat)] <- ""
j12$SCTN_CMBND_CD[is.na(j12$SCTN_CMBND_CD)] <- ""
j12$SCTN_CMBND_LD[is.na(j12$SCTN_CMBND_LD)] <- ""
j12$instrNm_src[is.na(j12$instrNm_src)] <- ""
j12$instrFirstNm[is.na(j12$instrFirstNm)] <- ""
j12$instrLastNm[is.na(j12$instrLastNm)] <- ""
j12$instrMiddleNm[is.na(j12$instrMiddleNm)] <- ""
j12$instrEmplid[is.na(j12$instrEmplid)] <- ""

# Convert NA values to -
j12$CLASS_MTG_NUM[is.na(j12$CLASS_MTG_NUM)] <- "-"
j12$CLASS_SECTION_CD.x[is.na(j12$CLASS_SECTION_CD.x)] <- "-"
j12$SCHED_PRINT_INSTR[is.na(j12$SCHED_PRINT_INSTR)] <- "-"
j12$GRADE_RSTR_ACCESS[is.na(j12$GRADE_RSTR_ACCESS)] <- "-"
j12$fcqStDt[is.na(j12$fcqStDt)] <- "-"
j12$fcqEnDt[is.na(j12$fcqEnDt)] <- "-"
j12$mtgStartDt[is.na(j12$mtgStartDt)] <- "-"
j12$mtgStartTm[is.na(j12$mtgStartTm)] <- "-"
j12$mtgEndDt[is.na(j12$mtgEndDt)] <- "-"
j12$mtgEndTm[is.na(j12$mtgEndTm)] <- "-"
j12$STND_MTG_PAT[is.na(j12$STND_MTG_PAT)] <- "-"
j12$INSTRCTR_ROLE_CD[is.na(j12$INSTRCTR_ROLE_CD)] <- "-"
j12$instrPersonID[is.na(j12$instrPersonID)] <- "-"
j12$PREF_EMAIL[is.na(j12$PREF_EMAIL)] <- "-"
j12$BLD_EMAIL[is.na(j12$BLD_EMAIL)] <- "-"
j12$CONT_ED_EMAIL[is.na(j12$CONT_ED_EMAIL)] <- "-"
j12$DEN_EMAIL[is.na(j12$DEN_EMAIL)] <- "-"
j12$instrConstituentID[is.na(j12$instrConstituentID)] <- "-"
j12$SCTN_CMBND_EN_TOT[is.na(j12$SCTN_CMBND_EN_TOT)] <- "-"
j12$totEnrl[is.na(j12$totEnrl)] <- "-"

# Update combined enrollment for non-combined classes
j12 <- j12 %>%
  mutate(totEnrl_nowd_comb = case_when(
    COMBINED_SECTION != "-" ~ SCTN_CMBND_EN_TOT,
    COMBINED_SECTION == "-" ~ totEnrl
  ))

# Convert - values to 0 in enrollment columns
j12$totEnrl_nowd_comb <- gsub("-", 0, j12$totEnrl_nowd_comb)
j12$totEnrl <- gsub("-", 0, j12$totEnrl)

# Add space after comma in instrNm_src column (e.g., "Smith,J" -> "Smith, J")
j12$instrNm_src <- gsub(",", ", ", j12$instrNm_src)

# Create instrEmailAddr column based on email columns
j12 <- j12 %>%
  mutate(instrEmailAddr = case_when(
    INSTITUTION_CD == "CUBLD" & BLD_EMAIL != "-" ~ BLD_EMAIL,
    CAMPUS_CD == "CEPS" & CONT_ED_EMAIL != "-" ~ CONT_ED_EMAIL,
    CAMPUS_CD == "CEPS" & BLD_EMAIL != "-" ~ BLD_EMAIL,
    INSTITUTION_CD == "CUDEN" & DEN_EMAIL != "-" ~ DEN_EMAIL,
    PREF_EMAIL != "-" ~ PREF_EMAIL
  ))
j12$instrEmailAddr[is.na(j12$instrEmailAddr)] <- "-"

# Drop unnecessary columns
clist = subset(j12, select = -c(BLD_EMAIL, ced, cet, CLASS_SECTION_CD.y, CLASS_SID, COMBINED_SECTION, CONT_ED_EMAIL, CRSE_ATTR_VALUE_CD, CRSE_CD, CRSE_OFFER_NUM, csd, cst, DEN_EMAIL, FCLTY_SID, INSTRCTN_MODE_LD, INSTRCTR_ROLE_SID, MEETING_TIME_END,MEETING_TIME_START,PERSON_SID, PREF_EMAIL, SBJCT_LD, SESSION_SID.x, SESSION_SID.y, SCTN_CMBND_EN_TOT, SSR_COMP_SD))

# Fix column names
names(clist)[names(clist) == "CLASS_MTG_NUM"] <- "minClassMtgNum"
names(clist)[names(clist) == "CLASS_SECTION_CD.x"] <- "CLASS_SECTION_CD"
names(clist)[names(clist) == "instrNm_src"] <- "instrNm"
names(clist)[names(clist) == "totEnrl"] <- "totEnrl_nowd"
names(clist)[names(clist) == "STND_MTG_PAT"] <- "mtgDays"

# Format combStat column
#clist$combStat[clist$combStat == "SPONSOR"] <- "S"
#clist$combStat[clist$combStat == "NON-SPONSR"] <- "N"

# Create campus column to generate two-digit campus code from CAMPUS_CD
cl0 <- clist %>%
  mutate(campus = case_when(
    INSTITUTION_CD == "CUBLD" & CAMPUS_CD == "BLD3" ~ "B3",
    INSTITUTION_CD == "CUBLD" & CAMPUS_CD == "BLDR" ~ "BD",
    INSTITUTION_CD == "CUBLD" & CAMPUS_CD == "CEPS" ~ "CE",
    INSTITUTION_CD == "CUDEN" & CAMPUS_CD == "DC" ~ "DN",
    INSTITUTION_CD == "CUDEN" & CAMPUS_CD == "EXSTD" ~ "DN",
    INSTITUTION_CD == "CUDEN" & SBJCT_CD == "CHPM" ~ "MC",
    INSTITUTION_CD == "CUDEN" & CAMPUS_CD == "AMC" ~ "MC",
  ))

# Create college column to generate two-digit college code from ACAD_GRP_CD
cl1 <- cl0 %>%
  mutate(college = case_when(
    campus == "B3" & ACAD_GRP_CD == "CRSS" ~ "AS",
    campus == "B3" & ACAD_GRP_CD == "EDUC" ~ "EB",
    campus == "B3" & ACAD_GRP_CD == "ENGR" ~ "EN",
    campus == "BD" & ACAD_GRP_CD == "ARSC" ~ "AS",
    campus == "BD" & ACAD_GRP_CD == "BUSN" ~ "BU",
    campus == "BD" & ACAD_GRP_CD == "CRSS" ~ "XY",
    campus == "BD" & ACAD_GRP_CD == "CMCI" ~ "MC",
    campus == "BD" & ACAD_GRP_CD == "ARPL" ~ "EV",
    campus == "BD" & ACAD_GRP_CD == "ENGR" ~ "EN",
    campus == "BD" & ACAD_GRP_CD == "LIBR" ~ "LB",
    campus == "BD" & ACAD_GRP_CD == "EDUC" ~ "EB",
    campus == "BD" & ACAD_GRP_CD == "MUSC" ~ "MB",
    campus == "BD" & ACAD_GRP_CD == "LAWS" ~ "LW",
    campus == "CE" ~ "CE",
    # campus == "CE" & ACAD_GRP_CD == "CMCI" ~ "?",
    # campus == "CE" & ACAD_GRP_CD == "ENGR" ~ "?",
    # campus == "CE" & ACAD_GRP_CD == "BUSN" ~ "?",
    # campus == "CE" & ACAD_GRP_CD == "EDUC" ~ "?",
    # campus == "CE" & ACAD_GRP_CD == "ARPL" ~ "?",
    # campus == "CE" & ACAD_GRP_CD == "CRSS" ~ "?",
    # campus == "CE" & ACAD_GRP_CD == "MUSC" ~ "?",
    # campus == "CE" & ACAD_GRP_CD == "CEPS" ~ "?",
    campus == "MC" ~ "MC",
    campus == "DN" & ACAD_GRP_CD == "BUSN" ~ "BD",
    campus == "DN" & ACAD_GRP_CD == "CLAS" ~ "LA",
    campus == "DN" & ACAD_GRP_CD == "ARPL" ~ "AP",
    campus == "DN" & ACAD_GRP_CD == "ARTM" ~ "AM",
    campus == "DN" & ACAD_GRP_CD == "ENGR" ~ "EN",
    campus == "DN" & ACAD_GRP_CD == "EDUC" ~ "ED",
    campus == "DN" & ACAD_GRP_CD == "PAFF" ~ "PA",
    campus == "DN" & ACAD_GRP_CD == "CRSS" ~ "XY",
    campus == "DN" & ACAD_GRP_CD == "NOCR" ~ "LA",
  ))

# Separate into RAP/non-RAP/CEPS sections
cl2 <- subset(cl1, !grepl("R$", CLASS_SECTION_CD))
cl2r <- subset(cl1, grepl("R$", CLASS_SECTION_CD))
cl2c <- subset(cl2, campus == "CE")
cl2s <- subset(cl2, campus != "CE")

# Non-RAP: Create keys to prep for fcqdept column
cl2s <- cl2s %>%
  mutate(key = case_when(
    campus == "B3" ~ paste(campus, college, SBJCT_CD, sep = "-"),
    ACAD_ORG_CD == "B-MCEN" ~ paste(campus, "EN", SBJCT_CD, sep = "-"),
    campus == "BD" ~ paste(campus, college, SBJCT_CD, sep = "-"),
    campus == "CE" ~ paste(campus, SBJCT_CD, sep = "-"),
    ACAD_GRP_CD == "MEDS" ~ paste("MC", "MC", SBJCT_CD, sep = "-"),
    ACAD_GRP_CD == "PHAR" ~ paste("MC", "MC", SBJCT_CD, sep = "-"),
    campus == "DN" ~ paste(campus, college, SBJCT_CD, sep = "-"),
  ))

# CEPS: Create numeric column for case_when range
cl2c <- cl2c %>%
  mutate(sec = substr(CLASS_SECTION_CD, 0, 2))
#cl2c <- transform(cl2c, sec = as.numeric(sec))

# CEPS: Sort by dept
cl2c2 <- cl2c %>%
  mutate(key = case_when(
    SBJCT_CD == "ORGL" ~ "CE-ORGL",
    SBJCT_CD == "ESLG" | SBJCT_CD == "NCIE" ~ "CE-IEC",
    SESSION_CD == "BEF" | SESSION_CD == "BET" | SESSION_CD == "BWS" ~ "CE-CONT",
    sec >= 73 & sec <= 75 ~ "CE-BBAC",
    sec >= 56 & sec <= 57 ~ "CE-TRCT",
    sec >= 58 & sec <= 59 ~ "CE-CC",
    sec >= 64 & sec <= 64 ~ "CE-CC",
    TRUE ~ "CE-CONT"
  ))

# RAP: Create numeric column for case_when range
cl2r <- cl2r %>%
  mutate(sec = substr(CLASS_SECTION_CD, 1, nchar(CLASS_SECTION_CD)-1))
cl2r <- transform(cl2r, sec = as.numeric(sec))

# RAP: Create keys to prep for fcqdept column
cl2r <- cl2r %>%
  mutate(key = case_when(
    sec >= 130 & sec < 160 ~ paste(campus, "AS", "GSAP", sep = "-"),
    sec >= 160 & sec < 190 ~ paste(campus, "AS", "SEWL", sep = "-"),
    sec >= 190 & sec < 220 ~ paste(campus, "AS", "HPRP", sep = "-"),
    sec >= 220 & sec < 250 ~ paste(campus, "AS", "COMR", sep = "-"),
    sec >= 250 & sec < 280 ~ paste(campus, "AS", "FARR", sep = "-"),
    sec >= 280 & sec < 310 ~ paste(campus, "AS", "LIBB", sep = "-"),
    sec >= 310 & sec < 340 ~ paste(campus, "AS", "SASC", sep = "-"),
    sec >= 370 & sec < 400 ~ paste(campus, "AS", "SSIR", sep = "-"),
    sec >= 400 & sec < 430 ~ paste(campus, "AS", "MASP", sep = "-"),
    sec >= 430 & sec < 460 ~ paste(campus, "AS", "BRAP", sep = "-"),
    sec >= 490 & sec < 520 ~ paste(campus, "AS", "VCAA", sep = "-"),
    sec >= 430 & sec < 460 ~ paste(campus, "AS", "BRAP", sep = "-"),
    sec == 549 ~ paste(campus, college, SBJCT_CD, sep = "-"),
    sec >= 550 & sec < 580 ~ paste(campus, "AS", "CUDC", sep = "-"),
    sec >= 610 & sec < 670 ~ paste(campus, "BU", "BU", sep = "-"),
    sec >= 888 & sec < 910 ~ paste(campus, "AS", "HRAP", sep = "-")
  ))

# subset RAPs with NA
cl2r <- subset(cl2r, !is.na(key))
cl2n <- subset(cl2r, is.na(key))

# replace key column for RAP with key formula for non-RAP
cl2n <- subset(cl2n, select = -key)
cl2n <- cl2n %>% mutate(key = paste(campus, CAMPUS_CD, SBJCT_CD, sep = "-"))

# remove sec columns
cl2c2 <- subset(cl2c2, select = -sec)
cl2n <- subset(cl2n, select = -sec)
cl2r <- subset(cl2r, select = -sec)

# load FCQ department keys
dept_key <- read.csv("L:\\mgt\\FCQ\\R_Code\\FCQDept_keys.csv")

# remove dups
dept_key2 <- subset(dept_key, fcqdept != "SBDR")
dept_key <- subset(dept_key2, !(campus == "CE" & subject == "EDUC" 
  & fcqdept == "CONT"))

# merge all classes back together now that they all have key columns
clmerge <- rbind(cl2s, cl2c2, cl2r, cl2n)

# merge tables to create fcqdept var
clmerge$fcqdept <- dept_key$fcqdept[match(clmerge$key, dept_key$key)]

# create deptOrgID column
cl4 <- clmerge %>%
  mutate(deptOrgID = case_when(
    INSTITUTION_CD == "CUBLD" ~ paste(INSTITUTION_CD, CAMPUS_CD, fcqdept, sep = ":"),
    TRUE ~ paste(INSTITUTION_CD, ACAD_GRP_CD, ACAD_ORG_CD, sep = ":")
  ))

# separate sponsor/blank from non-sponsor sections
spons <- subset(cl4, combStat != 'N')
nspons <- subset(cl4, combStat == 'N')

# create spons_id, _comp_cd, _deptOrgID, _AcadGrp, _AcadOrg, _fcqdept column
spons1 <- spons %>%
    mutate(spons_id = paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-"))
spons2 <- spons1 %>%
  mutate(spons_comp_cd = SSR_COMP_CD)
spons3 <- spons2 %>%
  mutate(spons_deptOrgID = deptOrgID)
spons4 <- spons3 %>%
  mutate(spons_AcadGrp = ACAD_GRP_CD)
spons5 <- spons4 %>%
  mutate(spons_AcadOrg = ACAD_ORG_CD)
spons6 <- spons5 %>%
  mutate(spons_fcqdept = fcqdept)

# filter rows and columns to sponsor sections and data
spons7 <- subset(spons6, combStat == 'S')
spons8 <- spons7[,c("INSTITUTION_CD","SCTN_CMBND_CD","SCTN_CMBND_LD", "instrNm","spons_id","spons_comp_cd","spons_deptOrgID","spons_AcadGrp", "spons_AcadOrg", "spons_fcqdept")]

# duplicate spons_comp_cd column to retain during join
spons8 <- spons8 %>% 
  mutate(spons_comp_cd2 = spons_comp_cd)

# merge spons columns onto nspons
nspons1 <- unique(nspons)
nspons2 <- subset(nspons1[!is.na(nspons1$SCTN_CMBND_CD),])
nspons3 <- left_join(nspons2, spons8, by = c("INSTITUTION_CD", "SCTN_CMBND_CD", "SCTN_CMBND_LD", "instrNm"))
nspons3 <- subset(nspons3, select = -spons_comp_cd2)
nspons4 <- nspons3[!duplicated(nspons3[,c('CLASS_NUM', 'instrPersonID')]),]

# recombine spons and nspons data
clist2 <- rbind(spons6, nspons4)

# duplicate spons_id column as assoc_class_secID
clist3 <- clist2 %>% 
  mutate(assoc_class_secID = case_when(
    ASSOCIATED_CLASS == 9999 ~ "",
    TRUE ~ spons_id))

# drop unnecessary columns
# clist4 = subset(clist3, select = -c(key, subject, college.x, 
    # college.y, campus.y))

# fix column names
names(clist3)[names(clist3) == "spons_comp_cd"] <- "crseSec_comp_cd"
names(clist3)[names(clist3) == "campus.x"] <- "campus"

# update order to prep for instrNum
clist5 <- clist3[order(clist3$INSTITUTION_CD,clist3$SBJCT_CD,clist3$CATALOG_NBR,clist3$CLASS_SECTION_CD,clist3$INSTRCTR_ROLE_CD,clist3$instrNm),]

# create number sequence for instrNum
clist6 <- clist5$instrNum <- sequence(rle(clist5$CLASS_NUM)$lengths)
clist6 <- as.data.frame(clist6)

# combine into crslistCU
crslistCU <- cbind(clist5, clist6)

# fix column names
names(crslistCU)[names(crslistCU) == "clist6"] <- "instrNum"

# arrange columns
crslistCU <- crslistCU[,c("campus","deptOrgID","fcqdept","TERM_CD", "INSTITUTION_CD","CAMPUS_CD","SESSION_CD","SBJCT_CD","CATALOG_NBR", "CLASS_SECTION_CD","instrNum","instrNm","instrLastNm","instrFirstNm", "instrMiddleNm","instrPersonID","instrConstituentID","instrEmplid", "instrEmailAddr","INSTRCTR_ROLE_CD","ASSOCIATED_CLASS","assoc_class_secID","GRADE_BASIS_CD","totEnrl_nowd_comb","totEnrl_nowd","ENRL_TOT", "ENRL_CAP", "ROOM_CAP_REQUEST","CRSE_LD","LOC_ID","CLASS_STAT", "crseSec_comp_cd","SSR_COMP_CD","combStat","spons_AcadGrp", "spons_AcadOrg","spons_fcqdept","spons_deptOrgID","spons_id","SCTN_CMBND_CD","SCTN_CMBND_LD","INSTRCTN_MODE_CD","CLASS_TYPE","SCHED_PRINT_INSTR","GRADE_RSTR_ACCESS","minClassMtgNum","mtgStartDt","mtgEndDt","mtgStartTm","mtgEndTm","mtgDays","CLASS_START_DT","fcqStDt","fcqEnDt","CLASS_END_DT","FCLTY_ID","FCLTY_LD","CLASS_NUM","ACAD_GRP_CD","ACAD_GRP_LD","ACAD_ORG_CD","ACAD_ORG_LD")]

# update order to match C10
crslistCU <- crslistCU[order(crslistCU$campus, crslistCU$deptOrgID),]

# filter crslistCU
clst_filter <- subset(crslistCU, totEnrl_nowd_comb != 0)

# create clslistCU_1 by grouping crslistCU
clslistCU_1 <- clst_filter %>%
  group_by(campus, fcqdept, SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD) %>%
  mutate(totSchedPrintInstr_Y = sum(SCHED_PRINT_INSTR == "Y")) %>%
  mutate(totSchedPrintInstr_Not_Y = sum(SCHED_PRINT_INSTR %in% c("N", "-"))) %>%
  mutate(totInstrPerClass = n(), across()) %>%
  ungroup()

# rejoin with filtered dataset
#clst_filter1 <- left_join(clst_filter,clslistCU_1,by = c("campus","fcqdept",     "SBJCT_CD","CATALOG_NBR","CLASS_SECTION_CD"))

# order clslistCU_1
clslistCU_1[order(c("campus", "deptOrgID", "SBJCT_CD", "CATALOG_NBR", "CLASS_SECTION_CD"))]

# create clslistCU_2 by summarise() crslistCU_1
clslistCU_2 <- clslistCU_1 %>%
  group_by(campus,deptOrgID,SBJCT_CD,CATALOG_NBR,crseSec_comp_cd,instrNm) %>%
  summarise(totCrsePerInstr = sum(SCHED_PRINT_INSTR == "Y"), across()) %>%
  ungroup()

# rejoin with filtered dataset
#clst_filter2 <- left_join(clst_filter1, clslistCU_2, by = c("campus", 
  #"deptOrgID", "SBJCT_CD", "CATALOG_NBR", "crseSec_comp_cd", "instrNm"))

# order clslistCU_2
clslistCU_2[order(c("campus", "deptOrgID", "SBJCT_CD", "CATALOG_NBR", "CLASS_SECTION_CD", "instrNm"))]

# create clslistCU_3 by summarise() crslistCU_2
clslistCU_3 <- clslistCU_2 %>%
  group_by(SBJCT_CD, CATALOG_NBR, ASSOCIATED_CLASS, instrNm) %>%
  mutate(indLEC = case_when(
    instrNm != "-" & crseSec_comp_cd %in% c("LEC", "SEM")
    & SCHED_PRINT_INSTR == "Y" ~ 1,
    TRUE ~ 0)) %>%
  mutate(indNotLEC = case_when(
    instrNm != "-" & !(crseSec_comp_cd %in% c("LEC", "SEM"))
    & SCHED_PRINT_INSTR == "Y" ~ 1,
    TRUE ~ 0)) %>%
  mutate(totLECInstr = sum(indLEC)) %>%
  mutate(totNotLECInstr = sum(indNotLEC)) %>%
  mutate(nm_prep = case_when(
    instrNm != "-" ~ 1)) %>%
  mutate(totAssocClassPerInstr = sum(nm_prep))

# order clslistCU_3
clslistCU_3[order(c("campus", "deptOrgID", "SBJCT_CD", "CATALOG_NBR", "CLASS_SECTION_CD", rev("SCHED_PRINT_INSTR"), "instrNm"))]

# arrange columns
clslistCU_3 <- clslistCU_3[,c("campus","deptOrgID","fcqdept","TERM_CD","INSTITUTION_CD", "CAMPUS_CD","SESSION_CD","SBJCT_CD","CATALOG_NBR","CLASS_SECTION_CD","instrNum","instrNm","instrLastNm","instrFirstNm","instrMiddleNm","instrPersonID","instrConstituentID", "instrEmplid","instrEmailAddr","INSTRCTR_ROLE_CD","ASSOCIATED_CLASS","assoc_class_secID","GRADE_BASIS_CD","totEnrl_nowd_comb", "totEnrl_nowd", "ENRL_TOT","ENRL_CAP","ROOM_CAP_REQUEST","CRSE_LD","LOC_ID","CLASS_STAT","crseSec_comp_cd","SSR_COMP_CD", "combStat","spons_AcadGrp","spons_AcadOrg","spons_fcqdept","spons_deptOrgID","spons_id","SCTN_CMBND_CD","SCTN_CMBND_LD","INSTRCTN_MODE_CD","CLASS_TYPE","SCHED_PRINT_INSTR","GRADE_RSTR_ACCESS","minClassMtgNum","mtgStartDt","mtgEndDt","mtgStartTm","mtgEndTm","mtgDays","CLASS_START_DT","CLASS_END_DT","FCLTY_ID","FCLTY_LD","CLASS_NUM","ACAD_GRP_CD","ACAD_GRP_LD","ACAD_ORG_CD","ACAD_ORG_LD","totSchedPrintInstr_Y","totSchedPrintInstr_Not_Y","totInstrPerClass","totCrsePerInstr","totAssocClassPerInstr","indLEC","indNotLEC","totLECInstr","totNotLECInstr","fcqStDt","fcqEnDt")]

# export files to working directory
write.csv(clslistCU_3, "c20.csv", row.names = FALSE)

##########################################################
dbDisconnect(con)
