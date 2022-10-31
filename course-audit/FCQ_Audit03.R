#########################################################################
# Third stage of FCQ admin setup/course audit sequence
# L:\mgt\FCQ\R_Code\FCQ_Audit03.R - Vince Darcangelo, 9/19/22
#########################################################################

# TOC (order of operations)
# Part IV:  Set administration dates                     (~ 40 secs)
# Part V:   Generate audit files                         (fcq_audit04.r)

#########################################################################
#'*Part IV: Set administration dates*
#########################################################################
# not sure if the conversion is necessary, but it is possible
# date <- as.Date(n14$mtgStartDt, format = "%m/%d/%Y")
# format(date, "%d%b%Y")

# exclude/include classes manually through adminInd
n15 <- n14 %>%
  mutate(adminInd = case_when(
    paste(SBJCT_CD, CATALOG_NBR, sep = "-") == "BMSC-7806" ~ 0,
    paste(SBJCT_CD, CATALOG_NBR, sep = "-") == "BMSC-7810" ~ 0,
    SBJCT_CD %in% c("AMBA", "CLSC", "EMEA") ~ 0,
    SBJCT_CD == "CMFT" & CATALOG_NBR %in% c("5910", "5911") ~ 0,
    paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-") == "INTS-3939-901" ~ 1,
    TRUE ~ adminInd
  ))

# set administration dates
n16 <- n15 %>%
  mutate(adminDtTxt = case_when(
    adminInd == 0 ~ "",

# semester exceptions
    campus == "BD" & SBJCT_CD == "GEOL" & CATALOG_NBR == "2700" ~ "Oct 24-Oct 28",
    campus == "BD" & SBJCT_CD == "LAWS" & CATALOG_NBR %in% c("5646", "6866", "7407") ~ "Aug 22-Aug 26",
    campus == "BD" & SBJCT_CD == "LAWS" & CATALOG_NBR %in% c("6823") ~ "Nov 14-Nov 18",
  SBJCT_CD %in% c("BCOR", "MBAC") & SESSION_CD == "B81" ~ "Oct 03-Oct 07",
  SBJCT_CD == "NCLL" & SESSION_CD == "DC1" ~ "Oct 03-Oct 07",
  campus == "DN" & paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-") %in% c("DSEP-8990-903", "MTED-4003-001", "MTED-5003-001") ~ "Nov 28-Dec 06",
  campus == "DN" & paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-") == "RSEM-7002-901" ~ "Oct 10-Oct 14",
  campus == "DN" & paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-") %in% c("HIST-4229-001", "HIST-5229-001") ~ "Oct 31-Nov 04",
  campus == "DN" & paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-") %in% c("HIST-4232-001", "HIST-5232-001") ~ "Nov 14-Nov 18",
  campus == "DN" & paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-") %in% c("PSCI-5084-ND1") ~ "Oct 24-Oct 28",
  campus == "DN" & paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-") %in% c("PSCI-5550-ND1") ~ "Oct 31-Nov 04",
  campus == "DN" & paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = "-") %in% c("INTS-3939-901") ~ "Nov 28-Dec 06",

# standard administration dates
    adminInd== 1 & between(fcqEnDt,"08/15/2022","08/23/2022") ~ "Aug 15-Aug 19",
    adminInd== 1 & between(fcqEnDt,"08/24/2022","08/30/2022") ~ "Aug 22-Aug 26",
    adminInd== 1 & between(fcqEnDt,"08/31/2022","09/06/2022") ~ "Aug 29-Sep 02",
    adminInd== 1 & between(fcqEnDt,"09/07/2022","09/13/2022") ~ "Sep 06-Sep 10",
    adminInd== 1 & between(fcqEnDt,"09/14/2022","09/20/2022") ~ "Sep 12-Sep 16",
    adminInd== 1 & between(fcqEnDt,"09/21/2022","09/27/2022") ~ "Sep 19-Sep 23",
    adminInd== 1 & between(fcqEnDt,"09/28/2022","10/04/2022") ~ "Sep 26-Sep 30",
    adminInd== 1 & between(fcqEnDt,"10/05/2022","10/11/2022") ~ "Oct 03-Oct 07",
    adminInd== 1 & between(fcqEnDt,"10/12/2022","10/18/2022") ~ "Oct 10-Oct 14",
    adminInd== 1 & between(fcqEnDt,"10/19/2022","10/25/2022") ~ "Oct 17-Oct 21",
    adminInd== 1 & between(fcqEnDt,"10/26/2022","11/01/2022") ~ "Oct 24-Oct 28",
    adminInd== 1 & between(fcqEnDt,"11/02/2022","11/08/2022") ~ "Oct 31-Nov 04",
    adminInd== 1 & between(fcqEnDt,"11/09/2022","11/15/2022") ~ "Nov 07-Nov 11",
    adminInd== 1 & between(fcqEnDt,"11/16/2022","11/22/2022") ~ "Nov 14-Nov 18",
  # Boulder/CEPS Final
    adminInd== 1 & between(fcqEnDt,"11/23/2022","12/31/2022") & campus %in% c("BD", "CE") ~ "Nov 28-Dec 06",
  # B3 Final
    adminInd== 1 & between(fcqEnDt,"11/23/2022","12/31/2022") & campus %in% c("B3") ~ "Nov 28-Dec 06",
  # Denver Final
    adminInd== 1 & between(fcqEnDt,"11/23/2022","12/31/2022") & campus %in% c("DN") & CAMPUS_CD !="AMC" ~ "Nov 28-Dec 06",
  # Anschutz Final
    adminInd== 1 & between(fcqEnDt,"11/23/2022","12/31/2022") & campus %in% c("MC") & CAMPUS_CD=="AMC" ~ "Nov 28-Dec 06"
  ))

###############################
# exception examples to use
  # BD-ENGR-GEEN
#    adminInd== 1 & between(mtgEndDt,"04/20/2022","05/31/2022")
#      & SBJCT_CD == "GEEN" ~ "Apr 18-Apr 29",
  # BD-ENGR-ASEN
#    adminInd== 1 & between(mtgEndDt,"04/20/2022","05/31/2022")
#      & SBJCT_CD == "ASEN" & CATALOG_NBR == "1400" ~ "Apr 18-Apr 29",
  # BD-ENGR-MCEN
#    adminInd== 1 & between(mtgEndDt,"04/20/2022","05/31/2022")
#      & SBJCT_CD == "MCEN" & CATALOG_NBR == "4085" ~ "Apr 18-Apr 29",

#   adminInd== 1 & campus == "DN" & ACAD_ORG_CD == "D-EDUC" &
#     SESSION_CD == "DMR" ~ "May 02-May 10",
#   adminInd== 1 & SBJCT_CD == "NCLL" & SESSION_CD == "DC1" ~ "Feb 28-Mar 04",
#   adminInd== 1 & campus == "BD" & ACAD_GRP_CD == "BUSN" &
#     SESSION_CD == "B81" ~ "Feb 21-Feb 25",
#   adminInd== 1 & campus == "CE" & SBJCT_CD == "MBAE" & SESSION_CD == "BM1"
#     ~ "Feb 07-Feb 11",
#   TRUE ~ "TBD"
# ))
################################

# create and format adminStartDt/adminEndDt columns
n17 <- n16 %>%
  mutate(adminStartDt = case_when(
    adminDtTxt != "" ~ substr(adminDtTxt, 1, 6),
    TRUE ~ "")) %>%
  mutate(adminEndDt = case_when(
    adminDtTxt != "" ~ substr(adminDtTxt, 8, 13),
    TRUE ~ "")) %>%
  mutate(adminStartDt = str_replace_all(adminStartDt, " ", "-")) %>%
  mutate(adminEndDt = str_replace_all(adminEndDt, " ", "-")) %>%
  mutate(adminStartDt = format(as.Date(adminStartDt, format = "%b-%d"), "%m/%d/%Y")) %>%
  mutate(adminEndDt = format(as.Date(adminEndDt, format = "%b-%d"), "%m/%d/%Y"))

clscu <- n17 %>%
  select(campus,deptOrgID,fcqdept,TERM_CD,INSTITUTION_CD,CAMPUS_CD,SESSION_CD,SBJCT_CD,CATALOG_NBR,CLASS_SECTION_CD,instrNum,instrNm,instrLastNm,instrFirstNm,instrMiddleNm,instrPersonID,instrConstituentID,instrEmplid,instrEmailAddr,INSTRCTR_ROLE_CD,ASSOCIATED_CLASS,assoc_class_secID,GRADE_BASIS_CD,totEnrl_nowd_comb,totEnrl_nowd,ENRL_TOT,ENRL_CAP,ROOM_CAP_REQUEST,CRSE_LD,LOC_ID,CLASS_STAT,crseSec_comp_cd,SSR_COMP_CD,combStat,spons_AcadGrp,spons_AcadOrg,spons_fcqdept,spons_deptOrgID,spons_id,SCTN_CMBND_CD,SCTN_CMBND_LD,INSTRCTN_MODE_CD,CLASS_TYPE,SCHED_PRINT_INSTR,GRADE_RSTR_ACCESS,minClassMtgNum,mtgStartDt,mtgEndDt,mtgStartTm,mtgEndTm,mtgDays,CLASS_START_DT,CLASS_END_DT,FCLTY_ID,FCLTY_LD,CLASS_NUM,ACAD_GRP_CD,ACAD_GRP_LD,ACAD_ORG_CD,ACAD_ORG_LD,totSchedPrintInstr_Y,totSchedPrintInstr_Not_Y,totInstrPerClass,totCrsePerInstr,totAssocClassPerInstr,indLEC,indNotLEC,totLECInstr,totNotLECInstr,cmbsecInfo,adminInd,fcqNote,indInstrNm,indDIS_IND_THE,indDEPT_RQST,indIndptStdy,indMinEnrl,indCombSect0,indCandDegr,indGiveOnlyLEC,indCrseTooLate,fcqStDt,fcqEnDt,adminDtTxt,adminStartDt,adminEndDt
    #,indBeijing,indCUSucceed,indEXSTD_MBA,indNoInstr,
  )

# print
write.csv(clscu, "L:\\mgt\\FCQ\\CourseAudit\\clscu_r.csv", row.names = FALSE)

# use to identify new subjects that are unassigned to an FCQ dept
nna <- n17 %>%
  filter(is.na(fcqdept))
