#!/usr/bin/ksh

##############################################################################################################
# Script Name          - rita_erdm_Housekeeping.sh
# Description          - The below script help us to truncate ERDM RITA tier 1 table
#
# Usage       		   - rita_erdm_tier1_cleanup.sh erdm CN ICBA
#Input Files needed 
#for Script execution  - erdm.param
#   Date              Author                  Modification                        Description
#   ----              ------                  ------------                        -----------
#  09-05-2015         Praveen G                Created                                N/A
#  05-06-2015		  Vignesh MB			   Modified IF ELSE Logic				
##############################################################################################################

set -x

#--Include the comm_functions.sh script to use the common functions------#

. common_functions.sh

#----------------------PRILIMINARY VALIDATION----------------------------#

[ $# -eq 3 ] || die "Error  : Insufficient Arguments .\nSyntax : $0 <project> <PCTRY_CD> <SRC SYSTEM> \nUsage  : $0 <CNTRY_CDE> <SYS_NAME>"

#-----------Project Configuration---------------------------------------#
project=${1}
. ${project}.config

[ $1 = $project_name ] || die "Not Valid Project Name .\nSyntax : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM> <PATTERN>"



[ "$2" = "CN" -o "$2" = "VN" -o "$2" = "KH" -o "$2" = "PG" -o "$2" = "HK" -o "$2" = "LA" -o "$2" = "US" -o "$2" = "UK" -o "$2" = "ML" -o "$2" = "MB" -o "$2" = "ZZ" -o "$2" = "XX" -o "$2" = "GLO" -o "$2" = "MY" -o "$2" = "ID" -o "$2" = "PH" ] || die "Not Valid country code \nSyntax : $0 <project> <country code> <Downstream system>\nUsage  : $0 Not proper Country code"

[ "$3" = "ICBA" -o "$3" = "SIBS" -o "$3" = "TRMS" -o "$3" = "IRRS" -o "$3" = "GCMS" -o "$3" = "INTL" ] || die "Error  : Not Valid Source System .\nSyntax : $0 <project>  \nUsage  : $0 Not proper Src sys code"

#-------------Variable Assignment---------------------------------------#

#--Filter criteria for MY(Labuan)

if [  "$2" = "ML" -o "$2" = "MB" ]
then
	PCTRY_CD=MY
	if [ "$2" = "ML" ] 
	then
	MY_FILTERCRT=" AND FILE_NM LIKE ''MY_%''"
	else
	MY_FILTERCRT=" AND FILE_NM LIKE ''MY1_%''"
	fi
else
	PCTRY_CD=$2
	MY_FILTERCRT=""
fi

CNTR_FIL_NM=$2
PSRC_SYS=$3;
ARH_CTRY_CD=`getParam pARH_CTRY_CD`
project=$project_name;
BATCH_ID=`head -1 $batch_file |awk -F '|' '{print $1}'`
BUZ_DT=`head -1 $batch_file |awk -F '|' '{print $3}'`

TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
FILE_LIST_PATH=`getParam pFILE_LIST_PATH`;
FILE_LIST_WTH_PTH=$FILE_LIST_PATH/${PSRC_SYS}_${CNTR_FIL_NM}_${BUZ_DT}_DATA_REMOVAL.sql;

AUDIT_DB_NM=`getParam pDATABASE_NM`;
AUDIT_TBL_NM=`getParam pAUDIT_SRC_FILE_AUDIT_TBL`;
ERDM_DATA_PURGE_TBL=`getParam pERDM_DATA_PURGE_TBL`;
T3_DATABASE_NM=`getParam pT3_DATABASE_NM`;

#---------------------DB LOGON VARIABLES-------------------------------#

T1_DATABASE_NM=`getParam pT1_DATABASE_NM`;

#----------DELETE_QUERY_PREPERATION & EXECUTION-------------------------#

[ -f $FILE_LIST_WTH_PTH ] && { rm $FILE_LIST_WTH_PTH ; }

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.EXPORT FILE=$FILE_LIST_WTH_PTH;
.SET WIDTH 50000;
DATABASE $T1_DATABASE_NM;

SELECT 
('DELETE  FROM '||TRIM(PRG.DBNAME)||'.'||TRIM(PRG.TBNAME)||' WHERE '||TRIM(COL.COLUMNNAME)||'='||'''$PCTRY_CD'''||'$MY_FILTERCRT'||';')  (title'')
FROM 
	$AUDIT_DB_NM.$ERDM_DATA_PURGE_TBL PRG,
	DBC.COLUMNS COL
WHERE 
	TRIM(PRG.TBNAME)=TRIM(COL.TABLENAME) AND 
	TRIM(PRG.DBNAME)=TRIM(COL.DATABASENAME) AND 
	TRIM(COL.COLUMNNAME) LIKE '%COUNTRY%CODE%' AND 
	PRG.SRC_SYS_NM='$PSRC_SYS';
	
.EXPORT RESET;
.RUN FILE=$FILE_LIST_WTH_PTH;
IF ERRORCODE = 0  THEN .GOTO REPORT_SUCCESS 
ELSE 
UPDATE $AUDIT_DB_NM.$AUDIT_TBL_NM SET STATUS='FAILED',MDFY_TIME=CURRENT_TIMESTAMP(1),REASON_DESC='$PSRC_SYS TABLES NOT TRUNCATED SUCESSFULLY' WHERE CTRY_CD='$ARH_CTRY_CD' AND BATCH_ID=$BATCH_ID AND  SRC_SYS_NM='$PSRC_SYS';

.LABEL REPORT_SUCCESS
UPDATE $AUDIT_DB_NM.$AUDIT_TBL_NM SET STATUS='PASSED',MDFY_TIME=CURRENT_TIMESTAMP(1),REASON_DESC='$PSRC_SYS TABLES TRUNCATED SUCESSFULLY' WHERE CTRY_CD='$ARH_CTRY_CD' AND BATCH_ID=$BATCH_ID AND  SRC_SYS_NM='$PSRC_SYS';

.QUIT 0;
.LOGOFF;
[BTEQ-FLG]

#EOF