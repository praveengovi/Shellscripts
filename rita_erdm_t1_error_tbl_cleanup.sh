#!/usr/bin/ksh

##############################################################################################################
# Script Name          - rita_erdm_Housekeeping.sh
# Description          - The below script help us to truncate ERDM RITA tier 1 table
#
# Usage       		   - rita_erdm_Housekeeping.sh erdm CN ICBA
#Input Files needed 
#for Script execution  - erdm.param
#   Date              Author                  Modification                        Description
#   ----              ------                  ------------                        -----------
#  09-05-2015         Praveen G                Created                                N/A
#  
##############################################################################################################

set -x

#--Include the comm_functions.sh script to use the common functions------#

. /rita/project/common/scripts/common_functions.sh

#----------------------PRILIMINARY VALIDATION----------------------------#

[ $# -eq 1 ] || die "Error  : Insufficient Arguments .\nSyntax : $0 <project> <PCTRY_CD> <SRC SYSTEM> \nUsage  : $0 CN ICBA"

#-----------Project Configuration---------------------------------------#

. /rita/project/common/config/erdm.config

#[ $1 = $project_name ] || die "Not Valid Project Name .\nSyntax : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM> <PATTERN>"

#-------------Variable Assignment---------------------------------------#
ARH_CTRY_CD=`getParam pARH_CTRY_CD`
project=$project_name;
BATCH_ID=`head -1 $batch_file |awk -F '|' '{print $1}'`
BUZ_DT=`head -1 $batch_file |awk -F '|' '{print $3}'`

TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
FILE_LIST_PATH=`getParam pFILE_LIST_PATH`;
FILE_LIST_WTH_PTH=$FILE_LIST_PATH/${PSRC_SYS}_${BUZ_DT}_DATA_REMOVAL.sql;

AUDIT_DB_NM=`getParam pDATABASE_NM`;
AUDIT_TBL_NM=`getParam pAUDIT_SRC_FILE_AUDIT_TBL`;
ERDM_DATA_PURGE_TBL=`getParam pERDM_DATA_PURGE_TBL`;
T3_DATABASE_NM=`getParam pT3_DATABASE_NM`;
ERR_TBL=$1;


#---------------------DB LOGON VARIABLES-------------------------------#

T1_DATABASE_NM=`getParam pT1_DATABASE_NM`;

#----------DELETE_QUERY_PREPERATION & EXECUTION-------------------------#


bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.SET WIDTH 50000;
DATABASE $T1_DATABASE_NM;

SELECT   1 FROM DBC.TABLES WHERE TABLENAME='$ERR_TBL'  AND DATABASENAME='$T1_DATABASE_NM';

.IF ACTIVITYCOUNT =0 THEN GOTO OK

DROP TABLE $T1_DATABASE_NM.$ERR_TBL;

.LABEL OK  


.QUIT 0;
.LOGOFF;
[BTEQ-FLG]

exit 0;

#EOF