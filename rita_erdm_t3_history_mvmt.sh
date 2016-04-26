#!/usr/bin/ksh

##############################################################################################################
# Script Name          - rita_erdm_t3_history_mvmt.sh
# Description          - The below script helps to moves the history data in RITA ERDM Tier 3 tables to its 
#                        respective history tables.
# Usage       		   - rita_erdm_t3_history_mvmt.sh erdm CN IRRS ACCOUNT CASA
#Input Files needed 
#for Script execution  - erdm.param
#   Date              Author                  Modification                        Description
#   ----              ------                  ------------                        -----------
#  14-05-2015         Venkat  P                Base Query & Business Logic            N/A
#  16-05-2015         Praveen G                Modified to RITA standard              N/A
#  
##############################################################################################################

set -x

#--Include the comm_functions.sh script to use the common functions------#

. common_functions.sh

#----------------------PRILIMINARY VALIDATION----------------------------#

[ $# -eq 5 ] || die "Error  : Insufficient Arguments .\nSyntax : $0 <project>  \nUsage  : $0 <PROJECT> <SRC SYSTEM>"

#-----------Project Configuration---------------------------------------#
project=${1}
. ${project}.config

#----------Parameter validation-----------------------------------------#
[ $1 = $project_name ] || die "Not Valid Project Name .\nSyntax : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM>"

[ "$2" = "CN" -o "$2" = "VN" -o "$2" = "KH" -o "$2" = "PG" -o "$2" = "HK" -o "$2" = "LA" -o "$2" = "US" -o "$2" = "UK" -o "$2" = "ML" -o "$2" = "MB" -o "$2" = "ZZ" -o "$2" = "XX" -o "$2" = "MY" -o "$2" = "ID" -o "$2" = "PH" ] || die "Not Valid country code \nSyntax : $0 <project> <country code> <Downstream system>\nUsage  : $0 Not proper Country code"

[ "$3" = "ICBA" -o "$3" = "SIBS" -o "$3" = "TRMS" -o "$3" = "IRRS" -o "$3" = "GCMS" ] || die "Error: Not Valid Source System .\nSyntax : $0 <project>  \nUsage  : $0 Not proper Src sys code"

[ "$4" = "ACCOUNT" -o "$4" = "FACILITY" -o "$4" = "FOREX" -o "$4" = "DERIVATIVE" -o "$4" = "WHOLESALE" -o "$4" = "INVESTMENT" ] || die "Error: Not Valid T3 table name .\nSyntax : $0 <project>  \nUsage  : $0 Not proper T3 table name"

[ "$5" = "CASA" -o "$5" = "FD" -o "$5" = "LOAN" -o "$5" = "TF" -o "$5" = "NA" ] || die "Error: Not Valid account type .\nSyntax : $0 <project>  \nUsage  : $0 Not proper account type"

#----------Global Variable Assignment------------------------------------#

project=$project_name;
PCTRY_CD=$2;
PSRCSYS=$3;
TBL_NAME=$4;
PACCT_TYP=$5;
TBL_NAME_PREFIX="ERDM_"$TBL_NAME;
FLR_COND_COL=FILE_NM;
FLR_COND_VAL=1;

TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
FILE_LIST_PATH=`getParam pFILE_LIST_PATH`;
T3_DATABASE_NM=`getParam pT3_DATABASE_NM`
SQL_FILE_PATH=`getParam pSQL_FILE_PATH`
BATCH_ID=`head -1 $batch_file |awk -F '|' '{print $1}'`
BUZ_DT=`head -1 $batch_file |awk -F '|' '{print $3}'`
CTRY_SYS_FILE_NM_WTH_PTH=$FILE_LIST_PATH/${PCTRY_CD}_${PSRCSYS}_T3_MTH_END_MVMT_${TBL_NAME_PREFIX}_${BUZ_DT}.sql.exec;
AUDIT_DATABASE_NM=`getParam pDATABASE_NM`
AUDIT_SRC_FILE_AUDIT_TBL=`getParam pAUDIT_SRC_FILE_AUDIT_TBL`
MONTH_END_FLG=`head -1 $batch_file |awk -F '|' '{print $4}'`
#--------------------------------------------------------------# 
#
# 	TIER 3 TABLE NM      MONTHEND SNAPSHOT 
#	 ---------------       ---------------
# 	 ERDM_ACCOUNT     --> ACCOUNT_ME
#	 ERDM_FACILITY    --> FACILITY_ME
#	 ERDM_FOREX       --> FOREX_ME
#	 ERDM_DERIVATIVE  --> DERIVATIVE_ME
#	 ERDM_WHOLESALE   --> WHOLESALE_ME
#	 ERDM_INVESTMENT  --> INVESTMENT_ME
#
#------PREDICATE-FORMATION-FOR-TIER 3 TABLE------------------#

#echo "FILE_NM like '%_CA%' OR FILE_NM like '%_SA%'"

[ "$MONTH_END_FLG" = "N" ] && { exit 0 ; };

case $PACCT_TYP in 
	CASA)
	     [ "$PSRCSYS" = "ICBA" -a "$TBL_NAME" = "ACCOUNT" ] && { PRED_ACCT_TYPPATTERN=" AND FILE_NM like '%_CA%' OR FILE_NM like '%_SA%'"; }
		 [ "$PSRCSYS" = "SIBS" -a "$TBL_NAME" = "ACCOUNT" ] && { PRED_ACCT_TYPPATTERN=" AND FILE_NM like '%_DD%'" ;}
		 [ "$PSRCSYS" = "ICBA" -a "$TBL_NAME" = "FACILITY" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_CAMAST%'"; }
		 [ "$PSRCSYS" = "SIBS" -a "$TBL_NAME" = "FACILITY" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_CASA%'" ;}
	;;
	FD)
	     [ "$PSRCSYS" = "ICBA" -a "$TBL_NAME" = "ACCOUNT" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_FD%'" ;}
		 [ "$PSRCSYS" = "SIBS" -a "$TBL_NAME" = "ACCOUNT" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_CD%' " ;}	 
	;;
	LOAN)
	     [ "$PSRCSYS" = "ICBA"  -a "$TBL_NAME" = "ACCOUNT" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_LOAN%'" ;}
		 [ "$PSRCSYS" = "SIBS"  -a "$TBL_NAME" = "ACCOUNT" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_LN%'" ;}
		 [ "$PSRCSYS" = "ICBA" -a "$TBL_NAME" = "FACILITY" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_LOANMAST%'"; }
		 [ "$PSRCSYS" = "SIBS" -a "$TBL_NAME" = "FACILITY" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_LNMAST%'" ;}
	;;
	TF)
	     [ "$PSRCSYS" = "ICBA" -o "$PSRCSYS" = "SIBS" ] && { PRED_ACCT_TYPPATTERN=" AND FILE_NM like '%_TF%'" ;}
		 [ "$PSRCSYS" = "ICBA" -a "$TBL_NAME" = "FACILITY" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_TFMAST%'"; }
		 [ "$PSRCSYS" = "SIBS" -a "$TBL_NAME" = "FACILITY" ] && { PRED_ACCT_TYPPATTERN="AND FILE_NM like '%_TFMAST%'" ;}
	;;
	NA)
		 PRED_ACCT_TYPPATTERN=""
	;;
	*)echo "NOT VALID HEADER TRAILER FORMAT";;
esac

#------Executable SQL File Preparation-----------------------#

sed -e "s/T3_DATABASE_NM/$T3_DATABASE_NM/g" -e "s/SYSTM_NM/$PSRCSYS/g" -e "s/FLR_COND_VAL/$FLR_COND_VAL/g" -e "s/CNTY_CD/$PCTRY_CD/g" -e "s/PRED_ACCT_TYPPATTERN/$PRED_ACCT_TYPPATTERN/g" -e "s/BIZ_DATE/'$BUZ_DT'/g"<${SQL_FILE_PATH}T3_ME_MVMT_${TBL_NAME_PREFIX}.sql > ${CTRY_SYS_FILE_NM_WTH_PTH}

#-- Frame the audit update DML statement

AUDIT_UPDATE_SUCCESS="UPDATE "$AUDIT_DATABASE_NM"."$AUDIT_SRC_FILE_AUDIT_TBL" SET STATUS='PASSED',MDFY_TIME=CURRENT_TIMESTAMP(1),REASON_DESC='RECORD MOVED SUCESSFULLY' WHERE BATCH_ID=$BATCH_ID AND CTRY_CD='"$PCTRY_CD"' AND SRC_SYS_NM='"$PSRCSYS"' AND IN_FILE_NM LIKE '%"$TBL_NAME"%"$PACCT_TYP"%'";
AUDIT_UPDATE_FAILED="UPDATE "$AUDIT_DATABASE_NM"."$AUDIT_SRC_FILE_AUDIT_TBL" SET STATUS='FAILED',MDFY_TIME=CURRENT_TIMESTAMP(1),REASON_DESC='RECORD NOT MOVED' WHERE BATCH_ID=$BATCH_ID AND CTRY_CD='"$PCTRY_CD"' AND SRC_SYS_NM='"$PSRCSYS"' AND IN_FILE_NM LIKE '%"$TBL_NAME"%"$PACCT_TYP"%'";

#--------Executable Portion---------------------------------#

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];

	SELECT TOP 1 1 FROM  $T3_DATABASE_NM.ERDM_ACCOUNT WHERE '$TBL_NAME'='ACCOUNT'; 

	.IF ACTIVITYCOUNT >0 THEN .GOTO  EXEC_EXP_LOG;

	SELECT TOP 1 1 FROM $T3_DATABASE_NM.ERDM_FACILITY WHERE '$TBL_NAME'='FACILITY' ; 

	.IF ACTIVITYCOUNT >0 THEN .GOTO  EXEC_EXP_LOG;

	SELECT TOP 1 1 FROM  $T3_DATABASE_NM.ERDM_FOREX WHERE '$TBL_NAME'= 'FOREX' ; 

	.IF ACTIVITYCOUNT >0 THEN .GOTO EXEC_EXP_LOG;

	SELECT TOP 1 1 FROM  $T3_DATABASE_NM.ERDM_DERIVATIVE WHERE  '$TBL_NAME' = 'DERIVATIVE' ; 
	
    .IF ACTIVITYCOUNT >0 THEN .GOTO EXEC_EXP_LOG;	

	SELECT TOP 1 1 FROM  $T3_DATABASE_NM.ERDM_WHOLESALE WHERE  '$TBL_NAME' = 'WHOLESALE' ; 

	.IF ACTIVITYCOUNT >0 THEN .GOTO  EXEC_EXP_LOG;
	
	SELECT TOP 1 1 FROM  $T3_DATABASE_NM.ERDM_INVESTMENT WHERE  '$TBL_NAME' = 'INVESTMENT' ;

	.IF ACTIVITYCOUNT >0 THEN .GOTO  EXEC_EXP_LOG;	

.LABEL EXEC_EXP_LOG

	 .RUN FILE=${CTRY_SYS_FILE_NM_WTH_PTH}
	 
	.IF ERRORCODE = 0 THEN  $AUDIT_UPDATE_SUCCESS

	.IF ERRORCODE <> 0 THEN $AUDIT_UPDATE_FAILED
    	
.EXPORT RESET;

.QUIT;

.LOGOFF;

[BTEQ-FLG]

#------------------------------------------------------------#
#.RUN FILE=${CTRY_SYS_FILE_NM_WTH_PTH}
#EOF