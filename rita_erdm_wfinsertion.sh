 #!/usr/bin/ksh
####################################################################################################################
#
# Script Name          - rita_erdm_wfinsertion.sh
#
# Description          - Unix Shell Script for insert records into batch tables into daily basis
#
# Usage       		   - sh rita_erdm_wfinsertion.sh erdm
#
#Input Files needed 
#for Script execution  - erdm.param
#
#   Date              Author                  Modification              Description
#   ----              ------                  ------------              -----------
# 18-05-2015		Vignesh M											Initial version of script to load workflow audit table
# 
###################################################################################################################
set -x
#-----------------------------------------------------------------------#
# Include the comm_functions.sh script to use the common functions
#-----------------------------------------------------------------------#

. common_functions.sh

[ $# -eq 1 ] || die "Error  : Insufficient Arguments .\nSyntax : $0 <project>  \nUsage  : $0 erdm"

#-----------------------------------------------------------------------#
# Invoke the Project Config for Project level variables
#-----------------------------------------------------------------------#

project=${1}
. ${project}.config

todays_pick="'D'"
TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`;
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
AUDIT_DATABASE=`getParam pDATABASE_NM`;
AUDIT_BTCH_STATS_TBL=`getParam pAUDIT_BTCH_STATS_TBL`;
AUDIT_SRC_FILE_MSTR_TBL=`getParam pAUDIT_SRC_FILE_MSTR_TBL`;
AUDIT_SRC_FILE_AUDIT_TBL=`getParam pAUDIT_WORKFLOW`;
bus_date=`head -1 $batch_file |awk -F '|' '{print $3}'`
MTH_END_IND=`head -1 $batch_file |awk -F '|' '{print $4}'`

[ $MTH_END_IND = "N" ] && { todays_pick="'D'" ; } || { todays_pick=$todays_pick",'M'" ;}

#-----------------------------------------------------------------------#
# Calling_Teradata_bteq_and_inserting_data_in_batch_param_table         #
#-----------------------------------------------------------------------#

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];


INSERT INTO $AUDIT_DATABASE.$AUDIT_SRC_FILE_AUDIT_TBL 
SELECT 
	BATCH_ST_TBL.BATCH_ID,
	BATCH_ST_TBL.BATCH_DT,
	DATE AS LOAD_DT,
	MST_TBL.CTRY_CD,
	MST_TBL.TIER_LEVEL,
	MST_TBL.SRC_SYS_NM,
    MST_TBL.WRKFLW_NM,
	NULL AS WF_START_TIME,
	NULL AS WF_END_TIME,
	MST_TBL.FREQ,
	'' AS STATUS
FROM 
$AUDIT_DATABASE.$AUDIT_SRC_FILE_MSTR_TBL MST_TBL LEFT JOIN 
 ( SELECT *  FROM $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL QUALIFY ROW_NUMBER() OVER (PARTITION BY BATCH_ID ORDER BY BATCH_ID DESC)=1
 WHERE STATUS='BATCH-RUNNING') BATCH_ST_TBL
 ON ( 1=1 )  WHERE MST_TBL.FREQ IN ($todays_pick)
AND MST_TBL.CATEGORY='WORKFLOW' ;
.logoff;

[BTEQ-FLG]

