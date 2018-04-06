 #!/usr/bin/ksh
####################################################################################################################
#
# Script Name          - RITA_FileInsertionScript.sh
#
# Description          - Unix Shell Script for insert records into batch tables into daily basis
#
# Usage       		   - RITA_FileInsertionScript.sh
#
#Input Files needed 
#for Script execution  - RITA.param
#
#   Date              Author                  Modification              Description
#   ----              ------                  ------------              -----------
#  06-02-2015         k.sivaprakasam           Created                      N/A
#  04-03-2015         G.Praveen                Modified                 Parameters were taken from the Param file 
#                                                                       table structure get changed
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
AUDIT_SRC_FILE_AUDIT_TBL=`getParam pAUDIT_SRC_FILE_AUDIT_TBL`;
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
	MST_TBL.CTRY_CD,
	MST_TBL.SRC_SYS_NM,
    Oreplace(MST_TBL.IN_FILE_NM,'YYYYMMDD',Oreplace(cast (BATCH_ST_TBL.BIZ_DT as varchar(12)),'-','') ) as IN_FILE_NM,
	Oreplace(MST_TBL.ZIP_OUT_FILE_NM,'YYYYMMDD',Oreplace(cast (BATCH_ST_TBL.BIZ_DT as varchar(12)),'-','') ) as ZIP_OUT_FILE_NM,
	CASE WHEN ( SRC_SYS_NM='RWA' OR SRC_SYS_NM='GCM') THEN Oreplace(MST_TBL.VALID_OUT_FILE_NM,'YYYYMMDD',Oreplace(cast (BATCH_ST_TBL.BIZ_DT as varchar(12)),'-','') ) ELSE MST_TBL.VALID_OUT_FILE_NM END AS VALID_OUT_FILE_NM,
	MST_TBL.CTL_FILE_FLG,
	MST_TBL.IS_COMP_FILE,
	MST_TBL.FILE_DIR,
	MST_TBL.TARGET_DIR,
	MST_TBL.WRKFLW_NM,
	'' AS STATUS,
	'' AS REC_CNT,
	NULL,
	'' AS REASON_DESC,
	MST_TBL.TRNSFR_IND
FROM 
$AUDIT_DATABASE.$AUDIT_SRC_FILE_MSTR_TBL MST_TBL LEFT JOIN 
( SELECT *  FROM $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL QUALIFY ROW_NUMBER() OVER (PARTITION BY BATCH_ID ORDER BY BATCH_ID DESC)=1
WHERE STATUS='BATCH-RUNNING') BATCH_ST_TBL
 ON ( 1=1 )  WHERE MST_TBL.FREQ IN ($todays_pick)
 and MST_TBL.CATEGORY in ('FILE','ARCHIVAL','DATA_MVMT');
 
.logoff;

[BTEQ-FLG]

