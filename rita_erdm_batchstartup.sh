 #!/usr/bin/ksh

####################################################################################################################
# Script Name          - RITA_BatchStartUp.sh
# Description          - Unix Shell Script for batch Start up
# Usage       		   - RITA_BatchStartUp.sh 
#Input Files needed 
#for Script execution  - RITA.param
#   Date              Author                  Modification              Description
#   ----              ------                  ------------              -----------
#  06-02-2015         k.sivaprakasam           Created                      N/A
#  04-03-2015         G.Praveen                Modified                 Parameters were taken from the Param file 
#                                                                       table structure get changed
###################################################################################################################

#set -x

#-----------------------------------------------------------------------#
# Include the comm_functions.sh script to use the common functions
#-----------------------------------------------------------------------#

. common_functions.sh

[ $# -eq 1 ] || { die "Error  : Insufficient Arguments .\nSyntax : $0 <project>  \nUsage  : $0 erdm" ; }

project=${1}
. ${project}.config


PARAM_FILE_PATH=`echo $Param_File`
STATS="BATCH-RUNNING"
STATS_CMP="BATCH-COMPLETED"
Parameter_Dir=`getParam pTD_LOG_ON_PATH`

echo "$Parameter_Dir"

BATCH_FILE_PATH=`getParam pBATCH_FILE_PATH`;
TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`;
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
AUDIT_DATABASE=`getParam pDATABASE_NM`;
AUDIT_BTCH_STATS_TBL=`getParam pAUDIT_BTCH_STATS_TBL`;
ADOC_BTCH_PTH=`getParam pADOC_BTCH_PTH`;
ADOC_BTCH_FILE_NM=`getParam pADOC_BTCH_FILE_NM`;
MTH_END_FLG="N";

[ -f $ADOC_BTCH_PTH/$ADOC_BTCH_FILE_NM ] && {
 cat $ADOC_BTCH_PTH/$ADOC_BTCH_FILE_NM |awk -F '|' '{print $1" "$2}'|read BIZ_DATE MTH_END_FLG ;
} || 
{ 
BIZ_DATE=DATE-1;
[ $(date '+%d') -eq 1 ] && { MTH_END_FLG="Y"; };
}


#---Calling_Teradata_bteq_and_inserting_data_in_batch_param_table----#

[ -f $BATCH_FILE_PATH$project.BATCH ] &&  { die "Error : Earlier batch did not complete yet. $pBATCH_FILE_PATH$project.BATCH is still available" ; }

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];

select * from $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL WHERE BATCH_ID <> 0;

.if ACTIVITYCOUNT = 0  THEN  INSERT INTO $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL (BATCH_ID,BATCH_DT,BIZ_DT,MONTH_END_IND,STATUS,STRT_TM) VALUES (CAST('1' AS INT),DATE,$BIZ_DATE,'$MTH_END_FLG','$STATS',CURRENT_TIMESTAMP(1)); 

.if ACTIVITYCOUNT <> 0 THEN INSERT INTO $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL SELECT ((SELECT MAX(BATCH_ID) FROM $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL ) +CAST('1' AS INT)) AS BATCH_ID ,DATE,$BIZ_DATE,'$MTH_END_FLG','$STATS',CURRENT_TIMESTAMP(1),null from  $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL X  WHERE X.BATCH_ID=0 AND  1=(SELECT COUNT(1)  FROM $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL WHERE STATUS='$STATS_CMP' AND BATCH_ID=(SELECT MAX(BATCH_ID) FROM  $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL ));

SELECT ((SELECT MAX(BATCH_ID) FROM $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL ) +CAST('1' AS INT)) AS BATCH_ID ,DATE,$BIZ_DATE,'$MTH_END_FLG','$STATS',CURRENT_TIMESTAMP(1),null from  $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL X  WHERE X.BATCH_ID=0 AND  1=(SELECT COUNT(1)  FROM $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL WHERE STATUS='$STATS_CMP' AND BATCH_ID=(SELECT MAX(BATCH_ID) FROM  $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL ));

.if ACTIVITYCOUNT = 0  THEN SELECT 'EXISTING-BATCH NOT YET COMPLETED - PLEASE TRIGGER ONCE ITS DONE' 

.export file=$BATCH_FILE_PATH$project.BATCH;

SELECT (TRIM(BATCH_ID)||'|'||CAST( BATCH_DT  AS  date  Format  'yyyy-mm-dd')||'|'||CAST( BIZ_DT  AS  date  Format  'yyyy-mm-dd')||'|'||MONTH_END_IND||'|'||ADD_MONTHS( DATE - EXTRACT(DAY FROM DATE), 0 ) ) (title'') FROM $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL WHERE  BATCH_ID=(SELECT MAX(BATCH_ID) FROM $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL);
 
 .export reset;

.quit 0;

.logoff;

[BTEQ-FLG]
