 #!/usr/bin/ksh

####################################################################################################################
# Script Name          - rita_erdm_endbatch.sh
# Description          - Unix Shell Script for update the Batch status table with end time
# Usage       		   - rita_erdm_endbatch.sh erdm
#Input Files needed 
#for Script execution  - erdm.param
#   Date              Author                  Modification              Description
#   ----              ------                  ------------              -----------
#  06-02-2015         k.sivaprakasam           Created                      N/A
#  04-03-2015         G.Praveen                Modified                 Parameters were taken from the Param file 
#                                                                        table structure get changed
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

BT_STATS="BATCH-COMPLETED"
BT_RUN_STATS="BATCH-RUNNING"

TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`;
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
BATCH_FILE_PATH=`getParam pBATCH_FILE_PATH`;
BATCH_FILE_NAME=`getParam Batch_File_Name`;
AUDIT_DATABASE=`getParam pDATABASE_NM`;
AUDIT_BTCH_STATS_TBL=`getParam pAUDIT_BTCH_STATS_TBL`;
ADOC_BTCH_PTH=`getParam pADOC_BTCH_PTH`;
ADOC_BTCH_FILE_NM=`getParam pADOC_BTCH_FILE_NM`;
LOG_DIR=`getParam pLOG_DIR`;
PROC_DIR=`getParam pFILE_LIST_PATH`;
TEMP_DIR=`getParam pTEMP_DIR`;
INFA_SESS_LOG=`getParam INFA_SESSION_LOG_DIR`;
INFA_WF_LOG=`getParam INFA_WF_LOG_DIR`;
INFA_CACH_LOG=`getParam INFA_CACHE_LOG_DIR`;
INFA_BAD_DIR=`getParam INFA_BAD_DIR`;
INFA_TEMPR_DIR=`getParam INFA_TEMPR_DIR`;

BTCH_ID=`head -1 $BATCH_FILE_PATH/$BATCH_FILE_NAME |awk -F '|' '{print $1}'`

#-------------------------------------------------------------------------#
# Removing log/temp/infa cache files older than 3 days from the run date  #
#-------------------------------------------------------------------------#

find ${LOG_DIR} -type f -name *.log -mtime +3 -ls -exec rm -f {} \;
find ${PROC_DIR} -type f -mtime +4 -ls -exec rm -f {} \;
find ${TEMP_DIR} -type f -mtime +4 -ls -exec rm -f {} \;


find ${INFA_SESS_LOG} -type f -mtime +2 -ls -exec rm -f {} \;
find ${INFA_WF_LOG} -type f -mtime +2 -ls -exec rm -f {} \;
find ${INFA_CACH_LOG} -type f -mtime +2 -ls -exec rm -f {} \;
find ${INFA_BAD_DIR} -type f -mtime +2 -ls -exec rm -f {} \;
find ${INFA_TEMPR_DIR} -type f -mtime +2 -ls -exec rm -f {} \;

#-----------------------------------------------------------------------#
# Calling_Teradata_bteq_and_inserting_data_in_batch_param_table         #
#-----------------------------------------------------------------------#

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];

UPDATE $AUDIT_DATABASE.$AUDIT_BTCH_STATS_TBL  SET STATUS='$BT_STATS',END_TM=CURRENT_TIMESTAMP(1) WHERE BATCH_ID=$BTCH_ID AND STATUS='$BT_RUN_STATS';

.quit 0;
.logoff;

[BTEQ-FLG]

#-----------------------------------------------------------------------#
# Deleting erdm.batch file                                              #
#-----------------------------------------------------------------------#

[ -f $BATCH_FILE_PATH/$BATCH_FILE_NAME ] && { rm -f $BATCH_FILE_PATH/$BATCH_FILE_NAME ; } 

#-----------------------------------------------------------------------#
# Deleting erdm.adhoc_batch file                                              #
#-----------------------------------------------------------------------#

[ -f $ADOC_BTCH_PTH/$ADOC_BTCH_FILE_NM ] && { rm -f $ADOC_BTCH_PTH/$ADOC_BTCH_FILE_NM ; } || { echo "No $ADOC_BTCH_PTH/$ADOC_BTCH_FILE_NM file OR No batch $BATCH_FILE_PATH/$BATCH_FILE_NAME file found";exit 0 ; } 

exit 0
#EOF
