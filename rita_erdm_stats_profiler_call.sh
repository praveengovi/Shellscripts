 #!/usr/bin/ksh

####################################################################################################################
# Script Name          - rita_erdm_stats_profiler_call.sh
# Description          - Unix Shell Script for calling profiler
# Usage       		   - rita_erdm_stats_profiler_call.sh
#Input Files needed 
#for Script execution  - RITA.param
#   Date              Author                  Modification              Description
#   ----              ------                  ------------              -----------
#  02-06-2015         Sathiyanathan             Created                      N/A
###################################################################################################################

#set -x

#-----------------------------------------------------------------------#
# Include the comm_functions.sh script to use the common functions
#-----------------------------------------------------------------------#

. common_functions.sh

[ $# -eq 2 ] || { die "Error  : Insufficient Arguments .\nSyntax : $0 <project> <ISO Country code> \nUsage  : $0 erdm CN" ; }

project=${1}
. ${project}.config

RUN_PARAM=$2

TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`;
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
AUDIT_DATABASE=`getParam pDATABASE_NM`;
AUDIT_BTCH_STATS_TBL=`getParam pAUDIT_BTCH_STATS_TBL`;
SQL_FILE_PATH=`getParam pSQL_FILE_PATH`;
PROFILER_SQL_FILE=profiler_country_level_${2}.sql;
STATS_SQL_FILE=collect_stats.sql;

#---Calling_Teradata_bteq_and_inserting_data_in_batch_param_table----#

if [ $RUN_PARAM = 'STATS' ] 
then 
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.RUN FILE=${SQL_FILE_PATH}${STATS_SQL_FILE}
.quit 0;
.logoff;
[BTEQ-FLG]
exit 0
else 
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.RUN FILE=${SQL_FILE_PATH}${PROFILER_SQL_FILE}
.export reset;
.quit;
.logoff;
[BTEQ-FLG]
exit 0
fi;
