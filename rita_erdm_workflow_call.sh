#!/usr/bin/sh
#_______________________________________________________________________________________________________________________#
#																														#
# Author		: Vignesh Meyyappan																						#
# Date			: 16/03/2015																							#
# Description	: The scripts runs the workflow within ETL server based on the parameters passed (Project Name, 		#
#				  Workflow name, Source System, and instance_name)														#
# Usage			: sh rita_erdm_workflow_call.sh erdm <WF_NAME> <T1 or T3> <Instance Name>								#
# Modification History:																									#
# Name									Date									Change Details							#
# Vignesh M								25-04-2015								Changes to trigger workflow only when	#
#																				all the files are available for the 	#
#																				specified workflow & added DB updates	#
#_______________________________________________________________________________________________________________________#
set -x
#-----------------------------------------------------------------------#
# Include the comm_functions.sh script to use the common functions
#-----------------------------------------------------------------------#

. common_functions.sh

[ $# -eq 4 ] || die "Error  : Insufficient Arguments .\nSyntax : $0 <project>  \nUsage  : sh $0 erdm <wf_name> <T1 or T3> <INSTANCE_NAME>"

#-----------------------------------------------------------------------#
# Invoke the Project Config for Project level variables
#-----------------------------------------------------------------------#

project=${1}
. ${project}.config

#-----------------------------------------------------------------------#
# Parameter/Dynamic Argument declaration within scripts
#-----------------------------------------------------------------------#

current_date=$(date '+%F')

param_root_location=`getParam param_root_location`;
script_location=`getParam script_location`;
param_temp_location=`getParam pTEMP_DIR`;
LogFilelocation=`getParam pLOG_DIR`;
InfaRootLocation=`getParam InfaRootLocation`;
Batch_File_Location=`getParam pBATCH_FILE_PATH`;
source_file_path=`getParam pSRC_DIR`;
Batch_File_Name=`getParam Batch_File_Name`;
sql_file_nm=`getParam sql_file_nm`;
TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`;
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
audit_table=`getParam pAUDIT_WORKFLOW`;
audit_schema=`getParam AUDIT_SCHMA`;
sql_location=`getParam pSQL_FILE_PATH`;
integration_service=`getParam integration_service`;
domain=`getParam infa_domain`;
username=`getParam infa_username`;
sec_domain=`getParam sec_domain`;
connection_t1=`getParam connection_t1`;
connection_t3=`getParam connection_t3`;
Folder_T1=`getParam Folder_T1`;
Folder_T3=`getParam Folder_T3`;

max_chk_iterations=`getParam max_chk_iterations`;
count_of_run=1
sleep_time=`getParam sleeping_query_exe_time`;

workflow_name=$2
src_system=$3
instance_name=$4
LogFileName=$workflow_name"_"$current_date.log
ParamFile_Tier1_Static=ERDM_TIER1_TMPLT.par
ParamFile_Tier3_Static=ERDM_TIER3_TMPLT.par
ParamFileName_T1="ERDM_TIER1_"$src_system"_"$instance_name.par
ParamFileName_T3="ERDM_TIER3_"$src_system"_"$instance_name.par
result=0
BSN_DT=`tail -1 $Batch_File_Location/$Batch_File_Name | cut -d '|' -f 3`
BATCH_ID=`tail -1 $Batch_File_Location/$Batch_File_Name | cut -d '|' -f 1`
INTL_BATCH_DT=`tail -1 $Batch_File_Location/$Batch_File_Name | cut -d '|' -f 5`
INTL_DT=`date +%Y-%m-01`
if [ "$workflow_name" = 'wf_ERDM_INTERNATIONAL_STG' ]
then
BATCH_DT=$INTL_DT
else
BATCH_DT=`tail -1 $Batch_File_Location/$Batch_File_Name | cut -d '|' -f 2`
fi

echo 'Initiating Validation & Execution for :' $workflow_name > $LogFilelocation/$LogFileName

echo 'Preparing SQL statements and executing via BTEQ, Please refer to BTEQ logs below for more information' >> $LogFilelocation/$LogFileName
#-----------------------------------------------------------------------------------------------------#
# Validating in Audit table whether all the files are arrived for corresponding Tier 1 workflow
#-----------------------------------------------------------------------------------------------------#
if [ "$src_system" = 'T1' ]
then
	cat $sql_location/$sql_file_nm | sed 's,ABCDEF,'$workflow_name',g' | sed 's,XXXXXX,'$BATCH_DT',g' | sed 's,CCCCC,'$instance_name',g' > $param_temp_location/VALIDATE_${workflow_name}_${instance_name}_${current_date}.sql
while [ "$max_chk_iterations" -ge "$count_of_run" ]
do
cat /dev/null > ${param_temp_location}/erdm_wkflow_trigger_${workflow_name}_${instance_name}.txt;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE  <<[BTEQ-FLG]  >> $LogFilelocation/$LogFileName;
.export file=${param_temp_location}/erdm_wkflow_trigger_${workflow_name}_${instance_name}.txt;
.RUN FILE=${param_temp_location}/VALIDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.export reset;
.QUIT 0;
.logoff;
[BTEQ-FLG]
result=$(cat $param_temp_location/erdm_wkflow_trigger_${workflow_name}_${instance_name}.txt | sed 's, *,,g')
[ "$result" -eq 1 ] && { echo 'All the files are available for triggering wf: '${workflow_name} >> $LogFilelocation/$LogFileName; break; }
count_of_run=$((count_of_run+1))
sleep $sleep_time
done

# -----------------------------------------------------------------------#
# Exit the script if files are not available
# -----------------------------------------------------------------------#
	[ "$result" -eq 0 -a "$workflow_name" = 'wf_ERDM_INTERNATIONAL_STG' ] && { echo 'All files are not available to trigger the workflow:'${workflow_name} >> $LogFilelocation/$LogFileName; exit 0; }
	[ "$result" -eq 0 ] && { echo 'All files are not available to trigger the workflow:'${workflow_name} >> $LogFilelocation/$LogFileName; exit 1; }
fi

# -----------------------------------------------------------------------#
# Initiating Parameter file changes for workflow trigger
# -----------------------------------------------------------------------#

if [ "$src_system" = 'T3' ]
then
	echo 'Processing Date:' $current_date >> $LogFilelocation/$LogFileName
	echo 'Param File Used:' $param_temp_location'/'$ParamFileName_T3 >> $LogFilelocation/$LogFileName
	echo 'Folder: ' $Folder_T3 >> $LogFilelocation/$LogFileName
	echo 'Instance:' $instance_name >> $LogFilelocation/$LogFileName
	echo 'Preparing Parameter files for the specified Job:'$workflow_name >> $LogFilelocation/$LogFileName
	echo 'Check if the workflow belongs to monthly load or daily load' >> $LogFilelocation/$LogFileName
	echo $workflow_name | egrep 'INTERNATIONAL|IRRS|GCMS' >> $LogFilelocation/$LogFileName
		if [ $? -eq 0 ]
		then
		cat $param_root_location'/'$ParamFile_Tier3_Static | sed 's,FOLDER_T3,'$Folder_T3',g' |sed '/$$COUNTRY_CODE=/s/AAA/'$instance_name'/g' |  sed '/$$BUSINESS_DATE=/s/YYYY-MM-DD/'$INTL_BATCH_DT'/g' | sed 's,ZZZZ,'$connection_t1',g' | sed 's,RRRRRR,'$connection_t3',g' | sed 's,QQQQ,'$script_location',g' > $param_temp_location/$ParamFileName_T3
		else
		cat $param_root_location'/'$ParamFile_Tier3_Static | sed 's,FOLDER_T3,'$Folder_T3',g'  | sed '/$$COUNTRY_CODE=/s/AAA/'$instance_name'/g' |  sed '/$$BUSINESS_DATE=/s/YYYY-MM-DD/'$BSN_DT'/g' | sed 's,ZZZZ,'$connection_t1',g' | sed 's,RRRRRR,'$connection_t3',g' | sed 's,QQQQ,'$script_location',g' > $param_temp_location/$ParamFileName_T3
		fi
elif [ "$src_system" = 'T1' ] && [ "$result" -eq 1 ]
then
echo 'Processing Date:' $current_date >> $LogFilelocation/$LogFileName
echo 'Param File Used:' $param_temp_location'/'$ParamFileName_T1 >> $LogFilelocation/$LogFileName
echo 'Folder: ' $Folder_T1 >> $LogFilelocation/$LogFileName
echo 'Preparing Parameter files for the specified Job:'$workflow_name >> $LogFilelocation/$LogFileName
if [ "$instance_name" = 'ML' ]
then
cat $param_root_location'/'$ParamFile_Tier1_Static | sed 's,FOLDER_T1,'$Folder_T1',g' | sed 's,XXXX,'$source_file_path',g' | sed 's,ZZZZ,'$connection_t1',g' | sed 's,MBBCNTCODE,MY,g' | sed 's,QQQQ,'$script_location',g' > $param_temp_location'/'$ParamFileName_T1
elif [ "$instance_name" = 'MB' ]
then
cat $param_root_location'/'$ParamFile_Tier1_Static | sed 's,FOLDER_T1,'$Folder_T1',g' | sed 's,XXXX,'$source_file_path',g' | sed 's,ZZZZ,'$connection_t1',g' | sed 's,MBBCNTCODE,MY1,g' | sed 's,QQQQ,'$script_location',g' > $param_temp_location'/'$ParamFileName_T1
else
cat $param_root_location'/'$ParamFile_Tier1_Static | sed 's,FOLDER_T1,'$Folder_T1',g' | sed 's,XXXX,'$source_file_path',g' | sed 's,ZZZZ,'$connection_t1',g' | sed 's,MBBCNTCODE,'$instance_name',g' | sed 's,QQQQ,'$script_location',g' > $param_temp_location'/'$ParamFileName_T1
fi
else
echo 'Invalid source system argument, Hence '${workflow_name}' is not triggered' >> $LogFilelocation/$LogFileName
exit 1
fi

#-----------------------------------------------------------------------------------------------------------------------------------------#
# Triggering the Workflows according to the corresponding Tiers and updating the AUDIT tables with load status
#-----------------------------------------------------------------------------------------------------------------------------------------#

if [ "$src_system" = 'T3' ]
then
cd $InfaRootLocation
if [ "$instance_name" = 'XX' -o "$instance_name" = 'ZZ' ]
then
echo UPDATE $audit_schema.$audit_table SET STATUS=\'LOADED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\' and TIER_LEVEL=\'TIER3\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
pmcmd startworkflow  -sv $integration_service -d $domain -u $username -pv INFA_PASSWORD -usd $sec_domain -f $Folder_T3 -paramfile $param_temp_location/$ParamFileName_T3 -wait $workflow_name >> $LogFilelocation/$LogFileName
if [ $? -eq 0 ]
then
echo 'Workflow completed successfully, Additional stats from repository below' >> $LogFilelocation/$LogFileName
pmcmd getworkflowdetails -sv $integration_service -d $domain -u $username -pv INFA_PASSWORD -usd $sec_domain -f $Folder_T3 $workflow_name >> $LogFilelocation/$LogFileName
else
echo $workflow_name' - FAILED, Please check the monitor logs for more information' >> $LogFilelocation/$LogFileName
echo UPDATE $audit_schema.$audit_table SET STATUS=\'WORKFLOW FAILED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\' and TIER_LEVEL=\'TIER3\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
exit 1;
fi
else
echo UPDATE $audit_schema.$audit_table SET STATUS=\'LOADED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\' and TIER_LEVEL=\'TIER3\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
pmcmd startworkflow  -sv $integration_service -d $domain -u $username -pv INFA_PASSWORD -usd $sec_domain -f $Folder_T3 -paramfile $param_temp_location/$ParamFileName_T3 -runinsname $instance_name -wait $workflow_name >> $LogFilelocation/$LogFileName
if [ $? -eq 0 ]
then
echo 'Workflow completed successfully, Additional stats from repository below' >> $LogFilelocation/$LogFileName
pmcmd getworkflowdetails -sv $integration_service -d $domain -u $username -pv INFA_PASSWORD -usd $sec_domain -f $Folder_T3 -rin $instance_name $workflow_name >> $LogFilelocation/$LogFileName
else
echo $workflow_name' - FAILED, Please check the monitor logs for more information' >> $LogFilelocation/$LogFileName
echo UPDATE $audit_schema.$audit_table SET STATUS=\'WORKFLOW FAILED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\' and TIER_LEVEL=\'TIER3\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
exit 1;
fi
fi
elif [ "$src_system" = 'T1' ] && [ "$result" -eq 1 ]
then
cd $InfaRootLocation
if [ "$instance_name" = 'XX' -o "$instance_name" = 'ZZ' ]
then
echo UPDATE $audit_schema.$audit_table SET STATUS=\'LOADED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\' and TIER_LEVEL=\'TIER1\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
pmcmd startworkflow  -sv $integration_service -d $domain -u $username -pv INFA_PASSWORD -usd $sec_domain -f $Folder_T1 -paramfile $param_temp_location/$ParamFileName_T1 -wait $workflow_name >> $LogFilelocation/$LogFileName
if [ $? -eq 0 ]
then
echo 'Workflow completed successfully, Additional stats from repository below' >> $LogFilelocation/$LogFileName
pmcmd getworkflowdetails -sv $integration_service -d $domain -u $username -pv INFA_PASSWORD -usd $sec_domain -f $Folder_T1 -rin $instance_name $workflow_name >> $LogFilelocation/$LogFileName
else
echo $workflow_name' - FAILED, Please check the monitor logs for more information' >> $LogFilelocation/$LogFileName
echo UPDATE $audit_schema.$audit_table SET STATUS=\'WORKFLOW FAILED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\' and TIER_LEVEL=\'TIER1\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
exit 1;
fi
else
echo UPDATE $audit_schema.$audit_table SET STATUS=\'LOADED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\' and TIER_LEVEL=\'TIER1\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
pmcmd startworkflow  -sv $integration_service -d $domain -u $username -pv INFA_PASSWORD -usd $sec_domain -f $Folder_T1 -paramfile $param_temp_location/$ParamFileName_T1 -runinsname $instance_name -wait $workflow_name >> $LogFilelocation/$LogFileName
if [ $? -eq 0 ]
then
echo 'Workflow completed successfully, Additional stats from repository below' >> $LogFilelocation/$LogFileName
pmcmd getworkflowdetails -sv $integration_service -d $domain -u $username -pv INFA_PASSWORD -usd $sec_domain -f $Folder_T1 -rin $instance_name $workflow_name >> $LogFilelocation/$LogFileName
else
echo $workflow_name' - FAILED, Please check the monitor logs for more information' >> $LogFilelocation/$LogFileName
echo UPDATE $audit_schema.$audit_table SET STATUS=\'WORKFLOW FAILED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\' and TIER_LEVEL=\'TIER1\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
exit 1;
fi
fi
else
echo 'Unexpectedly terminated or Error in triggering the workflow:'$workflow_name >> $LogFilelocation/$LogFileName;
echo UPDATE $audit_schema.$audit_table SET STATUS=\'WORKFLOW FAILED\'\,WF_START_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\'\; > $param_temp_location/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]
exit 1;
fi 

#---------------------------------------------------------------------------------------------------------------------------#
# Updating the AUDIT TABLE with end time status
#---------------------------------------------------------------------------------------------------------------------------#
echo UPDATE $audit_schema.$audit_table SET WF_END_TIME=CURRENT_TIMESTAMP\(6\) WHERE WRKFLW_NM=\'$workflow_name\' and batch_dt=\'$BATCH_DT\' and ctry_cd=\'$instance_name\'\; > $param_temp_location/UPDATE_ENDTIME_${workflow_name}_${instance_name}_${current_date}.sql;
echo UPDATING WORKFLOW_STATUS in $audit_table for $workflow_name >> $LogFilelocation/$LogFileName;
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG] >> $LogFilelocation/$LogFileName;
.RUN FILE=${param_temp_location}/UPDATE_ENDTIME_${workflow_name}_${instance_name}_${current_date}.sql;
.QUIT 0;
.logoff;
[BTEQ-FLG]

if [ $src_system = 'T1' ]
then
rm -f ${param_temp_location}/VALIDATE_${workflow_name}_${instance_name}_${current_date}.sql
rm -f ${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
rm -f ${param_temp_location}/UPDATE_ENDTIME_${workflow_name}_${instance_name}_${current_date}.sql;
rm -f ${param_temp_location}/erdm_wkflow_trigger_${workflow_name}_${instance_name}.txt
echo 'Removed all Temp SQL File generated for fetching the status of files and updating the status of loaded/failed files' >> $LogFilelocation/$LogFileName
echo 'Script completed'  >> $LogFilelocation/$LogFileName
else
rm -f ${param_temp_location}/UPDATE_${workflow_name}_${instance_name}_${current_date}.sql;
rm -f ${param_temp_location}/UPDATE_ENDTIME_${workflow_name}_${instance_name}_${current_date}.sql;
echo 'Script completed'  >> $LogFilelocation/$LogFileName
fi 