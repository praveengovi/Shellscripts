#!/usr/bin/ksh

##############################################################################################################
#
# Script Name          - RITA_Watch_Extract.sh
#
# Description          - The below script help us to watch compressed file in arrival directory
#                        and Validate it before its to be temp directory
#
# Usage       		   - rita_erdm_watch_zip_extract.sh erdm <CNTRY_CODE> <SRC_SYSTEM>
#
#Input Files needed 
#for Script execution  - RITA.param
#
#   Date              Author                  Modification                        Description
#   ----              ------                  ------------                        -----------
#  24-4-2015         Praveen G                 Created                                N/A
#
##############################################################################################################


set -x

#--Include the comm_functions.sh script to use the common functions------#

. common_functions.sh

#----------------------SCRIPT ARGUMENT VALIDATION------------

[ $# -eq 3 ] || { die "Insufficient Arguments \nSyntax : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM> \nUsage  : $0 erdm <CNTRY_CODE> <SRC_SYSTEM>" ;}

#-----Project Configuration----------------------------------------------#

project=${1}
. ${project}.config


echo $Log_dir

[ $1 = $project_name ] || die "Not Valid Project Name .\nSyntax : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM>"

[ "$2" = "CN" -o "$2" = "VN" -o "$2" = "KH" -o "$2" = "PG" -o "$2" = "HK" -o "$2" = "LA" -o "$2" = "US" -o "$2" = "UK" -o "$2" = "ML" -o "$2" = "MB" -o "$2" = "ID" -o "$2" = "PH" ] || die "Not Valid country code \nSyntax : $0 <project> <country code> <src system>\nUsage  : $0 Not proper Country code"


[ "$3" = "ICBA" -o "$3" = "SIBS" -o "$3" = "TRMS" -o "$3" = "IRRS" -o "$3" = "GCMS" ] || die "Error  : Not Valid Source System .\nSyntax : $0 <project>  \nUsage  : $0 Not proper Src sys code"


#-----Project Configuration----------------------------------------------#

project=${1}
. ${project}.config

#----------------------VARIABLE-ASSIGNMENT-------------------------------#

project=$project_name;
PCRTY_CD=$2;
PSRC_SYS=$3;

CTRY_CD=$PCRTY_CD

FILE_LIST_PATH=`getParam pFILE_LIST_PATH`;
BATCH_ID=`head -1 $batch_file |awk -F '|' '{print $1}'`
BUZ_DT=`head -1 $batch_file |awk -F '|' '{print $3}'`
FILE_LIST_WTH_PTH=$FILE_LIST_PATH/${PCRTY_CD}_${PSRC_SYS}_CMP_FILE_LIST_${BUZ_DT};
ALL_FILE_LIST_WTH_PTH=$FILE_LIST_PATH/${PCRTY_CD}_${PSRC_SYS}_FILE_LIST_${BUZ_DT};
TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
max_wait_time=`getParam p${PSRC_SYS}_max_wait_time`
wait_time=`getParam p${PSRC_SYS}_wait_time`
watcher_instance=1;
DATABASE_NM=`getParam pDATABASE_NM`;
AUDIT_SRC_FILE_AUDIT_TBL=`getParam pAUDIT_SRC_FILE_AUDIT_TBL`;
Temp_Dir=`getParam pTEMP_DIR`;

#-----------------FUNCTIONS-------------------------------

TD_BATCH_QRY_PREP()
{
set -x
   #------FUNCTION-VARIABLES------------      
	FN_UPDATE_FLAG=$1
	FN_FILE_NAME=$2
	FN_CTRY_CD=$3
	FN_BATCH_ID=$4
	FN_SRC_SYSTM=$5
	FN_REASON_DESC=$6
   #----------------------------------

	echo "UPDATE "$DATABASE_NM"."$AUDIT_SRC_FILE_AUDIT_TBL" SET STATUS='"$FN_UPDATE_FLAG"',MDFY_TIME=CURRENT_TIMESTAMP(1),REASON_DESC='"$FN_REASON_DESC"' WHERE IN_FILE_NM='"$FN_FILE_NAME"' AND CTRY_CD='"$FN_CTRY_CD"' AND BATCH_ID="$FN_BATCH_ID" AND SRC_SYS_NM='"$FN_SRC_SYSTM"';" >> ${Temp_Dir}${PCRTY_CD}_${PSRC_SYS}.sql;

}

#---------------------------------------------------------

########################################################################################

[ -f $FILE_LIST_WTH_PTH ] && { rm $FILE_LIST_WTH_PTH ; }

[ -f $ALL_FILE_LIST_WTH_PTH ] && { rm $ALL_FILE_LIST_WTH_PTH ; }

[ -f ${Temp_Dir}${PCRTY_CD}_${PSRC_SYS}.sql ] && { cat /dev/null > ${Temp_Dir}${PCRTY_CD}_${PSRC_SYS}.sql; } || { touch ${Temp_Dir}${PCRTY_CD}_${PSRC_SYS}.sql;}


bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.set width 50000;
.export file=$FILE_LIST_WTH_PTH;
   SELECT (trim(CTRY_CD)||','||trim(SRC_SYS_NM)||','||trim(IN_FILE_NM)||','||trim(FILE_DIR)||','||trim(TARGET_DIR))  (TITLE '') FROM  ${DATABASE_NM}.${AUDIT_SRC_FILE_AUDIT_TBL} WHERE BATCH_ID=$BATCH_ID AND CTRY_CD='$PCRTY_CD' AND SRC_SYS_NM='$PSRC_SYS' AND STATUS IN ('','FAILED') AND IS_COMP_FILE='Y';
.EXPORT RESET;
.export file=$ALL_FILE_LIST_WTH_PTH;
  SELECT CTRY_CD||','||SRC_SYS_NM||','||trim(IN_FILE_NM)||','||TRIM(ZIP_OUT_FILE_NM)||','||TRIM(FILE_DIR)||','||TRIM(TARGET_DIR) (title'') from ${DATABASE_NM}.${AUDIT_SRC_FILE_AUDIT_TBL} where batch_id=$BATCH_ID and CTRY_CD='$PCRTY_CD' and STATUS in ('','FAILED') and SRC_SYS_NM='$PSRC_SYS' AND IS_COMP_FILE='N';
.EXPORT RESET;
.quit 0;
.logoff;
[BTEQ-FLG]

chmod 777 $FILE_LIST_WTH_PTH;
chmod 777 $ALL_FILE_LIST_WTH_PTH;

sed '/^$/d' $FILE_LIST_WTH_PTH > $FILE_LIST_WTH_PTH.tmp;
mv $FILE_LIST_WTH_PTH.tmp $FILE_LIST_WTH_PTH;

#--------COMMPRESSED-FILE-WATCH-ITERATION-----------------#

while [ $max_wait_time -ge $watcher_instance ]
do

#----------------------TAKE NECESSARY INFORMATION FROM DATABASE---------------------------

cat $FILE_LIST_WTH_PTH | awk -F ',' '{print $1" "$2" "$3" "$4" "$5" "$6}'| read CTRY_CD1 SRC_SYSTEM FILE_NM FILE_DIR TARGET_DIR

[ -f ${Log_dir}${FILE_NM}.log ] && { rm ${Log_dir}${FILE_NM}.log ; }

#CHK_CMP_FILE_AVL $FILE_NM $FILE_DIR >> ${Log_dir}${FILE_NM}.log

[ -f $FILE_DIR/$FILE_NM ] && { info "File $FILE_DIR/$FILE_NM is available " ; FILE_AVAIL_STAT=1 ; break ; }

#--------------MOVE AND RENAME THE FILES TO ARRIVAL DIRECTORY----------#



watcher_instance=$((watcher_instance+1))



done

#-----------------CALL FILE AVAILABILITY CHECK--------------------#

[ "$FILE_AVAIL_STAT" -eq 0 ] && { die "$FILE_NM - File Not available in respective directory" ; }

#-----------------CALL CMP FILE MOVEMENT -------------------------#

	unzip -o ${FILE_DIR}${FILE_NM} -d ${TARGET_DIR};
    [ $? -eq 0 ] && { 
	echo ">> ${FILE_NM} File extracted successfully";
	MV_UNCOMPRESS_FLG=1 ;
	TD_BATCH_QRY_PREP "PASSED" $FILE_NM $CTRY_CD $BATCH_ID $SRC_SYSTEM "--FILE EXTRACTED SUCESSFULLY--" >> ${Log_dir}${FILE_NM}.log;
	} || { 
	die ">> "${FILE_NM}" Error During Extraction";
	}

	unzip -l ${FILE_DIR}${FILE_NM} | awk ' { if ( NF == 4 && $0 ~ /.txt/ ) print $4 }' > ${FILE_LIST_PATH}/${FILE_NM}_filelist.txt
	
	for FILE_NM in `cat ${FILE_LIST_PATH}/${FILE_NM}_filelist.txt`
	do
	grep $FILE_NM $ALL_FILE_LIST_WTH_PTH | awk -F ',' '{ print $1" "$2" "$4" "$5" "$6}' | read CTRY_CD1 SRC_SYSTM ZIP_OUT_FILE_NM CURR_FILE_PATH EXP_FILE_PATH;
	
	mv ${TARGET_DIR}/${FILE_NM} ${TARGET_DIR}/${FILE_NM}_tmp 
	mv ${TARGET_DIR}/${FILE_NM}_tmp ${EXP_FILE_PATH}/${ZIP_OUT_FILE_NM}
	
   	TD_BATCH_QRY_PREP "PROGRESS" $FILE_NM $CTRY_CD $BATCH_ID $SRC_SYSTEM "--FILE MOVED TO ARRIVAL DIRECTORY--" >> ${Log_dir}${FILE_NM}.log;

	done


#--------------UPDATE THE STATUS IN BATCH AUDIT TABLE----------#

bteq .run file=${TD_LOG_ON_PATH}/${B_TEQ_LOG_ON_FILE} << [BTEQ-FLG];
.RUN FILE=${Temp_Dir}${PCRTY_CD}_${PSRC_SYS}.sql
.LOGOFF;
.QUIT;
.EXIT;
[BTEQ-FLG];

sleep $wait_time;

#EOF