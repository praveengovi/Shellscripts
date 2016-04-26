#!/usr/bin/ksh

##############################################################################################################
# Script Name          - RITA_Watch_Download_Validate.sh
# Description          - The below script help us to watch the ERDM source files and download it and 
#                        and Validate it before its to be processed
# Usage       		   - rita_erdm_watch_download_validate.sh erdm <CNTRY_CD> <SYSTM>
#Input Files needed 
#for Script execution  - RITA.param
#   Date              Author                  Modification                        Description
#   ----              ------                  ------------                        -----------
#  25-02-2015         Praveen G                Created                                N/A
#  25-04-2015         Krishnamoorthy C         Modified                               Scheduling automation changes.
##############################################################################################################

set -x

#--Include the comm_functions.sh script to use the common functions------#

. common_functions.sh

#----------------------PRILIMINARY VALIDATION-----------------------------------------------------------------------#
[ $# -eq 3 ] || die "Error  : Insufficient Arguments .\nSyntax : $0 <project>  \nUsage  : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM>"

#-----Project Configuration----------------------------------------------#
project=${1}
. ${project}.config

[ $1 = $project_name ] || die "Not Valid Project Name .\nSyntax : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM>"

[ "$2" = "GLO" -o "$2" = "CN" -o "$2" = "VN" -o "$2" = "KH" -o "$2" = "PG" -o "$2" = "HK" -o "$2" = "LA" -o "$2" = "US" -o "$2" = "UK" -o "$2" = "ML" -o "$2" = "MB" -o "$2" = "ZZ" -o "$2" = "XX" -o "$2" = "MY" -o "$2" = "ID" -o "$2" = "PH" ] || die "Not Valid country code \nSyntax : $0 <project> <country code> <src system>\nUsage  : $0 Not proper Country code"

[ "$3" = "ICBA" -o "$3" = "SIBS" -o "$3" = "TRMS" -o "$3" = "IRRS" -o "$3" = "GCMS"  -o "$3" = "INTL" ] || die "Error  : Not Valid Source System .\nSyntax : $0 <project>  \nUsage  : $0 Not proper Src sys code"


PCRTY_CD=$2;
PSRC_SYS=$3;

#----------------------VARIABLE-ASSIGNMENT--------------------

#---------------------COMMON VARIABLES------------------------
COUNTRY=${2}

project=$project_name;
BUZ_DT=`head -1 $batch_file |awk -F '|' '{print $3}'`

TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
FILE_LIST_PATH=`getParam pFILE_LIST_PATH`;
FILE_LIST_WTH_PTH=$FILE_LIST_PATH/${COUNTRY}_FILE_LIST_${BUZ_DT};

#---------------------DB LOGON VARIABLES---------------------
DATABASE_NM=`getParam pDATABASE_NM`;
TABLE_NAME=`getParam pAUDIT_SRC_FILE_AUDIT_TBL`;

#---------------------FILE WATCHER------------------------

Temp_Dir=`getParam pTEMP_DIR`;
source_system=`getParam psource_system`;
header_file=`getParam pheader_file`;
file_type=`getParam pfile_type`;
ODS_server=`getParam pODS_server`;
ODS_username=`getParam pODS_username`;
ODS_password=`getParam pODS_password`;
ODS_folder=`getParam pODS_folder`;
SRC_DIR=`getParam pARR_DIR`;
TGT_DIR=`getParam pSRC_DIR`;
SRC_DIR_FL=`getParam pARR_DIR`;
TGT_DIR_FL=`getParam pARR_DIR`;
HDR_FILE_LOC_FL=`getParam pHDR_FILE_LOC`;
FINAL_DIR_FL=`getParam pSRC_DIR`;
REJ_DIR=`getParam pREJ_DIR`;
SRC_DIR_CTY=${SRC_DIR_FL}
TGT_DIR_CTY=${TGT_DIR_FL}
FINAL_DIR_CTY=${FINAL_DIR_FL}
HDR_FILE_LOC=${HDR_FILE_LOC_FL}
STD_CTRY_CD=`getParam pSTD_CTRY_CD`;
FN_REASON_DESC="\""
BATCH_ID=`head -1 $batch_file |awk -F '|' '{print $1}'`
INTL_BATCH_DT=`date +%Y-%m-01`
FN_FAILED_DESC="FAILED";
FN_PASSED_DESC="PASSED";
DELIMITER="|"
UNPRINTABLE_CHARS=`getParam pUNPRINTABLE_CHARS`;


if [ "$3" = 'INTL' ]
then
BATCH_DT=$INTL_BATCH_DT
else
BATCH_DT=`head -1 $batch_file |awk -F '|' '{print $2}'`
fi


#-- Execution timings & assignment 

max_wait_time=`getParam p${PSRC_SYS}_max_wait_time`
wait_time=`getParam p${PSRC_SYS}_wait_time`

#------------------------------------------------------------------------#
# Update the audit log table with Status based on File availability  and 
#                                                          file download
#------------------------------------------------------------------------#

batch_table_upd()
{
   #------FUNCTION-VARIABLES------------
      
	FN_UPDATE_FLAG=$1
	FN_FILE_NAME=$2
	FN_CTRY_CD=$3
	FN_BATCH_ID=$4
	FN_SRC_SYSTM=$5
	FN_ACT_FILE_CNT=$ACT_FILE_CNT
	FN_REASON_DESC=$6
   #----------------------------------

echo "UPDATE "$DATABASE_NM"."$TABLE_NAME" SET STATUS='"$FN_UPDATE_FLAG"',REC_CNT='"$FN_ACT_FILE_CNT"',MDFY_TIME=CURRENT_TIMESTAMP(1),REASON_DESC='"$FN_REASON_DESC"' WHERE zip_out_file_nm='"$FN_FILE_NAME"' AND CTRY_CD='"$FN_CTRY_CD"' AND BATCH_ID='"$FN_BATCH_ID"' AND SRC_SYS_NM='"$FN_SRC_SYSTM"';" >> ${Temp_Dir}${UPD_DML_FILE}
echo "RECORD - UPDATED"

}


#------------------------------------------------------------------------#
#   Validate the Header and Trailer based on source system
#------------------------------------------------------------------------#

HEADER_TRAILER_CHK()
{
set -x
	echo "\n**********HEADER-TRAILER-VALIDATION******************\n"
	echo "  File Name  -> "$FILE_NM_MODIF""
	echo "  SRC_SYSTEM -> "$SRC_SYS""
case "$1" in
	HLDR_CNT_CHK)
	HEADER_LINE=`head -1 $TGT_DIR_CTY$FILE_NM|sed 's/ //g'`
	HEADER_AVAIBTY=`grep -c "^HEADER" $TGT_DIR_CTY$FILE_NM`
	TLR_AVAIBTY=`grep -c "^TRAILER" $TGT_DIR_CTY$FILE_NM`
	TRAILER_LINE=`grep "^TRAILER" $TGT_DIR_CTY$FILE_NM|sed 's/ //g'`
	FILE_NM_MODIF=`echo "$FILE_NM"|awk -F '.' '{print $1}'`
	[ "$SRC_SYSTM" = "IRRS" ] && { HDR_FRAME="HEADER"${DELIMITER}${FILE_NM}; } || { HDR_FRAME="HEADER"${DELIMITER}${FILE_NM_MODIF}; }
	TRLR_FRAME="TRAILER"$DELIMITER
	echo ">> Actual   - Header Frame -> "$HEADER_LINE""
	echo ">> Expected - Header Frame -> "$HDR_FRAME""
	
	if [ "$HEADER_LINE" = "$HDR_FRAME" ]; then
	HEADER_VAL_CLEARED=1;
	echo ">> Header Line format get matched"
	if [ $HEADER_VAL_CLEARED -eq 1 ] && [ $HEADER_AVAIBTY -eq 1 ] && [ $TLR_AVAIBTY -eq 1 ]; then
		FILE_CNT=`wc -l ${TGT_DIR_CTY}${FILE_NM}|awk '{print $1}'`
		echo ">> Total File Count -> "$FILE_CNT""
		ACT_FILE_CNT=$(($FILE_CNT - 2))
		echo ">> Total Data Count -> "$ACT_FILE_CNT""
		TRLR_FRAME="TRAILER"$DELIMITER$ACT_FILE_CNT
		echo ">> Actual   - Trailer Frame -> "$TRAILER_LINE""
		echo ">> Expected - Trailer Frame -> "$TRLR_FRAME""
	if [ "$TRAILER_LINE" = "$TRLR_FRAME" ]; then
		echo ">> Trailer Line format and count matched"
		echo ">> HLDR_CNT_CHK - PASSED"
		HEADER_TRLR_VAL_CLEARED=1;
		echo ">> Header Line format get matched"
		echo "\n*****************************************************\n"
		echo "\n*************FILE-POST-PROCESSING********************\n"
		sed '1d;$d' ${TGT_DIR_CTY}${FILE_NM} > ${Temp_Dir}${FILE_NM}.temp
		echo ">> HEADER AND TRAILER GET REMOVED"											
		HD_TLR_RMV_STATUS=1;
		HEADER_AVL_STATS=1;
		TLR_AVL_STATS=1;
	else 
		echo ">> Trailer Line format or count not matched"
		HEADER_TRLR_VAL_CLEARED=0;
	fi;										   
	fi;	
   else 
   echo ">> **Header Line format not matched**"
   HEADER_VAL_CLEARED=0;
   HEADER_AVL_STATS=0;
   TLR_AVL_STATS=0;
	fi;
 ;;
 HLDR-FILE)
	# [ ${CTRY_CD} == 'VN' ] && { CTRY_CD1='VTN' ; }
	# [ ${CTRY_CD} == 'MB' ] && { CTRY_CD1='MILB' ; }
	# [ ${CTRY_CD} == 'ML' ] && { CTRY_CD1='MIL' ; }
	FILE_NM_MODIF=` echo $FILE_NM_MODIF | sed "s/${CTRY_CD}_//g"`
	if [ $CTRY_CD = 'ML' ]
	then
	HEADER_FILE_NM=MY_ODS_FILE.txt
	elif [ $CTRY_CD = 'MB' ]
	then
	HEADER_FILE_NM=MY1_ODS_FILE.txt
	else
	HEADER_FILE_NM="${CTRY_CD}"_ODS_FILE.txt
	fi
	[ -f ${TGT_DIR_CTY}${HEADER_FILE_NM} ] && { 
	tr -d '\377\0\376\015' < ${TGT_DIR_CTY}${HEADER_FILE_NM} > ${TGT_DIR_CTY}${HEADER_FILE_NM}_temp
	mv ${TGT_DIR_CTY}${HEADER_FILE_NM}_temp ${TGT_DIR_CTY}${HEADER_FILE_NM}	 
	} || {
	die "Control file not available" ; }
	EXP_HDR_FILE_CNT=`grep ${IN_FILE_NM} ${TGT_DIR_CTY}${HEADER_FILE_NM}|awk -F '|' '{print $2}'`
	FILE_CNT=`wc -l ${TGT_DIR_CTY}${FILE_NM}|awk '{print $1}'|bc`
	ACT_FILE_CNT=$(($FILE_CNT))
	HEADER_LINE=`grep ${FILE_NM_MODIF} ${TGT_DIR_CTY}${HEADER_FILE_NM}|sed 's/ //g'`
	HDR_FRAME=${FILE_NM}${DELIMITER}${ACT_FILE_CNT}
	TLR_AVAIBTY=`grep -c ${FILE_NM_MODIF} ${TGT_DIR_CTY}${HEADER_FILE_NM}`
	echo "$HDR_FRAME"
	echo ">> Total File Count    -> "$ACT_FILE_CNT""
	echo ">> Expected File Count -> "$EXP_HDR_FILE_CNT""
	 if [ "$ACT_FILE_CNT" -eq "$EXP_HDR_FILE_CNT" ] ; then
		echo ">> Header Format and Count Get matched"
		HEADER_TRLR_VAL_CLEARED=1;
		HEADER_VAL_CLEARED=1;
		TLR_AVL_STATS=1;
		HEADER_AVL_STATS=1;
		echo "\n*************FILE-POST-PROCESSING********************\n"
		echo ">> HEADER AND TRAILER REMOVAL NOT NEEDED"
		cp ${TGT_DIR_CTY}${FILE_NM} ${Temp_Dir}${FILE_NM}.temp
		HD_TLR_RMV_STATUS=1
	else 
		echo ">> Header Format or Count not get matched"
		HEADER_VAL_CLEARED=9;
		HEADER_TRLR_VAL_CLEARED=0;
		TLR_AVL_STATS=0;
	fi;
 ;;
 HDR_TLR_CNT_CHK)
		HEADER_AVAIBTY=`grep -c "HEADER" $TGT_DIR_CTY$FILE_NM`
		HEADER_LINE=`head -1 $TGT_DIR_CTY$FILE_NM`
		TRAILER_LINE=`tail -1 $TGT_DIR_CTY$FILE_NM||sed -e 's/ //g'`
		FILE_CNT=`wc -l ${TGT_DIR_CTY}${FILE_NM}|awk '{print $1}'`
		 ACT_FILE_CNT=$(($FILE_CNT - 2))
		 echo ">> Total File Count -> "$FILE_CNT""
		 echo ">> Total Data Count -> "$ACT_FILE_CNT""
		 echo ">> Trailer Count    -> "$TRAILER_LINE""								
		 [ $TRAILER_LINE -eq $ACT_FILE_CNT ] && {
		 echo ">> Trailer count & Data count get matched";
		 #grep "|" ${TGT_DIR_CTY}${FILE_NM} > ${Temp_Dir}${FILE_NM}.temp;
		 sed '1d;$d' ${TGT_DIR_CTY}${FILE_NM} > ${Temp_Dir}${FILE_NM}.temp;
		 HD_TLR_RMV_STATUS=1;HEADER_TRLR_VAL_CLEARED=1;HEADER_VAL_CLEARED=1;HEADER_AVL_STATS=1;TLR_AVL_STATS=1;TLR_AVAIBTY=1;
		 } || { HEADER_TRLR_VAL_CLEARED=0; echo ">> Trailer count & Data count not matched or trailer not available";
		 HEADER_VAL_CLEARED=0;HEADER_AVL_STATS=0;TLR_AVL_STATS=0; }										
 ;;
 N-A)
	HEADER_TRLR_VAL_CLEARED=1;
	HEADER_VAL_CLEARED=1;
	HEADER_AVL_STATS=1;
	TLR_AVL_STATS=1;
	HD_TLR_RMV_STATUS=1;
	ACT_FILE_CNT=`wc -l ${TGT_DIR_CTY}${FILE_NM}|awk '{print $1}'|bc`;
	#[ "$CTRY_CD" = "ZZ" ] && { sed '1d' ${TGT_DIR_CTY}${FILE_NM} > ${Temp_Dir}${FILE_NM}.temp ;} || { cp ${TGT_DIR_CTY}${FILE_NM} ${Temp_Dir}${FILE_NM}.temp ;}
	[ "$SRC_SYS" = "IRRS" ] && { sed '1d;$d' ${TGT_DIR_CTY}${FILE_NM} > ${Temp_Dir}${FILE_NM}.temp; } || { cp ${TGT_DIR_CTY}${FILE_NM} ${Temp_Dir}${FILE_NM}.temp; }
	;;
*) echo "NOT VALID HEADER TRAILER FORMAT";;
esac 

	if [ $HD_TLR_RMV_STATUS -eq 1 ] && [ $HEADER_TRLR_VAL_CLEARED -eq 1 ]  && [ $HEADER_VAL_CLEARED -eq 1 ] && [ $HEADER_AVL_STATS -eq 1 ] && [ $TLR_AVL_STATS -eq 1 ]; then
	echo "\n***************ADDING AUDIT FIELDS*******************\n"
	echo ">> Processing file Name: "$FILE_NM" "
	
	BUZ_DT_YYYYMMDD=`echo $BUZ_DT| sed 's/-//g'`
	
	if [ "$SRC_SYS"="GCMS" -o "$SRC_SYS"="IRRS" ]; then 
		AUD_FILE_NM=$FILE_NM ; 
		else 
		AUD_FILE_NM=`echo ""$FILE_NM""|cut -c4-100` ;
	fi;
	#-- For GCMS & IRRS System
	if [ "$CTRY_CD" = "XX" -o "$CTRY_CD" = "ZZ" ]; then 
		ACT_FILE_NM=$VALID_OUT_FILE_NM
		if [ "$CTRY_CD" = "ZZ" ]; then
		awk '{print $0"|'$CTRY_CD'|'$BTCH_ID'|'$BUZ_DT_YYYYMMDD'|'${AUD_FILE_NM}'|'$SRC_SYS'"}' ${Temp_Dir}${FILE_NM}.temp > ${FINAL_DIR_FL}${ACT_FILE_NM} 2> /dev/null
		else 
		awk '{print $0"|'${AUD_FILE_NM}'|'$BTCH_ID'|'$BUZ_DT_YYYYMMDD'|'$SRC_SYS'"}' ${Temp_Dir}${FILE_NM}.temp| grep '|' > ${FINAL_DIR_FL}${ACT_FILE_NM} 2> /dev/null
		fi;
	else
		[ "$CTRY_CD" = "MB" -o "$CTRY_CD" = "ML" ] && { MOD_CTRY_CD='MY' ; } || { MOD_CTRY_CD=$CTRY_CD; }
		ACT_FILE_NM=$VALID_OUT_FILE_NM
		awk '{print $0"|'$MOD_CTRY_CD'|'${AUD_FILE_NM}'|'$BTCH_ID'|'$BUZ_DT_YYYYMMDD'|'$SRC_SYS'"}' ${Temp_Dir}${FILE_NM}.temp | grep -v "^HEADER" | grep -v "^TRAILER" > ${FINAL_DIR_FL}${ACT_FILE_NM} 2> /dev/null ;
	fi;
	rm ${Temp_Dir}${FILE_NM}.temp;
	echo ">> Audit fields added  : "$FILE_NM_MODIF" "
	echo "\n*****************************************************\n"
	fi;
}
#----------------------------------------------------------------------------------#
#   Files does not meet header and trailer validation moved to reject directory with
#   description 
#----------------------------------------------------------------------------------#
					REJECT_FILE_HANDLING()
					{
					  FN_TGT_DIR_CTY=$1
					  FN_FILE_NM=$2
					  FN_REJ_DIR=$3
					  FN_HEADER_VAL_CLEARED=$4;
					  FN_HEADER_TRLR_VAL_CLEARED=$5;
					  FN_HEADER_AVL_STATS=$6;
					  FN_TLR_AVL_STATS=$7
					  FN_RES_DESC=""

					  if [ $FN_HEADER_VAL_CLEARED -eq 0 ]; then
					  FN_RES_DESC="**********Reject Reason - Header format not matched****************"
					  elif [ $FN_HEADER_VAL_CLEARED -eq 1 ] && [ $FN_HEADER_TRLR_VAL_CLEARED -eq 0 ]; then
					  FN_RES_DESC="********Trailer count and Actual file count differs****************"
					  elif [ $FN_HEADER_VAL_CLEARED -eq 9 ] && [ $FN_HEADER_TRLR_VAL_CLEARED -eq 0 ]; then
					  FN_RES_DESC="********Header file count differs with actual count*****************"
					  elif [ $FN_HEADER_AVL_STATS -eq 1 ] && [ $FN_TLR_AVL_STATS -eq 0 ]; then
					  FN_RES_DESC="********Trailer Not available in the respective file*****************"
					  elif [ $FN_HEADER_AVL_STATS -eq 0 ]; then
					  FN_RES_DESC="********Header Not available in the respective file*****************"					  
					  elif [ $FN_HEADER_AVL_STATS -eq 9 ] && [ $FN_TLR_AVL_STATS -eq 0 ]; then
					  FN_RES_DESC="******Record Count or Format mismatches with Actual format***********"					  
					  fi;
					  mv ${FN_TGT_DIR_CTY}${FN_FILE_NM} ${FN_REJ_DIR}${FN_FILE_NM};
					  `echo $FN_RES_DESC >> ${FN_REJ_DIR}${FN_FILE_NM}`;
					}

#------------------------------------------------------------------------#
# Refer the batch table Start pre processing (Watch,Download & Validate)
# with below mentioned lines of code 
#------------------------------------------------------------------------#

# Loop Variable initialization 

watcher_instance=1;

while [ $watcher_instance -le $max_wait_time ]
do
#----------------------TAKE NECESSARY INFORMATION FROM DATABASE---------------------------

#Take the file list from table and move into the specific directory

[ -f $FILE_LIST_WTH_PTH ] && { rm $FILE_LIST_WTH_PTH ; }

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.export file=$FILE_LIST_WTH_PTH;
.set width 50000;
	select  
		trim(batch_id)||','||batch_dt||','||CTRY_CD||','||SRC_SYS_NM||','||trim(zip_out_file_nm)||','||trim(valid_out_file_nm)||','||trim(IN_FILE_NM) (title'') 
	from 
		$DATABASE_NM.$TABLE_NAME 
	where 
		BATCH_DT=DATE '$BATCH_DT'
	and CTRY_CD='$PCRTY_CD' 
	and SRC_SYS_NM='$PSRC_SYS'
	and STATUS in ('PROGRESS','FAILED','') 
	and is_comp_file='N' 
	and CTL_FILE_FLG = 'N';
	.export reset; 
.quit 0;
.logoff;
[BTEQ-FLG]

chmod 777 $FILE_LIST_WTH_PTH;
echo "List of file need to be exported is written into "$FILE_LIST_PATH"/"$COUNTRY"_FILE_LIST_"$BUZ_DT"";
line="$(< $FILE_LIST_WTH_PTH)"

sed '/^$/d' $FILE_LIST_WTH_PTH > $FILE_LIST_WTH_PTH.tmp;mv $FILE_LIST_WTH_PTH.tmp $FILE_LIST_WTH_PTH;

FILE_CNT=`wc -l ${FILE_LIST_WTH_PTH}|awk '{print $1}'`

[ "$FILE_CNT" = "0" ] && { exit 0 ; }

i=0;

[ -f ${Temp_Dir}${COUNTRY}_${BUZ_DT} ] && { rm ${Temp_Dir}${COUNTRY}_${BUZ_DT} ; touch ${Temp_Dir}${COUNTRY}_${BUZ_DT} ;} || { touch ${Temp_Dir}${COUNTRY}_${BUZ_DT} ; chmod 777 ${Temp_Dir}${COUNTRY}_${BUZ_DT} ;}

for line in $line

do
		echo $line|awk -F ',' '{print $1" "$2" "$3" "$4" "$5" "$6" "$7}'|read BTCH_ID BUS_DATE CTRY_CD SRC_SYSTM FILE_NM VALID_OUT_FILE_NM IN_FILE_NM
		echo ""$IN_FILE_NM""
		VAL_PARAM=`cat $Param_File|grep ${SRC_SYSTM}'-FMT'`		
		echo $VAL_PARAM|awk -F ',' '{print $2" "$3" "$4" "$5}'|read SRC_SYS VAL_PARAM_HDR_VHK VAL_PARAM_SIZE_VAL VAL_PARAM_JNK_VAL
		FILE_NM_MODIF=`echo "$FILE_NM"|awk -F '.' '{print $1}'`
		[ -f ${Log_dir}${FILE_NM_MODIF}.log ] && { rm ${Log_dir}${FILE_NM_MODIF}.log ; touch ${Log_dir}${FILE_NM_MODIF}.log ;} || { touch ${Log_dir}${FILE_NM_MODIF}.log ;}		
		FILE_NAME=$FILE_NM
		BIZ_DATE=$BUS_DATE
		CTRY_CD=$CTRY_CD
		BATCH_ID=$BTCH_ID

#------------------FILE-WATCHING------------------------------------#

	[ -f $SRC_DIR_CTY/$FILE_NM ] && { 
	FILE_AVAIL_STAT=1 ; 
    tr -d '\377\0\376\015' < $SRC_DIR_CTY/$FILE_NM > $SRC_DIR_CTY/${FILE_NM}_temp
	mv $SRC_DIR_CTY/${FILE_NM}_temp $SRC_DIR_CTY/${FILE_NM}
    info "File $SRC_DIR_CTY/$FILE_NM is available" ;
	} || {
	FILE_AVAIL_STAT=0 ;
	continue ;
	}

#------------------------------------------------------------------------#
#  HEADER TRAILER VALIDATION AND SIZE VALIDATION                         #
#------------------------------------------------------------------------#

if [ "$VAL_PARAM_HDR_VHK"="HLDR_CNT_CHK" -o "$VAL_PARAM_HDR_VHK"="HLDR-FILE" ]; then
	HEADER_TRAILER_CHK $VAL_PARAM_HDR_VHK >> ${Log_dir}${FILE_NM_MODIF}.log
else 
	echo ">> Not Valid File format value in RITA.Param file"
fi;
 
#------------------------------------------------------------------------#
# Update the batch table about file validation and arrival status        #
#------------------------------------------------------------------------#

FN_REASON_DESC=""
UPDATE_FLAG=""
echo "$HEADER_TRLR_VAL_CLEARED"
		
if [ "$HEADER_TRLR_VAL_CLEARED" -eq 1 ] && [ "$HEADER_VAL_CLEARED" -eq 1 ] && [ "$HEADER_AVL_STATS" -eq 1 ] && [ "$TLR_AVL_STATS" -eq 1 ]; then 

		UPDATE_FLAG="PASSED"
		FN_REASON_DESC="ALL_VALIDATIONS_CLEARED"
else 
		UPDATE_FLAG="FAILED"  
		
		if [ "$HEADER_VAL_CLEARED" -eq 0 ]; then
		
		REJECT_FILE_HANDLING $TGT_DIR_CTY $FILE_NM $REJ_DIR $HEADER_VAL_CLEARED $HEADER_TRLR_VAL_CLEARED $HEADER_AVL_STATS $TLR_AVL_STATS>> ${Log_dir}${FILE_NM_MODIF}.log
		
		FN_REASON_DESC=${FN_REASON_DESC}"-FILE_HEADER_MISMATCH - PLEASE REFER THE REJECT FILE"
		
		elif [ "$HEADER_AVL_STATS" -eq 0 ]; then
		
		REJECT_FILE_HANDLING $TGT_DIR_CTY $FILE_NM $REJ_DIR $HEADER_VAL_CLEARED $HEADER_TRLR_VAL_CLEARED $HEADER_AVL_STATS $TLR_AVL_STATS >> ${Log_dir}${FILE_NM_MODIF}.log
		
		FN_REASON_DESC=${FN_REASON_DESC}"-FILE_HEADER_NOT_AVAILABLE - PLEASE REFER THE REJECT FILE"
		
		
		elif [ "$HEADER_TRLR_VAL_CLEARED" -eq 0 ]; then
		
		REJECT_FILE_HANDLING $TGT_DIR_CTY $FILE_NM $REJ_DIR $HEADER_VAL_CLEARED $HEADER_TRLR_VAL_CLEARED $HEADER_AVL_STATS $TLR_AVL_STATS >> ${Log_dir}${FILE_NM_MODIF}.log
					
		FN_REASON_DESC=${FN_REASON_DESC}"-TRAILER_VALIDATION_FAILED - PLEASE REFER THE REJECT FILE"
		
		elif [ "$TLR_AVL_STATS" -eq 0 ]; then
		
		REJECT_FILE_HANDLING $TGT_DIR_CTY $FILE_NM $REJ_DIR $HEADER_VAL_CLEARED $HEADER_TRLR_VAL_CLEARED $HEADER_AVL_STATS $TLR_AVL_STATS>> ${Log_dir}${FILE_NM_MODIF}.log
		
		FN_REASON_DESC=${FN_REASON_DESC}"-FILE_TRAILER_NOT_AVAILABLE - PLEASE REFER THE REJECT FILE"				
	  fi;
			  
	fi;
     
	 UPD_DML_FILE=${COUNTRY}_${BUZ_DT};
	 echo ""$UPD_DML_FILE""
  	 batch_table_upd $UPDATE_FLAG $FILE_NAME $CTRY_CD $BATCH_ID $SRC_SYSTM $FN_REASON_DESC
	
done
	
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.RUN FILE=${Temp_Dir}${UPD_DML_FILE}
.export reset;
.quit;
.logoff;
[BTEQ-FLG]

sleep $wait_time ;

watcher_instance=$(($watcher_instance + 1))                 # increase counter by 1
 
done 