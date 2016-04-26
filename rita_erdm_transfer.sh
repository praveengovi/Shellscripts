#!/usr/bin/ksh

##############################################################################################################
# Script Name          - rita_erdm_trasfer.sh
# Description          - The below script download will move the downstream file extract from Tier 3 views to 
#                        respective folder.
# Usage       		   - rita_erdm_trasfer.sh erdm CN RWA
#Input Files needed 
#for Script execution  - erdm.param
#   Date              Author                  Modification                        Description
#   ----              ------                  ------------                        -----------
#  09-05-2015         Praveen G                Created                                N/A
#  
##############################################################################################################

set -x

#--Include the comm_functions.sh script to use the common functions------#

. common_functions.sh

#----------------------PRILIMINARY VALIDATION----------------------------#

[ $# -eq 3 ] || die "Error  : Insufficient Arguments .\nSyntax : $0 <project>  \nUsage  : $0 <PROJECT> <SRC SYSTEM> <PATTERN>"

#-----------Project Configuration---------------------------------------#
project=${1}
. ${project}.config

[ $1 = $project_name ] || die "Not Valid Project Name .\nSyntax : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM> <PATTERN>"

[ "$2" = "CN" -o "$2" = "VN" -o "$2" = "KH" -o "$2" = "PG" -o "$2" = "HK" -o "$2" = "LA" -o "$2" = "US" -o "$2" = "UK" -o "$2" = "ML" -o "$2" = "MB" -o "$2" = "ZZ" -o "$2" = "XX" -o "$2" = "ID" -o "$2" = "PH" ] || die "Not Valid country code \nSyntax : $0 <project> <country code> <Downstream system>\nUsage  : $0 Not proper Country code"

[ "$3" = "RWA" -o "$3" = "GEMS" -o "$3" = "AML" -o "$3" = "INTL" -o "$3" = "BSM" -o "$3" = "GCM" -o "$3" = "GB" ] || die "Error  : Not Valid downstream System .\nSyntax : $0 <project>  \nUsage  : $0 Not proper downstream code"

#----------Global Variable Assignment---------------------------------------#

project=$project_name;
PCTRY_CD=$2;
PDWN_SYS=$3;

BATCH_ID=`head -1 $batch_file |awk -F '|' '{print $1}'`
BUZ_DT=`head -1 $batch_file |awk -F '|' '{print $3}'`

#-- Database related variables

TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
FILE_LIST_PATH=`getParam pFILE_LIST_PATH`;
FILE_LIST_WTH_PTH=$FILE_LIST_PATH/${PCTRY_CD}_DWN_SYS_FILE_LIST_${BUZ_DT};
AUDIT_DATABASE_NM=`getParam pDATABASE_NM`;
AUDIT_SRC_FILE_AUDIT_TBL=`getParam pAUDIT_SRC_FILE_AUDIT_TBL`;
UPD_DML_FILE=${PCTRY_CD}_DWN_FILE_MVMT_LIST_${BUZ_DT}.sql;
DWN_SYS_TRNSFR_IND=`getParam pDWN_SYS_TRNSFR_IND`;
Temp_Dir=`getParam pTEMP_DIR`;


#-------------Take necessary information from the database-----------------#

#--Take the file list from table and move into the specific directory

[ -f $FILE_LIST_WTH_PTH ] && { rm $FILE_LIST_WTH_PTH ; }

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.export file=$FILE_LIST_WTH_PTH;
.set width 50000;
	select  
		trim(batch_id)||','||batch_dt||','||CTRY_CD||','||SRC_SYS_NM||','||trim(IN_FILE_NM)||','||trim(valid_out_file_nm)||','||trim(FILE_DIR)||','||trim(TARGET_DIR) (title'') 
	from 
		$AUDIT_DATABASE_NM.$AUDIT_SRC_FILE_AUDIT_TBL 
	where 
		batch_id=$BATCH_ID 
	and CTRY_CD='$PCTRY_CD' 
	and SRC_SYS_NM='$PDWN_SYS'
	and STATUS in ('PROGRESS','FAILED','') 
	and is_comp_file='N' 
	and CTL_FILE_FLG='N'
	AND TRNSFR_IND='$DWN_SYS_TRNSFR_IND';
	.export reset; 
.quit 0;
.logoff;
[BTEQ-FLG]
chmod 777 $FILE_LIST_WTH_PTH;
sed '/^$/d' $FILE_LIST_WTH_PTH > $FILE_LIST_WTH_PTH.tmp;mv $FILE_LIST_WTH_PTH.tmp $FILE_LIST_WTH_PTH;
line="$(< $FILE_LIST_WTH_PTH)";


[ -f ${Temp_Dir}${UPD_DML_FILE} ] && { rm ${Temp_Dir}${UPD_DML_FILE};} || { touch ${Temp_Dir}${UPD_DML_FILE}; }

#--Move the List of files for particular country and source system to target system--#

for line in $line

do 

#-- Local Variable assignment --#

echo $line|awk -F ',' '{print $1" "$2" "$3" "$4" "$5" "$6" "$7" "$8}'|read BTCH_ID BATCH_DATE CTRY_CD SRC_SYSTM IN_FILE_NM VALID_OUT_FILE_NM IN_FILE_DIR TARGET_DIR

#-- Header & Trailer format --#

HDR_TLR_TMP=`cat $Param_File|grep ${SRC_SYSTM}'-HT_ADD'`;

echo $HDR_TLR_TMP|awk -F ',' '{print $2" "$3" "$4" "$5}'|read HDR_TLR_FMT HEADER_NM TRAILER_NM HDR_TLR_DELIMITER

[ -f ${TARGET_DIR}${VALID_OUT_FILE_NM} ] && { rm ${TARGET_DIR}${VALID_OUT_FILE_NM};touch ${TARGET_DIR}${VALID_OUT_FILE_NM}; }

case "$HDR_TLR_FMT" in
	H_T_ADD_COMMN_FRMT)
	#-- File Movement to target directory adding header and trailer --#
	
	( echo ""${HEADER_NM}${HDR_TLR_DELIMITER}${IN_FILE_NM}""
	cat ${IN_FILE_DIR}${IN_FILE_NM}
	awk '{print}END{print "'$TRAILER_NM$HDR_TLR_DELIMITER'" NR}' ${IN_FILE_DIR}${IN_FILE_NM} ) >> ${TARGET_DIR}${VALID_OUT_FILE_NM}
	;;
	N_A)
		cp ${IN_FILE_DIR}${IN_FILE_NM} ${TARGET_DIR}${VALID_OUT_FILE_NM}
	;;
	*) echo "Format not matched";;	
esac

[ $? -eq 0 ] && { UPDATE_FLAG='PASSED';REASON_DESC="File processed successfully"; } || { UPDATE_FLAG='FAILED';REASON_DESC="File not moved"; };

ACT_FILE_CNT=`wc -l ${IN_FILE_DIR}${IN_FILE_NM}|awk '{print $1}'`

echo "UPDATE "$AUDIT_DATABASE_NM"."$AUDIT_SRC_FILE_AUDIT_TBL" SET STATUS='"$UPDATE_FLAG"',REC_CNT='"$ACT_FILE_CNT"',MDFY_TIME=CURRENT_TIMESTAMP(1),REASON_DESC='"$REASON_DESC"' WHERE IN_FILE_NM='"$IN_FILE_NM"' AND CTRY_CD='"$CTRY_CD"' AND BATCH_ID='"$BATCH_ID"' AND SRC_SYS_NM='"$SRC_SYSTM"';" >> ${Temp_Dir}${UPD_DML_FILE};
done 

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.RUN FILE=${Temp_Dir}${UPD_DML_FILE}
.export reset;
.quit;
.logoff;
[BTEQ-FLG]

#EOF