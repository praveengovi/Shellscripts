 #!/usr/bin/ksh

####################################################################################################################
#
# Script Name          - rita_erdm_archival.sh
#
# Description          -  rita_erdm_archival script helps to archive the data from arrival directory to archival &
#                         purge the old archival files based on retention period
# Usage       		   -  rita_erdm_archival.sh erdm CN ICBA
#
#Input Files needed 
#for Script execution  - RITA.param
#
#   Date              Author                  Modification              Description
#   ----              ------                  ------------              -----------
#  18-05-2015         Praveen G                Created                   
#  25-05-2015         Praveen G                Modified 				Archival logic changed                                                                                          
###################################################################################################################


set -x
#-----------------------------------------------------------------------#
# Include the comm_functions.sh script to use the common functions
#-----------------------------------------------------------------------#

. common_functions.sh

[ $# -eq 3 ] || die "Error  : Insufficient Arguments .\nSyntax : $0 <project>  \nUsage  : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM>"


#-----------------------------------------------------------------------#
# Invoke the Project Config for Project level variables
#-----------------------------------------------------------------------#

project=${1}
. ${project}.config

#--- Script Parameter validation 

[ $1 = $project_name ] || die "Not Valid Project Name .\nSyntax : $0 <PROJECT> <ISO COUNTRY CODE> <SRC SYSTEM>"

[ "$2" = "GLO" -o "$2" = "CN" -o "$2" = "VN" -o "$2" = "KH" -o "$2" = "PG" -o "$2" = "HK" -o "$2" = "LA" -o "$2" = "US" -o "$2" = "UK" -o "$2" = "ML" -o "$2" = "MB" -o "$2" = "ZZ" -o "$2" = "XX" -o "$2" = "MY" -o "$2" = "ID" -o "$2" = "PH" ] || die "Not Valid country code \nSyntax : $0 <project> <country code> <src system>\nUsage  : $0 Not proper Country code"

[ "$3" = "ICBA" -o "$3" = "SIBS" -o "$3" = "TRMS" -o "$3" = "IRRS" -o "$3" = "GCMS"  -o "$3" = "INTL" ] || die "Error  : Not Valid Source System .\nSyntax : $0 <project>  \nUsage  : $0 Not proper Src sys code"

#----------------------VARIABLE-ASSIGNMENT--------------------#

#---------------------COMMON VARIABLES------------------------#

PCRTY_CD=${2}
PSRC_SYS=${3}

ACHIV_DIR=`getParam pACHIV_DIR`;
RET_PERIOD=`getParam pRET_PERIOD`;

LOG_DIR=`getParam pLOG_DIR`;
TD_LOG_ON_PATH=`getParam pTD_LOG_ON_PATH`
B_TEQ_LOG_ON_FILE=`getParam pB_TEQ_LOG_ON_FILE`;
AUDIT_SRC_FILE_AUDIT_TBL=`getParam pAUDIT_SRC_FILE_AUDIT_TBL`; 
AUDIT_BTCH_STATS_TBL=`getParam pAUDIT_BTCH_STATS_TBL`; 
AUDIT_DATABASE_NM=`getParam pDATABASE_NM`;
FILE_LIST_PATH=`getParam pFILE_LIST_PATH`;
ARCH_BATCH_DT_BATCHID=${FILE_LIST_PATH}/${PCRTY_CD}_${PSRC_SYS}

#-- Get Previous batch id and Business date for archival

[ -f $ARCH_BATCH_DT_BATCHID ] && { rm $ARCH_BATCH_DT_BATCHID ; }

bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.export file=$ARCH_BATCH_DT_BATCHID;
.set width 50000;
	SELECT 
		TRIM(BIZ_DT)||'|'||TRIM(BATCH_ID)(title'')
	FROM 
		$AUDIT_DATABASE_NM.$AUDIT_BTCH_STATS_TBL
	WHERE 
		BATCH_ID=(SELECT MAX(BATCH_ID) FROM $AUDIT_DATABASE_NM.$AUDIT_BTCH_STATS_TBL WHERE TRIM(STATUS)='BATCH-COMPLETED');
.export reset; 
.quit 0;
.logoff;
[BTEQ-FLG]

sed '/^$/d' $ARCH_BATCH_DT_BATCHID > $ARCH_BATCH_DT_BATCHID.tmp;mv $ARCH_BATCH_DT_BATCHID.tmp $ARCH_BATCH_DT_BATCHID;

# Assign the Batch id and batch date to variables

bus_date=`cat $ARCH_BATCH_DT_BATCHID |awk -F '|' '{print $1}'|sed 's/-//g'`
BATCH_ID=`cat $ARCH_BATCH_DT_BATCHID |awk -F '|' '{print $2}'`
BUZ_DT=`cat $ARCH_BATCH_DT_BATCHID |awk -F '|' '{print $1}'`

#----------------------------------------------------------------------------------------------#

FILE_LIST_WTH_PTH=$FILE_LIST_PATH/${PCRTY_CD}_${PSRC_SYS}_FILE_LIST_DATA_ARCH_${BUZ_DT};
FILE_LIST_MODF=$FILE_LIST_PATH/${PCRTY_CD}_${PSRC_SYS}_FILE_LIST_DATA_ARCH_DET_${BUZ_DT};
FILE_LIST_RM=$FILE_LIST_PATH/${PCRTY_CD}_${PSRC_SYS}_FILE_LIST_DATA_ARCH_DET_RM_${BUZ_DT};

ZIP_FILE_NAME=${PCRTY_CD}_${PSRC_SYS}_${bus_date}.zip
ZIP_FILE_PTN=${PCRTY_CD}_${PSRC_SYS}*.zip
LOG_FILE_PATH=${LOG_DIR}${PCRTY_CD}_${PSRC_SYS}_FILE_ARCHIVAL_LOG_${BUZ_DT}.log



#----PREPARE THE LIST FROM AUDIT TABLE FOR DATA MOVEMENT----# 
 
 [ -f $FILE_LIST_WTH_PTH ] && { rm $FILE_LIST_WTH_PTH ; }
 
 [ -f $FILE_LIST_MODF ]&& { rm $FILE_LIST_MODF; } || { touch $FILE_LIST_MODF; };
 
 [ -f $LOG_FILE_PATH ] && { rm $LOG_FILE_PATH ; touch $LOG_FILE_PATH; }
 
echo "---COUNTRY CD:"$PCRTY_CD" ---SOURCE SYSTEM:-"$PSRC_SYS"" >> $LOG_FILE_PATH
 
bteq .run file=$TD_LOG_ON_PATH/$B_TEQ_LOG_ON_FILE <<[BTEQ-FLG];
.export file=$FILE_LIST_WTH_PTH;
.set width 50000;
	select  	
		 trim(batch_id)||','||batch_dt||','||CTRY_CD||','||SRC_SYS_NM||','||trim(zip_out_file_nm)||','||trim(IN_FILE_NM)||','||trim(CASE WHEN  TRIM( is_comp_file)='Y'  THEN TRIM(FILE_DIR) ELSE   TRIM(TARGET_DIR)  END)||','||TRIM(is_comp_file) (title'')
	from 
		$AUDIT_DATABASE_NM.$AUDIT_SRC_FILE_AUDIT_TBL 
	where 
		batch_id=$BATCH_ID 
	and CTRY_CD='$PCRTY_CD' 
	and SRC_SYS_NM='$PSRC_SYS'
	and is_comp_file IN ('Y','N')
	and CTL_FILE_FLG IN ('Y','N');
.export reset; 
.quit 0;
.logoff;
[BTEQ-FLG]

sed '/^$/d' $FILE_LIST_WTH_PTH > $FILE_LIST_WTH_PTH.tmp;mv $FILE_LIST_WTH_PTH.tmp $FILE_LIST_WTH_PTH;

line="$(< $FILE_LIST_WTH_PTH)"

#-----------MOVE THE FILES TO ARCHIVAL DIRECTORY-----------------# 

for line in $line
do

echo $line|awk -F ',' '{print $1" "$2" "$3" "$4" "$5" "$6" "$7" "$8}'|read BTCH_ID BUS_DATE CTRY_CD SRC_SYS_NM VALID_OUT_FILE_NM IN_FILE_NM IN_FILE_DIR IS_COMP_FILE

[ "$SRC_SYS_NM" = "ICBA" -o "$SRC_SYS_NM" = "SIBS" ] && {
     
		[ "$IS_COMP_FILE" = "Y" ] && { 
			
			cp $IN_FILE_DIR$VALID_OUT_FILE_NM $ACHIV_DIR$VALID_OUT_FILE_NM;
			
			[ $? -eq 0 ] && {
			
			echo "::Zip file -"$IN_FILE_NM" moved to the archival directory "$ACHIV_DIR"" >> $LOG_FILE_PATH
			
			rm ${IN_FILE_DIR}${VALID_OUT_FILE_NM};
			
			[ $? -eq 0 ] && { echo "::File "${VALID_OUT_FILE_NM}" is removed successfully in arrival directory" >> $LOG_FILE_PATH ; } || { echo "::Problem in removing the "${VALID_OUT_FILE_NM}" in arrival directory" >> $LOG_FILE_PATH ; };
			
			VALID_OUT_FILE_NM_WITH_OUT_DT="${ACHIV_DIR}"`echo $VALID_OUT_FILE_NM|awk -F '_' '{ print $1}'`"*.zip"
			
			find ${ACHIV_DIR} -type f -name "$VALID_OUT_FILE_NM_WITH_OUT_DT" -mtime +$RET_PERIOD -ls -exec rm -f {} \;
			
			} || { echo "::Problem in file moving to the archival directory "$ACHIV_DIR""; }
		   };
	} || {
	echo ""${IN_FILE_DIR}${VALID_OUT_FILE_NM}"" >> $FILE_LIST_MODF;
	echo ""${IN_FILE_DIR}${VALID_OUT_FILE_NM}"" >> $FILE_LIST_RM;
	}

done

#--------Purging the files in archival directory based on retention policy------------------#


[ "$SRC_SYS_NM" = "ICBA" -o "$SRC_SYS_NM" = "SIBS" ] && { echo " Zip file is already moved to archival directory;" ;} || { 
zip -D ${ACHIV_DIR}${ZIP_FILE_NAME} `cat $FILE_LIST_MODF`;
[ $? -eq 0 ] && { echo "::File is zipped and moved to archival directory" >> $LOG_FILE_PATH ; } || { echo "::Problem in zipping and moved to archival directory" >> $LOG_FILE_PATH ; };
} >> $LOG_FILE_PATH;
  [ $? -eq 0 ] && { echo "::Files get zipped and moved to the archival directory under file name - "$ZIP_FILE_NAME"" >> $LOG_FILE_PATH ;} || { echo "::Problem in zipping and move the file to archival directory - "$ZIP_FILE_NAME"" >> $LOG_FILE_PATH ;}  
  for line in `cat $FILE_LIST_RM`
  do 
	[ -f $line ] && {
	rm $line;
	[ $? -eq 0 ] && { echo "::File "$line" is removed successfully in arrival directory" >> $LOG_FILE_PATH ; } || { echo "::Problem in removing the "$line" in arrival directory" >> $LOG_FILE_PATH ; }
	}||{
	echo "::File "$line" 	available in audit table but not available in arrival directory" >> $LOG_FILE_PATH;
	};
  done;
  
  find ${ACHIV_DIR} -type f -name "$ZIP_FILE_PTN" -mtime +$RET_PERIOD -ls -exec rm -f {} \;


#EOF
