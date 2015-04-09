 #!/usr/bin/ksh    #  Declares a Korn shell  ###############################
#                                                                          #
##!/usr/bin/sh     #  Declares a Bourne shell                              #
##!/usr/bin/bash   #  Declares a Bourne-Again shell                        #
##!/usr/bin/csh    #  Declares a C shell                                   #
##!/usr/bin/tsh    #  Declares a T shell                                   #
#                                                                          #
# PLATFORM: (AIX, HP-UX, Linux, Solaris & All Nix )                        #
#                                                                          #
#                                                                          #
# PURPOSE: This script take the 2 input as argument and fetch the          #
#           datastage job status and last Start & End time of the job      #
#                                                                          #
#                                                                          #
############################################################################
 
 
. /opt/IBM/InformationServer/Server/DSEngine/dsenv > /dev/null 2>&1
 
if [[ $# -eq 2 ]]; then
PROJECT="$1";
JOB="$2";
out=`dsjob -jobinfo $PROJECT $JOB | egrep 'Job Status|Job Start Time|Last Run Time'`
echo "$PROJECT\t$JOB\t$out";
else
echo "Please execute the script like : $0 PROJECT_NAME JOB_NAME";
fi
