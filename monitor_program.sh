# ! /bin/sh
#
SLEEP_SEC=2
#
PRO_NAME="$1"
if [ "${PRO_NAME}" = "" ];then
   echo "Usage: ${0##*/} <program_name>"
   exit 1
fi
#
LOG_FILE="`pwd`/${PRO_NAME}_info_${SLEEP_SEC}_`date +%Y%m%d%H%M%S`.log"
echo "USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND" > ${LOG_FILE}
#
while [ 1 ]
do
   echo "------ `date \"+%Y-%m-%d %H:%M:%S\"` ------"       >> ${LOG_FILE}
   ps -aux | grep ${PRO_NAME} | grep -v grep | grep -v tail >> ${LOG_FILE}
   sleep ${SLEEP_SEC}
done
#
