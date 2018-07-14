CURR_DATE=$(date +%Y_%m_%d_%H:%M:%S)
DIR=$(date +%Y_%m_%d)

#Get input from user
read -p "Enter proper reason behind this operation [Mandatory]: " message
if [ -z "$message" ]
then
	echo "====================================================================================================="
	echo "ERROR: Message cannot be empty. Please re-run script and specify proper reason behind this operation."
	echo "====================================================================================================="
	exit 1
else
	# Create backup directory
	 mkdir -p ~/backups/$DIR/$CURR_DATE
      echo "$message"> ~/backups/$DIR/$CURR_DATE/readme
      echo "\$readme created"
fi

mkdir -p ~/backups/$DIR/$CURR_DATE
echo "Backup Location: /home/centos/backups/"$DIR/$CURR_DATE
cp -r $1 ~/backups/$DIR/$CURR_DATE/
