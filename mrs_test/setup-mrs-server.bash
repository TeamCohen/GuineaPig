export GPHOME=`(cd ..;pwd)`
#so we can import gpextras and guineapig
export PYTHONPATH=$GPHOME
export GP_MRS_COMMAND="python $GPHOME/mrs_gp.py --task"
rm -f server.log
python $GPHOME/mrs_gp.py --serve &

