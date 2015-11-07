rm -f server.log
python mrs_gp.py --serve &
sleep 1
python mrs_gp.py --fs write test me line1
python mrs_gp.py --fs write test me line2
python mrs_gp.py --fs write test her she1
python mrs_gp.py --fs write test her she2
python mrs_gp.py --fs write try her try2
python mrs_gp.py --fs write try her try3


