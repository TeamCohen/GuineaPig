#!/bin/bash
MY_EMAIL=somebody@an.email.address
stat=`grep isMaster /mnt/var/lib/info/instance.json | cut -d: -f2`
if [ "$stat" != "" ]; then
    #get the code and unpack it
    wget http://www.cs.cmu.edu/~wcohen/10-605/gpigtut.tgz
    tar -xzf gpigtut.tgz
    #this is needed to initialize the HDFS
    hadoop jar ~/hadoop-examples.jar pi 10 10000000 >& pi-example.log
    #create the default HDFS directory for Guinea Pig on HDFS
    hadoop fs -mkdir /user/hadoop/gp_views
    ########################################
    #if you want, uncomment this section to get an email 
    #notification - after defining your own email address above
    #echo the cluster is ready now - ssh in and cd to tutorial | mail -s 'cluster is now up' $MY_EMAIL
fi
