MRS=pypy ../mrs_gp.py
K=5
ASYNC = 0

#
clean: 
	rm -rf tmp tmp.txt sharded parallel-events.txt server.log test test2 gpig_views

backup:
	cp ../mrs_gp_v4.py ../backup/`date "+%H:%M:%S"`-mrs_gp_v4.py

# file-file

shard-sample:

	time  $(MRS) --input unsharded-sample --output tmp/sample-copy --mapper cat --reducer cat --numReduceTasks $(K)
	echo should be the same
	sort unsharded-sample/* | sum
	sort tmp/sample-copy/* | sum

copy:
	$(MRS) --serve &
	sleep 1
	$(MRS) --task --input test --output tmp/test-copy --mapper cat
	$(MRS) --shutdown
	echo should be the same
	sum test/* tmp/test-copy/*

copy2:
	$(MRS) --serve &
	sleep 1
	$(MRS) --task --input test --output tmp/test-copy --mapper cat
	$(MRS) --task --input test --output tmp/test-copy --mapper cat
	$(MRS) --shutdown
	echo should be the same
	sum test/* tmp/test-copy/*

shard:
	$(MRS) --serve &
	sleep 1
	$(MRS) --task --input test --output tmp/test-copy --mapper cat --reducer cat --numReduceTasks 3
	$(MRS) --shutdown
	echo should have 3 files
	ls tmp/test-copy
	echo should be the same
	sort test/* | sum
	sort tmp/test-copy/* | sum

up:
	$(MRS) --serve 

down:
	$(MRS) --shutdown

shard-fixed-server:
	$(MRS) --task --input test --output tmp/test-copy --mapper cat --reducer cat --numReduceTasks 3

reshard: 
	make -B shard
	$(MRS) --serve &
	sleep 1
	$(MRS) --task --input tmp/test-copy --output tmp/test-copy2 --mapper cat --reducer cat --numReduceTasks 2
	$(MRS) --shutdown
	echo should have 3 files
	ls tmp/test-copy
	echo should have 2 files
	ls tmp/test-copy2
	echo should be the same
	sort test/* | sum
	sort tmp/test-copy/* | sum
	sort tmp/test-copy2/* | sum

# file-memory 

load:
	$(MRS) --serve &
	sleep 1
	$(MRS) --task --input test --output gpfs:test-copy --mapper cat
	$(MRS) --send '/getmerge?dir=test-copy' > tmp.txt
	$(MRS) --shutdown
	echo should be the same
	sum test/* tmp.txt

load-shard:
	$(MRS) --serve &
	sleep 1
	$(MRS) --task --input test --output gpfs:test-copy --mapper cat --reducer cat --numReduceTasks 3
	echo should have 3 files
	$(MRS) --send '/ls?dir=test-copy'
	$(MRS) --send '/getmerge?dir=test-copy' > tmp.txt
	$(MRS) --shutdown
	echo should be the same
	sort test/* | sum
	sort tmp.txt | sum

roundtrip:
	$(MRS) --serve &
	sleep 1
	$(MRS) --task --input test --output gpfs:test-copy --mapper cat 
	$(MRS) --task --output tmp/test-copy --input gpfs:test-copy --mapper cat
	$(MRS) --shutdown
	echo should be the same
	sort test/* | sum
	sort tmp/test-copy/* | sum

roundtrip-reshard:
	$(MRS) --serve &
	sleep 1
	$(MRS) --task --input test --output gpfs:test-copy --mapper cat --reducer cat --numReduceTasks 3
	$(MRS) --task --output tmp/test-copy --input gpfs:test-copy --mapper cat --reducer cat --numReduceTasks 2
	$(MRS) --shutdown
	echo should be the same
	sort test/* | sum
	sort tmp/test-copy/* | sum

parallel-events: 
	$(MRS) --serve &
	sleep 1
	echo sharding to $(K) shards
	time  $(MRS) --task --input unsharded --output gpfs:sharded --mapper cat --reducer cat --numReduceTasks $(K)
	echo training naive bayes in parallel
	time $(MRS) --task --input gpfs:sharded --output gpfs:events --numReduceTasks $(K) --async $(ASYNC) \
		--mapper 'python streamNaiveBayesLearner.py --streamTrain 100' \
		--reducer 'python sum-events.py'
	echo time $(MRS) --task --input gpfs:sharded --output gpfs:events --mapper 'python streamNaiveBayesLearner.py --streamTrain 100' --async $(ASYNC)
	echo export
	$(MRS) --send '/getmerge?dir=events' > parallel-events.txt
	$(MRS) --shutdown

parallel-events-ram: 
	echo sharding to $(K) shards
	time  $(MRS)  --input unsharded --output /mnt/ramdisk/mrs/sharded --mapper cat --reducer cat --numReduceTasks $(K)
	echo training naive bayes in parallel
	time $(MRS) --input /mnt/ramdisk/mrs/sharded --output /mnt/ramdisk/mrs/events --numReduceTasks $(K) \
		--mapper 'python streamNaiveBayesLearner.py --streamTrain 100' \
		--reducer 'python sum-events.py'

# --streamTrain 100
# threads      shard	learn
# <baseline>    n/a	240.50???
# 1	       	5.65	443.32
# 5	       	6.18	130.19
# 10		6.59	103.47
# 20		8.55	107.96

RAM=/mnt/ramdisk/mrs/
NBCOUNT='python streamNaiveBayesLearner.py --streamTrain 100'
SUM='python sum-events.py'
BIG=bigfile1  #100M
#BIG=bigfile2 #1G

bigfile-times:
	$(MRS) --serve &
	sleep 1
	echo sharding
	time $(MRS) --task --input $(RAM)/$(BIG) --output gpfs:sharded --mapper cat --reducer cat --numReduceTasks $(K)
	echo learning
	time $(MRS) --task --input gpfs:sharded --output gpfs:events --numReduceTasks $(K) --mapper $(NBCOUNT) --reducer $(SUM) --async $(ASYNC)
	$(MRS) --shutdown

bigfile-times-ram:
	echo sharding
	time $(MRS) --input $(RAM)/$(BIG) --output $(RAM)/sharded --mapper cat --reducer cat --numReduceTasks $(K)
	echo learning
	time $(MRS) --input $(RAM)/sharded --output $(RAM)/events --numReduceTasks $(K) --mapper $(NBCOUNT) --reducer $(SUM)

bigfile-baseline-time:
	time sh baseline.sh
