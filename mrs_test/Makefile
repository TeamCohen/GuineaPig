#DIR=/mnt/ramdisk/mrs/
DIR=.
MRS=pypy ../mrs_gp.py
K=2

clean:
	rm -rf sharded unsharded-copy sharded-copy 

tests: unsharded-copy sharded-copy compare-events

unsharded-copy: unsharded/train.txt
	echo checking a one-process map-only task
	$(MRS) --input $(DIR)/unsharded --output $(DIR)/unsharded-copy --mapper cat
	echo these should be the same
	sum unsharded/* unsharded-copy/*

sharded-copy: sharded
	echo checking a multi-process map-only task
	$(MRS) --input $(DIR)/sharded --output $(DIR)/sharded-copy --mapper cat
	echo checking file sizes
	wc sharded/*
	wc sharded-copy/* 

sharded: unsharded/train.txt
	echo checking a multi-process map-reduce task
	$(MRS) --input $(DIR)/unsharded --output $(DIR)/sharded --mapper cat --reducer cat --numReduceTasks $(K)
	echo checking file sizes
	wc unsharded/* 
	wc sharded/*

events: sharded
	echo checking job of training naive bayes in parallel
	time $(MRS) --input $(DIR)/sharded --output $(DIR)/events --numReduceTasks $(K) \
		--mapper 'python streamNaiveBayesLearner.py --streamTrain 100' \
		--reducer 'python sum-events.py'

tmp: sharded
	echo checking job of training naive bayes in parallel
	time $(MRS) --input $(DIR)/sharded --output $(DIR)/tmp --numReduceTasks $(K) \
		--mapper 'python streamNaiveBayesLearner.py --streamTrain 10' \
		--reducer 'cat'

events.txt: unsharded/train.txt
	echo == training naive bayes directly
	cat unsharded/train.txt | python streamNaiveBayesLearner.py --streamTrain 100 \
	| LC_COLLATE=C sort | python sum-events.py > events.txt

compare-events: events.txt events
	sort events.txt > tmp1
	sort events/* > tmp2
	sum tmp1 tmp2
	rm tmp1 tmp2

