MRS=python ../mrs_gp.py

clean:
	rm -rf sharded unsharded-copy sharded-copy events events.txt

unsharded-copy: unsharded/train.txt
	checking a one-process map-only task
	$(MRS) --input unsharded --output unsharded-copy --mapper cat
	echo these should be the same
	sum unsharded/* unsharded-copy/*

sharded-copy: sharded
	echo checking a multi-process map-only task
	$(MRS) --input sharded --output sharded-copy --mapper cat
	echo checking file sizes
	wc sharded/*
	wc sharded-copy/* 

sharded: unsharded/train.txt
	echo checking a multi-process map-reduce task
	$(MRS) --input unsharded --output sharded --mapper cat --reducer cat --numReduceTasks 10
	echo checking file sizes
	wc unsharded/* 
	wc sharded/*

events: sharded
	echo == training naive bayes in parallel
	$(MRS) 	--input sharded --output events --numReduceTasks 10 \
		--mapper 'python streamNaiveBayesLearner.py --train' \
		--reducer 'python sum-events.py'

nb-train-stream-and-sort: unsharded/train.txt
	echo == training naive bayes directly
	cat unsharded/train.txt | python streamNaiveBayesLearner.py --train \
	| LC_COLLATE=C sort | python sum-events.py > events.txt

compare-events: events.txt events/
	sort events.txt > tmp1
	sort events/* > tmp2
	sum tmp1 tmp2
	rm tmp1 tmp2

