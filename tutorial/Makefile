update:
	cp ../emr-extras/*-sample.sh
	cp ../guineapig.py  .

clean:
	rm -rf gpig_views
	rm -f total.gp
	rm -f *.pyc

tar: update
	echo created on `date` > marker.txt
	(cd ..; tar -cvzf tutorial.tgz tutorial/marker.txt tutorial/guineapig.py tutorial/*corpus.txt tutorial/id-parks.txt tutorial/*.py)
	mv ../tutorial.tgz .

upload: tar
	scp tutorial.tgz raff.ml.cmu.edu:~/afs-home/www/10-605/gpigtut.tgz

all-runs: run-wordcount run-ntup-wordcount run-prefcount run-wordcmp run-wordprob run-multi run-phirl run-reuse check-phirl
	echo all tests run, did you see any problems\?

run-wordcount:
	echo short version
	python wordcount.py --pprint wc
	python wordcount.py --store wc
	head gpig_views/wc.gp
	echo long version
	python longer-wordcount.py --store wordCount
	head gpig_views/wordCount.gp 
	echo instance version
	python instance-wordcount.py --pprint wordCount
	python instance-wordcount.py --store wordCount
	head gpig_views/wordCount.gp
	echo param version
	python param-wordcount.py --pprint wc --params corpus:bluecorpus.txt
	python param-wordcount.py --store wc --params corpus:bluecorpus.txt
	head -4000 gpig_views/wc.gp | tail

run-ntup-wordcount:
	python ntup-wordcount.py --store commonWords
	head gpig_views/commonWords.gp 

run-prefcount:
	python prefix-count.py --pprint pc3
	python prefix-count.py --store pc3 --params prefixLen:2
	head -1000 gpig_views/pc3.gp | tail

run-wordcmp:
	python wordcmp.py --pprint result
	python wordcmp.py --store result
	sort -gr gpig_views/result.gp | head
	sort -gr gpig_views/result.gp | tail

run-wordprob:
	python wordprob.py --pprint prob
	python wordprob.py --store prob
	sort -r gpig_views/prob.gp | head

run-multi:
	python multi-wordcount.py 3 bluecorpus.txt redcorpus.txt 
	ls -al wc-for*
	tail wc-for*

run-phirl:
	python phirl-naive.py --store look
	sort -gr gpig_views/look.gp | head
	sort -g gpig_views/look.gp | head


run-reuse:
	rm -f gpig_views/*.gp*
	time python phirl-naive.py --store look	
	time python phirl-naive.py --store look	--reuse gpig_views/*.gp

check-phirl: run-phirl
	sort -gr gpig_views/look.gp > phirl-actual-output.txt
	diff phirl-actual-output.txt phirl-expected-output.txt


