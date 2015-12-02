For more information, see:
http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig

There's also one larger example here, a soft-join program: phirl-naive.py.
 
The date this version was last modified is stored in the file marker.txt

Recent changes:

 8/1: Reworked the planning system so that *.done files are no longer
 stored - instead, to keep track of expected state and avoid
 re-generating views that have been already stored, a new option
 --alreadyStored v1,...,vk is passed in to the various view-bulding
 ("doXXX") invocations of the script.
 8/13: Bug fix related to Augment and hadoop in guineapig1_2.py. Added
 guineapig1_2.py to the tutorial.
 10/9: added SafeEvaluator to 1.3.
 11/11: added LC_COLLATE=C to sort command
