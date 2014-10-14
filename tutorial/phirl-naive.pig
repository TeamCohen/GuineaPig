data = FOREACH raw GENERATE rel,docid,FLATTEN(TOKENIZE(LOWER(str))) AS term;

-- compute relation-dependent document frequencies as docfreq:{rel,term,df:int}

docfreq =   
  FOREACH (GROUP data by (rel,term)) 
  GENERATE group.rel AS rel, group.term as term, COUNT(data) as df;

-- find the total number of documents in each relation as ndoc:{rel,c:long}

ndoc1 = DISTINCT(FOREACH data GENERATE rel,docid);
ndoc = FOREACH (GROUP ndoc1 by rel) GENERATE group AS rel, COUNT(ndoc1) AS c;

-- find the un-normalized document vectors as udocvec:{rel.docid,term,weight}
udocvec1 = JOIN data BY (rel,term), docfreq BY (rel,term);
udocvec2 = JOIN udocvec1 BY data::rel, ndoc BY rel;
udocvec = FOREACH udocvec2 GENERATE data::rel, data::docid, data::term, LOG(2.0)*LOG(ndoc::c/(double)docfreq::df) AS weight;

-- find the square of the normalizer for each document: norm:{rel,docid,z2:double}

norm1 = FOREACH udocvec GENERATE rel,docid,term,weight*weight as w2;
norm = FOREACH (GROUP norm1 BY (rel,docid)) GENERATE group.rel AS rel, group.docid AS docid, SUM(norm1.w2) AS z2;

-- compute the TFIDF weighted document vectors as: docvec:{rel,docid,term,weight:double}
docvec = 
   FOREACH (JOIN udocvec BY (rel,docid), norm BY (rel,docid)) 
   GENERATE data::rel AS rel, data::docid AS docid, data::term AS term,  weight/SQRT(z2) as weight;

-- fs -rmr phirl/docvec
-- STORE docvec INTO 'phirl/docvec';
-- docvec = LOAD 'phirl/docvec' AS (rel,docid,term,weight:double);

-- naive algorithm: use all terms for finding potentil matches

rel1Docs = FILTER docvec BY rel=='icepark';
rel2Docs = FILTER docvec BY rel=='npspark';
softjoin1 = JOIN rel1Docs BY term, rel2Docs BY term;
softjoin2 = FOREACH softjoin1 GENERATE rel1Docs::docid AS id1, rel2Docs::docid AS id2, rel1Docs::weight*rel2Docs::weight AS p;
softjoin = FOREACH (GROUP softjoin2 BY (id1,id2)) GENERATE group.id1, group.id2, SUM(softjoin2.p) AS sim;

-- find the top few pairs by similarity: topSimPairs:{id1,id2,sim}

simpairs = FILTER softjoin BY sim>0.5;
orderedSimpairs = ORDER simpairs BY sim DESC;
topSimPairs = LIMIT orderedSimpairs 1000;
-- STORE topSimPairs INTO 'phirl/topSimPairs';

-- diagnostic output: look: {sim,[01],id1,id2,str1,str2}

look1 = JOIN topSimPairs BY id1, raw BY docid; 
look2 = JOIN look1 BY id2, raw BY docid; 
look = FOREACH look2 GENERATE sim, (look1::raw::keyid==raw::keyid ? 1 : 0), id1,id2, look1::raw::str AS str1,raw::str AS str2;
-- STORE look INTO 'phirl/look';
