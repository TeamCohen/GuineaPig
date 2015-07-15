GuineaPig
=========

Guinea Pig is (yet another) workflow language for Hadoop.  For more
information, including a tutorial, see:
http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig

As the name suggests, Guinea Pig is similar to Pig, with some
important differences.

* Guinea Pig is pure Python, and embedded in Python, so there's less
new stuff to learn.

* Guinea Pig is simple.  Programs use only ten pre-defined classes
(like _Join_ and _Flatten_), and the full implementation is less than 1500
non-comment-source lines.

* Guinea Pig programs can be executed incrementally, and you can
inspect and/or re-use partially constructed outputs - similar to the
way that you might use _make_ to implement a workflow.

* Guinea Pig programs can be executed with or without a Hadoop
backend, so you can use it for smaller-to-medium sized workflows, and
then migrate these easily to a cluster.

