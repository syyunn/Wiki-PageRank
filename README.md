Wiki-PageRank
=============

PageRank calculation for Wikipedia pages


Team members: 
=============

Manu Sethi and Tanmay Garg


API:
==========================================================================================

We used the newer API for hadoop which worked on AWS version 2.4.2 (Hadoop 1.0.3) - latest.


Steps to develop the code:
==============================================================================================

(1) File WikiLinksGraphGenerator.java uses XmlInputFormat.java to parse the file into titles and text using regular expressions. 
This file also removes the duplicates links and self links. This file does not remove the red links yet. 

(2) Remove the red links
Done by first converting the graph obtained by the above file to an inlink graph. 
Then it is converted back to an outlink graph after removing the red links. 
This process happenes in two map reduce jobs by using the Redlink1 class and calling the runRedlinkRemover1 method twice.

(3) Output of the graph is obtained after removing red links is given to CountN.java. This counts the number of titles.
In order to use "=" and get the answer as "N=14128976" we used the following in the Configuration class of the file CountN.java
conf.set("mapred.textoutputformat.separator", "="); //Prior to Hadoop 2 (YARN)

(4) Modify the graph to give the input to the page rank algorithm. Page rank algorithm required input of the type:
title 1/N links

(5) Pass this input to the PageRank algorithm.  

(6) Implement the sorter to sort in descending order of values.
