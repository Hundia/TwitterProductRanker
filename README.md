Authors
-------
Research and development by Eli Hundia along side Menashe Fogel at Haifa Univercity as part of a data minig laboratory course in my MSC at CS. 

Introduction
------------
In this project we mined tweeter data in order to compare topic popularities comparing to google trends in corresponding timelines.
The assumption is that people tweet as they search, and that mining tweets in a given period of time will give similar behavior when it comes to the amount of tweets / search in tweeter / google search by web users.
In this project we learned how to work with tweeter large database (~1000 GIGA Bytes of tweets) and focused on the first year of 2015.
We compared our results to google trends for a wide variety of topics from different aspects of our society.
It appears, that people tweet as they search.

Database
--------
The twitter database was given to me by Haifa Univercity CS department and included tweets from USA of 2015.

The Project
-----------
The project is python based for most of the analysis, has a NodeJs server accompenied by a MongoDB database for easy storing and querying of our research results. At first we attempted to work with Hadoop, but eventually decided to simply manually reduce the DB files using multi-processed based python application.

Results
-------
Simply put, People Tweet as they search. The results clearly show (Not proven, worked on arbitrary popular topics) a linear relation between the trends results on our tweeter word count and google trends correlated results.
The applications for such a tool can vary, it gives a tweeter DB owner the ability to analyze trends in different aspects of our society merely by wordcounting topics.

Propect Presentation
--------------------
https://drive.google.com/open?id=0B1cisIvB7e8paVlKTkV5T2w4TDl3MGNWcVpFeUk1Y2FvZXkw

Contact
-------
Mail: Eli.Hundia@gmail.com
Web:  https://elihu.000webhostapp.com/
