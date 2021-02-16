# entity-matcher-AirBB
It performs Entity Matching on AirB&B datasets. 
Given two listings, it recognize if they are talking about the same entity.
It also contains an extract, transform and load pipeline to fetch the data to train the deep learning model. 
## ETL pipeline for complete listings (Blocking workflow)
It represents the blocking workflow to label the data by doing attribute equivalence.
It extracts the data from AirBnB website, then, it creates pair listings ready to be processed by the deep learning model offered by deepmatcher.
## ETL pipeline for summary listings (Blocking workflow)
It represents the blocking workflow to label the data for summary listings.
It extracts the data from AirBnB website, then, it creates pair listings by labeling them.
