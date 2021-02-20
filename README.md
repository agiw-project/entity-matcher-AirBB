# entity-matcher-AirBB
It performs Entity Matching on AirB&B datasets. 
Given two listings, it recognize if they are talking about the same entity. It uses two different metodologies:
- deepmatcher, available here https://github.com/anhaidgroup/deepmatcher
- DeepER, explained here https://arxiv.org/abs/1710.00597

It also contains an extract, transform and load pipeline to fetch the data and preprocess to be ready for the training phase.
The blocking workflows labels the listing pairs by performing attribute matching.

### ETL pipeline for complete listings (blocking workflow)
It represents the blocking workflow to label the data by doing attribute equivalence.
It extracts the data from AirBnB website, then, it creates pair listings ready to be processed by the deep learning model offered by deepmatcher
and DeepER.

### ETL pipeline for summary listings (blocking workflow)
It represents the blocking workflow to label the data for summary listings.
It extracts the data from AirBnB website, then, it creates pair listings by labeling them.

## How to use it
Use the functions in the `main.py` file to run  the pipeline entirely or partially (without extraction).
