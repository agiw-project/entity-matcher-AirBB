from etl import ETLPipeline
from etl_for_summary import ETLPipelineSummary
import pandas as pd


def run_etl_Rome_summary_without_extraction():
    etl = ETLPipelineSummary("Dataset_summary\ROM\\","")
    etl.run_without_extraction()

def run_etl_Rome_summary():
    etl = ETLPipelineSummary("Dataset_summary\ROM\\","http://data.insideairbnb.com/italy/lazio/rome/2020-12-17/visualisations/listings.csv")
    etl.run()

def run_etl_Rome_without_extraction():
    etl = ETLPipeline("Dataset_summary\ROM\\","")
    etl.run_without_extraction()

def run_etl_Rome_without_extraction():
    etl = ETLPipeline("Dataset\ROM\\","http://data.insideairbnb.com/italy/lazio/rome/2020-12-17/data/listings.csv.gz")
    etl.run()

def run_etl_Amsterdam_summary_without_extraction():
    etl = ETLPipelineSummary("Dataset_summary\AMS\\","")
    etl.run_without_extraction()

def run_etl_Amsterdam_summary():
    etl = ETLPipelineSummary("Dataset_summary\AMS\\","http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2020-12-12/visualisations/listings.csv")
    etl.run()

def run_etl_Amsterdam_without_extraction():
    etl = ETLPipeline("Dataset_summary\AMS\\","")
    etl.run_without_extraction()

def run_etl_Amsterdam_without_extraction():
    etl = ETLPipeline("Dataset\AMS\\","http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2020-12-12/data/listings.csv.gz")
    etl.run()



def get_listing_urls(left_listing_id:str, right_listing_id:str, path:str, complete_listing_filename = "listing.csv"):
    """
    :param left_listing_id: id of the listing we want to find the url
    :param right_listing_id: id of the listing we want to find the url
    :param path: the path of the file containing the complete listing data
    :param complete_listing_filename: the name of the file containing the complete data for the listing (expected csv)
    :return: a pair of the urls of the listings
    """
    df = pd.read_csv(path + complete_listing_filename, error_bad_lines=False, warn_bad_lines=True)
    df1 = df[df["id"] == left_listing_id]
    df2 = df[df["id"] == right_listing_id]
    url_left = df1["listing_url"].to_numpy()[0]
    url_right = df2["listing_url"].to_numpy()[0]
    return (url_left, url_right)




if __name__ == "__main__":
    etl = ETLPipelineSummary("Dataset_summary\ROM\\","")
    etl.run_last_step()
