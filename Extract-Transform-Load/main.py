from etl import ETLPipeline
from etl_for_summary import ETLPipelineSummary


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

if __name__ == "__main__":
    run_etl_Amsterdam_without_extraction()
