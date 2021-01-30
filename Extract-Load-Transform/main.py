from etl_for_summary_listings import start_etl_pipeline



if __name__ == "__main__":
    #example
    start_etl_pipeline("Dataset_summary\BAR\\","http://data.insideairbnb.com/spain/catalonia/barcelona/2020-12-16/visualisations/listings.csv", "AB_BAR_2020.csv")
