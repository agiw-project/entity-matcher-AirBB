import pandas as pd
import requests


"""
It extracts a csv file from a url and it saves 
in the local directory in path with the name filename
"""
def extract(path, filename, csv_url):
    req = requests.get(csv_url)
    url_content = req.content
    csv_file = open(path + filename, 'wb')
    csv_file.write(url_content)
    csv_file.close()

"""
It loads a csv and return a dataframe
"""
def load(path, filename):
    df = pd.read_csv(path + filename)
    return df


"""
It transforms the dataframe to be processed by deepmatch NN
"""
def transform(df):
    computable_df = df.copy()
    ground_truth = compute_ground_truth(computable_df)
    computable_df = df.copy()
    new_df1 = compute_false_dataset_with_same_neighbourhood_latitude_and_longitude(computable_df)
    computable_df = df.copy()
    new_df2 = compute_false_dataset_with_same_room_type_and_price_and_minimum_nights(computable_df)
    computable_df = df.copy()
    new_df3 = compute_false_dataset_with_same_last_review_and_calculated_host_listings_count_and_longitude(computable_df)
    computable_df = df.copy()
    new_df4 = compute_false_dataset_with_same_minimum_nights_and_availability_365_and_longitude(computable_df)
    #we merge the dfs in one
    frames = [ground_truth, new_df1, new_df2, new_df3, new_df4]
    result = pd.concat(frames)
    rename_columns(result)
    return result

def renamer(old_names):
    new_columns = {}
    for name in old_names:
        if name.endswith("_left"):
            new_name = "left_"
            new_name += name[0:-5]
            new_columns[name] = new_name
        else:
            if name.endswith("_right"):
                new_name = "right_"
                new_name += name[0:-6]
                new_columns[name] = new_name
            else:
                new_columns[name] = name
    return new_columns



def rename_columns(df):
    old_columns = df.columns
    new_names = renamer(old_columns)
    df.rename(columns=new_names, inplace=True)



def compute_ground_truth(df):
    new_df = pd.merge(df, df, left_on=['name'], right_on = ['name'], suffixes=('_left', '_right'))
    df = new_df[new_df["id_left"] != new_df["id_right"]]
    df.insert(1, "label", 1, True)
    df.rename(columns={'id_left': 'id'}, inplace=True)
    df = df[['id','label', 'host_id_left', 'host_name_left',
       'neighbourhood_group_left', 'neighbourhood_left', 'latitude_left',
       'longitude_left', 'room_type_left', 'price_left', 'minimum_nights_left',
       'number_of_reviews_left', 'last_review_left', 'reviews_per_month_left',
       'calculated_host_listings_count_left', 'availability_365_left',
       'neighbourhood_right', 'host_id_right',
       'host_name_right', 'neighbourhood_group_right', 'latitude_right',
       'longitude_right', 'room_type_right', 'price_right',
       'minimum_nights_right', 'number_of_reviews_right', 'last_review_right',
       'reviews_per_month_right', 'calculated_host_listings_count_right',
       'availability_365_right']]
    ground_truth = df
    return ground_truth


def compute_false_dataset_with_same_neighbourhood_latitude_and_longitude(df):
    new_df = pd.merge(df, df, left_on=['neighbourhood', 'latitude', 'longitude'],
                      right_on=['neighbourhood', 'latitude', 'longitude'], suffixes=('_left', '_right'))
    app = new_df['neighbourhood']
    new_df.rename(columns={'neighbourhood': 'neighbourhood_left'}, inplace=True)
    new_df.insert(18, "neighbourhood_right", app, True)
    new_df = new_df[new_df["id_left"] != new_df["id_right"]]
    df = new_df[(new_df['name_left'] != new_df['name_right'])].copy()
    app = df['latitude']
    df.insert(17, "latidute_right", app, True)
    df.insert(7, "latidute_left", app, True)
    app = df['longitude']
    df.insert(18, "longitude_right", app, True)
    output = df.copy()
    output.rename(columns={'longitude': 'longitude_left', 'neighbourhood': 'neighbourhood_left'}, inplace=True)
    output.insert(1, "label", 0, True)
    output.rename(columns={'id_left': 'id'}, inplace=True)
    output = output.reindex(columns=['id', 'label', 'host_id_left', 'host_name_left',
                                     'neighbourhood_group_left', 'neighbourhood_left', 'latitude_left',
                                     'longitude_left', 'room_type_left', 'price_left', 'minimum_nights_left',
                                     'number_of_reviews_left', 'last_review_left', 'reviews_per_month_left',
                                     'calculated_host_listings_count_left', 'availability_365_left',
                                     'neighbourhood_right', 'host_id_right',
                                     'host_name_right', 'neighbourhood_group_right', 'latitude_right',
                                     'longitude_right', 'room_type_right', 'price_right',
                                     'minimum_nights_right', 'number_of_reviews_right', 'last_review_right',
                                     'reviews_per_month_right', 'calculated_host_listings_count_right',
                                     'availability_365_right'])

    return output


def compute_false_dataset_with_same_room_type_and_price_and_minimum_nights(df):
    new_df = pd.merge(df, df, left_on=['room_type', 'price', 'minimum_nights', 'latitude'],
                      right_on=['room_type', 'price', 'minimum_nights', 'latitude'], suffixes=('_left', '_right'))
    new_df = new_df[new_df["id_left"] != new_df["id_right"]]
    df = new_df[(new_df['name_left'] != new_df['name_right'])].copy()
    app = df['room_type']
    df.insert(17, "room_type_right", app, True)
    app = df['price']
    df.insert(18, "price_right", app, True)
    app = df['minimum_nights']
    df.insert(18, "minimum_nights_right", app, True)
    app = df['latitude']
    df.insert(18, "latidute_right", app, True)
    output = df.copy()
    output.rename(
        columns={'room_type': 'room_type_left', 'price': 'price_left', 'minimum_nights': 'minimum_nights_left',
                 'latitude': 'latitude_left'}, inplace=True)
    output.insert(1, "label", 0, True)
    output.rename(columns={'id_left': 'id'}, inplace=True)
    output = output.reindex(columns=['id', 'label', 'host_id_left', 'host_name_left',
                                     'neighbourhood_group_left', 'neighbourhood_left', 'latitude_left',
                                     'longitude_left', 'room_type_left', 'price_left', 'minimum_nights_left',
                                     'number_of_reviews_left', 'last_review_left', 'reviews_per_month_left',
                                     'calculated_host_listings_count_left', 'availability_365_left',
                                     'neighbourhood_right', 'host_id_right',
                                     'host_name_right', 'neighbourhood_group_right', 'latitude_right',
                                     'longitude_right', 'room_type_right', 'price_right',
                                     'minimum_nights_right', 'number_of_reviews_right', 'last_review_right',
                                     'reviews_per_month_right', 'calculated_host_listings_count_right',
                                     'availability_365_right'])

    return output


def compute_false_dataset_with_same_last_review_and_calculated_host_listings_count_and_longitude(df):
    new_df = pd.merge(df, df, left_on=['last_review', 'calculated_host_listings_count', 'longitude'],
                      right_on=['last_review', 'calculated_host_listings_count', 'longitude'],
                      suffixes=('_left', '_right'))
    new_df = new_df[new_df["id_left"] != new_df["id_right"]]
    df = new_df[(new_df['name_left'] != new_df['name_right'])].copy()
    app = df['last_review']
    df.insert(17, "last_review_right", app, True)
    app = df['calculated_host_listings_count']
    df.insert(18, "calculated_host_listings_count_right", app, True)
    app = df['longitude']
    df.insert(18, "longitude_right", app, True)
    output = df.copy()
    output.rename(columns={'last_review': 'last_review_left', 'longitude': 'longitude_left',
                           'calculated_host_listings_count': 'calculated_host_listings_count_left'}, inplace=True)
    output.insert(1, "label", 0, True)
    output.rename(columns={'id_left': 'id'}, inplace=True)
    output = output.reindex(columns=['id', 'label', 'host_id_left', 'host_name_left',
                                     'neighbourhood_group_left', 'neighbourhood_left', 'latitude_left',
                                     'longitude_left', 'room_type_left', 'price_left', 'minimum_nights_left',
                                     'number_of_reviews_left', 'last_review_left', 'reviews_per_month_left',
                                     'calculated_host_listings_count_left', 'availability_365_left',
                                     'neighbourhood_right', 'host_id_right',
                                     'host_name_right', 'neighbourhood_group_right', 'latitude_right',
                                     'longitude_right', 'room_type_right', 'price_right',
                                     'minimum_nights_right', 'number_of_reviews_right', 'last_review_right',
                                     'reviews_per_month_right', 'calculated_host_listings_count_right',
                                     'availability_365_right'])

    return output


def compute_false_dataset_with_same_minimum_nights_and_availability_365_and_longitude(df):
    new_df = pd.merge(df, df, left_on=['minimum_nights', 'availability_365', 'longitude'],
                      right_on=['minimum_nights', 'availability_365', 'longitude'], suffixes=('_left', '_right'))
    new_df = new_df[new_df["id_left"] != new_df["id_right"]]
    df = new_df[(new_df['name_left'] != new_df['name_right'])].copy()
    app = df['minimum_nights']
    df.insert(17, "minimum_nights_right", app, True)
    app = df['availability_365']
    df.insert(18, "availability_365_right", app, True)
    app = df['longitude']
    df.insert(18, "longitude_right", app, True)
    output = df.copy()
    output.rename(columns={'minimum_nights': 'minimum_nights_left', 'availability_365': 'availability_365_left',
                           'longitude': 'longitude_left'}, inplace=True)
    output.insert(1, "label", 0, True)
    output.rename(columns={'id_left': 'id'}, inplace=True)
    output = output.reindex(columns=['id', 'label', 'host_id_left', 'host_name_left',
                                     'neighbourhood_group_left', 'neighbourhood_left', 'latitude_left',
                                     'longitude_left', 'room_type_left', 'price_left', 'minimum_nights_left',
                                     'number_of_reviews_left', 'last_review_left', 'reviews_per_month_left',
                                     'calculated_host_listings_count_left', 'availability_365_left',
                                     'neighbourhood_right', 'host_id_right',
                                     'host_name_right', 'neighbourhood_group_right', 'latitude_right',
                                     'longitude_right', 'room_type_right', 'price_right',
                                     'minimum_nights_right', 'number_of_reviews_right', 'last_review_right',
                                     'reviews_per_month_right', 'calculated_host_listings_count_right',
                                     'availability_365_right'])

    return output


"""
It splits the df into three different datasets:
- train 60%
- validation 20%
- test 20%
"""
def train_validate_test_split(df, train_percent=0.6, validate_percent=0.2):
    df = df.sample(frac=1).reset_index(drop=True)
    m = len(df.index)
    train_end = int(train_percent * m)
    validate_end = int(validate_percent * m) + train_end
    train = df.iloc[:train_end]
    validate = df.iloc[train_end:validate_end]
    test = df.iloc[validate_end:]
    return train, validate, test

"""
It saves the file created 
"""
def save_and_split(df, path):
    train, validate, test = train_validate_test_split(df)
    train.to_csv(path + 'train.csv', index=False, header=True)
    validate.to_csv(path + 'validation.csv', index=False, header=True)
    test.to_csv (path + 'test.csv', index = False, header=True)

"""
This is the main function of the etl pipeline
The pipeline starts from here.
path: e.g. Dataset_summary/BAR/
csv_url: the url of the csv file to download
filename: the name of the file to be saved
"""
def start_etl_pipeline(path, csv_url, filename):
    extract(path, filename, csv_url)
    df = load(path, filename)
    new_df = transform(df)
    save_and_split(new_df,path)
    print_statistics(path)

"""
It prints out the statistics of the files created.
The number of tuples labeled with 1 
The number of tuples labeled with 0
"""
def print_statistics(path):
    train = pd.read_csv(path + "train.csv")
    train_statistics = train.groupby(['label']).count()
    print(train_statistics)

    valid = pd.read_csv(path + "validation.csv")
    valid_statistics = valid.groupby(['label']).count()
    print(valid_statistics)

    test = pd.read_csv(path + "test.csv")
    test_statistics = test.groupby(['label']).count()
    print(test_statistics)



