import pandas as pd
import requests


class ETLPipeline:
    def __init__(self, path, csv_url):
        self.path = path
        self.csv_url = csv_url

    """
    It extracts a csv file from a url and it saves 
    in the local directory in path with the name filename
    """
    def extract(self):
        req = requests.get(self.csv_url)
        url_content = req.content
        file = open(self.path + "listing.csv.gz", 'wb')
        file.write(url_content)
        file.close()

    """
    It extracts a csv file from a url and it saves 
    in the local directory in path with the name filename
    """

    def extract_csv(self):
        req = requests.get(self.csv_url)
        url_content = req.content
        file = open(self.path + "listing.csv", 'wb')
        file.write(url_content)
        file.close()

    """
    It loads a csv and return a dataframe
    """
    def load_compressed(self) -> pd.DataFrame:
        df = pd.read_csv(self.path + "listing.csv.gz", compression='gzip', error_bad_lines=False)
        return df

    """
    It loads a csv and return a dataframe
    """
    def load_csv(self) -> pd.DataFrame:
        df = pd.read_csv(self.path + "listing.csv", error_bad_lines=False)
        return df


    """
    It loads the groundtruth and returns a dataframe
    """
    def load_ground_truth(self) -> pd.DataFrame:
        df = pd.read_csv(self.path + "groundtruth.csv")
        return df

    """
    It transforms the dataframes into a format to be processed by deepmatch NN
    """
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        ground_truth = self.load_ground_truth()
        new_df1 = self.compute_false_dataset_with_same_room_type_and_price_and_minimum_nights(df)
        new_df2 = self.compute_false_dataset_with_same_last_review_and_calculated_host_listings_count_and_longitude(df)
        new_df3 = self.compute_false_dataset_with_same_minimum_nights_and_availability_365_and_longitude(df)

        # we merge the dfs into one
        frames = [ground_truth, new_df1, new_df2, new_df3]
        result = pd.concat(frames)
        self.rename_columns(result)
        result["id"] = result.index + 1

        print(len(result))

        cols = result.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        result = result[cols]

        return result


    def get_columns_ordered(self, columns):
        column_names = []
        column_names.append("label")
        for col in columns:
            new_name = col + "_left"
            column_names.append(new_name)

        for col in columns:
            new_name = col + "_right"
            column_names.append(new_name)

        return column_names


    def renamer(self, old_names):
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


    def rename_columns(self, df):
        old_columns = df.columns
        new_names = self.renamer(old_columns)
        df.rename(columns=new_names, inplace=True)


    def compute_ground_truth(self, df):
        column_names = self.get_columns_ordered(df)

        new_df = pd.merge(df, df, left_on=['host_id','longitude','latitude','bedrooms','bathrooms','beds'],
                                  right_on=['host_id','longitude','latitude','bedrooms','bathrooms','beds'],
                                  suffixes=('_left', '_right'))

        df = new_df[new_df["id_left"] != new_df["id_right"]]

        app = df['host_id'].copy()
        df.rename(columns={'host_id': 'host_id_left'}, inplace=True)
        df.insert(18, "host_id_right", app, True)

        app = df['latitude'].copy()
        df.rename(columns={'latitude': 'latitude_left'}, inplace=True)
        df.insert(18, "latitude_right", app, True)

        app = df['longitude'].copy()
        df.rename(columns={'longitude': 'longitude_left'}, inplace=True)
        df.insert(18, "longitude_right", app, True)

        app = df['bedrooms'].copy()
        df.rename(columns={'bedrooms': 'bedrooms_left'}, inplace=True)
        df.insert(18, "bedrooms_right", app, True)

        app = df['bathrooms'].copy()
        df.rename(columns={'bathrooms': 'bathrooms_left'}, inplace=True)
        df.insert(18, "bathrooms_right", app, True)

        app = df['beds'].copy()
        df.rename(columns={'beds': 'beds_left'}, inplace=True)
        df.insert(18, "beds_right", app, True)

        df.insert(1, "label", 1, True)

        df = df[column_names]
        return df

    """
    It prepares the ground truth to be physically labeled.
    """
    def prepare_ground_truth_from_compressed(self):
        df = self.load_compressed()
        gt = self.compute_ground_truth(df)
        gt.to_csv(self.path + "groundtruth.csv", index=False, header=True)

    """
    It prepares the ground truth to be physically labeled.
    """

    def prepare_ground_truth_from_csv(self):
        df = self.load_csv()
        gt = self.compute_ground_truth(df)
        gt.to_csv(self.path + "groundtruth.csv", index=False, header=True)


    def compute_false_dataset_with_same_room_type_and_price_and_minimum_nights(self, df):
        column_names = self.get_columns_ordered(df.columns)

        new_df = pd.merge(df, df,
                          left_on=['room_type', 'price', 'minimum_nights', 'latitude'],
                          right_on=['room_type', 'price', 'minimum_nights', 'latitude'],
                          suffixes=('_left', '_right'))

        # Selection
        new_df = new_df[new_df["id_left"] != new_df["id_right"]]

        # Column renaming
        app = new_df['room_type'].copy()
        new_df.rename(columns={'room_type': 'room_type_left'}, inplace=True)
        new_df.insert(18, "room_type_right", app, True)

        app = new_df['price'].copy()
        new_df.rename(columns={'price': 'price_left'}, inplace=True)
        new_df.insert(17, "price_right", app, True)

        app = new_df['minimum_nights'].copy()
        new_df.rename(columns={'minimum_nights': 'minimum_nights_left'}, inplace=True)
        new_df.insert(18, "minimum_nights_right", app, True)

        app = new_df['latitude'].copy()
        new_df.rename(columns={'latitude': 'latitude_left'}, inplace=True)
        new_df.insert(18, "latitude_right", app, True)

        new_df = new_df[(new_df["host_id_left"] != new_df["host_id_right"])
                        | (new_df["latitude_left"] != new_df["latitude_right"])
                        | (new_df["longitude_left"] != new_df["longitude_right"])
                        | (new_df["bathrooms_left"] != new_df["bathrooms_right"])
                        | (new_df["bedrooms_left"] != new_df["bedrooms_right"])
                        | (new_df["beds_left"] != new_df["beds_right"])]

        # inserting the label column
        new_df.insert(1, "label", 0, True)

        # Projection
        new_df = new_df[column_names]

        return new_df

    def compute_false_dataset_with_same_last_review_and_calculated_host_listings_count_and_longitude(self, df):
        column_names = self.get_columns_ordered(df.columns)

        #Join
        new_df = pd.merge(df, df,
                          left_on=['last_review', 'calculated_host_listings_count', 'longitude'],
                          right_on=['last_review', 'calculated_host_listings_count', 'longitude'],
                          suffixes=('_left', '_right'))

        # Selection
        new_df = new_df[new_df["id_left"] != new_df["id_right"]]



        # Column renaming
        app = new_df['last_review'].copy()
        new_df.rename(columns={'last_review': 'last_review_left'}, inplace=True)
        new_df.insert(18, "last_review_right", app, True)

        app = new_df['calculated_host_listings_count'].copy()
        new_df.rename(columns={'calculated_host_listings_count': 'calculated_host_listings_count_left'}, inplace=True)
        new_df.insert(17, "calculated_host_listings_count_right", app, True)

        app = new_df['longitude'].copy()
        new_df.rename(columns={'longitude': 'longitude_left'}, inplace=True)
        new_df.insert(18, "longitude_right", app, True)

        new_df = new_df[(new_df["host_id_left"] != new_df["host_id_right"])
                        | (new_df["latitude_left"] != new_df["latitude_right"])
                        | (new_df["longitude_left"] != new_df["longitude_right"])
                        | (new_df["bathrooms_left"] != new_df["bathrooms_right"])
                        | (new_df["bedrooms_left"] != new_df["bedrooms_right"])
                        | (new_df["beds_left"] != new_df["beds_right"])]

        # inserting the label column
        new_df.insert(1, "label", 0, True)

        # Projection
        new_df = new_df[column_names]

        return new_df

    def compute_false_dataset_with_same_minimum_nights_and_availability_365_and_longitude(self, df):
        column_names = self.get_columns_ordered(df.columns)

        new_df = pd.merge(df, df,
                          left_on=['minimum_nights', 'availability_365', 'longitude'],
                          right_on=['minimum_nights', 'availability_365', 'longitude'],
                          suffixes=('_left', '_right'))

        # Selection
        new_df = new_df[new_df["id_left"] != new_df["id_right"]]


        # Column renaming
        app = new_df['minimum_nights'].copy()
        new_df.rename(columns={'minimum_nights': 'minimum_nights_left'}, inplace=True)
        new_df.insert(18, "minimum_nights_right", app, True)

        app = new_df['availability_365'].copy()
        new_df.rename(columns={'availability_365': 'availability_365_left'}, inplace=True)
        new_df.insert(17, "availability_365_right", app, True)

        app = new_df['longitude'].copy()
        new_df.rename(columns={'longitude': 'longitude_left'}, inplace=True)
        new_df.insert(18, "longitude_right", app, True)

        new_df = new_df[(new_df["host_id_left"] != new_df["host_id_right"])
                        | (new_df["latitude_left"] != new_df["latitude_right"])
                        | (new_df["longitude_left"] != new_df["longitude_right"])
                        | (new_df["bathrooms_left"] != new_df["bathrooms_right"])
                        | (new_df["bedrooms_left"] != new_df["bedrooms_right"])
                        | (new_df["beds_left"] != new_df["beds_right"])]

        # inserting the label column
        new_df.insert(1, "label", 0, True)

        # Projection
        new_df = new_df[column_names]

        return new_df

    """
    It splits the df into three different datasets:
    - train 60%
    - validation 20%
    - test 20%
    """
    def train_validate_test_split(self, df, train_percent=0.6, validate_percent=0.2):
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
    def split_and_save(self, df):
        train, validate, test = self.train_validate_test_split(df)
        train.to_csv(self.path + 'train.csv', index=False, header=True)
        validate.to_csv(self.path + 'validation.csv', index=False, header=True)
        test.to_csv(self.path + 'test.csv', index=False, header=True)

    """
    This is the main function of the etl pipeline
    The pipeline starts from here.
    path: e.g. Dataset/BAR/
    csv_url: the url of the csv file to download
    filename: the name of the file to be saved
    """

    def run_last_step(self):
        df = self.load_compressed()
        new_df = self.transform(df)
        self.split_and_save(new_df)
        self.print_statistics()

    """
       This is the main function of the etl pipeline
       The pipeline starts from here.
       path: e.g. Dataset/BAR/
       csv_url: the url of the csv file to download
       filename: the name of the file to be saved
    """

    def run(self):
        self.extract()
        self.prepare_ground_truth_from_compressed()
        df = self.load_compressed()
        new_df = self.transform(df)
        self.split_and_save(new_df)
        self.print_statistics()

    """
    This is the main function of the etl pipeline
    The pipeline starts from here.
    """

    def run_without_extraction(self):
        self.prepare_ground_truth_from_compressed()
        df = self.load_compressed()
        new_df = self.transform(df)
        self.split_and_save(new_df)
        self.print_statistics()

    """
    It prints out the statistics of the files created.
    The number of tuples labeled with 1 
    The number of tuples labeled with 0
    """
    def print_statistics(self):
        print("Training set size:")
        train = pd.read_csv(self.path + "train.csv")
        train_statistics = train.groupby(['label']).count()
        print(train_statistics)

        print("Validation set size:")
        valid = pd.read_csv(self.path + "validation.csv")
        valid_statistics = valid.groupby(['label']).count()
        print(valid_statistics)

        print("Test set size:")
        test = pd.read_csv(self.path + "test.csv")
        test_statistics = test.groupby(['label']).count()
        print(test_statistics)


