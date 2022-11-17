"""
Twitter importer.

- read Tweets from disc and create an Orange Table with features:
  [Content, Author, Date, Language, Location, Number of Likes,
  Number of Retweets, In Reply To, Author Name, Author Description,
  Author Tweets Count, Author Following Count, Author Followers Count,
  Author Listed Count, Author Verified, Longitude, Latitude]

- save the table to tweets.pkl

"""
import logging
import os
from typing import Tuple

import pandas as pd

from Orange.data import table_from_frame, Domain, Table, ContinuousVariable, \
    StringVariable, DiscreteVariable, TimeVariable

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "twitter")

MAPPERS = [
    ("Content", StringVariable),
    ("Author", DiscreteVariable),
    ("Date", TimeVariable),
    ("Language", DiscreteVariable),
    ("Location", DiscreteVariable),
    ("Number of Likes", ContinuousVariable),
    ("Number of Retweets", ContinuousVariable),
    ("In Reply To", DiscreteVariable),
    ("Author Name", DiscreteVariable),
    ("Author Description", StringVariable),
    ("Author Tweets Count", ContinuousVariable),
    ("Author Following Count", ContinuousVariable),
    ("Author Followers Count", ContinuousVariable),
    ("Author Listed Count", ContinuousVariable),
    ("Author Verified", DiscreteVariable),
    ("Longitude", ContinuousVariable),
    ("Latitude", ContinuousVariable),
]


def _read() -> Tuple[pd.DataFrame, str, str]:
    dfs = []
    file_names = []
    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith(".pkl"):
            path = os.path.join(DATA_DIR, file_name)
            df = pd.read_pickle(path)
            if isinstance(df, pd.DataFrame):
                file_names.append(os.path.splitext(file_name)[0])
                dfs.append(df)
    return pd.concat(dfs), min(file_names), max(file_names)


def _table_from_df(df: pd.DataFrame) -> Table:
    columns = [mapper[0] for mapper in MAPPERS]
    df = df.reset_index()
    df = df[columns]

    float_columns = ["Longitude", "Latitude"]
    df[float_columns] = df[float_columns].astype(float)

    table = table_from_frame(df)
    metas = [table.domain[col] for col in columns]
    domain = Domain([], metas=metas)
    table = table.transform(domain)
    for (col_name, cls), var in zip(MAPPERS, table.domain.metas):
        assert var.name == col_name
        if not isinstance(var, cls):
            logging.debug(f"{var.name} is {type(var)} instead of {cls}.")

    assert len(table.domain.attributes) == 0
    assert len(table.domain.class_vars) == 0
    assert len(table.domain.metas) == 17

    return table


def run():
    """
    Read Tweets from disc, crate Orange.data.Table and save it as .pkl.
    Name of the table file consist of first and last saved dataframe.
    """
    df, fn1, fn2 = _read()
    df = df.groupby("ID").first()
    table = _table_from_df(df)
    table.save(os.path.join(DATA_DIR, f"tweets_{fn1}_{fn2}.pkl"))


if __name__ == "__main__":
    dir_name, log_file_name = os.path.split(__file__)
    log_file_name, _ = os.path.splitext(log_file_name)
    log_path = os.path.join(dir_name, f"{log_file_name}.log")
    logging.basicConfig(level=logging.DEBUG,
                        filename=log_path,
                        filemode="a",
                        format="%(asctime)s %(levelname)-8s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    try:
        logging.info("Started")
        run()
        logging.info("Finished")
    except Exception as ex:
        logging.exception(ex)
        raise ex
