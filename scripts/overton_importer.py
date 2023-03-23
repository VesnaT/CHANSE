"""
Overton importer.

- read .csv with Overton urls from disc, scrape .pdf at url and create an
  Orange Table with features:
  [Policy_document_id, Title, Translated title, Document type, Source title,
   Source country, Source state, Source type, Source subtype, Published_on,
   Policy citations, Policy citations (inc self), Document URL, Overton URL,
   Source specific tags, Your tags, Top topics, Content]

- save the table to data/overton/overton_{country}.pkl

- the output table is feed to {nn}_Collect_Overton_{lang}.ows to produce
  data/overton/Overton_{lang}_PP.pkl (preprocessed and filtered corpus), which
  is manually uploaded to the server

"""
import contextlib
import logging
import os
from tempfile import NamedTemporaryFile

import pandas as pd
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

from Orange.data import table_from_frame, Table
from scripts.utils import run_with_log

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "overton")


def _read_country(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # remove duplicated IDs
    df = df.groupby(df.columns[0]).first().reset_index()
    df["Content"] = ""
    for i in range(len(df)):
        # use for 'english' for temp save (just in case...)
        # stopped at 3676 of total 100000
        # if i % 100 == 0 and i > 0:
        #     df.to_pickle(f"overton_temp-en.pkl")
        #     table = _table_from_df(df)
        #     table.save(os.path.join(DATA_DIR, f"overton_en.pkl"))
        # print(i, "of total", len(df))
        df["Content"].iloc[i] = _read_url(df.iloc[i]["Document URL"])
    return df


def _read_url(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
                             "55.0.2883.95 Safari/537.36"}
    request = Request(url=url, headers=headers)
    try:
        # TODO - handle HTTP Error 403: Bad Behavior / Forbidden
        with contextlib.closing(urlopen(request)) as response:
            if url.endswith(".pdf") or url.endswith("/pdf/"):
                text = __read_pdf(response)
            else:
                text = __read_html(response)
    except Exception as ex:
        print(ex)
        print(url)
        print()
        logging.debug(url)
        logging.exception(ex)
        text = ""
    return text


def __read_html(response) -> str:
    reader = BeautifulSoup(response.read(), features="lxml")
    return reader.get_text()


def __read_pdf(response) -> str:
    with NamedTemporaryFile(delete=False) as f:
        f.write(response.read())
    reader = PdfReader(f.name)
    return " ".join([page.extract_text() for page in reader.pages])


def _table_from_df(df: pd.DataFrame) -> Table:
    df = df.reset_index()
    # remove documents where the content could not be read
    df = df[df["Content"] != ""]
    return table_from_frame(df)


@run_with_log(os.path.splitext(os.path.split(__file__)[1])[0])
def run():
    """
    Read Overton .csv from disc, crate Orange.data.Table and save it as .pkl.
    """
    countries = [# "slovenia", "belgium-dutch", "belgium-french",
                 # "sweden", "denmark", "finland",
                  "england"]
    for key in countries:
        print()
        print(key)
        path = f"/Users/vesna/Documents/Work/Biolab/CHANSE/Data/" \
               f"Overton/export-2023-03-07-{key}.csv"
        df = _read_country(path)
        df.to_pickle(f"overton_temp-{key}.pkl")
        table = _table_from_df(df)
        table.save(os.path.join(DATA_DIR, f"overton_{key}.pkl"))


if __name__ == "__main__":
    run()
