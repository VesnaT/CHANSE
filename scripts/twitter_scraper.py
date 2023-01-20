"""
Twitter scaper.
    Functions for collecting tweets:
    - The query searches for tweets that include at least one word from
      1. Algorithmic System Words column CHANSE_Twitter_Query_Jan_2023.xlsx.
      For English tweets if also considers column Value Words column.
    - The query does not search for retweets.
    - The scraper fetches approximately 16.000 tweets and saves them on a disk
      (16.000 * 31 < 500.000).

To run a script, provide a BEARER_TOKEN.
The TOKEN can fetch 500k tweets/month.

To make a cron job (daily):
which python3
pwd
crontab -e

* * * * * /Users/vesna/miniconda3/bin/python3 /Users/vesna/CHANSE/scripts/twitter_scraper.py
0 7 * * * /home/vesna/venv/bin/python3 /home/vesna/CHANSE/scripts/twitter_scraper.py
"""
import datetime
import logging
import os
from functools import partial
from itertools import product
from typing import List, Optional

import pandas as pd
from tweepy import Client, Paginator
from tweepy.tweet import Tweet

from utils import run_with_log

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "twitter")

BEARER_TOKEN: Optional[str] = NotImplemented


# helper functions to create KEYWORDS lists
def __get_keywords(words_column: str) -> List[str]:
    path = os.path.join(DATA_DIR, "..", "CHANSE_Query_SAMPLE_2.xlsx")
    df = pd.read_excel(path)
    return df[~pd.isna(df[words_column])][words_column].tolist()


def __get_keywords_1(words_column: str, n: int) -> List[str]:
    # words_column: Unnamed: 2, Unnamed: 5
    # n: 2, 1
    path = os.path.join(DATA_DIR, "..", "CHANSE_Twitter_Query_Jan_2023.xlsx")
    df = pd.read_excel(path)
    return df[~pd.isna(df[words_column])][words_column].tolist()[n:]


def __get_country_code(tweet: Tweet, _, places) -> Optional[str]:
    place_id = tweet.geo.get("place_id", None) if tweet.geo else None
    return places[place_id].country_code if place_id else ""


def __get_coordinates(tweet: Tweet, _, __, dim: int):
    coord = tweet.geo.get("coordinates", None) if tweet.geo else None
    return coord["coordinates"][dim] if coord else None


MAPPERS = [
    ("ID", lambda doc, _, __: doc.id),
    ("Content", lambda doc, _, __: doc.text),
    ("Author", lambda doc, users, _: "@" + users[doc.author_id].username),
    ("Date", lambda doc, _, __: doc.created_at.isoformat()),
    ("Language", lambda doc, _, __: doc.lang),
    ("Location", __get_country_code),
    ("Number of Likes", lambda doc, _, __: doc.public_metrics["like_count"]),
    ("Number of Retweets", lambda doc, _, __: doc.public_metrics["retweet_count"]),
    ("In Reply To", lambda doc, users, _: "@" + users[doc.in_reply_to_user_id].username if doc.in_reply_to_user_id and doc.in_reply_to_user_id in users else ""),
    ("Author Name", lambda doc, users, __: users[doc.author_id].name),
    ("Author Description", lambda doc, users, _: users[doc.author_id].description),
    ("Author Tweets Count", lambda doc, users, _: users[doc.author_id].public_metrics["tweet_count"]),
    ("Author Following Count", lambda doc, users, _: users[doc.author_id].public_metrics["following_count"]),
    ("Author Followers Count", lambda doc, users, _: users[doc.author_id].public_metrics["followers_count"]),
    ("Author Listed Count", lambda doc, users, _: users[doc.author_id].public_metrics["listed_count"]),
    ("Author Verified", lambda doc, users, _: str(users[doc.author_id].verified)),
    ("Longitude", partial(__get_coordinates, dim=0)),
    ("Latitude", partial(__get_coordinates, dim=1)),
]


def _fetch_reduced_english() -> pd.DataFrame:
    sys_keywords = [
        "algorithm", "artificial intelligence", "AI", "automated",
        "automation", "machine learning", "deep learning", "neural net",
        "smart cit", "facial recognition", "robot", "self-driving",
        "autonomous", "big data", "data driven"
    ]
    val_keywords = [
        "fair", "accountab", "transparent", "explainab", "compet",
        "sustainable", "ethic", "autonomy", "solidarity", "justice",
        "responsib", "trust", "care", "wellness", "guarantee", "identity",
        "safety", "black box", "bias", "concern"
    ]
    queries = _create_query(20, sys_keywords, val_keywords)
    assert len(queries) == 1
    query = queries[0]
    logging.info("Language: en")
    df = _fetch_batch(query, lang="en", limit=140)
    logging.info(f"# Tweets: {len(df)}")
    return df


def _fetch_other_countries() -> List[pd.DataFrame]:
    keywords = [
        ("da", 3, ["algoritme", "visualisering", "kunstig intelligens",
                   "billedgenkendelse", "ansigtsgenkendelse"]),
        ("fi", 3, ["automaatti", "algoritmi", "tekoäly", "koneoppimi",
                   "syväoppimi"]),
        ("nl", 13, ["intelligence artificielle ", "automatisation",
                    "décision automatisée", "apprentissage automatique",
                    "apprentissage profond"]),
        ("sv", 1, ["maskininlärning", "automatisering", "beslutsstöd"]),
        ("sl", 1, ["Umetna inteligenca", "Algoritem", "Avtomatizacija",
                   "Strojno učenje"]),
    ]
    dfs = []
    for lng, limit, words in keywords:
        logging.info(f"Language: {lng}")
        query = " OR ".join(words)
        query = f"({query})"
        query += f" lang:{lng}" if lng != "nl" else f" (lang:{lng} OR lang:fr)"
        df = _fetch_batch(query, lang=None, limit=limit)
        logging.info(f"# Tweets: {len(df)}")
        dfs.append(df)
    return dfs


def _create_query(
        n: int,
        sys_keywords: List[str],
        val_keywords: List[str],
) -> str:
    assert n > 1

    def _expand(kwds):
        r = len(kwds) % n
        return kwds if r == 0 else kwds + [kwds[0]] * (n - r)

    sys_keywords = _expand(sys_keywords)  # to zip properly
    it = [iter(sys_keywords)] * n
    sys_combs = ["(" + " OR ".join(t) + ")" for t in zip(*it)]
    val_keywords = _expand(val_keywords)  # to zip properly
    it = [iter(val_keywords)] * n
    val_combs = ["(" + " OR ".join(t) + ")" for t in zip(*it)]
    sys_replace = f" OR {sys_keywords[0]}"
    val_replace = f" OR {val_keywords[0]}"
    return [f"{x.replace(sys_replace, '')} {y.replace(val_replace, '')}"
            for x, y in product(sys_combs, val_combs)]


def _fetch_batch(
        query: str,
        lang: Optional[str] = None,
        allow_retweets: bool = False,
        limit: int = 450,
) -> pd.DataFrame:
    assert len(query) <= 512
    client = Client(BEARER_TOKEN)

    if not allow_retweets:
        query += " -is:retweet"
    if lang:
        query += f" lang:{lang}"

    logging.info(query)

    paginator = Paginator(
        client.search_recent_tweets,
        query,
        max_results=100,
        tweet_fields=["lang", "public_metrics", "in_reply_to_user_id",
                      "author_id", "geo", "created_at"],
        user_fields=["description", "public_metrics", "verified"],
        place_fields=["country_code"],
        expansions=["author_id", "in_reply_to_user_id", "geo.place_id"],
        start_time=datetime.datetime.now() - datetime.timedelta(hours=24),
        limit=limit
    )

    series = []
    for i, response in enumerate(paginator):
        users = {u.id: u for u in response.includes.get("users", [])}
        places = {p.id: p for p in response.includes.get("places", [])}
        for tweet in response.data or []:
            ser = pd.Series(
                [func(tweet, users, places) for _, func in MAPPERS],
                [name for name, _ in MAPPERS]
            )
            series.append(ser)
    return pd.DataFrame(series)


def _save(df: pd.DataFrame):
    file_name = f"{datetime.datetime.now().strftime('%Y%m%d')}.pkl"
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    path = os.path.join(DATA_DIR, file_name)
    df.to_pickle(path)


@run_with_log(os.path.splitext(os.path.split(__file__)[1])[0])
def run():
    """
    Collect approximately 450.000 tweets and save them as a pd.DataFrame
    """
    dfs = _fetch_other_countries()
    df_en = _fetch_reduced_english()
    dfs.append(df_en)
    df = pd.concat(dfs)
    _save(df)


if __name__ == "__main__":
    run()
