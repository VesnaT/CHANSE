"""
Twitter scaper.
    Functions for collecting tweets:
    - The query will search for tweets that include at least one word from
      1. Algorithmic System Words column and one word form 2. Value Words
      column of CHANSE_Query_SAMPLE_2.xlsx file.
    - The query will also search for retweets.
    - The query will search only for English tweets.
      For keywords change the language.
    - The scraper will fetch approximately 450.000 tweets
      and save them on a disk.
    - The query will be executed every Thursday of the week.

To run a script, provide a BEARER_TOKEN.

To make a cron job:
which python3
pwd
crontab -e

0 0 * * 4 /Users/vesna/miniconda3/bin/python /Users/vesna/CHANSE/scripts/twitter_scraper.py
"""
import datetime
import os
from functools import partial
from itertools import product
from typing import List, Optional

import pandas as pd
from tweepy import Client, Paginator
from tweepy.tweet import Tweet

from scripts import run_with_log

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "twitter")

BEARER_TOKEN: Optional[str] = NotImplemented

SYSTEM_KEYWORDS = [
    "algorithm", "artificial intelligence", "AI", "automated", "automation",
    "machine intelligence", "machine learning", "deep learning ",
    "neuro network", "neural network", "neural net", "smart cit",
    "visualisation", "visualization", "image recognition",
    "facial recognition", "recommender system", "bot", "robot",
    "self-driving", "big data", "data driven", "bayesian", "artificial neur",
    "NLP", "natural language processing", "computer vision", "digitalisation",
    "digitalization", "information model", "sensor", "decision support",
    "calculation", "smart", "electrification", "app", "targeted"
]
VALUE_KEYWORDS = [
    "fair", "transparent", "responsib", "competit", "sustainable",
    "explainab", "ethic", "autonomy", "human", "solidarity", "just",
    "benevolence", "responsib", "trust", "care", "wellness", "effective",
    "robust", "efficien", "optimisation", "optimization", "excellen",
    "quality", "precision", "savings", "guarantee", "supplement", "workload",
    "speed", "distribut", "risk", "identity", "security", "safety", "flow",
    "seamlessness", "control", "standard"
]


def __get_keywords(words_column: str) -> List[str]:
    path = os.path.join(DATA_DIR, "..", "CHANSE_Query_SAMPLE_2.xlsx")
    df = pd.read_excel(path)
    return df[~pd.isna(df[words_column])][words_column].tolist()


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


def _fetch(
        sys_keywords: List[str],
        val_keywords: List[str],
        lang: str = None,
) -> pd.DataFrame:
    queries = _create_query(13, sys_keywords, val_keywords)
    limit = 450 // len(queries)
    dfs = [_fetch_batch(query, lang=lang, limit=limit) for query in queries]
    return pd.concat(dfs)


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
    client = Client(BEARER_TOKEN)

    if not allow_retweets:
        query += " -is:retweet"
    if lang:
        query += f" lang:{lang}"

    paginator = Paginator(
        client.search_recent_tweets,
        query,
        max_results=100,
        tweet_fields=["lang", "public_metrics", "in_reply_to_user_id",
                      "author_id", "geo", "created_at"],
        user_fields=["description", "public_metrics", "verified"],
        place_fields=["country_code"],
        expansions=["author_id", "in_reply_to_user_id", "geo.place_id"],
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
    df = _fetch(SYSTEM_KEYWORDS, VALUE_KEYWORDS, lang="en")
    _save(df)


if __name__ == "__main__":
    run()
