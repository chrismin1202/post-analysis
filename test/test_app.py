#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
from pathlib import Path
from typing import Optional

import pandas as pd

from src.app import Application, MissingColumnException, UnsupportedColumnTypeException, \
    C_ID, C_TITLE, C_PRIVACY, C_LIKES, C_VIEWS, C_COMMENTS, C_TIMESTAMP, \
    PRIVACY_TYPE, MIN_NUM_COMMENTS, MIN_NUM_VIEWS, MAX_NUM_TITLE_CHARS

_TEST_SAMPLE_PATH = Path(__file__).parent / "resources" / "posts_test_samples.csv"


def _load_posts(app: Optional[Application] = None) -> pd.DataFrame:
    if not app:
        app = Application(verbose=True)
    return app.load_posts(_TEST_SAMPLE_PATH)


def _count_rows(df: pd.DataFrame) -> int:
    return len(df.index)


class TestApp(unittest.TestCase):

    def test_validating_schema_missing_column(self):
        app = Application(verbose=True)
        data = {C_ID: [0, 1], C_TITLE: ["title 1", "title 2"]}
        df = pd.DataFrame.from_dict(data)
        with self.assertRaises(MissingColumnException):
            app.validate_schema(df)

    def test_validating_schema_unexpected_column_type(self):
        app = Application(verbose=True)
        data = {
            C_ID: [0],
            C_TITLE: ["title"],
            C_PRIVACY: [False],  # privacy column is expected to be str (object) column
            C_LIKES: [1],
            C_VIEWS: [1],
            C_COMMENTS: [1],
            C_TIMESTAMP: ["Wed Oct 16 23:42:44 2015"],
        }
        df = pd.DataFrame.from_dict(data)
        with self.assertRaises(UnsupportedColumnTypeException):
            app.validate_schema(df)

    def test_analyzing_posts(self):
        app = Application(verbose=True)
        df = _load_posts(app=app)

        top_posts_df, other_posts_df, daily_top_posts_df = app.analyze_posts(df)

        # Check to make sure that all rows in top_posts_df meet the criteria
        self.__assert_row_count(top_posts_df, 0, condition=top_posts_df[C_PRIVACY] != PRIVACY_TYPE)
        self.__assert_row_count(top_posts_df, 0, condition=top_posts_df[C_COMMENTS] < MIN_NUM_COMMENTS)
        self.__assert_row_count(top_posts_df, 0, condition=top_posts_df[C_VIEWS] < MIN_NUM_VIEWS)
        self.__assert_row_count(top_posts_df, 0, condition=top_posts_df[C_TITLE].str.len() > MAX_NUM_TITLE_CHARS)

        # There should not be any match between top_posts_df and other_posts_df
        top_other_joined_df = top_posts_df.merge(other_posts_df, on="id")
        self.__assert_row_count(top_other_joined_df, 0)

        # Check if daily_top_posts_df is a subset of top_posts_df
        daily_top_joined_df = daily_top_posts_df.merge(top_posts_df, on="id")
        self.assertEqual(_count_rows(daily_top_joined_df), _count_rows(daily_top_posts_df))

        # The ids in daily_top_posts_df and daily_top_joined_df should match exactly
        actual_ids = daily_top_posts_df["id"].values.tolist()
        actual_ids.sort()
        expected_ids = daily_top_joined_df["id"].values.tolist()
        expected_ids.sort()
        self.assertListEqual(expected_ids, actual_ids)

    def __assert_row_count(self, df: pd.DataFrame, expected_count: int, condition=None) -> None:
        if condition is not None:
            df = df.loc[condition]
        self.assertEqual(_count_rows(df), expected_count)


if __name__ == '__main__':
    unittest.main()
