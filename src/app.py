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

import logging
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd

_CSV_FORMAT = "csv"
JSON_FORMAT = "json"
DEFAULT_OUTPUT_FORMAT = _CSV_FORMAT
VALID_FILE_FORMATS = frozenset([DEFAULT_OUTPUT_FORMAT, JSON_FORMAT])
DEFAULT_JSON_RECORD_PER_LINE = True
DEFAULT_RUN_MODE = False  # ids only

_TOP_POSTS_FILE_NAME = "top_posts"
_OTHER_POSTS_FILE_NAME = "other_posts"
_DAILY_TOP_POSTS_FILE_NAME = "daily_top_posts"

C_ID = "id"
C_TITLE = "title"
C_PRIVACY = "privacy"
C_LIKES = "likes"
C_VIEWS = "views"
C_COMMENTS = "comments"
C_TIMESTAMP = "timestamp"
# Note that np.str is mapped to pandas type object
_VALID_COLUMNS = frozenset([
    (C_ID, np.int),
    (C_TITLE, np.object),
    (C_PRIVACY, np.object),
    (C_LIKES, np.int),
    (C_VIEWS, np.int),
    (C_COMMENTS, np.int),
    (C_TIMESTAMP, np.object),
])

MIN_NUM_COMMENTS = 10
MIN_NUM_VIEWS = 9000
MAX_NUM_TITLE_CHARS = 40
PRIVACY_TYPE = "public"


def _format_path(base_path: Path, file_name: str, file_format: str) -> str:
    return str(base_path / "{}.{}".format(file_name, file_format))


class MissingColumnException(Exception):
    pass


class UnsupportedColumnTypeException(Exception):
    pass


class Application:
    """The main class for the post analysis logic."""

    def __init__(self, verbose: bool = False):
        """Constructs a new instance of :class:`.Application`.

        :param verbose: Runs the application in verbose mode, i.e., lots of logging.
        """
        self.verbose = verbose

    def run(
            self,
            posts_path: Union[Path, str],
            output_dir_path: Union[Path, str],
            output_format: str = DEFAULT_OUTPUT_FORMAT,
            include_full_record: bool = DEFAULT_RUN_MODE,
            json_record_per_line: bool = DEFAULT_JSON_RECORD_PER_LINE) -> None:
        """Runs the application.

        The steps are:
          1. Load the posts from the given input path
          2. Analyze the posts into 3 pandas DataFrames
          3. Output the results of the analysis to the given output path.

        The outputs are top_posts, other_posts, and daily_top_posts. The format can be either CSV or JSON.
        The output can be either just the ids of the posts or full records.

        The criteria of the top posts are:
          1. The post are public
          2. The post have over 10 comments and over 9000 views
          3. The post title have under 40 characters

        The top_posts file will contain the records that match the aforementioned criteria,
        whereas the other_posts file will contain the rest.
        The daily_top_posts file will contain a subset of top_posts that have the top post for each day
        based on the number of likes.

        :param posts_path: The fully qualified path the posts CSV file.
        :param output_dir_path: The path to which the output files are written.
        :param output_format: The format of the output files.
        :param include_full_record: The boolean for indicating whether the full record should be included in the output.
        :param json_record_per_line: The boolean for indicating whether the records should be outputted
                                     as a single JSON array or each record should be outputted as its own JSON object
                                     to each line. This flag is ignored if the output format is not JSON.
        """
        df = self.load_posts(posts_path)

        top_posts_df, other_posts_df, daily_top_posts_df = self.analyze_posts(df)

        output_dir = output_dir_path if isinstance(output_dir_path, Path) else Path(output_dir_path)
        self.__write_df(
            top_posts_df,
            output_dir,
            _TOP_POSTS_FILE_NAME,
            output_format,
            include_full_record,
            json_record_per_line)
        self.__write_df(
            other_posts_df,
            output_dir,
            _OTHER_POSTS_FILE_NAME,
            output_format,
            include_full_record,
            json_record_per_line)
        self.__write_df(
            daily_top_posts_df,
            output_dir,
            _DAILY_TOP_POSTS_FILE_NAME,
            output_format,
            include_full_record,
            json_record_per_line)

    def analyze_posts(self, posts_df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """Analyzes the given DataFrame and produces 3 DataFrames.

        The output DataFrames are top_posts, other_posts, and daily_top_posts.

        The criteria of the top posts are:
          1. The post are public
          2. The post have over 10 comments and over 9000 views
          3. The post title have under 40 characters

        The top_posts DataFrame will contain the records that match the aforementioned criteria,
        whereas the other_posts DataFrame will contain the rest.
        The daily_top_posts DataFrame will contain a subset of top_posts DataFrame that have the top post for each day
        based on the number of likes.

        :param posts_df: The input DataFrame to analyze.
        :return: A tuple that comprises 3 DataFrames (top_posts, other_posts, daily_top_posts).
        """
        self.validate_schema(posts_df)
        condition = (posts_df[C_PRIVACY] == PRIVACY_TYPE) & \
                    (posts_df[C_COMMENTS] > MIN_NUM_COMMENTS) & \
                    (posts_df[C_VIEWS] > MIN_NUM_VIEWS) & \
                    (posts_df[C_TITLE].str.len() < MAX_NUM_TITLE_CHARS)

        top_posts_df = posts_df.loc[condition]
        other_posts_df = posts_df.loc[~condition]
        daily_top_posts_df = self.__collect_daily_top_posts(top_posts_df)
        return top_posts_df, other_posts_df, daily_top_posts_df

    def __collect_daily_top_posts(self, df: pd.DataFrame) -> pd.DataFrame:
        self.__log(logging.INFO, "Collecting top posts for each day...")
        groupby_timestamp = (df
                             .reset_index()
                             .groupby([pd.to_datetime(df[C_TIMESTAMP]).apply(lambda x: x.date())]))
        return df.loc[groupby_timestamp[C_LIKES].idxmax()]

    def load_posts(self, posts_path: Union[Path, str]) -> pd.DataFrame:
        path = str(posts_path) if isinstance(posts_path, Path) else posts_path
        self.__log(logging.INFO, "Loading the posts from {}".format(path))
        return pd.read_csv(str(posts_path))

    def validate_schema(self, df: pd.DataFrame) -> None:
        """Validates the schema of the given DataFrame.

        Checks to make sure that the required columns exist in the DataFrame.

        :param df: The DataFrame to validate.
        :raise MissingColumnException: Thrown when one or more required columns is missing.
        :raise UnsupportedColumnTypeException: Thrown if one or more required columns have unexpected data type.l
        """
        self.__log(logging.INFO, "Validating the schema of the DataFrame...")
        columns = df.columns
        for name, dtype in _VALID_COLUMNS:
            if name not in columns:
                raise MissingColumnException("The column '{}' is missing!".format(name))
            if df[name].dtype != dtype:
                raise UnsupportedColumnTypeException(
                    "The expected type for the column '{}' is {}, but it was {}!".format(name, dtype, df[name].dtype))

    def __write_df(
            self,
            df: pd.DataFrame,
            output_dir_path: Path,
            output_file_name: str,
            output_format: str,
            include_full_record: bool,
            json_record_per_line: bool) -> None:
        if not include_full_record:
            self.__log(logging.INFO, "Outputting '{}' column only...".format(C_ID))
            df = df[C_ID]
        self.__log(logging.INFO, "Output format: {}".format(output_format))
        output_path = _format_path(output_dir_path, output_file_name, output_format)
        self.__log(logging.INFO, "Output path: {}".format(output_path))
        if output_format.casefold() == DEFAULT_OUTPUT_FORMAT:
            df.to_csv(output_path, index=False)
        else:
            if json_record_per_line:
                self.__log(logging.INFO, "Each record will be written as a JSON entity in each line.")
            else:
                self.__log(logging.INFO, "The records will be written as a JSON array.")
            df.to_json(output_path, orient="records", lines=json_record_per_line)

    def __log(self, level: int, msg: str) -> None:
        """Logs the message with the given logging level if in verbose mode.

        :param level: the logging level.
        :param msg: the message to log.
        """
        if self.verbose:
            logging.log(level, msg)
