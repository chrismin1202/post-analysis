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
import os
from pathlib import Path

from tornado.options import define, options

from src.app import Application, \
    DEFAULT_OUTPUT_FORMAT, VALID_FILE_FORMATS, JSON_FORMAT, \
    DEFAULT_RUN_MODE, DEFAULT_JSON_RECORD_PER_LINE

_DEFAULT_BASE_PATH = Path(os.getcwd())
_DEFAULT_POSTS_CSV_PATH = _DEFAULT_BASE_PATH / "posts.csv"

define(
    "posts_file_path",
    default=str(_DEFAULT_POSTS_CSV_PATH),
    help="The fully qualified path to the input posts csv file")
define("output_dir_path", default=str(_DEFAULT_BASE_PATH), help="The fully qualified path to the output directory")
define(
    "output_file_format",
    default=DEFAULT_OUTPUT_FORMAT,
    help="The output file format. Supported types: {}".format(", ".join(VALID_FILE_FORMATS)))
define(
    "json_record_per_line",
    default=DEFAULT_JSON_RECORD_PER_LINE,
    type=bool,
    help="""The switch for indicating whether the output should be written as a single JSON array or
    each record as its own JSON entity per line. Note that this option is ignored if the output format is not {}.
    """.format(JSON_FORMAT))
define(
    "full_record",
    default=DEFAULT_RUN_MODE,
    type=bool,
    help="The switch for including the full record rather than just ids")
define("verbose", default=False, type=bool, help="Run in verbose mode, i.e., log stuff")

if __name__ == "__main__":
    options.parse_command_line()

    if options.help:
        options.print_help()
    else:
        logging.info(
            """Running the application with the following parameters:
                              Posts CSV Path:        %s
                              Output Directory Path: %s
                              Output File Format:    %s
                              Full Record:           %s
                              Verbose:               %s""",
            options.posts_file_path,
            options.output_dir_path,
            "{} (record per line: {})".format(
                options.output_file_format,
                options.json_record_per_line)
            if options.output_file_format == JSON_FORMAT
            else options.output_file_format,
            options.full_record,
            options.verbose)
        app = Application(verbose=options.verbose)
        app.run(
            options.posts_file_path,
            options.output_dir_path,
            output_format=options.output_file_format,
            include_full_record=options.full_record,
            json_record_per_line=options.json_record_per_line)
