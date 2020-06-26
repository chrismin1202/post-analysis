<!---
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

## Sample CSV parsing with pandas

This repository contains a sample Python project that uses `pandas` to parse a CSV in a specific format.
Check the header line of [`posts.csv`](posts.csv) for the required columns.<br />

From the input CSV file, the following outputs are generated:
1. `top_posts.[csv|json]`<br />
   The posts that are public, have over 10 comments and over 9000 views, and have titles shorter than 40 characters.
1. `other_posts.[csv|json]`<br />
   The posts that do not meet the criteria of `top_posts.[csv|json]`.
1. `daily_top_posts.[csv|json]`<br />
   A subset of `top_posts.[csv|json]` comprises the top post of the day based on the number of likes.


### Structure

1. The driver Python script [`__main__.py`](__main__.py).
1. The source Python scripts in [`src`](src) directory.
1. A few unit test cases in [`test`](test) directory.
1. A [`requirements.txt`](requirements.txt) file containing a list of required packages.
1. An [`posts.csv`](posts.csv) file containing the input CSV.
1. An [`top_posts.csv`](top_posts.csv) file containing the sample top posts output as a CSV file.
1. An [`top_posts.json`](top_posts.json) file containing the sample top posts output as a JSON file.
1. An [`other_posts.csv`](other_posts.csv) file containing the sample other posts output as a CSV file.
1. An [`other_posts.json`](other_posts.json) file containing the sample other posts output as a JSON file.
1. An [`daily_top_posts.csv`](daily_top_posts.csv) file containing the sample daily top posts output as a CSV file.
1. An [`daily_top_posts.json`](daily_top_posts.json) file containing the sample daily top posts output as a JSON file.

### Dependencies

* `Tornado` for parsing command line argument
* `pandas` for parsing CSV

### How to run

1. Install the packages in `requirements.txt`.
    ```sh
    pip3 install -r requirements.txt --user
    ```
1. Run `__main__.py` script.
    ```sh
    python3 __main__.py
    ```
   Examples:
    * Run with `--help` switch to see available command line options.
      ```sh
      python3 __main__.py --help
      ```
    * To output full record as a JSON file with each record in its own line.
      ```sh
      python3 __main__.py \
        --output-file-format=json \
        --full-record \
        --json_record-per-line
      ```
1. To run the unit test cases,
   ```sh
   python3 -m unittest
   ```
