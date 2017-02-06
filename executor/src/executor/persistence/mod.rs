// Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Apache License Version 2.0, and
// you may not use this file except in compliance with the Apache License
// Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
// http://www.apache.org/licenses/LICENSE-2.0.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Apache License Version 2.0 is distributed on an "AS
// IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the Apache License Version 2.0 for the specific language
// governing permissions and limitations there under.
//

use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::os::unix;
use std::path::Path;

use executor::server::JobRequest;

const LOG_SUFFIX: &'static str = ".log";
const DELIMITER: &'static str = ":";
// const LINE_SIZE_LIMIT: i32 = 10 * 1024 * 1024;

#[derive(Debug)]
pub struct Persistence {
    path: String
}

impl Persistence {
    pub fn new(directory: &str, filename: &str) -> Persistence {
        fs::create_dir(directory).unwrap_or_else(|why| {
            println!("! {:?}", why.kind());
        });

        Persistence {
            path: format!("{}/{}{}", directory, filename, LOG_SUFFIX)
        }
    }

    pub fn has_run(&self, job_request: &mut JobRequest) -> bool {
        false
    }
}
