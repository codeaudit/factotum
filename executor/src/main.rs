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

#[macro_use]
extern crate log;
extern crate log4rs;
extern crate docopt;
extern crate chrono;
#[macro_use]
extern crate lazy_static;
extern crate regex;
extern crate url;
extern crate uuid;
extern crate threadpool;
extern crate iron;
#[macro_use(router)]
extern crate router;
extern crate bodyparser;
extern crate persistent;
extern crate logger;
extern crate rustc_serialize;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

use std::fs;
use docopt::Docopt;

mod executor;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

const IP_DEFAULT: &'static str = "0.0.0.0";
const PORT_DEFAULT: u32 = 3000;
const MAX_JOBS_DEFAULT: usize = 1000;
const MAX_WORKERS_DEFAULT: usize = 20;

const FACTOTUM: &'static str = "factotum";

const VALID_IP_REGEX: &'static str = r"\b(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\b";

const USAGE: &'static str =
    "
Executor.

Usage:
  executor --factotum-bin=<path> [--ip=<address> --port=<number> --max-jobs=<size> --max-workers=<size> --webhook=<url>] [--no-colour]
  executor (-h | --help)
  executor (-v | --version)

Options:
  -h --help                             Show this screen.
  -v --version                          Display the version of Executor and exit.
  --port=<number>                       Specify port number.
  --max-jobs=<size>                     Max size of job requests queue.
  --max-workers=<size>                  Max number of workers.
  --factotum-bin=<path>                 Path to Factotum binary file.
  --webhook=<url>                       Factotum arg to post updates on job execution to the specified URL.
  --no-colour                           Factotum arg to turn off ANSI terminal colours/formatting in output.

";

#[derive(Debug, RustcDecodable)]
pub struct Args {
    flag_ip: String,
    flag_port: u32,
    flag_version: bool,
    flag_factotum_bin: String,
    flag_max_jobs: usize,
    flag_max_workers: usize,
    flag_webhook: String,
    flag_no_colour: bool,
}

fn get_log_config() -> Result<log4rs::config::Config, log4rs::config::Errors> {
    use log::LogLevelFilter;
    use log4rs::append::file::FileAppender;
    use log4rs::encode::pattern::PatternEncoder;
    use log4rs::config::{Appender, Config, Root};
    
    let file_appender = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} {l} - {m}{n}")))
        .build(".factotum/server.log")
        .unwrap();

    let root = Root::builder()
        .appender("file")
        .build(LogLevelFilter::Info);

    Config::builder()
        .appender(Appender::builder().build("file", Box::new(file_appender)))
        .build(root)
}

fn init_logger() {
    fs::create_dir(".factotum").ok();
    let log_config = get_log_config().unwrap();
    log4rs::init_config(log_config).unwrap();
}

fn main() {
    init_logger();

    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());
    if args.flag_version {
        println!("Factotum server version {}", VERSION);
    } else {
        executor::start(args);
    }
}
