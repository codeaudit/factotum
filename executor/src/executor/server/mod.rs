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

use std::error;
use std::fmt;
use std::path::Path;
use chrono::{DateTime, UTC};
use uuid::Uuid;

use executor::commander;
use executor::commander::Commander;

#[cfg(test)]
mod tests;

const LOG_NAME: &'static str = "executor";
const LOG_KEY_STARTED: &'static str = "started";
const LOG_KEY_QUEUED: &'static str = "queued";
const LOG_KEY_WORKING: &'static str = "working";
const LOG_KEY_DONE: &'static str = "done";
pub const SERVER_STATE_RUN: &'static str = "run";
pub const SERVER_STATE_DRAIN: &'static str = "drain";

#[derive(Debug)]
pub struct ServerManager {
    pub ip: String,
    pub port: u32,
    pub state: String,
    pub start_time: DateTime<UTC>,
    pub webhook_uri: String,
    pub no_colour: bool,
}

impl ServerManager {
    pub fn new(wrapped_ip: Option<String>, port: u32, webhook_uri: String, no_colour: bool) -> ServerManager {
        ServerManager {
            ip: if let Some(ip) = wrapped_ip { ip } else { ::IP_DEFAULT.to_string() },
            port: if port > 0 && port <= 65535 { port } else { ::PORT_DEFAULT },
            state: SERVER_STATE_RUN.to_string(),
            start_time: UTC::now(),
            webhook_uri: webhook_uri.to_string(),
            no_colour: no_colour,
        }
    }

    pub fn is_running(&self) -> bool {
        self.state == SERVER_STATE_RUN
    }

    pub fn get_uptime(&self) -> i64 {
        let uptime = UTC::now().signed_duration_since(self.start_time);
        uptime.num_milliseconds()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobRequest {
    #[serde(skip_deserializing)]
    pub job_id: String,
    pub job_name: String,
    pub factfile_path: String,
    pub factfile_args: Vec<String>
}

impl JobRequest {
    pub fn new(job_id: String, job_name: String, factfile_path: String, factfile_args: Vec<String>) -> JobRequest {
        JobRequest {
            job_id: job_id,
            job_name: job_name,
            factfile_path: factfile_path,
            factfile_args: factfile_args
        }
    }

    pub fn validate(request: JobRequest, commander: &Commander) -> Result<JobRequest, ValidationError> {
        // check job name not empty
        // check factfile path not empty
        // check factfile args not empty
        if request.job_name == "" {
            let message = "No valid value found: field 'jobName' cannot be empty".to_string();
            error!("{}", message);
            return Err(ValidationError::no_output(message))
        } else if request.factfile_path == "" {
            let message = "No valid value found: field 'factfilePath' cannot be empty".to_string();
            error!("{}", message);
            return Err(ValidationError::no_output(message))
        }
        // check valid factfile path exists
        if !Path::new(&request.factfile_path).exists() {
            let message = format!("Value does not exist on host for 'factfilePath':'{}'", request.factfile_path);
            error!("{}", message);
            return Err(ValidationError::no_output(message))
        }
        // attempt dry run
        let cmd_path = try!(commander.get_command(::FACTOTUM));
        let mut cmd_args = vec!["run".to_string(), request.factfile_path.clone(), "--dry-run".to_string()];
        cmd_args.extend_from_slice(request.factfile_args.as_slice());
        match commander::execute(cmd_path, cmd_args) {
            Ok(_) => {
                debug!("Dry run success");
            },
            Err(error) => {
                error!("{}", error);
                return Err(ValidationError::no_output(error))
            }
        }
        // generate unique job id
        let mut request = request;
        request.job_id = generate_id();
        Ok(request)
    }

    pub fn append_job_args(server: &ServerManager, job: &mut JobRequest) {
        if server.webhook_uri != "" {
            job.factfile_args.push("--webhook".to_string());
            job.factfile_args.push(server.webhook_uri.clone());
        }
        if server.no_colour {
            job.factfile_args.push("--no-colour".to_string());
        }
    }
}

impl PartialEq for JobRequest {
    fn eq(&self, other: &JobRequest) -> bool {
        self.job_id == other.job_id
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsRequest {
    pub state: String
}

impl PartialEq for SettingsRequest {
    fn eq(&self, other: &SettingsRequest) -> bool {
        self.state == other.state
    }
}

impl SettingsRequest {
    pub fn new(state: String) -> SettingsRequest {
        SettingsRequest {
            state: state
        }
    }

    pub fn validate(request: SettingsRequest) -> Result<SettingsRequest, ValidationError> {
        match request.state.as_ref() {
            SERVER_STATE_RUN | SERVER_STATE_DRAIN => Ok(request),
            _ => Err(ValidationError::no_output(format!("Invalid 'state', must be one of ({}|{})", SERVER_STATE_RUN, SERVER_STATE_DRAIN)))
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ValidationError {
    pub error: String,
    pub stdout: String,
    pub stderr: String
}

impl ValidationError {
    pub fn new(error: String, stdout: String, stderr: String) -> ValidationError {
        ValidationError {
            error: error,
            stdout: stdout,
            stderr: stderr,
        }
    }

    pub fn no_output(error: String) -> ValidationError {
        ValidationError::new(error, String::new(), String::new())
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Validation Error: {}", self.error)
    }
}

impl error::Error for ValidationError {
    fn description(&self) -> &str {
        &self.error
    }
}

impl From<String> for ValidationError {
    fn from(err: String) -> ValidationError {
        ValidationError::no_output(err)
    }
}

fn generate_id() -> String {
    Uuid::new_v4().hyphenated().to_string()
}
