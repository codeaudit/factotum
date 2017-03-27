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

use std::error::Error;
use std::ops::Deref;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use iron::mime::*;
use iron::prelude::*;
use iron::status;
use iron::status::Status;
use bodyparser;
use persistent::{Read, State};
use serde::Serialize;
use serde_json;

use executor::{Paths, Server, Storage, Updates};
use executor::server::{ServerManager, SettingsRequest, JobRequest};
use executor::dispatcher::{Dispatch, Query};

const JSON_CONTENT_TYPE: &'static str = "application/json; charset=UTF-8";

// JSON Response Structs

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ResponseMessage {
    message: String
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FactotumServerStatus {
    version: VersionStatus,
    server: ServerStatus,
    dispatcher: DispatcherStatus,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionStatus {
    executor: String
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ServerStatus {
    start_time: String,
    up_time: i64,
    state: String
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DispatcherStatus {
    pub workers: WorkerStatus,
    pub jobs: JobStatus,
}

#[derive(Debug,PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatus {
    pub total: usize,
    pub idle: usize,
    pub active: usize,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    pub max_queue_size: usize,
    pub in_queue: usize,
    pub fail_count: usize,
    pub success_count: usize,
}

// Response handlers

pub fn api(_: &mut Request) -> IronResult<Response> {
    let response = get_help_message();
    return_json(status::Ok, response)
}

pub fn status(request: &mut Request) -> IronResult<Response> {
    let rwlock = request.get::<State<Server>>().unwrap();
    let reader = rwlock.read().unwrap();
    let server_manager = reader.deref();
    let mutex = request.get::<Read<Updates>>().unwrap();
    let jobs_channel = mutex.try_lock().unwrap();
    let response = get_server_status(server_manager, jobs_channel.clone());
    return_json(status::Ok, response)
}

pub fn settings(request: &mut Request) -> IronResult<Response> {
    let rwlock = request.get::<State<Server>>().unwrap();
    let mut server = rwlock.write().unwrap();
    
    // get body
    let settings = match request.get::<bodyparser::Struct<SettingsRequest>>() {
        Ok(Some(decoded_settings)) => decoded_settings,
        Ok(None) => {
            return return_json(status::BadRequest, create_warn_response("Error: No body found in POST request"))
        },
        Err(e) => {
            return return_json(status::BadRequest, create_warn_response(&format!("Error decoding JSON string: {}", e.cause().unwrap())))
        }
    };

    // validate settings request
    let validated_settings = match SettingsRequest::validate(settings) {
        Ok(validated_settings) => validated_settings,
        Err(e) => {
            return return_json(status::BadRequest, create_warn_response(&format!("{}", e)))
        }
    };

    // update server state
    server.state = validated_settings.state.to_string();
    return_json(status::Ok, create_ok_response(&format!("Update acknowledged: [state: {}]", server.state)))
}

pub fn submit(request: &mut Request) -> IronResult<Response> {
    let server_rwlock = request.get::<State<Server>>().unwrap();
    let server = server_rwlock.write().unwrap();
    let storage_rwlock = request.get::<State<Storage>>().unwrap();
    let persistence = storage_rwlock.write().unwrap();
    let commander_rwlock = request.get::<Read<Paths>>().unwrap();
    let commander = commander_rwlock.read().unwrap();
    let sender_mutex = request.get::<Read<Updates>>().unwrap();
    let jobs_channel = sender_mutex.try_lock().unwrap();

    // get body
    let job_request = match request.get::<bodyparser::Struct<JobRequest>>() {
        Ok(Some(decoded_job_request)) => decoded_job_request,
        Ok(None) => {
            return return_json(status::BadRequest, create_warn_response("Error: No body found in POST request"))
        },
        Err(e) => {
            return return_json(status::BadRequest, create_warn_response(&format!("Error decoding JSON string: {}", e.cause().unwrap())))
        }
    };

    // check state
    if !server.is_running() {
        return return_json(status::BadRequest, create_warn_response(&format!("Server in [{}] state - cannot submit job", server.state)))
    }

    // validate job request
    let mut validated_job_request = match JobRequest::validate(job_request, &commander) {
        Ok(validated_job_request) => validated_job_request,
        Err(e) => {
            return return_json(status::BadRequest, create_warn_response(&format!("{}", e)))
        }
    };

    // check if job has been run before
    if persistence.has_run(&mut validated_job_request) {
        return return_json(status::BadRequest, create_warn_response("Job has already been run"))
    }

    // check queue size
    if is_requests_queue_full(jobs_channel.clone()) {
        return return_json(status::BadRequest, create_warn_response("Queue is full, cannot add job"))
    }

    // append args
    JobRequest::append_job_args(&server.deref(), &mut validated_job_request);
    let job_id = validated_job_request.job_id.clone();
    jobs_channel.send(Dispatch::NewRequest(validated_job_request)).expect("Job requests channel receiver has been deallocated");
    return_json(status::Ok, create_ok_response(&format!("JOB SUBMITTED  jobId:[{}]", job_id)))
}

pub fn check(request: &mut Request) -> IronResult<Response> {
    let message = ResponseMessage { message: "check".to_string() };
    let response = encode_compact(&message);
    return_json(status::Ok, response)
}

// Helpers

fn get_help_message() -> String {
    let message = json!(
        {
            "/help": {
                "function": "Returns this message!",
                "params": "pretty=1"
            },
            "/status": {
                "function": "Returns general information about the server and host system.",
                "params": "pretty=1"
            },
            "/settings": {
                "function": "Updates settings within the server.",
                "body": {
                    "state": "run|drain"
                },
                "params": "pretty=1"
            },
            "/submit": {
                "function": "Submits a job to the queue.",
                "body": {
                    "jobName": "com.acme-main",
                    "factfilePath": "/com.acme-main/factfile",
                    "factfileArgs": "[ --start step-2 ]"
                },
                "params": "pretty=1"
            },
            "/check": {
                "function": "Fetches the state of a job by the ID.",
                "params": "pretty=1, id=[id string]"
            }
        }
    );
    encode_pretty(&message)
}

fn get_server_status(server: &ServerManager, jobs_channel: Sender<Dispatch>) -> String {
    let (tx, rx) = mpsc::channel();
    jobs_channel.send(Dispatch::StatusUpdate(Query::new("status_query".to_string(), tx))).expect("Job requests channel receiver has been deallocated");
    let dispatcher_status = rx.recv().expect("Server status senders have been disconnected");

    let message = FactotumServerStatus {
        version: VersionStatus {
            executor: ::VERSION.to_string()
        },
        server: ServerStatus {
            start_time: server.start_time.to_rfc3339(),
            up_time: server.get_uptime(),
            state: server.state.to_string()
        },
        dispatcher: dispatcher_status,
    };
    encode_pretty(&message)
}

fn is_requests_queue_full(jobs_channel: Sender<Dispatch>) -> bool {
    let (tx, rx) = mpsc::channel();
    jobs_channel.send(Dispatch::CheckQueue(Query::new("queue_query".to_string(), tx))).expect("Job requests channel receiver has been deallocated");
    rx.recv().expect("Queue query senders have been disconnected")
}

fn encode_compact<T: Serialize>(message: T) -> String {
    serde_json::to_string(&message).expect("JSON compact encode error")
}

fn encode_pretty<T: Serialize>(message: T) -> String {
    serde_json::to_string_pretty(&message).expect("JSON pretty encode error")
}

fn create_response(message: &str) -> String {
    let response = ResponseMessage { message: message.to_string() };
    encode_compact(&response)
}

fn create_ok_response(message: &str) -> String {
    info!("{}", message);
    create_response(message)
}

fn create_warn_response(message: &str) -> String {
    warn!("{}", message);
    create_response(message)
}

fn return_json(code: Status, response: String) -> IronResult<Response> {
    let content_type = JSON_CONTENT_TYPE.parse::<Mime>().unwrap();
    Ok(Response::with((content_type, code, response)))
}
