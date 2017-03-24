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

use super::*;
use std::error::Error;

#[test]
fn create_new_server_manager() {
    let server_manager = ServerManager::new("0.0.0.0".to_string(), 8080, "http://a.webhook.com/".to_string(), true);

    assert_eq!(server_manager.ip, "0.0.0.0");
    assert_eq!(server_manager.port, 8080);
    assert_eq!(server_manager.state, SERVER_STATE_RUN);
    assert_eq!(server_manager.webhook_uri, "http://a.webhook.com/");
    assert!(server_manager.no_colour);
}

#[test]
fn server_manager_is_running() {
    let server_manager = ServerManager::new("0.0.0.0".to_string(), 8080, "http://dummy.test/".to_string(), false);
    assert!(server_manager.is_running());
}

#[test]
fn server_manager_is_not_running() {
    let mut server_manager = ServerManager::new("0.0.0.0".to_string(), 8080, "http://dummy.test/".to_string(), false);
    server_manager.state = SERVER_STATE_DRAIN.to_string();
    assert_eq!(server_manager.is_running(), false);
}

#[test]
fn job_request_empty_job_name() {
    let job_request = JobRequest::new("1".to_string(), String::new(), "/tmp/somewhere".to_string(), vec![]);
    let commander = commands![::FACTOTUM.to_string() => "/tmp/fake_path".to_string()];
    let validation_error = JobRequest::validate(job_request.clone(), &commander).unwrap_err();
    assert_eq!(validation_error, ValidationError::no_output("No valid value found for 'jobName'".to_string()));
}

#[test]
fn job_request_empty_factfile_path() {
    let job_request = JobRequest::new("1".to_string(), "dummy".to_string(), String::new(), vec![]);
    let commander = commands![::FACTOTUM.to_string() => "/tmp/fake_path".to_string()];
    let validation_error = JobRequest::validate(job_request.clone(), &commander).unwrap_err();
    assert_eq!(validation_error, ValidationError::no_output("No valid value found for 'factfilePath'".to_string()));
}

#[test]
fn job_request_invalid_factfile_path() {
    let job_request = JobRequest::new("1".to_string(), "dummy".to_string(), "/tmp/somewhere".to_string(), vec![]);
    let commander = commands![::FACTOTUM.to_string() => "/tmp/fake_path".to_string()];
    let validation_error = JobRequest::validate(job_request.clone(), &commander).unwrap_err();
    assert_eq!(validation_error, ValidationError::no_output("Value does not exist on host for 'factfilePath':'/tmp/somewhere'".to_string()));
}

#[test]
fn job_request_can_append_job_args() {
    let server_manager = ServerManager::new("0.0.0.0".to_string(), 8080, "http://dummy.test/".to_string(), true);
    let mut job_request = JobRequest::new("1".to_string(), "dummy".to_string(), "/tmp/somewhere".to_string(), vec!["--first-arg".to_string()]);
    JobRequest::append_job_args(&server_manager, &mut job_request);
    assert_eq!(job_request.factfile_args, vec!["--first-arg", "--webhook", "http://dummy.test/", "--no-colour"]);
}

#[test]
fn settings_request_is_valid() {
    let settings_request = SettingsRequest::new(SERVER_STATE_RUN.to_string());
    let validated_settings_request = SettingsRequest::validate(settings_request.clone()).unwrap();
    assert_eq!(validated_settings_request, settings_request);
}

#[test]
fn settings_request_is_invalid() {
    let settings_request = SettingsRequest::new("NOT A SERVER STATE".to_string());
    let validation_error = SettingsRequest::validate(settings_request).err().unwrap();
    assert_eq!(validation_error.description(), "Invalid state, must be one of (run|drain)");
}