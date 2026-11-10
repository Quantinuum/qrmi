use std::collections::HashMap;
use std::io::Cursor;

use anyhow::anyhow;
use async_trait::async_trait;
use log::debug;
use quantinuum_nexus_client::client::{NexusClient, NexusClientConfig, Package};
use quantinuum_nexus_client::errors::NexusClientError;
use quantinuum_nexus_client::models::jobs::{JobDefinition, StatusEnum};
use uuid::Uuid;

use crate::models::{ResourceType, Target, TaskResult, TaskStatus};
use crate::{Payload, QrmiError, QuantumResource, Result};

fn parse_job_id(task_id: &str) -> Result<Uuid> {
    task_id
        .parse()
        .map_err(|err| QrmiError::InvalidInput(format!("invalid job id: {err}")))
}

pub struct QuantinuumNexusCloud {
    client: NexusClient,
    system_name: String,
    project_name: String,
    project_id: Option<Uuid>,
}

impl QuantinuumNexusCloud {
    pub fn new(resource_id: &str) -> Result<Self> {
        Self::from_config(resource_id, HashMap::new())
    }

    pub fn from_config(resource_id: &str, mut config: HashMap<String, String>) -> Result<Self> {
        let client = NexusClient::try_new(&NexusClientConfig::default())?;

        // Takes precedence over project name
        let project_id = config
            .remove("project_id")
            .map(|project_id_str| {
                project_id_str
                    .parse::<Uuid>()
                    .map_err(|err| QrmiError::ParseError {
                        name: "project_id".to_string(),
                        value: project_id_str,
                        source: Box::new(err),
                    })
            })
            .transpose()?;

        let project_name = config
            .remove("project_name")
            .unwrap_or_else(|| "default-qrmi-project".to_string());

        Ok(Self {
            client,
            system_name: resource_id.to_string(),
            project_name,
            project_id,
        })
    }

    /// Retrieve the project_id if set or use the client to find a project.
    async fn project_id(&mut self) -> Result<Uuid> {
        if let Some(project_id) = self.project_id {
            Ok(project_id)
        } else {
            let project_data = self
                .client
                .find_or_create_project_data(&self.project_name, Some("QRMI created project"))
                .await?;

            let project_id = project_data.id();
            self.project_id = Some(project_id);

            Ok(project_id)
        }
    }
}

#[async_trait]
impl QuantumResource for QuantinuumNexusCloud {
    async fn resource_id(&mut self) -> Result<String> {
        Ok(self.system_name.clone())
    }

    async fn resource_type(&mut self) -> Result<ResourceType> {
        Ok(ResourceType::QuantinuumNexusCloud)
    }

    async fn is_accessible(&mut self) -> Result<bool> {
        // TODO: systems API
        Ok(true)
    }

    async fn task_start(&mut self, payload: Payload) -> Result<String> {
        debug!(
            "Starting task on QuantinuumCloud QRMI (system '{}')",
            self.system_name
        );
        let Payload::QuantinuumNexusCloud {
            program,
            program_type,
            n_shots,
        } = payload
        else {
            return Err(QrmiError::UnsupportedPayload(format!("{payload:?}")));
        };

        let project_id = self.project_id().await?;
        let program_id = match &*program_type {
            "circuit" => todo!(),
            "hugr" => {
                let package = Package::load(Cursor::new(&program), None).map_err(|err| {
                    QrmiError::InvalidInput(format!("Invalid HUGR package: {err}"))
                })?;
                let hugr_data = self
                    .client
                    .new_hugr_data("QRMI created HUGR", None, project_id, package)
                    .await?;
                hugr_data.id()
            }
            "qir" => {
                let qir_data = self
                    .client
                    .new_qir_data("QRMI created QIR", None, project_id, &program)
                    .await?;
                qir_data.id()
            }
            _ => unimplemented!(),
        };

        let job_data = self
            .client
            .new_job_data(
                "QRMI created job",
                None,
                project_id,
                // Only single program jobs are supported initially.
                [(program_id, n_shots)],
            )
            .await?;

        Ok(job_data.id().to_string())
    }

    async fn task_stop(&mut self, task_id: &str) -> Result<()> {
        debug!(
            "Stopping task '{}' on QuantinuumCloud QRMI (system {}')",
            task_id, self.system_name
        );
        let job_id = parse_job_id(task_id)?;
        self.client.cancel_job(job_id).await?;
        Ok(())
    }

    async fn task_status(&mut self, task_id: &str) -> Result<TaskStatus> {
        let job_id = parse_job_id(task_id)?;
        // TODO: This is inefficient as we do have support for getting
        // job status through a dedicated endpoint.
        let job = self.client.get_job(job_id).await?;
        match job.status_enum() {
            StatusEnum::Completed => Ok(TaskStatus::Completed),
            StatusEnum::Queued => Ok(TaskStatus::Queued),
            StatusEnum::Submitted => Ok(TaskStatus::Queued),
            StatusEnum::Running => Ok(TaskStatus::Running),
            StatusEnum::Cancelled => Ok(TaskStatus::Cancelled),
            StatusEnum::Error => Ok(TaskStatus::Failed),
            StatusEnum::Cancelling => Ok(TaskStatus::Running),
            // Not a direct mapping but perhaps the closest possible.
            StatusEnum::Retrying => Ok(TaskStatus::Queued),
            StatusEnum::Terminated => Ok(TaskStatus::Cancelled),
            StatusEnum::Depleted => Ok(TaskStatus::Cancelled),
        }
    }

    async fn task_result(&mut self, task_id: &str) -> Result<TaskResult> {
        let job_id = parse_job_id(task_id)?;

        let job = self.client.get_job(job_id).await?;
        let JobDefinition::ExecuteJobDefinition { items } = job.definition();

        if items.len() != 1 {
            return Err(QrmiError::Other(anyhow!(
                "Retrieving results from jobs with zero or multiple programs are not supported"
            )));
        }
        let item = &items[0];

        let result_id = item
            .result_id()
            .ok_or_else(|| anyhow!("Item is complete but has no `result_id`"))?;

        let mut full_result = Vec::new();
        let mut chunk_number = 0;
        while let Some(result) = self
            .client
            .get_qsys_result_chunk(result_id, chunk_number)
            .await?
        {
            full_result.extend(result.results());
            chunk_number += 1;
        }
        let value = serde_json::to_string(&full_result)?;
        Ok(TaskResult { value })
    }

    async fn task_logs(&mut self, _task_id: &str) -> Result<String> {
        Ok("There are no logs for this job.".to_string())
    }

    async fn target(&mut self) -> Result<Target> {
        // TODO: Use systems API to get system error information etc
        todo!()
    }

    async fn metadata(&mut self) -> std::collections::HashMap<String, String> {
        let mut metadata: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        metadata.insert("system_name".to_string(), self.system_name.clone());
        metadata
    }
}

// TODO: placeholder for proper error mapping
impl From<NexusClientError> for QrmiError {
    fn from(value: NexusClientError) -> Self {
        Self::Other(value.into())
    }
}
