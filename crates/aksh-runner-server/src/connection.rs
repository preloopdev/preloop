use super::*;

/// Stable deployment ID — generated once per server lifetime via lazy_static pattern.
/// The official service returns a fixed deployment GUID; we do the same.
const DEPLOYMENT_ID: &str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

/// Stable instance ID — same reasoning as deployment ID.
const INSTANCE_ID: &str = "bc944321-3dbc-431b-8cf2-8afa3e25e359";

pub(crate) async fn connection_data(
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    if params.get("connectOptions").map(String::as_str) == Some("0")
        && params
            .get("lastChangeId")
            .is_some_and(|last_change_id| last_change_id != "-1")
    {
        return axum::response::Json(json!({
            "deploymentId": DEPLOYMENT_ID,
            "deploymentType": "hosted",
            "instanceId": INSTANCE_ID,
            "locationServiceData": {
                "clientCacheFresh": true,
                "defaultAccessMappingMoniker": "ScaleUnitMapping",
                "lastChangeId": 1,
                "lastChangeId64": 1,
                "serviceOwner": "0000005a-0000-8888-8000-000000000000"
            }
        }))
        .into_response();
    }

    let service_root = public_base_url();
    let runner_root = runner_server_url();
    let body = serde_json::json!({
        "deploymentId": DEPLOYMENT_ID,
        "deploymentType": "hosted",
        "instanceId": INSTANCE_ID,
        "serverUrlV2": runner_root,
        "brokerUrl": public_base_url(),
        "resultsServiceUrl": runner_root,
        "locationServiceData": {
            "lastChangeId": 1,
            "lastChangeId64": 1,
            "clientCacheFresh": true,
            "serviceOwner": "0000005a-0000-8888-8000-000000000000",
            "serviceDefinitions": [
                area_svc("Location Service", "9f1fe989-7d0d-4a9b-a9bf-11330ab257c1", "LocationService2", "Framework", &service_root),
                area_svc("distributedtask", "a85b8835-c1a1-4aac-ae97-1c3d0ba72dbd", "LocationService2", "Framework", &runner_root),
                area_svc("pipelines", "2e0bf237-8973-4ec9-a581-9c3d679d1776", "LocationService2", "Framework", &service_root),
                area_svc("oauth2", "a7b3b527-4f4f-4dac-8e84-f144fa6d554b", "LocationService2", "Framework", &runner_root),
                svc("AgentPools", "a8c47e17-4d56-4a56-92bb-de7ea7dc65be", "/_apis/v1/AgentPools"),
                svc("Agent", "e298ef32-5878-4cab-993c-043836571f42", "/_apis/v1/Agent/{poolId}/{agentId}"),
                svc("AgentSession", "134e239e-2df3-4794-a6f6-24f1f19ec8dc", "/_apis/v1/AgentSession/{poolId}/{sessionId}"),
                svc("Message", "c3a054f6-7a8a-49c0-944e-3a8e5d7adfd7", "/_apis/v1/Message/{poolId}/{messageId}"),
                svc("AgentRequest", "fc825784-c92a-4299-9221-998a02d1b54f", "/_apis/v1/AgentRequest/{poolId}/{requestId}"),
                svc("ActionDownloadInfo", "27d7f831-88c1-4719-8ca1-6a061dad90eb", "/_apis/v1/ActionDownloadInfo/{scopeIdentifier}/{hubName}/{planId}"),
                svc("TimeLineWebConsoleLog", "858983e4-19bd-4c5e-864c-507b59b58b12", "/_apis/v1/TimeLineWebConsoleLog/{scopeIdentifier}/{hubName}/{planId}/{timelineId}/{recordId}"),
                svc("TimelineRecords", "8893bc5b-35b2-4be7-83cb-99e683551db4", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/{timelineId}"),
                svc("Logfiles", "46f5667d-263a-4684-91b1-dff7fdcf64e2", "/_apis/v1/Logfiles/{scopeIdentifier}/{hubName}/{planId}/{logId}"),
                svc("FinishJob", "557624af-b29e-4c20-8ab0-0399d2204f3f", "/_apis/v1/FinishJob/{scopeIdentifier}/{hubName}/{planId}"),
                svc("Artifact", "85023071-bd5e-4438-89b0-2a5bf362a19d", "/_apis/pipelines/workflows/{runId}/artifacts"),
                svc("ArtifactFileContainer", "e4f5c81e-e250-447b-9fef-bd48471bea5e", "/_apis/pipelines/workflows/container/{containerId}"),
                svc("TimelineAttachments", "7898f959-9cdf-4096-b29e-7f293031629e", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/{timelineId}/attachments/{recordId}/{type}/{name}"),
                svc("Timeline", "83597576-cc2c-453c-bea6-2882ae6a1653", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/timeline/{timelineId}"),
                svc("CustomerIntelligence", "b5cc35c2-ff2b-491d-a085-24b6e9f396fd", "/_apis/v1/tasks"),
                svc("Tasks", "60aac929-f0cd-4bc8-9ce4-6b30e8f1b1bd", "/_apis/v1/tasks/{taskId}/{versionString}"),
                svc("Cache", "a7c78d38-31a8-417e-ba6b-7e58b352f304", "_apis/artifactcache"),
                svc("BuildArtifacts", "1db06c96-014e-44e1-ac91-90b2d4b3e984", "_apis/pipelines/workflows/{buildId}/artifacts"),
                resource_svc("brokerlistener", "38f00041-0953-4d24-86c3-5432d23e2205", "distributedtask", "_apis/{area}/{resource}"),
                resource_svc("createdsession", "a4e1f2b5-0c3d-4e8a-9f6d-7b5c1a0e2d3f", "distributedtask", "_apis/{area}/brokerlistener/{resource}"),
                resource_svc("runnermessages", "25adab70-1379-4186-be8e-b643061ebe3a", "distributedtask", "_apis/{area}/{resource}/{messageId}"),
                resource_svc("runnerconfigrefresh", "13b5d709-74aa-470b-a8e9-bf9f3ded3f18", "distributedtask", "_apis/{area}/agents/{agentId}/{resource}/{configType}"),
                resource_svc("token", "10d13a60-2758-406c-8ab7-cffccb21fcf4", "oauth2", "_apis/{area}/{resource}"),
                resource_svc("steps", "99ea91b7-bbe9-4bd3-a924-874f13205b21", "pipelines", "_apis/{area}/plans/{planId}/jobs/{jobId}/{resource}"),
                resource_svc("jobs", "4818972d-29fa-4b86-92c1-de5ae7ef33f5", "pipelines", "_apis/{area}/plans/{planId}/{resource}/{jobId}"),
                resource_svc("logs", "fb1b6d27-3957-43d5-a14b-a2d70403e545", "pipelines", "{project}/_apis/{area}/{pipelineId}/runs/{runId}/{resource}/{logId}"),
            ],
            "accessMappings": [
                {
                    "moniker": "PublicAccessMapping",
                    "displayName": "Public Access Mapping",
                    "accessPoint": service_root,
                    "serviceOwner": "0000005a-0000-8888-8000-000000000000",
                    "virtualDirectory": ""
                },
                {
                    "moniker": "ScaleUnitMapping",
                    "displayName": "Scale Unit Access Mapping",
                    "accessPoint": runner_root,
                    "serviceOwner": "0000005a-0000-8888-8000-000000000000",
                    "virtualDirectory": ""
                }
            ],
            "defaultAccessMappingMoniker": "ScaleUnitMapping",
            "clientCacheFresh": true,
            "serviceOwner": "0000005a-0000-8888-8000-000000000000"
        }
    });
    axum::response::Json(body).into_response()
}


const SVC_OWNER: &str = "0000005a-0000-8888-8000-000000000000";

fn area_svc(
    display_name: &str,
    id: &str,
    service_type: &str,
    tool_id: &str,
    location: &str,
) -> serde_json::Value {
    serde_json::json!({
        "serviceType": service_type,
        "identifier": id,
        "displayName": display_name,
        "description": display_name,
        "toolId": tool_id,
        "relativeToSetting": "fullyQualified",
        "locationMappings": [
            {"accessMappingMoniker": "PublicAccessMapping", "location": location},
            {"accessMappingMoniker": "ScaleUnitMapping", "location": location}
        ],
        "serviceOwner": SVC_OWNER,
        "properties": {}
    })
}

fn resource_svc(name: &str, id: &str, area: &str, location: &str) -> serde_json::Value {
    serde_json::json!({
        "serviceType": area,
        "identifier": id,
        "displayName": name,
        "relativePath": location,
        "description": name,
        "toolId": area,
        "locationMappings": [
            {"accessMappingMoniker": "ScaleUnitMapping", "location": runner_server_url()},
            {"accessMappingMoniker": "PublicAccessMapping", "location": public_base_url()}
        ],
        "serviceOwner": SVC_OWNER,
        "resourceVersion": 1,
        "minVersion": "1.0",
        "maxVersion": "6.0",
        "releasedVersion": "0.0",
        "status": 1,
        "properties": {}
    })
}

fn svc(name: &str, id: &str, location: &str) -> serde_json::Value {
    serde_json::json!({
        "serviceType": name,
        "identifier": id,
        "displayName": name,
        "relativePath": location,
        "relativeToSetting": 2,
        "description": name,
        "toolId": name,
        "locationMappings": [
            {"accessMappingMoniker": "ScaleUnitMapping", "location": runner_server_url()},
            {"accessMappingMoniker": "PublicAccessMapping", "location": public_base_url()}
        ],
        "serviceOwner": SVC_OWNER,
        "resourceVersion": 6,
        "minVersion": "1.0",
        "maxVersion": "12.0",
        "status": 1,
        "properties": {}
    })
}
