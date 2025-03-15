### Consolidated Databricks REST API Endpoints

## 1. Compute Management

**Clusters:**
- Create: `POST /api/2.0/clusters/create`
- Start: `POST /api/2.0/clusters/start`
- Restart: `POST /api/2.0/clusters/restart`
- Resize: `POST /api/2.0/clusters/resize`
- Delete: `POST /api/2.0/clusters/delete`
- Get Info: `GET /api/2.0/clusters/get`
- List: `GET /api/2.0/clusters/list`

**Command Execution:**
- Create Context: `POST /api/1.2/contexts/create`
- Execute Command: `POST /api/1.2/commands/execute`

**Library Management:**
- Install: `POST /api/2.0/libraries/install`
- Uninstall: `POST /api/2.0/libraries/uninstall`
- List: `GET /api/2.0/libraries/list`

---

### Job Management:
- Create: `POST /api/2.1/jobs/create`
- Update: `POST /api/2.1/jobs/update`
- Delete: `POST /api/2.1/jobs/delete`
- Run/Trigger: `POST /api/2.0/jobs/run-now`
- Reset: `POST /api/2.1/jobs/reset`
- Get Output: `GET /api/2.1/jobs/runs/get-output`
- List Runs: `GET /api/2.1/jobs/runs/list`
- Cancel Run: `POST /api/2.1/jobs/runs/cancel`

---

**Unity Catalog Management:**

**Catalogs:**
- Create: `POST /api/2.1/unity-catalog/catalogs`
- Get Details: `GET /api/2.1/unity-catalog/catalogs/{name}`
- Update: `PATCH /api/2.1/unity-catalog/catalogs/{name}`
- Delete: `DELETE /api/2.1/unity-catalog/catalogs/{name}`

**Connections:**
- Create: `POST /api/2.1/unity-catalog/connections`
- List: `GET /api/2.1/unity-catalog/connections`
- Update: `PATCH /api/2.1/unity-catalog/connections/{name}`
- Delete: `DELETE /api/2.1/unity-catalog/connections/{name}`

**Credentials:**
- Create: `POST /api/2.1/unity-catalog/credentials`
- List: `GET /api/2.1/unity-catalog/credentials`
- Update: `PATCH /api/2.1/unity-catalog/credentials/{name}`
- Delete: `DELETE /api/2.1/unity-catalog/credentials/{name}`

**Schemas:**
- Create: `POST /api/2.1/unity-catalog/schemas`
- Get Details: `GET /api/2.1/unity-catalog/schemas/{name}`
- Update: `PATCH /api/2.1/unity-catalog/schemas/{name}`
- Delete: `DELETE /api/2.1/unity-catalog/schemas/{name}`

**Storage Credentials:**
- Create: `POST /api/2.1/unity-catalog/storage-credentials`
- Get Details: `GET /api/2.1/unity-catalog/storage-credentials/{name}`
- Update: `PATCH /api/2.1/unity-catalog/storage-credentials/{name}`
- Delete: `DELETE /api/2.1/unity-catalog/storage-credentials/{name}`

**Tables:**
- Create: `POST /api/2.1/unity-catalog/tables`
- Get Details: `GET /api/2.1/unity-catalog/tables/{name}`
- Update: `PATCH /api/2.1/unity-catalog/tables/{name}`
- Delete: `DELETE /api/2.1/unity-catalog/tables/{name}`

**Volumes:**
- Create: `POST /api/2.1/unity-catalog/volumes`
- Get Details: `GET /api/2.1/unity-catalog/volumes/{name}`
- Update: `PATCH /api/2.1/unity-catalog/volumes/{name}`
- Delete: `DELETE /api/2.1/unity-catalog/volumes/{name}`

**Service Principals:**
- Create: `POST /api/2.0/account/scim/v2/ServicePrincipals`
- List: `GET /api/2.0/account/scim/v2/ServicePrincipals`
- Update: `PATCH /api/2.0/account/scim/v2/ServicePrincipals/{id}`
- Delete: `DELETE /api/2.0/account/scim/v2/ServicePrincipals/{id}`

---

### Delta Live Tables
- Create Pipeline: `POST /api/2.0/pipelines`
- Update: `PUT /api/2.0/pipelines/{pipeline_id}`
- Delete: `DELETE /api/2.0/pipelines/{pipeline_id}`
- Start Update: `POST /api/2.0/pipelines/{pipeline_id}/updates`
- List Pipelines: `GET /api/2.0/pipelines`
- Get Details: `GET /api/2.0/pipelines/{pipeline_id}`

**LakeView APIs:**
- List Lakeviews: `GET /api/2.0/lakeview/lakeviews`
- Create Lakeview: `POST /api/2.0/lakeview/lakeviews`
- Get Lakeview: `GET /api/2.0/lakeview/lakeviews/{id}`
- Update Lakeview: `PATCH /api/2.0/lakeview/lakeviews/{id}`
- Delete Lakeview: `DELETE /api/2.0/lakeview/lakeviews/{id}`

**File Management:**
- Import File: `POST /api/2.0/workspace/import`
- Export File: `GET /api/2.0/workspace/export`
- List Files: `GET /api/2.0/dbfs/list`
- Create Directory: `POST /api/2.0/dbfs/mkdirs`
- Delete File: `POST /api/2.0/dbfs/delete`
- Read File: `GET /api/2.0/dbfs/read`
- Move File: `POST /api/2.0/dbfs/move`

