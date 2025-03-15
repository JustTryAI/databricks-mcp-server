### Phase 5 API Endpoints

---

### Unity Catalog

**Tables**
- **Get Table Details**
  - `GET /api/2.1/unity-catalog/tables/{name}`

**Volumes**
- **Get Volume Details**
  - `GET /api/2.1/unity-catalog/volumes/{name}`

**Service Principals**
- **Create Service Principal**
  - `POST /api/2.0/account/scim/v2/ServicePrincipals`
- **List Service Principals**
  - `GET /api/2.0/account/scim/v2/ServicePrincipals`
- **Get Service Principal**
  - `GET /api/2.0/account/scim/v2/ServicePrincipals/{id}`
- **Update Service Principal**
  - `PUT /api/2.0/account/scim/v2/ServicePrincipals/{id}`
  - `PATCH /api/2.0/account/scim/v2/ServicePrincipals/{id}`
- **Delete Service Principal**
  - `DELETE /api/2.0/account/scim/v2/ServicePrincipals/{id}`

---

### Databricks SQL

**SQL Warehouses**
- **List Warehouses**
  - `GET /api/2.0/sql/warehouses`
- **Create Warehouse**
  - `POST /api/2.0/sql/warehouses`
- **Get Warehouse**
  - `GET /api/2.0/sql/warehouses/{id}`
- **Delete Warehouse**
  - `DELETE /api/2.0/sql/warehouses/{id}`
- **Update a Warehouse**
  - `POST /api/2.0/sql/warehouses/{id}/edit`
- **Start a Warehouse**
  - `POST /api/2.0/sql/warehouses/{id}/start`
- **Stop a Warehouse**
  - `POST /api/2.0/sql/warehouses/{id}/stop`

---

### AI/BI

**Lakeview**
- **List Lakeviews**
  - `GET /api/2.0/lakeview/lakeviews`
- **Create Lakeview**
  - `POST /api/2.0/lakeview/lakeviews`
- **Get Lakeview**
  - `GET /api/2.0/lakeview/lakeviews/{id}`
- **Update Lakeview**
  - `PATCH /api/2.0/lakeview/lakeviews/{id}`
- **Delete Lakeview**
  - `DELETE /api/2.0/lakeview/lakeviews/{id}`

---

### File Management

**Files APIs**
- **List Files**
  - `GET /api/2.0/workspace/list`
- **Import Files**
  - `POST /api/2.0/workspace/import`
- **Export Files**
  - `GET /api/2.0/workspace/export`
- **Delete Files**
  - `POST /api/2.0/workspace/delete`
- **Get File Status**
  - `GET /api/2.0/workspace/get-status`
- **Mkdirs**
  - `POST /api/2.0/workspace/mkdirs`

---
