## 1. Delta Live Tables (DLT) Creation

- **Create Pipeline**
  - `POST /api/2.0/pipelines`

- **Start Pipeline Update**
  - `POST /api/2.0/pipelines/{pipeline_id}/updates`

- **Get Pipeline Details**
  - `GET /api/2.0/pipelines/{pipeline_id}`

- **List Pipeline Updates**
  - `GET /api/2.0/pipelines/{pipeline_id}/updates`

- **Get Pipeline Update Details**
  - `GET /api/2.0/pipelines/{pipeline_id}/updates/{update_id}`

---

## 2. Unity Catalog Advanced Management

- **Update Catalog**
  - `PATCH /api/2.1/unity-catalog/catalogs/{name}`

- **Update Schema**
  - `PATCH /api/2.1/unity-catalog/schemas/{name}`

- **Update Table**
  - `PATCH /api/2.1/unity-catalog/tables/{name}`

- **Update Volume**
  - `PATCH /api/2.1/unity-catalog/volumes/{name}`

- **Manage Permissions (Grant/Revoke)**
  - `PATCH /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}`

---

## 3. Serverless SQL Warehouse Creation

- **Create Warehouse**
  - `POST /api/2.0/sql/warehouses`

- **Start Warehouse**
  - `POST /api/2.0/sql/warehouses/{id}/start`

- **Update Warehouse Configuration**
  - `POST /api/2.0/sql/warehouses/{id}/edit`

- **Get Warehouse Details**
  - `GET /api/2.0/sql/warehouses/{id}`

---

## 4. Budget Policy Management

- **Create Budget**
  - `POST /api/2.0/budgets`

- **List Budgets**
  - `GET /api/2.0/budgets`

- **Update Budget**
  - `PATCH /api/2.0/budgets/{budget_id}`

- **Get Budget Details**
  - `GET /api/2.0/budgets/{budget_id}`

- **Delete Budget**
  - `DELETE /api/2.0/budgets/{budget_id}`

---

## 5. External Location Registration

- **Create External Location**
  - `POST /api/2.1/unity-catalog/external-locations`

- **List External Locations**
  - `GET /api/2.1/unity-catalog/external-locations`

- **Get External Location Details**
  - `GET /api/2.1/unity-catalog/external-locations/{name}`

- **Update External Location**
  - `PATCH /api/2.1/unity-catalog/external-locations/{name}`

- **Delete External Location**
  - `DELETE /api/2.1/unity-catalog/external-locations/{name}`

---
