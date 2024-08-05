# openmetadata-dremio-connector
Custom Openmetadata connector to connect data from Dremio to Openmetadata. 

This connector uses the [sqlalchemy-dremio](https://github.com/narendrans/sqlalchemy_dremio) package to establish a conncetion to Dremio over the arrow flight API. 

![Static Badge](https://img.shields.io/badge/Metadata-43a047?style=flat)
![Static Badge](https://img.shields.io/badge/Query_Usage-ff7c50?style=flat)
![Static Badge](https://img.shields.io/badge/Data_Profiler-ff7c50?style=flat)
![Static Badge](https://img.shields.io/badge/Data_Quality-ff7c50?style=flat)
![Static Badge](https://img.shields.io/badge/Lineage-ff7c50?style=flat)
![Static Badge](https://img.shields.io/badge/Column_level_Lineage-ff7c50?style=flat)
![Static Badge](https://img.shields.io/badge/dbt-ff7c50?style=flat)
![Static Badge](https://img.shields.io/badge/Owners-ff7c50?style=flat)
![Static Badge](https://img.shields.io/badge/Tags-ff7c50?style=flat)
![Static Badge](https://img.shields.io/badge/Stored_Procedures-ff7c50?style=flat)


## Configuration

This Connector is currently implemented as `CustomDatabase` connector and therefore the only way to configure any connection parameters is using the `connectionOptions`.

```yaml
connectionOptions:
  hostPort: <host>:<arrowFlightPort>
  username: <username>
  password: <password>
  # optional
  UseEncryption: False
  disableCertificateVerification: True
```

## Local Dev Stack

Requirements:
- Access to a Dremio instance

Steps:

1. Setup python venv and install requirements
2. Start openmetadata local stack
    ```bash
    make local-openmetadata-stack 
    ```
3. Create `workflow.dremio.yaml` as a copy from `workflow.dremio.template.yaml`
4. Configure `workflow.dremio.yaml`. Mainly editing connection credentials to Dremio and the jwtToken for OpenMetadata (e.g. from the ingestion bot)
5. Run / Debug ingestion
    ```bash
    python.exe -m metadata ingest -c ./workflow.dremio.yaml
    ```
    Or use the provided Run configuration for IntelliJ

