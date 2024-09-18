import textwrap
import traceback
from copy import deepcopy
from typing import Optional, Iterable, Dict, Any, Tuple, List

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.table import Column, TableConstraint, TableType
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import \
    CustomDatabaseConnection
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.connections.builders import create_generic_db_connection
from metadata.ingestion.connections.secrets import connection_with_options_secrets
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.common_db_source import CommonDbSourceService, TableNameAndType
from metadata.ingestion.source.database.multi_db_source import MultiDBSource
from metadata.utils import fqn
from metadata.utils.filters import filter_by_database
from metadata.utils.logger import ingestion_logger
from sqlalchemy import types
from sqlalchemy.engine import Engine, Inspector
from sqlalchemy.sql.sqltypes import STRINGTYPE

from sqlalchemy_dremio import flight

logger = ingestion_logger()

DREMIO_GET_DATABASES = textwrap.dedent(
    """
SELECT SCHEMA_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME NOT LIKE '%.%' 
  AND NOT STARTS_WITH(SCHEMA_NAME, '@') 
  AND NOT STARTS_WITH(SCHEMA_NAME, '$')
    """
)

DREMIO_GET_SCHEMAS = textwrap.dedent(
    """
SELECT SCHEMA_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME LIKE '{database_name}.%' 
    """
)


# SqlAlchemy < 2.0 doesn't have a DOUBLE type, but using Float here would be misleading and can be dangerous for the openmetadata users
class DOUBLE(types.Float):
    __visit_name__ = "DOUBLE"


# monkey patching the sql types of sqlalchemy_dremio package
flight._type_map.update({
    'double': DOUBLE,
    'DOUBLE': DOUBLE,
    'CHARACTER VARYING': STRINGTYPE,
    'BINARY VARYING': types.LargeBinary,
})


class InvalidDremioConnectorException(Exception):
    """
    Connection argument is missing
    """


class DremioConnector(CommonDbSourceService, MultiDBSource):
    """
    Dremio has following design:
    <Space>.<Folder>(.<Folder+N>).<Relation>
    In this connector / source implementation we mapped this design as follow:
    - Space = Database
    - Folders = Schema
    - Relation = Table / View name etc.
    But dremio interprets spaces and folders as schematas, so we have to handle them specially:
    - Only select spaces when retrieving databases
    - Remove spaces from schemas, like in get_raw_database_schema_names
    - Readd database to schema when querying tables / views, because dremio expects the full path
    """

    def __init__(
            self,
            config: WorkflowSource,
            metadata: OpenMetadata,
    ):
        self.test_connection = lambda: None
        super().__init__(config, metadata)
        self.database = None
        self.test_connection = self._test_connection
        self.test_connection()

    @classmethod
    def create(cls, config_dict: dict, metadata: OpenMetadata,
               pipeline_name: Optional[str] = None) -> "DremioConnector":
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: CustomDatabaseConnection = config.serviceConnection.root.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected CustomDatabaseConnection, but got {connection}"
            )
        return cls(config, metadata)

    # ------------------------------------------------------------------------------------------------------------------
    # ############################
    # ### extend MultiDBSource ###
    # ############################
    def get_configured_database(self) -> Optional[str]:
        return None

    def get_database_names_raw(self) -> Iterable[str]:
        yield from self._execute_database_query(DREMIO_GET_DATABASES)

    # ------------------------------------------------------------------------------------------------------------------
    # ### ################################
    # ### Extend CommonDbSourceService ###
    # ### ################################
    def get_database_names(self) -> Iterable[str]:
        configured_database = self.get_configured_database()
        if configured_database:
            self.set_inspector(database_name=configured_database)
            yield configured_database
        else:
            for new_database in self.get_database_names_raw():
                database_fqn = fqn.build(
                    self.metadata,
                    entity_type=Database,
                    service_name=self.context.get().database_service,
                    database_name=new_database,
                )
                if filter_by_database(
                        self.source_config.databaseFilterPattern,
                        database_fqn
                        if self.source_config.useFqnForFiltering
                        else new_database,
                ):
                    self.status.filter(database_fqn, "Database Filtered Out")
                    continue
                try:
                    self.set_inspector(database_name=new_database)
                    yield new_database
                except Exception as exc:
                    logger.error(traceback.format_exc())
                    logger.warning(
                        f"Error trying to process database {new_database}: {exc}"
                    )

    # TODO implement
    @staticmethod
    def get_table_description(
            schema_name: str, table_name: str, inspector: Inspector
    ) -> str:
        # inspector.get_table_comment(..) not available in sql-alchemy dremio dialect
        return ""

    def get_raw_database_schema_names(self) -> Iterable[str]:
        if self.database is not None:
            schemas = self._execute_database_query(DREMIO_GET_SCHEMAS.format(database_name=self.database))
        else:
            schemas = self.inspector.get_schema_names()

        for schema_name in schemas:
            cleaned_schema_name = self._remove_database_from_schema_name(schema_name)

            yield cleaned_schema_name

    def _remove_database_from_schema_name(self, schema_name: str) -> str:
        if self.database is not None:
            if not schema_name.startswith(self.database) or schema_name is None or schema_name == self.database:
                return schema_name

            schema_name = schema_name[len(self.database) + 1:]
        return schema_name

    def _add_database_to_schema_name(self, schema_name: str) -> str:
        if self.database is not None:
            if schema_name is None or schema_name.strip() == "":
                schema_name = self.database
            else:
                schema_name = self.database + "." + schema_name
        return schema_name

    def get_columns_and_constraints(  # pylint: disable=too-many-locals
            self, schema_name: str, table_name: str, db_name: str, inspector: Inspector
    ) -> Tuple[
        Optional[List[Column]], Optional[List[TableConstraint]], Optional[List[Dict]]
    ]:
        return super().get_columns_and_constraints(
            self._add_database_to_schema_name(schema_name), table_name, db_name, inspector)

    def get_schema_definition(
            self, table_type: TableType, table_name: str, schema_name: str, inspector: Inspector
    ) -> Optional[str]:
        return super().get_schema_definition(
            table_type, table_name, self._add_database_to_schema_name(schema_name), inspector)

    def query_table_names_and_types(
            self, schema_name: str
    ) -> Iterable[TableNameAndType]:
        return super().query_table_names_and_types(self._add_database_to_schema_name(schema_name))

    def query_view_names_and_types(
            self, schema_name: str
    ) -> Iterable[TableNameAndType]:
        return super().query_view_names_and_types(self._add_database_to_schema_name(schema_name))

    def set_inspector(self, database_name: str) -> None:
        # Mainly a copy of the parent class with the small change
        # of storing the database in the current connector instance,
        # since "database" parameter does not exist for CustomDatabaseConnection
        logger.info(f"Ingesting from database: {database_name}")

        new_service_connection = deepcopy(self.service_connection)
        self.engine = get_connection(new_service_connection)
        self.database = database_name

        self._connection_map = {}  # Lazy init as well
        self._inspector_map = {}

    # TODO implement
    def yield_view_lineage(self) -> Iterable[Either[AddLineageRequest]]:
        pass

    # TODO implement
    def _test_connection(self) -> None:
        # see https://docs.open-metadata.org/v1.4.x/sdk/python/build-connector/source
        #   test_connection is used (by OpenMetadata supported connectors ONLY) to validate permissions and connectivity
        #   before moving forward with the ingestion.
        pass


def get_connection_url(connection: CustomDatabaseConnection) -> str:
    def _get_option_or_else(option_name: str, *, default: Any = None, expected: bool = False):
        value = connection.connectionOptions.root.get(option_name)
        if not value:
            if expected:
                raise InvalidDremioConnectorException(f"Missing connection option: {option_name}")
            else:
                value = default
        return value

    scheme_value = "dremio+flight"
    username = _get_option_or_else("username", expected=True)
    # TODO password is in clear text and can be read by anyone in the ui
    password = _get_option_or_else("password", expected=True)
    host_port = _get_option_or_else("hostPort", expected=True)

    use_encryption = _get_option_or_else("UseEncryption", default=False, expected=False)
    disable_certificate_verification = _get_option_or_else("disableCertificateVerification", default=True,
                                                           expected=False)

    not_handled_options = (
            set(connection.connectionOptions.root.keys()) -
            {"username", "password", "hostPort", "UseEncryption", "disableCertificateVerification"}
    )

    additional_options = "&".join(
        [f'UseEncryption={use_encryption}', f'disableCertificateVerification={disable_certificate_verification}'] +
        [f'{k}={_get_option_or_else(k)}' for k in not_handled_options]
    )

    url = f"{scheme_value}://"
    url += f"{username}:{password}"
    url += f"@"
    url += f"{host_port}"
    url += f"/?"
    url += f"{additional_options}"

    return url


@connection_with_options_secrets
def get_connection_args(connection: CustomDatabaseConnection) -> Dict[str, Any]:
    return {}


def get_connection(connection: CustomDatabaseConnection) -> Engine:
    return create_generic_db_connection(
        connection=connection,
        get_connection_url_fn=get_connection_url,
        get_connection_args_fn=get_connection_args,
    )
