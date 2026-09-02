from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class SchemaDiscoveryRequest(BaseModel):
    """Request payload for the schema discovery endpoint."""

    connection_url: str = Field(
        ...,
        description=(
            "SQLAlchemy-compatible connection URL including credentials, e.g. "
            "'postgresql://user:password@host:5432/mydb'."  # pragma: allowlist secret
        ),
    )
    table: Optional[str] = Field(
        None,
        description=(
            "Table to describe. Either simple ('users') or schema-qualified "
            "('public.users'). When omitted, schemas are listed instead."
        ),
    )
    schema_name: Optional[str] = Field(
        None,
        alias="schema",
        description=(
            "Database schema/namespace. Qualifies `table` when `table` is "
            "unqualified; scopes the listing when `table` is omitted."
        ),
    )
    include_tables: bool = Field(
        True,
        description="When `table` is omitted, also list the tables in each schema.",
    )

    model_config = {"populate_by_name": True}


class ColumnSchema(BaseModel):
    """A single column of a database table."""

    name: str
    sql_type: str = Field(
        ..., description="Native SQL type as reported by the database."
    )
    nullable: bool
    primary_key: bool
    feast_type: Optional[str] = Field(
        None,
        description=(
            "Name of the mapped Feast ValueType, or 'UNKNOWN' when the native "
            "type has no Feast equivalent."
        ),
    )


class TableInfo(BaseModel):
    """A table or view within a schema."""

    name: str
    type: Literal["table", "view"] = "table"


class SchemaInfo(BaseModel):
    """A database schema (namespace), optionally with its tables."""

    name: str
    tables: Optional[List[TableInfo]] = None


class DatabaseSchemas(BaseModel):
    """Response returned when no table is requested."""

    database: str
    dialect: str
    schemas: List[SchemaInfo]


class TableSchema(BaseModel):
    """Response returned when a table is requested."""

    database: str
    dialect: str
    table: str
    columns: List[ColumnSchema]
