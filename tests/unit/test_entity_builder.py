import logging

from client.storage_reader import SourceBucket, SourceColumn, SourceTable
from mapping.entity_builder import EntityBuilder

UI = "https://connection.keboola.com"


def _builder():
    return EntityBuilder("keboola-stack", "Acme_Project", "1234", UI)


def test_database_service_and_database_bodies():
    b = _builder()
    svc = b.database_service_body()
    assert svc["name"] == "keboola-stack"
    assert svc["serviceType"] == "CustomDatabase"

    db = b.database_body(display_name="Acme Project")
    assert db["name"] == "Acme_Project"
    assert db["service"] == "keboola-stack"
    assert db["sourceUrl"] == "https://connection.keboola.com/admin/projects/1234/storage"


def test_regular_table_body_has_columns_pk_and_sourceurl():
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(
        id="out.c-sales.orders",
        name="orders",
        description="Orders",
        primary_key=["id"],
        columns=[
            SourceColumn(name="id", definition={"type": "NUMBER", "length": "38,0"}),
            SourceColumn(name="note", definition={"type": "VARCHAR"}),
            SourceColumn(name="code", definition={"type": "VARCHAR", "length": "50"}),
        ],
    )
    built = _builder().table_body(bucket, table)
    assert built.table_type == "Regular"
    assert built.fqn == "keboola-stack.Acme_Project.out_c-sales.orders"
    body = built.body
    assert body["tableConstraints"] == [{"constraintType": "PRIMARY_KEY", "columns": ["id"]}]
    assert body["databaseSchema"] == "keboola-stack.Acme_Project.out_c-sales"
    assert body["sourceUrl"].endswith("/storage/out.c-sales/table/out.c-sales.orders")
    cols = {c["name"]: c for c in body["columns"]}
    assert cols["id"]["dataType"] == "NUMERIC"
    # unknown-length varchar must become TEXT (length-free) and omit dataLength;
    # OM rejects a null dataLength for char/varchar, and we never emit dataLength:1.
    assert cols["note"]["dataType"] == "TEXT"
    assert "dataLength" not in cols["note"]
    # a known-length varchar keeps VARCHAR + dataLength
    assert cols["code"]["dataType"] == "VARCHAR"
    assert cols["code"]["dataLength"] == 50
    assert built.view_source_fqn is None


def test_view_table_yields_schema_definition_and_source_fqn():
    bucket = SourceBucket(
        id="in.c-linked",
        name="c-linked",
        stage="in",
        path="in.c-linked",
        source_bucket={"id": "out.c-shared", "project": {"name": "Owner_Project"}},
    )
    table = SourceTable(
        id="in.c-linked.customers",
        name="customers",
        columns=[SourceColumn(name="id")],
        source_table={"id": "out.c-shared.customers", "project": {"name": "Owner_Project"}},
    )
    built = _builder().table_body(bucket, table)
    assert built.table_type == "View"
    assert built.view_source_fqn == "keboola-stack.Owner_Project.out_c-shared.customers"
    assert built.body["schemaDefinition"].startswith(
        "CREATE VIEW keboola-stack.Acme_Project.in_c-linked.customers AS SELECT * FROM "
    )


def test_colliding_sanitized_column_names_are_disambiguated(caplog):
    # Two distinct source columns that sanitize to the SAME FQN-safe segment:
    # a dotted name and an underscored name both -> "properties_hs_migration_soft_delete".
    # OM rejects a table whose columns[] repeat a name; both must survive, disambiguated.
    bucket = SourceBucket(id="out.c-hs", name="c-hs", stage="out", path="out.c-hs")
    table = SourceTable(
        id="out.c-hs.contacts",
        name="contacts",
        columns=[
            SourceColumn(name="properties.hs_migration_soft_delete", definition={"type": "VARCHAR", "length": "50"}),
            SourceColumn(name="properties_hs_migration_soft_delete", definition={"type": "VARCHAR", "length": "50"}),
        ],
    )
    with caplog.at_level(logging.WARNING):
        built = _builder().table_body(bucket, table)

    cols = built.body["columns"]
    names = [c["name"] for c in cols]
    # both source columns survive, second occurrence gets an ordinal suffix
    assert names == [
        "properties_hs_migration_soft_delete",
        "properties_hs_migration_soft_delete_2",
    ]
    # no duplicate column name remains
    assert len(names) == len(set(names))
    # original source name preserved as displayName; dataTypeDisplay preserved
    assert cols[0]["displayName"] == "properties.hs_migration_soft_delete"
    assert cols[1]["displayName"] == "properties_hs_migration_soft_delete"
    assert cols[0]["dataTypeDisplay"] == cols[1]["dataTypeDisplay"] == "VARCHAR(50)"
    # a warning naming the table + the collided name is logged
    assert any(
        "contacts" in r.getMessage() and "properties_hs_migration_soft_delete" in r.getMessage()
        for r in caplog.records
    )


def test_normal_table_columns_are_not_disambiguated(caplog):
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(
        id="out.c-sales.orders",
        name="orders",
        columns=[
            SourceColumn(name="id", definition={"type": "NUMBER", "length": "38,0"}),
            SourceColumn(name="note", definition={"type": "VARCHAR"}),
        ],
    )
    with caplog.at_level(logging.WARNING):
        built = _builder().table_body(bucket, table)

    assert [c["name"] for c in built.body["columns"]] == ["id", "note"]
    assert caplog.records == []


def test_external_table_detected():
    bucket = SourceBucket(id="in.c-ext", name="c-ext", stage="in", path="in.c-ext", has_external_schema=True)
    table = SourceTable(id="in.c-ext.events", name="events", columns=[SourceColumn(name="id")])
    built = _builder().table_body(bucket, table)
    assert built.table_type == "External"
