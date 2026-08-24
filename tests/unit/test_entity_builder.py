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


def test_external_table_detected():
    bucket = SourceBucket(id="in.c-ext", name="c-ext", stage="in", path="in.c-ext", has_external_schema=True)
    table = SourceTable(id="in.c-ext.events", name="events", columns=[SourceColumn(name="id")])
    built = _builder().table_body(bucket, table)
    assert built.table_type == "External"
