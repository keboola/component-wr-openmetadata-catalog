import logging

from client.storage_reader import SourceBucket, SourceColumn, SourceTable
from mapping.entity_builder import (
    DATABASE_CUSTOM_PROPERTIES,
    SCHEMA_CUSTOM_PROPERTIES,
    TABLE_CUSTOM_PROPERTIES,
    EntityBuilder,
)

UI = "https://connection.keboola.com"
STACK = "connection.us-east4.gcp.keboola.com"


def _builder(stack_id=None):
    return EntityBuilder("keboola-stack", "Acme_Project", "1234", UI, stack_id)


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
        "contacts" in r.getMessage() and "properties_hs_migration_soft_delete" in r.getMessage() for r in caplog.records
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


# --------------------------------------------------------------------- custom property constants


def test_table_custom_properties_names_and_types():
    names = {n for n, *_ in TABLE_CUSTOM_PROPERTIES}
    assert names == {
        "kbcTableId",
        "kbcBucketId",
        "kbcStage",
        "kbcRowsCount",
        "kbcDataSizeBytes",
        "kbcLastImport",
        "kbcIsAlias",
        "kbcTableUrl",
        "kbcSyncedAt",
    }
    types = {n: t for n, t, *_ in TABLE_CUSTOM_PROPERTIES}
    assert types["kbcTableUrl"] == "hyperlink-cp"
    assert types["kbcTableId"] == "string"


def test_schema_custom_properties_names_and_types():
    names = {n for n, *_ in SCHEMA_CUSTOM_PROPERTIES}
    assert names == {"kbcBucketId", "kbcStage", "kbcBackend", "kbcSharing", "kbcBucketUrl", "kbcSyncedAt"}
    types = {n: t for n, t, *_ in SCHEMA_CUSTOM_PROPERTIES}
    assert types["kbcBucketUrl"] == "hyperlink-cp"


def test_database_custom_properties_names_and_types():
    names = {n for n, *_ in DATABASE_CUSTOM_PROPERTIES}
    assert names == {"kbcProjectId", "kbcProjectUrl", "kbcSyncedAt"}
    types = {n: t for n, t, *_ in DATABASE_CUSTOM_PROPERTIES}
    assert types["kbcProjectUrl"] == "hyperlink-cp"


# --------------------------------------------------------------------- table_body extension


def test_table_body_extension_full_population():
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(
        id="out.c-sales.orders",
        name="orders",
        columns=[SourceColumn(name="id")],
        row_count=42,
        data_size_bytes=1024,
        last_import_date="2026-04-14T20:56:16+0200",
        is_alias=False,
    )
    body = _builder(STACK).table_body(bucket, table, synced_at="2026-09-22 10:00 UTC").body
    ext = body["extension"]
    assert ext["kbcTableId"] == "out.c-sales.orders"
    assert ext["kbcBucketId"] == "out.c-sales"
    assert ext["kbcStage"] == "out"
    assert ext["kbcRowsCount"] == "42"
    assert ext["kbcDataSizeBytes"] == "1024"
    assert ext["kbcLastImport"] == "2026-04-14T20:56:16+0200"
    assert ext["kbcIsAlias"] == "false"
    assert ext["kbcSyncedAt"] == "2026-09-22 10:00 UTC"
    assert ext["kbcTableUrl"] == {
        "url": (
            "https://connection.us-east4.gcp.keboola.com/admin/projects/1234/storage/out.c-sales/table/"
            "out.c-sales.orders"
        ),
        "displayText": "Open table",
    }


def test_table_body_extension_filtered_by_available():
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(id="out.c-sales.orders", name="orders", columns=[SourceColumn(name="id")])
    body = _builder().table_body(bucket, table, available={"kbcTableId", "kbcBucketId"}).body
    assert set(body["extension"].keys()) == {"kbcTableId", "kbcBucketId"}


def test_table_body_extension_omits_missing_source_fields():
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(id="out.c-sales.orders", name="orders", columns=[SourceColumn(name="id")])
    # row_count / data_size_bytes / last_import_date default to None -> omitted
    ext = _builder().table_body(bucket, table).body["extension"]
    assert "kbcRowsCount" not in ext
    assert "kbcDataSizeBytes" not in ext
    assert "kbcLastImport" not in ext
    # kbcIsAlias is always present (derived, never None) regardless of the others
    assert ext["kbcIsAlias"] == "false"


def test_table_url_custom_property_falls_back_to_ui_base_without_stack_id():
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(id="out.c-sales.orders", name="orders", columns=[SourceColumn(name="id")])
    ext = _builder().table_body(bucket, table).body["extension"]
    assert ext["kbcTableUrl"]["url"] == f"{UI}/admin/projects/1234/storage/out.c-sales/table/out.c-sales.orders"


# --------------------------------------------------------------------- table_body owners


def test_table_body_owner_set_via_resolver_with_email_shaped_metadata():
    # Synthetic fixture: created_by_metadata never really holds an e-mail (it holds
    # component/config ids), but the extraction path must still work if it ever did.
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(
        id="out.c-sales.orders",
        name="orders",
        columns=[SourceColumn(name="id")],
        created_by_metadata={"KBC.createdBy.component.id": "someone@keboola.com"},
    )
    body = _builder().table_body(bucket, table, owner_resolver=lambda email: "om-user-1").body
    assert body["owners"] == [{"id": "om-user-1", "type": "user"}]


def test_table_body_owner_omitted_with_real_component_config_id_metadata():
    # The REAL shape: component/config ids, never an e-mail -> no owner e-mail found,
    # so the resolver is never called and "owners" is absent (omit, don't fabricate).
    calls = []

    def resolver(email):
        calls.append(email)
        return "om-user-1"

    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(
        id="out.c-sales.orders",
        name="orders",
        columns=[SourceColumn(name="id")],
        created_by_metadata={
            "KBC.createdBy.component.id": "keboola.ex-generic",
            "KBC.createdBy.configuration.id": "123",
        },
    )
    body = _builder().table_body(bucket, table, owner_resolver=resolver).body
    assert "owners" not in body
    assert calls == []


def test_table_body_owner_omitted_without_resolver():
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    table = SourceTable(
        id="out.c-sales.orders",
        name="orders",
        columns=[SourceColumn(name="id")],
        created_by_metadata={"KBC.createdBy.component.id": "someone@keboola.com"},
    )
    body = _builder().table_body(bucket, table).body
    assert "owners" not in body


# --------------------------------------------------------------------- schema_body extension


def test_schema_body_extension_full_population():
    bucket = SourceBucket(
        id="out.c-sales",
        name="c-sales",
        stage="out",
        path="out.c-sales",
        backend="snowflake",
        sharing="organization",
    )
    body = _builder(STACK).schema_body(bucket, synced_at="2026-09-22 10:00 UTC")
    ext = body["extension"]
    assert ext["kbcBucketId"] == "out.c-sales"
    assert ext["kbcStage"] == "out"
    assert ext["kbcBackend"] == "snowflake"
    assert ext["kbcSharing"] == "organization"
    assert ext["kbcSyncedAt"] == "2026-09-22 10:00 UTC"
    assert ext["kbcBucketUrl"] == {
        "url": "https://connection.us-east4.gcp.keboola.com/admin/projects/1234/storage/out.c-sales",
        "displayText": "Open bucket",
    }


def test_schema_body_extension_filtered_by_available():
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    body = _builder().schema_body(bucket, available={"kbcBucketId"})
    assert set(body["extension"].keys()) == {"kbcBucketId"}


def test_schema_body_extension_omits_missing_source_fields():
    bucket = SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")
    ext = _builder().schema_body(bucket)["extension"]
    assert "kbcBackend" not in ext
    assert "kbcSharing" not in ext


# --------------------------------------------------------------------- schema_body owners


def test_schema_body_owner_set_via_resolver_with_email_shaped_metadata():
    bucket = SourceBucket(
        id="out.c-sales",
        name="c-sales",
        stage="out",
        path="out.c-sales",
        created_by_metadata={"KBC.createdBy.component.id": "owner@keboola.com"},
    )
    body = _builder().schema_body(bucket, owner_resolver=lambda email: "om-user-2")
    assert body["owners"] == [{"id": "om-user-2", "type": "user"}]


def test_schema_body_owner_omitted_with_real_component_config_id_metadata():
    calls = []

    def resolver(email):
        calls.append(email)
        return "om-user-2"

    bucket = SourceBucket(
        id="out.c-sales",
        name="c-sales",
        stage="out",
        path="out.c-sales",
        created_by_metadata={
            "KBC.createdBy.component.id": "keboola.ex-generic",
            "KBC.createdBy.configuration.id": "123",
        },
    )
    body = _builder().schema_body(bucket, owner_resolver=resolver)
    assert "owners" not in body
    assert calls == []


def test_schema_body_owner_omitted_without_resolver():
    bucket = SourceBucket(
        id="out.c-sales",
        name="c-sales",
        stage="out",
        path="out.c-sales",
        created_by_metadata={"KBC.createdBy.component.id": "owner@keboola.com"},
    )
    body = _builder().schema_body(bucket)
    assert "owners" not in body


# --------------------------------------------------------------------- database_body


def test_database_body_extension_full_population_and_no_owners_key():
    body = _builder(STACK).database_body(display_name="Acme Project", synced_at="2026-09-22 10:00 UTC")
    ext = body["extension"]
    assert ext["kbcProjectId"] == "1234"
    assert ext["kbcSyncedAt"] == "2026-09-22 10:00 UTC"
    assert ext["kbcProjectUrl"] == {
        "url": "https://connection.us-east4.gcp.keboola.com/admin/projects/1234/storage",
        "displayText": "Open project",
    }
    assert "owners" not in body  # Database (Project) never gets an owner


def test_database_body_extension_filtered_by_available():
    body = _builder().database_body(available={"kbcProjectId"})
    assert set(body["extension"].keys()) == {"kbcProjectId"}
    assert "owners" not in body


def test_database_body_no_extension_when_no_properties_available():
    body = _builder().database_body(available=set())
    assert "extension" not in body
    assert "owners" not in body
