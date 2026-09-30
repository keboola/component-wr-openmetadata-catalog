from mapping import fqn


def test_sanitize_name_replaces_spaces_and_dots():
    assert fqn.sanitize_name("My Bucket.Name") == "My_Bucket_Name"


def test_sanitize_name_collapses_and_strips_underscores():
    assert fqn.sanitize_name("  a   b  ") == "a_b"
    assert fqn.sanitize_name("__weird__") == "weird"


def test_sanitize_name_keeps_dashes():
    assert fqn.sanitize_name("in.c-main") == "in_c-main"


def test_sanitize_name_none_and_empty():
    assert fqn.sanitize_name(None) == "unnamed"
    assert fqn.sanitize_name("   ") == "unnamed"


def test_sanitize_display_name_preserves_original():
    assert fqn.sanitize_display_name("My Bucket.Name") == "My Bucket.Name"
    assert fqn.sanitize_display_name(None) is None


def test_service_display_name_only_for_the_default_service():
    assert fqn.service_display_name(fqn.DEFAULT_SERVICE_NAME) == "Keboola"
    # An explicitly chosen service name is shown as typed: no displayName is sent.
    assert fqn.service_display_name("keboola-eu") is None


def test_table_fqn_deterministic():
    a = fqn.table_fqn("keboola-stack", "Acme Project", "out.c-sales", "orders")
    b = fqn.table_fqn("keboola-stack", "Acme Project", "out.c-sales", "orders")
    assert a == b
    assert a == "keboola-stack.Acme_Project.out_c-sales.orders"


def test_table_fqn_linked_bucket_resolves_to_owning_project():
    # A source table in project A resolves onto the node project A created,
    # even while cataloguing project B.
    node = fqn.table_fqn("svc", "project_a", "out.c-shared", "customers")
    assert node == "svc.project_a.out_c-shared.customers"


def test_pipeline_and_dashboard_fqns_live_under_a_per_project_service():
    # OM's Pipelines/Dashboards trees have no level between service and entity, so the
    # project is the service (by stable id); the entity name is just the config id.
    assert fqn.project_service_name("svc", "4214") == "svc-4214"
    assert fqn.pipeline_name("12345") == "12345"
    assert fqn.pipeline_fqn("svc", "4214", "12345") == "svc-4214.12345"
    assert fqn.dashboard_fqn("svc", "4214", "01app") == "svc-4214.01app"
