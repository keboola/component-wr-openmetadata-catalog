from mapping.dashboard_builder import DashboardBuilder

UI = "https://connection.keboola.com"
STACK = "connection.us-east4.gcp.keboola.com"


def _builder():
    return DashboardBuilder("keboola-stack", "Acme_Project", "1234", UI, STACK)


def test_dashboard_service_body():
    svc = _builder().dashboard_service_body()
    assert svc["serviceType"] == "CustomDashboard"
    assert svc["name"] == "keboola-stack"


def test_is_data_app():
    b = _builder()
    assert b.is_data_app("keboola.data-apps") is True
    assert b.is_data_app("keboola.snowflake-transformation") is False


def test_build_dashboard_structured_extension():
    config = {
        "id": "01kp6nkwmg6j45ha4w2z5b99k2",
        "name": "Transition Test App",
        "description": "A demo app",
        "created": "2026-04-14T20:56:15+0200",
        "currentVersion": {
            "created": "2026-04-14T20:56:16+0200",
            "creatorToken": {"description": "jakub.smagin@keboola.com"},
        },
        "configuration": {"parameters": {"id": "43022494", "dataApp": {"slug": "transition-test-app"}}},
    }
    body = _builder().build_dashboard(
        config, app_states={"01kp6nkwmg6j45ha4w2z5b99k2": "running"}, synced_at="2026-09-22 10:00 UTC"
    ).body
    assert body["displayName"] == "Transition Test App"
    # details go to structured custom properties, description stays the app's own text
    assert body["description"] == "A demo app"
    # deployed app URL from stack_id (public host, correct even on-platform)
    assert body["sourceUrl"] == "https://transition-test-app-43022494.hub.us-east4.gcp.keboola.com"
    ext = body["extension"]
    assert ext["kbcAppId"] == "43022494"
    assert ext["kbcStatus"] == "ACTIVE"  # "running" -> UI Status label
    assert ext["kbcSyncedAt"] == "2026-09-22 10:00 UTC"  # snapshot time (status is as of this)
    assert ext["kbcOwner"] == "jakub.smagin@keboola.com"  # last-change author
    assert ext["kbcLastChange"] == "2026-04-14 20:56"
    assert ext["kbcAppUrl"] == {
        "url": "https://transition-test-app-43022494.hub.us-east4.gcp.keboola.com",
        "displayText": "Open app",
    }
    # config URL uses the public connection host, not the internal ui_base
    assert (
        ext["kbcConfigUrl"]["url"]
        == "https://connection.us-east4.gcp.keboola.com/admin/projects/1234/data-apps/01kp6nkwmg6j45ha4w2z5b99k2"
    )


def test_extension_filtered_by_available_properties():
    config = {"id": "9", "configuration": {"parameters": {"id": "7", "dataApp": {"slug": "s"}}}}
    body = _builder().build_dashboard(config, available_properties={"kbcAppId"}).body
    assert set(body["extension"].keys()) == {"kbcAppId"}


def test_no_extension_when_no_properties_available():
    body = _builder().build_dashboard({"id": "9", "name": "App"}, available_properties=set()).body
    assert "extension" not in body


def test_app_url_falls_back_to_config_url_without_stack_id():
    b = DashboardBuilder("keboola-stack", "Acme_Project", "1234", UI)  # no stack_id
    body = b.build_dashboard({"id": "5", "configuration": {"parameters": {"id": "7", "dataApp": {"slug": "s"}}}}).body
    assert body["sourceUrl"].endswith("/data-apps/5")  # config URL via ui_base fallback


def test_owner_email_extracted_from_wrapped_description():
    config = {"id": "1", "currentVersion": {"creatorToken": {"description": "kbagent-cli [martin.struzsky@keboola.com]"}}}
    ext = _builder().build_dashboard(config).body["extension"]
    assert ext["kbcOwner"] == "martin.struzsky@keboola.com"


def test_owner_omitted_when_no_email():
    config = {"id": "1", "currentVersion": {"creatorToken": {"description": "some token label"}}}
    ext = _builder().build_dashboard(config).body.get("extension") or {}
    assert "kbcOwner" not in ext


def test_status_labels():
    b = _builder()

    def status(state):
        return b.build_dashboard({"id": "x"}, app_states={"x": state}).body["extension"].get("kbcStatus")

    assert status("created") == "NOT DEPLOYED"
    assert status("stopped") == "STOPPED"
    assert status("running") == "ACTIVE"
    assert status("weird") == "WEIRD"  # fallback: raw state upper-cased


def test_description_always_emitted_empty_when_absent():
    # empty string (not omitted) so a re-run overwrites any stale description
    assert _builder().build_dashboard({"id": "9", "name": "App"}).body["description"] == ""
