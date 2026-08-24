from configuration import MergeMode
from merge import (
    ACTION_CREATED,
    ACTION_SKIPPED_DIVERGED,
    ACTION_SKIPPED_UNCHANGED,
    ACTION_UPDATED,
    OUR_LINEAGE_SOURCES,
    OWNED_PIPELINE_FIELDS,
    OWNED_TABLE_FIELDS,
    SnapshotStore,
    ThreeWayMerger,
)

TWM = ThreeWayMerger(MergeMode.THREE_WAY_MERGE)
KAW = ThreeWayMerger(MergeMode.KEBOOLA_ALWAYS_WINS)


def _merge(merger, desired, current, base, owned=("description",)):
    return merger.merge(desired=desired, current=current, base=base, owned_fields=owned)


def test_create_when_no_current():
    d = _merge(TWM, {"description": "x"}, None, None)
    assert d.action == ACTION_CREATED
    assert d.is_create is True
    assert d.snapshot_fields == {"description": "x"}


def test_write_when_om_empty():
    d = _merge(TWM, {"description": "new"}, {"description": ""}, {"description": "old"})
    assert d.action == ACTION_UPDATED
    # key present (empty string) -> RFC-6902 replace
    assert d.patch == [{"op": "replace", "path": "/description", "value": "new"}]


def test_write_when_om_field_absent_uses_add():
    d = _merge(TWM, {"description": "new"}, {"name": "t"}, None)
    assert d.action == ACTION_UPDATED
    assert d.patch == [{"op": "add", "path": "/description", "value": "new"}]


def test_update_when_om_matches_base():
    d = _merge(TWM, {"description": "new"}, {"description": "old"}, {"description": "old"})
    assert d.action == ACTION_UPDATED
    assert d.patch == [{"op": "replace", "path": "/description", "value": "new"}]
    assert d.snapshot_fields["description"] == "new"


def test_noop_when_already_desired():
    d = _merge(TWM, {"description": "same"}, {"description": "same"}, {"description": "same"})
    assert d.action == ACTION_SKIPPED_UNCHANGED
    assert d.patch == []


def test_diverged_left_in_three_way_mode():
    d = _merge(TWM, {"description": "ours"}, {"description": "curator-edited"}, {"description": "old"})
    assert d.action == ACTION_SKIPPED_DIVERGED
    assert d.patch == []
    assert d.diverged_fields == ["description"]
    # base preserved as the new snapshot for the diverged field
    assert d.snapshot_fields["description"] == "old"


def test_diverged_overwritten_in_keboola_wins_mode():
    d = _merge(KAW, {"description": "ours"}, {"description": "curator-edited"}, {"description": "old"})
    assert d.action == ACTION_UPDATED
    assert d.patch == [{"op": "replace", "path": "/description", "value": "ours"}]
    assert d.snapshot_fields["description"] == "ours"


def test_table_and_pipeline_owned_fields_defined():
    assert "columns" in OWNED_TABLE_FIELDS
    assert "tableConstraints" in OWNED_TABLE_FIELDS
    assert "tasks" in OWNED_PIPELINE_FIELDS


def test_pipeline_merge_updates_tasks():
    desired = {"tasks": [{"name": "a"}], "description": "d"}
    current = {"tasks": [], "description": "d"}
    d = TWM.merge(desired=desired, current=current, base={"tasks": []}, owned_fields=OWNED_PIPELINE_FIELDS)
    assert d.action == ACTION_UPDATED
    assert {"op": "replace", "path": "/tasks", "value": [{"name": "a"}]} in d.patch


def test_manual_lineage_source_never_dropped():
    assert "Manual" not in OUR_LINEAGE_SOURCES
    assert set(OUR_LINEAGE_SOURCES) == {"PipelineLineage", "QueryLineage", "ViewLineage"}


def test_snapshot_store_roundtrip():
    store = SnapshotStore()
    store.record("svc.p.b.t", "Table", {"description": "d", "tableType": "Regular"})
    entries = store.entries()
    assert len(entries) == 1
    assert entries[0].entity_fqn == "svc.p.b.t"
    assert entries[0].content_hash

    # simulate reading back from the snapshot table
    store2 = SnapshotStore()
    store2.load_rows(
        [
            {
                "entity_fqn": e.entity_fqn,
                "entity_type": e.entity_type,
                "written_fields_json": e.written_fields_json,
                "content_hash": e.content_hash,
            }
            for e in entries
        ]
    )
    assert store2.base_fields("svc.p.b.t") == {"description": "d", "tableType": "Regular"}
    assert store2.base_fields("missing") is None
