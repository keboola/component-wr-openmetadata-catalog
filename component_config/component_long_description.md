The OpenMetadata Catalog Writer keeps an OpenMetadata catalog in sync with what lives in your Keboola project(s). It reads project metadata through the Keboola Storage, Configurations, and Job Queue APIs and pushes it to a self-hosted OpenMetadata tenant over the OpenMetadata REST API. Each configuration row catalogs one Keboola project; the OpenMetadata connection is shared across all rows.

Every run publishes:

- **Catalog** — databases, schemas (buckets), and tables with columns, native datatypes, primary keys, descriptions, and deep links back into the Keboola UI.
- **Pipelines** — component configurations and flows as OpenMetadata pipelines, including run-history status read from the Job Queue.
- **Lineage** — declared table-level lineage derived from producer input/output mapping, and column-level lineage parsed from transformation SQL.

Writes use a three-way merge that preserves manual edits made in OpenMetadata, and an incremental per-bucket digest skips unchanged buckets between runs. Connect to your OpenMetadata host with a JWT bot token — optionally through an SSH bastion — and catalog one project per configuration row, or every project in an organization with a management token.
