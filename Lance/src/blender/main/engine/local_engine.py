from Lance.src.blender.main.base.base_engine import BaseEngine
from Lance.src.blender.main.manager.dataset_manager import DatasetManager


class LocalBaseEngine(BaseEngine):
    def __init__(
        self,
        path,
        primary_key=None,
        default_write_mode="overwrite",
        optimize_config=None,
        index_config=None,
        storage_options=None,
        partition_cols=None,
    ):
        """
        Thin wrapper around DatasetManager to provide a simple engine-like API.

        :param path: Lance dataset path
        :param primary_key: primary key column for upsert/lookup
        :param default_write_mode: default write mode for .write (e.g., "overwrite")
        :param optimize_config: default kwargs passed to optimize()
        :param index_config: default kwargs passed to create_vector_index()
        :param storage_options: storage backend options for Lance
        :param partition_cols: optional partition columns for write/append
        """
        self.manager = DatasetManager(
            path, primary_key, storage_options=storage_options
        )
        self.default_write_mode = default_write_mode
        self.optimize_config = optimize_config or {}
        self.index_config = index_config or {}
        self.partition_cols = partition_cols

    # ---------------- IO ----------------
    def write(self, table, **kwargs):
        """
        Write table to the dataset using the configured default mode ("overwrite" by default).
        You can override with write(table, mode="append") etc.
        """
        mode = kwargs.pop("mode", self.default_write_mode)
        if self.partition_cols:
            kwargs.setdefault("partition_cols", self.partition_cols)
        return self.manager.write(table, mode=mode, **kwargs)

    def append(self, table, **kwargs):
        """Append rows to the dataset."""
        return self.write(table, mode="append", **kwargs)

    def upsert(self, table, **kwargs):
        """
        Upsert rows into the dataset based on the primary key.
        NOTE: Do NOT route through write(..., mode="upsert"). Use manager.upsert directly.
        """
        # Partitioning typically doesn't apply to logical upserts; skip partition_cols here deliberately.
        return self.manager.upsert(table, **kwargs)

    # ---------------- Read/Query ----------------
    def read(self, version=None, columns=None, filters=None, **read_opts):
        """Read as an Arrow Table with optional version/columns/filters/limit/order_by."""
        return self.manager.read(
            version=version, columns=columns, filters=filters, **read_opts
        )

    def search(
        self,
        filters=None,
        columns=None,
        limit=None,
        order_by=None,
        version=None,
    ):
        """Structured search that returns a pandas.DataFrame."""
        return self.manager.search(
            filters=filters,
            columns=columns,
            limit=limit,
            order_by=order_by,
            version=version,
        )

    def register(self, name="ds"):
        """Register the dataset as a SQL table in the internal SessionContext."""
        return self.manager.register(name)

    def query_sql(self, sql):
        """Run SQL against registered tables and return a pandas.DataFrame."""
        return self.manager.query_sql(sql)

    # ---------------- Versioning ----------------
    def list_versions(self):
        """List all dataset versions (metadata)."""
        return self.manager.list_versions()

    def rollback(self, version):
        """
        Restore content from the specified historical version as a new head.
        Returns (restored_from_version, old_head_version, new_head_version).
        """
        return self.manager.rollback(version)

    def forward_to(self, version):
        """
        Semantically the same as rollback, but indicates moving forward to a later version.
        Returns (restored_from_version, old_head_version, new_head_version).
        """
        return self.manager.forward_to(version)

    def preview_version(self, version):
        """
        Return a snapshot (Arrow Table) of a historical version WITHOUT modifying the dataset.
        """
        # Using manager.read to fetch a specific version snapshot.
        return self.manager.read(version=version)

    # ---------------- Maintenance ----------------
    def optimize(self, **opts):
        """Compact small files and clean up old versions."""
        cfg = {**self.optimize_config, **opts}
        return self.manager.optimize(**cfg)

    def create_vector_index(self, column, **opts):
        """Create a vector index on the specified column (e.g., 'embedding')."""
        cfg = {**self.index_config, **opts}
        return self.manager.create_vector_index(column, **cfg)

    def delete_rows(self, filters):
        """Hard delete rows matching the filter (creates a new version)."""
        return self.manager.delete_rows(filters)
