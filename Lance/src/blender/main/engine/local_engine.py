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
        self.manager = DatasetManager(
            path, primary_key, storage_options=storage_options
        )
        self.default_write_mode = default_write_mode
        self.optimize_config = optimize_config or {}
        self.index_config = index_config or {}
        self.partition_cols = partition_cols

    def write(self, table, **kwargs):
        mode = kwargs.pop("mode", self.default_write_mode)
        if self.partition_cols:
            kwargs["partition_cols"] = self.partition_cols
        return self.manager.write(table, mode=mode, **kwargs)

    def append(self, table, **kwargs):
        return self.write(table, mode="append", **kwargs)

    def upsert(self, table, **kwargs):
        return self.write(table, mode="upsert", **kwargs)

    def read(self, version=None, columns=None, filters=None, **read_opts):
        return self.manager.read(
            version=version, columns=columns, filters=filters, **read_opts
        )

    def list_versions(self):
        return self.manager.list_versions()

    def rollback(self, version):
        return self.manager.rollback(version)

    def optimize(self, **opts):
        cfg = {**self.optimize_config, **opts}
        return self.manager.optimize(**cfg)

    def create_vector_index(self, column, **opts):
        cfg = {**self.index_config, **opts}
        return self.manager.create_vector_index(column, **cfg)

    def query_sql(self, sql):
        return self.manager.query_sql(sql)
