from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
import valkey
import pytest
from valkeytestframework.conftest import resource_port_tracker
import os

INDEX_NAME = "test_replica_index"
CREATE_INDEX_SCHEMA = [
    "FT.CREATE",
    INDEX_NAME,
    "ON",
    "HASH",
    "SCHEMA",
    "embedding",
    "VECTOR",
    "HNSW",
    "12",
    "m",
    "10",
    "TYPE",
    "FLOAT32",
    "DIM",
    "100",
    "DISTANCE_METRIC",
    "COSINE",
    "EF_CONSTRUCTION",
    "5",
    "EF_RUNTIME",
    "10",
]


def ft_create_fails_on_replica(primary_client, replica_client):
    # Test that FT.CREATE fails on replica
    with pytest.raises(valkey.exceptions.ResponseError) as e:
        replica_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert "You can't write against a read only replica" in str(e.value)

    # Verify that FT.CREATE works on master
    result = primary_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert result == b"OK"
    assert primary_client.execute_command("FT._LIST") == [
        INDEX_NAME.encode("utf-8")
    ]

    # Test again that FT.CREATE fails on replica
    with pytest.raises(valkey.exceptions.ResponseError) as e:
        replica_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert "You can't write against a read only replica" in str(e.value)


class TestSearchFTCreateCME(ValkeySearchClusterTestCase):
    """
    Test suite for FT.CREATE search command. We expect that
    clients will not be able to create index on the replica.
    """

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_ft_create_fails_on_replica_cme(self):
        """Test that FT.CREATE fails when executed on a replica."""
        rg = self.get_replication_group(0)
        ft_create_fails_on_replica(
            rg.get_primary_connection(), rg.get_replica_connection(0)
        )


class TestSearchFTCreateCMD(ValkeySearchTestCaseBase):
    """
    Test suite for FT.CREATE search command. We expect that
    clients will not be able to create index on the replica.
    """

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_ft_create_fails_on_replica_cmd(self):
        """Test that FT.CREATE fails when executed on a replica."""
        ft_create_fails_on_replica(
            self.get_primary_connection(),
            self.get_replica_connection(0),
        )

    def test_same_field_as_text_and_tag(self):
        """Regression test for issue #1195.

        The same source field may be indexed multiple times under distinct
        aliases (e.g. once as TEXT and once as TAG). This used to be rejected
        with "Duplicate field in schema". Validate that the index is created
        and that both aliases are independently queryable.
        """
        client = self.server.get_new_client()

        # The exact command from the issue report.
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "blog:post:",
            "SCHEMA",
            "sku", "AS", "sku_text", "TEXT",
            "sku", "AS", "sku_tag", "TAG", "SORTABLE",
        ) == b"OK"

        assert client.execute_command(
            "HSET", "blog:post:1", "sku", "widget"
        ) == 1

        # The TAG alias matches the whole value.
        result = client.execute_command(
            "FT.SEARCH", "idx", "@sku_tag:{widget}", "NOCONTENT"
        )
        assert result[0] == 1
        assert result[1] == b"blog:post:1"

        # The TEXT alias matches the tokenized value.
        result = client.execute_command(
            "FT.SEARCH", "idx", "@sku_text:widget", "NOCONTENT"
        )
        assert result[0] == 1
        assert result[1] == b"blog:post:1"

    def test_duplicate_alias_still_rejected(self):
        """Two attributes sharing the same alias must still be rejected."""
        client = self.server.get_new_client()
        with pytest.raises(valkey.exceptions.ResponseError) as e:
            client.execute_command(
                "FT.CREATE", "iddup", "ON", "HASH",
                "SCHEMA",
                "sku", "AS", "dup", "TEXT",
                "price", "AS", "dup", "TAG",
            )
        assert "Duplicate field in schema - dup" in str(e.value)

    def test_nohl_and_sortable_unf_accepted(self):
        """Regression test for issues #1370 and #1371.

        NOHL and SORTABLE UNF are RediSearch compatibility no-ops: highlighting
        is not implemented, and SORTBY already compares the raw field value.
        """
        client = self.server.get_new_client()

        assert client.execute_command(
            "FT.CREATE", "idxcompat", "ON", "HASH", "PREFIX", "1", "p:",
            "NOHL",
            "SCHEMA",
            "sku", "TAG", "SORTABLE", "UNF",
            "price", "NUMERIC", "SORTABLE", "UNF",
        ) == b"OK"

        assert client.execute_command("HSET", "p:1", "sku", "a", "price", "2") == 2
        assert client.execute_command("HSET", "p:2", "sku", "B", "price", "1") == 2

        result = client.execute_command(
            "FT.SEARCH", "idxcompat", "@sku:{B}", "NOCONTENT"
        )
        assert result[0] == 1
        assert result[1] == b"p:2"

        # UNF ordering is raw bytes, so uppercase 'B' precedes lowercase 'a'.
        result = client.execute_command(
            "FT.SEARCH", "idxcompat", "@price:[0 10]",
            "SORTBY", "sku", "ASC", "NOCONTENT",
        )
        assert result[0] == 2
        assert result[1] == b"p:2"
        assert result[2] == b"p:1"

    def test_unf_without_sortable_rejected(self):
        """A bare UNF is not valid RediSearch syntax and must still fail."""
        client = self.server.get_new_client()
        with pytest.raises(valkey.exceptions.ResponseError) as e:
            client.execute_command(
                "FT.CREATE", "idxunf", "ON", "HASH",
                "SCHEMA",
                "sku", "TAG", "UNF",
            )
        assert "UNF" in str(e.value)


    def test_json_identifier_without_dollar_rejected(self):
        """Regression test for issue #737.

        A JSON attribute identifier must be a JSONPath starting with `$`.
        Other identifiers, including valkey-json legacy paths, don't index
        the value, so FT.CREATE must reject them for every field type.
        """
        client = self.server.get_new_client()
        field_types = [
            ["TAG"], ["NUMERIC"], ["TEXT"],
            ["VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
             "DISTANCE_METRIC", "L2"],
        ]
        for identifier in ["y", ".y", "nested.y"]:
            for field_type in field_types:
                with pytest.raises(valkey.exceptions.ResponseError) as e:
                    client.execute_command(
                        "FT.CREATE", "idxjson", "ON", "JSON",
                        "SCHEMA", identifier, "AS", "f", *field_type,
                    )
                assert "must start with `$`" in str(e.value), (
                    identifier, field_type)
        # The exact command from the issue, with no AS clause.
        with pytest.raises(valkey.exceptions.ResponseError) as e:
            client.execute_command(
                "FT.CREATE", "x", "ON", "JSON", "SCHEMA", "y", "TAG",
            )
        assert "must start with `$`" in str(e.value)
        # A `$` path still needs AS: the path is not a valid field name.
        with pytest.raises(valkey.exceptions.ResponseError) as e:
            client.execute_command(
                "FT.CREATE", "x", "ON", "JSON", "SCHEMA", "$.y", "TAG",
            )
        assert "contains invalid character" in str(e.value)
        assert client.execute_command("FT._LIST") == []


class TestFTCreateJsonAof(ValkeySearchTestCaseBase):
    """FT.CREATE rejection of non-`$` JSON paths must not break AOF replay."""

    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["appendonly"] = "yes"
        args["appendfsync"] = "always"
        return args

    def test_aof_replay_restores_json_index_without_dollar(self):
        """An AOF written before issue #737's check still restores the index.

        The FT.CREATE is appended to the AOF directly, as an older version
        would have logged it. On restart, replay must not reject it.
        """
        client = self.server.get_new_client()
        aof_dir = os.path.join(client.config_get("dir")["dir"],
                               client.config_get("appenddirname")["appenddirname"])
        manifest = next(f for f in os.listdir(aof_dir)
                        if f.endswith(".manifest"))
        with open(os.path.join(aof_dir, manifest)) as f:
            incr = [line.split()[1] for line in f if "type i" in line][-1]
        cmd = ["FT.CREATE", "x", "ON", "JSON", "SCHEMA", "y", "TAG"]
        resp = f"*{len(cmd)}\r\n" + "".join(
            f"${len(a)}\r\n{a}\r\n" for a in cmd)
        with open(os.path.join(aof_dir, incr), "a") as f:
            f.write(resp)

        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()
        assert client.execute_command("FT._LIST") == [b"x"]
