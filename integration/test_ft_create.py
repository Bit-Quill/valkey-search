from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
import valkey
import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
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


SKIP_LOG = "is above search.max-index-name-length (4)"


def set_name_limit(client, limit):
    client.execute_command(
        "CONFIG", "SET", "search.max-index-name-length", str(limit)
    )


def set_panic_on_propagation_error(client):
    client.execute_command(
        "CONFIG", "SET", "propagation-error-behavior", "panic"
    )


def create_tag_index(client, name):
    assert client.execute_command(
        "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", "d:",
        "SCHEMA", "t", "TAG"
    ) == b"OK"


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

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_index_name_length_limit_skips_cluster_metadata(self):
        """Nodes with a lower limit skip the long index from metadata,
        FT.INTERNAL_UPDATE and FLUSHALL, and keep applying later changes."""
        n0 = self.new_client_for_primary(0)
        n1 = self.new_client_for_primary(1)
        n2 = self.new_client_for_primary(2)
        replica = self.get_replication_group(0).get_replica_connection(0)
        set_name_limit(n1, 4)
        set_name_limit(replica, 4)
        with pytest.raises(valkey.exceptions.ResponseError):
            create_tag_index(n0, "longname")
        create_tag_index(n0, "ok")
        for client in (n1, replica):
            waiters.wait_for_true(
                lambda c=client: c.execute_command("FT._LIST") == [b"ok"]
            )
        assert self.get_replication_group(1).primary.does_logfile_contains(
            SKIP_LOG
        )
        assert self.get_replication_group(0).replicas[0].does_logfile_contains(
            SKIP_LOG
        )

        set_name_limit(n0, 4)
        n0.execute_command("FLUSHALL")
        assert n0.execute_command("FT._LIST") == [b"ok"]
        assert self.get_replication_group(0).primary.does_logfile_contains(
            SKIP_LOG
        )

        assert n2.execute_command("FT.DROPINDEX", "longname") == b"OK"
        for client in (n0, n1, replica):
            set_name_limit(client, 1024)
        create_tag_index(n0, "longname")


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

    def test_index_name_length_limit(self):
        """The limit counts bytes, so 4 two-byte characters fit in 8."""
        client = self.server.get_new_client()
        set_name_limit(client, 8)
        assert client.execute_command(
            "FT.CREATE", "éééé", "SCHEMA", "t", "TAG"
        ) == b"OK"
        with pytest.raises(valkey.exceptions.ResponseError) as e:
            client.execute_command("FT.CREATE", "ééééa", "SCHEMA", "t", "TAG")
        assert "An index name can have a maximum length of 8." in str(e.value)

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_index_name_length_limit_skips_on_replica(self):
        """A replica with a lower limit skips the long index, live and on
        full sync, and keeps applying the stream."""
        primary = self.get_primary_connection()
        replica = self.get_replica_connection(0)
        set_name_limit(replica, 4)
        create_tag_index(primary, "longname")
        create_tag_index(primary, "ok")
        primary.execute_command("WAIT", "1", "0")
        assert replica.execute_command("FT._LIST") == [b"ok"]
        assert self.rg.replicas[0].does_logfile_contains(SKIP_LOG)

        node = self.start_new_server(is_primary=False)
        set_name_limit(node.client, 4)
        self.rg.replicas.append(node)
        self.nodes.append(node)
        self.rg.setup_replications_cmd()
        waiters.wait_for_true(
            lambda: node.client.execute_command("FT._LIST") == [b"ok"]
        )
        assert node.does_logfile_contains(SKIP_LOG)

        for client in (replica, node.client):
            set_panic_on_propagation_error(client)
        primary.execute_command("FT.DROPINDEX", "longname")
        primary.execute_command("WAIT", "2", "5000")
        for client in (replica, node.client):
            assert client.execute_command("FT._LIST") == [b"ok"]

    def test_index_name_length_limit_skips_rdb_load(self):
        """Restart and DEBUG RELOAD skip the long index and load the rest."""
        client = self.server.get_new_client()
        create_tag_index(client, "longname")
        create_tag_index(client, "ok")
        client.execute_command("HSET", "d:1", "t", "x")
        set_name_limit(client, 4)
        client.execute_command("CONFIG", "REWRITE")
        client.execute_command("SAVE")
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()
        assert client.execute_command("FT._LIST") == [b"ok"]
        assert self.rg.primary.does_logfile_contains(SKIP_LOG)

        set_name_limit(client, 1024)
        create_tag_index(client, "longname")
        set_name_limit(client, 4)
        client.execute_command("DEBUG", "RELOAD")
        assert client.execute_command("FT._LIST") == [b"ok"]
        waiters.wait_for_true(
            lambda: client.execute_command("FT.SEARCH", "ok", "@t:{x}")[0]
            == 1
        )

    def test_index_name_length_limit_skips_aof_replay(self):
        """AOF replay skips the long index and replays the rest."""
        client = self.server.get_new_client()
        client.execute_command("CONFIG", "SET", "appendonly", "yes")
        waiters.wait_for_true(
            lambda: client.info("persistence")["aof_rewrite_in_progress"] == 0
            and client.info("persistence")["aof_rewrite_scheduled"] == 0
        )
        create_tag_index(client, "longname")
        create_tag_index(client, "ok")
        client.execute_command("FT.DROPINDEX", "longname")
        set_name_limit(client, 4)
        set_panic_on_propagation_error(client)
        client.execute_command("CONFIG", "REWRITE")
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()
        assert client.execute_command("FT._LIST") == [b"ok"]
        assert self.rg.primary.does_logfile_contains(SKIP_LOG)

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
