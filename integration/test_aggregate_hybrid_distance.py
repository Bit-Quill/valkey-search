import struct

import pytest
from valkey.client import Valkey
from valkey.exceptions import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def _vec(*values) -> bytes:
    return struct.pack(f"<{len(values)}f", *values)


class TestAggregateHybridDistance(ValkeySearchTestCaseBase):
    """
    FT.AGGREGATE must report the KNN distance in the `AS <alias>` column.

    For a hybrid `text=>[KNN ...]` query the alias column must not carry the
    BM25 text relevance (issue #1410); it reports the distance, as the
    equivalent FT.SEARCH and the pure-vector `*=>[KNN ...]` aggregate do. The
    corpus below puts distance order and relevance order in opposition so the
    two values can never be confused.
    """

    # doc key -> (title, x coordinate, squared L2 distance from the query)
    DOCS = [
        ("doc:1", "hello hello hello hello", 0.0, 0.0),
        ("doc:2", "hello world of many other words here to dilute", 1.0, 1.0),
        ("doc:3", "hello hello there", 2.0, 4.0),
        ("doc:4", "hello a b c d e f g h i j k l m n o p", 3.0, 9.0),
    ]

    QUERY_VECTOR = _vec(0.0, 0.0, 0.0)

    def _populate(self, client: Valkey):
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2",
        ) == b"OK"

        for key, title, x, _distance in self.DOCS:
            assert client.execute_command(
                "HSET", key, "title", title, "vec", _vec(x, 0.0, 0.0),
            ) == 2

    @staticmethod
    def _distances_by_key(reply) -> dict:
        """Map __key -> float(mydist) from an FT.AGGREGATE reply."""
        by_key = {}
        for row in reply[1:]:
            fields = dict(zip(row[::2], row[1::2]))
            by_key[fields[b"__key"].decode()] = float(fields[b"mydist"])
        return by_key

    def test_hybrid_text_knn_aggregate_reports_distance(self):
        client: Valkey = self.server.get_new_client()
        self._populate(client)

        expected = {key: distance for key, _t, _x, distance in self.DOCS}

        # Control: a pure-vector aggregate reports the true distances. This is
        # the boundary of the defect -- only the hybrid text shape is wrong.
        pure_vector = client.execute_command(
            "FT.AGGREGATE", "idx", "*=>[KNN 4 @vec $q AS mydist]",
            "LOAD", "2", "@__key", "@mydist",
            "PARAMS", "2", "q", self.QUERY_VECTOR,
            "DIALECT", "2",
        )
        assert self._distances_by_key(pure_vector) == expected

        # Control: FT.SEARCH over the hybrid query reports the true distances.
        search = client.execute_command(
            "FT.SEARCH", "idx", "@title:hello=>[KNN 4 @vec $q AS mydist]",
            "RETURN", "1", "mydist",
            "PARAMS", "2", "q", self.QUERY_VECTOR,
            "DIALECT", "2",
        )
        search_distances = {
            search[i].decode(): float(dict(
                zip(search[i + 1][::2], search[i + 1][1::2])
            )[b"mydist"])
            for i in range(1, len(search), 2)
        }
        assert search_distances == expected

        # The same query through FT.AGGREGATE reports the distance in the
        # alias, not the BM25 text relevance.
        hybrid = client.execute_command(
            "FT.AGGREGATE", "idx", "@title:hello=>[KNN 4 @vec $q AS mydist]",
            "LOAD", "2", "@__key", "@mydist",
            "PARAMS", "2", "q", self.QUERY_VECTOR,
            "DIALECT", "2",
        )
        assert self._distances_by_key(hybrid) == expected

    def test_param_alias_names_the_distance_column(self):
        """`AS $alias` names the column every stage reads, wherever PARAMS is."""
        client: Valkey = self.server.get_new_client()
        self._populate(client)
        expected = {key: distance for key, _t, _x, distance in self.DOCS}

        def aggregate(*stages, params_first=False, params=("alias", "mydist"),
                      query="*=>[KNN 4 @vec $q AS $alias]"):
            params = ["PARAMS", str(2 + len(params)), "q", self.QUERY_VECTOR,
                      *params]
            return client.execute_command(
                "FT.AGGREGATE", "idx", query,
                *(params if params_first else []), *stages,
                *([] if params_first else params), "DIALECT", "2",
            )

        def rows(reply):
            return [dict(zip(row[::2], row[1::2])) for row in reply[1:]]

        for params_first in (False, True):
            run = lambda *stages: aggregate(*stages, params_first=params_first)
            assert self._distances_by_key(run("LOAD", "1", "@__key")) == expected
            assert self._distances_by_key(
                run("LOAD", "2", "@__key", "@mydist")) == expected
            filtered = run("LOAD", "1", "@__key", "FILTER", "@mydist < 2")
            assert self._distances_by_key(filtered) == {"doc:1": 0.0,
                                                        "doc:2": 1.0}
            # A real field read before the alias still resolves to the field.
            mixed = run("APPLY", "upper(@title)", "AS", "t",
                        "FILTER", "@mydist > 2", "LOAD", "1", "@__key")
            assert self._distances_by_key(mixed) == {"doc:3": 4.0,
                                                     "doc:4": 9.0}
            ordered = run("SORTBY", "2", "@mydist", "DESC")
            assert [float(r[b"mydist"]) for r in rows(ordered)] == [
                9.0, 4.0, 1.0, 0.0]
            doubled = run("APPLY", "@mydist * 2", "AS", "dbl")
            assert sorted(float(r[b"dbl"]) for r in rows(doubled)) == [
                0.0, 2.0, 8.0, 18.0]
            grouped = run("GROUPBY", "1", "@mydist",
                          "REDUCE", "COUNT", "0", "AS", "c")
            assert sorted(float(r[b"mydist"]) for r in rows(grouped)) == [
                0.0, 1.0, 4.0, 9.0]
            with pytest.raises(ResponseError, match="`nosuch` does not exist"):
                run("FILTER", "@nosuch < 2")

        # PARAMS as text inside the query, inside an expression and as an
        # APPLY name is not the PARAMS clause.
        reply = aggregate(
            "APPLY", "'PARAMS 2 alias wrong'", "AS", "PARAMS",
            "FILTER", "@mydist < 2", "LOAD", "1", "@__key",
            query="(-@title:PARAMS)=>[KNN 4 @vec $q AS $alias]")
        assert self._distances_by_key(reply) == {"doc:1": 0.0, "doc:2": 1.0}

        with pytest.raises(ResponseError, match="Parameter alias not found"):
            aggregate("LOAD", "1", "@__key", params=())
