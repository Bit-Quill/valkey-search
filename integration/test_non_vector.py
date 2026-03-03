from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import json
import random
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
import time

"""
This file contains tests for non vector (numeric and tag) queries on Hash/JSON documents in Valkey Search - in CME / CMD.
"""
# Constants for numeric and tag queries on Hash/JSON documents.
numeric_tag_index_on_hash = "FT.CREATE products ON HASH PREFIX 1 product: SCHEMA price NUMERIC rating NUMERIC category TAG"
hash_docs = [
    ["HSET", "product:1", "category", "electronics", "name", "Laptop", "price", "999.99", "rating", "4.5", "desc", "Great"],
    ["HSET", "product:2", "category", "electronics", "name", "Tablet", "price", "499.00", "rating", "4.0", "desc", "Good"],
    ["HSET", "product:3", "category", "electronics", "name", "Phone", "price", "299.00", "rating", "3.8", "desc", "Ok"],
    ["HSET", "product:4", "category", "books", "name", "Book", "price", "19.99", "rating", "4.8", "desc", "Excellent"]
]
numeric_query = ["FT.SEARCH", "products", "@price:[300 1000] @rating:[4.4 +inf]"]
expected_hash_key = b'product:1'
expected_hash_value = {
    b'name': b"Laptop",
    b'price': b'999.99',
    b'rating': b'4.5',
    b'desc': b"Great",
    b'category': b"electronics"
}

numeric_tag_index_on_json = "FT.CREATE jsonproducts ON JSON PREFIX 1 jsonproduct: SCHEMA $.category as category TAG $.price as price NUMERIC $.rating as rating NUMERIC"
json_docs = [
    ['JSON.SET', 'jsonproduct:1', '$',
            '{"category":"electronics","name":"Laptop","price":999.99,"rating":4.5,"desc":"Great"}'],
    ['JSON.SET', 'jsonproduct:2', '$',
            '{"category":"electronics","name":"Tablet","price":499.00,"rating":4.0,"desc":"Good"}'],
    ['JSON.SET', 'jsonproduct:3', '$',
            '{"category":"electronics","name":"Phone","price":299.00,"rating":3.8,"desc":"Ok"}'],
    ['JSON.SET', 'jsonproduct:4', '$',
            '{"category":"books","name":"Book","price":19.99,"rating":4.8,"desc":"Excellent"}']
]
numeric_query_on_json = [
    "FT.SEARCH", "jsonproducts",
    "@price:[300 2000] @rating:[4.4 +inf]"
]
expected_numeric_json_key = b'jsonproduct:1'
expected_numeric_json_value = {
    "category": "electronics",
    "name": "Laptop",
    "price": 999.99,
    "rating": 4.5,
    "desc": "Great"
}
numeric_tag_query_on_json = [
    "FT.SEARCH", "jsonproducts",
    "@category:{books} @price:[10 30] @rating:[4.7 +inf]"
]
expected_numeric_tag_json_key = b'jsonproduct:4'
expected_numeric_tag_json_value = {
    "category": "books",
    "name": "Book",
    "price": 19.99,
    "rating": 4.8,
    "desc": "Excellent"
}

categories = ["electronics", "books"]

aggregate_complex_hash_docs = [
    ["HSET", f"product:{i+100}", "price", str(i + 1), "rating", str((i % 100) + 1.0), "category", categories[i % len(categories)]]
    for i in range(1000)
]

aggregate_complex_json_docs = [
    ["JSON.SET", f"jsonproduct:{i+100}", "$",
     json.dumps({"price": i + 1, "rating": (i % 100) + 1.0, "category": categories[i % len(categories)]})]
    for i in range(1000)
]

def create_indexes(client: Valkey):
    """
        Create the necessary indexes for numeric and tag queries on Hash/JSON documents.
    """
    assert client.execute_command(numeric_tag_index_on_hash) == b"OK"
    assert client.execute_command(numeric_tag_index_on_json) == b"OK"

def validate_non_vector_queries(client: Valkey):
    """
        Common validation for numeric and tag queries on Hash/JSON documents.
    """
    # Validate a numeric query on Hash documents.
    result = client.execute_command(*numeric_query)
    assert len(result) == 3
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_hash_key
    document = result[2]
    doc_fields = dict(zip(document[::2], document[1::2]))
    assert doc_fields == expected_hash_value
    # Test NOCONTENT on Hash documents
    result = client.execute_command(*(numeric_query + ["NOCONTENT"]))
    assert len(result) == 2
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_hash_key  # Only key, no content
    # Validate a numeric query on JSON documents.
    result = client.execute_command(*numeric_query_on_json)
    assert len(result) == 3
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_numeric_json_key
    json_data = result[2]
    assert json_data[0] == b'$'  # Check JSON path
    doc = json.loads(json_data[1].decode('utf-8'))
    for key, value in expected_numeric_json_value.items():
        assert key in doc, f"Key '{key}' not found in the document"
        assert doc[key] == value, f"Expected {key}={value}, got {key}={doc[key]}"
    assert set(doc.keys()) == set(expected_numeric_json_value.keys()), "Document contains unexpected fields"
    # Test NOCONTENT on JSON documents
    result = client.execute_command(*(numeric_query_on_json + ["NOCONTENT"]))
    assert len(result) == 2
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_numeric_json_key  # Only key, no content
    # Validate that a tag + numeric query on JSON document works.
    result = client.execute_command(*numeric_tag_query_on_json)
    assert len(result) == 3
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_numeric_tag_json_key
    json_data = result[2]
    assert json_data[0] == b'$'  # Check JSON path
    doc = json.loads(json_data[1].decode('utf-8'))
    for key, value in expected_numeric_tag_json_value.items():
        assert key in doc, f"Key '{key}' not found in the document"
        assert doc[key] == value, f"Expected {key}={value}, got {key}={doc[key]}"
    assert set(doc.keys()) == set(expected_numeric_tag_json_value.keys()), "Document contains unexpected fields"

def validate_limit_queries(client: Valkey):
    """
        Test LIMIT functionality on non-vector queries.
    """
    # Test LIMIT 0 2 - get first 2 results
    result = client.execute_command("FT.SEARCH", "products", "@price:[0 +inf]", "LIMIT", "0", "2")
    assert result[0] == 4  # Total count
    assert len(result) == 5  # 1 count + 2 docs (key + content each)
    # Test LIMIT 1 1 - skip first, get next 1
    result = client.execute_command("FT.SEARCH", "products", "@price:[0 +inf]", "LIMIT", "1", "1")
    assert result[0] == 4  # Total count
    assert len(result) == 3  # 1 count + 1 doc (key + content)
    # Test LIMIT with NOCONTENT
    result = client.execute_command("FT.SEARCH", "products", "@price:[0 +inf]", "LIMIT", "0", "2", "NOCONTENT")
    assert result[0] == 4  # Total count
    assert len(result) == 3  # 1 count + 2 keys only
    # Test LIMIT 0 0 - no results
    result = client.execute_command("FT.SEARCH", "products", "@price:[0 +inf]", "LIMIT", "0", "0")
    assert result[0] == 4  # Total count only
    assert len(result) == 1

def create_bulk_data_standalone(client: Valkey):
    """
        Create bulk data for standalone testing.
    """
    bulk_index = "FT.CREATE bulk_products ON HASH PREFIX 1 bulk_product: SCHEMA price NUMERIC category TAG rating NUMERIC"
    assert client.execute_command(bulk_index) == b"OK"
    # Insert 2500 documents with varying prices and categories
    for i in range(2500):
        price = 10 + (i * 2)  # Prices from 10 to 5008
        category = "cat" + str(i % 10)  # 10 different categories
        rating = 3.0 + (i % 3)  # Ratings 3.0, 4.0, 5.0
        client.execute_command("HSET", f"bulk_product:{i}", "price", str(price), "category", category, "rating", str(rating))

def create_bulk_data_cluster(index_client: Valkey, data_client):
    """
        Create bulk data for cluster testing.
    """
    bulk_index = "FT.CREATE bulk_products ON HASH PREFIX 1 bulk_product: SCHEMA price NUMERIC category TAG rating NUMERIC"
    assert index_client.execute_command(bulk_index) == b"OK"
    # Insert 2500 documents with varying prices and categories
    for i in range(2500):
        price = 10 + (i * 2)  # Prices from 10 to 5008
        category = "cat" + str(i % 10)  # 10 different categories
        rating = 3.0 + (i % 3)  # Ratings 3.0, 4.0, 5.0
        data_client.execute_command("HSET", f"bulk_product:{i}", "price", str(price), "category", category, "rating", str(rating))

def validate_buffer_multiplier_config(client: Valkey):
    """
        Test search result buffer multiplier configuration validation.
    """
    import pytest
    # Test valid positive values
    assert client.execute_command("CONFIG SET search.search-result-buffer-multiplier 2.5") == b"OK"
    assert client.execute_command("CONFIG SET search.search-result-buffer-multiplier 1.2") == b"OK"
    # Test that values outside range are rejected
    with pytest.raises(ResponseError, match=r"Buffer multiplier must be between 1.0 and 1000.0"):
        client.execute_command("CONFIG SET search.search-result-buffer-multiplier -1.0")
    with pytest.raises(ResponseError, match=r"Buffer multiplier must be between 1.0 and 1000.0"):
        client.execute_command("CONFIG SET search.search-result-buffer-multiplier 0.5")
    with pytest.raises(ResponseError, match=r"Buffer multiplier must be between 1.0 and 1000.0"):
        client.execute_command("CONFIG SET search.search-result-buffer-multiplier 1001.0")
    # Test that invalid strings are rejected
    with pytest.raises(ResponseError, match=r"Buffer multiplier must be a valid number"):
        client.execute_command("CONFIG SET search.search-result-buffer-multiplier invalid")

def validate_bulk_limit_queries(client: Valkey):
    """
        Test bulk operations with various LIMIT and OFFSET combinations to validate background limit changes.
    """
    validate_buffer_multiplier_config(client)
    assert client.execute_command("CONFIG SET search.search-result-buffer-multiplier 1.2") == b"OK"
    # Test various limit/offset combinations
    test_cases = [
        (0, 100),    # First 100 results
        (500, 50),   # 50 results starting from position 500
        (1000, 100), # 100 results starting from position 1000
        (2400, 200), # Last 100 results (should only return 100)
        (0, 1000),   # Large batch
        (2500, 10),  # Offset beyond available data
    ]
    for offset, limit in test_cases:
        # Test with content
        result = client.execute_command("FT.SEARCH", "bulk_products", "@price:[0 +inf]", "LIMIT", str(offset), str(limit))
        total_count = result[0]
        assert total_count == 2500  # Always should report total count
        expected_results = min(limit, max(0, 2500 - offset))
        actual_results = (len(result) - 1) // 2  # Subtract count, divide by 2 for key+content pairs
        assert actual_results == expected_results, f"Offset {offset}, Limit {limit}: expected {expected_results}, got {actual_results}"
        # Test with NOCONTENT
        result_nocontent = client.execute_command("FT.SEARCH", "bulk_products", "@price:[0 +inf]", "LIMIT", str(offset), str(limit), "NOCONTENT")
        assert result_nocontent[0] == 2500  # Total count
        actual_keys = len(result_nocontent) - 1  # Subtract count
        assert actual_keys == expected_results, f"NOCONTENT Offset {offset}, Limit {limit}: expected {expected_results}, got {actual_keys}"
    
    # Test filtered queries with limits
    result = client.execute_command("FT.SEARCH", "bulk_products", "@category:{cat0}", "LIMIT", "0", "50")
    assert result[0] == 250  # Should find 250 documents in cat0 (2500/10)
    assert (len(result) - 1) // 2 == 50  # Should return 50 results
    
    # Test with complex filter and offset
    result = client.execute_command("FT.SEARCH", "bulk_products", "@price:[100 500] @rating:[4.0 +inf]", "LIMIT", "2", "3")
    total_count = result[0]
    actual_results = (len(result) - 1) // 2
    assert actual_results <= 3  # Should return at most 3 results
    assert actual_results == min(3, max(0, total_count - 2))  # Respect offset of 2

def validate_aggregate_queries(client: Valkey):
    """
        Test FT.AGGREGATE with numeric and tag queries.
    """
    # Tag filter
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@category:{electronics}",
        "LOAD", "1", "price",
        "APPLY", "@price*2", "AS", "double_price"
    )
    assert result[0] == 3
    # Numeric filter
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[100 500]",
        "LOAD", "1", "category"
    )
    assert result[0] == 2

def validate_aggregate_complex_queries(client: Valkey):
    """
        Test complex FT.AGGREGATE queries with numeric and tag.
    """
    # 1. SORTBY DESC with LIMIT
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[0,1000]",
        "LOAD", "1", "price",
        "SORTBY", "2", "@price", "DESC",
        "LIMIT", "0", "3"
    )
    assert result[0] == 3
    assert result[1][1] == b'1000'
    assert result[2][1] == b'999'
    assert result[3][1] == b'998'

    # 2. SORTBY ASC with LIMIT
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[0 1000]",
        "LOAD", "1", "price",
        "SORTBY", "2", "@price", "ASC",
        "LIMIT", "0", "3"
    )
    assert result[0] == 3
    assert result[1][1] == b'1'
    assert result[2][1] == b'2'
    assert result[3][1] == b'3'

    # 3. SORTBY with MAX
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[0 1000]",
        "LOAD", "1", "price",
        "SORTBY", "2", "@price", "DESC", "MAX", "5"
    )
    assert result[0] == 5
    assert result[1][1] == b'1000'
    assert result[2][1] == b'999'
    assert result[3][1] == b'998'
    assert result[4][1] == b'997'
    assert result[5][1] == b'996'

    # 4. APPLY with arithmetic expression
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[100 100]",
        "LOAD", "1", "price",
        "APPLY", "@price * 2", "AS", "double_price"
    )
    assert result[0] == 1
    assert result[1][1] == b'100'
    assert result[1][3] == b'200'

    # 5. FILTER stage
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "1", "price",
        "FILTER", "@price > 998"
    )
    assert result[0] == 2
    assert {result[1][1], result[2][1]} == {b'999', b'1000'}

    # 6. GROUPBY with COUNT reducer
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "1", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "COUNT", "0", "AS", "count"
    )
    assert result[0] == 2  # electronics and books
    rows = {result[i][1]: result[i][3] for i in range(1, len(result))}
    assert rows[b'electronics'] == b'500'
    assert rows[b'books'] == b'500'

    # 7. GROUPBY with SUM reducer
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 10]",
        "LOAD", "2", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "SUM", "1", "@price", "AS", "total_price"
    )
    assert result[0] == 2
    rows = {result[i][1]: result[i][3] for i in range(1, len(result))}
    # electronics: 1+3+5+7+9 = 25, books: 2+4+6+8+10 = 30
    assert rows[b'electronics'] == b'25'
    assert rows[b'books'] == b'30'

    # 8. GROUPBY with AVG reducer
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 4]",
        "LOAD", "2", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "AVG", "1", "@price", "AS", "avg_price"
    )
    assert result[0] == 2
    rows = {result[i][1]: result[i][3] for i in range(1, len(result))}
    # electronics: avg(1,3) = 2, books: avg(2,4) = 3
    assert rows[b'electronics'] == b'2'
    assert rows[b'books'] == b'3'

    # 9. GROUPBY with MIN and MAX reducers
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "2", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "MIN", "1", "@price", "AS", "min_price",
        "REDUCE", "MAX", "1", "@price", "AS", "max_price"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        if row[b'category'] == b'electronics':
            assert row[b'min_price'] == b'1'
            assert row[b'max_price'] == b'999'
        else:
            assert row[b'min_price'] == b'2'
            assert row[b'max_price'] == b'1000'

    # 10. GROUPBY with COUNT_DISTINCT reducer
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "3", "price", "rating", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "COUNT_DISTINCT", "1", "@rating", "AS", "distinct_ratings"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert row[b'distinct_ratings'] == b'50'

    # 11. GROUPBY with STDDEV reducer
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 4]",
        "LOAD", "2", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "STDDEV", "1", "@price", "AS", "price_stddev"
    )
    assert result[0] == 2

    # 12. GROUPBY + SORTBY + LIMIT pipeline
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "2", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "SUM", "1", "@price", "AS", "total",
        "SORTBY", "2", "@total", "DESC",
        "LIMIT", "0", "1"
    )
    assert result[0] == 1

    # 13. APPLY + FILTER pipeline
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "2", "price", "rating",
        "APPLY", "@price * @rating", "AS", "score",
        "FILTER", "@score > 90000"
    )
    assert result[0] > 0
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert float(row[b'score']) > 90000

    # 14. APPLY + SORTBY + LIMIT pipeline
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "1", "price",
        "APPLY", "@price + 100", "AS", "adjusted",
        "SORTBY", "2", "@adjusted", "ASC",
        "LIMIT", "0", "3"
    )
    assert result[0] == 3
    assert result[1][3] == b'101'
    assert result[2][3] == b'102'
    assert result[3][3] == b'103'

    # 15. FILTER + SORTBY + LIMIT pipeline
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "1", "price",
        "FILTER", "@price >= 990",
        "SORTBY", "2", "@price", "ASC",
        "LIMIT", "0", "5"
    )
    assert result[0] == 5
    assert result[1][1] == b'990'
    assert result[-1][1] == b'994'

    # 16. multiple LIMIT stages
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[0 1000]",
        "LOAD", "1", "price",
        "SORTBY", "2", "@price", "ASC", "MAX", "500",
        "LIMIT", "400", "10",
        "FILTER", "@price >= 406",
        "LIMIT", "0", "5",
        "APPLY", "@price * 10", "AS", "scaled",
        "LIMIT", "0", "1"
    )
    # [1, [b'price', b'406', b'scaled', b'4060']]
    assert result[0] == 1
    assert result[1][1] == b'406'
    assert result[1][3] == b'4060'

    # 17. FIRST_VALUE reducer - simple mode (no BY clause)
    # Note: Simple mode is non-deterministic as it depends on retrieval order
    # We only verify that valid values are returned, not specific values
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "2", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "1", "@price", "AS", "first_price"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert b'category' in row
        assert b'first_price' in row
        first_price = float(row[b'first_price'])
        # Verify it's a valid price from the dataset (1-1000)
        assert 1.0 <= first_price <= 1000.0
        # Verify category matches expected values
        if row[b'category'] == b'electronics':
            # Electronics has odd prices (1, 3, 5, ..., 999)
            assert int(first_price) % 2 == 1
        else:
            # Books has even prices (2, 4, 6, ..., 1000)
            assert int(first_price) % 2 == 0

    # 18. FIRST_VALUE reducer - sorted ASC mode (with BY clause)
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "3", "price", "rating", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@rating", "ASC", "AS", "price_with_min_rating"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert b'category' in row
        assert b'price_with_min_rating' in row
        price_with_min_rating = float(row[b'price_with_min_rating'])
        if row[b'category'] == b'electronics':
            assert price_with_min_rating == 1.0
        else:
            assert price_with_min_rating == 2.0

    # 19. FIRST_VALUE reducer - sorted DESC mode (with BY clause)
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "3", "price", "rating", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@rating", "DESC", "AS", "price_with_max_rating"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert b'category' in row
        assert b'price_with_max_rating' in row
        price_with_max_rating = float(row[b'price_with_max_rating'])
        if row[b'category'] == b'electronics':
            assert price_with_max_rating == 99.0
        else:
            assert price_with_max_rating == 100.0

    # 20. FIRST_VALUE reducer - multiple groups with independent results
    # Note: Simple mode (first_price) is non-deterministic
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "3", "price", "rating", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "1", "@price", "AS", "first_price",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@rating", "ASC", "AS", "price_min_rating",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@rating", "DESC", "AS", "price_max_rating",
        "REDUCE", "COUNT", "0", "AS", "count"
    )
    assert result[0] == 2
    
    electronics_found = False
    books_found = False
    
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert b'category' in row
        assert b'first_price' in row
        assert b'price_min_rating' in row
        assert b'price_max_rating' in row
        assert b'count' in row
        
        category = row[b'category']
        first_price = float(row[b'first_price'])
        price_min_rating = float(row[b'price_min_rating'])
        price_max_rating = float(row[b'price_max_rating'])
        count = int(row[b'count'])
        
        if category == b'electronics':
            electronics_found = True
            # Simple mode is non-deterministic, just verify valid range
            assert 1.0 <= first_price <= 1000.0, f"Electronics first_price out of range: {first_price}"
            assert int(first_price) % 2 == 1, f"Electronics should have odd prices, got {first_price}"
            # Sorted modes are deterministic
            assert price_min_rating == 1.0, f"Electronics price_min_rating should be 1.0, got {price_min_rating}"
            assert price_max_rating == 99.0, f"Electronics price_max_rating should be 99.0, got {price_max_rating}"
            assert count == 500, f"Electronics count should be 500, got {count}"
        elif category == b'books':
            books_found = True
            # Simple mode is non-deterministic, just verify valid range
            assert 1.0 <= first_price <= 1000.0, f"Books first_price out of range: {first_price}"
            assert int(first_price) % 2 == 0, f"Books should have even prices, got {first_price}"
            # Sorted modes are deterministic
            assert price_min_rating == 2.0, f"Books price_min_rating should be 2.0, got {price_min_rating}"
            assert price_max_rating == 100.0, f"Books price_max_rating should be 100.0, got {price_max_rating}"
            assert count == 500, f"Books count should be 500, got {count}"
        else:
            raise AssertionError(f"Unexpected category: {category}")
    
    assert electronics_found, "Electronics group not found in results"
    assert books_found, "Books group not found in results"

    # 21. FIRST_VALUE reducer - numeric and string field type handling
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "2", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@price", "ASC", "AS", "min_price"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        min_price = float(row[b'min_price'])
        if row[b'category'] == b'electronics':
            assert min_price == 1.0, f"Electronics min_price should be 1.0, got {min_price}"
        else:
            assert min_price == 2.0, f"Books min_price should be 2.0, got {min_price}"
    
    client.execute_command(
        "FT.CREATE", "products_with_text", "ON", "HASH", "PREFIX", "1", "textproduct:",
        "SCHEMA", "name", "TEXT", "SORTABLE", "category", "TAG", "price", "NUMERIC"
    )
    
    test_docs = [
        ["HSET", "textproduct:1", "category", "fruits", "name", "Apple", "price", "1.5"],
        ["HSET", "textproduct:2", "category", "fruits", "name", "Banana", "price", "2.0"],
        ["HSET", "textproduct:3", "category", "fruits", "name", "Cherry", "price", "3.0"],
        ["HSET", "textproduct:4", "category", "vegetables", "name", "Zucchini", "price", "2.5"],
        ["HSET", "textproduct:5", "category", "vegetables", "name", "Carrot", "price", "1.0"],
        ["HSET", "textproduct:6", "category", "vegetables", "name", "Beet", "price", "1.8"],
    ]
    
    for doc in test_docs:
        client.execute_command(*doc)
    
    time.sleep(0.1)
    
    result = client.execute_command(
        "FT.AGGREGATE", "products_with_text", "*",
        "LOAD", "3", "name", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@name", "ASC", "AS", "price_of_first_name"
    )
    assert result[0] == 2
    
    fruits_found = False
    vegetables_found = False
    
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        category = row[b'category']
        price_of_first_name = float(row[b'price_of_first_name'])
        
        if category == b'fruits':
            fruits_found = True
            assert price_of_first_name == 1.5, f"Fruits price_of_first_name should be 1.5 (Apple), got {price_of_first_name}"
        elif category == b'vegetables':
            vegetables_found = True
            assert price_of_first_name == 1.8, f"Vegetables price_of_first_name should be 1.8 (Beet), got {price_of_first_name}"
        else:
            raise AssertionError(f"Unexpected category: {category}")
    
    assert fruits_found, "Fruits group not found in results"
    assert vegetables_found, "Vegetables group not found in results"
    
    result = client.execute_command(
        "FT.AGGREGATE", "products_with_text", "*",
        "LOAD", "3", "name", "price", "category",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@name", "DESC", "AS", "price_of_last_name"
    )
    assert result[0] == 2
    
    fruits_found = False
    vegetables_found = False
    
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        category = row[b'category']
        price_of_last_name = float(row[b'price_of_last_name'])
        
        if category == b'fruits':
            fruits_found = True
            assert price_of_last_name == 3.0, f"Fruits price_of_last_name should be 3.0 (Cherry), got {price_of_last_name}"
        elif category == b'vegetables':
            vegetables_found = True
            assert price_of_last_name == 2.5, f"Vegetables price_of_last_name should be 2.5 (Zucchini), got {price_of_last_name}"
        else:
            raise AssertionError(f"Unexpected category: {category}")
    
    assert fruits_found, "Fruits group not found in results"
    assert vegetables_found, "Vegetables group not found in results"
    
    client.execute_command("FT.DROPINDEX", "products_with_text", "DD")

    # 22. FIRST_VALUE reducer - error handling
    import pytest
    
    with pytest.raises(ResponseError):
        client.execute_command(
            "FT.AGGREGATE", "products", "@price:[1 1000]",
            "LOAD", "1", "price",
            "GROUPBY", "1", "@category",
            "REDUCE", "FIRST_VALUE", "0", "AS", "result"
        )
    
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "1", "price",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "2", "@price", "@rating", "AS", "result"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert row[b'result'] == b''
    
    with pytest.raises(ResponseError):
        client.execute_command(
            "FT.AGGREGATE", "products", "@price:[1 1000]",
            "LOAD", "2", "price", "rating",
            "GROUPBY", "1", "@category",
            "REDUCE", "FIRST_VALUE", "5", "@price", "BY", "@rating", "ASC", "extra", "AS", "result"
        )
    
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "2", "price", "rating",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "4", "@price", "WITH", "@rating", "ASC", "AS", "result"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert row[b'result'] == b''
    
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 1000]",
        "LOAD", "2", "price", "rating",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@rating", "UP", "AS", "result"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert row[b'result'] == b''
    
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[1 10]",
        "LOAD", "2", "price", "rating",
        "GROUPBY", "1", "@category",
        "REDUCE", "FIRST_VALUE", "4", "@price", "by", "@rating", "asc", "AS", "result_lower",
        "REDUCE", "FIRST_VALUE", "4", "@price", "BY", "@rating", "ASC", "AS", "result_upper",
        "REDUCE", "FIRST_VALUE", "4", "@price", "By", "@rating", "Asc", "AS", "result_mixed"
    )
    assert result[0] == 2
    for i in range(1, len(result)):
        row = dict(zip(result[i][::2], result[i][1::2]))
        assert row[b'result_lower'] == row[b'result_upper']
        assert row[b'result_upper'] == row[b'result_mixed']
        assert row[b'result_lower'] != b''

class TestNonVector(ValkeySearchTestCaseBase):

    def test_basic(self):
        """
            Test a numeric query and tag + numeric query on Hash/JSON docs in Valkey Search CMD.
        """
        # Create indexes:
        client: Valkey = self.server.get_new_client()
        create_indexes(client)
        # Data population:
        for doc in hash_docs:
            assert client.execute_command(*doc) == 5
        for doc in json_docs:
            assert client.execute_command(*doc) == b"OK"
        # Validation of numeric and tag queries.
        validate_non_vector_queries(client)
        # Test LIMIT functionality
        validate_limit_queries(client)
        # Test AGGREGATE functionality
        validate_aggregate_queries(client)

    def test_aggregate_complex(self):
        client: Valkey = self.server.get_new_client()
        create_indexes(client)
        for doc in aggregate_complex_hash_docs:
            assert client.execute_command(*doc) == 3
        for doc in json_docs:
            assert client.execute_command(*doc) == b"OK"
        validate_aggregate_complex_queries(client)

    def test_uningested_multi_field(self):
        """
            Test out the case where some index fields are not ingested. But other numeric and tag fields are.
        """
        client: Valkey = self.server.get_new_client()
        # Create multi-field index with TEXT, NUMERIC, and TAG fields
        multi_field_index = "FT.CREATE multifield_products ON HASH PREFIX 1 multifield_product: SCHEMA price NUMERIC rating NUMERIC new_field1 NUMERIC category TAG new_field2 TAG"
        assert client.execute_command(multi_field_index) == b"OK"
        # Data population with multifield_ prefix
        for doc in hash_docs:
            assert client.execute_command(*["HSET", "multifield_" + doc[1]] + doc[2:]) == 5
        # Test numeric query
        result = client.execute_command("FT.SEARCH", "multifield_products", "@price:[300 1000] @rating:[4.4 +inf]")
        assert result[0] == 1
        assert result[1] == b'multifield_product:1'
        # Test tag + numeric query
        result = client.execute_command("FT.SEARCH", "multifield_products", "@category:{books} @price:[10 30] @rating:[4.7 +inf]")
        assert result[0] == 1
        assert result[1] == b'multifield_product:4'

    def test_bulk_limit_background_changes(self):
        """
            Test bulk operations with various LIMIT and OFFSET combinations to validate background limit changes.
        """
        client: Valkey = self.server.get_new_client()
        create_bulk_data_standalone(client)
        validate_bulk_limit_queries(client)

class TestNonVectorCluster(ValkeySearchClusterTestCase):

    def test_non_vector_cluster(self):
        """
            Test a numeric query and tag + numeric query on Hash/JSON docs in Valkey Search CME.
        """
        # Create indexes:
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        create_indexes(client)
        # Data population:
        for doc in hash_docs:
            assert cluster_client.execute_command(*doc) == 5
        for doc in json_docs:
            assert cluster_client.execute_command(*doc) == b"OK"
        create_bulk_data_cluster(client, cluster_client)
        time.sleep(1)
        # Validation of numeric and tag queries.
        validate_non_vector_queries(client)
        # Test LIMIT functionality
        validate_limit_queries(client)
        # Test bulk limit functionality
        validate_bulk_limit_queries(client)
    
    def test_aggregate_complex_cluster(self):
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        create_indexes(client)
        for doc in aggregate_complex_hash_docs:
            assert cluster_client.execute_command(*doc) == 3
        for doc in aggregate_complex_json_docs:
            assert cluster_client.execute_command(*doc) == b"OK"
        validate_aggregate_complex_queries(cluster_client)
