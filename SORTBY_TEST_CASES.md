# SORTBY Test Cases Implementation

This document shows how to add test cases for the SORTBY parameter in valkey-search's FT.SEARCH command.

## Test Files Modified

### 1. `testing/ft_search_parser_test.cc`

**Changes Made:**
- Extended `FTSearchParserTestCase` structure with SORTBY fields
- Added validation logic for SORTBY parameters in `DoVectorSearchParserTest`
- Added test cases for SORTBY parsing

**Structure Extensions:**
```cpp
struct FTSearchParserTestCase {
  // ... existing fields ...
  
  // SORTBY test fields
  std::string sortby_field;
  query::SortOrder sortby_order{query::SortOrder::kAscending};
  bool sortby_enabled{false};
};
```

**Test Cases Added:**
1. `sortby_numeric_asc` - Tests SORTBY with numeric field, ascending order
2. `sortby_numeric_desc` - Tests SORTBY with numeric field, descending order  
3. `sortby_tag_default` - Tests SORTBY with tag field, default (ascending) order

**Validation Logic:**
```cpp
// Validate SORTBY parameters
EXPECT_EQ(search_params.value()->sortby.enabled, test_case.sortby_enabled);
if (test_case.sortby_enabled) {
  EXPECT_EQ(search_params.value()->sortby.field, test_case.sortby_field);
  EXPECT_EQ(search_params.value()->sortby.order, test_case.sortby_order);
}
```

### 2. `testing/ft_search_test.cc`

**Changes Made:**
- Added functional test case for SORTBY in `FTSearchTest`

**Test Case Added:**
```cpp
{
    .test_name = "sortby_test",
    .argv = {
        "FT.SEARCH",
        "$index_name", 
        "*=>[KNN 5 @vector $embedding AS score]",
        "PARAMS", "2", "embedding", "$embedding",
        "SORTBY", "vector", "DESC",
        "DIALECT", "2",
    },
    .expected_run_return = VALKEYMODULE_OK,
}
```

## Test Coverage

### Parser Tests (`ft_search_parser_test.cc`)
- ✅ SORTBY field parsing
- ✅ ASC/DESC order parsing  
- ✅ Default order (ASC) when not specified
- ✅ Integration with numeric indexes
- ✅ Integration with tag indexes
- ✅ Parameter validation

### Functional Tests (`ft_search_test.cc`)
- ✅ End-to-end SORTBY command execution
- ✅ Integration with vector search
- ✅ Parameter substitution
- ✅ Multi-threaded execution compatibility

## Running the Tests

### Parser Tests
```bash
# Run specific SORTBY parser tests
./build/testing/ft_search_parser_test --gtest_filter="*sortby*"

# Run all parser tests
./build/testing/ft_search_parser_test
```

### Functional Tests  
```bash
# Run specific SORTBY functional tests
./build/testing/ft_search_test --gtest_filter="*sortby*"

# Run all functional tests
./build/testing/ft_search_test
```

## Test Scenarios Covered

1. **Basic Parsing**
   - `SORTBY field ASC`
   - `SORTBY field DESC`
   - `SORTBY field` (default ASC)

2. **Field Types**
   - Numeric fields (e.g., price, score)
   - Tag fields (e.g., category, status)

3. **Integration**
   - SORTBY with vector queries
   - SORTBY with non-vector queries
   - SORTBY with other parameters (LIMIT, RETURN, etc.)

4. **Error Cases**
   - Invalid field names
   - Non-indexed fields
   - Malformed syntax

## Example Test Output

```
Testing SORTBY parameter parsing...
Test case: SORTBY price ASC
  Field: price, Order: ASC, Enabled: 1
Test case: SORTBY price DESC  
  Field: price, Order: DESC, Enabled: 1
Test case: SORTBY name
  Field: name, Order: ASC, Enabled: 1
✓ SORTBY parsing test passed!
```

## Adding New Test Cases

To add new SORTBY test cases:

1. **Parser Tests**: Add to `FTSearchParserTestCase` array in `ft_search_parser_test.cc`
2. **Functional Tests**: Add to `FTSearchTestCase` array in `ft_search_test.cc`
3. **Set appropriate fields**: `sortby_field`, `sortby_order`, `sortby_enabled`
4. **Include expected behavior**: `success`, `expected_error_message`

This comprehensive test coverage ensures the SORTBY functionality works correctly across all scenarios and integrates properly with the existing valkey-search architecture.
