# SORTBY Implementation for FT.SEARCH

This document demonstrates the implementation of the SORTBY option for the FT.SEARCH command in valkey-search.

## Implementation Overview

The SORTBY functionality has been added to allow sorting search results by indexed fields. The implementation includes:

### 1. Parser Changes (`src/commands/ft_search_parser.cc`)
- Added `kSortByParam` constant for "SORTBY" parameter
- Added `ConstructSortByParser()` function to parse SORTBY field and optional ASC/DESC
- Integrated SORTBY parser into `CreateSearchParser()`

### 2. Data Structure Changes (`src/query/search.h`)
- Added `SortOrder` enum with `kAscending` and `kDescending` values
- Added `SortByParameter` struct with field name, order, and enabled flag
- Added `sortby` field to `SearchParameters` struct

### 3. Sorting Logic (`src/query/search.cc`)
- Added `ApplySorting()` function that sorts results based on indexed field values
- Supports numeric and tag field types
- Handles null values by placing them at the end
- Updated main `Search()` function to apply sorting after content retrieval

### 4. Documentation Updates (`COMMANDS.md`)
- Updated FT.SEARCH syntax to include `[SORTBY <field> [ASC|DESC]]`
- Added parameter documentation explaining SORTBY behavior

## Usage Examples

```bash
# Sort by numeric field in ascending order (default)
FT.SEARCH products * SORTBY price

# Sort by numeric field in descending order  
FT.SEARCH products * SORTBY price DESC

# Sort by tag field in ascending order
FT.SEARCH products * SORTBY category ASC

# Combine with other options
FT.SEARCH products "laptop" SORTBY price DESC LIMIT 0 10
```

## Supported Field Types

- **Numeric fields**: Sorted by numeric value
- **Tag fields**: Sorted lexicographically by string value
- **Vector fields**: Not supported for sorting (will maintain original order)

## Error Handling

- Returns error if sort field is not indexed
- Documents with missing values for sort field are placed at end of results
- Unsupported field types maintain original order without error

## Implementation Details

The sorting is applied after search results are retrieved and content is added, ensuring that:
1. Search performance is not impacted during the initial query phase
2. Only the final result set is sorted, which is typically small due to LIMIT
3. Sorting works with both vector and non-vector queries
4. Integration with existing LIMIT and RETURN functionality is maintained

This implementation follows Redis FT.SEARCH SORTBY specification while being optimized for valkey-search's architecture.
