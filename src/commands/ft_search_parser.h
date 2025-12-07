/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_

#include "src/commands/commands.h"
#include "src/query/search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace options {
vmsdk::config::Number &GetMaxKnn();
}  // namespace options

absl::Status PreParseQueryString(query::SearchParameters &parameters);
absl::Status PostParseQueryString(query::SearchParameters &parameters);

enum class SortOrder { kAscending, kDescending };

struct SortByParameter {
  std::string field;
  SortOrder order{SortOrder::kAscending};
  bool enabled{false};
};

//
// Data Unique to the FT.SEARCH command
//
struct SearchCommand : public QueryCommand {
  SearchCommand(int db_num) : QueryCommand(db_num) {}
  absl::Status ParseCommand(vmsdk::ArgsIterator &itr) override;
  void SendReply(ValkeyModuleCtx *ctx,
                 std::deque<indexes::Neighbor> &neighbors) override;
  SortByParameter sortby;
};

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
