/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_exec.h"

#include <algorithm>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "src/commands/ft_aggregate_parser.h"
#include "vmsdk/src/info.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr

DEV_INTEGER_COUNTER(agg_stats, agg_limit_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_group_by_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_apply_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_reducer_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_sort_by_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_filter_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_filter_input_records);
DEV_INTEGER_COUNTER(agg_stats, agg_filter_output_records);
DEV_INTEGER_COUNTER(agg_stats, agg_limit_input_records);
DEV_INTEGER_COUNTER(agg_stats, agg_limit_output_records);
DEV_INTEGER_COUNTER(agg_stats, agg_group_by_input_records);
DEV_INTEGER_COUNTER(agg_stats, agg_group_by_output_records);
DEV_INTEGER_COUNTER(agg_stats, agg_apply_records);
DEV_INTEGER_COUNTER(agg_stats, agg_sort_by_records);

namespace valkey_search {
namespace aggregate {

expr::Value Attribute::GetValue(expr::Expression::EvalContext &ctx,
                                const expr::Expression::Record &record) const {
  auto rec = reinterpret_cast<const Record &>(record);
  return rec.fields_.at(record_index_);
};

expr::Expression::EvalContext ctx;

std::ostream &operator<<(std::ostream &os, const RecordSet &rs) {
  os << "<RecordSet> " << rs.size() << "\n";
  for (size_t i = 0; i < rs.size(); ++i) {
    os << i << ": ";
    rs[i]->Dump(os, rs.agg_params_);
    os << "\n";
  }
  os << "</RecordSet>\n";
  return os;
}

void Record::Dump(std::ostream &os,
                  const AggregateParameters *agg_params) const {
  os << '[';
  for (size_t i = 0; i < fields_.size(); ++i) {
    if (!fields_[i].IsNil()) {
      os << ' ';
      if (agg_params) {
        os << agg_params->record_info_by_index_[i] << ':';
      } else {
        os << '?' << i << '?';
      }
      os << fields_[i];
    }
  }
  if (!extra_fields_.empty()) {
    os << " Extra:" << extra_fields_.size() << ' ';
    for (const auto &[field, value] : extra_fields_) {
      os << " " << field << ":" << value;
    }
  }
  os << ']';
}

absl::Status Limit::Execute(RecordSet &records) const {
  DBG << "Executing LIMIT with offset: " << offset_ << " and limit: " << limit_
      << "\n";
  agg_limit_stages.Increment();
  agg_limit_input_records.Increment(records.size());
  for (auto i = 0; i < offset_ && !records.empty(); ++i) {
    records.pop_front();
  }
  while (records.size() > limit_) {
    records.pop_back();
  }
  agg_limit_output_records.Increment(records.size());
  return absl::OkStatus();
}

void SetField(Record &record, Attribute &dest, expr::Value value) {
  if (record.fields_.size() <= dest.record_index_) {
    record.fields_.resize(dest.record_index_ + 1);
  }
  record.fields_[dest.record_index_] = value;
}

absl::Status Apply::Execute(RecordSet &records) const {
  DBG << "Executing APPLY with expr: " << *expr_ << "\n";
  agg_apply_stages.Increment();
  agg_apply_records.Increment(records.size());
  for (auto &r : records) {
    SetField(*r, *name_, expr_->Evaluate(ctx, *r));
  }
  return absl::OkStatus();
}

absl::Status Filter::Execute(RecordSet &records) const {
  DBG << "Executing FILTER with expr: " << *expr_ << "\n";
  agg_filter_stages.Increment();
  agg_filter_input_records.Increment(records.size());
  RecordSet filtered(records.agg_params_);
  while (!records.empty()) {
    auto r = records.pop_front();
    auto result = expr_->Evaluate(ctx, *r);
    if (result.IsTrue()) {
      filtered.push_back(std::move(r));
    }
  }
  records.swap(filtered);
  agg_filter_output_records.Increment(records.size());
  return absl::OkStatus();
}

template <typename T>
struct SortFunctor {
  const absl::InlinedVector<SortBy::SortKey, 4> *sortkeys_;
  bool operator()(const T &l, const T &r) const {
    for (auto &sk : *sortkeys_) {
      auto lvalue = sk.expr_->Evaluate(ctx, *l);
      auto rvalue = sk.expr_->Evaluate(ctx, *r);
      auto cmp = expr::Compare(lvalue, rvalue);
      switch (cmp) {
        case expr::Ordering::kEQUAL:
        case expr::Ordering::kUNORDERED:
          continue;
        case expr::Ordering::kLESS:
          return sk.direction_ == SortBy::Direction::kASC;
        case expr::Ordering::kGREATER:
          return sk.direction_ == SortBy::Direction::kDESC;
      }
    }
    return false;
  }
};

absl::Status SortBy::Execute(RecordSet &records) const {
  DBG << "Executing SORTBY with sortkeys: " << sortkeys_.size() << "\n";
  agg_sort_by_stages.Increment();
  agg_sort_by_records.Increment(records.size());
  if (records.size() > max_) {
    // Sadly std::priority_queue can't operate on unique_ptr's. so we need an
    // extra copy
    SortFunctor<Record *> sorter{&sortkeys_};
    std::priority_queue<Record *, std::vector<Record *>, SortFunctor<Record *>>
        heap(sorter);
    for (auto i = 0; i < max_; ++i) {
      heap.push(records.pop_front().release());
    }
    while (!records.empty()) {
      heap.push(records.pop_front().release());
      auto top = RecordPtr(heap.top());  // no leak....
      heap.pop();
    }
    while (!heap.empty()) {
      records.emplace_front(RecordPtr(heap.top()));
      heap.pop();
    }
  } else {
    SortFunctor<RecordPtr> sorter{&sortkeys_};
    std::stable_sort(records.begin(), records.end(), sorter);
  }
  return absl::OkStatus();
}

absl::Status GroupBy::Execute(RecordSet &records) const {
  DBG << "Executing GROUPBY with groups: " << groups_.size()
      << " and reducers: " << reducers_.size() << "\n";

  // struct InstanceArgsPair {
  //   std::unique_ptr<ReducerInstance> instance;
  //   std::vector<ArgVector> args;
  // };
  using InstanceArgsPair =
      std::pair<std::unique_ptr<ReducerInstance>, std::vector<ArgVector>>;
  absl::flat_hash_map<GroupKey, absl::InlinedVector<InstanceArgsPair, 4>>
      groups;
  size_t record_field_count = 0;
  agg_group_by_stages.Increment();
  agg_group_by_input_records.Increment(records.size());
  while (!records.empty()) {
    auto record = records.pop_front();
    if (record_field_count == 0) {
      record_field_count = record->fields_.size();
    } else {
      CHECK(record_field_count == record->fields_.size());
    }
    GroupKey k;
    // todo: How do we handle keys that have a missing attribute in the key??
    // Skip them?
    for (auto &g : groups_) {
      k.keys_.emplace_back(g->GetValue(ctx, *record));
    }
    DBG << "Record: " << *record << " GroupKey: " << k << "\n";
    auto [group_it, inserted] = groups.try_emplace(std::move(k));
    if (inserted) {
      DBG << "Was inserted, now have " << groups.size() << " groups\n";
      for (auto &reducer : reducers_) {
        ArgVector args;
        for (auto &nargs : reducer.args_) {
          args.emplace_back(nargs->Evaluate(ctx, *record));
        }
        group_it->second.emplace_back(std::move(reducer.info_->make_instance()),
                                      std::vector<ArgVector>{});
      }
    }
    for (int i = 0; i < reducers_.size(); ++i) {
      ArgVector args;
      for (auto &nargs : reducers_[i].args_) {
        args.emplace_back(nargs->Evaluate(ctx, *record));
      }
      group_it->second[i].second.push_back(args);
    }
  }
  for (auto &group : groups) {
    DBG << "Making record for group " << group.first << "\n";
    RecordPtr record = std::make_unique<Record>(record_field_count);
    CHECK(groups_.size() == group.first.keys_.size());
    for (auto i = 0; i < groups_.size(); ++i) {
      SetField(*record, *groups_[i], group.first.keys_[i]);
    }
    CHECK(reducers_.size() == group.second.size());
    agg_reducer_stages.Increment(reducers_.size());
    for (auto i = 0; i < reducers_.size(); ++i) {
      auto &[instance, args] = group.second[i];
      instance->ProcessRecords(args);
      SetField(*record, *reducers_[i].output_, instance->GetResult());
    }
    DBG << "Record (" << records.size() << ") is : " << *record << "\n";
    records.push_back(std::move(record));
  }
  agg_group_by_output_records.Increment(records.size());
  return absl::OkStatus();
}

class Count : public GroupBy::ReducerInstance {
  size_t count_{0};
  void ProcessRecords(const std::vector<ArgVector> &all_values) override {
    count_ = all_values.size();
  }
  expr::Value GetResult() const override { return expr::Value(double(count_)); }
};

class Min : public GroupBy::ReducerInstance {
  expr::Value min_;
  void ProcessRecords(const std::vector<ArgVector> &all_values) override {
    for (const auto &values : all_values) {
      if (values[0].IsNil()) {
        continue;
      }
      if (min_.IsNil()) {
        DBG << "First Value Min is " << values[0] << "\n";
        min_ = values[0];
      } else if (min_ > values[0]) {
        DBG << " New Min: " << values[0] << "\n";
        min_ = values[0];
      } else {
        DBG << "Not new Min: " << values[0] << "\n";
      }
    }
  }
  expr::Value GetResult() const override { return min_; }
};

class Max : public GroupBy::ReducerInstance {
  expr::Value max_;
  void ProcessRecords(const std::vector<ArgVector> &all_values) override {
    for (const auto &values : all_values) {
      if (values[0].IsNil()) {
        continue;
      }
      if (max_.IsNil()) {
        max_ = values[0];
      } else if (max_ < values[0]) {
        max_ = values[0];
      }
    }
  }
  expr::Value GetResult() const override { return max_; }
};

class FirstValue : public GroupBy::ReducerInstance {
  expr::Value result_value_;
  expr::Value comparison_value_;

  // Mode is resolved once on the first ProcessRecords call, then reused.
  // Resolving once per group avoids redundant string parsing per record.
  enum class Mode { kUnresolved, kSimple, kSorted, kInvalid };
  Mode mode_ = Mode::kUnresolved;
  bool is_desc_ = false;
  // Tracks whether any record has been stored yet in sorted mode.
  // Needed because a default-constructed comparison_value_ (nil) is
  // indistinguishable from a legitimately nil field value on the first record.
  bool initialized_ = false;

  // Resolves the operating mode from the first record's argument vector.
  // Must be called exactly once per group, before iterating all_values.
  void ResolveMode(const ArgVector &first) {
    size_t nargs = first.size();

    if (nargs == 1) {
      mode_ = Mode::kSimple;
      return;
    }

    // nargs=2 is structurally invalid: a BY clause requires at least a
    // comparison expression, making the minimum sorted-mode count 3.
    // The parser enforces min/max arg counts, so this path is a safeguard.
    // Result: nil.
    if (nargs == 2) {
      mode_ = Mode::kInvalid;
      return;
    }

    // Sorted mode: validate the BY keyword at position [1].
    // first[1] is a StringLiteralExpression result — it is always a string
    // when the parser recognised "BY". A non-string here means the parser
    // fell through to normal field-expression compilation (e.g., the user
    // wrote a field reference where "BY" was expected). Result: nil.
    if (!first[1].IsString()) {
      mode_ = Mode::kInvalid;
      return;
    }
    auto by_upper = expr::FuncUpper(first[1]);
    // The string is present but is not "BY" (e.g., user wrote "NOTBY").
    // Result: nil.
    if (by_upper.AsStringView() != "BY") {
      mode_ = Mode::kInvalid;
      return;
    }

    // Parse sort direction (default: ASC when nargs=3).
    is_desc_ = false;
    if (nargs == 4) {
      // first[3] is a StringLiteralExpression result for ASC/DESC.
      // A non-string means the parser did not recognise the direction token.
      // Result: nil.
      if (!first[3].IsString()) {
        mode_ = Mode::kInvalid;
        return;
      }
      auto order_upper = expr::FuncUpper(first[3]);
      auto order_str = order_upper.AsStringView();
      if (order_str == "DESC") {
        is_desc_ = true;
      } else if (order_str != "ASC") {
        // Direction token is present but is neither "ASC" nor "DESC"
        // (e.g., user wrote "INVALID"). Result: nil.
        mode_ = Mode::kInvalid;
        return;
      }
    }

    mode_ = Mode::kSorted;
  }

  void ProcessRecords(const std::vector<ArgVector> &all_values) override {
    if (all_values.empty()) {
      return;
    }

    // Resolve mode once from the first record's argument layout.
    if (mode_ == Mode::kUnresolved) {
      ResolveMode(all_values[0]);
    }

    // Invalid argument layout — result remains nil. See class-level comment
    // for the list of conditions that trigger this path.
    if (mode_ == Mode::kInvalid) {
      return;
    }

    // Simple mode: return the value from the first record; order is
    // non-deterministic so there is no point scanning further.
    if (mode_ == Mode::kSimple) {
      result_value_ = all_values[0][0];
      return;
    }

    // Sorted mode: scan all records to find the one with the optimal
    // comparison value. Tie-breaking: first-encountered record wins.
    for (const auto &values : all_values) {
      const expr::Value &comparison_val = values[2];

      if (!initialized_) {
        result_value_ = values[0];
        comparison_value_ = comparison_val;
        initialized_ = true;
        continue;
      }

      // Skip records whose comparison value is nil — they cannot win.
      if (comparison_val.IsNil()) {
        continue;
      }

      // If the stored comparison value is nil, any non-nil value wins.
      if (comparison_value_.IsNil()) {
        result_value_ = values[0];
        comparison_value_ = comparison_val;
        continue;
      }

      // Strict < / > preserves first-encountered tie-breaking semantics.
      if (is_desc_ ? (comparison_val > comparison_value_)
                   : (comparison_val < comparison_value_)) {
        result_value_ = values[0];
        comparison_value_ = comparison_val;
      }
    }
  }

  expr::Value GetResult() const override { return result_value_; }
};

class Sum : public GroupBy::ReducerInstance {
  double sum_{0};
  void ProcessRecords(const std::vector<ArgVector> &all_values) override {
    for (const auto &values : all_values) {
      auto val = values[0].AsDouble();
      if (val) {
        sum_ += *val;
      }
    }
  }
  expr::Value GetResult() const override { return expr::Value(sum_); }
};

class Avg : public GroupBy::ReducerInstance {
  double sum_{0};
  size_t count_{0};
  void ProcessRecords(const std::vector<ArgVector> &all_values) override {
    for (const auto &values : all_values) {
      auto val = values[0].AsDouble();
      if (val) {
        sum_ += *val;
        count_++;
      }
    }
  }
  expr::Value GetResult() const override {
    return expr::Value(count_ ? sum_ / count_ : 0.0);
  }
};

class Stddev : public GroupBy::ReducerInstance {
  double sum_{0}, sq_sum_{0};
  size_t count_{0};
  void ProcessRecords(const std::vector<ArgVector> &all_values) override {
    for (const auto &values : all_values) {
      auto val = values[0].AsDouble();
      if (val) {
        sum_ += *val;
        sq_sum_ += (*val) * (*val);
        count_++;
      }
    }
  }
  expr::Value GetResult() const override {
    if (count_ <= 1) {
      return expr::Value(0.0);
    } else {
      double variance = (sq_sum_ - (sum_ * sum_) / count_) / (count_ - 1);
      return expr::Value(std::sqrt(variance));
    }
  }
};

class CountDistinct : public GroupBy::ReducerInstance {
  absl::flat_hash_set<expr::Value> values_;
  void ProcessRecords(const std::vector<ArgVector> &all_values) override {
    for (const auto &values : all_values) {
      if (!values[0].IsNil()) {
        values_.insert(values[0]);
      }
    }
  }
  expr::Value GetResult() const override {
    return expr::Value(double(values_.size()));
  }
};

template <typename T>
std::unique_ptr<GroupBy::ReducerInstance> MakeReducer() {
  return std::unique_ptr<GroupBy::ReducerInstance>(std::make_unique<T>());
}

absl::flat_hash_map<std::string, GroupBy::ReducerInfo> GroupBy::reducerTable{
    {"AVG", GroupBy::ReducerInfo{"AVG", 1, 1, &MakeReducer<Avg>}},
    {"COUNT", GroupBy::ReducerInfo{"COUNT", 0, 0, &MakeReducer<Count>}},
    {"COUNT_DISTINCT",
     GroupBy::ReducerInfo{"COUNT_DISTINCT", 1, 1, &MakeReducer<CountDistinct>}},
    {"FIRST_VALUE",
     GroupBy::ReducerInfo{"FIRST_VALUE", 1, 4, &MakeReducer<FirstValue>}},
    {"MIN", GroupBy::ReducerInfo{"MIN", 1, 1, &MakeReducer<Min>}},
    {"MAX", GroupBy::ReducerInfo{"MAX", 1, 1, &MakeReducer<Max>}},
    {"STDDEV", GroupBy::ReducerInfo{"STDDEV", 1, 1, &MakeReducer<Stddev>}},
    {"SUM", GroupBy::ReducerInfo{"SUM", 1, 1, &MakeReducer<Sum>}},
};

}  // namespace aggregate
}  // namespace valkey_search
