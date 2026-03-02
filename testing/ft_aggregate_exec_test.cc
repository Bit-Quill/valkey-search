/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_exec.h"

#include <algorithm>
#include <random>

#include <algorithm>
#include <random>

#include "gtest/gtest.h"
#include "src/commands/ft_aggregate_parser.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace aggregate {

struct FakeIndexInterface : public IndexInterface {
  std::map<std::string, indexes::IndexerType> fields_;
  absl::StatusOr<indexes::IndexerType> GetFieldType(
      absl::string_view fld_name) const override {
    std::string field_name(fld_name);
    std::cout << "Fake make reference " << field_name << "\n";
    auto itr = fields_.find(field_name);
    if (itr == fields_.end()) {
      return absl::NotFoundError(
          absl::StrCat("Unknown field ", fld_name, " in index."));
    } else {
      return itr->second;
    }
  }
  absl::StatusOr<std::string> GetIdentifier(
      absl::string_view alias) const override {
    std::cout << "Fake get identifier for " << alias << "\n";
    VMSDK_ASSIGN_OR_RETURN([[maybe_unused]] auto type, GetFieldType(alias));
    return std::string(alias);
  }
  absl::StatusOr<std::string> GetAlias(
      absl::string_view identifier) const override {
    std::cout << "Fake get alias for " << identifier << "\n";
    auto itr = fields_.find(std::string(identifier));
    if (itr == fields_.end()) {
      return absl::NotFoundError(
          absl::StrCat("Unknown identifier ", identifier, " in index."));
    } else {
      return itr->first;
    }
  }
};

static std::unique_ptr<Record> RecordNOfM(size_t n, size_t m) {
  auto rec = std::make_unique<Record>(2);
  rec->fields_[0] = expr::Value(double(n));
  rec->fields_[1] = expr::Value(double(m));
  return rec;
}

static RecordSet MakeData(size_t m) {
  RecordSet result(nullptr);
  for (auto i = 0; i < m; ++i) {
    result.emplace_back(RecordNOfM(i, m));
  }
  return result;
}

struct AggregateExecTest : public vmsdk::ValkeyTest {
  void SetUp() override {
    fakeIndex.fields_ = {
        {"n1", indexes::IndexerType::kNumeric},
        {"n2", indexes::IndexerType::kNumeric},
        {"n3", indexes::IndexerType::kNumeric},
        {"n3", indexes::IndexerType::kNumeric},
    };
    vmsdk::ValkeyTest::SetUp();
  }
  void TearDown() override { vmsdk::ValkeyTest::TearDown(); }
  FakeIndexInterface fakeIndex;

  std::unique_ptr<AggregateParameters> MakeStages(absl::string_view test) {
    auto argv = vmsdk::ToValkeyStringVector(test);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    auto params = std::make_unique<AggregateParameters>(0);
    params->parse_vars_.index_interface_ = &fakeIndex;
    EXPECT_EQ(
        params->AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric),
        0);
    EXPECT_EQ(
        params->AddRecordAttribute("n2", "n1", indexes::IndexerType::kNumeric),
        1);
    // params->attr_record_indexes_["n1"] = 0;
    // params->attr_record_indexes_["n2"] = 1;

    auto parser = CreateAggregateParser();

    auto result = parser.Parse(*params, itr);
    EXPECT_TRUE(result.ok()) << " Status is: " << result << "\n";

    // Free the allocated ValkeyModuleStrings to avoid memory leaks
    for (auto* str : argv) {
      ValkeyModule_FreeString(nullptr, str);
    }
    return params;
  }

  // Helper for FirstValue tests that need n3 attribute
  std::unique_ptr<AggregateParameters> MakeStagesWithN3(absl::string_view test) {
    auto argv = vmsdk::ToValkeyStringVector(test);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    auto params = std::make_unique<AggregateParameters>(0);
    params->parse_vars_.index_interface_ = &fakeIndex;
    EXPECT_EQ(
        params->AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric),
        0);
    EXPECT_EQ(
        params->AddRecordAttribute("n2", "n1", indexes::IndexerType::kNumeric),
        1);
    EXPECT_EQ(
        params->AddRecordAttribute("n3", "n3", indexes::IndexerType::kNumeric),
        2);

    auto parser = CreateAggregateParser();

    auto result = parser.Parse(*params, itr);
    EXPECT_TRUE(result.ok()) << " Status is: " << result << "\n";

    // Free the allocated ValkeyModuleStrings to avoid memory leaks
    for (auto* str : argv) {
      ValkeyModule_FreeString(nullptr, str);
    }
    return params;
  }

  // Helper for FirstValue tests that need n3 attribute
  std::unique_ptr<AggregateParameters> MakeStagesWithN3(absl::string_view test) {
    auto argv = vmsdk::ToValkeyStringVector(test);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    auto params = std::make_unique<AggregateParameters>(0);
    params->parse_vars_.index_interface_ = &fakeIndex;
    EXPECT_EQ(
        params->AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric),
        0);
    EXPECT_EQ(
        params->AddRecordAttribute("n2", "n1", indexes::IndexerType::kNumeric),
        1);
    EXPECT_EQ(
        params->AddRecordAttribute("n3", "n3", indexes::IndexerType::kNumeric),
        2);

    auto parser = CreateAggregateParser();

    auto result = parser.Parse(*params, itr);
    EXPECT_TRUE(result.ok()) << " Status is: " << result << "\n";

    // Free the allocated ValkeyModuleStrings to avoid memory leaks
    for (auto* str : argv) {
      ValkeyModule_FreeString(nullptr, str);
    }
    return params;
  }
};

TEST_F(AggregateExecTest, LimitTest) {
  std::cerr << "LimitTest\n";
  auto param = MakeStages("LIMIT 1 2");
  auto records = MakeData(4);
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 2);
  std::cerr << "Results:\n";
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_EQ(*records[0], *RecordNOfM(1, 4));
  EXPECT_EQ(*records[1], *RecordNOfM(2, 4));
}

TEST_F(AggregateExecTest, FilterTest) {
  std::cerr << "FilterTest\n";
  auto param = MakeStages("FILTER @n1==1");
  auto records = MakeData(4);
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  std::cerr << "Results:\n";
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_EQ(*records[0], *RecordNOfM(1, 4));
}

TEST_F(AggregateExecTest, ApplyTest) {
  std::cerr << "ApplyTest\n";
  auto param = MakeStages("APPLY @n1+1 as fred");
  auto records = MakeData(2);
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 2);
  std::cerr << "Results:\n";
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  auto r0 = RecordNOfM(0, 2);
  r0->fields_.resize(3);
  r0->fields_[2] = expr::Value(double(1.0));
  auto r1 = RecordNOfM(1, 2);
  r1->fields_.resize(3);
  r1->fields_[2] = expr::Value(double(2.0));
  EXPECT_EQ(*records[0], *r0);
  EXPECT_EQ(*records[1], *r1);
}

TEST_F(AggregateExecTest, SortTest) {
  struct Testcase {
    std::string text_;
    std::vector<size_t> order_;
    bool ordered;
    std::vector<size_t> max_order_;
  };
  Testcase testcases[]{
      {"Sortby 2 @n1 desc", {1, 0}, true, {9, 8}},
      {"sortby 2 @n1 asc", {0, 1}, true, {0, 1}},
      {"sortby 2 @n2 asc", {0, 1}, false, {0, 1}},
      {"sortby 2 @n2 desc", {0, 1}, false, {0, 1}},
      {"sortby 4 @n1 desc @n2 asc", {1, 0}, true, {9, 8}},
      {"sortby 4 @n1 asc  @n2 asc", {0, 1}, true, {0, 1}},
      {"sortby 4 @n1 desc @n2 desc", {1, 0}, true, {9, 8}},
      {"sortby 4 @n1 asc  @n2 desc", {0, 1}, true, {0, 1}},
      {"sortby 4 @n2 asc  @n1 asc", {0, 1}, false, {0, 1}},
      {"sortby 4 @n2 asc  @n1 desc", {1, 0}, false, {9, 8}},
      {"sortby 4 @n2 desc @n1 asc", {0, 1}, false, {0, 1}},
      {"sortby 4 @n2 desc @n1 desc", {1, 0}, false, {9, 8}},

  };
  for (auto do_max : {false, true}) {
    for (auto& tc : testcases) {
      std::string text = tc.text_;
      size_t input_count = tc.order_.size();
      auto order = tc.order_;
      if (do_max) {
        text += " MAX ";
        text += std::to_string(tc.max_order_.size());
        input_count = 10;
        order = tc.max_order_;
      }
      std::cerr << "SortTest: " << text << "\n";
      auto param = MakeStages(text);
      auto records = MakeData(input_count);
      for (auto& r : records) {
        std::cerr << *r << "\n";
      }
      EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
      EXPECT_EQ(records.size(), order.size());
      std::cerr << "Results:\n";
      for (auto& r : records) {
        std::cerr << *r << "\n";
      }
      if (!do_max || tc.ordered) {
        for (auto i = 0; i < order.size(); ++i) {
          EXPECT_EQ(*records[i], *RecordNOfM(order[i], input_count));
        }
      }
    }
  }
}

TEST_F(AggregateExecTest, GroupTest) {
  struct Testcase {
    std::string text_;
    size_t m;
    size_t num_groups;
  };
  Testcase testcases[]{
      {"groupby 1 @n1", 2, 2},
      {"groupby 2 @n1 @n2", 2, 2},
      {"groupby 1 @n2", 2, 1},
  };
  for (auto& tc : testcases) {
    std::cerr << "GroupTest: " << tc.text_ << "\n";
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    for (auto& r : records) {
      std::cerr << *r << "\n";
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), tc.num_groups);
    std::cerr << "Results:\n";
    for (auto& r : records) {
      std::cerr << *r << "\n";
    }
  }
}

TEST_F(AggregateExecTest, ReducerTest) {
  struct Testcase {
    std::string text_;
    size_t m;
    std::vector<double> values_;
  };
  Testcase testcases[]{
      {"groupby 1 @n2 reduce count 0", 4, {4}},
      {"groupby 1 @n2 reduce min 1 @n1", 4, {0}},
      {"groupby 1 @n2 reduce min 1 @n1 reduce count 0", 4, {0, 4}},
      {"groupby 1 @n2 reduce max 1 @n1", 4, {3}},
      {"groupby 1 @n2 reduce sum 1 @n1", 4, {6}},
      {"groupby 1 @n2 reduce stddev 1 @n1", 4, {1.2909944487358056}},
      {"groupby 1 @n2 reduce count_distinct 1 @n1", 4, {4}},
      {"groupby 1 @n2 reduce avg 1 @n1", 4, {1.5}}};
  for (auto& tc : testcases) {
    std::cerr << "GroupTest: " << tc.text_ << "\n";
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    for (auto& r : records) {
      std::cerr << *r << "\n";
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    std::cerr << "Result: " << *record << "\n";
    for (auto i = 0; i < tc.values_.size(); ++i) {
      EXPECT_TRUE(record->fields_.at(i + 2).IsDouble());
      EXPECT_NEAR(*(record->fields_.at(i + 2).AsDouble()), tc.values_[i], .001);
    }
  }
}

TEST_F(AggregateExecTest, FirstValueReducerTest) {
  struct Testcase {
    std::string text_;
    size_t m;
    std::vector<double> values_;
    bool should_succeed;
  };
  Testcase testcases[]{
      {"groupby 1 @n2 reduce first_value 1 @n1", 4, {0}, true},
      {"groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'", 4, {0}, true},
      {"groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"DESC\"'", 4, {3}, true},
      {"groupby 1 @n2 reduce first_value 3 @n1 '\"BY\"' @n1", 4, {0}, true},
      {"groupby 1 @n2 reduce first_value 3 @n1 '\"by\"' @n1", 4, {0}, true},
      {"groupby 1 @n2 reduce first_value 3 @n1 '\"By\"' @n1", 4, {0}, true},
      {"groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"asc\"'", 4, {0}, true},
      {"groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"desc\"'", 4, {3}, true},
      {"groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"Asc\"'", 4, {0}, true},
      {"groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"Desc\"'", 4, {3}, true},
      {"groupby 1 @n2 reduce first_value 2 @n1 '\"BY\"'", 4, {}, false},
      {"groupby 1 @n2 reduce first_value 3 @n1 '\"WITH\"' @n1", 4, {}, false},
      {"groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"UP\"'", 4, {}, false},
  };
  
  for (auto& tc : testcases) {
    std::cerr << "FirstValueReducerTest: " << tc.text_ << "\n";
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    for (auto& r : records) {
      std::cerr << *r << "\n";
    }
    
    auto status = param->stages_[0]->Execute(records);
    
    if (tc.should_succeed) {
      EXPECT_TRUE(status.ok()) << "Expected success but got: " << status;
      EXPECT_EQ(records.size(), 1);
      auto record = records.pop_front();
      std::cerr << "Result: " << *record << "\n";
      for (auto i = 0; i < tc.values_.size(); ++i) {
        EXPECT_TRUE(record->fields_.at(i + 2).IsDouble());
        EXPECT_NEAR(*(record->fields_.at(i + 2).AsDouble()), tc.values_[i], .001);
      }
    } else {
      if (status.ok() && records.size() == 1) {
        auto record = records.pop_front();
        std::cerr << "Result (expected nil): " << *record << "\n";
        EXPECT_TRUE(record->fields_.at(2).IsNil()) 
            << "Expected nil result for invalid arguments";
      }
    }
  }
}

TEST_F(AggregateExecTest, FirstValueReducerEdgeCasesTest) {
  {
    std::cerr << "Edge Case Test 1: Empty group returns nil\n";
    auto param = MakeStages("groupby 1 @n2 reduce first_value 1 @n1");
    RecordSet empty_records(nullptr);
    
    auto status = param->stages_[0]->Execute(empty_records);
    EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
    EXPECT_EQ(empty_records.size(), 0) << "Empty group should produce no results";
  }
  {
    std::cerr << "Edge Case Test 2: All nil comparison values returns nil\n";
    auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
    RecordSet test_records(nullptr);
    
    for (size_t i = 0; i < 4; ++i) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value();
      rec->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec));
    }
    
    auto status = param->stages_[0]->Execute(test_records);
    EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
    EXPECT_EQ(test_records.size(), 1) << "Expected one grouped record";
    
    auto record = test_records.pop_front();
    std::cerr << "Result: " << *record << "\n";
    EXPECT_TRUE(record->fields_.at(2).IsNil()) 
        << "Expected nil when all comparison values are nil";
  }
  {
    std::cerr << "Edge Case Test 3: Nil return value preserved with valid comparison\n";
    auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n2 '\"ASC\"'");
    RecordSet test_records(nullptr);
    
    auto rec1 = std::make_unique<Record>(2);
    rec1->fields_[0] = expr::Value();
    rec1->fields_[1] = expr::Value(1.0);
    test_records.emplace_back(std::move(rec1));
    
    auto rec2 = std::make_unique<Record>(2);
    rec2->fields_[0] = expr::Value(100.0);
    rec2->fields_[1] = expr::Value(5.0);
    test_records.emplace_back(std::move(rec2));
    
    auto rec3 = std::make_unique<Record>(2);
    rec3->fields_[0] = expr::Value(200.0);
    rec3->fields_[1] = expr::Value(10.0);
    test_records.emplace_back(std::move(rec3));
    
    auto status = param->stages_[0]->Execute(test_records);
    EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
    EXPECT_EQ(test_records.size(), 3) << "Expected three groups";
    
    bool found_target = false;
    for (auto& record : test_records) {
      std::cerr << "Result: " << *record << "\n";
      if (record->fields_.at(1).IsDouble() && 
          *(record->fields_.at(1).AsDouble()) == 1.0) {
        found_target = true;
        EXPECT_TRUE(record->fields_.at(2).IsNil()) 
            << "Expected nil return value to be preserved";
        break;
      }
    }
    EXPECT_TRUE(found_target) << "Expected to find group with n2=1.0";
  }
  
  {
    std::cerr << "Edge Case Test 4: Tie-breaking returns first encountered\n";
    
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
      RecordSet test_records(nullptr);
      
      auto rec1 = std::make_unique<Record>(2);
      rec1->fields_[0] = expr::Value(10.0);
      rec1->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec1));
      
      auto rec2 = std::make_unique<Record>(2);
      rec2->fields_[0] = expr::Value(10.0);
      rec2->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec2));
      
      auto rec3 = std::make_unique<Record>(2);
      rec3->fields_[0] = expr::Value(50.0);
      rec3->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec3));
      
      auto status = param->stages_[0]->Execute(test_records);
      EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
      EXPECT_EQ(test_records.size(), 1) << "Expected one grouped record";
      
      auto record = test_records.pop_front();
      std::cerr << "Result (ASC tie-breaking): " << *record << "\n";
      EXPECT_TRUE(record->fields_.at(2).IsDouble());
      EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 10.0, 0.001)
          << "Tie-breaking should return first encountered value";
    }
    
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"DESC\"'");
      RecordSet test_records(nullptr);
      
      auto rec1 = std::make_unique<Record>(2);
      rec1->fields_[0] = expr::Value(50.0);
      rec1->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec1));
      
      auto rec2 = std::make_unique<Record>(2);
      rec2->fields_[0] = expr::Value(100.0);
      rec2->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec2));
      
      auto rec3 = std::make_unique<Record>(2);
      rec3->fields_[0] = expr::Value(100.0);
      rec3->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec3));
      
      auto status = param->stages_[0]->Execute(test_records);
      EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
      EXPECT_EQ(test_records.size(), 1) << "Expected one grouped record";
      
      auto record = test_records.pop_front();
      std::cerr << "Result (DESC tie-breaking): " << *record << "\n";
      EXPECT_TRUE(record->fields_.at(2).IsDouble());
      EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 100.0, 0.001)
          << "Tie-breaking should return first encountered value";
    }
  }
  
  {
    std::cerr << "Edge Case Test 5: String comparison with lexicographic ordering\n";
    
    auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
    RecordSet test_records(nullptr);
    
    auto rec1 = std::make_unique<Record>(2);
    rec1->fields_[0] = expr::Value(2.0);
    rec1->fields_[1] = expr::Value(1.0);
    test_records.emplace_back(std::move(rec1));
    
    auto rec2 = std::make_unique<Record>(2);
    rec2->fields_[0] = expr::Value(10.0);
    rec2->fields_[1] = expr::Value(1.0);
    test_records.emplace_back(std::move(rec2));
    
    auto rec3 = std::make_unique<Record>(2);
    rec3->fields_[0] = expr::Value(1.0);
    rec3->fields_[1] = expr::Value(1.0);
    test_records.emplace_back(std::move(rec3));
    
    auto status = param->stages_[0]->Execute(test_records);
    EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
    EXPECT_EQ(test_records.size(), 1) << "Expected one grouped record";
    
    auto record = test_records.pop_front();
    std::cerr << "Result (numeric comparison): " << *record << "\n";
    EXPECT_TRUE(record->fields_.at(2).IsDouble());
    EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 1.0, 0.001)
        << "Numeric comparison should use numerical ordering";
  }
  
  {
    std::cerr << "Edge Case Test 6: Mixed nil and non-nil comparison values\n";
    
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
      RecordSet test_records(nullptr);
      
      auto rec1 = std::make_unique<Record>(2);
      rec1->fields_[0] = expr::Value();
      rec1->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec1));
      
      auto rec2 = std::make_unique<Record>(2);
      rec2->fields_[0] = expr::Value(50.0);
      rec2->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec2));
      
      auto rec3 = std::make_unique<Record>(2);
      rec3->fields_[0] = expr::Value();
      rec3->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec3));
      
      auto rec4 = std::make_unique<Record>(2);
      rec4->fields_[0] = expr::Value(100.0);
      rec4->fields_[1] = expr::Value(1.0);
      test_records.emplace_back(std::move(rec4));
      
      auto status = param->stages_[0]->Execute(test_records);
      EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
      EXPECT_EQ(test_records.size(), 1) << "Expected one grouped record";
      
      auto record = test_records.pop_front();
      std::cerr << "Result (mixed nil/non-nil): " << *record << "\n";
      EXPECT_TRUE(record->fields_.at(2).IsDouble());
      EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 50.0, 0.001)
          << "Should skip nil comparison values and use minimum non-nil";
    }
  }
  
  {
    std::cerr << "Edge Case Test 7: Comparing same property to itself\n";
    
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
      auto records = MakeData(4);  // Creates records with n1: [0, 1, 2, 3]
      
      auto status = param->stages_[0]->Execute(records);
      EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
      EXPECT_EQ(records.size(), 1) << "Expected one grouped record";
      
      auto record = records.pop_front();
      std::cerr << "Result (same property ASC): " << *record << "\n";
      EXPECT_TRUE(record->fields_.at(2).IsDouble());
      // Should return 0.0 (minimum n1 value)
      EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 0.0, 0.001)
          << "Comparing property to itself should work correctly";
    }
    
    // Test with DESC mode - should return record with maximum n1
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"DESC\"'");
      auto records = MakeData(4);  // Creates records with n1: [0, 1, 2, 3]
      
      auto status = param->stages_[0]->Execute(records);
      EXPECT_TRUE(status.ok()) << "Execution failed: " << status;
      EXPECT_EQ(records.size(), 1) << "Expected one grouped record";
      
      auto record = records.pop_front();
      std::cerr << "Result (same property DESC): " << *record << "\n";
      EXPECT_TRUE(record->fields_.at(2).IsDouble());
      // Should return 3.0 (maximum n1 value)
      EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 3.0, 0.001)
          << "Comparing property to itself should work correctly";
    }
  }
}

TEST_F(AggregateExecTest, FirstValueSimpleModePropertyTest) {
  
  struct PropertyTestCase {
    size_t group_size;
    double expected_first_value;
    std::string description;
  };
  
  std::vector<PropertyTestCase> test_cases;
  
  for (size_t group_size = 1; group_size <= 20; ++group_size) {
    test_cases.push_back({group_size, 0.0, 
                          "Group size " + std::to_string(group_size)});
  }
  
  for (size_t group_size : {25, 50, 75, 100}) {
    test_cases.push_back({group_size, 0.0,
                          "Large group size " + std::to_string(group_size)});
  }
  
  for (size_t i = 0; i < 20; ++i) {
    for (size_t group_size : {1, 2, 3, 5, 10, 15}) {
      test_cases.push_back({group_size, 0.0,
                            "Iteration " + std::to_string(i) + 
                            " with group size " + std::to_string(group_size)});
    }
  }
  
  size_t iteration = 0;
  for (const auto& tc : test_cases) {
    ++iteration;
    std::cerr << "Property Test Iteration " << iteration << ": " 
              << tc.description << "\n";
    
    auto param = MakeStages("groupby 1 @n2 reduce first_value 1 @n1");
    auto records = MakeData(tc.group_size);
    
    ASSERT_EQ(records.size(), tc.group_size) 
        << "Failed to generate correct number of records";
    
    if (!records.empty()) {
      ASSERT_TRUE(records[0]->fields_[0].IsDouble());
      EXPECT_EQ(*(records[0]->fields_[0].AsDouble()), tc.expected_first_value)
          << "First record doesn't have expected value";
    }
    
    auto status = param->stages_[0]->Execute(records);
    ASSERT_TRUE(status.ok()) << "Execution failed: " << status;
    
    ASSERT_EQ(records.size(), 1) << "Expected exactly one result record";
    auto record = records.pop_front();
    
    ASSERT_TRUE(record->fields_.at(2).IsDouble())
        << "Result is not a double value";
    
    double result = *(record->fields_.at(2).AsDouble());
    
    EXPECT_EQ(result, tc.expected_first_value)
        << "Property violated: Simple mode did not return first value. "
        << "Group size: " << tc.group_size
        << ", Expected: " << tc.expected_first_value
        << ", Got: " << result;
  }
  
  std::cerr << "Property test completed: " << iteration 
            << " iterations validated\n";
  EXPECT_GE(iteration, 100) 
      << "Property test should run at least 100 iterations";
}

TEST_F(AggregateExecTest, FirstValueSortedAscPropertyTest) {
  
  struct PropertyTestCase {
    std::vector<double> n1_values;
    double expected_min;
    std::string description;
  };
  
  std::vector<PropertyTestCase> test_cases;
  
  for (size_t group_size = 1; group_size <= 20; ++group_size) {
    std::vector<double> n1_values;
    double min_val = 10.0;
    
    for (size_t i = 0; i < group_size; ++i) {
      if (i == 0) {
        n1_values.push_back(min_val);  // Minimum at first position
      } else {
        n1_values.push_back(min_val + i * 10.0);
      }
    }
    
    test_cases.push_back({n1_values, min_val,
                          "Group size " + std::to_string(group_size) + " - min at start"});
  }
  
  for (size_t min_pos : {0, 1, 2, 3, 4}) {
    std::vector<double> n1_values;
    double min_val = 5.0;
    
    for (size_t i = 0; i < 5; ++i) {
      if (i == min_pos) {
        n1_values.push_back(min_val);
      } else {
        n1_values.push_back(50.0 + i * 10.0);
      }
    }
    
    test_cases.push_back({n1_values, min_val,
                          "Min at position " + std::to_string(min_pos)});
  }
  
  test_cases.push_back({{50.0, -100.0, 0.0, 25.0}, -100.0, "Negative minimum"});
  
  test_cases.push_back({{1.001, 1.0, 1.002, 1.003}, 1.0, "Close values precision"});
  
  test_cases.push_back({{1000000.0, -1000000.0, 500000.0, 0.0}, -1000000.0, "Large range"});
  
  test_cases.push_back({{50.0, 50.0, 50.0, 50.0}, 50.0, "All equal"});
  
  test_cases.push_back({{100.0, 10.0, 50.0, 10.0}, 10.0, "Multiple minimums"});
  
  test_cases.push_back({{42.0}, 42.0, "Single record"});
  
  for (size_t iter = 0; iter < 20; ++iter) {
    for (size_t pattern = 0; pattern < 4; ++pattern) {
      std::vector<double> n1_values;
      double min_val = 5.0 + iter;
      size_t group_size = 3 + (pattern * 2);
      
      for (size_t i = 0; i < group_size; ++i) {
        if ((pattern == 0 && i == 0) ||
            (pattern == 1 && i == group_size - 1) ||
            (pattern == 2 && i == group_size / 2) ||
            (pattern == 3 && i == 1)) {
          n1_values.push_back(min_val);
        } else {
          n1_values.push_back(min_val + 100.0 + i * 20.0);
        }
      }
      
      test_cases.push_back({n1_values, min_val,
                            "Iter " + std::to_string(iter) + " Pattern " + std::to_string(pattern)});
    }
  }
  
  size_t iteration = 0;
  for (const auto& tc : test_cases) {
    ++iteration;
    std::cerr << "Property Test Iteration " << iteration << ": " << tc.description << "\n";
    
    // Test with explicit ASC (nargs=4)
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
      RecordSet test_records(nullptr);
      
      // Create records using the same pattern as MakeData
      // All records will have n2 = tc.n1_values.size() (constant for grouping)
      size_t group_key = tc.n1_values.size();
      for (double n1_val : tc.n1_values) {
        auto rec = std::make_unique<Record>(2);
        rec->fields_[0] = expr::Value(n1_val);  // n1 = value to return and compare
        rec->fields_[1] = expr::Value(double(group_key));  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed: " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record";
      
      auto record = test_records.pop_front();
      ASSERT_TRUE(record->fields_.at(2).IsDouble()) << "Result not a double";
      
      double result = *(record->fields_.at(2).AsDouble());
      EXPECT_NEAR(result, tc.expected_min, 0.001)
          << "Property violated (ASC): Expected " << tc.expected_min
          << ", got " << result << " in " << tc.description;
    }
    
    // Test with default order (nargs=3) - should behave as ASC
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 3 @n1 '\"BY\"' @n1");
      RecordSet test_records(nullptr);
      
      size_t group_key = tc.n1_values.size();
      for (double n1_val : tc.n1_values) {
        auto rec = std::make_unique<Record>(2);
        rec->fields_[0] = expr::Value(n1_val);
        rec->fields_[1] = expr::Value(double(group_key));
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed: " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record";
      
      auto record = test_records.pop_front();
      ASSERT_TRUE(record->fields_.at(2).IsDouble()) << "Result not a double";
      
      double result = *(record->fields_.at(2).AsDouble());
      EXPECT_NEAR(result, tc.expected_min, 0.001)
          << "Property violated (default): Expected " << tc.expected_min
          << ", got " << result << " in " << tc.description;
    }
  }
  
  std::cerr << "Property test completed: " << iteration 
            << " iterations validated\n";
  EXPECT_GE(iteration, 100) 
      << "Property test should run at least 100 iterations";
}

// with BY and DESC, the result should equal the return property value from the record with the
// largest comparison property value.
TEST_F(AggregateExecTest, FirstValueSortedDescPropertyTest) {
  // the maximum comparison property across various group sizes and value ranges.
  //
  
  struct PropertyTestCase {
    std::vector<double> n1_values;  // Values for n1 field (return and comparison)
    double expected_max;  // Expected maximum n1 value
    std::string description;
  };
  
  std::vector<PropertyTestCase> test_cases;
  
  for (size_t group_size = 1; group_size <= 20; ++group_size) {
    std::vector<double> n1_values;
    double max_val = 100.0;
    
    for (size_t i = 0; i < group_size; ++i) {
      if (i == 0) {
        n1_values.push_back(max_val);  // Maximum at first position
      } else {
        n1_values.push_back(max_val - i * 10.0);
      }
    }
    
    test_cases.push_back({n1_values, max_val,
                          "Group size " + std::to_string(group_size) + " - max at start"});
  }
  
  for (size_t max_pos : {0, 1, 2, 3, 4}) {
    std::vector<double> n1_values;
    double max_val = 500.0;
    
    for (size_t i = 0; i < 5; ++i) {
      if (i == max_pos) {
        n1_values.push_back(max_val);
      } else {
        n1_values.push_back(50.0 + i * 10.0);
      }
    }
    
    test_cases.push_back({n1_values, max_val,
                          "Max at position " + std::to_string(max_pos)});
  }
  
  test_cases.push_back({{-50.0, -100.0, -10.0, -25.0}, -10.0, "Negative maximum"});
  
  test_cases.push_back({{-50.0, 100.0, 0.0, -25.0}, 100.0, "Mixed pos/neg maximum"});
  
  test_cases.push_back({{1.001, 1.0, 1.002, 1.003}, 1.003, "Close values precision"});
  
  test_cases.push_back({{1000000.0, -1000000.0, 500000.0, 0.0}, 1000000.0, "Large range"});
  
  test_cases.push_back({{50.0, 50.0, 50.0, 50.0}, 50.0, "All equal"});
  
  test_cases.push_back({{100.0, 200.0, 50.0, 200.0}, 200.0, "Multiple maximums"});
  
  test_cases.push_back({{42.0}, 42.0, "Single record"});
  
  test_cases.push_back({{0.0, -10.0, -5.0, -20.0}, 0.0, "Zero is maximum"});
  
  for (size_t iter = 0; iter < 20; ++iter) {
    for (size_t pattern = 0; pattern < 4; ++pattern) {
      std::vector<double> n1_values;
      double max_val = 500.0 + iter * 10.0;
      size_t group_size = 3 + (pattern * 2);
      
      for (size_t i = 0; i < group_size; ++i) {
        if ((pattern == 0 && i == 0) ||
            (pattern == 1 && i == group_size - 1) ||
            (pattern == 2 && i == group_size / 2) ||
            (pattern == 3 && i == 1)) {
          n1_values.push_back(max_val);
        } else {
          n1_values.push_back(max_val - 100.0 - i * 20.0);
        }
      }
      
      test_cases.push_back({n1_values, max_val,
                            "Iter " + std::to_string(iter) + " Pattern " + std::to_string(pattern)});
    }
  }
  
  size_t iteration = 0;
  for (const auto& tc : test_cases) {
    ++iteration;
    std::cerr << "Property Test Iteration " << iteration << ": " << tc.description << "\n";
    
    // Test with explicit DESC (nargs=4)
    auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"DESC\"'");
    RecordSet test_records(nullptr);
    
    size_t group_key = tc.n1_values.size();
    for (double n1_val : tc.n1_values) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(n1_val);  // n1 = value to return and compare
      rec->fields_[1] = expr::Value(double(group_key));  // n2 = constant for grouping
      test_records.emplace_back(std::move(rec));
    }
    
    auto status = param->stages_[0]->Execute(test_records);
    ASSERT_TRUE(status.ok()) << "Execution failed: " << status;
    ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record";
    
    auto record = test_records.pop_front();
    ASSERT_TRUE(record->fields_.at(2).IsDouble()) << "Result not a double";
    
    double result = *(record->fields_.at(2).AsDouble());
    EXPECT_NEAR(result, tc.expected_max, 0.001)
        << "Property violated (DESC): Expected " << tc.expected_max
        << ", got " << result << " in " << tc.description;
  }
  
  std::cerr << "Property test completed: " << iteration 
            << " iterations validated\n";
  EXPECT_GE(iteration, 100) 
      << "Property test should run at least 100 iterations";
}

// when FIRST_VALUE is used with BY clause, records with nil comparison values should be excluded
// from consideration, and the result should be determined only from records with non-nil comparison values.
TEST_F(AggregateExecTest, FirstValueNilComparisonSkippingPropertyTest) {
  // the optimal value. We test with 100+ iterations using groups with mixed nil and non-nil values.
  //
  
  struct PropertyTestCase {
    std::vector<std::optional<double>> n1_values;  // Values for n1 (nil if nullopt)
    std::optional<double> expected_result_asc;  // Expected result for ASC (nullopt if all nil)
    std::optional<double> expected_result_desc;  // Expected result for DESC (nullopt if all nil)
    std::string description;
  };
  
  std::vector<PropertyTestCase> test_cases;
  
  for (size_t nil_pos = 0; nil_pos < 5; ++nil_pos) {
    std::vector<std::optional<double>> n1_values;
    for (size_t i = 0; i < 5; ++i) {
      if (i == nil_pos) {
        n1_values.push_back(std::nullopt);  // nil value
      } else {
        n1_values.push_back(100.0 - i * 10.0);  // 100, 90, 80, 70, 60
      }
    }
    // Minimum non-nil is 60.0 (at position 4), maximum is 100.0 (at position 0)
    // But if nil_pos is 4, minimum is 70.0; if nil_pos is 0, maximum is 90.0
    double min_val = 60.0;
    double max_val = 100.0;
    if (nil_pos == 4) min_val = 70.0;
    if (nil_pos == 0) max_val = 90.0;
    if (nil_pos == 1) max_val = 100.0;
    if (nil_pos == 2) { min_val = 60.0; max_val = 100.0; }
    if (nil_pos == 3) min_val = 60.0;
    
    test_cases.push_back({n1_values, min_val, max_val,
                          "Single nil at position " + std::to_string(nil_pos)});
  }
  
  test_cases.push_back({
    {std::nullopt, 50.0, std::nullopt, 100.0, std::nullopt, 25.0},
    25.0, 100.0,
    "Multiple nils scattered"
  });
  
  test_cases.push_back({
    {std::nullopt, 50.0, 75.0, 100.0, std::nullopt},
    50.0, 100.0,
    "Nils at both ends"
  });
  
  test_cases.push_back({
    {std::nullopt, std::nullopt, 42.0, std::nullopt, std::nullopt},
    42.0, 42.0,
    "Only one non-nil value"
  });
  
  test_cases.push_back({
    {std::nullopt, 30.0, std::nullopt, std::nullopt, 70.0},
    30.0, 70.0,
    "Two non-nil values"
  });
  
  std::srand(12345);  // Fixed seed for reproducibility
  for (size_t group_size = 3; group_size <= 15; ++group_size) {
    std::vector<std::optional<double>> n1_values;
    std::optional<double> min_val;
    std::optional<double> max_val;
    
    for (size_t i = 0; i < group_size; ++i) {
      // ~10% probability of nil
      if (std::rand() % 10 == 0) {
        n1_values.push_back(std::nullopt);
      } else {
        double val = static_cast<double>(std::rand() % 1000 - 500);  // Range: -500 to 499
        n1_values.push_back(val);
        
        if (!min_val || val < *min_val) {
          min_val = val;
        }
        if (!max_val || val > *max_val) {
          max_val = val;
        }
      }
    }
    
    test_cases.push_back({n1_values, min_val, max_val,
                          "Random ~10% nil, size " + std::to_string(group_size)});
  }
  
  for (size_t iter = 0; iter < 30; ++iter) {
    for (size_t pattern = 0; pattern < 3; ++pattern) {
      std::vector<std::optional<double>> n1_values;
      std::optional<double> min_val;
      std::optional<double> max_val;
      size_t group_size = 5 + (pattern * 2);
      
      for (size_t i = 0; i < group_size; ++i) {
        // Different nil patterns
        bool is_nil = false;
        if (pattern == 0) {
          is_nil = (i % 3 == 0);  // Every 3rd is nil
        } else if (pattern == 1) {
          is_nil = (i == 0 || i == group_size - 1);  // First and last are nil
        } else {
          is_nil = (std::rand() % 10 == 0);  // Random ~10%
        }
        
        if (is_nil) {
          n1_values.push_back(std::nullopt);
        } else {
          double val = 100.0 + iter * 10.0 - i * 5.0;
          n1_values.push_back(val);
          
          if (!min_val || val < *min_val) {
            min_val = val;
          }
          if (!max_val || val > *max_val) {
            max_val = val;
          }
        }
      }
      
      test_cases.push_back({n1_values, min_val, max_val,
                            "Iter " + std::to_string(iter) + " Pattern " + std::to_string(pattern)});
    }
  }
  
  size_t iteration = 0;
  for (const auto& tc : test_cases) {
    ++iteration;
    std::cerr << "Property Test Iteration " << iteration << ": " << tc.description << "\n";
    
    // Test with ASC mode
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
      RecordSet test_records(nullptr);
      
      // Create records with mixed nil and non-nil n1 values
      size_t group_key = tc.n1_values.size();
      for (const auto& n1_opt : tc.n1_values) {
        auto rec = std::make_unique<Record>(2);
        if (n1_opt.has_value()) {
          rec->fields_[0] = expr::Value(*n1_opt);  // n1 = non-nil value
        } else {
          rec->fields_[0] = expr::Value();  // n1 = nil
        }
        rec->fields_[1] = expr::Value(double(group_key));  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed: " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record";
      
      auto record = test_records.pop_front();
      
      if (tc.expected_result_asc.has_value()) {
        ASSERT_TRUE(record->fields_.at(2).IsDouble()) 
            << "Result should be double when non-nil values exist";
        double result = *(record->fields_.at(2).AsDouble());
        EXPECT_NEAR(result, *tc.expected_result_asc, 0.001)
            << "Property violated (ASC): Expected " << *tc.expected_result_asc
            << ", got " << result << " in " << tc.description;
      } else {
        EXPECT_TRUE(record->fields_.at(2).IsNil())
            << "Property violated (ASC): Expected nil when all comparison values are nil in "
            << tc.description;
      }
    }
    
    // Test with DESC mode
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"DESC\"'");
      RecordSet test_records(nullptr);
      
      // Create records with mixed nil and non-nil n1 values
      size_t group_key = tc.n1_values.size();
      for (const auto& n1_opt : tc.n1_values) {
        auto rec = std::make_unique<Record>(2);
        if (n1_opt.has_value()) {
          rec->fields_[0] = expr::Value(*n1_opt);  // n1 = non-nil value
        } else {
          rec->fields_[0] = expr::Value();  // n1 = nil
        }
        rec->fields_[1] = expr::Value(double(group_key));  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed: " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record";
      
      auto record = test_records.pop_front();
      
      if (tc.expected_result_desc.has_value()) {
        ASSERT_TRUE(record->fields_.at(2).IsDouble()) 
            << "Result should be double when non-nil values exist";
        double result = *(record->fields_.at(2).AsDouble());
        EXPECT_NEAR(result, *tc.expected_result_desc, 0.001)
            << "Property violated (DESC): Expected " << *tc.expected_result_desc
            << ", got " << result << " in " << tc.description;
      } else {
        EXPECT_TRUE(record->fields_.at(2).IsNil())
            << "Property violated (DESC): Expected nil when all comparison values are nil in "
            << tc.description;
      }
    }
  }
  
  std::cerr << "Property test completed: " << iteration 
            << " iterations validated\n";
  EXPECT_GE(iteration, 100) 
      << "Property test should run at least 100 iterations";
}

// the result should be identical to using 4 arguments with ASC explicitly specified.
TEST_F(AggregateExecTest, FirstValueDefaultOrderPropertyTest) {
  // identically to the 4-argument form with explicit ASC across various group sizes and value ranges.
  //
  
  struct PropertyTestCase {
    std::vector<double> n1_values;  // Values for n1 field (return and comparison)
    double expected_result;  // Expected result (minimum n1 value)
    std::string description;
  };
  
  std::vector<PropertyTestCase> test_cases;
  
  for (size_t group_size = 1; group_size <= 20; ++group_size) {
    std::vector<double> n1_values;
    double min_val = 10.0;
    
    for (size_t i = 0; i < group_size; ++i) {
      if (i == 0) {
        n1_values.push_back(min_val);  // Minimum at first position
      } else {
        n1_values.push_back(min_val + i * 10.0);
      }
    }
    
    test_cases.push_back({n1_values, min_val,
                          "Group size " + std::to_string(group_size) + " - min at start"});
  }
  
  for (size_t min_pos : {0, 1, 2, 3, 4}) {
    std::vector<double> n1_values;
    double min_val = 5.0;
    
    for (size_t i = 0; i < 5; ++i) {
      if (i == min_pos) {
        n1_values.push_back(min_val);
      } else {
        n1_values.push_back(50.0 + i * 10.0);
      }
    }
    
    test_cases.push_back({n1_values, min_val,
                          "Min at position " + std::to_string(min_pos)});
  }
  
  test_cases.push_back({{-100.0, -50.0, -10.0, -25.0}, -100.0, "Negative minimum"});
  
  test_cases.push_back({{100.0, -50.0, 0.0, 25.0}, -50.0, "Mixed pos/neg minimum"});
  
  test_cases.push_back({{1.003, 1.001, 1.002, 1.0}, 1.0, "Close values precision"});
  
  test_cases.push_back({{1000000.0, -1000000.0, 500000.0, 0.0}, -1000000.0, "Large range"});
  
  test_cases.push_back({{50.0, 50.0, 50.0, 50.0}, 50.0, "All equal"});
  
  test_cases.push_back({{100.0, 50.0, 200.0, 50.0}, 50.0, "Multiple minimums"});
  
  test_cases.push_back({{42.0}, 42.0, "Single record"});
  
  test_cases.push_back({{0.0, 10.0, 5.0, 20.0}, 0.0, "Zero is minimum"});
  
  for (size_t iter = 0; iter < 20; ++iter) {
    for (size_t pattern = 0; pattern < 4; ++pattern) {
      std::vector<double> n1_values;
      double min_val = 10.0 + iter * 5.0;
      size_t group_size = 3 + (pattern * 2);
      
      for (size_t i = 0; i < group_size; ++i) {
        if ((pattern == 0 && i == 0) ||
            (pattern == 1 && i == group_size - 1) ||
            (pattern == 2 && i == group_size / 2) ||
            (pattern == 3 && i == 1)) {
          n1_values.push_back(min_val);
        } else {
          n1_values.push_back(min_val + 100.0 + i * 20.0);
        }
      }
      
      test_cases.push_back({n1_values, min_val,
                            "Iter " + std::to_string(iter) + " Pattern " + std::to_string(pattern)});
    }
  }
  
  size_t iteration = 0;
  for (const auto& tc : test_cases) {
    ++iteration;
    std::cerr << "Property Test Iteration " << iteration << ": " << tc.description << "\n";
    
    // Test with 3-arg form (default order)
    double result_3arg;
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 3 @n1 '\"BY\"' @n1");
      RecordSet test_records(nullptr);
      
      // Create records
      size_t group_key = tc.n1_values.size();
      for (double n1_val : tc.n1_values) {
        auto rec = std::make_unique<Record>(2);
        rec->fields_[0] = expr::Value(n1_val);  // n1 = value to return and compare
        rec->fields_[1] = expr::Value(double(group_key));  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed (3-arg): " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record (3-arg)";
      
      auto record = test_records.pop_front();
      ASSERT_TRUE(record->fields_.at(2).IsDouble()) << "Result not a double (3-arg)";
      
      result_3arg = *(record->fields_.at(2).AsDouble());
      EXPECT_NEAR(result_3arg, tc.expected_result, 0.001)
          << "3-arg form: Expected " << tc.expected_result
          << ", got " << result_3arg << " in " << tc.description;
    }
    
    // Test with 4-arg form (explicit ASC)
    double result_4arg;
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
      RecordSet test_records(nullptr);
      
      // Create records (same as 3-arg test)
      size_t group_key = tc.n1_values.size();
      for (double n1_val : tc.n1_values) {
        auto rec = std::make_unique<Record>(2);
        rec->fields_[0] = expr::Value(n1_val);  // n1 = value to return and compare
        rec->fields_[1] = expr::Value(double(group_key));  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed (4-arg): " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record (4-arg)";
      
      auto record = test_records.pop_front();
      ASSERT_TRUE(record->fields_.at(2).IsDouble()) << "Result not a double (4-arg)";
      
      result_4arg = *(record->fields_.at(2).AsDouble());
      EXPECT_NEAR(result_4arg, tc.expected_result, 0.001)
          << "4-arg form: Expected " << tc.expected_result
          << ", got " << result_4arg << " in " << tc.description;
    }
    
    // Property validation: 3-arg and 4-arg results must be identical
    EXPECT_NEAR(result_3arg, result_4arg, 0.001)
        << "Property violated: 3-arg form result (" << result_3arg
        << ") differs from 4-arg ASC form result (" << result_4arg
        << ") in " << tc.description;
  }
  
  std::cerr << "Property test completed: " << iteration 
            << " iterations validated\n";
  EXPECT_GE(iteration, 100) 
      << "Property test should run at least 100 iterations";
}

// (minimum for ASC, maximum for DESC), the result should be the return property value from the first
// record encountered with that optimal comparison value.
TEST_F(AggregateExecTest, FirstValueTieBreakingPropertyTest) {
  // the first encountered record's value is returned. We test with 100+ iterations using groups
  // with duplicate optimal values at different positions.
  //
  
  struct PropertyTestCase {
    std::vector<double> return_values;      // Values to return (n1)
    std::vector<double> comparison_values;  // Values to compare (n2)
    double expected_result_asc;   // Expected return value for ASC (first with min comparison)
    double expected_result_desc;  // Expected return value for DESC (first with max comparison)
    std::string description;
  };
  
  std::vector<PropertyTestCase> test_cases;
  
  // Minimum comparison value is 10.0, appears at positions 0 and 2
  test_cases.push_back({
    {100.0, 200.0, 300.0, 400.0},  // return values
    {10.0, 50.0, 10.0, 60.0},      // comparison values (min=10.0 at pos 0 and 2)
    100.0,  // ASC: first record with min (pos 0, return 100.0)
    400.0,  // DESC: first record with max (pos 3, return 400.0)
    "Two minimums at positions 0 and 2"
  });
  
  test_cases.push_back({
    {100.0, 200.0, 300.0, 400.0},  // return values
    {50.0, 60.0, 10.0, 10.0},      // comparison values (min=10.0 at pos 2 and 3)
    300.0,  // ASC: first record with min (pos 2, return 300.0)
    200.0,  // DESC: first record with max (pos 1, return 200.0)
    "Two minimums at positions 2 and 3"
  });
  
  test_cases.push_back({
    {111.0, 222.0, 333.0, 444.0, 555.0},  // return values
    {5.0, 100.0, 5.0, 200.0, 5.0},        // comparison values (min=5.0 at pos 0, 2, 4)
    111.0,  // ASC: first record with min (pos 0, return 111.0)
    444.0,  // DESC: first record with max (pos 3, return 444.0)
    "Three minimums at positions 0, 2, 4"
  });
  
  test_cases.push_back({
    {10.0, 20.0, 30.0, 40.0},  // return values
    {50.0, 50.0, 50.0, 50.0},  // comparison values (all same)
    10.0,  // ASC: first record (pos 0, return 10.0)
    10.0,  // DESC: first record (pos 0, return 10.0)
    "All comparison values equal"
  });
  
  test_cases.push_back({
    {100.0, 200.0, 300.0, 400.0},  // return values
    {90.0, 50.0, 90.0, 60.0},      // comparison values (max=90.0 at pos 0 and 2)
    200.0,  // ASC: first record with min (pos 1, return 200.0)
    100.0,  // DESC: first record with max (pos 0, return 100.0)
    "Two maximums at positions 0 and 2"
  });
  
  test_cases.push_back({
    {1.0, 2.0, 3.0, 4.0, 5.0, 6.0},  // return values (all different)
    {10.0, 10.0, 10.0, 10.0, 10.0, 10.0},  // comparison values (all same)
    1.0,  // ASC: first record (pos 0, return 1.0)
    1.0,  // DESC: first record (pos 0, return 1.0)
    "All comparison values equal, different return values"
  });
  
  test_cases.push_back({
    {77.0, 88.0, 99.0},  // return values
    {5.0, 5.0, 100.0},   // comparison values (min=5.0 at pos 0 and 1)
    77.0,  // ASC: first record with min (pos 0, return 77.0)
    99.0,  // DESC: first record with max (pos 2, return 99.0)
    "Tie at beginning positions 0 and 1"
  });
  
  test_cases.push_back({
    {77.0, 88.0, 99.0},  // return values
    {100.0, 5.0, 5.0},   // comparison values (min=5.0 at pos 1 and 2)
    88.0,  // ASC: first record with min (pos 1, return 88.0)
    77.0,  // DESC: first record with max (pos 0, return 77.0)
    "Tie at end positions 1 and 2"
  });
  
  for (size_t group_size = 2; group_size <= 10; ++group_size) {
    std::vector<double> return_values;
    std::vector<double> comparison_values;
    
    // Create group where first two records have the minimum comparison value
    double min_comp = 10.0;
    double max_comp = 100.0;
    
    for (size_t i = 0; i < group_size; ++i) {
      return_values.push_back(1000.0 + i * 100.0);  // Unique return values
      
      if (i < 2) {
        comparison_values.push_back(min_comp);  // First two have minimum
      } else if (i == group_size - 1) {
        comparison_values.push_back(max_comp);  // Last has maximum
      } else {
        comparison_values.push_back(50.0 + i * 5.0);  // Others in between
      }
    }
    
    test_cases.push_back({
      return_values,
      comparison_values,
      1000.0,  // ASC: first record with min (pos 0, return 1000.0)
      1000.0 + (group_size - 1) * 100.0,  // DESC: last record with max
      "Group size " + std::to_string(group_size) + " with tie at start"
    });
  }
  
  test_cases.push_back({
    {-100.0, -200.0, -300.0, -400.0},  // return values (all negative)
    {-50.0, 100.0, -50.0, 200.0},      // comparison values (min=-50.0 at pos 0 and 2)
    -100.0,  // ASC: first record with min (pos 0, return -100.0)
    -400.0,  // DESC: first record with max (pos 3, return -400.0)
    "Negative return values with tie"
  });
  
  std::srand(54321);  // Fixed seed for reproducibility
  for (size_t iter = 0; iter < 25; ++iter) {
    for (size_t pattern = 0; pattern < 4; ++pattern) {
      std::vector<double> return_values;
      std::vector<double> comparison_values;
      size_t group_size = 4 + (pattern * 2);
      
      double min_comp = 10.0 + iter * 2.0;
      double max_comp = 200.0 + iter * 5.0;
      
      // Determine positions for ties based on pattern
      size_t tie_pos1 = 0, tie_pos2 = 0;
      if (pattern == 0) {
        tie_pos1 = 0; tie_pos2 = 1;  // Tie at start
      } else if (pattern == 1) {
        tie_pos1 = group_size - 2; tie_pos2 = group_size - 1;  // Tie at end
      } else if (pattern == 2) {
        tie_pos1 = 0; tie_pos2 = group_size - 1;  // Tie at extremes
      } else {
        tie_pos1 = 1; tie_pos2 = 3;  // Tie in middle
      }
      
      double first_return_with_min = 0.0;
      double first_return_with_max = 0.0;
      bool found_max = false;
      
      for (size_t i = 0; i < group_size; ++i) {
        double ret_val = 500.0 + iter * 50.0 + i * 10.0;
        return_values.push_back(ret_val);
        
        if (i == tie_pos1 || i == tie_pos2) {
          comparison_values.push_back(min_comp);  // Tie for minimum
          if (i == tie_pos1) {
            first_return_with_min = ret_val;
          }
        } else if (i == (group_size / 2) && !found_max) {
          comparison_values.push_back(max_comp);  // Maximum
          first_return_with_max = ret_val;
          found_max = true;
        } else {
          comparison_values.push_back(50.0 + i * 10.0);  // Other values
        }
      }
      
      if (!found_max) {
        // If no max was set, use a different position
        comparison_values[group_size / 2] = max_comp;
        first_return_with_max = return_values[group_size / 2];
      }
      
      test_cases.push_back({
        return_values,
        comparison_values,
        first_return_with_min,
        first_return_with_max,
        "Iter " + std::to_string(iter) + " Pattern " + std::to_string(pattern)
      });
    }
  }
  
  size_t iteration = 0;
  for (const auto& tc : test_cases) {
    ++iteration;
    std::cerr << "Property Test Iteration " << iteration << ": " << tc.description << "\n";
    
    ASSERT_EQ(tc.return_values.size(), tc.comparison_values.size())
        << "Test case setup error: return and comparison vectors must have same size";
    
    // Test with ASC mode
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"ASC\"'");
      RecordSet test_records(nullptr);
      
      // Create records with n1 = comparison value (also used as return value)
      double group_key = 999.0;
      for (size_t i = 0; i < tc.comparison_values.size(); ++i) {
        auto rec = std::make_unique<Record>(2);
        rec->fields_[0] = expr::Value(tc.comparison_values[i]);  // n1 = comparison value (and return value)
        rec->fields_[1] = expr::Value(group_key);  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed (ASC): " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record (ASC)";
      
      auto record = test_records.pop_front();
      ASSERT_TRUE(record->fields_.at(2).IsDouble()) << "Result not a double (ASC)";
      
      double result = *(record->fields_.at(2).AsDouble());
      
      double expected_min = *std::min_element(tc.comparison_values.begin(), tc.comparison_values.end());
      
      EXPECT_NEAR(result, expected_min, 0.001)
          << "Property violated (ASC): Expected " << expected_min
          << " (minimum comparison value), got " << result
          << " in " << tc.description;
    }
    
    // Test with DESC mode
    {
      auto param = MakeStages("groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 '\"DESC\"'");
      RecordSet test_records(nullptr);
      
      // Create records with n1 = comparison value (also used as return value)
      double group_key = 999.0;
      for (size_t i = 0; i < tc.comparison_values.size(); ++i) {
        auto rec = std::make_unique<Record>(2);
        rec->fields_[0] = expr::Value(tc.comparison_values[i]);  // n1 = comparison value (and return value)
        rec->fields_[1] = expr::Value(group_key);  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed (DESC): " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record (DESC)";
      
      auto record = test_records.pop_front();
      ASSERT_TRUE(record->fields_.at(2).IsDouble()) << "Result not a double (DESC)";
      
      double result = *(record->fields_.at(2).AsDouble());
      
      double expected_max = *std::max_element(tc.comparison_values.begin(), tc.comparison_values.end());
      
      EXPECT_NEAR(result, expected_max, 0.001)
          << "Property violated (DESC): Expected " << expected_max
          << " (maximum comparison value), got " << result
          << " in " << tc.description;
    }
  }
  
  std::cerr << "Property test completed: " << iteration 
            << " iterations validated\n";
  EXPECT_GE(iteration, 100) 
      << "Property test should run at least 100 iterations";
}

// if that record has the optimal comparison value, the result should be nil.
TEST_F(AggregateExecTest, FirstValueNilReturnPreservationPropertyTest) {
  // with nil return property has the optimal comparison value. We test with 100+ iterations
  // using groups where the optimal record has a nil return value.
  //
  // **Validates: Requirements 7.2**
  //
  // has a nil return value, and verify that nil is preserved in the result.

  struct PropertyTestCase {
    std::string description;
    std::vector<std::optional<double>> return_values;  // nullopt represents nil
    std::vector<double> comparison_values;
    bool is_desc;
    std::optional<double> expected_result;  // nullopt if expecting nil
  };

  std::vector<PropertyTestCase> test_cases;
  std::mt19937 rng(42);  // Fixed seed for reproducibility
  std::uniform_int_distribution<size_t> size_dist(2, 20);
  std::uniform_real_distribution<double> value_dist(-1000.0, 1000.0);
  std::uniform_int_distribution<int> bool_dist(0, 1);

  for (size_t i = 0; i < 70; ++i) {
    size_t group_size = size_dist(rng);
    bool is_desc = bool_dist(rng);
    
    std::vector<std::optional<double>> return_vals;
    std::vector<double> comparison_vals;
    
    // Generate random comparison values
    for (size_t j = 0; j < group_size; ++j) {
      comparison_vals.push_back(value_dist(rng));
    }
    
    // Find the optimal comparison value index
    size_t optimal_idx = 0;
    double optimal_comp = comparison_vals[0];
    for (size_t j = 1; j < group_size; ++j) {
      if (is_desc) {
        if (comparison_vals[j] > optimal_comp) {
          optimal_comp = comparison_vals[j];
          optimal_idx = j;
        }
      } else {
        if (comparison_vals[j] < optimal_comp) {
          optimal_comp = comparison_vals[j];
          optimal_idx = j;
        }
      }
    }
    
    // Generate return values, making the optimal one nil
    for (size_t j = 0; j < group_size; ++j) {
      if (j == optimal_idx) {
        return_vals.push_back(std::nullopt);  // nil
      } else {
        return_vals.push_back(value_dist(rng));
      }
    }
    
    std::ostringstream desc;
    desc << "Group size " << group_size << ", " 
         << (is_desc ? "DESC" : "ASC") << " mode, "
         << "optimal at index " << optimal_idx << " with nil return";
    
    test_cases.push_back({
      desc.str(),
      return_vals,
      comparison_vals,
      is_desc,
      std::nullopt  // expect nil result
    });
  }
  
  for (size_t i = 0; i < 50; ++i) {
    size_t group_size = size_dist(rng);
    if (group_size < 2) group_size = 2;  // Need at least 2 records
    bool is_desc = bool_dist(rng);
    
    std::vector<std::optional<double>> return_vals;
    std::vector<double> comparison_vals;
    
    // Generate random comparison values
    for (size_t j = 0; j < group_size; ++j) {
      comparison_vals.push_back(value_dist(rng));
    }
    
    // Find the optimal comparison value index
    size_t optimal_idx = 0;
    double optimal_comp = comparison_vals[0];
    for (size_t j = 1; j < group_size; ++j) {
      if (is_desc) {
        if (comparison_vals[j] > optimal_comp) {
          optimal_comp = comparison_vals[j];
          optimal_idx = j;
        }
      } else {
        if (comparison_vals[j] < optimal_comp) {
          optimal_comp = comparison_vals[j];
          optimal_idx = j;
        }
      }
    }
    
    // Generate return values, making a NON-optimal one nil
    size_t nil_idx = (optimal_idx + 1) % group_size;
    double optimal_return_val = value_dist(rng);
    for (size_t j = 0; j < group_size; ++j) {
      if (j == nil_idx) {
        return_vals.push_back(std::nullopt);  // nil
      } else if (j == optimal_idx) {
        return_vals.push_back(optimal_return_val);
      } else {
        return_vals.push_back(value_dist(rng));
      }
    }
    
    std::ostringstream desc;
    desc << "Group size " << group_size << ", " 
         << (is_desc ? "DESC" : "ASC") << " mode, "
         << "nil at index " << nil_idx << ", optimal at " << optimal_idx;
    
    test_cases.push_back({
      desc.str(),
      return_vals,
      comparison_vals,
      is_desc,
      optimal_return_val  // expect non-nil result (the optimal value)
    });
  }
  
  size_t iteration = 0;
  for (const auto& tc : test_cases) {
    ++iteration;
    std::cerr << "Property Test Iteration " << iteration << ": " << tc.description << "\n";
    
    ASSERT_EQ(tc.return_values.size(), tc.comparison_values.size())
        << "Test case setup error: return and comparison value counts must match";
    
    // Test with the specified order
    std::string order = tc.is_desc ? "'\"DESC\"'" : "'\"ASC\"'";
    std::string query = "groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n3 " + order;
    
    auto param = MakeStagesWithN3(query);
    RecordSet test_records(nullptr);
    
    size_t group_key = tc.return_values.size();
    for (size_t i = 0; i < tc.return_values.size(); ++i) {
      auto rec = std::make_unique<Record>(3);
      
      if (tc.return_values[i].has_value()) {
        rec->fields_[0] = expr::Value(*tc.return_values[i]);
      } else {
        rec->fields_[0] = expr::Value();  // nil
      }
      
      rec->fields_[1] = expr::Value(double(group_key));
      
      rec->fields_[2] = expr::Value(tc.comparison_values[i]);
      
      test_records.emplace_back(std::move(rec));
    }
    
    auto status = param->stages_[0]->Execute(test_records);
    ASSERT_TRUE(status.ok()) << "Execution failed: " << status;
    ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record";
    
    auto record = test_records.pop_front();
    
    // Result field layout after groupby with 3 input fields:
    // Field 0: Nil (n1 not in output)
    // Field 1: group key (@n2)
    // Field 2: Nil (n3 not in output)
    // Field 3: reducer result
    size_t result_field_index = 3;
    
    if (tc.expected_result.has_value()) {
      ASSERT_TRUE(record->fields_.at(result_field_index).IsDouble()) 
          << "Result should be double when optimal record has non-nil return value"
          << " in " << tc.description;
      double result = *(record->fields_.at(result_field_index).AsDouble());
      EXPECT_NEAR(result, *tc.expected_result, 0.001)
          << "Property violated: Expected " << *tc.expected_result
          << ", got " << result << " in " << tc.description;
    } else {
      EXPECT_TRUE(record->fields_.at(result_field_index).IsNil())
          << "Property violated: Expected nil when optimal record has nil return value in "
          << tc.description;
    }
  }
  
  std::cerr << "Property test completed: " << iteration 
            << " iterations validated\n";
  EXPECT_GE(iteration, 100) 
      << "Property test should run at least 100 iterations";
}

// produce correct ordering for both numeric values (numerical comparison) and string values (lexicographic comparison).
TEST_F(AggregateExecTest, FirstValueTypeCompatibilityPropertyTest) {
  // with appropriate comparison semantics. We test with 100+ iterations using groups with both
  // numeric and string comparison values.
  //
  // **Validates: Requirements 2.3, 2.4, 8.1, 8.2, 8.3, 8.4**
  //

  struct PropertyTestCase {
    std::string description;
    std::vector<double> numeric_values;  // For numeric comparison tests
    std::vector<std::string> string_values;  // For string comparison tests
    bool is_numeric;  // true = test numeric, false = test string
    bool is_desc;
  };

  std::vector<PropertyTestCase> test_cases;
  std::mt19937 rng(12345);  // Fixed seed for reproducibility
  std::uniform_int_distribution<size_t> size_dist(2, 20);
  std::uniform_real_distribution<double> value_dist(-1000.0, 1000.0);
  std::uniform_int_distribution<int> bool_dist(0, 1);
  std::uniform_int_distribution<int> char_dist('a', 'z');

  for (size_t i = 0; i < 50; ++i) {
    size_t group_size = size_dist(rng);
    bool is_desc = bool_dist(rng);
    
    std::vector<double> numeric_vals;
    for (size_t j = 0; j < group_size; ++j) {
      numeric_vals.push_back(value_dist(rng));
    }
    
    test_cases.push_back({
      "Numeric comparison, group size " + std::to_string(group_size) + 
        (is_desc ? " DESC" : " ASC"),
      numeric_vals,
      {},  // empty string values
      true,  // is_numeric
      is_desc
    });
  }

  for (size_t i = 0; i < 50; ++i) {
    size_t group_size = size_dist(rng);
    bool is_desc = bool_dist(rng);
    
    std::vector<std::string> string_vals;
    for (size_t j = 0; j < group_size; ++j) {
      // Generate random strings of length 3-8
      size_t str_len = 3 + (rng() % 6);
      std::string str;
      for (size_t k = 0; k < str_len; ++k) {
        str += static_cast<char>(char_dist(rng));
      }
      string_vals.push_back(str);
    }
    
    test_cases.push_back({
      "String comparison, group size " + std::to_string(group_size) + 
        (is_desc ? " DESC" : " ASC"),
      {},  // empty numeric values
      string_vals,
      false,  // is_numeric
      is_desc
    });
  }

  // Negative numbers
  test_cases.push_back({
    "Numeric: negative numbers ASC",
    {-100.0, -50.0, -200.0, -10.0},
    {},
    true,
    false
  });
  
  test_cases.push_back({
    "Numeric: negative numbers DESC",
    {-100.0, -50.0, -200.0, -10.0},
    {},
    true,
    true
  });

  // Mixed positive and negative
  test_cases.push_back({
    "Numeric: mixed positive/negative ASC",
    {100.0, -50.0, 0.0, -100.0, 50.0},
    {},
    true,
    false
  });

  test_cases.push_back({
    "Numeric: mixed positive/negative DESC",
    {100.0, -50.0, 0.0, -100.0, 50.0},
    {},
    true,
    true
  });

  // Very small differences
  test_cases.push_back({
    "Numeric: small differences ASC",
    {1.001, 1.002, 1.000, 1.003},
    {},
    true,
    false
  });

  test_cases.push_back({
    "Numeric: small differences DESC",
    {1.001, 1.002, 1.000, 1.003},
    {},
    true,
    true
  });

  // Lexicographic ordering where "10" < "2" (string comparison)
  test_cases.push_back({
    "String: lexicographic ordering ASC (10 < 2)",
    {},
    {"10", "2", "20", "3"},
    false,
    false
  });

  test_cases.push_back({
    "String: lexicographic ordering DESC (3 > 20)",
    {},
    {"10", "2", "20", "3"},
    false,
    true
  });

  // Case sensitivity
  test_cases.push_back({
    "String: case sensitivity ASC",
    {},
    {"apple", "Apple", "APPLE", "aPpLe"},
    false,
    false
  });

  test_cases.push_back({
    "String: case sensitivity DESC",
    {},
    {"apple", "Apple", "APPLE", "aPpLe"},
    false,
    true
  });

  // Prefixes
  test_cases.push_back({
    "String: prefix ordering ASC",
    {},
    {"abc", "ab", "abcd", "a"},
    false,
    false
  });

  test_cases.push_back({
    "String: prefix ordering DESC",
    {},
    {"abc", "ab", "abcd", "a"},
    false,
    true
  });

  // Empty strings
  test_cases.push_back({
    "String: with empty string ASC",
    {},
    {"", "a", "aa", "aaa"},
    false,
    false
  });

  test_cases.push_back({
    "String: with empty string DESC",
    {},
    {"", "a", "aa", "aaa"},
    false,
    true
  });

  // Special characters
  test_cases.push_back({
    "String: special characters ASC",
    {},
    {"abc", "a-c", "a_c", "a c"},
    false,
    false
  });

  test_cases.push_back({
    "String: special characters DESC",
    {},
    {"abc", "a-c", "a_c", "a c"},
    false,
    true
  });

  size_t iteration = 0;
  for (const auto& tc : test_cases) {
    ++iteration;
    std::cerr << "Property Test Iteration " << iteration << ": " << tc.description << "\n";

    if (tc.is_numeric) {
      // Test numeric comparison
      ASSERT_FALSE(tc.numeric_values.empty()) 
          << "Test case setup error: numeric values should not be empty for numeric test";

      std::string order = tc.is_desc ? "'\"DESC\"'" : "'\"ASC\"'";
      std::string query = "groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 " + order;
      
      auto param = MakeStages(query);
      RecordSet test_records(nullptr);
      
      // Create records with n1 = comparison value (also used as return value)
      double group_key = 999.0;
      for (size_t i = 0; i < tc.numeric_values.size(); ++i) {
        auto rec = std::make_unique<Record>(2);
        rec->fields_[0] = expr::Value(tc.numeric_values[i]);  // n1 = numeric value
        rec->fields_[1] = expr::Value(group_key);  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed (numeric): " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record (numeric)";
      
      auto record = test_records.pop_front();
      
      // Result is at field index 2 when using 2 input fields (n1, n2)
      // Field layout: [0: Nil (n1 not in output), 1: group key (@n2), 2: reducer result]
      size_t result_field_index = 2;
      
      ASSERT_TRUE(record->fields_.at(result_field_index).IsDouble()) << "Result not a double (numeric)";
      
      double result = *(record->fields_.at(result_field_index).AsDouble());
      
      double expected;
      if (tc.is_desc) {
        expected = *std::max_element(tc.numeric_values.begin(), tc.numeric_values.end());
      } else {
        expected = *std::min_element(tc.numeric_values.begin(), tc.numeric_values.end());
      }
      
      EXPECT_NEAR(result, expected, 0.001)
          << "Property violated (numeric): Expected " << expected
          << " (" << (tc.is_desc ? "maximum" : "minimum") << "), got " << result
          << " in " << tc.description;

    } else {
      // Test string comparison
      ASSERT_FALSE(tc.string_values.empty()) 
          << "Test case setup error: string values should not be empty for string test";

      // For string comparison, we need to use the actual string values
      // Since the test infrastructure uses numeric fields, we'll encode strings as their
      // lexicographic order and verify the comparison works correctly
      
      // Create a sorted version to find expected result
      std::vector<std::string> sorted_strings = tc.string_values;
      std::sort(sorted_strings.begin(), sorted_strings.end());
      
      std::string expected_str;
      if (tc.is_desc) {
        expected_str = sorted_strings.back();  // Last in sorted order (maximum)
      } else {
        expected_str = sorted_strings.front();  // First in sorted order (minimum)
      }
      
      auto it = std::find(tc.string_values.begin(), tc.string_values.end(), expected_str);
      ASSERT_NE(it, tc.string_values.end()) << "Expected string not found in original array";
      size_t expected_index = std::distance(tc.string_values.begin(), it);
      
      // Create records using string values directly
      std::string order = tc.is_desc ? "'\"DESC\"'" : "'\"ASC\"'";
      std::string query = "groupby 1 @n2 reduce first_value 4 @n1 '\"BY\"' @n1 " + order;
      
      auto param = MakeStages(query);
      RecordSet test_records(nullptr);
      
      // Create records with n1 = string value (both return and comparison)
      double group_key = 999.0;
      for (size_t i = 0; i < tc.string_values.size(); ++i) {
        auto rec = std::make_unique<Record>(2);
        rec->fields_[0] = expr::Value(tc.string_values[i]);  // n1 = string value
        rec->fields_[1] = expr::Value(group_key);  // n2 = constant for grouping
        test_records.emplace_back(std::move(rec));
      }
      
      auto status = param->stages_[0]->Execute(test_records);
      ASSERT_TRUE(status.ok()) << "Execution failed (string): " << status;
      ASSERT_EQ(test_records.size(), 1) << "Expected one grouped record (string)";
      
      auto record = test_records.pop_front();
      
      // Result is at field index 2 when using 2 input fields (n1, n2)
      // Field layout: [0: Nil (n1 not in output), 1: group key (@n2), 2: reducer result]
      size_t result_field_index = 2;
      
      ASSERT_TRUE(record->fields_.at(result_field_index).IsString()) 
          << "Result not a string (string comparison)";
      
      std::string result = std::string(record->fields_.at(result_field_index).AsStringView());
      
      EXPECT_EQ(result, expected_str)
          << "Property violated (string): Expected \"" << expected_str
          << "\" (" << (tc.is_desc ? "lexicographic maximum" : "lexicographic minimum") 
          << "), got \"" << result << "\" in " << tc.description;
    }
  }
  
  std::cerr << "Property test completed: " << iteration 
            << " iterations validated\n";
  EXPECT_GE(iteration, 100) 
      << "Property test should run at least 100 iterations";
}

/*
TEST_F(AggregateExecTest, testHash) {
  GroupKey key1({expr::Value(1.0), expr::Value(2.0)});
  GroupKey key2({expr::Value(true), expr::Value(), expr::Value(2.0)});
  std::cerr << (key1 == key2) << "\n";
  std::cerr << "Key1: " << key1 << " Key2: " << key2 << "\n";
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly({
      GroupKey{{expr::Value(0.0)}},
      GroupKey{{expr::Value(1.0), expr::Value(2.0)}},
      GroupKey{{expr::Value("a"), expr::Value("b")}},
      GroupKey{{expr::Value("a"), expr::Value(), expr::Value(2.0)}},
      GroupKey{{expr::Value(true), expr::Value()}},
      GroupKey{{expr::Value(false), expr::Value("1.2")}},
  }));
}
*/
}  // namespace aggregate
}  // namespace valkey_search
