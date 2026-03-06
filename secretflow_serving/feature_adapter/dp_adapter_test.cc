// Copyright 2024 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "secretflow_serving/feature_adapter/dp_adapter.h"

#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"

#include "secretflow_serving/feature_adapter/feature_adapter_factory.h"
#include "secretflow_serving/util/arrow_helper.h"

#include "secretflow_serving/protos/feature.pb.h"

namespace secretflow::serving::feature {

namespace {

const std::string kTestModelServiceId = "test_service_id";
const std::string kTestPartyId = "alice";

}  // namespace

class DPAdapterTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DPAdapterTest, CreateAdapter) {
  // Test basic adapter creation
  FeatureSourceConfig config;
  auto* dp_opts = config.mutable_dp_opts();
  dp_opts->set_dm_host("localhost:8080");
  dp_opts->set_domaindata_id("test_domain_id");
  dp_opts->set_id_name("id");

  auto model_schema = arrow::schema({arrow::field("x1", arrow::int32()),
                                     arrow::field("x2", arrow::float32()),
                                     arrow::field("x3", arrow::utf8())});

  // This will create the adapter but won't actually connect to DataProxy
  // since we're not calling FetchFeature
  auto adapter = FeatureAdapterFactory::GetInstance()->Create(
      config, kTestModelServiceId, kTestPartyId, model_schema);

  ASSERT_TRUE(adapter != nullptr);
}

// TODO: Add integration tests with a mock DataProxy service
// The following tests require a running DataProxy service or a mock server
//
// TEST_F(DPAdapterTest, FetchFeatureSuccess) {
//   // Test successful feature fetching from DataProxy
//   // This requires setting up a mock DataProxy server
// }
//
// TEST_F(DPAdapterTest, FetchFeatureWithRetry) {
//   // test retry scenario using query_context
//   // Verify that cached data is returned for retry requests
// }
//
// TEST_F(DPAdapterTest, FetchFeatureNetworkError) {
//   // Test network error handling
//   // Verify proper error codes are returned
// }
//
// TEST_F(DPAdapterTest, FetchFeatureInvalidData) {
//   // Test handling of invalid data format
//   // Verify schema validation works correctly
// }

}  // namespace secretflow::serving::feature
