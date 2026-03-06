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

#include "dataproxy_sdk/cc/api.h"
#include "spdlog/spdlog.h"

#include "secretflow_serving/feature_adapter/feature_adapter_factory.h"
#include "secretflow_serving/util/arrow_helper.h"
#include "secretflow_serving/util/csv_util.h"

namespace secretflow::serving::feature {

DPAdapter::DPAdapter(const FeatureSourceConfig& spec,
                     const std::string& service_id,
                     const std::string& party_id,
                     const std::shared_ptr<const arrow::Schema>& feature_schema)
    : FeatureAdapter(spec, service_id, party_id, feature_schema) {
  SERVING_ENFORCE(spec_.has_dp_opts(), errors::ErrorCode::INVALID_ARGUMENT,
                  "invalid dp options");

  dm_host_ = spec_.dp_opts().dm_host();
  domaindata_id_ = spec_.dp_opts().domaindata_id();
  id_name_ = spec_.dp_opts().id_name();
}

void DPAdapter::OnFetchFeature(const Request& request, Response* response) {
  SERVING_ENFORCE_GT(request.fs_param->query_datas_size(), 0);

  // Check if we can use cached data (retry scenario)
  if (!request.fs_param->query_context().empty() &&
      request.fs_param->query_context() == last_query_context_ &&
      cached_features_) {
    response->features = cached_features_;
    return;
  }

  // Clear cache
  cached_features_ = nullptr;
  last_query_context_.clear();

  // Configure DataProxy SDK
  dataproxy_sdk::proto::DataProxyConfig sdk_config;
  sdk_config.set_data_proxy_addr(dm_host_);

  if (spec_.dp_opts().has_tls_config()) {
    sdk_config.mutable_tls_config()->set_certificate_path(
        spec_.dp_opts().tls_config().certificate_path());
    sdk_config.mutable_tls_config()->set_private_key_path(
        spec_.dp_opts().tls_config().private_key_path());
    sdk_config.mutable_tls_config()->set_ca_file_path(
        spec_.dp_opts().tls_config().ca_file_path());
  }

  // Create DataProxy file object
  auto dp_file = dataproxy_sdk::DataProxyFile::Make(sdk_config);

  // Download feature data to temporary file
  std::string temp_file_path =
      fmt::format("/tmp/dp_features_{}.csv",
                  std::chrono::system_clock::now().time_since_epoch().count());

  dataproxy_sdk::proto::DownloadInfo download_info;
  download_info.set_domaindata_id(domaindata_id_);

  try {
    dp_file->DownloadFile(download_info, temp_file_path,
                         dataproxy_sdk::proto::FileFormat::CSV);
    dp_file->Close();

    SPDLOG_INFO(
        "DPAdapter: download feature data from domain id {} to {} success",
        domaindata_id_, temp_file_path);

    // Use CSVExtractor to extract features
    csv::CSVExtractor extractor(feature_schema_, temp_file_path, id_name_);

    response->features =
        extractor.ExtractRows(feature_schema_, request.fs_param->query_datas());

    // Cache result
    cached_features_ = response->features;
    last_query_context_ = request.fs_param->query_context();

    // Clean up temporary file
    std::remove(temp_file_path.c_str());

  } catch (const std::exception& e) {
    dp_file->Close();
    std::remove(temp_file_path.c_str());
    SERVING_THROW(errors::ErrorCode::IO_ERROR,
                  "DPAdapter: failed to download feature data: {}", e.what());
  }
}

REGISTER_ADAPTER(FeatureSourceConfig::OptionsCase::kDpOpts, DPAdapter);

}  // namespace secretflow::serving::feature
