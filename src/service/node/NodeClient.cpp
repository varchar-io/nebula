/*
 * Copyright 2017-present Shawn Cao
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "NodeClient.h"

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace service {

void NodeClient::echo(const std::string& name) {
  // build request message through fb builder
  flatbuffers::grpc::MessageBuilder mb;
  auto name_offset = mb.CreateString(name);
  auto request_offset = nebula::service::CreateEchoPing(mb, name_offset);
  mb.Finish(request_offset);
  auto request_msg = mb.ReleaseMessage<EchoPing>();

  // a response message placeholder
  flatbuffers::grpc::Message<EchoReply> response_msg;

  grpc::ClientContext context;
  auto status = stub_->Echo(&context, request_msg, &response_msg);
  if (status.ok()) {
    const EchoReply* response = response_msg.GetRoot();
    LOG(INFO) << "From Node Server: " << response->message()->str();
    return;
  }

  LOG(ERROR) << "RPC failed: code=" << status.error_code() << ", msg=" << status.error_message();
}

void NodeClient::echos(const std::string& name, size_t count) {
  flatbuffers::grpc::MessageBuilder mb;
  auto name_offset = mb.CreateString(name);
  auto request_offset = nebula::service::CreateManyEchoPings(mb, name_offset, count);
  mb.Finish(request_offset);
  auto request_msg = mb.ReleaseMessage<ManyEchoPings>();

  flatbuffers::grpc::Message<EchoReply> response_msg;

  grpc::ClientContext context;
  auto stream = stub_->Echos(&context, request_msg);

  // reading back all replies until server says no more (read returns false)
  while (stream->Read(&response_msg)) {
    const EchoReply* response = response_msg.GetRoot();
    LOG(INFO) << "From Node Server: " << response->message()->str();
  }

  auto status = stream->Finish();
  if (!status.ok()) {
    LOG(ERROR) << "RPC failed: code=" << status.error_code() << ", msg=" << status.error_message();
  }
}

} // namespace service
} // namespace nebula