/*
 * Copyright 2017-present varchar.io
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

#include <uriparser/Uri.h>

#include "NFS.h"
#include "common/Likely.h"

/**
 * File system and related APIs
 */
namespace nebula {
namespace storage {

std::unique_ptr<NFileSystem> makeFS(const std::string& proto, const std::string& bucket) {
  if (proto == "local") {
    return std::make_unique<nebula::storage::local::File>();
  }

  if (proto == "s3") {
    return std::make_unique<nebula::storage::aws::S3>(bucket);
  }

  if (proto == "gs") {
    return std::make_unique<nebula::storage::gcp::GCS>(bucket);
  }

  throw NException(fmt::format("unsupported file system: {0}", proto));
}

UriInfo parse(const std::string& strUri) {
  UriUriA uri;
  UriParserStateA state;
  state.uri = &uri;
  if (uriParseUriA(&state, strUri.c_str()) != URI_SUCCESS) {
    throw NException(fmt::format("Error parsing {0} at {1}: {2}", strUri, state.errorPos, state.errorCode));
  }

// got uri parsed successfully
#define UF_STR(F) std::string(uri.F.first, uri.F.afterLast - uri.F.first)
  UriInfo info;
  if (uri.scheme.first) {
    info.schema = UF_STR(scheme);
  }

  if (uri.hostText.first) {
    info.host = UF_STR(hostText);
  }

  // TODO(cao) - doesn't seem system compatible? probably remove
  // handle local path without starting '/'
  if (info.schema.empty() && info.host.empty() && strUri.at(0) != '/') {
    info.path = fmt::format("/{0}", strUri);
  } else if (uri.pathHead) {
    const UriPathSegmentA* p = uri.pathHead;
    const char* start = p->text.first;
    const char* end = nullptr;

    // locate the last position
    // why do we need a loop for the start and end, pathTail won't help if last character is "/"
    for (; p; p = p->next) {
      if (LIKELY(p->text.first != p->text.afterLast)) {
        end = p->text.afterLast;
      }
    }

    // if schema is empty, assume it's a local file.
    // allow it to carry the char before it.
    if (info.schema.empty() || info.host.empty()) {
      start -= 1;
    }

    info.path = std::string(start, end - start);

    // decode the data in place, and mark end position to be 0
    char* head = info.path.data();
    const char* tail = uriUnescapeInPlaceExA(head, false, UriBreakConversion::URI_BR_DONT_TOUCH);
    info.path.resize(tail - head);
  }

#undef UF_STR

  // this is c-library requiring an explicit free
  uriFreeUriMembersA(&uri);

  // return the just constructed info
  return info;
}

} // namespace storage
} // namespace nebula