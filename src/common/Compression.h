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

#pragma once

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <fstream>
#include <iostream>

/**
 * Value conversion utility
 */
namespace nebula {
namespace common {

// ungzip a file into a new file
static void ungzip(const std::string& source, const std::string& dest) {
  // build input and output streams
  std::ifstream input(source, std::ios_base::in | std::ios_base::binary);
  std::ofstream output(dest, std::ios_base::out | std::ios_base::binary);

  // build the decompression filter
  boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
  inbuf.push(boost::iostreams::gzip_decompressor());
  inbuf.push(input);

  // copy everything over
  boost::iostreams::copy(inbuf, output);

  // unncessary cleanup but cool
  input.close();
  output.close();
}

static inline size_t filesize(const std::string& file) {
  return std::ifstream(file, std::ifstream::ate | std::ifstream::binary).tellg();
}

} // namespace common
} // namespace nebula