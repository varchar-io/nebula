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

#pragma once

#include <array>
#include <cmath>
#include <folly/Conv.h>

#include "common/Errors.h"
#include "common/Memory.h"
#include "surface/DataSurface.h"

/*
 * Implement nebula data partitioning algo.
 */
namespace nebula {
namespace meta {

// make all 1s mask based on number of bits
// such as 1 -> 0x1
//         2 -> 0x11
//         3 -> 0x111
template <typename T>
T msk(size_t bits) {
  // do not mask any value, space will be its index
  if (bits == 0) {
    return 0;
  }

  T value = 1;
  if (bits > 1) {
    value <<= --bits;
    value |= msk<T>(bits);
    return value;
  }

  return value;
}

// define a single predicate
template <typename T>
class Predicate {
public:
  explicit Predicate(const std::string& n, std::vector<T> v)
    : name_{ n }, values_{ std::move(v) } {
  }

  std::string_view name() const {
    return name_;
  }

  const std::vector<T>& values() const {
    return values_;
  }

private:
  std::string name_;
  std::vector<T> values_;
};

struct Span {
  size_t begin;
  size_t end;

  friend inline bool operator==(const Span& s1, const Span& s2) {
    return s1.begin == s2.begin && s1.end == s2.end;
  }

  friend inline Span operator*(const Span& s, size_t product) {
    return { s.begin * product, s.end * product };
  }

  friend inline Span operator+(const Span& s1, const Span& s2) {
    return { s1.begin + s2.begin, s1.end + s2.end };
  }
};

// recording position of any given value, it follows
// position = space * chunk + offset;
struct Position {
  size_t space;
  size_t offset;

  friend inline bool operator==(const Position& p1, const Position& p2) {
    return p1.space == p2.space && p1.offset == p2.offset;
  }
};

template <typename T>
class PartitionKey;

class PK {
public:
  PK(const std::string& name, nebula::type::Kind kind)
    : name_{ name }, kind_{ kind } {
#define DISPATCH_KIND(KIND, READ)                             \
  case nebula::type::Kind::KIND: {                            \
    dispatch_ = [this](const nebula::surface::RowData& row) { \
      return space(row.READ(name_));                          \
    };                                                        \
    break;                                                    \
  }

    switch (kind_) {
      DISPATCH_KIND(BOOLEAN, readBool)
      DISPATCH_KIND(TINYINT, readByte)
      DISPATCH_KIND(SMALLINT, readShort)
      DISPATCH_KIND(INTEGER, readInt)
      DISPATCH_KIND(BIGINT, readLong)
    case nebula::type::Kind::VARCHAR: {
      dispatch_ = [this](const nebula::surface::RowData& row) {
        std::string str(row.readString(name_));
        return space(str);
      };
      break;
    }
    default:
      throw NException("partition column type not supported.");
    }

#undef DISPATCH_KIND
  }

  virtual ~PK() = default;

public:
  virtual size_t spaces() const = 0;
  virtual size_t width() const = 0;
  virtual Span range(const std::vector<std::string>&) const = 0;
  // for given string value requires type conversion
  virtual Position slot(const std::string&) const = 0;

public:
  inline const std::string& name() const {
    return name_;
  }

  inline Position slot(const nebula::surface::RowData& row) const {
    return dispatch_(row);
  }

  template <typename T>
  inline Position space(const T& v) const {
    auto p = static_cast<const PartitionKey<T>*>(this);
    return p->space(v);
  }

  template <typename T>
  inline T value(size_t bits, size_t space) const {
    auto p = static_cast<const PartitionKey<T>*>(this);
    return p->value(bits, space);
  }

  template <typename T>
  inline std::vector<T> values(size_t space) const {
    auto p = static_cast<const PartitionKey<T>*>(this);
    return p->values(space);
  }

protected:
  // partition key name
  std::string name_;
  // partition key type kind
  nebula::type::Kind kind_;

  // dispatch a row to its designated space
  std::function<Position(const nebula::surface::RowData& row)> dispatch_;
};

// define partition key with value type T and cardinality N
// partition key is designed to know its pre-set values
// such as examples in real life: gender, age, country, etc.
template <typename T>
class PartitionKey : public PK {
  using SpaceType = uint8_t;
  static constexpr auto SpaceLimit = std::numeric_limits<SpaceType>::max();

public:
  explicit PartitionKey(const std::string& n, const std::vector<T>& v, SpaceType c = 1)
    : PK(n, nebula::type::TypeDetect<T>::kind),
      values_{ v },
      chunk_{ c } {
    N_ENSURE(chunk_ > 0, "chunk size must be positive");

    // calculate how many spaces does this key needs
    const auto N = values_.size();
    size_t s = (N / chunk_) + (N % chunk_ ? 1 : 0);
    N_ENSURE_LT(s, SpaceLimit, "too many spaces, please increase chunk size.");
    spaces_ = (SpaceType)s;

    // get value mask to filter the index value in each space no matter what value is given
    bits_ = std::ceil(std::log2(chunk_));
    mask_ = msk<SpaceType>(bits_);
  }
  virtual ~PartitionKey() = default;

public:
  inline size_t spaces() const override {
    return spaces_;
  }

  inline size_t width() const override {
    return bits_;
  }

  inline SpaceType mask() const {
    return mask_;
  }

  // get the value for given bits in specific space
  // bits = BESS encoding result
  inline auto value(size_t bits, size_t space) const
    -> std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T> {
    // chunk + internal
    auto index = (space * chunk_) + (mask_ & bits);
    return values_.at(index);
  }

  inline auto values(size_t space) const {
    auto start = space * chunk_;
    auto end = std::min(start + chunk_, values_.size());
    return std::vector<T>(values_.begin() + start, values_.begin() + end);
  }

  // get which space the value is at
  // TODO(cao): use hash_map to speed the look up?
  // TODO(cao): or ensure values_ are sorted, use bin-search here.
  inline Position space(const T& v) const {
    auto itr = std::find(values_.begin(), values_.end(), v);
    N_ENSURE(itr != values_.end(), "given value is not in dimension");
    auto index = (size_t)std::distance(values_.begin(), itr);
    return { index / chunk_, index % chunk_ };
  }

  // for generic function that doesn't know the internal type
  inline Position slot(const std::string& v) const override {
    if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>) {
      return space(v);
    } else {
      return space(folly::to<T>(v));
    }
  }

#define SPAN_COMPUTE(F, E)  \
  size_t end = spaces_ - 1; \
  if (v.empty()) {          \
    return { 0, end };      \
  }                         \
  Span r{ end, 0 };         \
  for (const E& e : v) {    \
    auto sp = F(e).space;   \
    if (sp < r.begin) {     \
      r.begin = sp;         \
    }                       \
    if (sp > r.end) {       \
      r.end = sp;           \
    }                       \
  }                         \
  return r;

  // get space span (inclusive range) for given value list
  inline Span span(const std::vector<T>& v) const {
    SPAN_COMPUTE(space, T)
  }

  Span range(const std::vector<std::string>& v) const override {
    SPAN_COMPUTE(slot, std::string)
  }

#undef SPAN_COMPUTE

private:
  // partition key cardinality
  std::vector<T> values_;
  // partition value space chunking size
  SpaceType chunk_;

  // cache value of space bits
  SpaceType spaces_;
  SpaceType bits_;
  SpaceType mask_;
};

template <>
inline Position PK::space<std::string_view>(const std::string_view& v) const {
  auto p = static_cast<const PartitionKey<std::string>*>(this);
  std::string str(v);
  return p->space(str);
}

template <>
inline std::string_view PK::value<std::string_view>(size_t bits, size_t space) const {
  auto p = static_cast<const PartitionKey<std::string>*>(this);
  return p->value(bits, space);
}

// a cube defines a multi-dimensional space.
// it accepts a tuple of partition keys
// and provides all function mapping on these dimension definitions
template <typename... P>
class Cube {
  using Tuple = typename std::tuple<P...>;
  static constexpr size_t TupleSize = std::tuple_size<typename std::remove_reference<Tuple>::type>::value;
  // Note: a good discussion on iterating over a tuple
  // https://stackoverflow.com/questions/1198260/how-can-you-iterate-over-the-elements-of-an-stdtuple
  // Basically two approaches 1. use std::apply 2. use if constexpr and recursive

public:
  Cube(P... keys) : keys_{ std::forward<P>(keys)... } {
    // caculate total combination of possible blocks
    // we're doing multi-dimension (spaces) mapping to single dimension (partition ID)
    // {S1, S2, S3} => Pi, for example, P = S3*(Spaces(2)*Spaces(1))+S2*Spaces(1)+S1
    auto spaces = 1;
    auto index = 0;
    std::apply([&spaces, &index, &offsets = offsets_](auto&&... key) { ((offsets[index++] = spaces, spaces *= key.spaces()), ...); },
               keys_);
    capacity_ = spaces;
  }

  // max number of blocks = product of (Ki spaces)
  size_t capacity() const {
    return capacity_;
  }

  inline constexpr size_t numKeys() const {
    return TupleSize;
  }

  inline size_t offset(size_t index) const {
    return offsets_.at(index);
  }

  template <typename... T>
  inline size_t pod(const std::tuple<T...>& vs) const {
    return pod(vs, std::make_index_sequence<TupleSize>());
  }

  template <typename... T>
  inline Span span(const std::tuple<std::vector<T>...>& ps) const {
    return span(ps, std::make_index_sequence<TupleSize>());
  }

  // we can find out each space for any dimension for any given ID
  // take 3 dimensions as example, their max spaces are {S1, S2, S3}
  // Given P = K1*1 + K2*S1 + K3*(S1*S2)
  // =>
  //   K3 = P/(S1 * S2)
  //   K2 = (P - K3 * S1 * S2) / S1
  //   K1 = (P - K3*S1*S2 - K2*S1)
  inline std::array<size_t, TupleSize> locate(size_t pid) const {
    std::array<size_t, TupleSize> dimensions;

    // from last item
    auto value = pid;
    for (size_t i = TupleSize; i > 0; --i) {
      auto index = i - 1;
      auto offset = offsets_.at(index);
      auto space = value / offset;
      value -= space * offset;

      // save space value for current dimension
      dimensions[index] = space;
    }

    return dimensions;
  }

private:
  template <typename... T, size_t... S>
  inline auto pod(const std::tuple<T...>& vs, std::index_sequence<S...>) const {
    size_t pod = 0;
    ((pod += offsets_[S] * std::get<S>(keys_).space(std::get<S>(vs)).space), ...);
    return pod;
  }

  template <typename... T, size_t... S>
  inline auto span(const std::tuple<std::vector<T>...>& ps, std::index_sequence<S...>) const {
    Span span{ 0, 0 };
    ((span = span + std::get<S>(keys_).span(std::get<S>(ps)) * offsets_[S]), ...);
    return span;
  }

private:
  Tuple keys_;
  std::array<size_t, TupleSize> offsets_;
  size_t capacity_;
};

} // namespace meta
} // namespace nebula