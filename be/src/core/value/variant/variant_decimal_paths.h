// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "core/string_ref.h"

namespace doris {

// One Schema Template entry of a VARIANT column whose declared type is DECIMAL or ARRAY<DECIMAL>, as
// the FE sends it on the VARIANT type descriptor (TScalarType.variant_decimal_paths). `pattern` is the
// template's own spelling: a dotted path for MATCH_NAME, a glob for MATCH_NAME_GLOB.
struct VariantDecimalPath {
    std::string pattern;
    bool is_glob = false;
    int32_t precision = 0;
    int32_t scale = 0;
};

// The DECIMAL template paths of one VARIANT type. Built once per type, shared by its serde and by the
// parse functions, consulted only when a JSON number token carries a fraction.
class VariantDecimalPathSet {
public:
    explicit VariantDecimalPathSet(std::vector<VariantDecimalPath> paths);

    bool empty() const noexcept { return _paths.empty(); }
    const std::vector<VariantDecimalPath>& paths() const noexcept { return _paths; }

    // True when `path` (the object keys from the root joined by '.') is a declared DECIMAL path,
    // using the same name / glob rules the shredder applies to typed paths.
    bool matches(std::string_view path) const;

private:
    std::vector<VariantDecimalPath> _paths;
    std::unordered_set<std::string> _exact;
    std::vector<std::string> _globs;
};

using VariantDecimalPathSetPtr = std::shared_ptr<const VariantDecimalPathSet>;

// Rewrites `json` so that every number token with a fraction and no exponent (`-?digits.digits`) that
// sits on a declared DECIMAL path becomes a JSON string of the same characters; every other byte is
// copied as it is. The parser then takes the documented exact route for that value — a string on a
// DECIMAL template path is cast from its text at flush — instead of materializing a binary64 first.
// Integers and exponent forms are left alone (integers are already exact, exponent forms stay DOUBLE
// as before), and so is every path the template does not declare as DECIMAL.
//
// Returns true and fills `out` when at least one token was quoted. Returns false, leaving `out`
// unspecified, when no token qualified or when the text is not JSON this scanner understands; the
// caller then parses the original text and the parser renders its own verdict. Object keys are matched
// as written between their quotes, so a key that needs an escape sequence never matches and keeps the
// old behavior. Never throws.
bool quote_decimal_numbers_on_paths(StringRef json, const VariantDecimalPathSet& paths,
                                    std::string* out);

} // namespace doris
