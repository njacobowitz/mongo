/**
 *    Copyright (C) 2017 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <pcrecpp.h>

#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/expression_with_placeholder.h"

namespace mongo {

class InternalSchemaAllowedPropertiesMatchExpression final : public MatchExpression {
public:
    struct Regex {
        Regex(std::string x) : regex(x), serializedRegex(std::move(x)) {}
        Regex clone() const {
            return Regex(serializedRegex);
        };

        pcrecpp::RE regex;
        std::string serializedRegex;
    };

    using PropertiesSet = stdx::unordered_set<std::string>;
    using Placeholder = std::unique_ptr<ExpressionWithPlaceholder>;
    using PatternElem = std::pair<Regex, Placeholder>;
    using PatternArray = std::vector<PatternElem>;

    static constexpr StringData kName = "$_internalSchemaAllowedProperties"_sd;
    static constexpr StringData kProperties = "properties"_sd;
    static constexpr StringData kPatternProperties = "patternProperties"_sd;
    static constexpr StringData kOtherwise = "otherwise"_sd;
    static constexpr StringData kNamePlaceholder = "namePlaceholder"_sd;

    InternalSchemaAllowedPropertiesMatchExpression()
        : MatchExpression(INTERNAL_SCHEMA_ALLOWED_PROPERTIES),
          _otherwise(nullptr),
          _boolOtherwise(true) {}

    void init(PropertiesSet properties,
              PatternArray patternProperties,
              Placeholder otherwise,
              std::string namePlaceholder) {
        invariant(!namePlaceholder.empty());
        _properties = std::move(properties);
        _patternProperties = std::move(patternProperties);
        _otherwise = std::move(otherwise);
        _namePlaceholder = std::move(namePlaceholder);
    }

    void init(PropertiesSet properties,
              PatternArray patternProperties,
              bool otherwise,
              std::string namePlaceholder) {
        if (patternProperties.size() > 0) {
            invariant(!namePlaceholder.empty());
        };
        _properties = std::move(properties);
        _patternProperties = std::move(patternProperties);
        _boolOtherwise = std::move(otherwise);
        _namePlaceholder = std::move(namePlaceholder);
    }

    std::unique_ptr<MatchExpression> shallowClone() const final;

    bool matches(const MatchableDocument* doc, MatchDetails* details) const final {
        return _matchesObject(doc->toBSON());
    }

    bool matchesSingleElement(const BSONElement& e) const final;

    void debugString(StringBuilder& debug, int level) const final;

    void serialize(BSONObjBuilder* out) const final;

    bool equivalent(const MatchExpression* other) const final;

    MatchCategory getCategory() const final {
        return MatchCategory::kOther;
    }

private:
    bool _matchesObject(const BSONObj& e) const;

    PropertiesSet _properties;
    PatternArray _patternProperties;
    Placeholder _otherwise;
    bool _boolOtherwise;
    std::string _namePlaceholder;
};

}  // namespace mongo
