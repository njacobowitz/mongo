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

#include "mongo/platform/basic.h"

#include "mongo/db/matcher/schema/expression_internal_schema_allowed_properties.h"

namespace mongo {

constexpr StringData InternalSchemaAllowedPropertiesMatchExpression::kName;
constexpr StringData InternalSchemaAllowedPropertiesMatchExpression::kProperties;
constexpr StringData InternalSchemaAllowedPropertiesMatchExpression::kPatternProperties;
constexpr StringData InternalSchemaAllowedPropertiesMatchExpression::kOtherwise;
constexpr StringData InternalSchemaAllowedPropertiesMatchExpression::kNamePlaceholder;

std::unique_ptr<MatchExpression> InternalSchemaAllowedPropertiesMatchExpression::shallowClone()
    const {
    auto newExpression = stdx::make_unique<InternalSchemaAllowedPropertiesMatchExpression>();

    if (getTag()) {
        newExpression->setTag(getTag()->clone());
    }

    PatternArray clonedExpressions;
    for (auto&& elem : _patternProperties) {
        clonedExpressions.emplace_back(PatternElem(elem.first.clone(),
                                                   stdx::make_unique<ExpressionWithPlaceholder>(
                                                       elem.second->getPlaceholder().toString(),
                                                       elem.second->getFilter()->shallowClone())));
    }

    if (_otherwise) {
        newExpression->init(
            std::move(_properties),
            std::move(clonedExpressions),
            stdx::make_unique<ExpressionWithPlaceholder>(_otherwise->getPlaceholder().toString(),
                                                         _otherwise->getFilter()->shallowClone()),
            std::move(_namePlaceholder));

    } else {
        newExpression->init(std::move(_properties),
                            std::move(clonedExpressions),
                            std::move(_boolOtherwise),
                            std::move(_namePlaceholder));
    }

    return std::move(newExpression);
}

bool InternalSchemaAllowedPropertiesMatchExpression::_matchesObject(const BSONObj& obj) const {
    for (auto&& item : obj) {
        bool checkOtherwise = true;

        if (_properties.find(item.fieldName()) != _properties.end()) {
            checkOtherwise = false;
        }

        for (auto&& elem : _patternProperties) {
            auto matchExp = elem.second->getFilter();

            if (elem.first.regex.PartialMatch(item.fieldName())) {
                checkOtherwise = false;
                if (!matchExp->matchesSingleElement(item)) {
                    return false;
                }
            }
        }

        if (checkOtherwise) {
            if (_otherwise) {
                if (!_otherwise->getFilter()->matchesSingleElement(item)) {
                    return false;
                }
            } else if (!_boolOtherwise) {
                return false;
            }
        }
    }

    return true;
}

bool InternalSchemaAllowedPropertiesMatchExpression::matchesSingleElement(
    const BSONElement& elem) const {
    if (elem.type() != BSONType::Object) {
        return false;
    }

    return _matchesObject(elem.Obj());
}

void InternalSchemaAllowedPropertiesMatchExpression::debugString(StringBuilder& debug,
                                                                 int level) const {
    _debugAddSpace(debug, level);

    BSONObjBuilder builder;
    serialize(&builder);
    debug << builder.obj().toString() << "\n";

    const auto* tag = getTag();
    if (tag) {
        debug << " ";
        tag->debugString(&debug);
    }
    debug << "\n";
}

void InternalSchemaAllowedPropertiesMatchExpression::serialize(BSONObjBuilder* out) const {
    BSONObjBuilder allowedPropBob(out->subobjStart(kName));
    BSONArrayBuilder propBob(allowedPropBob.subarrayStart(kProperties));
    for (auto&& prop : _properties) {
        propBob.append(prop);
    }
    propBob.doneFast();

    allowedPropBob.append(kNamePlaceholder, _namePlaceholder);

    BSONArrayBuilder patternPropBob(allowedPropBob.subarrayStart(kPatternProperties));
    for (auto&& item : _patternProperties) {
        BSONObjBuilder objBuilder(patternPropBob.subobjStart());
        objBuilder.appendRegex("regex", item.first.serializedRegex);

        BSONObjBuilder subBob(objBuilder.subobjStart("expression"));
        item.second->getFilter()->serialize(&subBob);
        subBob.doneFast();
        objBuilder.doneFast();
    }
    patternPropBob.doneFast();

    if (_otherwise) {
        BSONObjBuilder otherwiseBob(allowedPropBob.subobjStart(kOtherwise));
        _otherwise->getFilter()->serialize(&otherwiseBob);
        otherwiseBob.doneFast();
    } else {
        allowedPropBob.append(kOtherwise, _boolOtherwise);
    }

    allowedPropBob.doneFast();
}

bool InternalSchemaAllowedPropertiesMatchExpression::equivalent(
    const MatchExpression* other) const {
    if (matchType() != other->matchType()) {
        return false;
    }
    const InternalSchemaAllowedPropertiesMatchExpression* realOther =
        static_cast<const InternalSchemaAllowedPropertiesMatchExpression*>(other);

    if (_properties != realOther->_properties) {
        return false;
    }

    if (_otherwise) {
        if (!_otherwise->getFilter()->equivalent(realOther->_otherwise->getFilter()) ||
            _otherwise->getPlaceholder() != realOther->_otherwise->getPlaceholder()) {
            return false;
        }
    } else if (_boolOtherwise != realOther->_boolOtherwise) {
        return false;
    }

    if (_namePlaceholder != realOther->_namePlaceholder) {
        return false;
    }

    return std::is_permutation(
        _patternProperties.begin(),
        _patternProperties.end(),
        realOther->_patternProperties.begin(),
        realOther->_patternProperties.end(),
        [](const auto& expr1, const auto& expr2) {
            return (expr1.second->getFilter()->equivalent(expr2.second->getFilter()) &&
                    expr1.first.serializedRegex == expr2.first.serializedRegex);
        });
}

}  // namespace mongo
