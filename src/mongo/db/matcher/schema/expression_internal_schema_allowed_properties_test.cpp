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
 *    all of the code used other than as permitted hereallowedProps. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/json.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/matcher/extensions_callback_disallow_extensions.h"
#include "mongo/db/matcher/schema/expression_internal_schema_allowed_properties.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

namespace {

TEST(InternalSchemaAllowedPropertiesMatchExpression, RejectsNonObjectElements) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_FALSE(allowedProps->matchesSingleElement(BSON("a" << 1).firstElement()));
    ASSERT_FALSE(allowedProps->matchesSingleElement(BSON("a"
                                                         << "string")
                                                        .firstElement()));
    ASSERT_FALSE(
        allowedProps->matchesSingleElement(BSON("a" << BSON_ARRAY(1 << 2)).firstElement()));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, CorrectlyMatchesProperties) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a', 'b'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^x/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'string'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a" << 1)));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a" << 1 << "b" << 1)));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("b" << BSONObj())));
}


TEST(InternalSchemaAllowedPropertiesMatchExpression, CorrectlyMatchesPatternProperties) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['x'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'number'}}}],"
        "otherwise: {i: {$type: 'string'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a" << 1)));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("aa" << 1)));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("ba" << 1)));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("b" << BSONObj())));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, CorrectlyMatchesOtherwise) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['x'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^x/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a" << 1)));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("b" << 2)));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("c"
                                                << "string")));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression,
     CorrectlyMatchesPropertiesAndPatternPropertiesAndOtherwise) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['x'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_TRUE(allowedProps->matchesBSON(BSON("x" << BSON("z" << 1))));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a"
                                               << "string")));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("c" << 5)));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("c"
                                                << "string")));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("abc" << 3)));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, CorrectlyMatchesWithPropertiesAbsent) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^x/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a" << 1)));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("b" << 2)));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, CorrectlyMatchesWithPatternPropertiesAbsent) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['x'],"
        "namePlaceholder: 'i',"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a" << 1)));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("b" << 2)));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, CorrectlyMatchesWithOtherwiseAbsent) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['x'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}]"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_TRUE(allowedProps->matchesBSON(BSON("x" << BSON("z" << 1))));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a"
                                               << "string")));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("c" << 5)));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("c"
                                               << "string")));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("abc" << 3)));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, CorrectlyMatchesWithOtherwiseFalse) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['x'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: false"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_TRUE(allowedProps->matchesBSON(BSON("x" << BSON("z" << 1))));
    ASSERT_TRUE(allowedProps->matchesBSON(BSON("a"
                                               << "string")));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("c" << 5)));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("c"
                                                << "string")));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("abc" << 3)));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression,
     CorrectlyRejectsEverythingWhenPropertiesAndPatternPropertiesAbsentAndOtherwiseFalse) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "otherwise: false"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_FALSE(allowedProps->matchesBSON(BSON("a" << 1)));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("b" << 2)));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, CorrectlyRejectsNotAllowedProperties) {
    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a', 'b'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^x/, expression: {i: {$type: 'number'}}}],"
        "otherwise: {i: {$type: 'string'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = std::move(parsedExpr.getValue());

    ASSERT_FALSE(allowedProps->matchesBSON(BSON("c" << 1 << "d" << 2)));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("a" << 1 << "c" << 1)));
    ASSERT_FALSE(allowedProps->matchesBSON(BSON("a" << 1 << "b" << 1 << "d" << 1)));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, EquivalentReturnsCorrectResults) {
    auto filter1 = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr1 =
        MatchExpressionParser::parse(filter1, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr1.getStatus());
    auto allowedProps1 = std::move(parsedExpr1.getValue());

    auto filter2 = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr2 =
        MatchExpressionParser::parse(filter2, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr2.getStatus());
    auto allowedProps2 = std::move(parsedExpr2.getValue());

    auto filter3 = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^b/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr3 =
        MatchExpressionParser::parse(filter3, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr3.getStatus());
    auto allowedProps3 = std::move(parsedExpr3.getValue());

    auto filter4 = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'number'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr4 =
        MatchExpressionParser::parse(filter4, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr4.getStatus());
    auto allowedProps4 = std::move(parsedExpr4.getValue());

    auto filter5 = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'string'}}"
        "}}");
    auto parsedExpr5 =
        MatchExpressionParser::parse(filter5, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr5.getStatus());
    auto allowedProps5 = std::move(parsedExpr5.getValue());

    auto filter6 = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['b', 'a'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr6 =
        MatchExpressionParser::parse(filter6, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr6.getStatus());
    auto allowedProps6 = std::move(parsedExpr6.getValue());

    auto filter7 = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a', 'b'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr7 =
        MatchExpressionParser::parse(filter7, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr7.getStatus());
    auto allowedProps7 = std::move(parsedExpr7.getValue());

    auto filter8 = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a'],"
        "namePlaceholder: 'j',"
        "patternProperties: [{regex: /^a/, expression: {j: {$type: 'string'}}}],"
        "otherwise: {j: {$type: 'number'}}"
        "}}");
    auto parsedExpr8 =
        MatchExpressionParser::parse(filter8, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr8.getStatus());
    auto allowedProps8 = std::move(parsedExpr8.getValue());

    ASSERT_TRUE(allowedProps1->equivalent(allowedProps2.get()));
    ASSERT_TRUE(allowedProps2->equivalent(allowedProps1.get()));

    ASSERT_FALSE(allowedProps1->equivalent(allowedProps3.get()));
    ASSERT_FALSE(allowedProps3->equivalent(allowedProps2.get()));

    ASSERT_FALSE(allowedProps1->equivalent(allowedProps4.get()));
    ASSERT_FALSE(allowedProps4->equivalent(allowedProps2.get()));

    ASSERT_FALSE(allowedProps1->equivalent(allowedProps5.get()));
    ASSERT_FALSE(allowedProps5->equivalent(allowedProps2.get()));

    ASSERT_TRUE(allowedProps6->equivalent(allowedProps7.get()));
    ASSERT_TRUE(allowedProps7->equivalent(allowedProps6.get()));

    ASSERT_FALSE(allowedProps1->equivalent(allowedProps8.get()));
    ASSERT_FALSE(allowedProps8->equivalent(allowedProps2.get()));
}

TEST(InternalSchemaAllowedPropertiesMatchExpression, EquivalentToClone) {

    auto filter = fromjson(
        "{$_internalSchemaAllowedProperties: {"
        "properties: ['a'],"
        "namePlaceholder: 'i',"
        "patternProperties: [{regex: /^a/, expression: {i: {$type: 'string'}}}],"
        "otherwise: {i: {$type: 'number'}}"
        "}}");
    auto parsedExpr =
        MatchExpressionParser::parse(filter, ExtensionsCallbackDisallowExtensions(), nullptr);
    ASSERT_OK(parsedExpr.getStatus());
    auto allowedProps = parsedExpr.getValue().release();

    auto clone = allowedProps->shallowClone();
    ASSERT_TRUE(allowedProps->equivalent(clone.get()));
}

}  // namespace
}  // namespace mongo
