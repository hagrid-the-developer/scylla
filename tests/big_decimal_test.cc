/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_MODULE big_decimal

#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include "utils/big_decimal.hh"
#include "exceptions/exceptions.hh"
#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "core/future-util.hh"
#include "transport/messages/result_message.hh"

#include "disk-error-handler.hh"
#include "db/config.hh"


thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

namespace {

void test_div(const char *r_cstr, const int64_t q, const char *expected_cstr) {
    big_decimal r{r_cstr};
    auto res = r / q;
    big_decimal expected{expected_cstr};
    BOOST_REQUIRE_EQUAL(res.unscaled_value(), expected.unscaled_value());
    BOOST_REQUIRE_EQUAL(res.scale(), expected.scale());
}

void test_assignadd(const char *x_cstr, const char *y_cstr, const char *expected_cstr) {
    big_decimal x{x_cstr};
    big_decimal y{y_cstr};
    big_decimal expected{expected_cstr};
    x +=  y;
    BOOST_REQUIRE_EQUAL(x.unscaled_value(), expected.unscaled_value());
    BOOST_REQUIRE_EQUAL(x.scale(), expected.scale());
}

} /* anonymous namespoce */

BOOST_AUTO_TEST_CASE(test_big_decimal_construct_from_string) {
    big_decimal x0{"0"};
    big_decimal x1{"0.0"};
    big_decimal x2{"0.00"};
    big_decimal x3{"0.000"};

    BOOST_REQUIRE_EQUAL(x0.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x0.scale(), 0);

    BOOST_REQUIRE_EQUAL(x1.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x1.scale(), 1);

    BOOST_REQUIRE_EQUAL(x2.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x2.scale(), 2);

    BOOST_REQUIRE_EQUAL(x3.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x3.scale(), 3);
}

BOOST_AUTO_TEST_CASE(test_big_decimal_div) {
    test_div("1", 4, "0");
    test_div("1.00", 4, "0.25");
    test_div("1.000", 4, "0.250");
    test_div("1", 3, "0");
    test_div("1.00", 3, "0.33");
    test_div("1.000", 3, "0.333");
    test_div("11", 10, "1");
    test_div("15", 10, "2");
    test_div("16", 10, "2");
    test_div("25", 10, "2");
    test_div("26", 10, "3");

    test_div("-1", 4, "0");
    test_div("-1.00", 4, "-0.25");
    test_div("-1.000", 4, "-0.250");
    test_div("-1", 3, "0");
    test_div("-1.00", 3, "-0.33");
    test_div("-1.000", 3, "-0.333");
    test_div("-11", 10, "-1");
    test_div("-15", 10, "-2");
    test_div("-16", 10, "-2");
    test_div("-25", 10, "-2");
    test_div("-26", 10, "-3");
}

BOOST_AUTO_TEST_CASE(test_big_decimal_assignadd) {
    test_assignadd("1", "4", "5");
    test_assignadd("1.00", "4.00", "5.00");
    test_assignadd("1.000", "4.000", "5.000");
    test_assignadd("1", "-1", "0");
    test_assignadd("1.00", "-1.00", "0.00");
    test_assignadd("1.000", "-1.000", "0.000");
}
