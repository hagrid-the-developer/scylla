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
    BOOST_REQUIRE_EQUAL(res.to_string(), expected_cstr);
}

} /* anonymous namespoce */

BOOST_AUTO_TEST_CASE(test_big_decimal_div) {
    test_div("1", 4, "0");
    test_div("1.00", 4, "0.25");
    test_div("1.000", 4, "0.25");
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
    test_div("-1.000", 4, "-0.25");
    test_div("-1", 3, "0");
    test_div("-1.00", 3, "-0.33");
    test_div("-1.000", 3, "-0.333");
    test_div("-11", 10, "-1");
    test_div("-15", 10, "-2");
    test_div("-16", 10, "-2");
    test_div("-25", 10, "-2");
    test_div("-26", 10, "-3");
}
