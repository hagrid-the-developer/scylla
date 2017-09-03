/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
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


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

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

template<typename T>
struct cql_type_name {
};
template<>
struct cql_type_name<int> {
    static constexpr char value[] = "int";
};
constexpr char cql_type_name<int>::value[];
template<>
struct cql_type_name<long> {
    static constexpr char value[] = "bigint";
};
constexpr char cql_type_name<long>::value[];
template<>
struct cql_type_name<float> {
    static constexpr char value[] = "float";
};
constexpr char cql_type_name<float>::value[];
template<>
struct cql_type_name<double> {
    static constexpr char value[] = "double";
};
constexpr char cql_type_name<double>::value[];

template<typename RetType, typename Type>
auto test_explicit_type_casting_in_avg_function() {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql(sprint("CREATE TABLE air_quality_data (sensor_id text, time timestamp, co_ppm %s, PRIMARY KEY (sensor_id, time));", cql_type_name<Type>::value)).get();
        e.execute_cql(
            "begin unlogged batch \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:00', 17); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:01', 18); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:02', 19); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:03', 20); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:04', 30); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:05', 31); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:10', 20); \n"
            "apply batch;").get();
            auto msg = e.execute_cql(sprint("select avg(CAST(co_ppm AS %s)) from air_quality_data;", cql_type_name<RetType>::value)).get0();
            assert_that(msg).is_rows().with_size(1).with_row({{data_type_for<RetType>()->decompose( RetType(17 + 18 + 19 + 20 + 30 + 31 + 20) / RetType(7) )}});
    });
}

} /* anonymous namespace */

SEASTAR_TEST_CASE(test_explicit_type_casting_in_avg_function_int) {
    return test_explicit_type_casting_in_avg_function<double, int>();
}
SEASTAR_TEST_CASE(test_explicit_type_casting_in_avg_function_long) {
    return test_explicit_type_casting_in_avg_function<double, long>();
}

SEASTAR_TEST_CASE(test_explicit_type_casting_in_avg_function_float) {
    return test_explicit_type_casting_in_avg_function<float, float>();
}

SEASTAR_TEST_CASE(test_explicit_type_casting_in_avg_function_double) {
    return test_explicit_type_casting_in_avg_function<double, double>();
}

SEASTAR_TEST_CASE(test_unsupported_conversions) {
    auto validate_request_failure = [] (cql_test_env& env, const sstring& request, const sstring& expected_message) {
        BOOST_REQUIRE_EXCEPTION(env.execute_cql(request).get(),
                                exceptions::invalid_request_exception,
                                [&expected_message](auto &&ire) {
                                    BOOST_REQUIRE_EQUAL(expected_message, ire.what());
                                    return true;
                                });

        return make_ready_future<>();
    };

    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE air_quality_data (sensor_id text, time timestamp, co_ppm text, PRIMARY KEY (sensor_id, time));").get();
        validate_request_failure(e, "select CAST(co_ppm AS int) from air_quality_data", "org.apache.cassandra.db.marshal.UTF8Type cannot be cast to org.apache.cassandra.db.marshal.Int32Type");
    });
}
