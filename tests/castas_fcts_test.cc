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
        e.execute_cql("CREATE TABLE air_quality_data_text (sensor_id text, time timestamp, co_ppm text, PRIMARY KEY (sensor_id, time));").get();
        validate_request_failure(e, "select CAST(co_ppm AS int) from air_quality_data_text", "org.apache.cassandra.db.marshal.UTF8Type cannot be cast to org.apache.cassandra.db.marshal.Int32Type");
        e.execute_cql("CREATE TABLE air_quality_data_ascii (sensor_id text, time timestamp, co_ppm ascii, PRIMARY KEY (sensor_id, time));").get();
        validate_request_failure(e, "select CAST(co_ppm AS int) from air_quality_data_ascii", "org.apache.cassandra.db.marshal.AsciiType cannot be cast to org.apache.cassandra.db.marshal.Int32Type");
    });
}

#if 0
    // https://github.com/apache/cassandra/compare/trunk...blerer:10310-3.0#diff-34f509a73496e57ec9d7786e56cad0a0
    public void testInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b text, c double)");

        assertInvalidSyntaxMessage("no viable alternative at input '(' (... b, c) VALUES ([CAST](...)",
                                   "INSERT INTO %s (a, b, c) VALUES (CAST(? AS int), ?, ?)", 1.6, "test", 6.3);

        assertInvalidSyntaxMessage("no viable alternative at input '(' (..." + KEYSPACE + "." + currentTable()
                + " SET c = [cast](...)",
                                   "UPDATE %s SET c = cast(? as double) WHERE a = ?", 1, 1);

        assertInvalidSyntaxMessage("no viable alternative at input '(' (...= ? WHERE a = [CAST] (...)",
                                   "UPDATE %s SET c = ? WHERE a = CAST (? AS INT)", 1, 2.0);

        assertInvalidSyntaxMessage("no viable alternative at input '(' (..." + KEYSPACE + "." + currentTable()
                + " WHERE a = [CAST] (...)",
                                   "DELETE FROM %s WHERE a = CAST (? AS INT)", 1, 2.0);

        assertInvalidSyntaxMessage("no viable alternative at input '(' (..." + KEYSPACE + "." + currentTable()
                + " WHERE a = [CAST] (...)",
                                   "SELECT * FROM %s WHERE a = CAST (? AS INT)", 1, 2.0);

        assertInvalidMessage("a cannot be cast to boolean", "SELECT CAST(a AS boolean) FROM %s");
    }
#endif

SEASTAR_TEST_CASE(test_numeric_casts_in_selection_clause) {
    std::cerr << "XYZ: " << __LINE__ << std::endl;
    return do_with_cql_env_thread([&] (auto& e) {
        std::cerr << "XYZ: " << __LINE__ << std::endl;
        e.execute_cql("CREATE TABLE test (a tinyint primary key,"
                      " b smallint,"
                      " c int,"
                      " d bigint,"
                      " e float,"
                      " f double,"
                      " g decimal,"
                      " h varint,"
                      " i int)").get();

        std::cerr << "XYZ: " << __LINE__ << std::endl;
        e.execute_cql("INSERT INTO test (a, b, c, d, e, f, g, h) VALUES (1, 2, 3, 4, 5.2, 6.3, 7.3, 8)").get();
        std::cerr << "Running conversion..." << std::endl;
        {
            auto msg = e.execute_cql("SELECT CAST(a AS tinyint), "
                                     "CAST(b AS tinyint), "
                                     "CAST(c AS tinyint), "
                                     "CAST(d AS tinyint), "
                                     "CAST(e AS tinyint), "
                                     "CAST(f AS tinyint), "
                                     "CAST(g AS tinyint), "
                                     "CAST(h AS tinyint), "
                                     "CAST(i AS tinyint) FROM test").get0();
            //std::cerr << "XYZ: [8]: " << (int)value_cast<int8_t>( byte_type->deserialize(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[8].value()) ) << ";" << std::endl;
            assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(1))},
                                                              {byte_type->decompose(int8_t(2))},
                                                              {byte_type->decompose(int8_t(3))},
                                                              {byte_type->decompose(int8_t(4))},
                                                              {byte_type->decompose(int8_t(5))},
                                                              {byte_type->decompose(int8_t(6))},
                                                              {byte_type->decompose(int8_t(7))},
                                                              {byte_type->decompose(int8_t(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS smallint), "
                                     "CAST(b AS smallint), "
                                     "CAST(c AS smallint), "
                                     "CAST(d AS smallint), "
                                     "CAST(e AS smallint), "
                                     "CAST(f AS smallint), "
                                     "CAST(g AS smallint), "
                                     "CAST(h AS smallint), "
                                     "CAST(i AS smallint) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{short_type->decompose(int16_t(1))},
                                                              {short_type->decompose(int16_t(2))},
                                                              {short_type->decompose(int16_t(3))},
                                                              {short_type->decompose(int16_t(4))},
                                                              {short_type->decompose(int16_t(5))},
                                                              {short_type->decompose(int16_t(6))},
                                                              {short_type->decompose(int16_t(7))},
                                                              {short_type->decompose(int16_t(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS int), "
                                     "CAST(b AS int), "
                                     "CAST(c AS int), "
                                     "CAST(d AS int), "
                                     "CAST(e AS int), "
                                     "CAST(f AS int), "
                                     "CAST(g AS int), "
                                     "CAST(h AS int), "
                                     "CAST(i AS int) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{int32_type->decompose(int32_t(1))},
                                                              {int32_type->decompose(int32_t(2))},
                                                              {int32_type->decompose(int32_t(3))},
                                                              {int32_type->decompose(int32_t(4))},
                                                              {int32_type->decompose(int32_t(5))},
                                                              {int32_type->decompose(int32_t(6))},
                                                              {int32_type->decompose(int32_t(7))},
                                                              {int32_type->decompose(int32_t(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS bigint), "
                                     "CAST(b AS bigint), "
                                     "CAST(c AS bigint), "
                                     "CAST(d AS bigint), "
                                     "CAST(e AS bigint), "
                                     "CAST(f AS bigint), "
                                     "CAST(g AS bigint), "
                                     "CAST(h AS bigint), "
                                     "CAST(i AS bigint) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(1))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(3))},
                                                              {long_type->decompose(int64_t(4))},
                                                              {long_type->decompose(int64_t(5))},
                                                              {long_type->decompose(int64_t(6))},
                                                              {long_type->decompose(int64_t(7))},
                                                              {long_type->decompose(int64_t(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS float), "
                                     "CAST(b AS float), "
                                     "CAST(c AS float), "
                                     "CAST(d AS float), "
                                     "CAST(e AS float), "
                                     "CAST(f AS float), "
                                     "CAST(g AS float), "
                                     "CAST(h AS float), "
                                     "CAST(i AS float) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{float_type->decompose(float(1))},
                                                              {float_type->decompose(float(2))},
                                                              {float_type->decompose(float(3))},
                                                              {float_type->decompose(float(4))},
                                                              {float_type->decompose(float(5.2))},
                                                              {float_type->decompose(float(6.3))},
                                                              {float_type->decompose(float(7.3))},
                                                              {float_type->decompose(float(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS double), "
                                     "CAST(b AS double), "
                                     "CAST(c AS double), "
                                     "CAST(d AS double), "
                                     "CAST(e AS double), "
                                     "CAST(f AS double), "
                                     "CAST(g AS double), "
                                     "CAST(h AS double), "
                                     "CAST(i AS double) FROM test").get0();
            // Conversions to double cannot be compared with assert_that(), because result
            //   of double conversions may be slightly different from theoretical values.
            auto cmp = [&](::size_t index, double req) {
                auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front();
                auto val = value_cast<double>( double_type->deserialize(row[index].value()) );
                BOOST_CHECK_CLOSE(val, req, 1e-4);
            };
            auto cmp_null = [&](::size_t index) {
                auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front();
                BOOST_CHECK(!row[index]);
            };
            cmp(0, 1.d);
            cmp(1, 2.d);
            cmp(2, 3.d);
            cmp(3, 4.d);
            cmp(4, 5.2d);
            cmp(5, 6.3d);
            cmp(6, 7.3d);
            cmp(7, 8.d);
            cmp_null(8);
            /*
               std::cerr << "XYZ: [0]: " << value_cast<double>( double_type->deserialize(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[0].value()) ) << ";" << std::endl;
             */
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS ascii), "
                                     "CAST(b AS ascii), "
                                     "CAST(c AS ascii), "
                                     "CAST(d AS ascii), "
                                     "CAST(e AS ascii), "
                                     "CAST(f AS ascii), "
                                     "CAST(g AS ascii), "
                                     "CAST(h AS ascii), "
                                     "CAST(i AS ascii) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{ascii_type->decompose("1")},
                                                              {ascii_type->decompose("2")},
                                                              {ascii_type->decompose("3")},
                                                              {ascii_type->decompose("4")},
                                                              {ascii_type->decompose("5.2")},
                                                              {ascii_type->decompose("6.3")},
                                                              {ascii_type->decompose("7.3")},
                                                              {ascii_type->decompose("8")},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS text), "
                                     "CAST(b AS text), "
                                     "CAST(c AS text), "
                                     "CAST(d AS text), "
                                     "CAST(e AS text), "
                                     "CAST(f AS text), "
                                     "CAST(g AS text), "
                                     "CAST(h AS text), "
                                     "CAST(i AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->decompose("1")},
                                                              {utf8_type->decompose("2")},
                                                              {utf8_type->decompose("3")},
                                                              {utf8_type->decompose("4")},
                                                              {utf8_type->decompose("5.2")},
                                                              {utf8_type->decompose("6.3")},
                                                              {utf8_type->decompose("7.3")},
                                                              {utf8_type->decompose("8")},
                                                              {}});
        }
    });
}

#if 0
        assertColumnNames(execute("SELECT CAST(b AS int), CAST(c AS int), CAST(d AS double) FROM %s"),
                          "cast(b as int)",
                          "c",
                          "cast(d as double)");

        assertRows(execute("SELECT CAST(a AS decimal), " +
                "CAST(b AS decimal), " +
                "CAST(c AS decimal), " +
                "CAST(d AS decimal), " +
                "CAST(e AS decimal), " +
                "CAST(f AS decimal), " +
                "CAST(g AS decimal), " +
                "CAST(h AS decimal), " +
                "CAST(i AS decimal) FROM %s"),
                   row(BigDecimal.valueOf(1.0),
                       BigDecimal.valueOf(2.0),
                       BigDecimal.valueOf(3.0),
                       BigDecimal.valueOf(4.0),
                       BigDecimal.valueOf(5.2F),
                       BigDecimal.valueOf(6.3),
                       BigDecimal.valueOf(6.3),
                       BigDecimal.valueOf(4.0),
                       null));

#endif

SEASTAR_TEST_CASE(test_time_casts_in_selection_clause) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test (a timeuuid primary key,"
                      "b timestamp,"
                      "c date,"
                      "d time)").get();

        e.execute_cql("INSERT INTO test (a, b, c, d) VALUES (d2177dd0-eaa2-11de-a572-001b779c76e3, '2015-05-21 11:03:02+00', '2015-05-21', '11:03:02')").get();
        {
            auto msg = e.execute_cql("SELECT CAST(a AS timestamp), CAST(a AS date), CAST(a AS time), CAST(b as date), CAST(b AS time), CAST(c AS timestamp) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{timestamp_type->from_string("2009-12-17t00:26:29.805+00")},
                                                              {simple_date_type->from_string("2009-12-17")},
                                                              {time_type->from_string("00:26:29.805000000")},
                                                              {simple_date_type->from_string("2015-05-21")},
                                                              {time_type->from_string("11:03:02.000000000")},
                                                              {timestamp_type->from_string("2015-05-21t00:00:00+00")}});
        }
            /*
        std::cerr << "XYZ: [0]: " << ( timestamp_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[0].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [0]: " << value_cast<db_clock::time_point>( timestamp_type->deserialize(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[0].value()) ).time_since_epoch().count() << ";" << std::endl;
        std::cerr << "XYZ: [0]: " << value_cast<db_clock::time_point>( timestamp_type->deserialize( timestamp_type->from_string("2009-12-17t00:26:29.805+00")) ).time_since_epoch().count() << ";" << std::endl;
        std::cerr << "XYZ: [1]: " << ( simple_date_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[1].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [2]: " << ( time_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[2].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [3]: " << ( simple_date_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[3].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [4]: " << ( time_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[4].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [5]: " << ( timestamp_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[5].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [0]: " << ( utf8_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[0].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [1]: " << ( utf8_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[1].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [2]: " << ( utf8_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[2].value()) ) << ";" << std::endl;
        std::cerr << "XYZ: [3]: " << ( utf8_type->to_string(dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().rows().front()[3].value()) ) << ";" << std::endl;
        */
        {
            auto msg = e.execute_cql("SELECT CAST(CAST(a AS timestamp) AS text), CAST(CAST(a AS date) AS text), CAST(CAST(a AS time) AS text), CAST(CAST(b as date) AS text), CAST(CAST(b AS time) AS text), CAST(CAST(c AS timestamp) AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->from_string("2009-12-17T00:26:29.805000")},
                                                              {utf8_type->from_string("2009-12-17")},
                                                              {utf8_type->from_string("00:26:29.805000000")},
                                                              {utf8_type->from_string("2015-05-21")},
                                                              {utf8_type->from_string("11:03:02.000000000")},
                                                              {utf8_type->from_string("2015-05-21T00:00:00")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS text), CAST(b as text), CAST(c AS text), CAST(d AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->from_string("d2177dd0-eaa2-11de-a572-001b779c76e3")},
                                                              {utf8_type->from_string("2015-05-21T11:03:02")},
                                                              {utf8_type->from_string("2015-05-21")},
                                                              {utf8_type->from_string("11:03:02.000000000")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(CAST(a AS timestamp) AS ascii), CAST(CAST(a AS date) AS ascii), CAST(CAST(a AS time) AS ascii), CAST(CAST(b as date) AS ascii), CAST(CAST(b AS time) AS ascii), CAST(CAST(c AS timestamp) AS ascii) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{ascii_type->from_string("2009-12-17T00:26:29.805000")},
                                                              {ascii_type->from_string("2009-12-17")},
                                                              {ascii_type->from_string("00:26:29.805000000")},
                                                              {ascii_type->from_string("2015-05-21")},
                                                              {ascii_type->from_string("11:03:02.000000000")},
                                                              {ascii_type->from_string("2015-05-21T00:00:00")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS ascii), CAST(b as ascii), CAST(c AS ascii), CAST(d AS ascii) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{ascii_type->from_string("d2177dd0-eaa2-11de-a572-001b779c76e3")},
                                                              {ascii_type->from_string("2015-05-21T11:03:02")},
                                                              {ascii_type->from_string("2015-05-21")},
                                                              {ascii_type->from_string("11:03:02.000000000")}});
        }
    });
}

#if 0
    @Test
    public void testTimeCastsInSelectionClause() throws Throwable
    {
        createTable("CREATE TABLE %s (a timeuuid primary key, b timestamp, c date, d time)");

        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        DateTime date = DateTimeFormat.forPattern("yyyy-MM-dd")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21");

        long timeInMillis = dateTime.getMillis();

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, '2015-05-21 11:03:02+00', '2015-05-21', '11:03:02')",
                UUIDGen.getTimeUUID(timeInMillis));

        assertRows(execute("SELECT CAST(a AS timestamp), " +
                           "CAST(b AS timestamp), " +
                           "CAST(c AS timestamp) FROM %s"),
                   row(new Date(dateTime.getMillis()), new Date(dateTime.getMillis()), new Date(date.getMillis())));

        int timeInMillisToDay = SimpleDateSerializer.timeInMillisToDay(date.getMillis());
        assertRows(execute("SELECT CAST(a AS date), " +
                           "CAST(b AS date), " +
                           "CAST(c AS date) FROM %s"),
                   row(timeInMillisToDay, timeInMillisToDay, timeInMillisToDay));

        assertRows(execute("SELECT CAST(b AS text), " +
                           "CAST(c AS text), " +
                           "CAST(d AS text) FROM %s"),
                   row("2015-05-21T11:03:02.000Z", "2015-05-21", "11:03:02.000000000"));
    }

    @Test
    public void testOtherTypeCastsInSelectionClause() throws Throwable
    {
        createTable("CREATE TABLE %s (a ascii primary key,"
                                   + " b inet,"
                                   + " c boolean)");

        execute("INSERT INTO %s (a, b, c) VALUES (?, '127.0.0.1', ?)",
                "test", true);

        assertRows(execute("SELECT CAST(a AS text), " +
                "CAST(b AS text), " +
                "CAST(c AS text) FROM %s"),
                   row("test", "127.0.0.1", "true"));
    }

    @Test
    public void testCastsWithReverseOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (a int,"
                                   + " b smallint,"
                                   + " c double,"
                                   + " primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC);");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)",
                1, (short) 2, 6.3);

        assertRows(execute("SELECT CAST(a AS tinyint), " +
                "CAST(b AS tinyint), " +
                "CAST(c AS tinyint) FROM %s"),
                   row((byte) 1, (byte) 2, (byte) 6));

        assertRows(execute("SELECT CAST(CAST(a AS tinyint) AS smallint), " +
                "CAST(CAST(b AS tinyint) AS smallint), " +
                "CAST(CAST(c AS tinyint) AS smallint) FROM %s"),
                   row((short) 1, (short) 2, (short) 6));

        assertRows(execute("SELECT CAST(CAST(CAST(a AS tinyint) AS double) AS text), " +
                "CAST(CAST(CAST(b AS tinyint) AS double) AS text), " +
                "CAST(CAST(CAST(c AS tinyint) AS double) AS text) FROM %s"),
                   row("1.0", "2.0", "6.0"));

        String f = createFunction(KEYSPACE, "int",
                                  "CREATE FUNCTION %s(val int) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS double " +
                                          "LANGUAGE java " +
                                          "AS 'return (double)val;'");

        assertRows(execute("SELECT " + f + "(CAST(b AS int)) FROM %s"),
                   row((double) 2));

        assertRows(execute("SELECT CAST(" + f + "(CAST(b AS int)) AS text) FROM %s"),
                   row("2.0"));
    }
}
View

#endif
