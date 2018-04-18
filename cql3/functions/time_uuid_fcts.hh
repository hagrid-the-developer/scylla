/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 *
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

#pragma once

#include "native_scalar_function.hh"
#include "utils/UUID_gen.hh"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace cql3 {

namespace functions {

namespace time_uuid_fcts {

inline
shared_ptr<function>
make_now_fct() {
    return make_native_scalar_function<false>("now", timeuuid_type, {},
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        return {to_bytes(utils::UUID_gen::get_time_UUID())};
    });
}

inline
shared_ptr<function>
make_min_timeuuid_fct() {
    return make_native_scalar_function<true>("mintimeuuid", timeuuid_type, { timestamp_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto ts = value_cast<db_clock::time_point>(ts_obj);
        auto uuid = utils::UUID_gen::min_time_UUID(ts.time_since_epoch().count());
        return {timeuuid_type->decompose(uuid)};
    });
}

inline
shared_ptr<function>
make_max_timeuuid_fct() {
    return make_native_scalar_function<true>("maxtimeuuid", timeuuid_type, { timestamp_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        // FIXME: should values be a vector<optional<bytes>>?
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto ts = value_cast<db_clock::time_point>(ts_obj);
        auto uuid = utils::UUID_gen::max_time_UUID(ts.time_since_epoch().count());
        return {timeuuid_type->decompose(uuid)};
    });
}

inline
shared_ptr<function>
make_date_of_fct() {
    return make_native_scalar_function<true>("dateof", timestamp_type, { timeuuid_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts = db_clock::time_point(db_clock::duration(UUID_gen::unix_timestamp(UUID_gen::get_UUID(*bb))));
        return {timestamp_type->decompose(ts)};
    });
}

inline
shared_ptr<function>
make_unix_timestamp_of_fcf() {
    return make_native_scalar_function<true>("unixtimestampof", long_type, { timeuuid_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        return {long_type->decompose(UUID_gen::unix_timestamp(UUID_gen::get_UUID(*bb)))};
    });
}

// :;DF: Begin
inline
shared_ptr<function>
make_todate_timeuuid_fct() {
    return make_native_scalar_function<true>("todate", date_type, { timeuuid_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto uuid_obj = timeuuid_type->deserialize(*bb);
        if (uuid_obj.is_null()) {
            return {};
        }
        auto uuid = value_cast<utils::UUID>(uuid_obj);
        const auto tp = millis_to_time_point(utils::UUID_gen::unix_timestamp(uuid));
        const auto date = time_point_to_date(tp);
        return {simple_date_type->decompose(simple_date_native_type{date})};
    });
}

inline
shared_ptr<function>
make_todate_timestamp_fct() {
    return make_native_scalar_function<true>("todate", date_type, { timestamp_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto tp = value_cast<db_clock::time_point>(ts_obj);
        const uint32_t date = time_point_to_date(tp);
        return {simple_date_type->decompose(simple_date_native_type{date})};
    });
}


inline
shared_ptr<function>
make_totimestamp_timeuuid_fct() {
    return make_native_scalar_function<true>("totimestamp", timestamp_type, { timeuuid_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto uuid_obj = timeuuid_type->deserialize(*bb);
        if (uuid_obj.is_null()) {
            return {};
        }
        auto uuid = value_cast<utils::UUID>(uuid_obj);
        return {timestamp_type->decompose(utils::UUID_gen::unix_timestamp(uuid))};
    });
}

inline
shared_ptr<function>
make_totimestamp_date_fct() {
    return make_native_scalar_function<true>("totimestamp", timestamp_type, { simple_date_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto date_obj = simple_date_type->deserialize(*bb);
        if (date_obj.is_null()) {
            return {};
        }
        const auto date = value_cast<uint32_t>(date_obj);
        const auto tp = date_to_time_point(date);
        return {timestamp_type->decompose(tp)};
    });
}

// :;DF: End
inline
shared_ptr<function>
make_tounixtimestamp_timeuuid_fct() {
    return make_native_scalar_function<true>("tounixtimestamp", long_type, { timeuuid_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        // FIXME: should values be a vector<optional<bytes>>?
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto ts = value_cast<db_clock::time_point>(ts_obj);
        auto uuid = utils::UUID_gen::max_time_UUID(ts.time_since_epoch().count());
        return {timeuuid_type->decompose(uuid)};
    });
}

#if 0
inline
shared_ptr<function>
make_tounixtimestamp_timestamp_fct() {
    return make_native_scalar_function<true>("tounixtimestamp", long_type, { timestamp_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        // FIXME: should values be a vector<optional<bytes>>?
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto ts = value_cast<db_clock::time_point>(ts_obj);
        auto uuid = utils::UUID_gen::max_time_UUID(ts.time_since_epoch().count());
        return {timeuuid_type->decompose(uuid)};
    });
}

inline
shared_ptr<function>
make_tounixtimestamp_date_fct() {
    return make_native_scalar_function<true>("tounixtimestamp", long_type, { simple_date_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& values) -> bytes_opt {
        // FIXME: should values be a vector<optional<bytes>>?
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto ts = value_cast<db_clock::time_point>(ts_obj);
        auto uuid = utils::UUID_gen::max_time_UUID(ts.time_since_epoch().count());
        return {timeuuid_type->decompose(uuid)};
    });
}
#endif
}
}
}
