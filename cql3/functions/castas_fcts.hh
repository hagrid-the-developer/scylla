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
 * Copyright (C) 2017 ScyllaDB
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

#include <boost/functional/hash.hpp>
#include <tuple>
#include <unordered_map>

#include "cql3/functions/function.hh"
#include "cql3/functions/abstract_function.hh"
#include "exceptions/exceptions.hh"
#include "core/print.hh"
#include "cql3/cql3_type.hh"
#include "cql3/selection/selector.hh"

namespace cql3 {
namespace functions {

class castas_functions {
public:
    // Map <ToType, FromType> -> Function
    using castas_fcts_key = std::tuple<data_type, data_type>;
    struct castas_fcts_hash {
        std::size_t operator()(const castas_fcts_key &x) const noexcept {
            return boost::hash_value(x);
        }
    };
    using castas_fcts_map = std::unordered_map<castas_fcts_key, shared_ptr<cql3::functions::function>, castas_fcts_hash>;
    static shared_ptr<function> get(data_type to_type, const std::vector<shared_ptr<cql3::selection::selector>>& provided_args, schema_ptr s);

private:
    static thread_local castas_fcts_map _declared;
    static castas_fcts_map init();
};

}
}
