/*
 * Copyright (C) 2017 ScyllaDB
 *
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

#include <boost/program_options.hpp>
#include <unordered_map>
#include <vector>

#include "config_file.hh"

namespace utils {

namespace bpo = boost::program_options;

struct options_subsection : public config_file::named_value<std::unordered_map<std::string, std::vector<std::string>>, config_file::value_status::Used> {
    options_subsection(const stdx::string_view& name);

    // do not add to boost::options. We only care about yaml config
    void add_command_line_option(boost::program_options::options_description_easy_init&,
                    const stdx::string_view&, const stdx::string_view&) override {}

    void set_value(const YAML::Node&) override;

    /**
     * Converts internally stored map to bpo::parsed_options. It is caller responsibility that
     * bpo::options_description from the argument isn't destroyed before returned parsed_options.
     */
    boost::program_options::parsed_options parsed_options(const boost::program_options::options_description&) const;
};

} // namespace utils
