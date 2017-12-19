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

#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include "stdx.hh"
#include "options_subsection.hh"
#include "utils/config_file_impl.hh" // utils::hyphenate(.)

namespace bpo = boost::program_options;

namespace {

class yaml_parser {
public:
    yaml_parser(std::unordered_map<std::string, std::vector<std::string>>& map)
        : _map{map} {
    }

    void parse(const YAML::Node& node) {
            parse_subnode(node);
    }

private:
    void parse_subnode(const YAML::Node& node, const std::string& key = std::string()) {
        switch (node.Type()) {
        case YAML::NodeType::Scalar:
            add_option(key, node.as<std::string>());
            break;
        case YAML::NodeType::Sequence:
            parse_subnode_sequence(node, key);
            break;
        case YAML::NodeType::Map:
            parse_subnode_map(node, key);
            break;
        default:
            break;
        }
    }

    void parse_subnode_sequence(const YAML::Node& node, const std::string& key) {
        for (const auto& subnode : node) {
            parse_subnode(subnode, key);
        }
    }

    void parse_subnode_map(const YAML::Node& node, const std::string& key) {
        for (const auto& pair : node) {
            const std::string& node_key = utils::hyphenate(pair.first.as<std::string>());
            std::string real_key = key.empty() ? node_key : key + '.' + node_key;
            parse_subnode(pair.second, real_key);
        }
    }

    void add_option(const std::string& key, const std::string& value) {
        if (key.empty()) {
            throw std::runtime_error("Subsection contains empty node key");
        }

        _map[key].push_back(value);
    }

    std::unordered_map<std::string, std::vector<std::string>>& _map;
};

} /* anonymous namespace */

namespace utils {

options_subsection::options_subsection(const stdx::string_view& name)
    : named_value{name} {
}

void options_subsection::set_value(const YAML::Node& node) {
    yaml_parser{(*this)()}.parse(node);
    source(config_file::config_source::SettingsFile);
}

bpo::parsed_options options_subsection::parsed_options(const bpo::options_description& opts) const {
    bpo::parsed_options po{&opts};
    for (const auto &x: (*this)()) {
        po.options.emplace_back(x.first, x.second);
    }
    return po;
}

} // namespace utils
