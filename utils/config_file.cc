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

#include <unordered_map>
#include <regex>

#include <yaml-cpp/yaml.h>

#include <boost/program_options.hpp>
#include <boost/any.hpp>
#include <boost/range/adaptor/filtered.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/print.hh>

#include "config_file.hh"
#include "config_file_impl.hh"

namespace bpo = boost::program_options;

namespace {

class BpoYaml {
public:
    BpoYaml(boost::program_options::parsed_options& po)
        : _po(po) {
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
            const std::string& node_key = pair.first.as<std::string>();
            std::string real_key = utils::hyphenate(key.empty() ? node_key : key + '.' + node_key);
            parse_subnode(pair.second, real_key);
        }
    }

    void add_option(const std::string& key, const std::string& value) {
        if (key.empty()) {
            throw std::runtime_error("Empty node key");
        }

        auto option_iter = std::find_if(_po.options.begin(), _po.options.end(), [&key](const auto& item) {
            return item.string_key == key;
        });

        if (option_iter == _po.options.end()) {
            _po.options.emplace_back();
            option_iter = _po.options.end() - 1;
            option_iter->string_key = key;
        }

        option_iter->value.push_back(value);
    }

    boost::program_options::parsed_options& _po;
};

} /* Anonymous Namespace */


template<>
std::istream& std::operator>>(std::istream& is, std::unordered_map<seastar::sstring, seastar::sstring>& map) {
   std::istreambuf_iterator<char> i(is), e;

   int level = 0;
   bool sq = false, dq = false, qq = false;
   sstring key, val;
   sstring* ps = &key;

   auto add = [&] {
      if (!key.empty()) {
         map[key] = std::move(val);
      }
      key = {};
      val = {};
      ps = &key;
   };

   while (i != e && level >= 0) {
      auto c = *i++;

      switch (c) {
      case '\\':
         qq = !qq;
         if (qq) {
            continue;
         }
         break;
      case '\'':
         if (!qq) {
            sq = !sq;
         }
         break;
      case '"':
         if (!qq) {
            dq = !dq;
         }
         break;
      case '=':
         if (level <= 1 && !sq && !dq && !qq) {
            ps = &val;
            continue;
         }
         break;
      case '{': case '[':
         if (!sq && !dq && !qq) {
            ++level;
            continue;
         }
         break;
      case ']': case '}':
         if (!sq && !dq && !qq && level > 0) {
            --level;
            continue;
         }
         break;
      case ',':
         if (level == 1 && !sq && !dq && !qq) {
            add();
            continue;
         }
         break;
      case ' ': case '\t': case '\n':
         if (!sq && !dq && !qq) {
            continue;
         }
         break;
      default:
         break;
      }

      if (level == 0) {
         ++level;
      }
      qq = false;
      ps->append(&c, 1);
   }

   add();

   return is;
}

template<>
std::istream& std::operator>>(std::istream& is, std::vector<seastar::sstring>& res) {
   std::istreambuf_iterator<char> i(is), e;

   int level = 0;
   bool sq = false, dq = false, qq = false;
   sstring val;

   auto add = [&] {
      if (!val.empty()) {
         res.emplace_back(std::exchange(val, {}));
      }
      val = {};
   };

   while (i != e && level >= 0) {
      auto c = *i++;

      switch (c) {
      case '\\':
         qq = !qq;
         if (qq) {
            continue;
         }
         break;
      case '\'':
         if (!qq) {
            sq = !sq;
         }
         break;
      case '"':
         if (!qq) {
            dq = !dq;
         }
         break;
      case '{': case '[':
         if (!sq && !dq && !qq) {
            ++level;
            continue;
         }
         break;
      case '}': case ']':
         if (!sq && !dq && !qq && level > 0) {
            --level;
            continue;
         }
         break;
      case ',':
         if (level == 1 && !sq && !dq && !qq) {
            add();
            continue;
         }
         break;
      case ' ': case '\t': case '\n':
         if (!sq && !dq && !qq) {
            continue;
         }
         break;
      default:
         break;
      }

      if (level == 0) {
         ++level;
      }
      qq = false;
      val.append(&c, 1);
   }

   add();

   return is;
}
template std::istream& std::operator>>(std::istream&, std::unordered_map<seastar::sstring, seastar::sstring>&);

sstring utils::hyphenate(const stdx::string_view& v) {
    sstring result(v.begin(), v.end());
    std::replace(result.begin(), result.end(), '_', '-');
    return result;
}

utils::config_file::config_file(std::initializer_list<cfg_ref> cfgs)
    : _cfgs(cfgs)
{}

void utils::config_file::add(cfg_ref cfg) {
    _cfgs.emplace_back(cfg);
}

void utils::config_file::add(std::initializer_list<cfg_ref> cfgs) {
    _cfgs.insert(_cfgs.end(), cfgs.begin(), cfgs.end());
}

bpo::options_description utils::config_file::get_options_description() {
    bpo::options_description opts("");
    return get_options_description(opts);
}

bpo::options_description utils::config_file::get_options_description(boost::program_options::options_description opts) {
    auto init = opts.add_options();
    add_options(init);
    return std::move(opts);
}

bpo::options_description_easy_init&
utils::config_file::add_options(bpo::options_description_easy_init& init) {
    for (config_src& src : _cfgs) {
        if (src.status() == value_status::Used) {
            auto&& name = src.name();
            sstring tmp = hyphenate(name);
            src.add_command_line_option(init, tmp, src.desc());
        }
    }
    return init;
}

void utils::config_file::add_seastar_options(const boost::program_options::options_description& seastar_opts) {
    _seastar_opts.add(seastar_opts);
}

boost::program_options::parsed_options utils::config_file::read_from_yaml(const char* yaml, error_handler h) {
    std::unordered_map<sstring, cfg_ref> values;

    if (!h) {
        h = [](auto & opt, auto & msg, auto) {
            throw std::invalid_argument(msg + " : " + opt);
        };
    }
    bpo::parsed_options seastar_po{&_seastar_opts};
    /*
     * Note: this is not very "half-fault" tolerant. I.e. there could be
     * yaml syntax errors that origin handles and still sets the options
     * where as we don't...
     * There are no exhaustive attempts at converting, we rely on syntax of
     * file mapping to the data type...
     */
    auto doc = YAML::Load(yaml);
    for (auto node : doc) {
        auto label = node.first.as<sstring>();

        if (label == "seastar") {
            try {
                BpoYaml{seastar_po}.parse(node.second);
            } catch (const std::runtime_error& e) {
                h(label, e.what(), value_status::Invalid);
            }
            continue;
        }
        auto i = std::find_if(_cfgs.begin(), _cfgs.end(), [&label](const config_src& cfg) { return cfg.name() == label; });
        if (i == _cfgs.end()) {
            h(label, "Unknown option", stdx::nullopt);
            continue;
        }

        config_src& cfg = *i;

        if (cfg.source() > config_source::SettingsFile) {
            // already set
            continue;
        }
        switch (cfg.status()) {
        case value_status::Invalid:
            h(label, "Option is not applicable", cfg.status());
            continue;
        case value_status::Unused:
        default:
            break;
        }
        if (node.second.IsNull()) {
            continue;
        }
        // Still, a syntax error is an error warning, not a fail
        try {
            cfg.set_value(node.second);
        } catch (std::exception& e) {
            h(label, e.what(), cfg.status());
        } catch (...) {
            h(label, "Could not convert value", cfg.status());
        }
    }
    return seastar_po;
}

utils::config_file::configs utils::config_file::set_values() const {
    return boost::copy_range<configs>(_cfgs | boost::adaptors::filtered([] (const config_src& cfg) {
        return cfg.status() > value_status::Used || cfg.source() > config_source::None;
    }));
}

utils::config_file::configs utils::config_file::unset_values() const {
    configs res;
    for (config_src& cfg : _cfgs) {
        if (cfg.status() > value_status::Used) {
            continue;
        }
        if (cfg.source() > config_source::None) {
            continue;
        }
        res.emplace_back(cfg);
    }
    return res;
}

boost::program_options::parsed_options utils::config_file::read_from_file(const sstring& filename, error_handler h) {
    std::ifstream stream(filename);
    if (!stream) {
        throw std::runtime_error(sprint("Could not open configuration file at %s. Make sure it exists.", filename));
    }
    std::stringstream ss;
    ss << stream.rdbuf();
    if (stream.bad()) {
        throw std::runtime_error(sprint("Error occured during read of configuration file at %s.", filename));
    }
    return read_from_yaml(ss.str().c_str(), h);
}



