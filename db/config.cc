/*
 * Copyright (C) 2015 ScyllaDB
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

#include <yaml-cpp/yaml.h>
#include <boost/program_options.hpp>
#include <unordered_map>
#include <regex>
#include "config.hh"
#include "core/file.hh"
#include "core/reactor.hh"
#include "core/shared_ptr.hh"
#include "core/fstream.hh"
#include "core/do_with.hh"
#include "core/print.hh"
#include "log.hh"
#include <boost/any.hpp>

static logging::logger clogger("config");

db::config::config()
:
        // Initialize members to defaults.
#define _mk_init(name, type, deflt, status, desc, ...)      \
        name(deflt),
    _make_config_values(_mk_init)
    _dummy(0)
{}

namespace bpo = boost::program_options;

namespace YAML {
/*
 * Add converters as needed here...
 */
template<>
struct convert<sstring> {
    static Node encode(sstring rhs) {
        auto p = rhs.c_str();
        return convert<const char *>::encode(p);
    }
    static bool decode(const Node& node, sstring& rhs) {
        std::string tmp;
        if (!convert<std::string>::decode(node, tmp)) {
            return false;
        }
        rhs = tmp;
        return true;
    }
};

template <>
struct convert<db::config::string_list> {
    static Node encode(const db::config::string_list& rhs) {
        Node node(NodeType::Sequence);
        for (auto& s : rhs) {
            node.push_back(convert<sstring>::encode(s));
        }
        return node;
    }
    static bool decode(const Node& node, db::config::string_list& rhs) {
        if (!node.IsSequence()) {
            return false;
        }
        rhs.clear();
        for (auto& n : node) {
            sstring tmp;
            if (!convert<sstring>::decode(n,tmp)) {
                return false;
            }
            rhs.push_back(tmp);
        }
        return true;
    }
};

template<>
struct convert<seastar::program_options::string_map>{
    static Node encode(const seastar::program_options::string_map& rhs) {
        Node node(NodeType::Map);
        for (auto& p : rhs) {
            node.force_insert(p.first, p.second);
        }
        return node;
    }
    static bool decode(const Node& node, seastar::program_options::string_map& rhs) {
        if (!node.IsMap()) {
            return false;
        }
        rhs.clear();
        for (auto& n : node) {
            rhs[n.first.as<sstring>()] = n.second.as<sstring>();
        }
        return true;
    }
};

template<>
struct convert<db::config::seed_provider_type> {
    static Node encode(const db::config::seed_provider_type& rhs) {
        throw std::runtime_error("should not reach");
    }
    static bool decode(const Node& node, db::config::seed_provider_type& rhs) {
        if (!node.IsSequence()) {
            return false;
        }
        rhs = db::config::seed_provider_type();
        for (auto& n : node) {
            if (!n.IsMap()) {
                continue;
            }
            for (auto& n2 : n) {
                if (n2.first.as<sstring>() == "class_name") {
                    rhs.class_name = n2.second.as<sstring>();
                }
                if (n2.first.as<sstring>() == "parameters") {
                    auto v = n2.second.as<std::vector<db::config::string_map>>();
                    if (!v.empty()) {
                        rhs.parameters = v.front();
                    }
                }
            }
        }
        return true;
    }
};


}

/*
 * Helper type to do compile time exclusion of Unused/invalid options from
 * command line.
 *
 * Only opts marked "used" should get a boost::opt
 *
 */
template<typename T, db::config::value_status S>
struct do_value_opt;

template<typename T>
struct do_value_opt<T, db::config::value_status::Used> {
    template<typename Func>
    void operator()(Func&& func, const char* name, const T& dflt, T * dst, db::config::config_source & src, const char* desc) const {
        func(name, db::config::value_status::Used, dflt, dst, src, desc);
    }
};

template <typename T>
struct do_value_opt<T, db::config::value_status::UsedFromSeastar> {
    template <typename Func>
    void operator()(Func&& func, const char* name, const T& dflt, T* dst, db::config::config_source& src, const char* desc) const {
        func(name, db::config::value_status::UsedFromSeastar, dflt, dst, src, desc);
    }
};

template<>
struct do_value_opt<db::config::seed_provider_type, db::config::value_status::Used> {
    using seed_provider_type = db::config::seed_provider_type;
    template<typename Func>
    void operator()(Func&& func, const char* name, const seed_provider_type& dflt, seed_provider_type * dst, db::config::config_source & src, const char* desc) const {
        func((sstring(name) + "_class_name").c_str(), db::config::value_status::Used, dflt.class_name, &dst->class_name, src, desc);
        func((sstring(name) + "_parameters").c_str(), db::config::value_status::Used, dflt.parameters, &dst->parameters, src, desc);
    }
};

template<typename T>
struct do_value_opt<T, db::config::value_status::Unused> {
    template<typename... Args> void operator()(Args&&... args) const {}
};
template<typename T>
struct do_value_opt<T, db::config::value_status::Invalid> {
    template<typename... Args> void operator()(Args&&... args) const {}
};

/*
 * Our own bpo::typed_valye.
 * Only difference is that we _don't_ apply defaults (they are already applied)
 * Needed to make aliases work properly.
 */
template<class T, class charT = char>
class typed_value_ex : public bpo::typed_value<T, charT> {
public:
    typedef bpo::typed_value<T, charT> _Super;

    typed_value_ex(T* store_to)
        : _Super(store_to)
    {}
    bool apply_default(boost::any& value_store) const override {
        return false;
    }
};

template<class T>
inline typed_value_ex<T>* value_ex(T* v) {
    typed_value_ex<T>* r = new typed_value_ex<T>(v);
    return r;
}

template<class T>
inline typed_value_ex<std::vector<T>>* value_ex(std::vector<T>* v) {
    auto r = new typed_value_ex<std::vector<T>>(v);
    r->multitoken();
    return r;
}

static sstring replace_underscores_with_hyphens(const char* v) {
    sstring result(v);
    std::replace(result.begin(), result.end(), '_', '-');
    return result;
}

bpo::options_description_easy_init db::config::add_options(bpo::options_description& opts) {
    bpo::options_description_easy_init init = opts.add_options();

    auto opt_add =
            [&init, &opts](const char* name, const value_status& status, const auto& dflt, auto* dst, auto& src, const char* desc) mutable {
                const auto hyphenated_name = replace_underscores_with_hyphens(name);

                switch (status) {
                case value_status::Used: {
                    init(hyphenated_name.c_str(),
                         value_ex(dst)->default_value(dflt)->notifier([&src](auto) mutable {
                             src = config_source::CommandLine;
                         }),
                         desc);

                    break;
                }

                case value_status::UsedFromSeastar: {
                    auto* seastar_opt = opts.find_nothrow(hyphenated_name, false);
                    if (!seastar_opt) {
                        throw std::runtime_error(seastar::sprint("Expected Seastar to define a command-line option '%s'",
                                                                 hyphenated_name));
                    }

                    //
                    // Just a type-check.
                    //

                    using value_type = typename std::remove_pointer<decltype(dst)>::type;

                    auto value = boost::dynamic_pointer_cast<const bpo::typed_value<value_type>>(seastar_opt->semantic());
                    if (value == nullptr) {
                        throw std::runtime_error(
                                    sprint(
                                        "The type of the Seastar-defined option '%s' does not match its declared type "
                                        "in Scylla's configuration",
                                        hyphenated_name));
                    }

                    break;
                }

                case value_status::Invalid:
                case value_status::Unused:
                    // Nothing.
                    break;
                }
            };

    // Add all used opts as command line opts
#define _add_boost_opt(name, type, deflt, status, desc, ...)      \
        do_value_opt<type, value_status::status>()(opt_add, #name, type( deflt ), &name._value, name._source, desc);
    _make_config_values(_add_boost_opt)

    auto alias_add =
            [&init](const char* name, auto& dst, const char* desc) mutable {
                init(name,
                        value_ex(&dst._value)->notifier([&dst](auto& v) mutable {
                                    dst._source = config_source::CommandLine;
                                })
                        , desc);
            };


    // Handle "old" syntax with "aliases"
    alias_add("datadir", data_file_directories, "alias for 'data-file-directories'");
    alias_add("thrift-port", rpc_port, "alias for 'rpc-port'");
    alias_add("cql-port", native_transport_port, "alias for 'native-transport-port'");

    return opts.add_options();
}

void db::config::apply_seastar_options(boost::program_options::variables_map& vars) {
    const auto do_apply = [&vars](const char* name, const db::config::value_status& status, auto* dst, auto& src) {
        // This could be done at compile-time, but this is more clear and it's not a bottleneck.
        if (status == db::config::value_status::UsedFromSeastar) {
            const auto hyphenated_name = replace_underscores_with_hyphens(name);

            using value_type = typename std::remove_pointer<decltype(dst)>::type;
            const auto& variable_value = vars[hyphenated_name];
            *dst = variable_value.as<value_type>();

            if (!variable_value.defaulted()) {
                src = db::config::config_source::CommandLine;
            }
        }
    };

#define _add_seastar_opt(name, type, deflt, status, desc, ...) \
    do_apply(#name, db::config::value_status::status, &(name._value), name._source);

    _make_config_values(_add_seastar_opt)
}

// Virtual dispatch to convert yaml->data type.
struct handle_yaml {
    virtual ~handle_yaml() {};
    virtual void operator()(const YAML::Node&) = 0;
    virtual db::config::value_status status() const = 0;
    virtual db::config::config_source source() const = 0;
};

template<typename T, db::config::value_status S>
struct handle_yaml_impl : public handle_yaml {
    typedef db::config::value<T, S> value_type;

    handle_yaml_impl(value_type& v, db::config::config_source& src) : _dst(v), _src(src) {}
    void operator()(const YAML::Node& node) override {
        _dst(node.as<T>());
        _src = db::config::config_source::SettingsFile;
    }
    db::config::value_status status() const override {
        return _dst.status();
    }
    db::config::config_source source() const override {
        return _src;
    }
    value_type& _dst;
    db::config::config_source&
        _src;
};

void db::config::read_from_yaml(const sstring& yaml) {
    read_from_yaml(yaml.c_str());
}

void db::config::read_from_yaml(const char* yaml) {
    std::unordered_map<sstring, std::unique_ptr<handle_yaml>> values;

#define _add_yaml_opt(name, type, deflt, status, desc, ...)      \
        values.emplace(#name, std::make_unique<handle_yaml_impl<type, value_status::status>>(name, name._source));
    _make_config_values(_add_yaml_opt)

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
        auto i = values.find(label);
        if (i == values.end()) {
            clogger.warn("Unknown option {} ignored.", label);
            continue;
        }
        if (i->second->source() > config_source::SettingsFile) {
            clogger.debug("Option {} already set by commandline. ignored.", label);
            continue;
        }
        switch (i->second->status()) {
        case value_status::Invalid:
            clogger.warn("Option {} is not applicable. Ignoring.", label);
            continue;
        case value_status::Unused:
            clogger.warn("Option {} is not (yet) used.", label);
            break;
        default:
            break;
        }
        if (node.second.IsNull()) {
            clogger.debug("Option {}, empty value. Skipping.", label);
            continue;
        }
        // Still, a syntax error is an error warning, not a fail
        try {
            (*i->second)(node.second);
        } catch (...) {
            clogger.error("Option {}, exception while converting value.", label);
        }
    }

    for (auto& p : values) {
        if (p.second->status() > value_status::Used) {
            continue;
        }
        if (p.second->source() > config_source::None) {
            continue;
        }
        clogger.debug("Option {} not set", p.first);
    }
}

future<> db::config::read_from_file(file f) {
    return f.size().then([this, f](size_t s) {
        return do_with(make_file_input_stream(f), [this, s](input_stream<char>& in) {
            return in.read_exactly(s).then([this](temporary_buffer<char> buf) {
               read_from_yaml(sstring(buf.begin(), buf.end()));
            });
        });
    });
}

future<> db::config::read_from_file(const sstring& filename) {
    return open_file_dma(filename, open_flags::ro).then([this](file f) {
       return read_from_file(std::move(f));
    });
}

boost::filesystem::path db::config::get_conf_dir() {
    using namespace boost::filesystem;

    path confdir;
    auto* cd = std::getenv("SCYLLA_CONF");
    if (cd != nullptr) {
        confdir = path(cd);
    } else {
        auto* p = std::getenv("SCYLLA_HOME");
        if (p != nullptr) {
            confdir = path(p);
        }
        confdir /= "conf";
    }

    return confdir;
}

void db::config::check_experimental(const sstring& what) const {
    if (!experimental()) {
        throw std::runtime_error(sprint("%s is currently disabled. Start Scylla with --experimental=on to enable.", what));
    }
}
