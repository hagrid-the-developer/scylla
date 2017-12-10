#pragma once

#include <algorithm>
#include <set>
#include <string>
#include <stdexcept>

#include <yaml-cpp/yaml.h>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/options_description.hpp>

class BpoYaml {
public:
    BpoYaml(const boost::program_options::options_description& desc, bool allow_unregistered = false)
        : _desc(desc)
        , _allow_unregistered(allow_unregistered) {
    }

    template <typename CharT = char>
    boost::program_options::basic_parsed_options<CharT> parse(const YAML::Node& node) {
            boost::program_options::basic_parsed_options<CharT> result(&_desc);
            parseSubnode(node, "", result);
            return result;
    }

private:

    template <typename CharT>
    void parseSubnode(const YAML::Node& node, const std::string& key, boost::program_options::basic_parsed_options<CharT>& result) {
        switch (node.Type()) {
        case YAML::NodeType::Scalar:
            addOption(key, node.as<std::basic_string<CharT>>(), result);
            break;
        case YAML::NodeType::Sequence:
            parseSubnodeSequence(node, key, result);
            break;
        case YAML::NodeType::Map:
            parseSubnodeMap(node, key, result);
            break;
        default:
            break;
        }
    }

    template <typename CharT>
    void parseSubnodeSequence(const YAML::Node& node, const std::string& key, boost::program_options::basic_parsed_options<CharT>& result) {
        for (const auto& subnode : node) {
            parseSubnode(subnode, key, result);
        }
    }

    template <typename CharT>
    void parseSubnodeMap(const YAML::Node& node, const std::string& key, boost::program_options::basic_parsed_options<CharT>& result) {
        for (const auto& pair : node) {
            const std::string& node_key = pair.first.as<std::string>();
            const std::string& real_key = key.empty() ? node_key : key + '.' + node_key;
            parseSubnode(pair.second, real_key, result);
        }
    }

    template <typename CharT>
     void addOption(const std::string& key, const std::basic_string<CharT>& value, boost::program_options::basic_parsed_options<CharT>& result) {
            if (key.empty()) {
                throw std::logic_error("Empty key - malformed YAML?");
            }
                // FIXME: XYZ: work with unallowed options
            /*
            auto allowed_iter = allowed_options.find(key);
            if (!allow_unregistered && allowed_iter == allowed_options.end()) {
                throw std::logic_error("Unallowed option in YAML node");
            }
            */

            auto option_iter = std::find_if(
                result.options.begin(), result.options.end(),
                [&key](const boost::program_options::basic_option<CharT>& test) {
                    return test.string_key == key;
                }
            );

            if (option_iter == result.options.end()) {
                result.options.emplace_back();
                option_iter = result.options.end() - 1;
                option_iter->string_key = key;
                // FIXME: XYZ: work with unallowed options
              /*  if (allowed_iter == allowed_options.end()) {
                    option_iter->unregistered = true;
                }*/
            }

            option_iter->value.push_back(value);
    }

    const boost::program_options::options_description& _desc;
    std::set<std::string> _allowed_options;
    bool _allow_unregistered;
};
