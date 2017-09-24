#include "castas_fcts.hh"
#include "cql3/functions/native_scalar_function.hh"

namespace cql3 {
namespace functions {

namespace {

using opt_bytes = std::experimental::optional<bytes>;

shared_ptr<function> make_castas_function(data_type to_type, data_type from_type, castas_fctn f) {
    auto name = "castas" + to_type->as_cql3_type()->to_string();

    return cql3::functions::make_native_scalar_function<true>(name, to_type, { from_type },
        [from_type, to_type, f] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> opt_bytes {
        auto&& val = parameters[0];
        if (!val) {
            return val;
        }
        auto val_from = from_type->deserialize(*val);
        auto val_to = f(val_from);
        return to_type->decompose(val_to);
    });
}

} /* Anonymous Namespace */

thread_local castas_functions::castas_fcts_map castas_functions::_declared = castas_functions::init();
castas_functions::castas_fcts_map castas_functions::init() {
    castas_fcts_map ret;
    for (auto item: castas_fctns) {
        auto to_type = std::get<0>(item);
        auto from_type = std::get<1>(item);
        auto f = std::get<2>(item);
        ret.emplace(std::make_pair(std::make_tuple(to_type, from_type), make_castas_function(to_type, from_type, f)));
    }
    return ret;
}

shared_ptr<function> castas_functions::get(data_type to_type, const std::vector<shared_ptr<cql3::selection::selector>>& provided_args, schema_ptr s) {
    if (provided_args.size() != 1)
        throw exceptions::invalid_request_exception("Invalid CAST expression");
    auto from_type = provided_args[0]->get_type();
    auto from_type_key = from_type;
    if (from_type_key->is_reversed()) {
        from_type_key = dynamic_cast<const reversed_type_impl&>(*from_type).underlying_type();
    }
    std::cerr << "XYZ: ToType:" << to_type->name() << "; from-type:" << from_type->name() << std::endl;

    auto it_candidate = _declared.find(castas_fcts_key{to_type, from_type_key});
    if (it_candidate == _declared.end())
        throw exceptions::invalid_request_exception(sprint("%s cannot be cast to %s", from_type->name(), to_type->name()));

    return it_candidate->second;
}

}
}
