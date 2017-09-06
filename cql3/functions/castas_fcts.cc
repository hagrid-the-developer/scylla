#include "castas_fcts.hh"
#include "cql3/functions/native_scalar_function.hh"

namespace cql3 {
namespace functions {

thread_local castas_map castas_functions::_declared;

shared_ptr<function> castas_functions::get(data_type to_type, const std::vector<shared_ptr<cql3::selection::selector>>& provided_args, schema_ptr s) {
    if (provided_args.size() != 1)
        throw exceptions::invalid_request_exception("Invalid CAST expression");
    auto from_type = provided_args[0]->get_type();
    std::cerr << "XYZ: ToType:" << to_type->name() << "from-type:" << from_type->name() << std::endl;

    auto it_candidate = _declared.find(castas_map_key{to_type, from_type});
    if (it_candidate == _declared.end())
        throw exceptions::invalid_request_exception(sprint("%s cannot be cast to %s", from_type->name(), to_type->name()));

    return it_candidate->second;
}

}
}
