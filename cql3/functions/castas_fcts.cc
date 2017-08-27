#include <castas_fcts.hh>

namespace cql3 {
namespace functions {

namespace {

template<typename ToType, typename FromType>
CastAs { };

tamplate<>
CastAs<double, int> {
    double operator()(int x) {
        return double(x);
    }
}

template<typename ToType, typename FromType>
inline
shared_ptr<function>
make_castas_function() {
    auto from_type = data_type_for<FromType>();
    auto to_type = data_type_for<ToType>();
    auto name = "castas" + to_type->as_cql3_type()->to_string();
    return make_native_scalar_function<true>(name, from_type, { to_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) {
        auto&& val = parameters[0];
        if (!val) {
            return val;
        }
        FromType val_from = value_cast<Type>(data_type_for<Type>()->deserialize(*values[0]));
        ToType val_to = CastAs<ToType, FromType>{}(val_from);
        return  data_type_for<Type>()->decompose(val_to);
    });
}

template<typename ToType, typename FromType>
declare_castas_function(std::unordered_multimap<std::tuple<data_type, data_type>, shared_ptr<function>> &map) {
    map.emplace(
        std::make_tuple(data_type_for<ToType>(), data_type_for<FromType>()),
        make_castas_function<ToType, FromType>());
}

static thread_local std::unordered_multimap<std::tuple<data_type, data_type>, shared_ptr<function>>
init() {
    std::unordered_multimap<std::tuple<data_type, data_type>, shared_ptr<function>> ret;
    declare_castas_function<double, int>(ret);

    return ret;
}

}

thread_local std::unordered_multimap<function_name, shared_ptr<function>> castas_functions::_declared = init();

shared_ptr<function> castas_functions::get(data_type to_type, data_type from_type) {
    return {};
}

}
}
