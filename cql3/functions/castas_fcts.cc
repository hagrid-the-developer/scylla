#include "castas_fcts.hh"
#include "cql3/functions/native_scalar_function.hh"

namespace cql3 {
namespace functions {

namespace {

using opt_bytes = std::experimental::optional<bytes>;

template<typename ToType, typename FromType>
struct CastAs {
    ToType operator()(const FromType x) {
        return ToType(x);
    }
};

template<>
struct CastAs<double, int> {
    double operator()(int x) {
        return double(x);
    }
};

template<typename ToType, typename FromType>
struct MakeCastAsFunction {
    shared_ptr<function> operator()() {
        auto from_type = data_type_for<FromType>();
        auto to_type = data_type_for<ToType>();
        auto name = "castas" + to_type->as_cql3_type()->to_string();
        return make_native_scalar_function<true>(name, to_type, { from_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> opt_bytes {
            auto&& val = parameters[0];
            if (!val) {
                return val;
            }
            FromType val_from = value_cast<FromType>(data_type_for<FromType>()->deserialize(*val));
            ToType val_to = ToType(val_from);
            return data_type_for<ToType>()->decompose(val_to);
        });
    }
};

template<typename FromType>
struct MakeCastAsFunction<sstring, FromType> {
    shared_ptr<function> operator()() {
        auto from_type = data_type_for<FromType>();
        auto to_type = data_type_for<sstring>();
        auto name = "castas" + to_type->as_cql3_type()->to_string();
        return make_native_scalar_function<true>(name, to_type, { from_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> opt_bytes {
            auto&& val = parameters[0];
            if (!val) {
                return val;
            }
            return data_type_for<sstring>()->decompose(data_type_for<FromType>()->to_string(*val));
        });
    }
};

template<typename FromType>
struct MakeCastAsFunction<big_decimal, FromType> {
    shared_ptr<function> operator()() {
        auto from_type = data_type_for<FromType>();
        auto to_type = data_type_for<sstring>();
        auto name = "castas" + to_type->as_cql3_type()->to_string();
        return make_native_scalar_function<true>(name, to_type, { from_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> opt_bytes {
            auto&& val = parameters[0];
            if (!val) {
                return val;
            }
            return data_type_for<big_decimal>()->decompose(data_type_for<FromType>()->to_string(*val));
        });
    }
};

template<typename ToType, typename FromType>
void declare_castas_function(castas_functions::Map &map) {
    map.emplace(castas_functions::Key{data_type_for<ToType>(), data_type_for<FromType>()},
                MakeCastAsFunction<ToType, FromType>{}());
}

template <typename FromType, typename ...ToTypes>
struct FromToImpl;
template <typename FromType>
struct FromToImpl<FromType> {
    static void def(castas_functions::Map&) {
    }
};
template <typename FromType, typename ToType, typename ...ToTypes>
struct FromToImpl<FromType, ToType, ToTypes...> {
    static void def(castas_functions::Map &map) {
        declare_castas_function<ToType, FromType>(map);
        FromToImpl<FromType, ToTypes...>::def(map);
    }
};

template <typename FromType>
struct From {
    template <typename ...ToTypes>
    using To = FromToImpl<FromType, ToTypes...>;
};

static castas_functions::Map
init() {
    castas_functions::Map ret;
    From<int>::To<int, long, float, double, sstring>::def(ret);
    From<long>::To<int, long, float, double, sstring>::def(ret);
    From<float>::To<int, long, float, double, sstring>::def(ret);
    From<double>::To<int, long, float, double, sstring>::def(ret);
    return ret;
}

}

thread_local castas_functions::Map castas_functions::_declared = init();

shared_ptr<function> castas_functions::get(data_type to_type, const std::vector<shared_ptr<cql3::selection::selector>>& provided_args, schema_ptr s) {
    if (provided_args.size() != 1)
        throw exceptions::invalid_request_exception("Invalid CAST expression");
    auto from_type = provided_args[0]->get_type();
    std::cerr << "XYZ: ToType:" << to_type->name() << "from-type:" << from_type->name() << std::endl;

    auto it_candidate = _declared.find(castas_functions::Key{to_type, from_type});
    if (it_candidate == _declared.end())
        throw exceptions::invalid_request_exception(sprint("%s cannot be cast to %s", from_type->name(), to_type->name()));

    return it_candidate->second;
}

}
}
