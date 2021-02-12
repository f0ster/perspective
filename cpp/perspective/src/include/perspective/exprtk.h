/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once

#include <perspective/scalar.h>

namespace exprtk {

using perspective::t_tscalar;
using perspective::mktscalar;
using perspective::mknone;

namespace details {
namespace numeric {
namespace details {

/**
 * Allows for tag dispatching so that the overloads in this header
 * are called for t_tscalar operations.
 */
struct t_tscalar_type_tag;

template <typename T> inline T const_pi_impl(t_tscalar_type_tag);
template <typename T> inline T const_e_impl(t_tscalar_type_tag);

template <typename T>
inline int to_int32_impl(const T& v, t_tscalar_type_tag);

template <typename T>
inline long long int to_int64_impl(const T& v, t_tscalar_type_tag);

#define define_unary_function_impl_header(FunctionName)                 \
template <typename T>                                                   \
inline T FunctionName##_impl(const T v, t_tscalar_type_tag);            \

define_unary_function_impl_header(abs)
define_unary_function_impl_header(acos)
define_unary_function_impl_header(acosh)
define_unary_function_impl_header(asin)
define_unary_function_impl_header(asinh)
define_unary_function_impl_header(atan)
define_unary_function_impl_header(atanh)
define_unary_function_impl_header(ceil)
define_unary_function_impl_header(cos)
define_unary_function_impl_header(cosh)
define_unary_function_impl_header(exp)
define_unary_function_impl_header(expm1)
define_unary_function_impl_header(floor)
define_unary_function_impl_header(log)
define_unary_function_impl_header(log10)
define_unary_function_impl_header(log2)
define_unary_function_impl_header(log1p)
define_unary_function_impl_header(neg)
define_unary_function_impl_header(pos)
define_unary_function_impl_header(round)
define_unary_function_impl_header(sin)
define_unary_function_impl_header(sinc)
define_unary_function_impl_header(sinh)
define_unary_function_impl_header(sqrt)
define_unary_function_impl_header(tan)
define_unary_function_impl_header(tanh)
define_unary_function_impl_header(cot)
define_unary_function_impl_header(sec)
define_unary_function_impl_header(csc)
define_unary_function_impl_header(r2d)
define_unary_function_impl_header(d2r)
define_unary_function_impl_header(d2g)
define_unary_function_impl_header(g2d)
define_unary_function_impl_header(notl)
define_unary_function_impl_header(sgn)
define_unary_function_impl_header(erf)
define_unary_function_impl_header(erfc)
define_unary_function_impl_header(ncdf)
define_unary_function_impl_header(frac)
define_unary_function_impl_header(trunc)

#undef define_unary_function_impl_header

#define define_binary_function_impl_header(FunctionName)                  \
template <typename T>                                                     \
inline T FunctionName##_impl(const T v0, const T v1, t_tscalar_type_tag); \

// except reserved words: and, or, xor
define_binary_function_impl_header(min)
define_binary_function_impl_header(max)
define_binary_function_impl_header(equal)
define_binary_function_impl_header(nequal)
define_binary_function_impl_header(modulus)
define_binary_function_impl_header(pow)
define_binary_function_impl_header(logn)
define_binary_function_impl_header(root)
define_binary_function_impl_header(roundn)
define_binary_function_impl_header(hypot)
define_binary_function_impl_header(atan2)
define_binary_function_impl_header(shr)
define_binary_function_impl_header(shl)
define_binary_function_impl_header(nand)
define_binary_function_impl_header(nor)
define_binary_function_impl_header(xnor)

#undef define_binary_function_impl_header

template <typename T>
inline T and_impl(const T v0, const T v1, t_tscalar_type_tag);

template <typename T>
inline T or_impl(const T v0, const T v1, t_tscalar_type_tag);

template <typename T>
inline T xor_impl(const T v0, const T v1, t_tscalar_type_tag);

template <typename T>
inline bool is_integer_impl(const T& v, t_tscalar_type_tag);

// #if (defined(_MSC_VER) && (_MSC_VER >= 1900)) || !defined(_MSC_VER)
// #define exprtk_define_erf(TT,impl)           \
// inline TT erf_impl(TT v) { return impl(v); } \

// exprtk_define_erf(      float,::erff)
// exprtk_define_erf(     double,::erf )
// exprtk_define_erf(long double,::erfl)
// #undef exprtk_define_erf
// #endif

// #if (defined(_MSC_VER) && (_MSC_VER >= 1900)) || !defined(_MSC_VER)
// #define exprtk_define_erfc(TT,impl)           \
// inline TT erfc_impl(TT v) { return impl(v); } \

// exprtk_define_erfc(      float,::erfcf)
// exprtk_define_erfc(     double,::erfc )
// exprtk_define_erfc(long double,::erfcl)
// #undef exprtk_define_erfc
// #endif

} // end namespace details
} // end namespace numeric

inline bool is_true(const t_tscalar& v);

inline bool is_false(const t_tscalar& v);

template <typename Iterator>
inline bool string_to_real(Iterator& itr_external, const Iterator end, t_tscalar& t, numeric::details::t_tscalar_type_tag);

} // end namespace details
} // end namespace exprtk

// exprtk needs to be imported after the type tags have been declared.
#define exprtk_disable_rtl_io_file
#define exprtk_enable_debugging
#include <exprtk.hpp>

namespace exprtk {
namespace details {
namespace numeric {
namespace details {

#define UNARY_STD_FUNCTION_BODY(FUNC)                                                                    \
    switch (v.get_dtype()) {                                                                             \
        case perspective::t_dtype::DTYPE_INT64: return mktscalar(static_cast<double>(std::FUNC(v.get<std::int64_t>())));      \
        case perspective::t_dtype::DTYPE_INT32: return mktscalar(static_cast<double>(std::FUNC(v.get<std::int32_t>())));      \
        case perspective::t_dtype::DTYPE_INT16: return mktscalar(static_cast<double>(std::FUNC(v.get<std::int16_t>())));      \
        case perspective::t_dtype::DTYPE_INT8: return mktscalar(static_cast<double>(std::FUNC(v.get<std::int8_t>())));        \
        case perspective::t_dtype::DTYPE_UINT64: return mktscalar(static_cast<double>(std::FUNC(v.get<std::uint64_t>())));    \
        case perspective::t_dtype::DTYPE_UINT32: return mktscalar(static_cast<double>(std::FUNC(v.get<std::uint32_t>())));    \
        case perspective::t_dtype::DTYPE_UINT16: return mktscalar(static_cast<double>(std::FUNC(v.get<std::uint16_t>())));    \
        case perspective::t_dtype::DTYPE_UINT8: return mktscalar(static_cast<double>(std::FUNC(v.get<std::uint8_t>())));      \
        case perspective::t_dtype::DTYPE_FLOAT64: return mktscalar(static_cast<double>(std::FUNC(v.get<double>())));          \
        case perspective::t_dtype::DTYPE_FLOAT32: return mktscalar(static_cast<double>(std::FUNC(v.get<float>())));           \
        default: return mknone();                                                                        \
    }                                                                                                    \

/**
 * @brief a function that returns none for all types besides float.
 * 
 */
#define UNARY_STD_INT_FUNCTION_BODY(FUNC)                                                                \
    switch (v.get_dtype()) {                                                                             \
        case perspective::t_dtype::DTYPE_FLOAT64: return mktscalar(static_cast<double>(std::FUNC(v.get<double>())));          \
        case perspective::t_dtype::DTYPE_FLOAT32: return mktscalar(static_cast<double>(std::FUNC(v.get<float>())));           \
        default: return mknone();                                                                        \
    }                                                                                                    \

#define BINARY_STD_FUNCTION_INNER(V0_TYPE, FUNC)                                                                           \
    switch (v1.get_dtype()) {                                                                                              \
        case perspective::t_dtype::DTYPE_INT64: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<std::int64_t>())));      \
        case perspective::t_dtype::DTYPE_INT32: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<std::int32_t>())));      \
        case perspective::t_dtype::DTYPE_INT16: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<std::int16_t>())));      \
        case perspective::t_dtype::DTYPE_INT8: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<std::int8_t>())));        \
        case perspective::t_dtype::DTYPE_UINT64: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<std::uint64_t>())));    \
        case perspective::t_dtype::DTYPE_UINT32: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<std::uint32_t>())));    \
        case perspective::t_dtype::DTYPE_UINT16: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<std::uint16_t>())));    \
        case perspective::t_dtype::DTYPE_UINT8: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<std::uint8_t>())));      \
        case perspective::t_dtype::DTYPE_FLOAT64: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<double>())));          \
        case perspective::t_dtype::DTYPE_FLOAT32: return mktscalar(static_cast<double>(std::FUNC(v0.get<V0_TYPE>, v1.get<float>())));           \
        default: return mknone();                                                                                          \
    }                                                                                                                      \

#define BINARY_STD_FUNCTION_BODY(FUNC)                                                            \
    switch (v0.get_dtype()) {                                                                     \
        case perspective::t_dtype::DTYPE_INT64: BINARY_STD_FUNCTION_INNER(std::int64_t, FUNC)     \
        case perspective::t_dtype::DTYPE_INT32: BINARY_STD_FUNCTION_INNER(std::int32_t, FUNC)     \
        case perspective::t_dtype::DTYPE_INT16: BINARY_STD_FUNCTION_INNER(std::int16_t, FUNC)     \
        case perspective::t_dtype::DTYPE_INT8: BINARY_STD_FUNCTION_INNER(std::int8_t, FUNC)       \
        case perspective::t_dtype::DTYPE_UINT64: BINARY_STD_FUNCTION_INNER(std::int64_t, FUNC)    \
        case perspective::t_dtype::DTYPE_UINT32: BINARY_STD_FUNCTION_INNER(std::int32_t, FUNC)    \
        case perspective::t_dtype::DTYPE_UINT16: BINARY_STD_FUNCTION_INNER(std::int16_t, FUNC)    \
        case perspective::t_dtype::DTYPE_UINT8: BINARY_STD_FUNCTION_INNER(std::int8_t, FUNC)      \
        case perspective::t_dtype::DTYPE_FLOAT64: BINARY_STD_FUNCTION_INNER(double, FUNC)         \
        case perspective::t_dtype::DTYPE_FLOAT32: BINARY_STD_FUNCTION_INNER(float, FUNC)          \
        default: return mknone();                                                                 \
    }                                                                                             \

struct t_tscalar_type_tag {};

template<>
struct number_type<t_tscalar> {
    typedef t_tscalar_type_tag type;
    number_type() {}
};

template <>
inline t_tscalar const_pi_impl(t_tscalar_type_tag) {
    t_tscalar rval;
    rval.set(constant::pi);
    return rval;
};

template <>
inline t_tscalar const_e_impl(t_tscalar_type_tag) {
    t_tscalar rval;
    rval.set(constant::e);
    return rval;
};

template <>
struct epsilon_type<t_tscalar> {
    static inline t_tscalar value() {
        t_tscalar rval;
        rval.set(0.0000000001);
        return rval;
    }
};    

inline bool is_nan_impl(const t_tscalar& v, t_tscalar_type_tag) {
    return v.is_nan();
}

template <>
inline int to_int32_impl(const t_tscalar& v, t_tscalar_type_tag) {
    switch (v.get_dtype()) {
        case perspective::t_dtype::DTYPE_INT64: return static_cast<int>(v.get<std::int64_t>());
        case perspective::t_dtype::DTYPE_INT32: return static_cast<int>(v.get<std::int32_t>());
        case perspective::t_dtype::DTYPE_INT16: return static_cast<int>(v.get<std::int16_t>());
        case perspective::t_dtype::DTYPE_INT8: return static_cast<int>(v.get<std::int8_t>());
        case perspective::t_dtype::DTYPE_UINT64: return static_cast<int>(v.get<std::uint64_t>());
        case perspective::t_dtype::DTYPE_UINT32: return static_cast<int>(v.get<std::uint32_t>());
        case perspective::t_dtype::DTYPE_UINT16: return static_cast<int>(v.get<std::uint16_t>());
        case perspective::t_dtype::DTYPE_UINT8: return static_cast<int>(v.get<std::uint8_t>());
        case perspective::t_dtype::DTYPE_FLOAT64: return static_cast<int>(v.get<double>());
        case perspective::t_dtype::DTYPE_FLOAT32: return static_cast<int>(v.get<float>());
        default: return 0;
    }
}

template <>
inline long long int to_int64_impl(const t_tscalar& v, t_tscalar_type_tag) {
    switch (v.get_dtype()) {
        case perspective::t_dtype::DTYPE_INT64: return v.get<std::int64_t>();
        case perspective::t_dtype::DTYPE_INT32: return v.get<std::int32_t>();
        case perspective::t_dtype::DTYPE_INT16: return v.get<std::int16_t>();
        case perspective::t_dtype::DTYPE_INT8: return v.get<std::int8_t>();
        case perspective::t_dtype::DTYPE_UINT64: return v.get<std::uint64_t>();
        case perspective::t_dtype::DTYPE_UINT32: return v.get<std::uint32_t>();
        case perspective::t_dtype::DTYPE_UINT16: return v.get<std::uint16_t>();
        case perspective::t_dtype::DTYPE_UINT8: return v.get<std::uint8_t>();
        case perspective::t_dtype::DTYPE_FLOAT64: return v.get<double>();
        case perspective::t_dtype::DTYPE_FLOAT32: return v.get<float>();
        default: return 0;
    }
}

/******************************************************************************
 *
 * Unary functions
 */
template <> inline t_tscalar abs_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar acos_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(acos) }
template <> inline t_tscalar acosh_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar asin_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(asin) }
template <> inline t_tscalar asinh_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar atan_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(atan) }
template <> inline t_tscalar atanh_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar ceil_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_FUNCTION_BODY(ceil) }
template <> inline t_tscalar cos_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(cos) }
template <> inline t_tscalar cosh_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(cosh) }
template <> inline t_tscalar exp_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_FUNCTION_BODY(exp) }
template <> inline t_tscalar expm1_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar floor_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_FUNCTION_BODY(floor) }
template <> inline t_tscalar log_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_FUNCTION_BODY(log) }
template <> inline t_tscalar log10_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_FUNCTION_BODY(log10) }
template <> inline t_tscalar log2_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar log1p_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar neg_impl(const t_tscalar v, t_tscalar_type_tag) { return -v; }
template <> inline t_tscalar round_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_FUNCTION_BODY(round) }
template <> inline t_tscalar pos_impl(const t_tscalar v, t_tscalar_type_tag) { return +v; }
template <> inline t_tscalar sgn_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar sin_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(sin) }
template <> inline t_tscalar sinc_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar sinh_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(sinh) }
template <> inline t_tscalar sqrt_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_FUNCTION_BODY(sqrt) }
template <> inline t_tscalar tan_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(tan) }
template <> inline t_tscalar tanh_impl(const t_tscalar v, t_tscalar_type_tag) { UNARY_STD_INT_FUNCTION_BODY(tanh) }
template <> inline t_tscalar cot_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar sec_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar csc_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar r2d_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar d2r_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar d2g_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar g2d_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar notl_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar frac_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar trunc_impl(const t_tscalar v, t_tscalar_type_tag) {
    // TODO: this is used in vector construction and doesn't work for some reason.
    switch (v.get_dtype()) {
        case perspective::t_dtype::DTYPE_INT64: return mktscalar(v.get<std::int64_t>());
        case perspective::t_dtype::DTYPE_INT32: return mktscalar(static_cast<std::int64_t>(v.get<std::int32_t>()));
        case perspective::t_dtype::DTYPE_INT16: return mktscalar(static_cast<std::int64_t>(v.get<std::int16_t>()));
        case perspective::t_dtype::DTYPE_INT8: return mktscalar(static_cast<std::int64_t>(v.get<std::int8_t>()));
        case perspective::t_dtype::DTYPE_UINT64: return mktscalar(static_cast<std::int64_t>(v.get<std::uint64_t>()));
        case perspective::t_dtype::DTYPE_UINT32: return mktscalar(static_cast<std::int64_t>(v.get<std::uint32_t>()));
        case perspective::t_dtype::DTYPE_UINT16: return mktscalar(static_cast<std::int64_t>(v.get<std::uint16_t>()));
        case perspective::t_dtype::DTYPE_UINT8: return mktscalar(static_cast<std::int64_t>(v.get<std::uint8_t>()));
        case perspective::t_dtype::DTYPE_FLOAT64: return mktscalar(static_cast<std::int64_t>(v.get<double>()));
        case perspective::t_dtype::DTYPE_FLOAT32: return mktscalar(static_cast<std::int64_t>(v.get<float>()));
        default: return mknone();
    }
}
template <> inline t_tscalar erf_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar erfc_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar ncdf_impl(const t_tscalar v, t_tscalar_type_tag) { return mktscalar(0); }


/******************************************************************************
 *
 * Binary functions
 */
template <> inline t_tscalar min_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return (v1 < v0) ? v1 : v0; }
template <> inline t_tscalar max_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return (v1 > v0) ? v1 : v0;  }
template <> inline t_tscalar equal_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) {
    t_tscalar rval;
    // TODO: this doesn't work between scalars of different dtypes - how do
    // we handle equality, comparison etc. between different dtyped scalars?
    rval.set(v0 == v1);
    return rval;    
}
template <> inline t_tscalar nequal_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) {
    t_tscalar rval;
    rval.set(v0 != v1);
    return rval;
}
template <> inline t_tscalar modulus_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar pow_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar logn_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar root_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar roundn_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar hypot_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar atan2_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar shr_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar shl_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar nand_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar nor_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar xnor_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar and_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar or_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }
template <> inline t_tscalar xor_impl(const t_tscalar v0, const t_tscalar v1, t_tscalar_type_tag) { return mktscalar(0); }

template <> inline bool is_integer_impl(const t_tscalar& v, t_tscalar_type_tag) {
    switch (v.get_dtype()) {
        case perspective::t_dtype::DTYPE_INT64:
        case perspective::t_dtype::DTYPE_INT32:
        case perspective::t_dtype::DTYPE_INT16:
        case perspective::t_dtype::DTYPE_INT8:
        case perspective::t_dtype::DTYPE_UINT64:
        case perspective::t_dtype::DTYPE_UINT32:
        case perspective::t_dtype::DTYPE_UINT16: 
        case perspective::t_dtype::DTYPE_UINT8: return true;
        case perspective::t_dtype::DTYPE_FLOAT64:
        case perspective::t_dtype::DTYPE_FLOAT32: return false;
        default: return false;
    }
};

#undef UNARY_STD_FUNCTION_BODY
#undef UNARY_STD_INT_FUNCTION_BODY

} // end namespace details
} // end namespace numeric

inline bool is_true (const t_tscalar& v) {
    return static_cast<bool>(v);
}

inline bool is_false(const t_tscalar& v) {
    return !is_true(v);
};
/******************************************************************************
 *
 * String -> t_tscalar
 */

template <typename Iterator>
static inline bool parse_inf(Iterator& itr, const Iterator end, t_tscalar& t, bool negative) {
    static const char_t inf_uc[] = "INFINITY";
    static const char_t inf_lc[] = "infinity";
    static const std::size_t inf_length = 8;

    const std::size_t length = static_cast<std::size_t>(std::distance(itr,end));

    if ((3 != length) && (inf_length != length)) return false;

    char_cptr inf_itr = ('i' == (*itr)) ? inf_lc : inf_uc;

    while (end != itr) {
        if (*inf_itr == static_cast<char>(*itr)) {
            ++itr;
            ++inf_itr;
            continue;
        } else {
            return false;
        }

    }

    switch (t.get_dtype()) {
        case perspective::t_dtype::DTYPE_INT64:
        case perspective::t_dtype::DTYPE_INT32:
        case perspective::t_dtype::DTYPE_INT16:
        case perspective::t_dtype::DTYPE_INT8:
        case perspective::t_dtype::DTYPE_UINT64:
        case perspective::t_dtype::DTYPE_UINT32:
        case perspective::t_dtype::DTYPE_UINT16:
        case perspective::t_dtype::DTYPE_UINT8: {
            t.set(0);
        } break;
        case perspective::t_dtype::DTYPE_FLOAT64: {
            if (negative) {
                t.set(-std::numeric_limits<double>::infinity());
            } else {
                t.set(std::numeric_limits<double>::infinity());
            }
        } break;
        case perspective::t_dtype::DTYPE_FLOAT32: {
            if (negative) {
                t.set(-std::numeric_limits<float>::infinity());
            } else {
                t.set(std::numeric_limits<float>::infinity());
            }
        } break;
        default: {}
    }

    return true;
}


template <>
inline t_tscalar compute_pow10(t_tscalar d, const int exponent) {
    static const double fract10[] = {
    0.0,
    1.0E+001, 1.0E+002, 1.0E+003, 1.0E+004, 1.0E+005, 1.0E+006, 1.0E+007, 1.0E+008, 1.0E+009, 1.0E+010,
    1.0E+011, 1.0E+012, 1.0E+013, 1.0E+014, 1.0E+015, 1.0E+016, 1.0E+017, 1.0E+018, 1.0E+019, 1.0E+020,
    1.0E+021, 1.0E+022, 1.0E+023, 1.0E+024, 1.0E+025, 1.0E+026, 1.0E+027, 1.0E+028, 1.0E+029, 1.0E+030,
    1.0E+031, 1.0E+032, 1.0E+033, 1.0E+034, 1.0E+035, 1.0E+036, 1.0E+037, 1.0E+038, 1.0E+039, 1.0E+040,
    1.0E+041, 1.0E+042, 1.0E+043, 1.0E+044, 1.0E+045, 1.0E+046, 1.0E+047, 1.0E+048, 1.0E+049, 1.0E+050,
    1.0E+051, 1.0E+052, 1.0E+053, 1.0E+054, 1.0E+055, 1.0E+056, 1.0E+057, 1.0E+058, 1.0E+059, 1.0E+060,
    1.0E+061, 1.0E+062, 1.0E+063, 1.0E+064, 1.0E+065, 1.0E+066, 1.0E+067, 1.0E+068, 1.0E+069, 1.0E+070,
    1.0E+071, 1.0E+072, 1.0E+073, 1.0E+074, 1.0E+075, 1.0E+076, 1.0E+077, 1.0E+078, 1.0E+079, 1.0E+080,
    1.0E+081, 1.0E+082, 1.0E+083, 1.0E+084, 1.0E+085, 1.0E+086, 1.0E+087, 1.0E+088, 1.0E+089, 1.0E+090,
    1.0E+091, 1.0E+092, 1.0E+093, 1.0E+094, 1.0E+095, 1.0E+096, 1.0E+097, 1.0E+098, 1.0E+099, 1.0E+100,
    1.0E+101, 1.0E+102, 1.0E+103, 1.0E+104, 1.0E+105, 1.0E+106, 1.0E+107, 1.0E+108, 1.0E+109, 1.0E+110,
    1.0E+111, 1.0E+112, 1.0E+113, 1.0E+114, 1.0E+115, 1.0E+116, 1.0E+117, 1.0E+118, 1.0E+119, 1.0E+120,
    1.0E+121, 1.0E+122, 1.0E+123, 1.0E+124, 1.0E+125, 1.0E+126, 1.0E+127, 1.0E+128, 1.0E+129, 1.0E+130,
    1.0E+131, 1.0E+132, 1.0E+133, 1.0E+134, 1.0E+135, 1.0E+136, 1.0E+137, 1.0E+138, 1.0E+139, 1.0E+140,
    1.0E+141, 1.0E+142, 1.0E+143, 1.0E+144, 1.0E+145, 1.0E+146, 1.0E+147, 1.0E+148, 1.0E+149, 1.0E+150,
    1.0E+151, 1.0E+152, 1.0E+153, 1.0E+154, 1.0E+155, 1.0E+156, 1.0E+157, 1.0E+158, 1.0E+159, 1.0E+160,
    1.0E+161, 1.0E+162, 1.0E+163, 1.0E+164, 1.0E+165, 1.0E+166, 1.0E+167, 1.0E+168, 1.0E+169, 1.0E+170,
    1.0E+171, 1.0E+172, 1.0E+173, 1.0E+174, 1.0E+175, 1.0E+176, 1.0E+177, 1.0E+178, 1.0E+179, 1.0E+180,
    1.0E+181, 1.0E+182, 1.0E+183, 1.0E+184, 1.0E+185, 1.0E+186, 1.0E+187, 1.0E+188, 1.0E+189, 1.0E+190,
    1.0E+191, 1.0E+192, 1.0E+193, 1.0E+194, 1.0E+195, 1.0E+196, 1.0E+197, 1.0E+198, 1.0E+199, 1.0E+200,
    1.0E+201, 1.0E+202, 1.0E+203, 1.0E+204, 1.0E+205, 1.0E+206, 1.0E+207, 1.0E+208, 1.0E+209, 1.0E+210,
    1.0E+211, 1.0E+212, 1.0E+213, 1.0E+214, 1.0E+215, 1.0E+216, 1.0E+217, 1.0E+218, 1.0E+219, 1.0E+220,
    1.0E+221, 1.0E+222, 1.0E+223, 1.0E+224, 1.0E+225, 1.0E+226, 1.0E+227, 1.0E+228, 1.0E+229, 1.0E+230,
    1.0E+231, 1.0E+232, 1.0E+233, 1.0E+234, 1.0E+235, 1.0E+236, 1.0E+237, 1.0E+238, 1.0E+239, 1.0E+240,
    1.0E+241, 1.0E+242, 1.0E+243, 1.0E+244, 1.0E+245, 1.0E+246, 1.0E+247, 1.0E+248, 1.0E+249, 1.0E+250,
    1.0E+251, 1.0E+252, 1.0E+253, 1.0E+254, 1.0E+255, 1.0E+256, 1.0E+257, 1.0E+258, 1.0E+259, 1.0E+260,
    1.0E+261, 1.0E+262, 1.0E+263, 1.0E+264, 1.0E+265, 1.0E+266, 1.0E+267, 1.0E+268, 1.0E+269, 1.0E+270,
    1.0E+271, 1.0E+272, 1.0E+273, 1.0E+274, 1.0E+275, 1.0E+276, 1.0E+277, 1.0E+278, 1.0E+279, 1.0E+280,
    1.0E+281, 1.0E+282, 1.0E+283, 1.0E+284, 1.0E+285, 1.0E+286, 1.0E+287, 1.0E+288, 1.0E+289, 1.0E+290,
    1.0E+291, 1.0E+292, 1.0E+293, 1.0E+294, 1.0E+295, 1.0E+296, 1.0E+297, 1.0E+298, 1.0E+299, 1.0E+300,
    1.0E+301, 1.0E+302, 1.0E+303, 1.0E+304, 1.0E+305, 1.0E+306, 1.0E+307, 1.0E+308
    };

    static const int fract10_size = static_cast<int>(sizeof(fract10) / sizeof(double));

    const int e = std::abs(exponent);
    t_tscalar min_exponent10;
    t_tscalar rval;

    switch (d.get_dtype()) {
        case perspective::t_dtype::DTYPE_INT64:
        case perspective::t_dtype::DTYPE_INT32:
        case perspective::t_dtype::DTYPE_INT16:
        case perspective::t_dtype::DTYPE_INT8:
        case perspective::t_dtype::DTYPE_UINT64:
        case perspective::t_dtype::DTYPE_UINT32:
        case perspective::t_dtype::DTYPE_UINT16:
        case perspective::t_dtype::DTYPE_UINT8: {
            // always 0 for ints
            min_exponent10.set(0);
        } break;
        case perspective::t_dtype::DTYPE_FLOAT64: {
            min_exponent10.set(std::numeric_limits<double>::min_exponent10);
        } break;
        case perspective::t_dtype::DTYPE_FLOAT32: {
            min_exponent10.set(std::numeric_limits<float>::min_exponent10);
        } break;
        default: {/* no-op */}
    }

    std::cout << "min_exponent10: " << min_exponent10 << std::endl;

    if (exponent >= min_exponent10) {
        if (e < fract10_size) {
            if (exponent > 0) {
                // TODO: I think we need to specifically set dtype everywhere
                // set() is called, otherwise could very easily get out of
                // hand.
                rval.set(d * fract10[e]);
            } else {
                rval.set(d / fract10[e]);
            }
        } else {
            rval.set(d * std::pow(10.0, 10.0 * exponent));
        }
    } else {
        t_tscalar fract;
        fract.set(fract10[min_exponent10.negate()]);
        // TODO: check if this is correct
        d = d / fract;
        t_tscalar fract2;
        fract2.set(fract10[-exponent + min_exponent10.get<int>()]);
        rval.set(d / fract2);
    }

    return rval;
}

inline bool valid_exponent(const t_tscalar& d, const int exponent, numeric::details::t_tscalar_type_tag) {
    using namespace details::numeric;
    // template <> struct numeric_info<int>         { enum { length = 10, size = 16, bound_length = 9}; };
    // template <> struct numeric_info<float>       { enum { min_exp =  -38, max_exp =  +38}; };
    // template <> struct numeric_info<double>      { enum { min_exp = -308, max_exp = +308}; };
    // template <> struct numeric_info<long double> { enum { min_exp = -308, max_exp = +308}; };

    int min_exp;
    int max_exp;

    switch (d.get_dtype()) {
        case perspective::t_dtype::DTYPE_INT64:
        case perspective::t_dtype::DTYPE_INT32:
        case perspective::t_dtype::DTYPE_INT16:
        case perspective::t_dtype::DTYPE_INT8:
        case perspective::t_dtype::DTYPE_UINT64:
        case perspective::t_dtype::DTYPE_UINT32:
        case perspective::t_dtype::DTYPE_UINT16:
        case perspective::t_dtype::DTYPE_UINT8: {
            min_exp = 0;
            max_exp = 0;
        } break;
        case perspective::t_dtype::DTYPE_FLOAT64: {
            min_exp = -308;
            max_exp = 308;
        } break;
        case perspective::t_dtype::DTYPE_FLOAT32: {
            min_exp = -38;
            max_exp = 38;
        } break;
        default: {
            std::stringstream ss;
            ss << "[exprtk::valid_exponent] Invalid exponent " << exponent << " for scalar " << d << std::endl;
            perspective::psp_abort(ss.str());
            abort();
        }
    }

    return (min_exp <= exponent) && (exponent <= max_exp);
}

template <typename Iterator>
inline bool string_to_real(Iterator& itr_external, const Iterator end, t_tscalar& t, numeric::details::t_tscalar_type_tag) {
    if (end == itr_external) return false;

    Iterator itr = itr_external;

    t_tscalar d;
    d.set(0.0); // FIXME: always float, for now

    const bool negative = ('-' == (*itr));

    if (negative || '+' == (*itr)) {
        if (end == ++itr) return false;
    }

    bool instate = false;

    static const char_t zero = static_cast<uchar_t>('0');

    #define parse_digit_1(d)          \
    if ((digit = (*itr - zero)) < 10) \
    { d = d * 10 + digit; }     \
    else                              \
    { break; }                     \
    if (end == ++itr) break;          \

    #define parse_digit_2(d)          \
    if ((digit = (*itr - zero)) < 10) \
    { d = d * 10 + digit; }     \
    else { break; }                   \
    ++itr;                         \

    if ('.' != (*itr)) {
        const Iterator curr = itr;

        while ((end != itr) && (zero == (*itr))) ++itr;

        while (end != itr) {
            unsigned int digit;
            parse_digit_1(d)
            parse_digit_1(d)
            parse_digit_2(d)
        }

        if (curr != itr) instate = true;
    }

    int exponent = 0;

    if (end != itr) {
        if ('.' == (*itr)) {
            const Iterator curr = ++itr;
            t_tscalar tmp_d;
            tmp_d.set(0.0);

            while (end != itr) {
                unsigned int digit;
                parse_digit_1(tmp_d)
                parse_digit_1(tmp_d)
                parse_digit_2(tmp_d)
            }

            if (curr != itr) {
                instate = true;

                const int frac_exponent = static_cast<int>(-std::distance(curr, itr));

                // is the exponent valid for the dtype of the final output
                // TODO: I think we need to fix the final output dtype earlier.
                if (!valid_exponent(d, frac_exponent, numeric::details::t_tscalar_type_tag()))
                    return false;

                d += compute_pow10(tmp_d, frac_exponent);
            }

            #undef parse_digit_1
            #undef parse_digit_2
        }

        if (end != itr) {
            typename std::iterator_traits<Iterator>::value_type c = (*itr);

            if (('e' == c) || ('E' == c)) {
                int exp = 0;

                if (!details::string_to_type_converter_impl_ref(++itr, end, exp)) {
                    if (end == itr) {
                        return false;
                    } else {
                        c = (*itr);
                    }
                }

                exponent += exp;
            }

            if (end != itr) {
                if (('f' == c) || ('F' == c) || ('l' == c) || ('L' == c)) {
                    ++itr;
                } else if ('#' == c) {
                    if (end == ++itr) {
                        return false;
                    } else if (('I' <= (*itr)) && ((*itr) <= 'n')) {
                        if (('i' == (*itr)) || ('I' == (*itr))) {
                            return parse_inf(itr, end, t, negative);
                        } else if (('n' == (*itr)) || ('N' == (*itr))) {
                            return parse_nan(itr, end, t);
                        } else
                            return false;
                    } else {
                        return false;
                    }
                } else if (('I' <= (*itr)) && ((*itr) <= 'n')) {
                    if (('i' == (*itr)) || ('I' == (*itr))) {
                        return parse_inf(itr, end, t, negative);
                    } else if (('n' == (*itr)) || ('N' == (*itr))) {
                        return parse_nan(itr, end, t);
                    } else {
                    return false;
                    }
                }
                else
                    return false;
            }
        }
    }

    if ((end != itr) || (!instate)) {
        return false;
    } else if (!valid_exponent(d, exponent, numeric::details::t_tscalar_type_tag())) {
        return false;
    } else if (exponent) {
        d = compute_pow10(d, exponent);
    }

    if (negative) {
        t.set(-d);
    } else {
        t.set(d);
    }

    return true;
}

} // end namespace details
} // end namespace exprtk
