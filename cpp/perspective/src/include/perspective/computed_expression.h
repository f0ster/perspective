/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once

#include <perspective/first.h>
#include <perspective/base.h>
#include <perspective/exports.h>
#include <perspective/raw_types.h>
#include <perspective/scalar.h>
#include <perspective/column.h>
#include <perspective/data_table.h>
#include <date/date.h>

// a precompiled header that includes exprtk.
#include <perspective/exprtk.h>


namespace perspective {
namespace computed_expression {

/**
 * @brief A custom exprtk function that reaches into a column and returns the
 * value of the next row. Basically like an iterator but slow and bad, and this
 * should be fully deleted with a better implementation of "get a value from
 * a column". Unfortunately, because ExprTk UDFs don't allow vector return
 * this seems like a logical first step.
 * 
 * @tparam T 
 */
template <typename T>
struct col : public exprtk::igeneric_function<T> {
    typedef typename exprtk::igeneric_function<T>::parameter_list_t t_parameter_list;
    typedef typename exprtk::igeneric_function<T>::generic_type t_generic_type;
    typedef typename t_generic_type::string_view t_string_view;

    std::shared_ptr<t_data_table> m_data_table;
    std::map<std::string, std::shared_ptr<t_column>> m_columns;
    t_uindex m_ridx;

    col(std::shared_ptr<t_data_table> data_table);

    T next(std::shared_ptr<t_column> column);

    T operator()(t_parameter_list parameters);
};

}; // end namespace computed_expression

class PERSPECTIVE_EXPORT t_computed_expression {
public:
    static void compute(
        const std::string& expression,
        std::shared_ptr<t_data_table> data_table,
        const std::vector<std::string>& input_column_names,
        const std::vector<std::shared_ptr<t_column>> input_columns,
        std::shared_ptr<t_column> output_column
    );

    static void generate_functions();

    static std::shared_ptr<exprtk::parser<double>> NUMERIC_PARSER;

    // store all UDFs here, eventually - once we can separate state from
    // functions like col()
    static exprtk::symbol_table<double> FUNCTION_SYMTABLE;
    static computed_expression::col<double> COL_FUNCTION;
};

} // end namespace perspective