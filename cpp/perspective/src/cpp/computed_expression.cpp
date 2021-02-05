/******************************************************************************
 *
 * Copyright (c) 2019, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#include <perspective/computed_expression.h>

namespace perspective {

std::shared_ptr<exprtk::parser<double>>
t_computed_expression::NUMERIC_PARSER = std::make_shared<exprtk::parser<double>>();

template <typename T>
computed_expression::col<T>::col(std::shared_ptr<t_data_table> data_table)
        : m_data_table(data_table)
        , m_columns({})
        , m_ridxs({}) {}

template <typename T>
computed_expression::col<T>::~col() {}

template <typename T>
T computed_expression::col<T>::next(
    std::shared_ptr<t_column> column,
    const std::string& column_name) {
    std::cout << "NOT IMPLEMENTED" << std::endl;
    std::string error = "next<T>() Not implemented!\n";
    PSP_COMPLAIN_AND_ABORT(error);
}

template <>
t_tscalar computed_expression::col<t_tscalar>::next(
    std::shared_ptr<t_column> column,
    const std::string& column_name) {
    t_uindex ridx = m_ridxs[column_name];
    return column->get_scalar(ridx);
}

template <>
double computed_expression::col<double>::next(
    std::shared_ptr<t_column> column,
    const std::string& column_name) {
    // will always return a valid scalar
    t_uindex ridx = m_ridxs[column_name];
    t_tscalar scalar = column->get_scalar(ridx);
    bool valid = column->is_valid(ridx);
    m_ridxs[column_name] += 1;

    if (!valid || scalar.is_none()) {
        return NAN;
    } else {
        return scalar.get<double>();
    }
}

template <>
std::string computed_expression::col<std::string>::next(
    std::shared_ptr<t_column> column,
    const std::string& column_name) {
    // will always return a valid scalar
    t_uindex ridx = m_ridxs[column_name];
    t_tscalar scalar = column->get_scalar(ridx);
    bool valid = column->is_valid(ridx);
    m_ridxs[column_name] += 1;

    if (!valid || scalar.is_none()) {
        return "";
    }  else {
        return scalar.get<std::string>();
    }
}

template <typename T>
T computed_expression::col<T>::operator()(t_parameter_list parameters) {
    auto num_params = parameters.size();

    if (num_params == 0) {
        std::stringstream ss;
        ss << "Expression error: col() function cannot be empty." << std::endl;
        std::cout << ss.str();
        PSP_COMPLAIN_AND_ABORT(ss.str());
    }

    t_string_view param = t_string_view(parameters[0]);
    std::string column_name(param.begin(), param.size());

    std::cout << "cname '" << column_name << "'" << std::endl;

    if (m_columns.count(column_name) == 1) {
        auto column = m_columns[column_name];
        return next(column, column_name);
    } else {
        auto column = m_data_table->operator[](column_name);

        if (column == nullptr) {
            std::stringstream ss;
            ss << column_name << " does not exist!" << std::endl;
            std::cout << ss.str();
            PSP_COMPLAIN_AND_ABORT(ss.str());
        }

        m_columns[column_name] = column;
        m_ridxs[column_name] = 0;
        return next(column, column_name);
    }
}

/******************************************************************************
 *
 * t_computed_expression
 */

void
t_computed_expression::init() {
    t_computed_expression::NUMERIC_PARSER->enable_unknown_symbol_resolver();
}

void
t_computed_expression::compute(
    const std::string& expression,
    std::shared_ptr<t_data_table> data_table) {
    exprtk::symbol_table<double> sym_table;

    // register the data table with col() so it can grab values from
    // each column.
    computed_expression::col<double> col_fn(data_table);
    sym_table.add_function("col", col_fn);

    exprtk::expression<double> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    if (!t_computed_expression::NUMERIC_PARSER->compile(expression, expr_definition)) {
        std::stringstream ss;
        ss << "Failed to parse expression: `"
            << expression
            << "`, failed with error: "
            << t_computed_expression::NUMERIC_PARSER->error().c_str()
            << std::endl;

        PSP_COMPLAIN_AND_ABORT(ss.str());
    }
    
    // create or get output column
    auto output_column = data_table->add_column_sptr(expression, t_dtype::DTYPE_FLOAT64, true);
    output_column->reserve(data_table->size());

    for (t_uindex ridx = 0; ridx < data_table->size(); ++ridx) {
        double value = expr_definition.value();

        if (std::isnan(value)) {
            output_column->clear(ridx);
        } else {
            output_column->set_nth<double>(ridx, value);
        }
    }

    output_column->pprint();
};

void
t_computed_expression::recompute(
    const std::string& expression,
    std::shared_ptr<t_data_table> tbl,
    std::shared_ptr<t_data_table> flattened,
    const std::vector<t_rlookup>& changed_rows) {
    // t_uindex num_rows = changed_rows.size();

    // if (num_rows == 0) {
    //     num_rows = tbl->size();
    // }

    // exprtk::symbol_table<double> sym_table;
    // exprtk::symbol_table<t_tscalar> sym_table2;

    // // register the data table with col() so it can grab values from
    // // each column.
    // computed_expression::col<double> col_fn(data_table);
    // sym_table.add_function("col", col_fn);

    // exprtk::expression<double> expr_definition;
    // expr_definition.register_symbol_table(sym_table);

    // if (!t_computed_expression::NUMERIC_PARSER->compile(expression, expr_definition)) {
    //     std::stringstream ss;
    //     ss << "Failed to parse expression: `"
    //         << expression
    //         << "`, failed with error: "
    //         << t_computed_expression::NUMERIC_PARSER->error().c_str()
    //         << std::endl;

    //     PSP_COMPLAIN_AND_ABORT(ss.str());
    // }
    
    // // create or get output column
    // auto output_column = flattened->add_column_sptr(expression, t_dtype::DTYPE_FLOAT64, true);
    // output_column.reserve(flattened->size());

    // for (t_uindex ridx = 0; ridx < num_rows; ++ridx) {
    //     bool row_already_exists = false;
    //     t_uindex ridx = idx;

    //     // Look up the changed row index, and whether the row already exists
    //     if (changed_rows.size() > 0) {
    //         ridx = changed_rows[idx].m_idx;
    //         row_already_exists = changed_rows[idx].m_exists;
    //     }


    //     double value = expr_definition.value();

    //     if (std::isnan(value)) {
    //         output_column->clear(ridx);
    //     } else {
    //         output_column->set_nth<double>(ridx, value);
    //     }
    // }

    // output_column->pprint();
    }

} // end namespace perspective