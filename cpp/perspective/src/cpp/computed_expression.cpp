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
std::shared_ptr<exprtk::parser<t_tscalar>> t_compute::PARSER = std::make_shared<exprtk::parser<t_tscalar>>();

t_computed_expression::t_computed_expression(
    const std::string& expression_string, t_dtype dtype)
    : m_expression_string(expression_string)
    , m_dtype(dtype) {}

void
t_compute::init() {}

void
t_compute::compute(
    t_computed_expression expression,
    std::shared_ptr<t_data_table> data_table) {
    auto start = std::chrono::high_resolution_clock::now(); 
    exprtk::symbol_table<t_tscalar> sym_table;
    sym_table.add_constants();

    // register the data table with col() so it can grab values from
    // each column.
    computed_function::col<t_tscalar> col_fn(data_table);
    // TODO: move to global functions symbol table
    // computed_function::toupper<t_tscalar> toupper_fn = computed_function::toupper<t_tscalar>();
    sym_table.add_function("col", col_fn);
    // sym_table.add_function("toupper", toupper_fn);

    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    if (!t_compute::PARSER->compile(expression.m_expression_string, expr_definition)) {
        std::stringstream ss;
        ss << "[compute] Failed to parse expression: `"
            << expression.m_expression_string
            << "`, failed with error: "
            << t_compute::PARSER->error().c_str()
            << std::endl;

        PSP_COMPLAIN_AND_ABORT(ss.str());
    }
    
    // create or get output column
    auto output_column = data_table->add_column_sptr(
        expression.m_expression_string, expression.m_dtype, true);

    std::cout << "col: " << expression.m_expression_string << ", " << get_dtype_descr(expression.m_dtype) << std::endl;

    output_column->reserve(data_table->size());

    for (t_uindex ridx = 0; ridx < data_table->size(); ++ridx) {
        t_tscalar value = expr_definition.value();
        output_column->set_scalar(ridx, value);
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "compute: " << duration.count() << std::endl;
};

void
t_compute::recompute(
    t_computed_expression expression,
    std::shared_ptr<t_data_table> tbl,
    std::shared_ptr<t_data_table> flattened,
    const std::vector<t_rlookup>& changed_rows) {
    auto start = std::chrono::high_resolution_clock::now(); 
    exprtk::symbol_table<t_tscalar> sym_table;
    sym_table.add_constants();

    // TODO: will break if flattened[row] is null - add col(tbl, flattened)
    computed_function::col<t_tscalar> col_fn(flattened);
    // computed_function::toupper<t_tscalar> toupper_fn = computed_function::toupper<t_tscalar>();
    sym_table.add_function("col", col_fn);
    // sym_table.add_function("toupper", toupper_fn);

    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    if (!t_compute::PARSER->compile(expression.m_expression_string, expr_definition)) {
        std::stringstream ss;
        ss << "[recompute] Failed to parse expression: `"
            << expression.m_expression_string
            << "`, failed with error: "
            << t_compute::PARSER->error().c_str()
            << std::endl;

        PSP_COMPLAIN_AND_ABORT(ss.str());
    }
    
    // create or get output column
    auto output_column = flattened->add_column_sptr(
        expression.m_expression_string, expression.m_dtype, true);

    output_column->reserve(tbl->size());

    t_uindex num_rows = changed_rows.size();

    if (num_rows == 0) {
        num_rows = tbl->size();
    } 

    for (t_uindex idx = 0; idx < num_rows; ++idx) {
        bool row_already_exists = false;
        t_uindex ridx = idx;

        if (changed_rows.size() > 0) {
            ridx = changed_rows[idx].m_idx;
            row_already_exists = changed_rows[idx].m_exists;
        }

        // TODO: implement unsetting computed output col when an update comes
        // in and nullifies out one of the values.

        t_tscalar value = expr_definition.value();
        output_column->set_scalar(ridx, value);
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "recompute exprtk: " << duration.count() << std::endl;
}

t_dtype
t_compute::get_expression_dtype(
    const std::string& expression,
    std::shared_ptr<t_schema> schema
) {
    auto start = std::chrono::high_resolution_clock::now(); 
    exprtk::symbol_table<t_tscalar> sym_table;
    sym_table.add_constants();

    // register the data table with col() so it can grab values from
    // each column.
    computed_function::col<t_tscalar> col_fn(schema);
    // computed_function::toupper<t_tscalar> toupper_fn = computed_function::toupper<t_tscalar>();
    sym_table.add_function("col", col_fn);
    // sym_table.add_function("toupper", toupper_fn);

    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    if (!t_compute::PARSER->compile(expression, expr_definition)) {
        std::stringstream ss;
        ss << "[get_expression_dtype] Failed to parse expression: `"
            << expression
            << "`, failed with error: "
            << t_compute::PARSER->error().c_str()
            << std::endl;

        return DTYPE_NONE;
    }

    t_tscalar v = expr_definition.value();

    std::cout << "get_dtype value: " << v.repr() << std::endl;

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "get dtype took: " << duration.count() << std::endl;
    return v.get_dtype();
}

} // end namespace perspective