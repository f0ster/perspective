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

std::shared_ptr<exprtk::parser<t_tscalar>>
t_computed_expression::PARSER = std::make_shared<exprtk::parser<t_tscalar>>();

void
t_computed_expression::init() {
    //t_computed_expression::PARSER->enable_unknown_symbol_resolver();
}

void
t_computed_expression::compute(
    const std::string& expression,
    std::shared_ptr<t_data_table> data_table) {
    auto start = std::chrono::high_resolution_clock::now(); 
    exprtk::symbol_table<t_tscalar> sym_table;
    sym_table.add_constants();

    // register the data table with col() so it can grab values from
    // each column.
    computed_function::col<t_tscalar> col_fn(data_table);
    computed_function::toupper<t_tscalar> toupper_fn = computed_function::toupper<t_tscalar>();
    sym_table.add_function("col", col_fn);
    sym_table.add_function("toupper", toupper_fn);

    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    if (!t_computed_expression::PARSER->compile(expression, expr_definition)) {
        std::stringstream ss;
        ss << "[compute] Failed to parse expression: `"
            << expression
            << "`, failed with error: "
            << t_computed_expression::PARSER->error().c_str()
            << std::endl;

        PSP_COMPLAIN_AND_ABORT(ss.str());
    }
    
    // create or get output column
    auto schema = std::make_shared<t_schema>(data_table->get_schema());
    t_dtype dtype = t_computed_expression::get_expression_dtype(expression, schema);
    std::cout << "compute: " << expression << ": " << get_dtype_descr(dtype) << std::endl;
    auto output_column = data_table->add_column_sptr(expression, dtype, true);
    output_column->reserve(data_table->size());

    for (t_uindex ridx = 0; ridx < data_table->size(); ++ridx) {
        t_tscalar value = expr_definition.value();

        std::cout << ridx << ": " << value << std::endl;

        if (std::isnan(value)) {
            output_column->unset(ridx);
        } else {
            output_column->set_scalar(ridx, value);
        }
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "exprtk: " << duration.count() << std::endl;
};

void
t_computed_expression::recompute(
    const std::string& expression,
    std::shared_ptr<t_data_table> tbl,
    std::shared_ptr<t_data_table> flattened,
    const std::vector<t_rlookup>& changed_rows) {
    auto start = std::chrono::high_resolution_clock::now(); 
    exprtk::symbol_table<t_tscalar> sym_table;
    sym_table.add_constants();

    // TODO: will break if flattened[row] is null
    computed_function::col<t_tscalar> col_fn(flattened);
    computed_function::toupper<t_tscalar> toupper_fn = computed_function::toupper<t_tscalar>();
    sym_table.add_function("col", col_fn);
    sym_table.add_function("toupper", toupper_fn);

    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    if (!t_computed_expression::PARSER->compile(expression, expr_definition)) {
        std::stringstream ss;
        ss << "[recompute] Failed to parse expression: `"
            << expression
            << "`, failed with error: "
            << t_computed_expression::PARSER->error().c_str()
            << std::endl;

        PSP_COMPLAIN_AND_ABORT(ss.str());
    }
    
    // create or get output column
    auto schema = std::make_shared<t_schema>(tbl->get_schema());
    t_dtype dtype = t_computed_expression::get_expression_dtype(expression, schema);
    std::cout << "recompute: " << expression << ": " << get_dtype_descr(dtype) << std::endl;
    auto output_column = flattened->add_column_sptr(expression, dtype, true);
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

        std::cout << ridx << ": " << value << std::endl;

        if (std::isnan(value)) {
            output_column->unset(ridx);
        } else {
            output_column->set_scalar(ridx, value);
        }
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "recompute exprtk: " << duration.count() << std::endl;
}

t_dtype
t_computed_expression::get_expression_dtype(
    const std::string& expression,
    std::shared_ptr<t_schema> schema
) {
    auto start = std::chrono::high_resolution_clock::now(); 
    exprtk::symbol_table<t_tscalar> sym_table;
    sym_table.add_constants();

    // register the data table with col() so it can grab values from
    // each column.
    computed_function::col<t_tscalar> col_fn(schema);
    computed_function::toupper<t_tscalar> toupper_fn = computed_function::toupper<t_tscalar>();
    sym_table.add_function("col", col_fn);
    sym_table.add_function("toupper", toupper_fn);


    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    if (!t_computed_expression::PARSER->compile(expression, expr_definition)) {
        std::stringstream ss;
        ss << "[compute] Failed to parse expression: `"
            << expression
            << "`, failed with error: "
            << t_computed_expression::PARSER->error().c_str()
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