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
std::shared_ptr<exprtk::parser<t_tscalar>> t_compute::EXPRESSION_PARSER = std::make_shared<exprtk::parser<t_tscalar>>();
std::shared_ptr<exprtk::parser<t_tscalar>> t_compute::VALIDATION_PARSER = std::make_shared<exprtk::parser<t_tscalar>>();
t_compute_column_resolver<t_tscalar> t_compute::COLUMN_RESOLVER = t_compute_column_resolver<t_tscalar>();
t_tscalar t_compute::NONE = mknone();

t_computed_expression::t_computed_expression(
        const std::string& expression_string,
        const std::string& parsed_expression_string,
        const tsl::hopscotch_map<std::string, std::string>& column_id_map,
        t_dtype dtype)
    : m_expression_string(expression_string)
    , m_parsed_expression_string(parsed_expression_string)
    , m_column_id_map(std::move(column_id_map))
    , m_dtype(dtype) {}

void
t_compute::init() {
    EXPRESSION_PARSER->enable_unknown_symbol_resolver(&t_compute::COLUMN_RESOLVER);
    VALIDATION_PARSER->enable_unknown_symbol_resolver();
}

template <typename T>
t_compute_column_resolver<T>::t_compute_column_resolver() : usr_t(usr_t::e_usrmode_extended) {}

template <typename T>
bool
t_compute_column_resolver<T>::process(
    const std::string& unknown_symbol,
    exprtk::symbol_table<T>& symbol_table,
    std::string& error_message) {
    if (unknown_symbol.find("COLUMN") != -1) {
        symbol_table.add_variable(unknown_symbol, t_compute::NONE);
        return true;
    } else {
        std::stringstream ss;
        ss << "Unknown symbol in expression: " << unknown_symbol << std::endl;
        error_message = ss.str();
        return false;
    }
}

void
t_compute::compute(
    t_computed_expression expression,
    std::shared_ptr<t_data_table> data_table) {
    auto start = std::chrono::high_resolution_clock::now(); 
    exprtk::symbol_table<t_tscalar> sym_table;
    sym_table.add_constants();

    // register the data table with col() so it can grab values from
    // each column.
    // computed_function::col<t_tscalar> col_fn(data_table, expression.m_input_columns);
    // sym_table.add_function("col", col_fn);
    // TODO: move to global functions symbol table
    // computed_function::toupper<t_tscalar> toupper_fn = computed_function::toupper<t_tscalar>();
    // sym_table.add_function("toupper", toupper_fn);

    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    auto stop1 = std::chrono::high_resolution_clock::now();
    auto duration1 = std::chrono::duration_cast<std::chrono::microseconds>(stop1 - start); 
    std::cout << "[compute] register symtable: " << duration1.count() << std::endl;

    if (!t_compute::VALIDATION_PARSER->compile(expression.m_parsed_expression_string, expr_definition)) {
        std::stringstream ss;
        ss << "[compute] Failed to parse expression: `"
            << expression.m_parsed_expression_string
            << "`, failed with error: "
            << t_compute::VALIDATION_PARSER->error().c_str()
            << std::endl;

        PSP_COMPLAIN_AND_ABORT(ss.str());
    }
    
    auto stop2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(stop2 - stop1); 
    std::cout << "[compute] parse: " << duration2.count() << std::endl;

    // create or get output column - m_expression_string is the string as
    // the user typed it.
    auto output_column = data_table->add_column_sptr(
        expression.m_expression_string, expression.m_dtype, true);

    output_column->reserve(data_table->size());

    tsl::hopscotch_map<std::string, std::vector<t_tscalar>> icols;

    for (const auto& column_id : expression.m_column_id_map) {
        std::vector<t_tscalar> col_data;
        col_data.resize(data_table->size());
        std::shared_ptr<t_column> col_sptr = data_table->get_column(column_id.second);
        
        for (t_uindex ridx = 0; ridx < data_table->size(); ++ridx) {
            col_data[ridx] = col_sptr->get_scalar(ridx);
        }

        icols[column_id.first] = std::move(col_data);
    }

    for (t_uindex ridx = 0; ridx < data_table->size(); ++ridx) {
        for (const auto& column_id : expression.m_column_id_map) {
            t_tscalar& v = sym_table.get_variable(column_id.first)->ref();
            v = icols[column_id.first][ridx];
        }

        t_tscalar value = expr_definition.value();
        output_column->set_scalar(ridx, value);
    }

    auto stop3 = std::chrono::high_resolution_clock::now();
    auto duration3 = std::chrono::duration_cast<std::chrono::microseconds>(stop3 - stop2); 
    std::cout << "[compute] make and write to column: " << duration3.count() << std::endl;

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "[compute] total: " << duration.count() << std::endl;
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

    // TODO: will break if flattened[row] is null - add col*
    // computed_function::col<t_tscalar> col_fn(flattened, expression.m_input_columns);
    // sym_table.add_function("col", col_fn);
    // computed_function::toupper<t_tscalar> toupper_fn = computed_function::toupper<t_tscalar>();
    // sym_table.add_function("toupper", toupper_fn);

    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    if (!t_compute::EXPRESSION_PARSER->compile(expression.m_parsed_expression_string, expr_definition)) {
        std::stringstream ss;
        ss << "[recompute] Failed to parse expression: `"
            << expression.m_parsed_expression_string
            << "`, failed with error: "
            << t_compute::EXPRESSION_PARSER->error().c_str()
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
    std::cout << "[recompute] took: " << duration.count() << std::endl;
}

t_computed_expression
t_compute::precompute(
    const std::string& expression_string,
    const std::string& parsed_expression_string,
    const tsl::hopscotch_map<std::string, std::string>& column_id_map,
    std::shared_ptr<t_schema> schema
) {
    auto start = std::chrono::high_resolution_clock::now(); 
    exprtk::symbol_table<t_tscalar> sym_table;
    sym_table.add_constants();

    exprtk::expression<t_tscalar> expr_definition;
    expr_definition.register_symbol_table(sym_table);
    
    /**
     * basically, replace $'col' with c1, c2, c15, c100 etc., store it
     * in the parsed_expression_string field. now that the colnames have
     * been converted to unknown symbols, they can be looked up in the
     * symbol table and the vectors can be fetched all at once.
     * 
     * $'sales', $'profit', $'abc def hijk' -> col1, col0, col12 (idx in schema)
     * symbol table will see these symbols and the custom unknown symbol
     * resolver will resolve them into vectors of t_tscalars in the symtable.
     */

    // default unknown symbol resolution will allow for parse.
    if (!t_compute::VALIDATION_PARSER->compile(parsed_expression_string, expr_definition)) {
        std::stringstream ss;
        ss << "[precompute] Failed to validate expression: `"
            << parsed_expression_string
            << "`, failed with error: "
            << t_compute::VALIDATION_PARSER->error().c_str()
            << std::endl;
        PSP_COMPLAIN_AND_ABORT(ss.str());
    }

    t_tscalar v = expr_definition.value();

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "[precompute] took: " << duration.count() << std::endl;

    return t_computed_expression(expression_string, parsed_expression_string, column_id_map, v.get_dtype());
}

} // end namespace perspective