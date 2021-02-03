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

void t_computed_expression::compute(
    const std::string& expression,
    const std::vector<std::string>& input_column_names,
    const std::vector<std::shared_ptr<t_column>> input_columns,
    std::shared_ptr<t_column> output_column) {
    exprtk::symbol_table<double> sym_table;

    std::vector<double> initial(input_columns.size());
    std::fill(initial.begin(), initial.end(), 0);

    for (auto i = 0; i < input_column_names.size(); ++i) {
        sym_table.add_variable(input_column_names[i], initial[i]);
    }

    exprtk::expression<double> expr_definition;
    expr_definition.register_symbol_table(sym_table);

    exprtk::parser<double> NUMERIC_PARSER;

    if (!NUMERIC_PARSER.compile(expression, expr_definition)) {
        std::stringstream ss;
        ss << "Failed to parse expression: `"
            << expression
            << "`, failed with error: "
            << NUMERIC_PARSER.error().c_str()
            << std::endl;

        PSP_COMPLAIN_AND_ABORT(ss.str());
    }

    for (auto ridx = 0; ridx < output_column->size(); ++ridx) {

        // TODO: this is bad
        for (auto cidx = 0; cidx < input_columns.size(); ++cidx) {
            auto val = *(input_columns[cidx]->get_nth<double>(ridx));
            initial[cidx] = val;
        }

        output_column->set_nth<double>(ridx, expr_definition.value());
    }
};

} // end namespace perspective