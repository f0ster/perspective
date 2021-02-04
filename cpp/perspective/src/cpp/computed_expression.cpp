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

std::shared_ptr<exprtk::parser<double>> t_computed_expression::NUMERIC_PARSER = std::make_shared<exprtk::parser<double>>();

template <typename T>
computed_expression::col<T>::col(std::shared_ptr<t_data_table> data_table)
        : m_data_table(data_table)
        , m_columns({})
        , m_ridx(0) {}

template <typename T>
T computed_expression::col<T>::next(std::shared_ptr<t_column> column) {
    // TODO: finish null handling
    // if (!column->is_valid(m_ridx)) {
    T value = *(column->get_nth<T>(m_ridx));
    m_ridx += 1;
    return value;
}

template <typename T>
T computed_expression::col<T>::operator()(t_parameter_list parameters) {
    auto num_params = parameters.size();

    if (num_params == 0) {
        std::stringstream ss;
        ss << "col() cannot be empty" << std::endl;
        PSP_COMPLAIN_AND_ABORT(ss.str());
    }

    t_string_view param = t_string_view(parameters[0]);
    std::string column_name(param.begin(), param.size());

    if (m_columns.count(column_name) == 1) {
        auto column = m_columns[column_name];
        return next(column);
    } else {
        auto column = m_data_table->operator[](column_name);
        if (column == nullptr) {
            std::stringstream ss;
            ss << column_name << " does not exist!" << std::endl;
            PSP_COMPLAIN_AND_ABORT(ss.str());
        }

        m_columns[column_name] = column;
        return next(column);
    }
}

/**
 * @brief TODO: instead of implementing a col() UDF, impl. a pre-parser for
 * the expression in the form of foo(expr) -> list(str), returning a list of
 * column names. pre-serialize each column out into a vector of primitives -
 * I think we can do this through memcpy and just reading that underlying
 * memory as a vector<double> etc. and then serialize the vector to a string
 * in order to take advantage of exprtk's ability to have user-defined
 * vectors inside expressions, and the issue that UDFs can only return
 * scalars or strings and not vectors. Thus, right now we do a get_nth()
 * from a column pointer per iteration - grabbing a whole vector at once
 * should be more optimizable. We would still need a col() UDF for columns
 * with spaces in the name, but there might be a way to make that cleaned
 * in the precompile step (i.e. col("Name with spaces") -> Name_with_spaces,
 * assuming there are no collisions between something like "abc def" and "abc   def",
 * which should really just resolve to abc_def and abc___def).
 * 
 * Thus, valid tokens would be:
 * 
 * - column: any non-quote enclosed token that contains [a-Z][0-9][_], or
 *   col("abc def") that can be reduced to [a-Z][0-9][_]
 * - scalar: any numeric value or 'string' in single quotes
 * - function: a function expression like abs() or sqrt()
 * 
 * so when the user types an expression like:
 * 
 * a + 12 / b * (c + d - 50)
 * 
 * it would be preprocessed into:
 * 
 * {
 *  expression: ...,
 *  input_columns: [a, b, c, d]
 * }
 * 
 * and then the expression string passed to parser.compile():
 * 
 * "
 * var a[100] != {1, 2, 3, 4, ...};
 * var b[100] != {5, 6, 7, 8, ...};
 * var c[100] != {1.5, 2.5, 3.5, 4.5, ...};
 * var d[100] != {0.1, 0.2, 0.3, 0.4, ...};
 * a + 12 / b * (c + d - 50)
 * "
 * 
 * this would get us vector level operations that could very well be
 * SIMD'd, and is a cleaner API that covers up a lot of internal complexity -
 * this can be good or bad.
 * 
 * @param expression 
 * @param data_table 
 * @param input_column_names 
 * @param input_columns 
 * @param output_column 
 */
void t_computed_expression::compute(
    const std::string& expression,
    std::shared_ptr<t_data_table> data_table,
    const std::vector<std::string>& input_column_names,
    const std::vector<std::shared_ptr<t_column>> input_columns,
    std::shared_ptr<t_column> output_column) {
    exprtk::symbol_table<double> sym_table;

    std::vector<double> initial(input_columns.size());
    std::fill(initial.begin(), initial.end(), 0);

    for (auto i = 0; i < input_column_names.size(); ++i) {
        sym_table.add_variable(input_column_names[i], initial[i]);
    }

    exprtk::symbol_table<double> functions;
    computed_expression::col<double> col_fn(data_table);

    functions.add_function("col", col_fn);

    exprtk::expression<double> expr_definition;
    
    expr_definition.register_symbol_table(functions);
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

    for (auto ridx = 0; ridx < output_column->size(); ++ridx) {

        // TODO: this is bad
        for (auto cidx = 0; cidx < input_columns.size(); ++cidx) {
            auto val = *(input_columns[cidx]->get_nth<double>(ridx));
            initial[cidx] = val;
        }

        double value = expr_definition.value();
        
        if (std::isnan(value)) {
            output_column->clear(ridx);
        } else {
            output_column->set_nth<double>(ridx, value);
        }

    }
};

void
t_computed_expression::generate_functions() {}

} // end namespace perspective