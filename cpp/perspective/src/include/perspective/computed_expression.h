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
#include <perspective/column.h>
#include <perspective/data_table.h>
#include <perspective/rlookup.h>
#include <perspective/computed_function.h>
#include <date/date.h>
#include <tsl/hopscotch_set.h>

// a header that includes exprtk and overload definitions for `t_tscalar` so
// it can be used inside exprtk.
#include <perspective/exprtk.h>

namespace perspective {

struct PERSPECTIVE_EXPORT t_computed_expression {
    t_computed_expression() = default;
    t_computed_expression(
        const std::string& expression_string,
        const tsl::hopscotch_set<std::string>& input_columns,
        t_dtype dtype);

    std::string m_expression_string;
    std::string m_parsed_expression_string;
    tsl::hopscotch_set<std::string> m_input_columns;
    t_dtype m_dtype;
};

class PERSPECTIVE_EXPORT t_compute {
public:
    static void init();

    static void compute(
        t_computed_expression expression,
        std::shared_ptr<t_data_table> data_table);
    
    static void recompute(
        t_computed_expression expression,
        std::shared_ptr<t_data_table> tbl,
        std::shared_ptr<t_data_table> flattened,
        const std::vector<t_rlookup>& changed_rows);

    static t_computed_expression precompute(
        const std::string& expression,
        std::shared_ptr<t_schema> schema
    );

    // TODO: use this in table::computed_schema
    // rename computed_schema?
    // TODO: minimize copy/allocation/destruct for t_tscalar usage inside
    // the expression parser.

    static std::shared_ptr<exprtk::parser<t_tscalar>> PARSER;
};

} // end namespace perspective