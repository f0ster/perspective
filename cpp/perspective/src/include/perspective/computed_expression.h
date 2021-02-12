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

// a header that includes exprtk and overload definitions for `t_tscalar` so
// it can be used inside exprtk.
#include <perspective/exprtk.h>

namespace perspective {

class PERSPECTIVE_EXPORT t_computed_expression {
public:
    static void init();

    static void compute(
        const std::string& expression,
        std::shared_ptr<t_data_table> data_table);
    
    static void recompute(
        const std::string& expression,
        std::shared_ptr<t_data_table> tbl,
        std::shared_ptr<t_data_table> flattened,
        const std::vector<t_rlookup>& changed_rows);

    static t_dtype get_expression_dtype(
        const std::string& expression,
        std::shared_ptr<t_schema> schema
    );

    static std::shared_ptr<exprtk::parser<t_tscalar>> PARSER;
};

} // end namespace perspective