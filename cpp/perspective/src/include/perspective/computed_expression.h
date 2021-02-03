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
#include <date/date.h>
#include <exprtk.hpp>


namespace perspective {

class PERSPECTIVE_EXPORT t_computed_expression {
public:
    static void compute(
        const std::string& expression,
        const std::vector<std::string>& input_column_names,
        const std::vector<std::shared_ptr<t_column>> input_columns,
        std::shared_ptr<t_column> output_column
    );
};

} // end namespace perspective