#ifndef ENGINE_H
#define ENGINE_H
#include "table.hpp"
#include "operators.hpp"
#include <optional>

namespace qe {

    using Scalar = std::variant<std::monostate, int64_t, double, std::string>; 

    struct FilterClause {
        size_t col;
        Predicate op;
        Value constant;
    };

    struct GroupByClause {
        size_t key_col;
        size_t value_col;
        AggKind agg;
    };

    struct OrderByClause {
        size_t col;  
        SortOrder order;
    };

    struct LimitClause {
        size_t n;
    };

    struct Query {
        std::optional<FilterClause> where;
        std::optional<GroupByClause> group_by;
        std::vector<size_t> select_cols;  
        std::optional<OrderByClause> order_by;
        std::optional<LimitClause> limit;
    };
    
    Table execute(const Table& input, const Query& q);
}

#endif