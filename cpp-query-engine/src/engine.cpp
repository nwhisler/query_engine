#include "engine/engine.hpp"
#include "engine/table.hpp"
#include "engine/operators.hpp"

namespace qe {
    Table execute(const Table& input, const Query& q) {
        Table table = input;
        if(q.where) {
            const auto& w = *q.where;
            if (w.col <= input.cols.size()) {
                table = filter(table, w.col, w.op, w.constant);
            }
        }
        if(q.group_by) {
            const auto& w = *q.group_by;
            if(w.key_col <= input.cols.size()) {
                table = group_by(table, w.key_col, w.value_col, w.agg);
            }
        }
        if(!q.select_cols.empty()) {
            table = project(table, q.select_cols);
        }
        if(q.order_by) {
            const auto& w = *q.order_by;
            if(w.col < input.cols.size()) {
                table = order_by(table, w.col, w.order);
            }
        }
        if(q.limit) {
            const auto& w = *q.limit;
            table = limit(table, w.n);
        }
        return table;
    }
}