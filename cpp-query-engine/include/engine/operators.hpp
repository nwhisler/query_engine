#ifndef OPERATORS_H
#define OPERATORS_H
#include "table.hpp"

namespace qe {

    using Value = std::variant<int64_t, double, std::string>;

    enum class Predicate {
        EQ, 
        NEQ, 
        LT, 
        LE, 
        GT, 
        GE
    }; 

    enum class SortOrder {
        ASC, 
        DESC
    }; 

    enum class AggKind { 
        COUNT, 
        SUM, 
        AVG, 
        MIN, 
        MAX 
    };

    template <class T>
    bool compare(Predicate op, const T& a, const T& b) {
        switch(op) {
            case Predicate::EQ:  return a == b;
            case Predicate::NEQ: return a != b;
            case Predicate::LT:  return a <  b;
            case Predicate::LE:  return a <= b;
            case Predicate::GT:  return a >  b;
            case Predicate::GE:  return a >= b;
        }
        return false;
    }

    template<class T>
    std::vector<size_t> iterateRows(Predicate op,  std::vector<T>& data, const T& v) {
        std::vector<size_t> rows;
        for(int idx = 0; idx < static_cast<int>(data.size()); idx ++) {
            if(compare(op, data.at(idx), v)) {
                rows.push_back(static_cast<size_t>(idx));
            }
        }
        return rows;
    }

    template <class T>
    bool compareSort(SortOrder so, const T& a, const T& b) {
        switch(so) {
            case SortOrder::ASC:
                return a < b;
            case SortOrder::DESC:
                return a > b;
        }
        return false;
    }

    struct RowComparator {
        const Column& col;
        SortOrder order;

        bool operator()(size_t a, size_t b) const {
            switch(col.type) {
                case Type::INT64:
                    return compareSort(order, col.i64.at(a), col.i64.at(b));
                case Type::DOUBLE:
                    return compareSort(order, col.f64.at(a), col.f64.at(b));
                case Type::STRING:
                    return compareSort(order, col.str.at(a), col.str.at(b));
            }
            return false;
        };
    };

    Table project(const Table& input, const std::vector<size_t>& column_indices);
    Table order_by(const Table& input, size_t column_index, SortOrder order);
    Table limit(const Table& input, size_t n);

    Table filter(const Table& input,
            size_t column_index,
            Predicate op,
            Value constant);

    std::vector<size_t> select_rows(
        const Column& col,
        Predicate op,
        const Value& constant);

    Table group_by(
        const Table& input,
        size_t key_col,
        size_t value_col,
        AggKind agg
    );

}


#endif