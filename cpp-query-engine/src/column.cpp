#include "engine/column.hpp"
#include <cstdint>
#include <stdexcept>
#include <iostream>

namespace qe {
    size_t Column::size() const {
        switch(type) {
            case Type::INT64: return i64.size();
            case Type::DOUBLE: return f64.size();
            case Type::STRING: return str.size();
        }
        return 0;
    }
    Column::Column(Type t) {
        type = t;
    }
    void Column::append_int(int64_t v) {
        if(Column::type == qe::Type::INT64) {
            Column::i64.push_back(v);
        }
        else {
            throw std::logic_error("Incorrect datatype.");
        }
    }
    void Column::append_double(double v) {
        if(Column::type == qe::Type::DOUBLE) {
            Column::f64.push_back(v);
        }
        else {
            std::cout << "here" << std::endl;
            throw std::logic_error("Incorrect datatype.");
        }
    }
    void Column::append_string(std::string v) {
        if(Column::type == qe::Type::STRING) {
            Column::str.push_back(v);
        }
        else {
            throw std::logic_error("Incorrect datatype.");
        }
    }
}
