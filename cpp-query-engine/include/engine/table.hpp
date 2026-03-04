#ifndef TABLE_H
#define TABLE_H
#include "column.hpp"
#include <string>
#include <vector>

namespace qe {

    struct Table {

        std::vector<std::string> names;
        std::vector<Column> cols;

        size_t rows() const;
        void add_column(std::string name, Column col);
        bool add_name(std::string name);

    };

}

#endif