#include "engine/table.hpp"
#include "engine/column.hpp"
#include <string>
#include <iostream>

namespace qe {

    size_t Table::rows() const {
        if(Table::cols.size() != 0) {
            return Table::cols.at(0).size();
        }
        return 0;
    }

    bool Table::add_name(std::string name) {
        bool isIn = false;
        for(int idx=0; idx < static_cast<int>(Table::names.size()); idx++) {
            if(name == Table::names.at(idx)) {
                isIn = true;
            }
        }

        if(!isIn) {
            Table::names.push_back(name);
        }

        return isIn;
    }

    void Table::add_column(std::string name, Column col) {
        bool condition = ((col.type == Type::INT64) || (col.type == Type::DOUBLE) || (col.type == Type::STRING));
        if(condition) {
            if(Table::cols.size() == 0) {
                bool isIn = Table::add_name(name);
                if(!isIn) {
                    Table::cols.push_back(col);
                }
            }
            else {
                if(Table::rows() == col.size()) {
                    bool isIn = Table::add_name(name);
                    if(!isIn) {
                        Table::cols.push_back(col);
                    }
                }
            }
        }
    }

}