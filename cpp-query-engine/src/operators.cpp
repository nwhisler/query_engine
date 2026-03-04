#include "engine/operators.hpp"
#include "engine/column.hpp"
#include <iostream>
#include <map>
#include <algorithm>
#include <utility>

namespace qe {

    Table filter(const Table& input, size_t column_index, Predicate op, Value v) {
        std::vector<size_t> rows;
        if(column_index < input.cols.size()) {
            Column col = input.cols.at(column_index);
            Type type = col.type; 
            if(type == Type::INT64) {
                int64_t value = std::get<int64_t>(v);
                std::vector<int64_t> i64 = col.i64;
                rows = iterateRows(op, i64, value);
                Column inputCol(type);
            }
            else if(type == Type::DOUBLE) {
                double value = std::get<double>(v);
                std::vector<double> f64 = col.f64;
                rows = iterateRows(op, f64, value);
            }
            else if(type == Type::STRING) {
                std::string value = std::get<std::string>(v);
                std::vector<std::string> str = col.str;
                rows = iterateRows(op, str, value);
            }
        }
        Table table;
        if(input.names.size() == input.cols.size()) {
            for(int idx = 0; idx < static_cast<int>(input.cols.size()); idx++) {
                std::string table_name = input.names.at(idx);
                Type table_col_type = input.cols.at(idx).type;
                Column table_col(table_col_type);
                table.add_column(table_name, table_col);
            }
            std::vector<size_t> used_idx;
            for(int row_idx = 0; row_idx < static_cast<int>(input.rows()); row_idx++) {
                std::vector<size_t>::iterator it = std::find(rows.begin(), rows.end(), static_cast<size_t>(row_idx));
                if(it != rows.end()) {
                    std::vector<size_t>::iterator used = std::find(used_idx.begin(), used_idx.end(), static_cast<size_t>(row_idx));
                    if(used == used_idx.end()) {
                        for(int col_idx = 0; col_idx < static_cast<int>(input.cols.size()); col_idx++) {
                            Type col_type = input.cols.at(col_idx).type;
                            switch(col_type) {
                                case Type::INT64: {
                                    int64_t v = input.cols.at(col_idx).i64.at(row_idx);
                                    table.cols.at(col_idx).append_int(v);
                                    break;
                                }
                                case Type::DOUBLE: {
                                    double v = input.cols.at(col_idx).f64.at(row_idx);
                                    table.cols.at(col_idx).append_double(v);
                                    break;
                                }
                                case Type::STRING: {
                                    table.cols.at(col_idx).append_string(input.cols.at(col_idx).str.at(row_idx));
                                    break;
                                }
                            }
                        }
                        used_idx.push_back(row_idx);
                    }
                }
            }
        }
        return table;
    }
    Table project(const Table& input, const std::vector<size_t>& column_indices) {
        Table table;
        if(input.names.size() == input.cols.size()) {
            std::vector<size_t> used_idx;
            for(size_t idx = 0; idx < input.cols.size(); idx++) {
                auto it = std::find(column_indices.begin(), column_indices.end(), idx);
                if(it != column_indices.end()){
                    std::vector<size_t>::iterator used = std::find(used_idx.begin(), used_idx.end(), idx);
                    if(used == used_idx.end()) {
                        std::string name = input.names.at(idx);
                        Column col = input.cols.at(idx);
                        table.add_column(name, col);
                    }
                }
            }
        }
        return table;
    }

    Table order_by(const Table& input, size_t column_index, SortOrder order) {
        Table table;
        if(input.names.size() == input.cols.size()) {
            Column col = input.cols.at(column_index);
            RowComparator comp{col, order};  
            std::vector<size_t> indices;
            for(size_t idx = 0; idx < input.rows(); idx++) {
                indices.push_back(idx);
            }    
            std::stable_sort(indices.begin(), indices.end(), comp);   
            if(indices.size() == input.rows()) {
                for(size_t idx = 0; idx < input.cols.size(); idx++) {
                    std::string table_name = input.names.at(idx);
                    Column table_col(input.cols.at(idx).type);
                    table.add_column(table_name, table_col);
                }
                for(size_t row_idx:indices) {
                    for(size_t col_idx = 0; col_idx < table.cols.size(); col_idx++) {
                        Type col_type = table.cols.at(col_idx).type;
                        switch(col_type) {
                            case Type::INT64: {
                                int64_t i64_v = input.cols.at(col_idx).i64.at(row_idx);
                                table.cols.at(col_idx).append_int(i64_v);
                                break;
                            }
                            case Type::DOUBLE: {
                                double f64_v = input.cols.at(col_idx).f64.at(row_idx);
                                table.cols.at(col_idx).append_double(f64_v);
                                break;
                            }
                            case Type::STRING: {
                                table.cols.at(col_idx).append_string(input.cols.at(col_idx).str.at(row_idx));
                                break;
                            }
                        }
                    }
                }
            }      
        }
        return table;
    }
    Table group_by(const Table& input, size_t key_col, size_t value_col, AggKind agg) {
        Table table;
        if(input.names.size() == input.cols.size()) {
            Column table_key_col = input.cols.at(key_col);
            Column table_value_col = input.cols.at(value_col);
            Type key_type = table_key_col.type;
            Type value_type = table_value_col.type;
            table.add_column(input.names.at(key_col), Column(key_type));
            switch(agg) {
                case AggKind::COUNT: {
                    table.add_column("Count", Column(Type::INT64));
                    std::map<Value, int64_t> table_map;
                    for(size_t idx = 0; idx < input.rows(); idx++) {
                        Value v;
                        switch(key_type) {
                            case Type::INT64: {
                                v = table_key_col.i64.at(idx);
                                break;
                            }
                            case Type::DOUBLE: {
                                v = table_key_col.f64.at(idx);
                                break;
                            }
                            case Type::STRING: {
                                v = table_key_col.str.at(idx);
                                break;
                            }
                        }
                        if(table_map.find(v) == table_map.end()) {
                            table_map[v] = 1;
                        }
                        else {
                            table_map[v] = table_map[v] + 1;
                        }
                    }
                    for (auto const& pair : table_map) {
                        switch(key_type) {
                            case Type::INT64: {
                                table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                break;
                            }
                            case Type::DOUBLE: {
                                table.cols.at(0).append_double(std::get<double>(pair.first));
                                break;
                            }
                            case Type::STRING: {
                                table.cols.at(0).append_string(std::get<std::string>(pair.first));
                                break;
                            }
                        }
                        table.cols.at(1).append_int(static_cast<int64_t>(pair.second));
                    }
                    break;
                }
                case AggKind::SUM: {
                    if(value_type == Type::INT64 || value_type == Type::DOUBLE) {
                        Value v;
                        switch(value_type) {
                            case Type::INT64: {
                                table.add_column("Sum", Column(Type::INT64));
                                break;
                            }
                            case Type::DOUBLE: {
                                table.add_column("Sum", Column(Type::DOUBLE));
                                break;
                            }
                            case Type::STRING: {
                                break;
                            }
                        }
                        std::map<Value, int64_t> int_map;
                        std::map<Value, double> double_map;
                        for(size_t idx = 0; idx < input.rows(); idx++) {
                            switch(key_type) {
                                case Type::INT64: {
                                    v = table_key_col.i64.at(idx);
                                    if(int_map.find(v) == int_map.end()) {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                int_map[v] = table_value_col.i64.at(idx);
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                int_map[v] = table_value_col.f64.at(idx);
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    else {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                int_map[v] = int_map[v] + table_value_col.i64.at(idx);
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                int_map[v] = int_map[v] + table_value_col.f64.at(idx);
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    break;
                                }
                                case Type::DOUBLE: {
                                    v = table_key_col.f64.at(idx);
                                    if(double_map.find(v) == double_map.end()) {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                double_map[v] = table_value_col.i64.at(idx);
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double_map[v] = table_value_col.f64.at(idx);
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    else {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                double_map[v] = double_map[v] + table_value_col.i64.at(idx);
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double_map[v] = double_map[v] + table_value_col.f64.at(idx);
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    break;
                                }
                                case Type::STRING: {
                                    break;
                                }
                            }
                        }
                        switch(key_type) {
                            case Type::INT64: {
                                for(auto const& pair: int_map) {
                                    switch(value_type) {
                                        case Type::INT64: {
                                            table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                            table.cols.at(1).append_int(static_cast<int64_t>(pair.second));
                                            break;
                                        }
                                        case Type::DOUBLE: {
                                            table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                            table.cols.at(1).append_double(static_cast<double>(pair.second));
                                            break;
                                        }
                                        case Type::STRING: {
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case Type::DOUBLE: {
                                for(auto const& pair: double_map) {
                                    switch(value_type) {
                                        case Type::INT64: {
                                            table.cols.at(0).append_double(std::get<double>(pair.first));
                                            table.cols.at(1).append_int(static_cast<int64_t>(pair.second));
                                            break;
                                        }
                                        case Type::DOUBLE: {
                                            table.cols.at(0).append_double(std::get<double>(pair.first));
                                            table.cols.at(1).append_double(static_cast<double>(pair.second));
                                            break;
                                        } 
                                        case Type::STRING: {
                                            break;
                                        }  
                                    }
                                }
                                break;
                            }
                            case Type::STRING: {
                                break;
                            }
                        }
                    }
                    break;
                }
                case AggKind::AVG: {
                    if(value_type == Type::INT64 || value_type == Type::DOUBLE) {
                        Value v;
                        switch(value_type) {
                            case Type::INT64: {
                                table.add_column("Avg", Column(Type::INT64));
                                break;
                            }
                            case Type::DOUBLE: {
                                table.add_column("Avg", Column(Type::DOUBLE));
                                break;
                            }
                            case Type::STRING: {
                                break;
                            }
                        }
                        std::pair<int64_t, int> int_count;
                        std::map<Value, std::pair<int, int>> int_map;
                        std::pair<double, int> double_count;
                        std::map<Value, std::pair<double, int>> double_map;
                        for(size_t idx = 0; idx < input.rows(); idx++) {
                            switch(key_type) {
                                case Type::INT64: {
                                    v = table_key_col.i64.at(idx);
                                    if(int_map.find(v) == int_map.end()) {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                int_count.first = table_value_col.i64.at(idx);
                                                int_count.second = 1;
                                                int_map[v] = int_count;
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                int_count.first = table_value_col.f64.at(idx);
                                                int_count.second = 1;
                                                int_map[v] = int_count;
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    else {
                                        switch(value_type) {
                                                case Type::INT64: {
                                                    int_count.first += table_value_col.i64.at(idx);
                                                    int_count.second += 1;
                                                    int_map[v] = int_count;         
                                                    break;
                                                }
                                                case Type::DOUBLE: {
                                                    int_count.first += table_value_col.f64.at(idx);
                                                    int_count.second += 1;
                                                    int_map[v] = int_count;         
                                                    break;
                                                }
                                                case Type::STRING: {
                                                    break;
                                                }
                                        }     
                                    }
                                    break;
                                }
                                case Type::DOUBLE: {
                                    v = table_key_col.f64.at(idx);
                                    if(double_map.find(v) == double_map.end()) {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                double_count.first = table_value_col.i64.at(idx);
                                                double_count.second = 1;
                                                double_map[v] = double_count;
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double_count.first = table_value_col.f64.at(idx);
                                                double_count.second = 1;
                                                double_map[v] = double_count;
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    else {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                double_count.first += table_value_col.i64.at(idx);
                                                double_count.second += 1;
                                                double_map[v] = double_count; 
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double_count.first += table_value_col.f64.at(idx);
                                                double_count.second += 1;
                                                double_map[v] = double_count; 
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }             
                                    }
                                    break;
                                }
                                case Type::STRING: {
                                    break;
                                }
                            }
                        } 

                        switch(key_type) {
                            case Type::INT64: {
                                for(auto const& pair: int_map) {
                                    switch(value_type) {
                                        case Type::INT64: {
                                            table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                            int64_t i64_v = static_cast<int64_t>(pair.second.first/pair.second.second);
                                            table.cols.at(1).append_int(i64_v);
                                            break;
                                        }
                                        case Type::DOUBLE: {
                                            table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                            double f64_v = static_cast<double>(pair.second.first/pair.second.second);
                                            table.cols.at(1).append_double(f64_v);
                                            break;
                                        }
                                        case Type::STRING: {
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case Type::DOUBLE: {
                                for(auto const& pair: double_map) {
                                    switch(value_type) {
                                        case Type::INT64: {
                                            table.cols.at(0).append_double(std::get<double>(pair.first));
                                            int64_t i64_v = static_cast<int64_t>(pair.second.first/pair.second.second);
                                            table.cols.at(1).append_int(static_cast<int64_t>(i64_v));
                                            break;
                                        }
                                        case Type::DOUBLE: {
                                            table.cols.at(0).append_double(std::get<double>(pair.first));
                                            double f64_v = static_cast<double>(pair.second.first/pair.second.second);
                                            table.cols.at(1).append_double(f64_v);
                                            break;
                                        }
                                        case Type::STRING: {
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case Type::STRING: {
                                break;
                            }
                        }
                    }
                    break;
                }
                case AggKind::MIN: {
                    if(value_type == Type::INT64 || value_type == Type::DOUBLE) {
                        Value v;
                        switch(value_type) {
                            case Type::INT64: {
                                table.add_column("Min", Column(Type::INT64));
                                break;
                            }
                            case Type::DOUBLE: {
                                table.add_column("Min", Column(Type::DOUBLE));
                                break;
                            }
                            case Type::STRING: {
                                break;
                            }
                        }
                        std::map<Value, int64_t> int_map;
                        std::map<Value, double> double_map;
                        for(size_t idx = 0; idx < input.rows(); idx++) {                    
                            switch(key_type) {
                                case Type::INT64: {
                                    v = table_key_col.i64.at(idx);
                                    if(int_map.find(v) == int_map.end()) {
                                        switch(value_type) {
                                            case Type::INT64: {
                                              int_map[v] = table_value_col.i64.at(idx);  
                                              break;
                                            }
                                            case Type::DOUBLE: {
                                                int_map[v] = table_value_col.f64.at(idx);
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    else {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                int64_t lv = table_value_col.i64.at(idx);
                                                if(lv < int_map[v]) {
                                                    int_map[v] = lv;
                                                }  
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double lv = table_value_col.f64.at(idx);
                                                if(lv < int_map[v]) {
                                                    int_map[v] = lv;
                                                } 
                                                break; 
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }             
                                    }
                                    break;
                                }
                                case Type::DOUBLE: {
                                    v = table_key_col.f64.at(idx);
                                    if(double_map.find(v) == double_map.end()) {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                double_map[v] = table_value_col.i64.at(idx);
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double_map[v] = table_value_col.f64.at(idx);
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    else {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                int64_t lv =  table_value_col.i64.at(idx);
                                                if(lv < double_map[v]) {
                                                    double_map[v] = lv;
                                                }  
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double lv =  table_value_col.f64.at(idx);
                                                if(lv < double_map[v]) {
                                                    double_map[v] = lv;
                                                } 
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }           
                                    }
                                    break;
                                }
                                case Type::STRING: {
                                    break;
                                }
                            } 
                        }
                        switch(key_type) {
                            case Type::INT64: {
                                for(auto const& pair: int_map) {
                                    switch(value_type) {
                                        case Type::INT64: {
                                            table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                            table.cols.at(1).append_int(static_cast<int64_t>(pair.second));
                                            break;
                                        }
                                        case Type::DOUBLE: {
                                            table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                            table.cols.at(1).append_double(static_cast<double>(pair.second));
                                            break;
                                        }
                                        case Type::STRING: {
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case Type::DOUBLE: {
                                for(auto const& pair: double_map) {
                                    switch(value_type) {
                                        case Type::INT64: {
                                            table.cols.at(0).append_double(std::get<double>(pair.first));
                                            table.cols.at(1).append_int(static_cast<int64_t>(pair.second));
                                            break;
                                        }
                                        case Type::DOUBLE: {
                                            table.cols.at(0).append_double(std::get<double>(pair.first));
                                            table.cols.at(1).append_double(static_cast<double>(pair.second));
                                            break;
                                        }
                                        case Type::STRING: {
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case Type::STRING: {
                                break;
                            }
                        }
                    }
                    break;
                }
                case AggKind::MAX: {
                    if(value_type == Type::INT64 || value_type == Type::DOUBLE) {
                        Value v;
                        switch(value_type) {
                            case Type::INT64: {
                                table.add_column("Max", Column(Type::INT64));
                                break;
                            }
                            case Type::DOUBLE: {
                                table.add_column("Max", Column(Type::DOUBLE));
                                break;
                            }
                            case Type::STRING: {
                                break;
                            }
                        }
                        std::map<Value, int64_t> int_map;
                        std::map<Value, double> double_map;
                        for(size_t idx = 0; idx < input.rows(); idx++) {                    
                            switch(key_type) {
                                case Type::INT64: {
                                    v = table_key_col.i64.at(idx);
                                    if(int_map.find(v) == int_map.end()) {
                                        switch(value_type) {
                                            case Type::INT64: {
                                              int_map[v] = table_value_col.i64.at(idx);  
                                              break;
                                            }
                                            case Type::DOUBLE: {
                                                int_map[v] = table_value_col.f64.at(idx);
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    else {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                int64_t lv = table_value_col.i64.at(idx);
                                                if(lv > int_map[v]) {
                                                    int_map[v] = lv;
                                                }  
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double lv = table_value_col.f64.at(idx);
                                                if(lv > int_map[v]) {
                                                    int_map[v] = lv;
                                                } 
                                                break; 
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }             
                                    }
                                    break;
                                }
                                case Type::DOUBLE: {
                                    v = table_key_col.f64.at(idx);
                                    if(double_map.find(v) == double_map.end()) {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                double_map[v] = table_value_col.i64.at(idx);
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double_map[v] = table_value_col.f64.at(idx);
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }
                                    }
                                    else {
                                        switch(value_type) {
                                            case Type::INT64: {
                                                int64_t lv =  table_value_col.i64.at(idx);
                                                if(lv > double_map[v]) {
                                                    double_map[v] = lv;
                                                }  
                                                break;
                                            }
                                            case Type::DOUBLE: {
                                                double lv =  table_value_col.f64.at(idx);
                                                if(lv > double_map[v]) {
                                                    double_map[v] = lv;
                                                } 
                                                break;
                                            }
                                            case Type::STRING: {
                                                break;
                                            }
                                        }           
                                    }
                                    break;
                                }
                                case Type::STRING: {
                                    break;
                                }
                            } 
                        }
                        switch(key_type) {
                            case Type::INT64: {
                                for(auto const& pair: int_map) {
                                    switch(value_type) {
                                        case Type::INT64: {
                                            table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                            table.cols.at(1).append_int(static_cast<int64_t>(pair.second));
                                            break;
                                        }
                                        case Type::DOUBLE: {
                                            table.cols.at(0).append_int(std::get<int64_t>(pair.first));
                                            table.cols.at(1).append_double(static_cast<double>(pair.second));
                                            break;
                                        }
                                        case Type::STRING: {
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case Type::DOUBLE: {
                                for(auto const& pair: double_map) {
                                    switch(value_type) {
                                        case Type::INT64: {
                                            table.cols.at(0).append_double(std::get<double>(pair.first));
                                            table.cols.at(1).append_int(static_cast<int64_t>(pair.second));
                                            break;
                                        }
                                        case Type::DOUBLE: {
                                            table.cols.at(0).append_double(std::get<double>(pair.first));
                                            table.cols.at(1).append_double(static_cast<double>(pair.second));
                                            break;
                                        }
                                        case Type::STRING: {
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case Type::STRING: {
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }
        return table;
    }
    Table limit(const Table& input, size_t n) {
        Table table;
        if(input.names.size() == input.cols.size()) {
            for(size_t idx = 0; idx < input.cols.size(); idx++) {
                std::string table_name = input.names.at(idx);
                Type table_col_type = input.cols.at(idx).type;
                Column table_col(table_col_type);
                table.add_column(table_name, table_col);
            }     
            for(size_t row_idx = 0; row_idx < n; row_idx++) {
                for(size_t col_idx = 0; col_idx < input.cols.size(); col_idx++) {
                    Column col = input.cols.at(col_idx);
                    Type type = col.type;
                    switch(type) {
                        case Type::INT64: {
                            int64_t i64 = col.i64.at(row_idx);
                            table.cols.at(col_idx).append_int(i64);
                            break;
                        }
                        case Type::DOUBLE: {
                            double f64 = col.f64.at(row_idx);
                            table.cols.at(col_idx).append_double(f64);
                            break;
                        }
                        case Type::STRING: {
                            std::string str = col.str.at(row_idx);
                            table.cols.at(col_idx).append_string(str);
                            break;
                        }
                    }
                }
            }   
        }
        return table;
    }
}