#include "engine/csv.hpp"
#include "engine/column.hpp"
#include "engine/csv.hpp"
#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <cstdint>

namespace qe {

    bool parse_int(std::string str) {
        try {
            std::stoi(str);
            return true;
        }
        catch (std::exception& e){
            return false;
        }
    }

    bool parse_double(std::string str) {
        try {
            std::stod(str);
            return true;
        }
        catch (std::exception& e){
            return false;
        }
    }

    Table load_csv(const std::string& path, char delimiter, bool has_header) {
        Table table;

        std::vector<Column> cols;
        std::vector<std::string> names;

        std::string line;
        std::vector<std::string> tokens;
        std::string token;
        std::ifstream inputFile(path); 

        if (!inputFile.is_open()) {
            throw std::runtime_error("Invalid file path.");
        }

        while(std::getline(inputFile, line)) {
            int idx = 0;
            std::stringstream ss(line);
            std::string token;
            if(has_header) {
                while(std::getline(ss, token, delimiter)) {
                    names.push_back(token);
                }
                table.names = names;
                has_header = false;
                continue;
            }
            while(std::getline(ss, token, delimiter)) {
                if(token.empty() || token == "null" || token == "Null") {
                    continue;
                }
                if(parse_int(token)) {
                    int64_t v = std::stoi(token);
                    if(idx >= static_cast<int>(cols.size())) {
                        Column col(Type::INT64);
                        col.append_int(v);
                        cols.push_back(col);
                    }
                    else {
                        if(cols.at(idx).type == Type::INT64) {
                            cols.at(idx).append_int(v);
                        }
                    }
                }
                else if(parse_double(token)) {
                    double v = std::stod(token);
                    if(idx >= static_cast<int>(cols.size())) {
                        Column col(Type::DOUBLE);
                        col.append_double(v);
                        cols.push_back(col);
                    }
                    else {
                        if(cols.at(idx).type == Type::DOUBLE) {
                            cols.at(idx).append_double(v);
                        }
                    }
                }
                else {
                    if(idx >= static_cast<int>(cols.size())) {
                        Column col(Type::STRING);
                        col.append_string(token);
                        cols.push_back(col);
                    }
                    else {
                        if(cols.at(idx).type == Type::STRING) {
                            cols.at(idx).append_string(token);
                        }
                    }
                }
                idx += 1;
            }
        }

        table.cols = cols;

        return table;

    }

}