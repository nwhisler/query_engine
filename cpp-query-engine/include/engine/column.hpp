#ifndef COLUMN_H
#define COLUMN_H
#include <cstdint>
#include <string>
#include <vector>
#include <variant>

namespace qe {

    enum class Type {
        INT64,
        DOUBLE,
        STRING
    };

struct Column {

    Type type;

    explicit Column(Type t);

    std::vector<int64_t> i64;
    std::vector<double> f64;
    std::vector<std::string> str;

    std::vector<uint8_t> valid;

    size_t size() const;
    void append_int(int64_t v);
    void append_double(double v);
    void append_string(std::string v);

    };

}

#endif