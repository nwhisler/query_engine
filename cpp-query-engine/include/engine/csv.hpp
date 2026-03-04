#ifndef CSV_H
#define CSV_H
#include "table.hpp"
#include <string>
#include <cstdint>

namespace qe {

    Table load_csv(const std::string& path, char delimiter = ',', bool has_header = true);

}

#endif