#include "engine/table.hpp"
#include "engine/column.hpp"
#include "engine/engine.hpp"
#include "engine/operators.hpp"
#include "engine/csv.hpp"
#include<iostream>
#include <filesystem>

int main() {
    
    std::filesystem::path dir_path = std::filesystem::current_path();
    std::filesystem::path file_path = dir_path / "tests/employees.csv";
    qe::Table table = qe::load_csv(file_path.string());

    qe::FilterClause fc;
    fc.col = 1;
    fc.op = qe::Predicate::GT;
    fc.constant = 50;
    
    qe::GroupByClause gbc;
    gbc.key_col = 0;
    gbc.value_col = 0;
    gbc.agg = qe::AggKind::COUNT;

    qe::OrderByClause obc;
    obc.col = 1;
    obc.order = qe::SortOrder::DESC;
    
    qe::LimitClause lc;
    lc.n = 3;

    qe::Query query;
    query.where = fc;
    query.group_by = gbc;
    query.order_by = obc;
    query.limit = lc;

    qe::Table execute = qe::execute(table, query);
                
    std::cout << execute.cols.at(0).str.at(0) << " " << execute.cols.at(1).i64.at(0) << std::endl;
    std::cout << execute.cols.at(0).str.at(1) << " " << execute.cols.at(1).i64.at(1) << std::endl;
    std::cout << execute.cols.at(0).str.at(2) << " " << execute.cols.at(1).i64.at(2) << std::endl;
}