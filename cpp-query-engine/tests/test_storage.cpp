#include "engine/table.hpp"
#include "engine/column.hpp"
#include "engine/csv.hpp"
#include "engine/operators.hpp"
#include "engine/engine.hpp"
#include <cassert>
#include <cstdint>
#include <string>
#include <iostream>
#include <filesystem>

void emptyRowsTest() {
    qe::Table table;
    assert(table.rows() == 0);
}

void threeRowsTest() {
    std::string name = "number";

    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    
    qe::Table table;
    table.add_column(name, col);
    
    assert(table.rows() == 3);
    assert(table.names.size() == table.cols.size());
}

void incorrectRowSizeTest() {

    std::string name = "number";

    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    
    qe::Table table;
    table.add_column(name, col);

    std::string another_name = "more_numbers";

    qe::Column another_col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    col.append_int(3);

    table.add_column(another_name, another_col);

    assert(table.rows() == 3);

}

void incorrectDataTypeTest() {

    std::string name = "number";
    qe::Table table;

    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    try {
        col.append_int(3.0);
    }
    catch(std::exception& e) {

        assert(true);

    }

}

void csvTest() {

    std::filesystem::path dir_path = std::filesystem::current_path();
    std::filesystem::path file_path = dir_path / "tests/test.csv";
    qe::Table table = qe::load_csv(file_path.string());

    assert(table.rows() == 2);
    assert(table.cols.size() == 2);

}

void filterTest() {

    qe::Table table;
    
    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(0);
    col.append_int(2);
    col.append_int(0);

    qe::Column another_col = qe::Column(qe::Type::DOUBLE);
    another_col.append_double(0.0);
    another_col.append_double(1.0);
    another_col.append_double(2.0);
    another_col.append_double(3.0);
    another_col.append_double(4.0);

    table.add_column("first_column", col);
    table.add_column("second_column", another_col);

    qe::Table filtered = qe::filter(table, static_cast<size_t>(0), qe::Predicate::EQ, static_cast<int64_t>(0));

    assert(filtered.cols.size() == 2);
    assert(filtered.rows() == 3);

}

void projectTest() {

    qe::Table table;
    
    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    col.append_int(3);


    qe::Column another_col = qe::Column(qe::Type::DOUBLE);
    another_col.append_double(0.0);
    another_col.append_double(1.0);
    another_col.append_double(2.0);
    another_col.append_double(3.0);

    qe::Column final_col = qe::Column(qe::Type::STRING);
    final_col.append_string("zero");
    final_col.append_string("one");
    final_col.append_string("two");
    final_col.append_string("three");

    table.add_column("first_column", col);
    table.add_column("second_column", another_col);
    table.add_column("third_column", final_col);

    std::vector<size_t> idx;
    idx.push_back(1);

    qe::Table project = qe::project(table, idx);

    assert(project.cols.size() == 1);
    assert(project.rows() == 4);
    assert(project.cols.at(0).f64.at(0) == 0.0);
    assert(project.cols.at(0).f64.at(1) == 1.0);
    assert(project.cols.at(0).f64.at(2) == 2.0);
    assert(project.cols.at(0).f64.at(3) == 3.0);

}

void orderByTest() {

    qe::Table table;
    
    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    col.append_int(3);


    qe::Column another_col = qe::Column(qe::Type::DOUBLE);
    another_col.append_double(0.0);
    another_col.append_double(1.0);
    another_col.append_double(2.0);
    another_col.append_double(3.0);

    qe::Column final_col = qe::Column(qe::Type::STRING);
    final_col.append_string("zero");
    final_col.append_string("one");
    final_col.append_string("two");
    final_col.append_string("three");

    table.add_column("first_column", col);
    table.add_column("second_column", another_col);
    table.add_column("third_column", final_col);

    qe::Table ordered = order_by(table, 0, qe::SortOrder::DESC);

    assert(ordered.cols.size() == 3);
    assert(ordered.rows() == 4);
    
    assert(ordered.cols.at(0).i64.at(0) == 3);
    assert(ordered.cols.at(0).i64.at(1) == 2);
    assert(ordered.cols.at(0).i64.at(2) == 1);
    assert(ordered.cols.at(0).i64.at(3) == 0);

    assert(ordered.cols.at(1).f64.at(0) == 3.0);
    assert(ordered.cols.at(1).f64.at(1) == 2.0);
    assert(ordered.cols.at(1).f64.at(2) == 1.0);
    assert(ordered.cols.at(1).f64.at(3) == 0.0);

    assert(ordered.cols.at(2).str.at(0) == "three");
    assert(ordered.cols.at(2).str.at(1) == "two");
    assert(ordered.cols.at(2).str.at(2) == "one");
    assert(ordered.cols.at(2).str.at(3) == "zero");

}

void groupByTest() {

    qe::Table table_double;
    
    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    col.append_int(1);

    qe::Column another_col = qe::Column(qe::Type::DOUBLE);
    another_col.append_double(0.0);
    another_col.append_double(1.0);
    another_col.append_double(2.0);
    another_col.append_double(3.0);

    qe::Column final_col = qe::Column(qe::Type::STRING);
    final_col.append_string("zero");
    final_col.append_string("one");
    final_col.append_string("two");
    final_col.append_string("three");

    table_double.add_column("first_column", col);
    table_double.add_column("second_column", another_col);
    table_double.add_column("third_column", final_col);

    qe::Table groupBy_count = qe::group_by(table_double, 0, 1, qe::AggKind::COUNT);

    assert(groupBy_count.cols.size() == 2);
    assert(groupBy_count.rows() == 3);

    assert(groupBy_count.cols.at(0).i64.at(0) == 0);
    assert(groupBy_count.cols.at(0).i64.at(1) == 1);
    assert(groupBy_count.cols.at(0).i64.at(2) == 2);

    assert(groupBy_count.cols.at(1).i64.at(0) == 1);
    assert(groupBy_count.cols.at(1).i64.at(1) == 2);
    assert(groupBy_count.cols.at(1).i64.at(2) == 1);

    qe::Table groupBy_sum = qe::group_by(table_double, 0, 1, qe::AggKind::SUM);

    assert(groupBy_sum.cols.size() == 2);
    assert(groupBy_sum.rows() == 3);
    
    assert(groupBy_sum.cols.at(0).i64.at(0) == 0);
    assert(groupBy_sum.cols.at(0).i64.at(1) == 1);
    assert(groupBy_sum.cols.at(0).i64.at(2) == 2);

    assert(groupBy_sum.cols.at(1).f64.at(0) == 0.0);
    assert(groupBy_sum.cols.at(1).f64.at(1) == 4.0);
    assert(groupBy_sum.cols.at(1).f64.at(2) == 2.0);

    qe::Table groupBy_avg = qe::group_by(table_double, 0, 1, qe::AggKind::AVG);

    assert(groupBy_avg.cols.size() == 2);
    assert(groupBy_avg.rows() == 3);
    
    assert(groupBy_avg.cols.at(0).i64.at(0) == 0);
    assert(groupBy_avg.cols.at(0).i64.at(1) == 1);
    assert(groupBy_avg.cols.at(0).i64.at(2) == 2);

    assert(groupBy_avg.cols.at(1).f64.at(0) == 0.0);
    assert(groupBy_avg.cols.at(1).f64.at(1) == 2.0);
    assert(groupBy_avg.cols.at(1).f64.at(2) == 2.0);

    qe::Table groupBy_min = qe::group_by(table_double, 0, 1, qe::AggKind::MIN);

    assert(groupBy_min.cols.size() == 2);
    assert(groupBy_min.rows() == 3);
    
    assert(groupBy_min.cols.at(0).i64.at(0) == 0);
    assert(groupBy_min.cols.at(0).i64.at(1) == 1);
    assert(groupBy_min.cols.at(0).i64.at(2) == 2);

    assert(groupBy_min.cols.at(1).f64.at(0) == 0.0);
    assert(groupBy_min.cols.at(1).f64.at(1) == 1.0);
    assert(groupBy_min.cols.at(1).f64.at(2) == 2.0);

    qe::Table groupBy_max = qe::group_by(table_double, 0, 1, qe::AggKind::MAX);

    assert(groupBy_max.cols.size() == 2);
    assert(groupBy_max.rows() == 3);
    
    assert(groupBy_max.cols.at(0).i64.at(0) == 0);
    assert(groupBy_max.cols.at(0).i64.at(1) == 1);
    assert(groupBy_max.cols.at(0).i64.at(2) == 2);

    assert(groupBy_max.cols.at(1).f64.at(0) == 0.0);
    assert(groupBy_max.cols.at(1).f64.at(1) == 3.0);
    assert(groupBy_max.cols.at(1).f64.at(2) == 2.0);

    qe::Table table_int;
    
    qe::Column col_ = qe::Column(qe::Type::INT64);
    col_.append_int(0);
    col_.append_int(1);
    col_.append_int(2);
    col_.append_int(1);

    qe::Column another_col_ = qe::Column(qe::Type::INT64);
    another_col_.append_int(0);
    another_col_.append_int(1);
    another_col_.append_int(2);
    another_col_.append_int(3);

    qe::Column final_col_ = qe::Column(qe::Type::STRING);
    final_col_.append_string("zero");
    final_col_.append_string("one");
    final_col_.append_string("two");
    final_col_.append_string("three");

    table_int.add_column("first_column", col_);
    table_int.add_column("second_column", another_col_);
    table_int.add_column("third_column", final_col_);

    groupBy_count = qe::group_by(table_int, 0, 1, qe::AggKind::COUNT);

    assert(groupBy_count.cols.size() == 2);
    assert(groupBy_count.rows() == 3);

    assert(groupBy_count.cols.at(0).i64.at(0) == 0);
    assert(groupBy_count.cols.at(0).i64.at(1) == 1);
    assert(groupBy_count.cols.at(0).i64.at(2) == 2);

    assert(groupBy_count.cols.at(1).i64.at(0) == 1);
    assert(groupBy_count.cols.at(1).i64.at(1) == 2);
    assert(groupBy_count.cols.at(1).i64.at(2) == 1);

    groupBy_sum = qe::group_by(table_int, 0, 1, qe::AggKind::SUM);

    assert(groupBy_sum.cols.size() == 2);
    assert(groupBy_sum.rows() == 3);
    
    assert(groupBy_sum.cols.at(0).i64.at(0) == 0);
    assert(groupBy_sum.cols.at(0).i64.at(1) == 1);
    assert(groupBy_sum.cols.at(0).i64.at(2) == 2);

    assert(groupBy_sum.cols.at(1).i64.at(0) == 0);
    assert(groupBy_sum.cols.at(1).i64.at(1) == 4);
    assert(groupBy_sum.cols.at(1).i64.at(2) == 2);

    groupBy_avg = qe::group_by(table_int, 0, 1, qe::AggKind::AVG);

    assert(groupBy_avg.cols.size() == 2);
    assert(groupBy_avg.rows() == 3);
    
    assert(groupBy_avg.cols.at(0).i64.at(0) == 0);
    assert(groupBy_avg.cols.at(0).i64.at(1) == 1);
    assert(groupBy_avg.cols.at(0).i64.at(2) == 2);

    assert(groupBy_avg.cols.at(1).i64.at(0) == 0);
    assert(groupBy_avg.cols.at(1).i64.at(1) == 2);
    assert(groupBy_avg.cols.at(1).i64.at(2) == 2);

    groupBy_min = qe::group_by(table_int, 0, 1, qe::AggKind::MIN);

    assert(groupBy_min.cols.size() == 2);
    assert(groupBy_min.rows() == 3);
    
    assert(groupBy_min.cols.at(0).i64.at(0) == 0);
    assert(groupBy_min.cols.at(0).i64.at(1) == 1);
    assert(groupBy_min.cols.at(0).i64.at(2) == 2);

    assert(groupBy_min.cols.at(1).i64.at(0) == 0);
    assert(groupBy_min.cols.at(1).i64.at(1) == 1);
    assert(groupBy_min.cols.at(1).i64.at(2) == 2);

    groupBy_max = qe::group_by(table_int, 0, 1, qe::AggKind::MAX);

    assert(groupBy_max.cols.size() == 2);
    assert(groupBy_max.rows() == 3);
    
    assert(groupBy_max.cols.at(0).i64.at(0) == 0);
    assert(groupBy_max.cols.at(0).i64.at(1) == 1);
    assert(groupBy_max.cols.at(0).i64.at(2) == 2);

    assert(groupBy_max.cols.at(1).i64.at(0) == 0);
    assert(groupBy_max.cols.at(1).i64.at(1) == 3);
    assert(groupBy_max.cols.at(1).i64.at(2) == 2);

}

void limitTest() {

    qe::Table table;
    
    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    col.append_int(3);


    qe::Column another_col = qe::Column(qe::Type::DOUBLE);
    another_col.append_double(0.0);
    another_col.append_double(1.0);
    another_col.append_double(2.0);
    another_col.append_double(3.0);

    qe::Column final_col = qe::Column(qe::Type::STRING);
    final_col.append_string("zero");
    final_col.append_string("one");
    final_col.append_string("two");
    final_col.append_string("three");

    table.add_column("first_column", col);
    table.add_column("second_column", another_col);
    table.add_column("third_column", final_col);

    qe::Table limit = qe::limit(table, 2);

    assert(limit.cols.size() == 3);
    assert(limit.rows() == 2);
    assert(limit.cols.at(0).i64.at(0) == 0);
    assert(limit.cols.at(0).i64.at(1) == 1);
    assert(limit.cols.at(1).f64.at(0) == 0.0);
    assert(limit.cols.at(1).f64.at(1) == 1.0);
    assert(limit.cols.at(2).str.at(0) == "zero");
    assert(limit.cols.at(2).str.at(1) == "one");

}

void executeTest() {

    qe::Table table;
    
    qe::Column col = qe::Column(qe::Type::INT64);
    col.append_int(0);
    col.append_int(1);
    col.append_int(2);
    col.append_int(3);


    qe::Column another_col = qe::Column(qe::Type::DOUBLE);
    another_col.append_double(0.0);
    another_col.append_double(1.0);
    another_col.append_double(2.0);
    another_col.append_double(3.0);

    qe::Column final_col = qe::Column(qe::Type::STRING);
    final_col.append_string("zero");
    final_col.append_string("one");
    final_col.append_string("two");
    final_col.append_string("three");

    table.add_column("first_column", col);
    table.add_column("second_column", another_col);
    table.add_column("third_column", final_col);

    qe::FilterClause fc;
    fc.col = 0;
    fc.op = qe::Predicate::EQ;
    fc.constant = 0;

    qe::Query where_query;
    where_query.where = fc;

    qe::Table execute = qe::execute(table, where_query);

    assert(execute.cols.at(0).i64.at(0) == 0);
    assert(execute.cols.at(1).f64.at(0) == 0.0);
    assert(execute.cols.at(0).str.at(0) == "zero");

    qe::GroupByClause gbc;
    gbc.key_col = 0;
    gbc.value_col = 0.0;
    gbc.agg = qe::AggKind::MIN;

    qe::Query where_and_group_by_query;
    where_and_group_by_query.where = fc;
    where_and_group_by_query.group_by = gbc;

    execute = qe::execute(table, where_and_group_by_query);

    assert(execute.cols.at(0).i64.at(0) == 0);
    assert(execute.cols.at(1).f64.at(0) == 0.0);

    qe::OrderByClause obc;
    obc.col = 0;
    obc.order = qe::SortOrder::DESC;

    qe::Query where_group_by_order_by_query;
    where_group_by_order_by_query.where = fc;
    where_group_by_order_by_query.group_by = gbc;
    where_group_by_order_by_query.order_by = obc;

    execute = qe::execute(table, where_group_by_order_by_query);

    assert(execute.cols.at(0).i64.at(0) == 0);
    assert(execute.cols.at(1).f64.at(0) == 0.0);

    qe::LimitClause lc;
    lc.n = 1;

    qe::Query where_group_by_order_by_limit_query;
    where_group_by_order_by_limit_query.where = fc;
    where_group_by_order_by_limit_query.group_by = gbc;
    where_group_by_order_by_limit_query.order_by = obc;
    where_group_by_order_by_limit_query.limit = lc;

    execute = qe::execute(table, where_group_by_order_by_limit_query);

    assert(execute.cols.at(0).i64.at(0) == 0);
    assert(execute.cols.at(1).f64.at(0) == 0.0);

}

void employeeTest() {
 
    std::filesystem::path dir_path = std::filesystem::current_path();
    std::filesystem::path file_path = dir_path / "tests/employees.csv";
    qe::Table table = qe::load_csv(file_path.string());

    assert(table.rows() == 14);
    assert(table.cols.size() == 3);
    assert(table.names.size() == table.cols.size());

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

    assert(execute.rows() == 3);
    assert(execute.cols.size() == 2);
                
    assert(execute.cols.at(0).str.at(0) == "Engineering");
    assert(execute.cols.at(0).str.at(1) == "Sales");
    assert(execute.cols.at(0).str.at(2) == "HR");

    assert(execute.cols.at(1).i64.at(0) == 3);
    assert(execute.cols.at(1).i64.at(1) == 2);
    assert(execute.cols.at(1).i64.at(2) == 1);

}

int main() {

    emptyRowsTest();
    threeRowsTest();
    incorrectRowSizeTest();
    incorrectDataTypeTest();
    csvTest();
    filterTest();
    projectTest();
    groupByTest();
    limitTest();
    employeeTest();

}