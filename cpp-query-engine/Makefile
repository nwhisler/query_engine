CXX = g++
CXXFLAGS = -std=c++20 -O2 -Wall -Wextra -Iinclude

SRC = src/main.cpp src/engine.cpp src/table.cpp src/column.cpp src/csv.cpp src/operators.cpp
OUT = qe_main

TST_SRC = tests/test_storage.cpp src/table.cpp src/column.cpp src/csv.cpp src/operators.cpp src/engine.cpp
TST_OUT = qe_tests

.PHONY: all test clean

all: $(OUT)

$(OUT): $(SRC) 
	$(CXX) $(CXXFLAGS) $(SRC) -o $(OUT)

$(TST_OUT): $(TST_SRC)
	$(CXX) $(CXXFLAGS) $(TST_SRC) -o $(TST_OUT)

test: $(TST_OUT)
	./$(TST_OUT)

clean:
	rm -f $(OUT) $(TST_OUT)