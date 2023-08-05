cmake_minimum_required(VERSION 3.12)
project(multithreaded-external-merge-sort CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_library(multithreaded_external_merge_sorter INTERFACE)
target_include_directories(multithreaded_external_merge_sorter INTERFACE src)

add_executable(bench src/bench.cpp)
target_link_libraries(bench multithreaded_external_merge_sorter)
