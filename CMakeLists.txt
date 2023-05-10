cmake_minimum_required(VERSION 3.3 FATAL_ERROR)
project(dslr)
set(CMAKE_CXX_STANDARD 20)

find_package(Threads REQUIRED)

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()

add_subdirectory(third_party/rdmapp)
include_directories(include)
add_library(dslr src/dslr.cc)
target_link_libraries(dslr PUBLIC rdmapp Threads::Threads)
add_executable(dslr_test src/main.cc)
target_link_libraries(dslr_test dslr)