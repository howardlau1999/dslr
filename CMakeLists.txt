cmake_minimum_required(VERSION 3.27 FATAL_ERROR)
project(dslr)
set(CMAKE_CXX_STANDARD 20)

find_package(Threads REQUIRED)

find_package(Boost 1.83 REQUIRED COMPONENTS system log)

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()

add_subdirectory(third_party/rdmapp-unsafe)
include_directories(include)
add_library(dslr src/dslr.cc)
target_link_libraries(dslr PUBLIC rdmapp Threads::Threads ${Boost_LIBRARIES})
add_executable(dslr_test src/main.cc)
target_link_libraries(dslr_test dslr ${Boost_LIBRARIES})