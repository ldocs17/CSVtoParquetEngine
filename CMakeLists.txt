cmake_minimum_required(VERSION 3.16)
project(csv2parquet LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Apache Arrow + Parquet C++ libraries.
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(csv2parquet CSVtoParquet.cpp)

# Link Arrow/Parquet. Newer Arrow exposes namespaced targets; fall back to the
# plain names on older installs.
if(TARGET Arrow::arrow_shared AND TARGET Parquet::parquet_shared)
  target_link_libraries(csv2parquet PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
else()
  target_link_libraries(csv2parquet PRIVATE arrow parquet)
endif()

# pthreads for std::async / worker threads on Linux.
find_package(Threads REQUIRED)
target_link_libraries(csv2parquet PRIVATE Threads::Threads)
