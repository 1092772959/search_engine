cmake_minimum_required(VERSION 3.20)
project(index_builder)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_subdirectory(third_lib/gflags)
include_directories(third_lib/gflags)

include_directories(component)
include_directories(component/utils)
add_executable(index_builder
        main.cpp
        component/HTMLParser.cpp
        component/CoreBuilder.cpp
        component/PostingsBuilder.cpp
        component/DocTableBuilder.cpp
        component/utils/BitStream.cpp
        component/utils/encoding.cpp
        component/InvertedListBuilder.cpp
        component/BlockEncoder.cpp
        component/LexiconBuilder.cpp component/query_process/QueryExecution.cpp component/query_process/QueryExecution.h)

target_link_libraries(${PROJECT_NAME} gflags::gflags)
