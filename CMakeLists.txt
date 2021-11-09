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
include_directories(component/query_process)
include_directories(component/mongo_client)
find_package(libmongocxx REQUIRED)
find_package(libbsoncxx REQUIRED)
include_directories(${LIBMONGOCXX_INCLUDE_DIR})
include_directories(${LIBBSONCXX_INCLUDE_DIR})
include_directories("/usr/local/include/mongocxx/v_noabi")
include_directories("/usr/local/include/bsoncxx/v_noabi")
include_directories("/usr/local/include/libmongoc-1.0")
include_directories("/usr/local/include/libbson-1.0")
include_directories("/usr/local/lib")

file(GLOB SOURCES
        component/HTMLParser.cpp
        component/CoreBuilder.cpp
        component/PostingsBuilder.cpp
        component/DocTableBuilder.cpp
        component/utils/BitStream.cpp
        component/utils/encoding.cpp
        component/InvertedListBuilder.cpp
        component/BlockEncoder.cpp
        component/LexiconBuilder.cpp
        component/query_process/QueryExecution.cpp
)
file(GLOB HEADERS)
add_library(inverted_index ${SOURCES} component/mongo_client/MongoService.cpp component/mongo_client/MongoService.h)
target_link_libraries(inverted_index gflags::gflags ${LIBMONGOCXX_LIBRARIES} ${LIBBSONCXX_LIBRARIES})

add_executable(index_builder main.cpp)
target_link_libraries(index_builder inverted_index)

add_executable(query_process
        query_process.cpp
        )
target_link_libraries(query_process inverted_index )