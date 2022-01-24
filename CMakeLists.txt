message("\n==== cpb ====")
cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 11)

project(cpb)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
add_executable(${PROJECT_NAME} cpb/main.cpp)
target_include_directories(${PROJECT_NAME} PUBLIC cpb/hlib)
target_link_libraries(${PROJECT_NAME} pthread)
