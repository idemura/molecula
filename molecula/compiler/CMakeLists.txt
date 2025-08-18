add_library(
    molecula_compiler
    STATIC
    Compiler.cpp
    Compiler.hpp
)

target_include_directories(
    molecula_compiler
    PUBLIC
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    molecula_common
    PRIVATE
    glog::glog)
