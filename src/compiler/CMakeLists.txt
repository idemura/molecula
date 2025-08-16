add_library(compiler STATIC
    Compiler.cpp
)

target_include_directories(compiler
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/..
)
