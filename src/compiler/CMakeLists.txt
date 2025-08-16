add_library(molecula_compiler STATIC
    Compiler.cpp
)

target_include_directories(molecula_compiler
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/..
)
