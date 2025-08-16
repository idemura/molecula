add_library(molecula_compiler STATIC
    Compiler.cpp
)

target_include_directories(molecula_compiler
    PRIVATE
        ${CMAKE_SOURCE_DIR}
)
target_include_directories(molecula_compiler
    PRIVATE
        ${CMAKE_SOURCE_DIR}/velox
)
