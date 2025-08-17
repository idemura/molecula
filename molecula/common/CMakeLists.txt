add_library(
    molecula_common
    STATIC

    Common.cpp
)

target_include_directories(
    molecula_common
    PRIVATE
    ${CMAKE_SOURCE_DIR}
)
