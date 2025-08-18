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

find_package(glog REQUIRED)

target_link_libraries(
    molecula_common
    PRIVATE
    glog::glog)
