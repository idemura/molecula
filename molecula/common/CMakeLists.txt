add_library(
    molecula_common
    STATIC
    Common.cpp
    Common.hpp
)

target_include_directories(
    molecula_common
    PUBLIC
    ${CMAKE_SOURCE_DIR}
)

if(MOLECULA_BUILD_TESTS)
    add_executable(
        molecula_common_test
        Common_Test.cpp
    )

    target_link_libraries(
        molecula_common_test
        PRIVATE
        molecula_common
        GTest::gtest
        GTest::gtest_main
    )

    add_test(
        NAME molecula_common
        COMMAND
        molecula_common_test)
endif()
