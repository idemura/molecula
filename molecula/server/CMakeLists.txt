add_executable(
    molecula_server
    main.cpp
)

target_include_directories(
    molecula_server
    PUBLIC
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    molecula_common
    PRIVATE
    glog::glog)

target_include_directories(
    molecula_server
    PRIVATE
    ${CMAKE_SOURCE_DIR}/velox
)

target_link_libraries(
    molecula_server
    PUBLIC
    molecula_compiler
    molecula_http_client
    molecula_s3
    PRIVATE
    velox_core
    velox_exception
    velox_exec
    velox_status
    velox_type
    velox_vector
)
