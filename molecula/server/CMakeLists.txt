add_executable(molecula_server
    main.cpp
)

target_link_libraries(molecula_server
    PRIVATE
        molecula_compiler
        velox_core
        velox_exception
        velox_exec
        velox_status
        velox_type
        velox_vector
)

target_include_directories(molecula_server
    PRIVATE
        ${CMAKE_SOURCE_DIR}
)
target_include_directories(molecula_server
    PRIVATE
        ${CMAKE_SOURCE_DIR}/velox
)
