add_executable(molecula_server
    main.cpp
)

target_link_libraries(molecula_server
    PRIVATE compiler
)
