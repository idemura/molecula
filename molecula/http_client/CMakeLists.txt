add_library(molecula_http_client STATIC
    HttpClientImpl.cpp
)

target_include_directories(molecula_http_client
    PRIVATE
        ${CMAKE_SOURCE_DIR}
)

find_path(LIBEVENT_INCLUDE_DIR event2/event.h)
target_include_directories(molecula_http_client
    PRIVATE
        ${LIBEVENT_INCLUDE_DIR}
)

find_library(LIBEVENT_LIBRARY NAMES event)
target_link_libraries(molecula_http_client
    PRIVATE
        ${LIBEVENT_LIBRARY}
)
