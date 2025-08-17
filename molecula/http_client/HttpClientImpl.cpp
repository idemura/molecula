#include "molecula/http_client/HttpClientImpl.hpp"

#include <iostream>

static void http_request_done(evhttp_request* request, void* arg) {
  reinterpret_cast<molecula::HttpClientImpl*>(arg)->requestDone(request);
}

namespace molecula {
std::unique_ptr<HttpClient> createHttpClient() {
  auto base = event_base_new();
  if (!base) {
    return nullptr;
  }
  return std::make_unique<HttpClientImpl>(base);
}

HttpClientImpl::~HttpClientImpl() {
  event_base_free(base_);
}

void HttpClientImpl::makeRequest(const char* host, int port, const char* path) {
  // Create connection
  auto* connection = evhttp_connection_base_new(base_, nullptr, host, port);
  std::cerr << "Connection " << connection << " opened\n";

  evhttp_connection_set_closecb(
      connection,
      [](evhttp_connection* conn, void* arg) {
        std::cerr << "Connection " << conn << " closed\n";
      },
      nullptr);

  // Make request
  auto* request = evhttp_request_new(http_request_done, this);
  // evhttp_add_header(evhttp_request_get_output_headers(request), "Host",
  // host);
  evhttp_make_request(connection, request, EVHTTP_REQ_GET, path);

  // Enter async loop and wait
  event_base_dispatch(base_);
  // event_base_loop(base_, EVLOOP_NO_EXIT_ON_EMPTY);

  std::cout << "Done\n";
  evhttp_connection_free(connection);
}

void HttpClientImpl::requestDone(evhttp_request* request) {
  if (!request) {
    std::cerr << "Request failed (timeout or connection error)\n";
    return;
  }

  // Get the response buffer
  auto* buffer = evhttp_request_get_input_buffer(request);
  size_t bufferLength = evbuffer_get_length(buffer);

  std::cout << "HTTP status: " << evhttp_request_get_response_code(request)
            << "\n";
  std::cout << "Data size: " << bufferLength << "\n";

  auto data = std::make_unique<char[]>(bufferLength + 1);
  evbuffer_copyout(buffer, data.get(), bufferLength);
  data[bufferLength] = 0;

  std::cout << "Response body:\n" << data.get() << "\n";

  // Exit after handling one request
  event_base_loopexit(base_, nullptr);
}
} // namespace molecula
