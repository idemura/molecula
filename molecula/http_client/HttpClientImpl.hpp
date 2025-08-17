#pragma once

#include <event2/buffer.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include "molecula/http_client/HttpClient.hpp"

namespace molecula {
/// Async HTTP client based on libevent.
class HttpClientImpl final : public HttpClient {
 public:
  explicit HttpClientImpl(event_base* base) : base_{base} {}
  ~HttpClientImpl() override;

  void makeRequest(const char* host, int port, const char* path) override;
  void requestDone(evhttp_request* request);

 private:
  event_base* base_{nullptr};
};
} // namespace molecula
