#pragma once

#include <string_view>

namespace molecula {
struct S3AsyncClientConfig {
  std::string_view host;
  int port = 0;
};

class S3AsyncClient {
public:
  static std::unique_ptr<S3AsyncClient> create(S3AsyncClientConfig* config);

private:
  S3AsyncClient();
};
}
