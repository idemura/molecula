#include "molecula/storage/S3AsyncClient.hpp"

namespace molecula {
std::unique_ptr<S3AsyncClient> create(S3AsyncClientConfig* config) {
  return nullptr;
}

S3AsyncClient::S3AsyncClient() {
  // Empty
}
}
