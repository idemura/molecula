#include "molecula/http_client/HttpClientImpl.hpp"

#include <iostream>

static size_t write_callback(char* ptr, size_t size, size_t nmemb, void* arg) {
  size_t total = size * nmemb;
  reinterpret_cast<molecula::HttpRequest*>(arg)->appendData(ptr, total);
  return total;
}

namespace molecula {
static std::once_flag curlGlobalInitFlag;
std::unique_ptr<HttpClient> createHttpClient(const HttpClientParams& params) {
  std::call_once(
      curlGlobalInitFlag, []() { curl_global_init(CURL_GLOBAL_DEFAULT); });

  auto* curlMmultiHandle = curl_multi_init();
  if (!curlMmultiHandle) {
    return nullptr;
  }
  return std::make_unique<HttpClientImpl>(curlMmultiHandle, params);
}

HttpClientImpl::~HttpClientImpl() {
  curl_multi_cleanup(curlMultiHandle_);
}

std::unique_ptr<HttpRequest> HttpClientImpl::makeRequest(const char* url) {
  auto request = std::make_unique<HttpRequest>();
  CURL* easyHandle = curl_easy_init();
  curl_easy_setopt(easyHandle, CURLOPT_URL, url);
  curl_easy_setopt(easyHandle, CURLOPT_WRITEFUNCTION, write_callback);
  curl_easy_setopt(easyHandle, CURLOPT_WRITEDATA, request.get());
  curl_multi_add_handle(curlMultiHandle_, easyHandle);

  // Event loop
  int stillRunning = 0;
  curl_multi_perform(curlMultiHandle_, &stillRunning);
  while (stillRunning) {
    int numfds = 0;
    CURLMcode mc = curl_multi_poll(curlMultiHandle_, nullptr, 0, 1000, &numfds);
    if (mc != CURLM_OK) {
      std::cerr << "curl_multi_poll() failed: " << curl_multi_strerror(mc)
                << "\n";
      break;
    }
    curl_multi_perform(curlMultiHandle_, &stillRunning);
  }

  long status = 0;
  curl_easy_getinfo(easyHandle, CURLINFO_RESPONSE_CODE, &status);
  request->setHttpStatus(status);

  curl_multi_remove_handle(curlMultiHandle_, easyHandle);
  curl_easy_cleanup(easyHandle);

  return request;
}
} // namespace molecula
