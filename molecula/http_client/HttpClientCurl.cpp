#include "molecula/http_client/HttpClientCurl.hpp"

#include <unistd.h>
#include <iostream>
#include <type_traits>

static size_t buffer_write_callback(char* ptr, size_t size, size_t nmemb, void* arg) {
  size_t total = size * nmemb;
  reinterpret_cast<molecula::HttpBuffer*>(arg)->append(ptr, total);
  return total;
}

namespace molecula {

static_assert(std::is_move_constructible_v<HttpBuffer>);
static_assert(std::is_move_constructible_v<HttpRequest>);
static_assert(std::is_move_constructible_v<HttpResponse>);
static_assert(std::is_move_constructible_v<HttpContext>);

static std::once_flag initOnceFlag;

std::unique_ptr<HttpClient> createHttpClient(const HttpClientParams& params) {
  std::call_once(initOnceFlag, []() { curl_global_init(CURL_GLOBAL_DEFAULT); });

  auto* multiHandle = curl_multi_init();
  if (!multiHandle) {
    return nullptr;
  }
  return std::make_unique<HttpClientCurl>(multiHandle, params);
}

HttpClientCurl::HttpClientCurl(CURLM* multiHandle, const HttpClientParams& params)
    : multiHandle_{multiHandle}, eventThread_{&HttpClientCurl::eventLoop, this}, params_{params} {
  pipe(pipe_);
}

HttpClientCurl::~HttpClientCurl() {
  running_.store(false, std::memory_order_release);
  write(pipe_[1], "", 1);
  if (eventThread_.joinable()) {
    eventThread_.join();
  }
  curl_multi_cleanup(multiHandle_);
  close(pipe_[0]);
  close(pipe_[1]);
}

folly::Future<HttpResponse> HttpClientCurl::makeRequest(HttpRequest request) {
  auto* context = new HttpContext{std::move(request)};

  {
    std::lock_guard<std::mutex> lock{mutex_};
    queue_.emplace(context);
  }
  write(pipe_[1], "", 1);

  // Under the lock, because we delete context in the event loop.
  return context->promise.getFuture();
}

void HttpClientCurl::eventLoop() {
  char buf[16]{};

  while (running_.load(std::memory_order_acquire)) {
    int stillRunning = 0;
    while (curl_multi_perform(multiHandle_, &stillRunning) == CURLM_CALL_MULTI_PERFORM) {
      // Keep performing until no more actions are pending
    }

    // Handle completed transfers
    CURLMsg* msg = nullptr;
    int msgsLeft = 0;
    while ((msg = curl_multi_info_read(multiHandle_, &msgsLeft))) {
      if (msg->msg == CURLMSG_DONE) {
        CURL* easyHandle = msg->easy_handle;

        HttpContext* context = nullptr;
        curl_easy_getinfo(easyHandle, CURLINFO_PRIVATE, (void**)&context);

        long status = 0;
        curl_easy_getinfo(easyHandle, CURLINFO_RESPONSE_CODE, &status);
        curl_multi_remove_handle(multiHandle_, easyHandle);
        curl_easy_cleanup(easyHandle);

        context->promise.setValue(HttpResponse{status, std::move(context->buffer)});

        delete context;
      }
    }

    int numfds = 0;
    curl_waitfd curl_fd{pipe_[0], CURL_WAIT_POLLIN};
    curl_multi_wait(multiHandle_, &curl_fd, 1, 1000, &numfds);

    // Add new requests from queue
    {
      std::unique_lock<std::mutex> lock(mutex_);
      while (!queue_.empty()) {
        // Every time we enqueue a new request, we write to the pipe. Read one byte back.
        read(pipe_[0], buf, 1);

        auto* context = queue_.front();
        queue_.pop();

        CURL* easyHandle = curl_easy_init();
        curl_easy_setopt(easyHandle, CURLOPT_URL, context->request.getUrl().c_str());
        curl_easy_setopt(easyHandle, CURLOPT_PRIVATE, context);
        curl_easy_setopt(easyHandle, CURLOPT_WRITEFUNCTION, &buffer_write_callback);
        curl_easy_setopt(easyHandle, CURLOPT_WRITEDATA, &context->buffer);
        curl_multi_add_handle(multiHandle_, easyHandle);
      }
    }
  }
}

} // namespace molecula
