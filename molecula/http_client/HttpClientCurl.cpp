#include "molecula/http_client/HttpClientCurl.hpp"

#include <glog/logging.h>

#include <curl/curl.h>
#include <unistd.h>
#include <type_traits>

namespace molecula {

class HttpContext {
public:
  explicit HttpContext(HttpRequest request) : request{std::move(request)} {}

  long id = 0;
  HttpRequest request;
  curl_slist* headers{nullptr};
  HttpResponse response;
  folly::Promise<HttpResponse> promise;
};

static_assert(std::is_move_constructible_v<HttpRequest>);
static_assert(std::is_move_constructible_v<HttpResponse>);
static_assert(std::is_move_constructible_v<HttpContext>);

static std::once_flag curlInitOnce;

static size_t curlWriteCallback(char* buffer, size_t size, size_t nmemb, void* arg) {
  size_t total = size * nmemb;
  static_cast<HttpResponse*>(arg)->getBody().append(buffer, total);
  // Must return number of bytes taken
  return total;
}

static size_t curlHeaderCallback(char* buffer, size_t size, size_t nmemb, void* arg) {
  size_t total = size * nmemb;
  static_cast<HttpResponse*>(arg)->addHeader(std::string{buffer, total});
  // Must return number of bytes taken
  return total;
}

std::unique_ptr<HttpClient> createHttpClientCurl(const HttpClientConfig& config) {
  std::call_once(curlInitOnce, []() { curl_global_init(CURL_GLOBAL_DEFAULT); });

  void* multiHandle = curl_multi_init();
  if (!multiHandle) {
    return nullptr;
  }
  return std::make_unique<HttpClientCurl>(multiHandle, config);
}

HttpClientCurl::HttpClientCurl(void* multiHandle, const HttpClientConfig& config)
    : multiHandle_{multiHandle}, eventThread_{&HttpClientCurl::eventLoop, this}, config_{config} {
  ::pipe(pipe_);
}

HttpClientCurl::~HttpClientCurl() {
  running_.store(false, std::memory_order_release);
  ::write(pipe_[1], "", 1);
  if (eventThread_.joinable()) {
    eventThread_.join();
  }
  curl_multi_cleanup(multiHandle_);
  ::close(pipe_[0]);
  ::close(pipe_[1]);
}

folly::Future<HttpResponse> HttpClientCurl::makeRequest(HttpRequest request) {
  auto* context = new HttpContext{std::move(request)};
  // Take future before adding to the queue. Context will be deleted in the event loop. Thus,
  // we need take future before adding to the queue (or under the lock).
  auto future = context->promise.getFuture();
  {
    std::lock_guard<std::mutex> lock{mutex_};
    queue_.emplace(context);
  }
  ::write(pipe_[1], "", 1);
  return future;
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
        void* easyHandle = msg->easy_handle;

        HttpContext* context = nullptr;
        curl_easy_getinfo(easyHandle, CURLINFO_PRIVATE, &context);
        LOG(INFO) << "HTTP request #" << context->id << ": Done";

        long status = 0;
        curl_easy_getinfo(easyHandle, CURLINFO_RESPONSE_CODE, &status);
        context->response.setStatus(status);

        // Destroy easy handle and clean up
        curl_multi_remove_handle(multiHandle_, easyHandle);
        curl_easy_cleanup(easyHandle);
        if (context->headers) {
          curl_slist_free_all(context->headers);
          context->headers = nullptr;
        }

        context->promise.setValue(std::move(context->response));
        delete context;
      }
    }

    int numfds = 0;
    curl_waitfd curl_fd{pipe_[0], CURL_WAIT_POLLIN};
    curl_multi_wait(multiHandle_, &curl_fd, 1, 1000, &numfds);

    // Add new requests from queue
    HttpContext* context = nullptr;
    while ((context = takeNextFromQueue())) {
      // Every time we enqueue a new request, we write to the pipe. Read one byte back.
      ::read(pipe_[0], buf, 1);

      curl_multi_add_handle(multiHandle_, createEasyHandle(context));
    }
  }
}

HttpContext* HttpClientCurl::takeNextFromQueue() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (queue_.empty()) {
    return nullptr;
  } else {
    auto* context = queue_.front();
    queue_.pop();
    return context;
  }
}

void* HttpClientCurl::createEasyHandle(HttpContext* context) {
  context->id = counter_++;
  LOG(INFO) << "HTTP request #" << context->id << ": Create easy handle";

  void* easyHandle = curl_easy_init();
  curl_easy_setopt(easyHandle, CURLOPT_URL, context->request.getUrl().data());
  curl_easy_setopt(easyHandle, CURLOPT_PRIVATE, context);
  curl_easy_setopt(easyHandle, CURLOPT_WRITEFUNCTION, &curlWriteCallback);
  curl_easy_setopt(easyHandle, CURLOPT_WRITEDATA, &context->response);
  curl_easy_setopt(easyHandle, CURLOPT_HEADERFUNCTION, &curlHeaderCallback);
  curl_easy_setopt(easyHandle, CURLOPT_HEADERDATA, &context->response);
  switch (context->request.getMethod()) {
    case HttpMethod::GET:
      // curl_easy_setopt(easyHandle, CURLOPT_HTTPGET, 1L);
      break;
    case HttpMethod::HEAD:
      curl_easy_setopt(easyHandle, CURLOPT_CUSTOMREQUEST, "HEAD");
      break;
    case HttpMethod::POST:
      curl_easy_setopt(easyHandle, CURLOPT_POSTFIELDS, nullptr);
      curl_easy_setopt(easyHandle, CURLOPT_POSTFIELDSIZE, 0);
      break;
    case HttpMethod::PUT:
      curl_easy_setopt(easyHandle, CURLOPT_CUSTOMREQUEST, "PUT");
      break;
    case HttpMethod::DELETE:
      curl_easy_setopt(easyHandle, CURLOPT_CUSTOMREQUEST, "DELETE");
      break;
  }

  // Add headers
  for (const std::string& header : context->request.getHeaders()) {
    context->headers = curl_slist_append(context->headers, header.c_str());
  }
  curl_easy_setopt(easyHandle, CURLOPT_HTTPHEADER, context->headers);

  return easyHandle;
}

} // namespace molecula
