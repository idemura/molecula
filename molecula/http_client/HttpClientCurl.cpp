#include "molecula/http_client/HttpClientCurl.hpp"

#include <glog/logging.h>

#include <curl/curl.h>
#include <unistd.h>
#include <type_traits>

namespace molecula {

class HttpContext {
public:
  explicit HttpContext(HttpRequest request) : request{std::move(request)} {}

  long id{};
  HttpRequest request;
  curl_slist* headers{};
  HttpResponse response;
  folly::Promise<HttpResponse> promise;
};

static_assert(std::is_move_constructible_v<HttpHeaders>);
static_assert(std::is_move_constructible_v<HttpRequest>);
static_assert(std::is_move_constructible_v<HttpResponse>);
static_assert(std::is_move_constructible_v<HttpContext>);

static size_t curlWriteCallback(char* buffer, size_t size, size_t nmemb, void* arg) {
  size_t total = size * nmemb;
  static_cast<HttpResponse*>(arg)->appendToBody(buffer, total);
  // Must return number of bytes taken
  return total;
}

static size_t curlHeaderCallback(char* buffer, size_t size, size_t nmemb, void* arg) {
  size_t total = size * nmemb;
  static_cast<HttpResponse*>(arg)->headers.add(std::string{buffer, total});
  // Must return number of bytes taken
  return total;
}

std::unique_ptr<HttpClient> createHttpClientCurl(const HttpClientConfig& config) {
  return HttpClientCurl::create(config);
}

std::mutex HttpClientCurl::globalMutex;
long HttpClientCurl::numClients{};

std::unique_ptr<HttpClient> HttpClientCurl::create(const HttpClientConfig& config) {
  {
    std::lock_guard<std::mutex> lock{globalMutex};
    if (numClients == 0) {
      curl_global_init(CURL_GLOBAL_DEFAULT);
    }
    ++numClients;
  }

  void* multiHandle = curl_multi_init();
  if (!multiHandle) {
    return nullptr;
  }
  return std::make_unique<HttpClientCurl>(multiHandle, config);
}

HttpClientCurl::HttpClientCurl(void* multiHandle, const HttpClientConfig& config) :
    multiHandle{multiHandle}, eventThread{&HttpClientCurl::eventLoop, this}, config{config} {
  ::pipe(pipe);
}

HttpClientCurl::~HttpClientCurl() {
  running.store(false, std::memory_order_release);
  ::write(pipe[1], "", 1);
  if (eventThread.joinable()) {
    eventThread.join();
  }
  curl_multi_cleanup(multiHandle);
  ::close(pipe[0]);
  ::close(pipe[1]);

  {
    std::lock_guard<std::mutex> lock{globalMutex};
    --numClients;
    if (numClients == 0) {
      curl_global_cleanup();
    }
  }
}

folly::Future<HttpResponse> HttpClientCurl::makeRequest(HttpRequest request) {
  auto* context = new HttpContext{std::move(request)};
  // Take future before adding to the queue. Context will be deleted in the event loop. Thus,
  // we need take future before adding to the queue (or under the lock).
  auto future = context->promise.getFuture();
  {
    std::lock_guard<std::mutex> lock{mutex};
    queue.emplace(context);
  }
  ::write(pipe[1], "", 1);
  return future;
}

void HttpClientCurl::eventLoop() {
  char buf[16]{};

  while (running.load(std::memory_order_acquire)) {
    int stillRunning = 0;
    while (curl_multi_perform(multiHandle, &stillRunning) == CURLM_CALL_MULTI_PERFORM) {
      // Keep performing until no more actions are pending
    }

    // Handle completed transfers
    CURLMsg* msg = nullptr;
    int msgsLeft = 0;
    while ((msg = curl_multi_info_read(multiHandle, &msgsLeft))) {
      if (msg->msg == CURLMSG_DONE) {
        void* easyHandle = msg->easy_handle;

        HttpContext* context = nullptr;
        curl_easy_getinfo(easyHandle, CURLINFO_PRIVATE, &context);
        // LOG(INFO) << "HTTP request #" << context->id << ": Done";

        long status = 0;
        curl_easy_getinfo(easyHandle, CURLINFO_RESPONSE_CODE, &status);
        context->response.status = status;

        // Destroy easy handle and clean up
        curl_multi_remove_handle(multiHandle, easyHandle);
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
    curl_waitfd curl_fd{pipe[0], CURL_WAIT_POLLIN};
    curl_multi_wait(multiHandle, &curl_fd, 1, 1000, &numfds);

    // Add new requests from queue
    HttpContext* context = nullptr;
    while ((context = takeNextFromQueue())) {
      // Every time we enqueue a new request, we write to the pipe. Read one byte back.
      ::read(pipe[0], buf, 1);

      curl_multi_add_handle(multiHandle, createEasyHandle(context));
    }
  }
}

HttpContext* HttpClientCurl::takeNextFromQueue() {
  std::unique_lock<std::mutex> lock(mutex);
  if (queue.empty()) {
    return nullptr;
  } else {
    auto* context = queue.front();
    queue.pop();
    return context;
  }
}

void* HttpClientCurl::createEasyHandle(HttpContext* context) {
  context->id = counter++;
  // LOG(INFO) << "HTTP request #" << context->id << ": Create";

  void* easyHandle = curl_easy_init();
  curl_easy_setopt(easyHandle, CURLOPT_URL, context->request.url.c_str());
  curl_easy_setopt(easyHandle, CURLOPT_PRIVATE, context);
  curl_easy_setopt(easyHandle, CURLOPT_WRITEFUNCTION, &curlWriteCallback);
  curl_easy_setopt(easyHandle, CURLOPT_WRITEDATA, &context->response);
  curl_easy_setopt(easyHandle, CURLOPT_HEADERFUNCTION, &curlHeaderCallback);
  curl_easy_setopt(easyHandle, CURLOPT_HEADERDATA, &context->response);
  switch (context->request.method) {
    case HttpMethod::GET:
      // curl_easy_setopt(easyHandle, CURLOPT_HTTPGET, 1L);
      break;
    case HttpMethod::HEAD:
      curl_easy_setopt(easyHandle, CURLOPT_NOBODY, 1L);
      break;
    case HttpMethod::POST:
      curl_easy_setopt(easyHandle, CURLOPT_POST, 1L);
      curl_easy_setopt(easyHandle, CURLOPT_POSTFIELDS, context->request.body.data());
      curl_easy_setopt(easyHandle, CURLOPT_POSTFIELDSIZE, context->request.body.size());
      break;
    case HttpMethod::PUT:
      curl_easy_setopt(easyHandle, CURLOPT_CUSTOMREQUEST, "PUT");
      curl_easy_setopt(easyHandle, CURLOPT_POSTFIELDS, context->request.body.data());
      curl_easy_setopt(easyHandle, CURLOPT_POSTFIELDSIZE, context->request.body.size());
      break;
    case HttpMethod::DELETE:
      curl_easy_setopt(easyHandle, CURLOPT_CUSTOMREQUEST, "DELETE");
      break;
  }

  // Add headers
  for (const std::string& header : context->request.headers.span()) {
    context->headers = curl_slist_append(context->headers, header.c_str());
  }
  curl_easy_setopt(easyHandle, CURLOPT_HTTPHEADER, context->headers);

  return easyHandle;
}

} // namespace molecula
