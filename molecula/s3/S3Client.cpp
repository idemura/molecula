#include "molecula/s3/S3Client.hpp"

#include <glog/logging.h>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace molecula {

S3GetObjectInfo::S3GetObjectInfo(HttpResponse response) : status{response.status} {
  if (is2xx(status)) {
    etag = response.headers.get("etag");
    size = response.headers.getContentLength();

    auto lastModifiedStr = response.headers.get("last-modified");
    if (!lastModifiedStr.empty()) {
      std::tm tm{};
      std::istringstream ss{std::string{lastModifiedStr}};
      ss >> std::get_time(&tm, "%a, %d %b %Y %H:%M:%S GMT");
      lastModified = ::timegm(&tm);
    }
  } else {
    LOG(ERROR) << "Failed GetObjectInfo: " << status << "\n" << response.body.view();
  }
}

S3GetObject::S3GetObject(HttpResponse response) : status{response.status} {
  if (is2xx(status)) {
    data = std::move(response.body);
  } else {
    LOG(ERROR) << "Failed GetObject: " << status << "\n" << response.body.view();
  }
}

} // namespace molecula
