BUILD_DIR := build
BUILD_TYPE := Debug

.PHONY: all configure build clean

all: build

configure:
	@mkdir -p $(BUILD_DIR)
	@cmake -S . -B build -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DCMAKE_POLICY_VERSION_MINIMUM=3.5

build: configure
	@cmake --build $(BUILD_DIR) -j8

test:
	@ctest --test-dir build --output-on-failure

build_only:
	@cmake --build $(BUILD_DIR) -j8

clean:
	@rm -rf $(BUILD_DIR)

format:
	@find molecula/ -name '*.cpp' -o -name '*.hpp' | xargs clang-format -i
