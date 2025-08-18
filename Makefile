BUILD_TYPE := Debug

.PHONY: all configure build build_all test clean format

all: build

configure:
	@mkdir -p build
	@cmake -S . -B build -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DCMAKE_POLICY_VERSION_MINIMUM=3.5

build: configure
	@cmake --build build -j8

build_only:
	@cmake --build build -j8

test: build_only
	@ctest --test-dir build --output-on-failure

clean:
	@rm -rf build

format:
	@find molecula/ -name '*.cpp' -o -name '*.hpp' | xargs clang-format -i
