BUILD_DIR := build
BUILD_TYPE := Debug

.PHONY: all configure build clean

all: build

configure:
	@mkdir -p $(BUILD_DIR)
	@cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) ..

build: configure
	@cmake --build $(BUILD_DIR) -j

clean:
	@rm -r $(BUILD_DIR)
