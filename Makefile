
RT?=gnu

all: x86 aarch64

x86: 
	cross build --target x86_64-unknown-linux-${RT} --release
	cd daemon && cargo deb --target x86_64-unknown-linux-${RT} --no-build
	cd client && cargo deb --target x86_64-unknown-linux-${RT} --no-build

aarch64:
	cargo build --target aarch64-unknown-linux-${RT} --release
	cd daemon && cargo deb --target aarch64-unknown-linux-${RT} --no-build
	cd client && cargo deb --target aarch64-unknown-linux-${RT} --no-build

armhf:
	cargo build --target armv7-unknown-linux-${RT}eabihf --release
	cd daemon && cargo deb --target armv7-unknown-linux-${RT}eabihf --no-build
	cd client && cargo deb --target armv7-unknown-linux-${RT}eabihf --no-build
