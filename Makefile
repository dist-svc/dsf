# Helper makefile for building DSF components and installers

# Runtime, `gnu` or `musl`, defaults to `gnu`
RT?=gnu

all: x86_64 aarch64

# linux-x86_64
x86_64:
	cross build --target x86_64-unknown-linux-${RT} --release
	cd daemon && cargo deb --target x86_64-unknown-linux-${RT} --no-build
	cd client && cargo deb --target x86_64-unknown-linux-${RT} --no-build

# linux-aarch64
aarch64:
	cargo build --target aarch64-unknown-linux-${RT} --release
	cd daemon && cargo deb --target aarch64-unknown-linux-${RT} --no-build
	cd client && cargo deb --target aarch64-unknown-linux-${RT} --no-build

# linux-armhf
armhf:
	cargo build --target armv7-unknown-linux-${RT}eabihf --release
	cd daemon && cargo deb --target armv7-unknown-linux-${RT}eabihf --no-build
	cd client && cargo deb --target armv7-unknown-linux-${RT}eabihf --no-build

docker-build:
	docker buildx bake --load

docker-publish:
	docker manifest rm ghcr.io/dist-svc/dsf:latest
	docker manifest create ghcr.io/dist-svc/dsf:latest \
		ghcr.io/dist-svc/dsf:amd64 \
		ghcr.io/dist-svc/dsf:arm64 \
		ghcr.io/dist-svc/dsf:armv7
	docker manifest push ghcr.io/dist-svc/dsf:latest

docker-inspect:
	docker manifest inspect ghcr.io/dist-svc/dsf:latest

docker-push:
	docker push ghcr.io/dist-svc/dsf:amd64
	docker push ghcr.io/dist-svc/dsf:arm64
	docker push ghcr.io/dist-svc/dsf:armv7
