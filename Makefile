# Helper makefile for building DSF components and installers

# Runtime, `gnu` or `musl`, defaults to `gnu`
RT?=gnu

all: x86_64 aarch64 armhf

# linux-x86_64
x86_64:
	cross build --target x86_64-unknown-linux-${RT} --release
	cd daemon && cargo deb --target x86_64-unknown-linux-${RT} --no-build
	cd client && cargo deb --target x86_64-unknown-linux-${RT} --no-build
	docker buildx bake amd64 --load

# linux-aarch64
aarch64:
	cargo build --target aarch64-unknown-linux-${RT} --release
	cd daemon && cargo deb --target aarch64-unknown-linux-${RT} --no-build
	cd client && cargo deb --target aarch64-unknown-linux-${RT} --no-build
	docker buildx bake arm64 --load

# linux-armhf
armhf:
	cargo build --target armv7-unknown-linux-${RT}eabihf --release
	cd daemon && cargo deb --target armv7-unknown-linux-${RT}eabihf --no-build
	cd client && cargo deb --target armv7-unknown-linux-${RT}eabihf --no-build
	docker buildx bake armv7

# Build images (uses pre-compiled assets from above)
docker-build:
	docker buildx bake --load --load

# Push latest images to registry
docker-push:
	docker push ghcr.io/dist-svc/dsf:amd64
	docker push ghcr.io/dist-svc/dsf:arm64
	docker push ghcr.io/dist-svc/dsf:armv7

# Pull latest images from registry
docker-pull:
	docker pull ghcr.io/dist-svc/dsf:amd64
	docker pull ghcr.io/dist-svc/dsf:arm64
	docker pull ghcr.io/dist-svc/dsf:armv7

# Generate manifest for updates images 
# (requires that images are already pushed to registry)
docker-publish:
	-docker manifest rm ghcr.io/dist-svc/dsf:latest
	docker manifest create ghcr.io/dist-svc/dsf:latest \
		ghcr.io/dist-svc/dsf:amd64 \
		ghcr.io/dist-svc/dsf:arm64 \
		ghcr.io/dist-svc/dsf:armv7
	docker manifest push ghcr.io/dist-svc/dsf:latest

# Inspect newly created manifest
docker-inspect:
	docker manifest inspect ghcr.io/dist-svc/dsf:latest

