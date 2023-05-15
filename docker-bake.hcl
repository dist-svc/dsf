
group "default" {
    targets = [ "amd64", "arm64", "armv7" ]
    tags = [
        "ghcr.io/dist-svc/dsf:latest"
    ]
}

target "base" {
    dockerfile = "Dockerfile"
    output = ["type=docker"]
    platforms = ["linux/amd64", "linux/arm64", "linux/armv7"]
}

variable TARGET {
    default = "unloaded"
}

target "amd64" {
    inherits = [ "base" ]
    platforms = ["linux/amd64"]
    tags = [
        "ghcr.io/dist-svc/dsf:amd64"
    ]
    args = {
        TARGET = "x86_64-unknown-linux-gnu"
    }
}

target "arm64" {
    inherits = [ "base" ]
    platforms = ["linux/arm64"]
    tags = [
        "ghcr.io/dist-svc/dsf:arm64"
    ]
    args = {
        TARGET = "aarch64-unknown-linux-gnu"
    }
}

target "armv7" {
    inherits = [ "base" ]
    platforms = ["linux/arm/v7"]
    tags = [
        "ghcr.io/dist-svc/dsf:armv7"
    ]
    args = {
        TARGET = "aarch64-unknown-linux-gnu"
    }
}

function "resolve_target" {
    params = [arch]
    result = lookup({
            "linux/amd64": "x86_64-unknown-linux-gnu",
        },
        arch, "fail")
}
