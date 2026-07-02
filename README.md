# SwiftNIO QUIC

SwiftNIO QUIC provides bindings between [SwiftNIO][swift-nio] and a Swift
implementation of the QUIC network protocol from
[Swift Network Evolution][swift-network]. It makes use
of [Swift TLS][swift-tls] and [SwiftNIO QUIC Helpers][swift-nio-quic-helpers]
and integrates with [SwiftNIO HTTP/3][swift-nio-http3].

> [!IMPORTANT]
> This package is still in active development and does not offer a stable API
> yet.

## Quick Start

The following snippet contains a Swift Package manifest to use SwiftNIO QUIC:

```swift
// swift-tools-version: 6.3
import PackageDescription

let package = Package(
    name: "Application",
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.100.0"),
        .package(url: "https://github.com/apple/swift-nio-quic", .upToNextMinor(from: "0.1.0")),
    ],
    targets: [
        .executableTarget(
            name: "QUICServer",
            dependencies: [
                .product(name: "NIOQUIC", package: "swift-nio-quic"),
                .product(name: "NIOCore", package: "swift-nio"),
            ]
        )
    ]
)
```

## Getting Started

### Prerequisites

- [Swift 6.3 and up](https://swift.org/install)
- macOS 26.0 and up or Linux (Ubuntu 22.04+)
- Xcode 26.0 and up (Apple platforms only)

### Building and testing

SwiftNIO QUIC currently depends on a beta release of swift-crypto. Set the
environment variable `SWIFT_CERTIFICATES_ALLOW_SWIFT_CRYPTO_BETA` to allow
swift-certificates (in the dependency tree) to adopt swift-crypto beta
releases as well.

To build via the command line (for all platforms), run at the root of the
package:

```
SWIFT_CERTIFICATES_ALLOW_SWIFT_CRYPTO_BETA=1 swift build
```

To run all unit tests, run

```
SWIFT_CERTIFICATES_ALLOW_SWIFT_CRYPTO_BETA=1 swift test
```

Unit tests can also be run by filtering a specific class or function:

```
SWIFT_CERTIFICATES_ALLOW_SWIFT_CRYPTO_BETA=1 swift test --filter QUICConnectionIDTests
SWIFT_CERTIFICATES_ALLOW_SWIFT_CRYPTO_BETA=1 swift test --filter QUICConnectionIDTests.testZeroLengthConnectionID
```

Use `SWIFT_CERTIFICATES_ALLOW_SWIFT_CRYPTO_BETA=1 xed Package.swift` to open
the project in Xcode with the environment variable set.

### Versioning

While the library is in the 0.x.x version range, you should adopt it using
the `.upToNextMinor(from: "0.1.0")` specifier. During this period, breaking
changes are intended to map to minor version bumps, so depending on the
library this way picks up smaller, non-breaking changes automatically while
protecting against API-breaking ones.

[swift-nio]: https://github.com/apple/swift-nio
[swift-nio-quic-helpers]: https://github.com/apple/swift-nio-quic-helpers
[swift-nio-http3]: https://github.com/apple/swift-nio-http3
[swift-network]: https://github.com/apple/swift-network-evolution
[swift-tls]: https://github.com/apple/swift-tls