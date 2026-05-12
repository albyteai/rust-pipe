// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "RustPipe",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "RustPipe", targets: ["RustPipe"]),
    ],
    targets: [
        .target(name: "RustPipe"),
    ]
)
