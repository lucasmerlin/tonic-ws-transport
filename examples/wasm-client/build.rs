fn main() {
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["protos/helloworld.proto"], &["protos"])
        .unwrap();
}
