extern crate tonic_build;
fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "./src/protos/consensus_transport.proto",
                "./src/protos/configure_transport.proto",
            ],
            &["./src/protos"],
        )
        .unwrap();
}
