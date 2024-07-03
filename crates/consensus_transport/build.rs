extern crate tonic_build;
fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Deserialize, serde::Serialize)]")
        .compile_well_known_types(true)
        .build_server(true)
        .out_dir("./src/")
        .compile(
            &["./src/protos/consensus_transport.proto", "./src/protos/configure_transport.proto"],
            &["./src/protos"],
        )
        .unwrap();
}
