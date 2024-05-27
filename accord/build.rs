extern crate tonic_build;
fn main() {
    // tonic_build::compile_protos("src/protos/accord.proto")?;
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Deserialize, serde::Serialize)]")
        .compile_well_known_types(true)
        .build_server(true)
        .out_dir("./src/")
        .compile(
            &["./src/protos/accord.proto"],
           &["./src/protos"],
        )
        .unwrap();
}
