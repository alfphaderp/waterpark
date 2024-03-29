use std::cell::RefCell;

use hydro_deploy::{Deployment, HydroflowCrate};
use hydroflow_plus_cli_integration::{DeployClusterSpec, DeployProcessSpec};

#[tokio::main]
async fn main() {
    let deployment = RefCell::new(Deployment::new());
    let localhost = deployment.borrow_mut().Localhost();
    let profile = "dev";

    let builder = hydroflow_plus::FlowBuilder::new();
    flow::democracy::democracy(
        &builder,
        &DeployProcessSpec::new(|| {
            let mut deployment = deployment.borrow_mut();
            deployment.add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("democracy")
                    .profile(profile)
                    .display_name("leader"),
            )
        }),
        &DeployClusterSpec::new(|| {
            let mut deployment = deployment.borrow_mut();
            (0..5)
                .map(|idx| {
                    deployment.add_service(
                        HydroflowCrate::new(".", localhost.clone())
                            .bin("democracy")
                            .profile(profile)
                            .display_name(format!("follower/{}", idx)),
                    )
                })
                .collect()
        }),
    );

    let mut deployment = deployment.into_inner();

    deployment.deploy().await.unwrap();

    deployment.start().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap()
}
