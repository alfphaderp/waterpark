use hydroflow_plus::*;
use stageleft::*;

use rand::Rng;

/// Original spec: A leader broadcasts a value to be voted on, followers send in their votes, and
/// the leader decides "yes" if all votes are "yes" votes.
///
/// As it was unspecified what candidates to be voted on, how many followers, and how the followers
/// vote, I had 5 followers vote randomly on the first 10 natural numbers.
pub fn democracy<'a, D: Deploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
) {
    let leader = flow.process(process_spec);
    let followers = flow.cluster(cluster_spec);

    let candidates = leader.source_iter(q!(0..10));
    candidates
        // Leader sends candidates to followers
        .broadcast_bincode(&followers)
        // Followers decide how to vote
        .map(q!(|n| {
            let vote: bool = rand::thread_rng().gen_range(0..10) > 0;
            println!("Voting {} on {}", if vote { "yes" } else { "no" }, n);
            (n, vote)
        }))
        // Followers send votes back to leader
        .send_bincode_tagged(&leader)
        // Consider all responses when counting votes
        .all_ticks()
        // Decide for each candidate
        .fold(
            q!(|| [0; 10]), // stores -1 if anybody votes "no" and number of "yes" votes otherwise
            q!(|votes: &mut [i32; 10], (id, (n, vote))| {
                if vote {
                    if votes[n] >= 0 {
                        votes[n] += 1;
                    }
                    if votes[n] == 5 {
                        println!("Deciding yes on {}", n);
                    }
                } else {
                    if votes[n] != -1 {
                        votes[n] = -1;
                        println!("Deciding no on {} (received no from follower/{})", n, id);
                    }
                }
            }),
        );
}

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};

#[stageleft::entry]
pub fn democracy_runtime<'a>(
    flow: &'a FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    democracy(flow, &cli, &cli);
    flow.build(q!(cli.meta.subgraph_id))
}
