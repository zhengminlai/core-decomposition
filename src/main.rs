use itertools::Itertools;
use clap::{App, Arg};

use rust_graph::{UnStaticGraph, UnGraphMap};
use rust_graph::generic::graph::GraphTrait;

use rust_graph::io::read_from_csv;

use std::path::PathBuf;
use std::time::Instant;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cmp::min;

use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{ToStream, Concat, Feedback, ConnectLoop};
use std::collections::HashMap;
use rust_graph::graph_gen::random_gnm_graph_unlabeled;

/// Run this using random graph we generate: cargo run --release -- --num_nodes 50 --num_edges 250 -w 4
/// Run this using the edge file you provide: cargo run --release -- -e <your-edge-file> -w 4
/// where `-w` represents how many workers(threads) you want to use
fn main() {
    // You can read graph from csv file or using the random graph we generate
    let mut timely_args = Vec::new();
    let g = read_graph(&mut timely_args);

    let node_count = g.node_count();
    println!("node count:{}, edge count: {}\n", node_count, g.edge_count());
    let mut seq_core = Vec::with_capacity(node_count);
    let mut seq_cnt = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        seq_core.push(0);
        seq_cnt.push(0);
    }
    for i in g.node_indices() {
        seq_core[i as usize] = g.degree(i);
    }
    let seq_start = std::time::Instant::now();
    //println!("Sequential Initial core: {:?}", seq_core);
    semi_core(&g, &mut seq_core, &mut seq_cnt);
    //println!("Sequential Computed Final core: {:?}", seq_core);

    println!("*********Sequential version time: {:?} **********\n", seq_start.elapsed());

    let _arc_g = Arc::new(g);

    let mut core = Vec::<usize>::with_capacity(node_count);
    let mut cnt = Vec::<usize>::with_capacity(node_count);
    for _ in 0..node_count {
        core.push(0);
        cnt.push(0);
    }
    for i in _arc_g.node_indices() {
        core[i as usize] = _arc_g.degree(i);
    }

    let _arc_core = Arc::new(Mutex::new(core.clone()));
    let _arc_core_clone = _arc_core.clone();
    let _arc_cnt = Arc::new(Mutex::new(cnt.clone()));
    let _arc_cnt_clone = _arc_cnt.clone();

    //println!("Shared Memory Initial core: {:?}", _arc_core);

    let cmp_count = Arc::new(AtomicUsize::new(0));

    let start = std::time::Instant::now();
    timely::execute_from_args(timely_args.into_iter(), move |worker| {
        let cmp_node_count_clone = cmp_count.clone();

        let g = _arc_g.clone();
        let arc_core = _arc_core.clone();
        let arc_cnt = _arc_cnt.clone();

        let index = worker.index();

        let mut node_lists = HashMap::new();
        let mut batch_output_size = 10000;
        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>( |scope| {
            let node_stream = (0..1).to_stream(scope);
            let (handle, stream) = scope.feedback(1usize);
            node_stream.binary_notify(
                &stream,
                Exchange::new(|&x| x as u64),
                Exchange::new(|&x| x),
                "core_decomposition",
                vec![],
                move
                    |input1,
                      input2,
                      output,
                      notify| {


                   input1.for_each(|_, _| {
                        // notify.notify_at(time.retain());
                        //edge_list.push(data.replace(Vec::new()));
                   });

                    input2.for_each(|time, data| {
                        node_lists.entry(time.time().clone())
                            .or_insert_with(|| {
                                notify.notify_at(time.retain());
                                Vec::new()
                            })
                            .push(data.replace(Vec::new()));
                    });

                    notify.for_each(|time, _num, _notify| {
                        if let Some(mut todo) = node_lists.remove(&time) {
                            let mut session = output.session(&time);

                            for buffer in todo.drain(..) {
                                for node in buffer {
                                    let mut arc_cnt_mut = arc_cnt.lock().unwrap();
                                    let mut arc_core_mut = arc_core.lock().unwrap();
                                    if arc_cnt_mut[node as usize] < arc_core_mut[node as usize] {
                                        let v = cmp_node_count_clone.fetch_add(1, Ordering::SeqCst);
                                        if v >= batch_output_size - 1 {
                                            batch_output_size += 10000;
                                            println!("Current Worker {}, Shared Memory Total Computed Node Count:{:?}", index, v + 1);
                                        }
                                        let nbrs = &g.neighbors_iter(node as u32).collect_vec();
                                        let c_old = arc_core_mut[node as usize];
                                        arc_core_mut[node as usize] = local_core(c_old, nbrs, &arc_core_mut);
                                        arc_cnt_mut[node as usize] = compute_cnt(nbrs, &arc_core_mut, arc_core_mut[node as usize]);
                                        //println!("Worker {} Before update, cnt: {:?}, core: {:?}", index, arc_cnt_mut, arc_core_mut);
                                        update_cnt(nbrs, c_old, &arc_core_mut, arc_core_mut[node as usize], &mut arc_cnt_mut);
                                        //println!("Worker {} After update cnt: {:?}, core: {:?}", index, arc_cnt_mut, arc_core_mut);
                                    }
                                    for nbr in g.neighbors_iter(node as u32){
                                        if arc_cnt_mut[nbr as usize] < arc_core_mut[nbr as usize]{
                                            session.give(nbr as u64);
                                        }
                                    }
                                }
                            }
                        }
                    });
                    }).concat(&(0..node_count as u64).map(|x| (x)).to_stream(scope)).connect_loop(handle);
        });

        // finish computation, we check if the shared memory algorithm has the same output as the
        // sequential's
        if index == 0 {
            println!("****** Shared Memory Using Workers: {} *******", worker.peers());
        }
    }).unwrap();
    println!("******* Shared Memory Version Time: {:?} ********\n", start.elapsed());

    //println!("Shared Memory Computed Final core: {:?}", _arc_core_clone.lock().unwrap());
    for i in 0..node_count {
        assert_eq!(seq_core[i], _arc_core_clone.lock().unwrap()[i]);
        assert_eq!(seq_cnt[i], _arc_cnt_clone.lock().unwrap()[i]);
    }

}

// Sequential Algorithm
pub fn semi_core(g: &UnStaticGraph<String>, core: &mut [usize], cnt: &mut [usize]){
    let mut update = true;
    let mut iteration = 1;
    let mut computed_node_count = 0;
    while update {
        update = false;
        iteration += 1;
        for node in g.node_indices() {
            if cnt[node as usize] < core[node as usize] {
                update = true;
                computed_node_count += 1;
                //println!("Processing node: {}", node);
                let nbrs = &g.neighbors_iter(node).collect_vec();
                let c_old = core[node as usize];
                core[node as usize] = local_core(c_old, nbrs, core);
                cnt[node as usize] = compute_cnt(nbrs, core, core[node as usize]);
                //println!("Before update cnt: {:?}", cnt);
                update_cnt(nbrs, c_old, core, core[node as usize], cnt);
                //println!("After update cnt: {:?}", cnt);
            }
        }
    }
    println!("****** Sequentail Algorithm Total Node Computation Count: {}, Total Iteration: {} ******\n", computed_node_count, iteration);
}

pub fn compute_cnt(nbrs: &[u32], core: &[usize], core_v: usize) -> usize{
    let mut s = 0;
    for &node in nbrs{
        if core[node as usize] >= core_v {
            s += 1;
        }
    }
    s
}

pub fn update_cnt(nbrs: &[u32], c_old: usize, core: &[usize], core_v: usize, cnt: &mut [usize]){
    for &node in nbrs{
        if core_v < core[node as usize] && core[node as usize] <= c_old{
            //println!("Cnt[{}]: {}", node,cnt[node as usize]);
            if cnt[node as usize] > 0{
                cnt[node as usize] -= 1;
            }
        }
    }
}

pub fn local_core(c_old: usize, nbrs: &[u32], core: &[usize]) -> usize{
    let mut nums = Vec::with_capacity(c_old + 1);
    for _ in 0..(c_old + 1) {
        nums.push(0);
    }
    for &nbr in nbrs {
        let i = min(c_old, core[nbr as usize]);
        nums[i] = nums[i] + 1;
    }

    let mut s = 0;
    let mut k = c_old;
    while k != 0 {
        s = s + nums[k];
        if s >= k{
            break;
        }
        k -= 1;
    }
    k
}

pub fn read_graph(timely_args: &mut Vec<String>) -> UnStaticGraph<String> {
    let matches = App::new("CSV to StaticGraph Converter")
        .arg(
            Arg::with_name("edge_file")
                .short("e")
                .long("edge")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("separator")
                .short("s")
                .long("separator")
                .long_help("allowed separator: [comma|space|tab]")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("workers")
                .short("w")
                .long("workers")
                .required(true)
                .help("Number of workers for Timely")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("has_headers")
                .short("h")
                .long("headers")
                .multiple(true),
        )
        .arg(
            Arg::with_name("is_flexible")
                .short("f")
                .long("flexible")
                .multiple(true),
        )
        .arg(
            Arg::with_name("reorder_node_id")
                .short("i")
                .long("reorder_nodes")
                .multiple(true),
        )
        .arg(
            Arg::with_name("reorded_label_id")
                .short("l")
                .long("reorder_labels")
                .multiple(true),
        )
        .arg(
            Arg::with_name("node_count")
                .short("n_num")
                .long("num_nodes")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("edge_count")
                .short("e_num")
                .long("num_edges")
                .takes_value(true),
        )
        .get_matches();

    if matches.is_present("workers") {
        timely_args.push("-w".to_string());
        timely_args.push(matches.value_of("workers").unwrap().to_string());
    }

    if !matches.is_present("edge_file"){
        let n_count = matches.value_of("node_count").unwrap().parse::<usize>().unwrap();
        let e_count = matches.value_of("edge_count").unwrap().parse::<usize>().unwrap();
        return random_gnm_graph(n_count, e_count);
    }

    let edge_file = PathBuf::from(matches.value_of("edge_file").unwrap());

    let edge_path = if edge_file.is_dir() {
        let mut vec = ::std::fs::read_dir(edge_file)
            .unwrap()
            .map(|x| x.unwrap().path())
            .collect::<Vec<_>>();
        vec.sort();

        vec
    } else {
        vec![edge_file]
    };

    //let out_file = PathBuf::from(matches.value_of("out_file").unwrap_or("graph.static"));
    let separator = matches.value_of("separator");
    let has_headers = matches.is_present("has_headers");
    let is_flexible = matches.is_present("is_flexible");
    let reorder_node_id = matches.is_present("reorder_node_id");
    let reorder_label_id = matches.is_present("reorder_label_id");

    let start = Instant::now();

    let mut g = UnGraphMap::<String, String>::new();
    println!("Reading graph");
    read_from_csv(
        &mut g,
        Vec::new(),
        edge_path,
        separator,
        has_headers,
        is_flexible,
    );

    println!("Converting graph");
    let static_graph = g
        .reorder_id(reorder_node_id, reorder_label_id, reorder_label_id)
        .take_graph()
        .unwrap()
        .into_static();

    //static_graph.export(out_file).unwrap()

    let duration = start.elapsed();
    println!(
        "Finished in {} seconds.",
        duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9
    );

    static_graph
}

fn random_gnm_graph(num_of_nodes: usize, num_of_edges: usize) -> UnStaticGraph<String>{
    let g2: UnGraphMap<String> = random_gnm_graph_unlabeled(num_of_nodes, num_of_edges);
    g2.into_static()
}