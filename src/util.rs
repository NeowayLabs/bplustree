use crate::{BPlusTree, Node, InternalNode, LeafNode, ParentHandler, Direction};
use crate::latch::HybridLatch;
use smallvec::smallvec;
use crossbeam_epoch::{self as epoch, Atomic};

use serde::Deserialize;
use std::sync::atomic::AtomicUsize;

type DefaultNode<K, V> = Node<K, V, 128, 256>;

#[derive(Deserialize, Debug)]
struct Edge {
    key: String,
    child: TreeNode
}

#[derive(Deserialize, Debug)]
struct Value {
    key: String,
    value: u64
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum TreeNode {
    Internal {
        edges: Vec<Edge>,
        upper_edge: Box<Option<TreeNode>>,
        lower_fence: Option<String>,
        upper_fence: Option<String>,
        sample_key: Option<String>
    },
    Leaf {
        values: Vec<Value>,
        lower_fence: Option<String>,
        upper_fence: Option<String>,
        sample_key: Option<String>
    }
}

#[derive(Deserialize, Debug)]
struct Tree {
    root: TreeNode,
    height: usize
}

fn translate_node(tree_node: TreeNode) -> Atomic<HybridLatch<DefaultNode<String, u64>>> {
    match tree_node {
        TreeNode::Internal { edges, upper_edge, lower_fence, upper_fence, sample_key } => {
            let mut out_keys = smallvec![];
            let mut out_edges = smallvec![];
            for edge in edges {
                out_keys.push(edge.key);
                out_edges.push(translate_node(edge.child));
            }

            let out_upper_edge = upper_edge.map(|tn| translate_node(tn));

            return Atomic::new(HybridLatch::new(Node::Internal(
                        InternalNode {
                            len: out_keys.len() as u16,
                            keys: out_keys,
                            edges: out_edges,
                            upper_edge: out_upper_edge,
                            lower_fence,
                            upper_fence,
                            sample_key
                        }
            )))
        }
        TreeNode::Leaf { values, lower_fence, upper_fence, sample_key } => {
            let mut out_keys = smallvec![];
            let mut out_values = smallvec![];
            for value in values {
                out_keys.push(value.key);
                out_values.push(value.value);
            }

            return Atomic::new(HybridLatch::new(Node::Leaf(
                        LeafNode {
                            len: out_keys.len() as u16,
                            keys: out_keys,
                            values: out_values,
                            lower_fence,
                            upper_fence,
                            sample_key
                        }
            )))
        }
    }
}

pub fn sample_tree<P: AsRef<std::path::Path>>(path: P) -> BPlusTree<String, u64> {
    use std::io::Read;
    let file = std::fs::File::open(path).expect("failed to find file");
    let tree: Tree = serde_json::from_reader(file).unwrap();
    // println!("{:?}", tree);
    let translated = translate_node(tree.root);
    BPlusTree {
        root: HybridLatch::new(translated),
        height: AtomicUsize::new(tree.height)
    }
}
