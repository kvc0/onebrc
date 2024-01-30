use std::{
    fs::File,
    io::{BufRead, Read, Seek},
    ops::AddAssign,
    os::unix::fs::MetadataExt,
    thread::JoinHandle,
};

use ahash::{HashMap, HashMapExt};
use memmap::Mmap;

fn main() {
    let start = std::time::Instant::now();
    run(Options {
        file_name: "measurements.txt".to_string(),
        workers: num_cpus::get(),
        // workers: 1,
        unique_names_hint: 10000,
    });
    eprintln!("ran in {:#?}", start.elapsed());
}

#[derive(Clone)]
struct Options {
    file_name: String,
    workers: usize,
    unique_names_hint: usize,
}

struct Aggregate {
    min: f32,
    max: f32,
    sum: f64,
    count: usize,
}

impl Aggregate {
    fn absorb(&mut self, other: Aggregate) {
        self.min = min(self.min, other.min);
        self.max = max(self.max, other.max);
        self.sum += other.sum;
        self.count += other.count;
    }
}

impl Default for Aggregate {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            max: f32::MIN,
            sum: 0.0,
            count: 0,
        }
    }
}

impl AddAssign<f32> for Aggregate {
    fn add_assign(&mut self, rhs: f32) {
        self.min = min(self.min, rhs);
        self.max = max(self.max, rhs);
        self.sum += rhs as f64;
        self.count += 1;
    }
}

fn min(a: f32, b: f32) -> f32 {
    if a < b {
        a
    } else {
        b
    }
}

fn max(a: f32, b: f32) -> f32 {
    if a < b {
        b
    } else {
        a
    }
}

fn run(options: Options) {
    println!("looking for {}", options.file_name);
    let metadata = std::fs::metadata(&options.file_name).expect("can query {file}");
    // workers will complete any partial line in which they find themselves, and will skip any partial line they start at
    let worker_offset_step = metadata.size() as usize / options.workers;

    let mut handles = Vec::with_capacity(options.workers);

    for worker_index in 0..options.workers {
        let metadata = metadata.clone();
        let options = options.clone();
        let aggregates = std::thread::Builder::new()
            .name(format!("worker-{worker_index:02}"))
            .spawn(move || run_worker(&options, worker_offset_step, worker_index, metadata))
            .expect("can spawn a thread");
        handles.push(aggregates);
    }

    let mut aggregates = handles
        .into_iter()
        .map(JoinHandle::join)
        .map(|r| r.expect("join was successful"))
        .collect::<Vec<_>>();

    let mut stations = aggregates.pop().expect("there should be batches");
    for other in aggregates.into_iter() {
        for (station, aggregate) in other {
            match stations.get_mut(&station) {
                Some(existing) => {
                    existing.absorb(aggregate);
                }
                None => {
                    stations.insert(station, aggregate);
                }
            }
        }
    }

    let mut stations = stations.drain().collect::<Vec<_>>();
    stations.sort_unstable_by(|i, o| i.0.cmp(&o.0));
    print!("{{");
    let mut first = true;
    for (station, aggregate) in stations.drain(..) {
        if first {
            first = false;
        } else {
            print!(", ");
        }
        print!(
            "{name}={mi}/{av}/{ma}",
            name = unsafe { String::from_utf8_unchecked(station) },
            mi = aggregate.min,
            av = aggregate.sum / (aggregate.count as f64),
            ma = aggregate.max
        );
    }
    print!("}}");

    // let how_many = aggregates.iter().map(|m| m.iter().map(|(_, aggregate)| aggregate.count).sum::<usize>() ).sum::<usize>();
    // println!("done: {how_many}");
}

fn run_worker(
    options: &Options,
    worker_offset_step: usize,
    worker_index: usize,
    metadata: std::fs::Metadata,
) -> HashMap<Vec<u8>, Aggregate> {
    let fin = File::open(&options.file_name).expect("can open file");
    let file = unsafe { memmap::Mmap::map(&fin) }.expect("can map file");

    let mut worker_file_offset = worker_offset_step * worker_index;
    let worker_file_end = if worker_index + 1 == options.workers {
        metadata.size() as usize // make sure there isn't any funny business with the last line with lots of cores
    } else {
        worker_offset_step * (worker_index + 1)
    };
    seek_to_line_start(&mut worker_file_offset, &file);
    eprintln!("worker {worker_index:02} offset {worker_file_offset}");

    let mut stations: ahash::HashMap<Vec<u8>, Aggregate> =
        ahash::HashMap::with_capacity(options.unique_names_hint);

    while worker_file_offset < worker_file_end {
        let mut name_len = 1;
        while file[worker_file_offset + name_len] != b';' {
            name_len += 1;
        }

        let station_name = &file[worker_file_offset..(worker_file_offset + name_len)];
        let station = match stations.get_mut(station_name) {
            Some(station) => station,
            None => {
                stations.insert(station_name.to_vec(), Aggregate::default());
                stations
                    .get_mut(station_name)
                    .expect("I just inserted this")
            }
        };
        worker_file_offset += name_len + 1;

        let mut number_len = 1;
        while worker_file_offset + number_len < file.len()
            && file[worker_file_offset + number_len] != b'\n'
        {
            number_len += 1;
        }

        let mut value = &file[worker_file_offset..(worker_file_offset + number_len)];
        let negative = if value[0] == b'-' {
            value = &value[1..number_len];
            true
        } else {
            false
        };
        let mut fractional = false;
        let mut parsed = 0_f32;
        for byte in value {
            if *byte == b'.' {
                fractional = true;
                continue;
            }
            let v = (byte - b'0') as f32;
            if !fractional {
                parsed *= 10.0;
                parsed += v;
            } else {
                // there is only 1 fractional digit according to the rules
                parsed += v / 10.0;
                break;
            }
        }
        if negative {
            parsed = -parsed;
        }
        worker_file_offset += number_len + 1;

        *station += parsed;
    }

    stations
}

fn seek_to_line_start(worker_file_offset: &mut usize, file: &Mmap) {
    // if reading from start, it's at a line start. No seek necessary
    if 0 < *worker_file_offset {
        let mut previous_byte = file[*worker_file_offset - 1];
        *worker_file_offset -= 1;
        while *worker_file_offset < file.len() && file[*worker_file_offset] != b'\n' {
            *worker_file_offset += 1;
        }
        if *worker_file_offset < file.len() {
            *worker_file_offset += 1;
        }
    }
}
