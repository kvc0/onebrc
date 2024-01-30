use std::{
    io::{BufRead, Read, Seek},
    ops::AddAssign,
    os::unix::fs::MetadataExt,
    thread::JoinHandle,
};

use ahash::{HashMap, HashMapExt};

fn main() {
    let start = std::time::Instant::now();
    run(Options {
        file_name: "measurements.txt".to_string(),
        workers: num_cpus::get(),
        // workers: 1,
        buffer_size: 128 * 2 << 20,
        unique_names_hint: 10000,
    });
    eprintln!("ran in {:#?}", start.elapsed());
}

#[derive(Clone)]
struct Options {
    file_name: String,
    workers: usize,
    buffer_size: usize,
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
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(&options.file_name)
        .expect("can open {file}");
    let mut worker_file_offset = worker_offset_step * worker_index;
    let worker_file_end = if worker_index + 1 == options.workers {
        metadata.size() as usize // make sure there isn't any funny business with the last line with lots of cores
    } else {
        worker_offset_step * (worker_index + 1)
    };
    file.seek(std::io::SeekFrom::Start(worker_file_offset as u64))
        .expect("initial seek");
    seek_to_line_start(&mut worker_file_offset, &mut file);
    eprintln!("worker {worker_index:02} offset {worker_file_offset}");

    let mut reader = std::io::BufReader::with_capacity(options.buffer_size, file);
    assert_eq!(
        worker_file_offset as u64,
        reader.stream_position().expect("it knows"),
        "stream is at the right place"
    );
    let mut line_buffer = Vec::with_capacity(256);

    let mut stations: ahash::HashMap<Vec<u8>, Aggregate> =
        ahash::HashMap::with_capacity(options.unique_names_hint);

    while worker_file_offset < worker_file_end {
        let len = reader
            .read_until(b';', &mut line_buffer)
            .expect("can read a station name");
        worker_file_offset += len;

        let station_name = &line_buffer[0..(len-1)];
        let station = match stations.get_mut(station_name) {
            Some(station) => station,
            None => {
                stations.insert(station_name.to_vec(), Aggregate::default());
                stations
                    .get_mut(station_name)
                    .expect("I just inserted this")
            }
        };
        // SAFETY: I don't need to zero the bytes
        unsafe {
            line_buffer.set_len(0);
        }

        let len = reader
            .read_until(b'\n', &mut line_buffer)
            .expect("can read a value");
        worker_file_offset += len;

        let mut value = &line_buffer[0..(len-1)];
        let negative = if value[0] == b'-' {
            value = &line_buffer[1..len];
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
        // SAFETY: I don't need to zero the bytes
        unsafe {
            line_buffer.set_len(0);
        }

        *station += parsed;
    }

    stations
}

fn seek_to_line_start(worker_file_offset: &mut usize, file: &mut std::fs::File) {
    // if reading from start, it's at a line start. No seek necessary
    if 0 < *worker_file_offset {
        file.seek(std::io::SeekFrom::Current(-1))
            .expect("seek prior byte");
        let mut previous_byte: [u8; 1] = [0];
        let read = file.read(&mut previous_byte).expect("read previous byte");
        assert_eq!(1, read, "must be able to read previous byte");
        // if we luck out, we're already at a line start. No seek necessary
        if previous_byte[0] != b'\n' {
            let mut seek_buffer: [u8; 1024] = [0; 1024];
            let read_bytes = file.read(&mut seek_buffer).expect("read end of line");
            assert!(512 < read_bytes, "must be able to read end of line");
            let mut i = 0;
            while i < read_bytes {
                if seek_buffer[i] == b'\n' {
                    break;
                }
                i += 1;
            }
            assert_ne!(i, read_bytes, "must be able to find end of line");
            *worker_file_offset += i + 1; // +1 to skip past \n
            file.seek(std::io::SeekFrom::Start(*worker_file_offset as u64))
                .expect("seek to next line");
        }
    }
}
