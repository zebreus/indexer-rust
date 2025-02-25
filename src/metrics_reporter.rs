use std::time::Duration;

use opentelemetry::{global, KeyValue};
use opentelemetry_semantic_conventions::{
    attribute::{
        NETWORK_INTERFACE_NAME, NETWORK_IO_DIRECTION, SYSTEM_CPU_LOGICAL_NUMBER, SYSTEM_DEVICE,
        SYSTEM_MEMORY_STATE,
    },
    metric::{
        SYSTEM_CPU_FREQUENCY, SYSTEM_CPU_LOGICAL_COUNT, SYSTEM_CPU_UTILIZATION,
        SYSTEM_LINUX_MEMORY_AVAILABLE, SYSTEM_MEMORY_LIMIT, SYSTEM_MEMORY_USAGE,
        SYSTEM_MEMORY_UTILIZATION, SYSTEM_NETWORK_ERRORS, SYSTEM_NETWORK_IO,
        SYSTEM_NETWORK_PACKETS,
    },
};
use sysinfo::{Networks, System};
use tokio::{
    task::{block_in_place, yield_now},
    time::{interval_at, Instant},
};

const METRICS_INTERVAL: Duration = Duration::from_secs(2);
pub async fn export_system_metrics() {
    let meter = global::meter("system");

    let mut system = System::new_all();
    let mut networks = Networks::new();
    tokio::task::block_in_place(|| {
        system.refresh_all();
        networks.refresh(true);
    });
    yield_now().await;

    let cpu_utilization_meter = meter
        .f64_gauge(SYSTEM_CPU_UTILIZATION)
        .with_description("Difference in system.cpu.time since the last measurement, divided by the elapsed time and number of logical CPUs")
        .with_unit("1")
        .build();
    let cpu_logical_count_meter = meter
    .i64_up_down_counter(SYSTEM_CPU_LOGICAL_COUNT)
    .with_description("Reports the number of logical (virtual) processor cores created by the operating system to manage multitasking")
    .with_unit("{cpu}")
    .build();
    let cpu_frequency_meter = meter
        .f64_gauge(SYSTEM_CPU_FREQUENCY)
        .with_description("Reports the current frequency of the CPU in Hz")
        .with_unit("{Hz}")
        .build();
    let memory_usage_meter = meter
        .i64_up_down_counter(SYSTEM_MEMORY_USAGE)
        .with_description("Reports memory in use by state")
        .with_unit("By")
        .build();
    let memory_limit_meter = meter
        .i64_up_down_counter(SYSTEM_MEMORY_LIMIT)
        .with_description("Total memory available in the system")
        .with_unit("By")
        .build();
    let memory_utilization_meter = meter
        .f64_gauge(SYSTEM_MEMORY_UTILIZATION)
        .with_unit("1")
        .build();
    let memory_available_meter = meter
        .i64_up_down_counter(SYSTEM_LINUX_MEMORY_AVAILABLE)
        .with_description("An estimate of how much memory is available for starting new applications, without causing swapping")
        .with_unit("By")
        .build();
    let network_packets_meter = meter
        .u64_counter(SYSTEM_NETWORK_PACKETS)
        .with_unit("{packet}")
        .build();
    let network_errors_meter = meter
        .u64_counter(SYSTEM_NETWORK_ERRORS)
        .with_unit("{error}")
        .build();
    let network_io_meter = meter.u64_counter(SYSTEM_NETWORK_IO).with_unit("By").build();

    let mut previous_cpu_logical_count = 0;
    let mut previous_free_memory = 0u64;
    let mut previous_total_memory = 0u64;
    let mut previous_used_memory = 0u64;
    let mut previous_availabe_memory = 0u64;

    let mut interval = interval_at(Instant::now(), METRICS_INTERVAL);
    loop {
        interval.tick().await;

        block_in_place(|| {
            system.refresh_cpu_all();
            system.refresh_memory();
            networks.refresh(true);
        });
        yield_now().await;
        block_in_place(|| {
            for (id, cpu) in system.cpus().iter().enumerate() {
                cpu_utilization_meter.record(
                    cpu.cpu_usage() as f64,
                    &[KeyValue::new(SYSTEM_CPU_LOGICAL_NUMBER, id as i64)],
                );
                cpu_frequency_meter.record(
                    cpu.frequency() as f64,
                    &[KeyValue::new(SYSTEM_CPU_LOGICAL_NUMBER, id as i64)],
                );
            }
            cpu_utilization_meter.record(system.global_cpu_usage() as f64, &[]);

            cpu_logical_count_meter
                .add(system.cpus().len() as i64 - previous_cpu_logical_count, &[]);
            previous_cpu_logical_count = system.cpus().len() as i64;

            memory_usage_meter.add(
                system.used_memory() as i64 - previous_used_memory as i64,
                &[KeyValue::new(SYSTEM_MEMORY_STATE, "used")],
            );
            memory_usage_meter.add(
                system.free_memory() as i64 - previous_free_memory as i64,
                &[KeyValue::new(SYSTEM_MEMORY_STATE, "free")],
            );
            memory_limit_meter.add(
                system.total_memory() as i64 - previous_total_memory as i64,
                &[KeyValue::new(SYSTEM_MEMORY_STATE, "total")],
            );
            memory_utilization_meter.record(
                system.used_memory() as f64 / system.total_memory() as f64,
                &[KeyValue::new(SYSTEM_MEMORY_STATE, "used")],
            );
            memory_utilization_meter.record(
                system.free_memory() as f64 / system.total_memory() as f64,
                &[KeyValue::new(SYSTEM_MEMORY_STATE, "free")],
            );
            memory_available_meter.add(
                system.available_memory() as i64 - previous_availabe_memory as i64,
                &[KeyValue::new(SYSTEM_MEMORY_STATE, "available")],
            );

            previous_free_memory = system.free_memory();
            previous_total_memory = system.total_memory();
            previous_used_memory = system.used_memory();
            previous_availabe_memory = system.available_memory();

            for (name, data) in networks.iter() {
                network_packets_meter.add(
                    data.packets_received(),
                    &[
                        KeyValue::new(SYSTEM_DEVICE, name.clone()),
                        KeyValue::new(NETWORK_IO_DIRECTION, "receive"),
                    ],
                );
                network_packets_meter.add(
                    data.packets_transmitted(),
                    &[
                        KeyValue::new(SYSTEM_DEVICE, name.clone()),
                        KeyValue::new(NETWORK_IO_DIRECTION, "transmit"),
                    ],
                );
                network_errors_meter.add(
                    data.errors_on_received(),
                    &[
                        KeyValue::new(NETWORK_INTERFACE_NAME, name.clone()),
                        KeyValue::new(NETWORK_IO_DIRECTION, "receive"),
                    ],
                );
                network_errors_meter.add(
                    data.errors_on_transmitted(),
                    &[
                        KeyValue::new(NETWORK_INTERFACE_NAME, name.clone()),
                        KeyValue::new(NETWORK_IO_DIRECTION, "transmit"),
                    ],
                );
                network_io_meter.add(
                    data.received(),
                    &[
                        KeyValue::new(NETWORK_INTERFACE_NAME, name.clone()),
                        KeyValue::new(NETWORK_IO_DIRECTION, "receive"),
                    ],
                );
                network_io_meter.add(
                    data.transmitted(),
                    &[
                        KeyValue::new(NETWORK_INTERFACE_NAME, name.clone()),
                        KeyValue::new(NETWORK_IO_DIRECTION, "transmit"),
                    ],
                );
            }
        });
    }
}
