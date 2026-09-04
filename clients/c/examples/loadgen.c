#define _POSIX_C_SOURCE 200809L

#include "talon.h"

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Keep the workload honest: these are production-sized Talon blocks. */
#define PRODUCTION_BLOCK_SIZE (UINT32_C(256) << 20)
#define DEFAULT_MAX_SAMPLES UINT64_C(5000000)

typedef struct sample {
    uint64_t latency_ns;
    uint32_t bucket;
    uint8_t success;
} sample;

typedef struct config {
    const char *coordinator;
    const char *uri;
    const char *scenario;
    uint64_t request_bytes;
    uint64_t concurrency;
    uint64_t warmup_seconds;
    uint64_t measure_seconds;
    uint64_t object_count;
    uint64_t bucket_ms;
    uint64_t max_samples;
    uint64_t round;
} config;

struct benchmark;

typedef struct slot {
    struct benchmark *benchmark;
    uint64_t rng;
    uint8_t *buffer;
    uint64_t offset;
    uint64_t submitted_ns;
    bool measured;
    bool finished;
} slot;

typedef struct benchmark {
    config config;
    talon_client *client;
    char *version;
    char **object_uris;
    uint64_t object_size;
    uint64_t measure_start_ns;
    uint64_t measure_end_ns;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    uint64_t unfinished_slots;
    char *first_error;
    sample *samples;
    size_t sample_capacity;
    atomic_uint_fast64_t next_sample;
    atomic_uint_fast64_t attempts;
    atomic_uint_fast64_t measured_attempts;
    atomic_uint_fast64_t successes;
    atomic_uint_fast64_t bytes;
    atomic_uint_fast64_t total_logical_errors;
    atomic_uint_fast64_t logical_errors;
    atomic_uint_fast64_t submission_errors;
    atomic_uint_fast64_t dropped_samples;
    atomic_uint_fast64_t active;
    atomic_uint_fast64_t peak_active;
} benchmark;

typedef struct stat_waiter {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool done;
    int status;
    uint64_t size;
    char *version;
    char *error;
} stat_waiter;

static uint64_t monotonic_ns(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        perror("clock_gettime");
        exit(2);
    }
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static uint64_t realtime_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        perror("clock_gettime");
        exit(2);
    }
    return (uint64_t)now.tv_sec * UINT64_C(1000) + (uint64_t)now.tv_nsec / UINT64_C(1000000);
}

static void sleep_until(uint64_t deadline_ns) {
    for (;;) {
        uint64_t now = monotonic_ns();
        if (now >= deadline_ns) {
            return;
        }
        uint64_t remaining = deadline_ns - now;
        struct timespec duration = {
            .tv_sec = (time_t)(remaining / UINT64_C(1000000000)),
            .tv_nsec = (long)(remaining % UINT64_C(1000000000)),
        };
        if (nanosleep(&duration, NULL) == 0) {
            return;
        }
        if (errno != EINTR) {
            perror("nanosleep");
            exit(2);
        }
    }
}

static char *copy_string(const char *value) {
    if (value == NULL) {
        return NULL;
    }
    size_t length = strlen(value) + 1;
    char *copy = malloc(length);
    if (copy != NULL) {
        memcpy(copy, value, length);
    }
    return copy;
}

static uint64_t parse_u64(const char *name, const char *value) {
    char *end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        exit(2);
    }
    return (uint64_t)parsed;
}

static uint64_t parse_size(const char *value) {
    char *end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value) {
        fprintf(stderr, "invalid size: %s\n", value);
        exit(2);
    }
    uint64_t multiplier = 1;
    if (*end != '\0') {
        if (end[1] != '\0') {
            fprintf(stderr, "invalid size suffix: %s\n", value);
            exit(2);
        }
        switch (*end) {
        case 'k':
        case 'K':
            multiplier = UINT64_C(1024);
            break;
        case 'm':
        case 'M':
            multiplier = UINT64_C(1024) * UINT64_C(1024);
            break;
        default:
            fprintf(stderr, "invalid size suffix: %s\n", value);
            exit(2);
        }
    }
    if ((uint64_t)parsed > UINT64_MAX / multiplier) {
        fprintf(stderr, "size overflows uint64: %s\n", value);
        exit(2);
    }
    return (uint64_t)parsed * multiplier;
}

static void usage(FILE *out, const char *program) {
    fprintf(out,
            "usage: %s --coordinator HOST:PORT --uri URI [options]\n"
            "  --request-bytes N[K|M]  bytes per logical read (default 4K)\n"
            "  --concurrency N         independent closed-loop reads (default 32)\n"
            "  --warmup-seconds N      warmup before sampling (default 3)\n"
            "  --seconds N             measured duration (default 10)\n"
            "  --object-count N        URI variants in the working set (default 1)\n"
            "  --bucket-ms N           emit per-bucket JSONL, 0 disables (default 0)\n"
            "  --max-samples N         latency sample cap (default 5000000)\n"
            "  --scenario NAME         result label (default hot)\n"
            "  --round N               repeat label (default 1)\n",
            program);
}

static config parse_args(int argc, char **argv) {
    config result = {
        .coordinator = NULL,
        .uri = NULL,
        .scenario = "hot",
        .request_bytes = UINT64_C(4) << 10,
        .concurrency = 32,
        .warmup_seconds = 3,
        .measure_seconds = 10,
        .object_count = 1,
        .bucket_ms = 0,
        .max_samples = DEFAULT_MAX_SAMPLES,
        .round = 1,
    };
    static const struct option options[] = {
        {"coordinator", required_argument, NULL, 'c'},
        {"uri", required_argument, NULL, 'u'},
        {"request-bytes", required_argument, NULL, 'b'},
        {"concurrency", required_argument, NULL, 'n'},
        {"warmup-seconds", required_argument, NULL, 'w'},
        {"seconds", required_argument, NULL, 's'},
        {"object-count", required_argument, NULL, 'o'},
        {"bucket-ms", required_argument, NULL, 'i'},
        {"max-samples", required_argument, NULL, 'm'},
        {"scenario", required_argument, NULL, 'x'},
        {"round", required_argument, NULL, 'r'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0},
    };
    int option;
    while ((option = getopt_long(argc, argv, "", options, NULL)) != -1) {
        switch (option) {
        case 'c': result.coordinator = optarg; break;
        case 'u': result.uri = optarg; break;
        case 'b': result.request_bytes = parse_size(optarg); break;
        case 'n': result.concurrency = parse_u64("concurrency", optarg); break;
        case 'w': result.warmup_seconds = parse_u64("warmup-seconds", optarg); break;
        case 's': result.measure_seconds = parse_u64("seconds", optarg); break;
        case 'o': result.object_count = parse_u64("object-count", optarg); break;
        case 'i': result.bucket_ms = parse_u64("bucket-ms", optarg); break;
        case 'm': result.max_samples = parse_u64("max-samples", optarg); break;
        case 'x': result.scenario = optarg; break;
        case 'r': result.round = parse_u64("round", optarg); break;
        case 'h': usage(stdout, argv[0]); exit(0);
        default: usage(stderr, argv[0]); exit(2);
        }
    }
    if (result.coordinator == NULL || result.uri == NULL || optind != argc) {
        usage(stderr, argv[0]);
        exit(2);
    }
    if (result.request_bytes == 0 || result.request_bytes >= PRODUCTION_BLOCK_SIZE ||
        result.concurrency == 0 || result.measure_seconds == 0 ||
        result.object_count == 0 || result.max_samples == 0 ||
        result.concurrency > SIZE_MAX || result.max_samples > SIZE_MAX) {
        fprintf(stderr,
                "request bytes must be in [1, 256MiB), concurrency/seconds/objects/samples "
                "must be non-zero\n");
        exit(2);
    }
    return result;
}

static void on_stat(talon_result *result, void *user_data) {
    stat_waiter *waiter = user_data;
    int status = talon_result_status(result);
    uint64_t size = talon_result_object_size(result);
    char *version = copy_string(talon_result_version(result));
    char *error = copy_string(talon_result_error(result));
    talon_result_free(result);

    pthread_mutex_lock(&waiter->mutex);
    waiter->status = status;
    waiter->size = size;
    waiter->version = version;
    waiter->error = error;
    waiter->done = true;
    pthread_cond_signal(&waiter->cond);
    pthread_mutex_unlock(&waiter->mutex);
}

static bool stat_once(talon_client *client, const char *uri, uint64_t *size, char **version) {
    stat_waiter waiter = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .done = false,
        .status = TALON_STATUS_RUNTIME_ERROR,
        .size = 0,
        .version = NULL,
        .error = NULL,
    };
    uint64_t request_id = 0;
    int status = talon_stat_async(client, uri, on_stat, &waiter, &request_id);
    if (status != TALON_STATUS_OK) {
        fprintf(stderr, "stat submission failed: %s\n", talon_last_error());
        pthread_cond_destroy(&waiter.cond);
        pthread_mutex_destroy(&waiter.mutex);
        return false;
    }
    pthread_mutex_lock(&waiter.mutex);
    while (!waiter.done) {
        pthread_cond_wait(&waiter.cond, &waiter.mutex);
    }
    pthread_mutex_unlock(&waiter.mutex);
    bool ok = waiter.status == TALON_STATUS_OK && waiter.version != NULL &&
              waiter.version[0] != '\0';
    if (!ok) {
        fprintf(stderr, "stat failed: %s\n", waiter.error != NULL ? waiter.error : "unknown");
        free(waiter.version);
    } else {
        *size = waiter.size;
        *version = waiter.version;
    }
    free(waiter.error);
    pthread_cond_destroy(&waiter.cond);
    pthread_mutex_destroy(&waiter.mutex);
    return ok;
}

static uint64_t xorshift64(uint64_t *state) {
    uint64_t value = *state;
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    *state = value;
    return value;
}

static char *object_uri(const char *base, uint64_t object_index, uint64_t object_count) {
    if (object_count == 1) {
        return copy_string(base);
    }
    int needed = snprintf(NULL, 0, "%s-%08" PRIu64, base, object_index);
    if (needed < 0) {
        return NULL;
    }
    char *result = malloc((size_t)needed + 1);
    if (result != NULL) {
        (void)snprintf(result, (size_t)needed + 1, "%s-%08" PRIu64, base, object_index);
    }
    return result;
}

static void free_object_uris(char **uris, size_t count) {
    if (uris == NULL) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        free(uris[i]);
    }
    free(uris);
}

static void remember_error(benchmark *bench, const char *error) {
    if (error == NULL) {
        error = "unknown error";
    }
    pthread_mutex_lock(&bench->mutex);
    if (bench->first_error == NULL) {
        bench->first_error = copy_string(error);
    }
    pthread_mutex_unlock(&bench->mutex);
}

static void finish_slot(slot *current) {
    benchmark *bench = current->benchmark;
    if (current->finished) {
        return;
    }
    current->finished = true;
    pthread_mutex_lock(&bench->mutex);
    if (--bench->unfinished_slots == 0) {
        pthread_cond_signal(&bench->cond);
    }
    pthread_mutex_unlock(&bench->mutex);
}

static void update_peak(atomic_uint_fast64_t *peak, uint64_t value) {
    uint64_t previous = atomic_load_explicit(peak, memory_order_relaxed);
    while (previous < value &&
           !atomic_compare_exchange_weak_explicit(peak, &previous, value,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
    }
}

static bool submit_next(slot *current);

static void on_read(talon_result *result, void *user_data) {
    slot *current = user_data;
    benchmark *bench = current->benchmark;
    uint64_t finished_ns = monotonic_ns();
    atomic_fetch_sub_explicit(&bench->active, 1, memory_order_relaxed);

    int status = talon_result_status(result);
    size_t bytes = talon_result_bytes_written(result);
    const char *error = talon_result_error(result);
    bool success = status == TALON_STATUS_OK && bytes == bench->config.request_bytes;
    if (success && bytes != 0) {
        uint8_t expected_first = (uint8_t)(current->offset % UINT64_C(251));
        uint8_t expected_last =
            (uint8_t)((current->offset + (uint64_t)bytes - 1) % UINT64_C(251));
        success = current->buffer[0] == expected_first &&
                  current->buffer[bytes - 1] == expected_last;
        if (!success) {
            error = "payload validation failed";
        }
    } else if (status == TALON_STATUS_OK) {
        error = "short logical read";
    }

    if (!success) {
        atomic_fetch_add_explicit(&bench->total_logical_errors, 1,
                                  memory_order_relaxed);
    }
    if (current->measured) {
        if (success) {
            atomic_fetch_add_explicit(&bench->successes, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&bench->bytes, bytes, memory_order_relaxed);
        } else {
            atomic_fetch_add_explicit(&bench->logical_errors, 1, memory_order_relaxed);
            remember_error(bench, error);
        }
        uint64_t sample_index =
            atomic_fetch_add_explicit(&bench->next_sample, 1, memory_order_relaxed);
        if (sample_index < bench->sample_capacity) {
            uint64_t relative_ns = current->submitted_ns - bench->measure_start_ns;
            uint64_t bucket_ns = bench->config.bucket_ms * UINT64_C(1000000);
            uint64_t bucket = bucket_ns == 0 ? 0 : relative_ns / bucket_ns;
            if (bucket > UINT32_MAX) {
                bucket = UINT32_MAX;
            }
            bench->samples[(size_t)sample_index] = (sample){
                .latency_ns = finished_ns - current->submitted_ns,
                .bucket = (uint32_t)bucket,
                .success = success ? 1 : 0,
            };
        } else {
            atomic_fetch_add_explicit(&bench->dropped_samples, 1, memory_order_relaxed);
        }
    }
    talon_result_free(result);

    if (finished_ns >= bench->measure_end_ns || !submit_next(current)) {
        finish_slot(current);
    }
}

static bool submit_next(slot *current) {
    benchmark *bench = current->benchmark;
    uint64_t now = monotonic_ns();
    if (now >= bench->measure_end_ns) {
        return false;
    }

    uint64_t object_index = xorshift64(&current->rng) % bench->config.object_count;
    const char *uri = bench->object_uris[object_index];

    uint64_t last_start = bench->object_size - bench->config.request_bytes;
    uint64_t offset = last_start == 0 ? 0 : xorshift64(&current->rng) % (last_start + 1);
    offset &= ~UINT64_C(4095);
    if (offset > last_start) {
        offset = last_start;
    }
    uint64_t end = offset + bench->config.request_bytes - 1;
    if (offset / PRODUCTION_BLOCK_SIZE != end / PRODUCTION_BLOCK_SIZE) {
        remember_error(bench, "generated request crossed a 256 MiB block boundary");
        atomic_fetch_add_explicit(&bench->submission_errors, 1, memory_order_relaxed);
        return false;
    }

    current->offset = offset;
    current->submitted_ns = now;
    current->measured = now >= bench->measure_start_ns;
    atomic_fetch_add_explicit(&bench->attempts, 1, memory_order_relaxed);
    if (current->measured) {
        atomic_fetch_add_explicit(&bench->measured_attempts, 1, memory_order_relaxed);
    }
    uint64_t active =
        atomic_fetch_add_explicit(&bench->active, 1, memory_order_relaxed) + 1;
    update_peak(&bench->peak_active, active);

    uint64_t request_id = 0;
    int status = talon_read_async(
        bench->client, uri, offset, current->buffer,
        (size_t)bench->config.request_bytes, bench->version, &bench->object_size,
        on_read, current, &request_id);
    if (status != TALON_STATUS_OK) {
        atomic_fetch_sub_explicit(&bench->active, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&bench->submission_errors, 1, memory_order_relaxed);
        remember_error(bench, talon_last_error());
        return false;
    }
    return true;
}

static int compare_u64(const void *left, const void *right) {
    uint64_t a = *(const uint64_t *)left;
    uint64_t b = *(const uint64_t *)right;
    return (a > b) - (a < b);
}

static double percentile_ms(const uint64_t *sorted, size_t count, double percentile) {
    if (count == 0) {
        return 0.0;
    }
    double rank = percentile * (double)(count - 1);
    size_t index = (size_t)(rank + 0.5);
    return (double)sorted[index] / 1000000.0;
}

static void print_json_string(const char *value) {
    if (value == NULL) {
        fputs("null", stdout);
        return;
    }
    putchar('"');
    for (const unsigned char *cursor = (const unsigned char *)value; *cursor != '\0'; ++cursor) {
        switch (*cursor) {
        case '"': fputs("\\\"", stdout); break;
        case '\\': fputs("\\\\", stdout); break;
        case '\n': fputs("\\n", stdout); break;
        case '\r': fputs("\\r", stdout); break;
        case '\t': fputs("\\t", stdout); break;
        default:
            if (*cursor < 0x20) {
                fprintf(stdout, "\\u%04x", *cursor);
            } else {
                putchar((int)*cursor);
            }
        }
    }
    putchar('"');
}

static size_t stored_sample_count(const benchmark *bench) {
    uint64_t produced = atomic_load_explicit(&bench->next_sample, memory_order_relaxed);
    return produced < bench->sample_capacity ? (size_t)produced : bench->sample_capacity;
}

static size_t collect_success_latencies(const benchmark *bench, uint64_t *latencies) {
    size_t count = 0;
    size_t samples = stored_sample_count(bench);
    for (size_t i = 0; i < samples; ++i) {
        if (bench->samples[i].success != 0) {
            latencies[count++] = bench->samples[i].latency_ns;
        }
    }
    return count;
}

static void emit_buckets(const benchmark *bench) {
    if (bench->config.bucket_ms == 0) {
        return;
    }
    uint64_t duration_ms = bench->config.measure_seconds * UINT64_C(1000);
    uint64_t bucket_count =
        (duration_ms + bench->config.bucket_ms - 1) / bench->config.bucket_ms;
    for (uint64_t bucket = 0; bucket < bucket_count; ++bucket) {
        uint64_t successes = 0;
        uint64_t errors = 0;
        size_t samples = stored_sample_count(bench);
        for (size_t i = 0; i < samples; ++i) {
            if (bench->samples[i].bucket != bucket) {
                continue;
            }
            if (bench->samples[i].success != 0) {
                ++successes;
            } else {
                ++errors;
            }
        }
        uint64_t begin_ms = bucket * bench->config.bucket_ms;
        uint64_t end_ms = begin_ms + bench->config.bucket_ms;
        if (end_ms > duration_ms) {
            end_ms = duration_ms;
        }
        double seconds = (double)(end_ms - begin_ms) / 1000.0;
        printf("{\"type\":\"bucket\",\"scenario\":");
        print_json_string(bench->config.scenario);
        printf(",\"round\":%" PRIu64 ",\"begin_ms\":%" PRIu64
               ",\"end_ms\":%" PRIu64 ",\"successes\":%" PRIu64
               ",\"logical_errors\":%" PRIu64 ",\"success_qps\":%.3f}\n",
               bench->config.round, begin_ms, end_ms, successes, errors,
               seconds == 0.0 ? 0.0 : (double)successes / seconds);
    }
}

static void emit_summary(const benchmark *bench) {
    size_t sampled_successes = 0;
    size_t samples = stored_sample_count(bench);
    for (size_t i = 0; i < samples; ++i) {
        sampled_successes += bench->samples[i].success != 0 ? 1U : 0U;
    }
    uint64_t *latencies = calloc(sampled_successes == 0 ? 1 : sampled_successes,
                                 sizeof(*latencies));
    if (latencies == NULL) {
        fprintf(stderr, "out of memory collecting latency samples\n");
        exit(2);
    }
    size_t latency_count = collect_success_latencies(bench, latencies);
    qsort(latencies, latency_count, sizeof(*latencies), compare_u64);

    uint64_t successes = atomic_load_explicit(&bench->successes, memory_order_relaxed);
    uint64_t attempts = atomic_load_explicit(&bench->measured_attempts, memory_order_relaxed);
    uint64_t total_attempts = atomic_load_explicit(&bench->attempts, memory_order_relaxed);
    uint64_t bytes = atomic_load_explicit(&bench->bytes, memory_order_relaxed);
    double seconds = (double)bench->config.measure_seconds;
    double qps = (double)successes / seconds;
    double attempt_qps = (double)attempts / seconds;
    double mibps = (double)bytes / (1024.0 * 1024.0) / seconds;

    printf("{\"type\":\"summary\",\"scenario\":");
    print_json_string(bench->config.scenario);
    printf(",\"round\":%" PRIu64 ",\"coordinator\":", bench->config.round);
    print_json_string(bench->config.coordinator);
    printf(",\"uri\":");
    print_json_string(bench->config.uri);
    printf(",\"version\":");
    print_json_string(bench->version);
    printf(",\"object_size\":%" PRIu64 ",\"object_count\":%" PRIu64
           ",\"block_size\":%u,\"request_bytes\":%" PRIu64
           ",\"requested_concurrency\":%" PRIu64
           ",\"actual_concurrency\":%" PRIuFAST64
           ",\"warmup_seconds\":%" PRIu64 ",\"seconds\":%" PRIu64
           ",\"successes\":%" PRIu64 ",\"attempts\":%" PRIu64
           ",\"total_attempts\":%" PRIu64
           ",\"success_qps\":%.3f,\"attempt_qps\":%.3f,\"mib_per_second\":%.3f"
           ",\"p50_ms\":%.3f,\"p95_ms\":%.3f,\"p99_ms\":%.3f"
           ",\"p999_ms\":%.3f,\"max_ms\":%.3f"
           ",\"submission_errors\":%" PRIuFAST64
           ",\"logical_errors\":%" PRIuFAST64
           ",\"total_logical_errors\":%" PRIuFAST64
           ",\"sample_count\":%zu,\"dropped_samples\":%" PRIuFAST64
           ",\"all_requests_below_block_size\":true"
           ",\"all_requests_single_block\":true,\"first_error\":",
           bench->object_size, bench->config.object_count, PRODUCTION_BLOCK_SIZE,
           bench->config.request_bytes, bench->config.concurrency,
           atomic_load_explicit(&bench->peak_active, memory_order_relaxed),
           bench->config.warmup_seconds, bench->config.measure_seconds, successes,
           attempts, total_attempts, qps, attempt_qps, mibps,
           percentile_ms(latencies, latency_count, 0.50),
           percentile_ms(latencies, latency_count, 0.95),
           percentile_ms(latencies, latency_count, 0.99),
           percentile_ms(latencies, latency_count, 0.999),
           percentile_ms(latencies, latency_count, 1.0),
           atomic_load_explicit(&bench->submission_errors, memory_order_relaxed),
           atomic_load_explicit(&bench->logical_errors, memory_order_relaxed),
           atomic_load_explicit(&bench->total_logical_errors, memory_order_relaxed),
           latency_count,
           atomic_load_explicit(&bench->dropped_samples, memory_order_relaxed));
    print_json_string(bench->first_error);
    puts("}");
    free(latencies);
}

int main(int argc, char **argv) {
    (void)setvbuf(stdout, NULL, _IOLBF, 0);
    config parsed = parse_args(argc, argv);
    talon_client_options options;
    talon_client_options_init(&options);
    options.block_size = PRODUCTION_BLOCK_SIZE;

    talon_client *client = NULL;
    int status = talon_client_new(parsed.coordinator, &options, &client);
    if (status != TALON_STATUS_OK) {
        fprintf(stderr, "client init failed: %s\n", talon_last_error());
        return 1;
    }

    uint64_t object_size = 0;
    char *version = NULL;
    if (!stat_once(client, parsed.uri, &object_size, &version)) {
        talon_client_free(client);
        return 1;
    }
    if (object_size != (UINT64_C(64) << 20)) {
        fprintf(stderr, "benchmark object must be exactly 64 MiB, got %" PRIu64 "\n",
                object_size);
        free(version);
        talon_client_free(client);
        return 2;
    }
    if (parsed.request_bytes > object_size) {
        fprintf(stderr, "request exceeds 64 MiB benchmark object\n");
        free(version);
        talon_client_free(client);
        return 2;
    }

    benchmark bench = {
        .config = parsed,
        .client = client,
        .version = version,
        .object_uris = NULL,
        .object_size = object_size,
        .measure_start_ns = 0,
        .measure_end_ns = 0,
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .unfinished_slots = parsed.concurrency,
        .first_error = NULL,
        .samples = NULL,
        .sample_capacity = (size_t)parsed.max_samples,
    };
    slot *slots = calloc((size_t)parsed.concurrency, sizeof(*slots));
    bench.samples = calloc(bench.sample_capacity, sizeof(*bench.samples));
    bench.object_uris = calloc((size_t)parsed.object_count, sizeof(*bench.object_uris));
    if (slots == NULL || bench.samples == NULL || bench.object_uris == NULL) {
        fprintf(stderr, "out of memory allocating slots, samples, or object URIs\n");
        free(slots);
        free(bench.samples);
        free_object_uris(bench.object_uris, (size_t)parsed.object_count);
        free(version);
        talon_client_free(client);
        return 2;
    }
    for (size_t i = 0; i < (size_t)parsed.object_count; ++i) {
        bench.object_uris[i] = object_uri(parsed.uri, i, parsed.object_count);
        if (bench.object_uris[i] == NULL) {
            fprintf(stderr, "out of memory allocating object URI %zu\n", i);
            free(slots);
            free(bench.samples);
            free_object_uris(bench.object_uris, (size_t)parsed.object_count);
            free(version);
            talon_client_free(client);
            return 2;
        }
    }
    for (size_t i = 0; i < (size_t)parsed.concurrency; ++i) {
        slots[i].benchmark = &bench;
        slots[i].rng = UINT64_C(0x9e3779b97f4a7c15) ^ ((uint64_t)i + 1);
        slots[i].buffer = malloc((size_t)parsed.request_bytes);
        if (slots[i].buffer == NULL) {
            fprintf(stderr, "out of memory allocating slot %zu\n", i);
            for (size_t j = 0; j <= i; ++j) {
                free(slots[j].buffer);
            }
            free(slots);
            free(bench.samples);
            free_object_uris(bench.object_uris, (size_t)parsed.object_count);
            free(version);
            talon_client_free(client);
            return 2;
        }
    }

    uint64_t start_ns = monotonic_ns();
    bench.measure_start_ns =
        start_ns + parsed.warmup_seconds * UINT64_C(1000000000);
    bench.measure_end_ns =
        bench.measure_start_ns + parsed.measure_seconds * UINT64_C(1000000000);
    for (size_t i = 0; i < (size_t)parsed.concurrency; ++i) {
        if (!submit_next(&slots[i])) {
            finish_slot(&slots[i]);
        }
    }
    sleep_until(bench.measure_start_ns);
    printf("{\"type\":\"measurement_start\",\"scenario\":");
    print_json_string(parsed.scenario);
    printf(",\"round\":%" PRIu64 ",\"unix_ms\":%" PRIu64 "}\n",
           parsed.round, realtime_ms());
    sleep_until(bench.measure_end_ns);
    pthread_mutex_lock(&bench.mutex);
    while (bench.unfinished_slots != 0) {
        pthread_cond_wait(&bench.cond, &bench.mutex);
    }
    pthread_mutex_unlock(&bench.mutex);

    emit_buckets(&bench);
    emit_summary(&bench);

    bool failed = atomic_load_explicit(&bench.submission_errors, memory_order_relaxed) != 0 ||
                  atomic_load_explicit(&bench.successes, memory_order_relaxed) == 0;
    talon_client_free(client);
    for (size_t i = 0; i < (size_t)parsed.concurrency; ++i) {
        free(slots[i].buffer);
    }
    free(slots);
    free(bench.samples);
    free_object_uris(bench.object_uris, (size_t)parsed.object_count);
    free(bench.first_error);
    free(version);
    pthread_cond_destroy(&bench.cond);
    pthread_mutex_destroy(&bench.mutex);
    return failed ? 1 : 0;
}
