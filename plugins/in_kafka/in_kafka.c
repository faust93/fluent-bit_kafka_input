/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2021 The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_engine.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_parser.h>
#include <fluent-bit/flb_error.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_input_thread.h>
#include <msgpack.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "fluent-bit/flb_input.h"
#include "fluent-bit/flb_kafka.h"
#include "in_kafka.h"
#include "rdkafka.h"

static int try_json(struct flb_input_instance *in, msgpack_packer *mpck, rd_kafka_message_t *rkm)
{
    int root_type;
    char *buf = NULL;
    size_t bufsize;
    int ret;

    size_t off = 0;
    msgpack_unpacked result;
    msgpack_object entry;

    ret = flb_pack_json(rkm->payload, rkm->len, &buf, &bufsize, &root_type);
    if (ret) {
        if (buf) {
            flb_free(buf);
        }
        return ret;
    }

    flb_debug("[in-kafka] rcv payload size: %d\n", bufsize);

    msgpack_unpacked_init(&result);

    while (msgpack_unpack_next(&result, buf, bufsize, &off) == MSGPACK_UNPACK_SUCCESS) {
        entry = result.data;

        if (entry.type == MSGPACK_OBJECT_MAP) {
            msgpack_pack_object(mpck, entry);
        }
        else if (entry.type == MSGPACK_OBJECT_ARRAY) {
            msgpack_pack_map(mpck, 1);
            msgpack_pack_object(mpck, entry);
        }
        else {
            flb_error("[in-kafka] record is not a JSON map or array");
            msgpack_unpacked_destroy(&result);
            return -1;
        }
    }

    msgpack_unpacked_destroy(&result);
    flb_free(buf);
    return 0;
}

static int process_message(struct flb_input_instance *in, rd_kafka_message_t *rkm)
{
    msgpack_packer mp_pck;
    msgpack_sbuffer mp_sbuf;
    int ret;

    /* Initialize local msgpack buffer */
    msgpack_sbuffer_init(&mp_sbuf);
    msgpack_packer_init(&mp_pck, &mp_sbuf, msgpack_sbuffer_write);

    msgpack_pack_array(&mp_pck, 2);
    flb_pack_time_now(&mp_pck);

    msgpack_pack_map(&mp_pck, 6);

    msgpack_pack_str(&mp_pck, 5);
    msgpack_pack_str_body(&mp_pck, "topic", 5);
    if (rkm->rkt) {
        const char *rktopic = rd_kafka_topic_name(rkm->rkt);
        msgpack_pack_str(&mp_pck, strlen(rktopic));
        msgpack_pack_str_body(&mp_pck, rktopic, strlen(rktopic));
    } else {
        msgpack_pack_nil(&mp_pck);
    }

    msgpack_pack_str(&mp_pck, 9);
    msgpack_pack_str_body(&mp_pck, "partition", 9);
    msgpack_pack_int32(&mp_pck, rkm->partition);

    msgpack_pack_str(&mp_pck, 6);
    msgpack_pack_str_body(&mp_pck, "offset", 6);
    msgpack_pack_int64(&mp_pck, rkm->offset);

    msgpack_pack_str(&mp_pck, 5);
    msgpack_pack_str_body(&mp_pck, "error", 5);
    if (rkm->err) {
        const char *rkerr = rd_kafka_message_errstr(rkm);
        msgpack_pack_str(&mp_pck, strlen(rkerr));
        msgpack_pack_str_body(&mp_pck, rkerr, strlen(rkerr));
    } else {
        msgpack_pack_nil(&mp_pck);
    }

    msgpack_pack_str(&mp_pck, 3);
    msgpack_pack_str_body(&mp_pck, "key", 3);
    if (rkm->key) {
        msgpack_pack_str(&mp_pck, rkm->key_len);
        msgpack_pack_str_body(&mp_pck, rkm->key, rkm->key_len);
    } else {
        msgpack_pack_nil(&mp_pck);
    }

    msgpack_pack_str(&mp_pck, 7);
    msgpack_pack_str_body(&mp_pck, "payload", 7);
    if (rkm->payload) {
        if (try_json(in, &mp_pck, rkm)) {
            msgpack_pack_str(&mp_pck, rkm->len);
            msgpack_pack_str_body(&mp_pck, rkm->payload, rkm->len);
        }
    } else {
        msgpack_pack_nil(&mp_pck);
    }

    ret = flb_input_chunk_append_raw(in, NULL, 0, mp_sbuf.data,
                                         mp_sbuf.size);

    if (ret < 0)
        flb_warn("[in_kafka] flb_input_chunk_append_raw failed of size %u", mp_sbuf.size);

    msgpack_sbuffer_destroy(&mp_sbuf);

    return ret;
}

static int in_kafka_collect(struct flb_input_instance *in,
                            struct flb_config *config, void *in_context)
{
    struct flb_in_kafka_config *ctx = in_context;
    int i, msgn, wPoll, pTime;

    msgn = 0;
    wPoll = 1;
    pTime = ctx->batch_size + 10;

    while (wPoll) {
        pTime--;
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(ctx->kafka.rk, 50);
        if (!rkm)
            continue;

        if (rkm->err) {
            rd_kafka_message_destroy(rkm);
            continue;
        }

        ctx->rkm_batch[msgn] = rkm;
        msgn++;

        if(pTime <= 0 || msgn >= ctx->batch_size)
          wPoll = 0;
    }

    if(msgn) {
         for(i=0; i != msgn; i++) {
            rd_kafka_message_t *rkmm = ctx->rkm_batch[i];
            /* commit offsets only if chunk was appended successfully */
            if(!process_message(in, rkmm))
                rd_kafka_commit(ctx->kafka.rk, NULL, 0);
            rd_kafka_message_destroy(rkmm);
         }
    }

    return 0;
}

/* Initialize plugin */
static int in_kafka_init(struct flb_input_instance *ins,
                         struct flb_config *config, void *data)
{
    int ret;
    const char *conf;
    struct timespec tm;
    struct flb_in_kafka_config *ctx;

    rd_kafka_conf_t *kafka_conf = NULL;
    rd_kafka_topic_partition_list_t *kafka_topics = NULL;
    rd_kafka_resp_err_t err;
    char errstr[512];
    (void) data;

    /* Allocate space for the configuration context */
    ctx = flb_calloc(1, sizeof(struct flb_in_kafka_config));
    if (!ctx) {
        flb_plg_error(ins, "flb_calloc failed: %s", strerror(errno));
        return -1;
    }
    ctx->ins = ins;

    kafka_conf = flb_kafka_conf_create(&ctx->kafka, &ins->properties, 1);
    if (!kafka_conf) {
        flb_plg_error(ins, "Could not initialize kafka config object");
        goto init_error;
    }

    ctx->kafka.rk = rd_kafka_new(RD_KAFKA_CONSUMER, kafka_conf, errstr,
            sizeof(errstr));

    /* Create Kafka consumer handle */
    if (!ctx->kafka.rk) {
        flb_plg_error(ins, "Failed to create new consumer: %s", errstr);
        goto init_error;
    }

    conf = flb_input_get_property("topics", ins);
    if (!conf) {
        flb_plg_error(ins, "config: no topics specified");
        goto init_error;
    }

    kafka_topics = flb_kafka_parse_topics(conf);
    if (!kafka_topics) {
        flb_plg_error(ins, "Failed to parse topic list");
        goto init_error;
    }

    if ((err = rd_kafka_subscribe(ctx->kafka.rk, kafka_topics))) {
        flb_plg_error(ins, "Failed to start consuming topics: %s", rd_kafka_err2str(err));
        goto init_error;
    }
    rd_kafka_topic_partition_list_destroy(kafka_topics);
    kafka_topics = NULL;

    ctx->batch_size = BATCH_SIZE;
    ctx->rkm_batch = flb_malloc(sizeof(rd_kafka_message_t *) * ctx->batch_size);
    if (!ctx->rkm_batch) {
        flb_plg_error(ins, "flb_calloc failed: %s", strerror(errno));
        goto init_error;
    }

    /* interval settings */
    tm.tv_sec  = 1;
    tm.tv_nsec = 0;
//    tm.tv_nsec = 100000000;

    flb_input_set_context(ins, ctx);
    ret = flb_input_set_collector_time(ins,
                                       in_kafka_collect,
                                       tm.tv_sec,
                                       tm.tv_nsec, config);
    if (ret < 0) {
        flb_plg_error(ins, "Could not set collector for kafka input plugin");
        goto init_error;
    }

    ctx->c_id = ret;

    return 0;

init_error:
    if (kafka_topics) {
        rd_kafka_topic_partition_list_destroy(kafka_topics);
    }
    if (ctx->kafka.rk) {
        rd_kafka_destroy(ctx->kafka.rk);
    } else if (kafka_conf) {
        // conf is already destroyed when rd_kafka is initialized
        rd_kafka_conf_destroy(kafka_conf);
    }
    flb_free(ctx);

    return -1;
}

static void in_kafka_pause(void *data, struct flb_config *config)
{
    struct flb_in_kafka_config *ctx = data;
    flb_debug("[in_kafka] pausing endpoint %d", ctx->c_id);
    flb_input_collector_pause(ctx->c_id, ctx->ins);
}

static void in_kafka_resume(void *data, struct flb_config *config)
{
    struct flb_in_kafka_config *ctx = data;
    flb_debug("[in_kafka] resuming endpoint %d", ctx->c_id);
    flb_input_collector_resume(ctx->c_id, ctx->ins);
}

/* Cleanup serial input */
static int in_kafka_exit(void *in_context, struct flb_config *config)
{
    struct flb_in_kafka_config *ctx = in_context;

    flb_debug("[in_kafka] %d exiting..", ctx->c_id);

    rd_kafka_consumer_close(ctx->kafka.rk);
    rd_kafka_destroy(ctx->kafka.rk);
    flb_free(ctx->kafka.brokers);
    flb_free(ctx->rkm_batch);
    flb_free(ctx);

    return 0;
}

/* Plugin reference */
struct flb_input_plugin in_kafka_plugin = {
    .name         = "kafka",
    .description  = "Kafka consumer input plugin",
    .cb_init      = in_kafka_init,
    .cb_pre_run   = NULL,
    .cb_collect   = in_kafka_collect,
    .cb_flush_buf = NULL,
    .cb_pause     = in_kafka_pause,
    .cb_resume    = in_kafka_resume,
    .cb_exit      = in_kafka_exit
};

