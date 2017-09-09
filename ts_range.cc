#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ts/ts.h>
#include <ts/remap.h>
#include <ts/ink_defs.h>

const char PLUGIN_NAME[] = "ts_range";

struct txndata {
    int64_t content_length;
    int64_t start;
    int64_t end;
};

typedef struct {
    TSVIO output_vio;
    TSIOBuffer output_buffer;
    TSIOBufferReader output_reader;

    int64_t content_length;
    int64_t range_length;
    int64_t start;
    int64_t end;
} MyData;

static MyData *
my_data_alloc()
{
    MyData *data;
    data = (MyData *)TSmalloc(sizeof(MyData));
    data->output_vio = NULL;
    data->output_buffer = NULL;
    data->output_reader = NULL;

    data->content_length = 0;
    data->range_length = 0;
    data->start = 0;
    data->end = 0;

    return data;
}

static void
my_data_destroy(MyData *data)
{
    if (data) {
        if(data->output_buffer) {
            TSIOBufferDestroy(data->output_buffer);
        }
        TSfree(data);
    }
}

static void
handle_transform(TSCont contp)
{
    TSVConn output_conn;
    TSIOBuffer input_buff;
    TSVIO input_vio;
    MyData *data;
    int64_t towrite;
    int64_t avail;
    int64_t donewrite;
    int64_t consume_size;

    consume_size = 0;

    TSDebug(PLUGIN_NAME, "Entering handle_transform()");

    output_conn = TSTransformOutputVConnGet(contp);

    input_vio = TSVConnWriteVIOGet(contp);

    data = (MyData *)TSContDataGet(contp);
    if (!data) {
        data = my_data_alloc();
        data->output_buffer = TSIOBufferCreate();
        data->output_reader = TSIOBufferReaderAlloc(data->output_buffer);
        //TSDebug(PLUGIN_NAME, "\tWriting %" PRId64 " bytes on VConn", TSVIONBytesGet(input_vio));

        data->output_vio = TSVConnWrite(output_conn, contp, data->output_reader, INT64_MAX);

        TSContDataSet(contp, data);
    }

    input_buff = TSVIOBufferGet(input_vio);

    if (!input_buff) {
        //TSVIONBytesSet(data->output_vio, TSVIONDoneGet(input_vio));
        //TSVIONBytesSet(data->output_vio, data->done_size);
        //TSVIOReenable(data->output_vio);
        return;
    }

    towrite = TSVIONTodoGet(input_vio);
    TSDebug(PLUGIN_NAME, "\ttowrite=%ld", towrite);
    //TSDebug(PLUGIN_NAME, "\ttoWrite is %" PRId64 "", towrite);

    if (towrite <= 0)
        goto LDone;

    donewrite = TSVIONDoneGet(input_vio);
    TSDebug(PLUGIN_NAME, "\tdonewrite=%ld", donewrite);
    avail = TSIOBufferReaderAvail(TSVIOReaderGet(input_vio));
    //TSDebug(PLUGIN_NAME, "\tavail is %" PRId64 "", avail);
    if (towrite > avail) {
        towrite = avail;
    }

    if (data->end > 0) {
        TSDebug(PLUGIN_NAME, "data->end > 0, end=%ld", data->end);
        if (donewrite <= data->start) {
            if (donewrite + towrite < data->start) {

            } else if (donewrite + towrite >= data->start && donewrite + towrite <= data->end) {
                // 4 = 10 + 5 - 11
                consume_size = donewrite + towrite - data->start;
                // 4, 5-4
                TSIOBufferCopy(TSVIOBufferGet(data->output_vio), TSVIOReaderGet(input_vio), consume_size,
                               towrite - consume_size);

            } else {
                // 4 = 10 + 5 - 11
                consume_size = data->end - data->start + 1;
                //  10+20 11-15
                TSIOBufferCopy(TSVIOBufferGet(data->output_vio), TSVIOReaderGet(input_vio), consume_size,
                               data->start - donewrite);
            }
        } else if (donewrite > data->start && donewrite <= data->end) {
            if (donewrite + towrite <= data->end) {
                consume_size = towrite;
                TSIOBufferCopy(TSVIOBufferGet(data->output_vio), TSVIOReaderGet(input_vio), towrite, 0);
            } else {
                // 4 = 10+5 - 11
                consume_size = data->end - donewrite + 1;
                TSIOBufferCopy(TSVIOBufferGet(data->output_vio), TSVIOReaderGet(input_vio), consume_size, 0);
            }
        }
    } else {
        if (donewrite <= data->start) {
            if (donewrite + towrite < data->start) {

            } else {
                // 4 = 10 + 5 - 11
                consume_size = donewrite + towrite - data->start;
                // 4, 5-4
                TSIOBufferCopy(TSVIOBufferGet(data->output_vio), TSVIOReaderGet(input_vio), consume_size,
                               towrite - consume_size);
            }
        } else {
            consume_size = towrite;
            TSIOBufferCopy(TSVIOBufferGet(data->output_vio), TSVIOReaderGet(input_vio), towrite, 0);
        }
    }

    TSDebug(PLUGIN_NAME, "consume_size=%ld", consume_size);
    if (consume_size) {
        //TSVIONBytesSet(data->output_vio, TSVIONDoneGet(data->output_vio) + consume_size);
        TSVIONDoneSet(data->output_vio, TSVIONDoneGet(data->output_vio) + consume_size);
        TSDebug(PLUGIN_NAME, "output_vio Done Get=%ld", TSVIONDoneGet(data->output_vio));
    }

    TSIOBufferReaderConsume(TSVIOReaderGet(input_vio), towrite);

    TSVIONDoneSet(input_vio, TSVIONDoneGet(input_vio) + towrite);

//    towrite = TSVIONTodoGet(input_vio);
//    avail = TSIOBufferReaderAvail(TSVIOReaderGet(input_vio));
//    if (towrite <= 0 || avail <= 0)
//        goto LDone;
//
//    donewrite = TSVIONDoneGet(input_vio);
//
//    if (donewrite > end) {
//        TSIOBufferReaderConsume(TSVIOReaderGet(input_vio), towrite);
//
//        TSVIONDoneSet(input_vio, TSVIONDoneGet(input_vio) + towrite);
//    }


    LDone:
    //if (TSVIONTodoGet(input_vio) > 0) {
    TSDebug(PLUGIN_NAME, "LDone todo=%ld, Done Get=%ld, range_length=%ld",TSVIONTodoGet(input_vio),
            TSVIONDoneGet(data->output_vio), data->range_length);
    if (TSVIONTodoGet(input_vio) > 0 && TSVIONDoneGet(data->output_vio) < data->range_length) {
        if (towrite > 0) {

            if (consume_size)
                TSVIOReenable(data->output_vio);

            TSContCall(TSVIOContGet(input_vio), TS_EVENT_VCONN_WRITE_READY, input_vio);
        }

    } else {
        //TSVIONBytesSet(data->output_vio, TSVIONDoneGet(input_vio));
        //TSVIONBytesSet(data->output_vio, data->done_size);
        TSVIONBytesSet(data->output_vio, TSVIONDoneGet(data->output_vio));
        TSDebug(PLUGIN_NAME, "TS_EVENT_VCONN_WRITE_COMPLETE Done Get=%ld, range_length=%ld",
                TSVIONDoneGet(data->output_vio), data->range_length);
        //if (consume_size)
        TSVIOReenable(data->output_vio);

        TSContCall(TSVIOContGet(input_vio), TS_EVENT_VCONN_WRITE_COMPLETE, input_vio);
    }

}

static int
range_transform(TSCont contp, TSEvent event, void *edata ATS_UNUSED)
{
    TSDebug(PLUGIN_NAME, "Entering range_transform()");

    if(TSVConnClosedGet(contp)) {
        TSDebug(PLUGIN_NAME, "\tVConn is closed");
        my_data_destroy((MyData *)TSContDataGet(contp));
        TSContDestroy(contp);
        return 0;
    } else {
        switch (event) {
            case TS_EVENT_ERROR: {
                TSVIO input_vio;

                TSDebug(PLUGIN_NAME, "\tEvent is TS_EVENT_ERROR");

                input_vio = TSVConnWriteVIOGet(contp);

                TSContCall(TSVIOContGet(input_vio), TS_EVENT_ERROR, input_vio);
            } break;
            case TS_EVENT_VCONN_WRITE_COMPLETE:
                TSDebug(PLUGIN_NAME, "\tEvent is TS_EVENT_VCONN_WRITE_COMPLETE");

                TSVConnShutdown(TSTransformOutputVConnGet(contp), 0, 1);
                break;
            case TS_EVENT_VCONN_WRITE_READY:
                TSDebug(PLUGIN_NAME, "\tEvent is TS_EVENT_VCONN_WRITE_READY");
            default:
                TSDebug(PLUGIN_NAME, "\tEvent is %d", event);
                handle_transform(contp);
                break;

        }
    }

    return 0;
}

static void
transform_add(TSHttpTxn txnp, struct txndata *txn_state)
{
    TSVConn connp;
    TSVConn output_conn;
    if (!txn_state)
        return;

    if (txn_state->content_length <= 0 || txn_state->start >= (txn_state->content_length -1))
        return;

    if (txn_state->end >= (txn_state->content_length -1))
        txn_state->end = 0;


    TSDebug(PLUGIN_NAME, "Entering transform_add()");
    connp = TSTransformCreate(range_transform, txnp);

    MyData *data;
    data = my_data_alloc();
    data->output_buffer = TSIOBufferCreate();
    data->output_reader = TSIOBufferReaderAlloc(data->output_buffer);
    output_conn = TSTransformOutputVConnGet(connp);
    data->output_vio = TSVConnWrite(output_conn, connp, data->output_reader, INT64_MAX);
    data->start = txn_state->start;
    data->end = txn_state->end;
    data->content_length = txn_state->content_length;

    TSDebug(PLUGIN_NAME, "transform_add start=%ld, end=%ld, content_length=%ld", data->start, data->end, data->content_length);
    if (data->end == 0) {
        data->range_length = data->content_length - data->start;
    } else {
        data->range_length = data->end - data->start + 1;
    }
    TSDebug(PLUGIN_NAME, "transform_add range_length=%ld", data->range_length);


    TSContDataSet(connp, data);

    TSHttpTxnHookAdd(txnp, TS_HTTP_RESPONSE_TRANSFORM_HOOK, connp);
}

static int
transformable(TSHttpTxn txnp, struct txndata *txn_state)
{
    /*
     *  We are only interested in transforming "200 OK" responses.
     */

    TSMBuffer bufp;
    TSMLoc hdr_loc;
    TSMLoc cl_field;
    TSHttpStatus resp_status;
    int retv = 0;
    int64_t n;

    TSDebug(PLUGIN_NAME, "Entering transformable()");

    if (!txn_state)
        return 0;

    if (TS_SUCCESS == TSHttpTxnServerRespGet(txnp, &bufp, &hdr_loc)) {
        resp_status = TSHttpHdrStatusGet(bufp, hdr_loc);
        if(resp_status == TS_HTTP_STATUS_OK) {
            n = 0;
            cl_field = TSMimeHdrFieldFind(bufp, hdr_loc, TS_MIME_FIELD_CONTENT_LENGTH, TS_MIME_LEN_CONTENT_LENGTH);
            if (cl_field) {
                n = TSMimeHdrFieldValueInt64Get(bufp, hdr_loc, cl_field, -1);
                TSHandleMLocRelease(bufp, hdr_loc, cl_field);
            }
            txn_state->content_length = n;
            if (n > 0) {
                retv = 1;
            }
        }

        TSHandleMLocRelease(bufp, TS_NULL_MLOC, hdr_loc);
    }

    TSDebug(PLUGIN_NAME, "Exiting transformable with return %d", retv);
    return retv;
}

static int
cache_lookup_handle(TSHttpTxn txnp, struct txndata *txn_state)
{
    TSMBuffer bufp;
    TSMLoc hdrp;
    TSMLoc cl_field;
    TSHttpStatus code;
    int obj_status;
    int64_t n;

    if (!txn_state)
        return 0;

    if (TSHttpTxnCacheLookupStatusGet(txnp, &obj_status) == TS_ERROR) {
        TSError("[%s] %s Couldn't get cache status of object", PLUGIN_NAME, __FUNCTION__);
        return 0;
    }
    TSDebug(PLUGIN_NAME, " %s object status %d", __FUNCTION__, obj_status);
    if (obj_status != TS_CACHE_LOOKUP_HIT_STALE && obj_status != TS_CACHE_LOOKUP_HIT_FRESH)
        return 0;

    if (TSHttpTxnCachedRespGet(txnp, &bufp, &hdrp) != TS_SUCCESS) {
        TSError("[%s] %s Couldn't get cache resp", PLUGIN_NAME, __FUNCTION__);
        return 0;
    }

    n = 0;

    code = TSHttpHdrStatusGet(bufp, hdrp);
    if (code != TS_HTTP_STATUS_OK) {
        goto release;
    }


    cl_field = TSMimeHdrFieldFind(bufp, hdrp, TS_MIME_FIELD_CONTENT_LENGTH, TS_MIME_LEN_CONTENT_LENGTH);
    if (cl_field) {
        n = TSMimeHdrFieldValueInt64Get(bufp, hdrp, cl_field, -1);
        TSHandleMLocRelease(bufp, hdrp, cl_field);
    }

    release:
    TSHandleMLocRelease(bufp, TS_NULL_MLOC, hdrp);

    txn_state->content_length = n;

    if(n <=0) {
        TSError("[%s] %s Not include content_length", PLUGIN_NAME, __FUNCTION__);
        return 0;
    }
    return 1;
}

static int
transform_plugin(TSCont contp ATS_UNUSED, TSEvent event, void *edata)
{
    TSHttpTxn txnp = (TSHttpTxn)edata;
    struct txndata *txn_state = (struct txndata *)TSContDataGet(contp);

    TSDebug(PLUGIN_NAME, "Entering transform_plugin()");
    switch (event) {
        case TS_EVENT_HTTP_CACHE_LOOKUP_COMPLETE:
            if (cache_lookup_handle(txnp, txn_state)){
                transform_add(txnp, txn_state);
            }
            break;
        case TS_EVENT_HTTP_READ_RESPONSE_HDR:
            TSDebug(PLUGIN_NAME, "\tEvent is TS_EVENT_HTTP_READ_RESPONSE_HDR");
            if (transformable(txnp, txn_state)) {
                transform_add(txnp, txn_state);
            }
            break;
        case TS_EVENT_HTTP_TXN_CLOSE:
            TSDebug(PLUGIN_NAME, "TS_EVENT_HTTP_TXN_CLOSE");
            if (txn_state != NULL) {
                TSfree(txn_state);
            }
            TSContDestroy(contp);
            break;
        default:
            break;
    }
    TSHttpTxnReenable(txnp, TS_EVENT_HTTP_CONTINUE);
    return 0;
}

/**
 * Remap initialization.
 */
TSReturnCode
TSRemapInit(TSRemapInterface *api_info, char *errbuf, int errbuf_size)
{
    if (!api_info) {
        strncpy(errbuf, "[tsremap_init] - Invalid TSRemapInterface argument", errbuf_size - 1);
        return TS_ERROR;
    }

    if (api_info->tsremap_version < TSREMAP_VERSION) {
        snprintf(errbuf, errbuf_size - 1, "[TSRemapInit] - Incorrect API version %ld.%ld", api_info->tsremap_version >> 16,
                 (api_info->tsremap_version & 0xffff));
        return TS_ERROR;
    }

    TSDebug(PLUGIN_NAME, "cache_range_requests remap is successfully initialized.");
    return TS_SUCCESS;
}


/**
 * not used.
 */
TSReturnCode
TSRemapNewInstance(int argc, char *argv[], void **ih, char * /*errbuf */, int /* errbuf_size */)
{
    return TS_SUCCESS;
}

/**
 * not used.
 */
void
TSRemapDeleteInstance(void *ih)
{
    return;
}

/**
 * Remap entry point.
 */
TSRemapStatus
TSRemapDoRemap(void *ih, TSHttpTxn txnp, TSRemapRequestInfo *rri)
{

    const char *method, *query;
    int method_len, query_len;
    const char *f_start, *f_end;
    TSCont txn_contp;
    TSMLoc ae_field, range_field;
    struct txndata *txn_state;
    int64_t start,end;

    method = TSHttpHdrMethodGet(rri->requestBufp, rri->requestHdrp, &method_len);
    if (method != TS_HTTP_METHOD_GET) {
        return TSREMAP_NO_REMAP;
    }

    start = 0;
    end = 0;
    query = TSUrlHttpQueryGet(rri->requestBufp, rri->requestUrl, &query_len);
    if(!query) {
        return TSREMAP_NO_REMAP;
    }

    f_start = strcasestr(query, "start=");
    if (!f_start) {
        return TSREMAP_NO_REMAP;
    }
    start = strtol(f_start + 1, NULL, 10);

    f_end = strcasestr(query, "&end=");

    if(f_end != NULL) {
        end = strtol(f_end + 1, NULL, 10);
    }


    if (start < 0 || end < 0 || start >= end) {
        return TSREMAP_NO_REMAP;
    }

    txn_state = (struct txndata *)TSmalloc(sizeof(struct txndata));

    // remove Range
    range_field = TSMimeHdrFieldFind(rri->requestBufp, rri->requestHdrp, TS_MIME_FIELD_RANGE, TS_MIME_LEN_RANGE);
    if (range_field) {
        TSMimeHdrFieldDestroy(rri->requestBufp, rri->requestHdrp, range_field);
        TSHandleMLocRelease(rri->requestBufp, rri->requestHdrp, range_field);
    }

    // remove Accept-Encoding
    ae_field = TSMimeHdrFieldFind(rri->requestBufp, rri->requestHdrp, TS_MIME_FIELD_ACCEPT_ENCODING, TS_MIME_LEN_ACCEPT_ENCODING);
    if (ae_field) {
        TSMimeHdrFieldDestroy(rri->requestBufp, rri->requestHdrp, ae_field);
        TSHandleMLocRelease(rri->requestBufp, rri->requestHdrp, ae_field);
    }

    txn_state->start = start;
    txn_state->end = end;
    txn_state->content_length = 0;

    TSDebug(PLUGIN_NAME, "TSRemapDoRemap start=%ld, end=%ld", start, end);

    if (NULL == (txn_contp = TSContCreate((TSEventFunc) transform_plugin, NULL))) {
        if(txn_state != NULL)
            TSfree(txn_state);
        TSError("[%s] TSContCreate(): failed to create the transaction handler continuation.", PLUGIN_NAME);
    } else {
        TSContDataSet(txn_contp, txn_state);
        TSHttpTxnHookAdd(txnp, TS_HTTP_READ_RESPONSE_HDR_HOOK, txn_contp);
        TSHttpTxnHookAdd(txnp, TS_HTTP_CACHE_LOOKUP_COMPLETE_HOOK, txn_contp);
        TSHttpTxnHookAdd(txnp, TS_HTTP_TXN_CLOSE_HOOK, txn_contp);
    }

    return TSREMAP_NO_REMAP;
}