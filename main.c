#include <stdio.h>
#include <string.h>

#include <bolt/bolt.h>

#define BOLT_HOST "localhost"
#define BOLT_PORT "7687"
#define BOLT_USER "neo4j"
#define BOLT_LOG "0"
#define BOLT_PASSWORD "password"
#define BOLT_USER_AGENT "seabolt-demo/1.7"
#define HEADER_SEPERATOR "============"

#define BOLTVALUE_STR(connection, value, buffer, size) { \
    if (BoltValue_to_string(value, buffer, size, connection)>size) {\
        string_buffer[size-1] = 0; \
    } \
}
#define TRY(connection, code) { \
    int error_try = (code); \
    if (error_try != BOLT_SUCCESS) { \
        check_and_print_error(connection, BoltConnection_status(connection), NULL); \
        return error_try; \
    } \
}

const char* getenv_or_default(const char* name, const char* default_value)
{
    const char* value = getenv(name);
    return (value==NULL) ? default_value : value;
}

void log_to_stderr(void* state, const char* message)
{
    fprintf(stderr, "%s\n", message);
}

struct BoltLog* create_logger(int enabled)
{
    struct BoltLog* log = BoltLog_create(NULL);
    BoltLog_set_debug_func(log, enabled ? log_to_stderr : NULL);
    BoltLog_set_warning_func(log, enabled ? log_to_stderr : NULL);
    BoltLog_set_info_func(log, enabled ? log_to_stderr : NULL);
    BoltLog_set_error_func(log, log_to_stderr);
    return log;
}

int32_t check_and_print_error(BoltConnection* connection, BoltStatus* status, const char* error_text)
{
    int32_t error_code = BoltStatus_get_error(status);
    if (error_code==BOLT_SUCCESS) {
        return BOLT_SUCCESS;
    }

    if (error_code==BOLT_SERVER_FAILURE) {
        char string_buffer[4096];
        BOLTVALUE_STR(connection, BoltConnection_failure(connection), string_buffer, 4096);
        fprintf(stderr, "%s: %s", error_text==NULL ? "server failure" : error_text, string_buffer);
    }
    else {
        const char* error_ctx = BoltStatus_get_error_context(status);
        fprintf(stderr, "%s (code: %04X, text: %s, context: %s)\n", error_text==NULL ? "Bolt failure" : error_text,
                error_code, BoltError_get_string(error_code), error_ctx);
    }
    return error_code;
}

BoltConnector* create_connector()
{
    BoltAddress* address = BoltAddress_create(getenv_or_default("BOLT_HOST", BOLT_HOST),
            getenv_or_default("BOLT_HOST", BOLT_PORT));
    BoltValue* auth_token = BoltAuth_basic(getenv_or_default("BOLT_USER", BOLT_USER),
            getenv_or_default("BOLT_PASSWORD", BOLT_PASSWORD), NULL);
    BoltLog* logger = create_logger(strcmp(getenv_or_default("BOLT_LOG", BOLT_LOG), "1")==0);
    BoltConfig* config = BoltConfig_create();
    BoltConfig_set_user_agent(config, BOLT_USER_AGENT);
    BoltConfig_set_log(config, logger);

    BoltConnector* connector = BoltConnector_create(address, auth_token, config);

    BoltAddress_destroy(address);
    BoltValue_destroy(auth_token);
    BoltLog_destroy(logger);
    BoltConfig_destroy(config);

    return connector;
}

int32_t run_cypher(BoltConnection* connection, char* cypher)
{
    char string_buffer[4096];

    TRY(connection, BoltConnection_set_run_cypher(connection, cypher, strlen(cypher), 0));
    TRY(connection, BoltConnection_load_run_request(connection));
    BoltRequest run = BoltConnection_last_request(connection);

    TRY(connection, BoltConnection_load_pull_request(connection, -1));
    BoltRequest pull_all = BoltConnection_last_request(connection);

    TRY(connection, BoltConnection_send(connection));

    if (BoltConnection_fetch_summary(connection, run)<0 || !BoltConnection_summary_success(connection)) {
        return check_and_print_error(connection, BoltConnection_status(connection), "cypher execution failed");
    }

    const BoltValue* field_names = BoltConnection_field_names(connection);
    for (int i = 0; i<BoltValue_size(field_names); i++) {
        BoltValue* field_name = BoltList_value(field_names, i);
        if (i>0) {
            printf("\t");
        }
        BOLTVALUE_STR(connection, field_name, string_buffer, 4096);
        printf("%-12s", string_buffer);
    }
    printf("\n");

    for (int i = 0; i<BoltValue_size(field_names); i++) {
        if (i>0) {
            printf("\t");
        }
        printf(HEADER_SEPERATOR);
    }
    printf("\n");

    while (BoltConnection_fetch(connection, pull_all)>0) {
        const BoltValue* field_values = BoltConnection_field_values(connection);
        for (int i = 0; i<BoltValue_size(field_values); i++) {
            BoltValue* field_value = BoltList_value(field_values, i);
            if (i>0) {
                printf("\t");
            }
            BOLTVALUE_STR(connection, field_value, string_buffer, 4096);
            printf("%s", string_buffer);
        }
        printf("\n");
    }

    return check_and_print_error(connection, BoltConnection_status(connection), "cypher execution failed");;
}

int32_t begin_transaction(BoltConnection* connection)
{
    TRY(connection, BoltConnection_load_begin_request(connection));
    BoltRequest begin_request = BoltConnection_last_request(connection);
    TRY(connection, BoltConnection_send(connection));
    BoltConnection_fetch_summary(connection, begin_request);
    return check_and_print_error(connection, BoltConnection_status(connection), "begin transaction failed");
}

int32_t commit(BoltConnection* connection)
{
    TRY(connection, BoltConnection_load_commit_request(connection));
    BoltRequest commit_request = BoltConnection_last_request(connection);
    TRY(connection, BoltConnection_send(connection));
    BoltConnection_fetch_summary(connection, commit_request);
    return check_and_print_error(connection, BoltConnection_status(connection), "commit transaction failed");
}

int main(int argc, char** argv)
{
    if (argc==1) {
        fprintf(stderr, "USAGE: %s cypher [cypher]... ", argv[0]);
        return 1;
    }
    int error = BOLT_SUCCESS;

    // This should be the first call before any seabolt functions
    Bolt_startup();

    // Get a connector instance
    BoltConnector* connector = create_connector();

    // Try to acquire a connection
    BoltStatus* status = BoltStatus_create();
    BoltConnection* connection = BoltConnector_acquire(connector, BOLT_ACCESS_MODE_READ, status);
    if (connection!=NULL) {
        printf("starting transaction\n");
        error = begin_transaction(connection);

        for (int i = 1; i<argc && error==BOLT_SUCCESS; i++) {
            printf("executing cypher \"%s\"\n", argv[i]);
            error = run_cypher(connection, argv[i]);
        }

        if (error==BOLT_SUCCESS) {
            printf("committing transaction\n");
            error = commit(connection);
        }
    }
    else {
        check_and_print_error(NULL, status, "unable to acquire connection");
    }

    BoltStatus_destroy(status);

    BoltConnector_destroy(connector);

    Bolt_shutdown();

    return error!=BOLT_SUCCESS;
}
