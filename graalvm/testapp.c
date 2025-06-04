#include <stdio.h>
#include <stdlib.h>
#include "graal_isolate.h"            // For isolate/thread structs and APIs
#include "libdebeziumshared.h"        // For the startEngine/stopEngine C entrypoints
#include <unistd.h>

int main(int argc, char** argv) {
    graal_isolate_t* isolate = NULL;
    graal_isolatethread_t* thread = NULL;

    // Create isolate
    graal_create_isolate_params_t params = {0};
    params.version = __graal_create_isolate_params_version;

    if (graal_create_isolate(&params, &isolate, &thread) != 0) {
        fprintf(stderr, "Failed to create isolate.\n");
        return 1;
    }

    // Call startEngine with a sample config path
    char* configPath = "/path/to/config.properties";  // <- Set your own valid path
    startEngine(thread, configPath);

    // Optionally wait or do more work
    printf("Debezium engine started...\n");
	
	sleep(100);
    // Call stopEngine when done
    stopEngine(thread);
    printf("Debezium engine stopped.\n");

	sleep(5);

    // Tear down isolate
    if (graal_detach_all_threads_and_tear_down_isolate(thread) != 0) {
        fprintf(stderr, "Failed to tear down isolate.\n");
        return 1;
    }

    return 0;
}

