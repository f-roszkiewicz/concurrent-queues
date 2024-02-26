#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <threads.h>

#include "HazardPointer.h"

thread_local int _thread_id = -1;
int _num_threads = -1;

void HazardPointer_register(int thread_id, int num_threads) {
    _thread_id = thread_id;
    if (_num_threads != num_threads) {
        _num_threads = num_threads;
    }
}

void HazardPointer_initialize(HazardPointer* hp) {
    for (int i = 0; i < MAX_THREADS; i++) {
        atomic_init(&hp->pointer[i], NULL);
        hp->retired_size[i] = 0;
    }
}

void HazardPointer_finalize(HazardPointer* hp) {
    for (int i = 0; i < _num_threads; i++) {
        for (int j = 0; j < hp->retired_size[i]; j++) {
            if (hp->retired[i][j]) {
                free(hp->retired[i][j]);
            }
        }
    }
}

void* HazardPointer_protect(HazardPointer* hp, const _Atomic(void*)* atom) {
    void* pointer = atomic_load(atom);
    atomic_store(&hp->pointer[_thread_id], *atom);
    if (atomic_load(&hp->pointer[_thread_id]) != pointer) {
        atomic_store(&hp->pointer[_thread_id], NULL);
        return atomic_load(atom);
    } else {
        return pointer;
    }
}

void HazardPointer_clear(HazardPointer* hp) {
    atomic_store(&hp->pointer[_thread_id], NULL);
}

void free_retired_pointers(HazardPointer* hp, int size) {
    size_t not_freed = 0;
    for (int i = 0; i < size; i++) {
        bool collision = false;
        for (int j = 0; j < _num_threads; j++) {
            if (hp->retired[_thread_id][i] == atomic_load(&hp->pointer[j])) {
                collision = true;
                hp->retired[_thread_id][not_freed++] = hp->retired[_thread_id][i];
                break;
            }
        }
        if (!collision && hp->retired[_thread_id][i]) {
            free(hp->retired[_thread_id][i]);
        }
    }
    hp->retired_size[_thread_id] = not_freed;
}

void HazardPointer_retire(HazardPointer* hp, void* ptr) {
    if (hp->retired_size[_thread_id] >= MAX_THREADS) {
        free_retired_pointers(hp, MAX_THREADS);
        HazardPointer_retire(hp, ptr);
    }
    hp->retired[_thread_id][hp->retired_size[_thread_id]++] = ptr;
    if (hp->retired_size[_thread_id] >= RETIRED_THRESHOLD) {
        free_retired_pointers(hp, hp->retired_size[_thread_id]);
    }
}
