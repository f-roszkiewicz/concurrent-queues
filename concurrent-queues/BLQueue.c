#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include "BLQueue.h"
#include "HazardPointer.h"

struct BLNode;
typedef struct BLNode BLNode;
typedef _Atomic(BLNode*) AtomicBLNodePtr;

struct BLNode {
    AtomicBLNodePtr next;
    _Atomic(Value) buffer[BUFFER_SIZE];
    _Atomic(size_t) pop_idx;
    _Atomic(size_t) push_idx;
};

BLNode* BLNode_new(void) {
    BLNode* node = (BLNode*) malloc(sizeof (BLNode));
    for (int i = 0; i < BUFFER_SIZE; i++) {
        atomic_init(&node->buffer[i], EMPTY_VALUE);
    }
    atomic_init(&node->next, NULL);
    atomic_init(&node->pop_idx, 0);
    atomic_init(&node->push_idx, 0);
    return node;
}

struct BLQueue {
    AtomicBLNodePtr head;
    AtomicBLNodePtr tail;
    HazardPointer hp;
};

BLQueue* BLQueue_new(void) {
    BLQueue* queue = (BLQueue*)malloc(sizeof(BLQueue));
    BLNode* head = BLNode_new();
    atomic_init(&queue->head, head);
    atomic_init(&queue->tail, head);
    HazardPointer_initialize(&queue->hp);
    return queue;
}

void BLQueue_delete(BLQueue* queue) {
    HazardPointer_finalize(&queue->hp);
    BLNode* node = atomic_load(&queue->head);
    while (node) {
        BLNode* node_to_delete = node;
        node = atomic_load(&node->next);
        free(node_to_delete);
    }
    free(queue);
}

void BLQueue_push(BLQueue* queue, Value item) {
    _Atomic(void*) tail_pointer;
    BLNode* tail = atomic_load(&queue->tail);
    atomic_init(&tail_pointer, tail);
    HazardPointer_protect(&queue->hp, &tail_pointer);
    while (true) {
        _Atomic(size_t) tmp_push_idx = atomic_fetch_add(&tail->push_idx, 1);
        size_t push_idx = atomic_load(&tmp_push_idx);
        if (push_idx < BUFFER_SIZE) {
            Value empty_value = EMPTY_VALUE;
            if (atomic_compare_exchange_strong(&tail->buffer[push_idx], &empty_value, item)) {
                break;
            } else {
                continue;
            }
        } else {
            BLNode* next = atomic_load(&tail->next);
            if (next) {
                HazardPointer_clear(&queue->hp);
                atomic_compare_exchange_strong(&queue->tail, &tail, next);
                atomic_store(&tail_pointer, tail);
                HazardPointer_protect(&queue->hp, &tail_pointer);
                continue;
            } else {
                BLNode* new_tail = BLNode_new();
                atomic_store(&new_tail->buffer[0], item);
                atomic_fetch_add(&new_tail->push_idx, 1);
                if (!atomic_compare_exchange_strong(&tail->next, &next, new_tail)) {
                    HazardPointer_clear(&queue->hp);
                    free(new_tail);
                    if (atomic_compare_exchange_strong(&queue->tail, &tail, next)) {
                        tail = next;
                    }
                    atomic_store(&tail_pointer, tail);
                    HazardPointer_protect(&queue->hp, &tail_pointer);
                    continue;
                } else {
                    HazardPointer_clear(&queue->hp);
                    atomic_compare_exchange_strong(&queue->tail, &tail, new_tail);
                    break;
                }
            }
        }
    }
}

Value BLQueue_pop(BLQueue* queue) {
    _Atomic(void*) head_pointer;
    BLNode* head = atomic_load(&queue->head);
    atomic_init(&head_pointer, head);
    HazardPointer_protect(&queue->hp, &head_pointer);
    while (true) {
        _Atomic(size_t) tmp_pop_idx = atomic_fetch_add(&head->pop_idx, 1);
        size_t pop_idx = atomic_load(&tmp_pop_idx);
        if (pop_idx < BUFFER_SIZE) {
            Value previous = atomic_exchange(&head->buffer[pop_idx], TAKEN_VALUE);
            if (previous == EMPTY_VALUE) {
                continue;
            } else {
                HazardPointer_clear(&queue->hp);
                return previous;
            }
        } else {
            BLNode* next = atomic_load(&head->next);
            if (!next) {
                HazardPointer_clear(&queue->hp);
                return EMPTY_VALUE;
            }
            if (atomic_compare_exchange_strong(&queue->head, &head, next)) {
                HazardPointer_retire(&queue->hp, head);
                head = atomic_load(&queue->head);
            }
            HazardPointer_clear(&queue->hp);
            atomic_store(&head_pointer, head);
            HazardPointer_protect(&queue->hp, &head_pointer);
        }
    }
}

bool BLQueue_is_empty(BLQueue* queue) {
    _Atomic(void*) head_pointer;
    BLNode* head = atomic_load(&queue->head);
    atomic_init(&head_pointer, head);
    HazardPointer_protect(&queue->hp, &head_pointer);
    while (true) {
        size_t pop_idx = atomic_load(&head->pop_idx);
        if (pop_idx < BUFFER_SIZE) {
            Value item = atomic_load(&head->buffer[pop_idx]);
            if (item == TAKEN_VALUE) {
                continue;
            } else {
                HazardPointer_clear(&queue->hp);
                return item == EMPTY_VALUE;
            }
        } else {
            BLNode* next = atomic_load(&head->next);
            if (!next) {
                HazardPointer_clear(&queue->hp);
                return true;
            }
            if (atomic_compare_exchange_strong(&queue->head, &head, next)) {
                HazardPointer_retire(&queue->hp, head);
                head = atomic_load(&queue->head);
            }
            HazardPointer_clear(&queue->hp);
            atomic_store(&head_pointer, head);
            HazardPointer_protect(&queue->hp, &head_pointer);
        }
    }
}
