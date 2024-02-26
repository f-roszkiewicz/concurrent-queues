#include <malloc.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>

#include "HazardPointer.h"
#include "RingsQueue.h"

struct RingsQueueNode;
typedef struct RingsQueueNode RingsQueueNode;

void assert_zero(int arg) {
    if (arg != 0) {
        exit(1);
    }
}

struct RingsQueueNode {
    _Atomic(RingsQueueNode*) next;
    Value* buffer;
    _Atomic(size_t) pop_idx;
    _Atomic(size_t) push_idx;
};

RingsQueueNode* RingsQueueNode_new(void) {
    RingsQueueNode* node = (RingsQueueNode*) malloc(sizeof (RingsQueueNode));
    node->buffer = (Value*) malloc(RING_SIZE * sizeof (Value));
    atomic_init(&node->next, NULL);
    atomic_init(&node->pop_idx, 0);
    atomic_init(&node->push_idx, 0);
    return node;
}

void RingsQueueNode_delete(RingsQueueNode* node) {
    free(node->buffer);
    free(node);
}

struct RingsQueue {
    RingsQueueNode* head;
    RingsQueueNode* tail;
    pthread_mutex_t pop_mtx;
    pthread_mutex_t push_mtx;
};

RingsQueue* RingsQueue_new(void) {
    RingsQueue* queue = (RingsQueue*) malloc(sizeof (RingsQueue));
    queue->head = RingsQueueNode_new();
    queue->tail = queue->head;
    assert_zero(pthread_mutex_init(&queue->pop_mtx, NULL));
    assert_zero(pthread_mutex_init(&queue->push_mtx, NULL));
    return queue;
}

void RingsQueue_delete(RingsQueue* queue) {
    RingsQueueNode* node = queue->head;
    while (node) {
        RingsQueueNode* node_to_delete = node;
        node = atomic_load(&node->next);
        RingsQueueNode_delete(node_to_delete);
    }
    assert_zero(pthread_mutex_destroy(&queue->pop_mtx));
    assert_zero(pthread_mutex_destroy(&queue->push_mtx));
    free(queue);
}

void RingsQueue_push(RingsQueue* queue, Value item) {
    assert_zero(pthread_mutex_lock(&queue->push_mtx));
    RingsQueueNode* tail_node = queue->tail;
    size_t push_idx = atomic_load(&tail_node->push_idx);
    tail_node->buffer[push_idx] = item;
    if ((push_idx + 1) % RING_SIZE == atomic_load(&queue->tail->pop_idx)) {
        RingsQueueNode* new_tail = RingsQueueNode_new();
        atomic_store(&tail_node->next, new_tail);
        queue->tail = new_tail;
    } else {
        atomic_store(&tail_node->push_idx, (push_idx + 1) % RING_SIZE);
    }
    assert_zero(pthread_mutex_unlock(&queue->push_mtx));
}

Value RingsQueue_pop(RingsQueue* queue) {
    assert_zero(pthread_mutex_lock(&queue->pop_mtx));
    RingsQueueNode* head_node = queue->head;
    RingsQueueNode* next_node = atomic_load(&head_node->next);
    size_t pop_idx = atomic_load(&head_node->pop_idx);
    size_t push_idx = atomic_load(&head_node->push_idx);
    Value ret_value;
    if (next_node && pop_idx == push_idx) {
        ret_value = head_node->buffer[pop_idx];
        queue->head = next_node;
        RingsQueueNode_delete(head_node);
    } else if (!next_node && pop_idx == push_idx) {
        ret_value = EMPTY_VALUE;
    } else {
        ret_value = head_node->buffer[pop_idx];
        atomic_store(&head_node->pop_idx, (pop_idx + 1) % RING_SIZE);
    }
    assert_zero(pthread_mutex_unlock(&queue->pop_mtx));
    return ret_value;
}

bool RingsQueue_is_empty(RingsQueue* queue) {
    assert_zero(pthread_mutex_lock(&queue->pop_mtx));
    RingsQueueNode* next_node = atomic_load(&queue->head->next);
    size_t pop_idx = atomic_load(&queue->head->pop_idx);
    size_t push_idx = atomic_load(&queue->head->push_idx);
    bool ret_value = !next_node && pop_idx == push_idx;
    assert_zero(pthread_mutex_unlock(&queue->pop_mtx));
    return ret_value;
}
