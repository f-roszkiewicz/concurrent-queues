#include <malloc.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>

#include "SimpleQueue.h"

struct SimpleQueueNode;
typedef struct SimpleQueueNode SimpleQueueNode;

void assert_zero0(int arg) {
    if (arg != 0) {
        exit(1);
    }
}

struct SimpleQueueNode {
    _Atomic(SimpleQueueNode*) next;
    Value item;
};

SimpleQueueNode* SimpleQueueNode_new(Value item) {
    SimpleQueueNode* node = (SimpleQueueNode*) malloc(sizeof (SimpleQueueNode));
    atomic_init(&node->next, NULL);
    node->item = item;
    return node;
}

struct SimpleQueue {
    SimpleQueueNode* head;
    SimpleQueueNode* tail;
    pthread_mutex_t head_mtx;
    pthread_mutex_t tail_mtx;
};

SimpleQueue* SimpleQueue_new(void) {
    SimpleQueue* queue = (SimpleQueue*) malloc(sizeof (SimpleQueue));
    queue->head = SimpleQueueNode_new(EMPTY_VALUE);
    queue->tail = queue->head;
    assert_zero0(pthread_mutex_init(&queue->head_mtx, NULL));
    assert_zero0(pthread_mutex_init(&queue->tail_mtx, NULL));
    return queue;
}

void SimpleQueue_delete(SimpleQueue* queue) {
    SimpleQueueNode* node = queue->head;
    while (node) {
        SimpleQueueNode* node_to_free = node;
        node = atomic_load(&node->next);
        free(node_to_free);
    }
    assert_zero0(pthread_mutex_destroy(&queue->head_mtx));
    assert_zero0(pthread_mutex_destroy(&queue->tail_mtx));
    free(queue);
}

void SimpleQueue_push(SimpleQueue* queue, Value item) {
    assert_zero0(pthread_mutex_lock(&queue->tail_mtx));
    SimpleQueueNode* new_node = SimpleQueueNode_new(item);
    atomic_store(&queue->tail->next, new_node);
    queue->tail = new_node;
    assert_zero0(pthread_mutex_unlock(&queue->tail_mtx));
}

Value SimpleQueue_pop(SimpleQueue* queue) {
    assert_zero0(pthread_mutex_lock(&queue->head_mtx));
    SimpleQueueNode* true_head = atomic_load(&queue->head->next);
    if (!true_head) {
        assert_zero0(pthread_mutex_unlock(&queue->head_mtx));
        return EMPTY_VALUE;
    }
    Value ret_value = true_head->item;
    assert_zero0(pthread_mutex_lock(&queue->tail_mtx));
    SimpleQueueNode* next = atomic_load(&true_head->next);
    if (!next) {
        queue->tail = queue->head;
        atomic_store(&queue->head->next, next);
        assert_zero0(pthread_mutex_unlock(&queue->tail_mtx));
    } else {
        assert_zero0(pthread_mutex_unlock(&queue->tail_mtx));
        atomic_store(&queue->head->next, next);
    }
    assert_zero0(pthread_mutex_unlock(&queue->head_mtx));
    free(true_head);
    return ret_value;
}

bool SimpleQueue_is_empty(SimpleQueue* queue) {
    assert_zero0(pthread_mutex_lock(&queue->head_mtx));
    bool ret_value = !atomic_load(&queue->head->next);
    assert_zero0(pthread_mutex_unlock(&queue->head_mtx));
    return ret_value;
}
