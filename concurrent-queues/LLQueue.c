#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "HazardPointer.h"
#include "LLQueue.h"

struct LLNode;
typedef struct LLNode LLNode;
typedef _Atomic(LLNode*) AtomicLLNodePtr;

struct LLNode {
    AtomicLLNodePtr next;
    _Atomic(Value) item;
};

LLNode* LLNode_new(Value item) {
    LLNode* node = (LLNode*) malloc(sizeof (LLNode));
    atomic_init(&node->next, NULL);
    atomic_init(&node->item, item);
    return node;
}

struct LLQueue {
    AtomicLLNodePtr head;
    AtomicLLNodePtr tail;
    HazardPointer hp;
};

LLQueue* LLQueue_new(void) {
    LLQueue* queue = (LLQueue*) malloc(sizeof (LLQueue));
    LLNode* head = LLNode_new(EMPTY_VALUE);
    atomic_init(&queue->head, head);
    atomic_init(&queue->tail, head);
    HazardPointer_initialize(&queue->hp);
    return queue;
}

void LLQueue_delete(LLQueue* queue) {
    HazardPointer_finalize(&queue->hp);
    LLNode* node = atomic_load(&queue->head);
    while (node) {
        LLNode* node_to_delete = node;
        node = atomic_load(&node->next);
        free(node_to_delete);
    }
    free(queue);
}

void LLQueue_push(LLQueue* queue, Value item) {
    _Atomic(void*) tail_pointer;
    LLNode* new_tail = LLNode_new(item);
    LLNode* expected = NULL;
    LLNode* tail = atomic_load(&queue->tail);
    atomic_init(&tail_pointer, tail);
    HazardPointer_protect(&queue->hp, &tail_pointer);
    while (!atomic_compare_exchange_strong(&tail->next, &expected, new_tail)) {
        if (atomic_compare_exchange_strong(&queue->tail, &tail, expected)) {
            tail = expected;
        }
        HazardPointer_clear(&queue->hp);
        expected = NULL;
        atomic_store(&tail_pointer, tail);
        HazardPointer_protect(&queue->hp, &tail_pointer);
    }
    atomic_compare_exchange_strong(&queue->tail, &tail, new_tail);
    HazardPointer_clear(&queue->hp);
}

Value LLQueue_pop(LLQueue* queue) {
    _Atomic(void*) head_pointer;
    LLNode* head = atomic_load(&queue->head);
    atomic_init(&head_pointer, head);
    while (true) {
        HazardPointer_protect(&queue->hp, &head_pointer);
        Value head_item = atomic_exchange(&head->item, EMPTY_VALUE);
        if (head_item != EMPTY_VALUE) {
            HazardPointer_clear(&queue->hp);
            return head_item;
        } else {
            LLNode* next = atomic_load(&head->next);
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
        }
    }
}

bool LLQueue_is_empty(LLQueue* queue) {
    _Atomic(void*) head_pointer;
    LLNode* head = atomic_load(&queue->head);
    atomic_init(&head_pointer, head);
    while (true) {
        HazardPointer_protect(&queue->hp, &head_pointer);
        if (atomic_load(&head->item) != EMPTY_VALUE) {
            HazardPointer_clear(&queue->hp);
            return false;
        } else {
            LLNode* next = atomic_load(&head->next);
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
        }
    }
}
