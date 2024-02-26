#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <threads.h>

#include "BLQueue.h"
#include "HazardPointer.h"
#include "LLQueue.h"
#include "RingsQueue.h"
#include "SimpleQueue.h"

// A structure holding function pointers to methods of some queue type.
struct QueueVTable {
    const char* name;
    void* (*new)(void);
    void (*push)(void* queue, Value item);
    Value (*pop)(void* queue);
    bool (*is_empty)(void* queue);
    void (*delete)(void* queue);
};
typedef struct QueueVTable QueueVTable;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wincompatible-pointer-types"

const QueueVTable queueVTables[] = {
    { "SimpleQueue", SimpleQueue_new, SimpleQueue_push, SimpleQueue_pop, SimpleQueue_is_empty, SimpleQueue_delete },
    /*{ "RingsQueue", RingsQueue_new, RingsQueue_push, RingsQueue_pop, RingsQueue_is_empty, RingsQueue_delete },*/
    { "LLQueue", LLQueue_new, LLQueue_push, LLQueue_pop, LLQueue_is_empty, LLQueue_delete },
    { "BLQueue", BLQueue_new, BLQueue_push, BLQueue_pop, BLQueue_is_empty, BLQueue_delete }
};

#pragma GCC diagnostic pop

void basic_test(QueueVTable Q)
{
    HazardPointer_register(0, 1);
    void* queue = Q.new();

    Q.push(queue, 1);
    Q.push(queue, 2);
    Q.push(queue, 3);
    Value a = Q.pop(queue);
    Value b = Q.pop(queue);
    Value c = Q.pop(queue);
    printf("%lu %lu %lu\n", a, b, c);

    Q.delete(queue);
}

#define NUM_OF_THREADS 10

struct TestData {
    void* queue;
    QueueVTable q;
    int thread_num;
};
typedef struct TestData TestData;

void* producer_thread(void* data) {
    TestData* test = data;
    HazardPointer_register(test->thread_num, NUM_OF_THREADS * 2);
    for (int i = 1; i <= 1000; i++) {
        test->q.push(test->queue, NUM_OF_THREADS * i + test->thread_num);
    }
    return NULL;
}

void* consumer_thread(void* data) {
    TestData* test = data;
    HazardPointer_register(test->thread_num, NUM_OF_THREADS * 2);
    for (int i = 0; i < 1000; i++) {
        test->q.pop(test->queue);
        // printf("thread %d: got %lu (%lu : %lu)\n", test->thread_num, ret, ret % NUM_OF_THREADS, ret / NUM_OF_THREADS);
    }
    return NULL;
}

TestData* testData_new(void* queue, QueueVTable q, int thread_num) {
    TestData* t = (TestData*) malloc(sizeof (TestData));
    t->queue = queue;
    t->q = q;
    t->thread_num = thread_num;
    return t;
}

void test(QueueVTable q) {
    void* queue = q.new();
    TestData* t1 = testData_new(queue, q, 0);
    TestData* t2 = testData_new(queue, q, 1);
    TestData* t3 = testData_new(queue, q, 2);
    TestData* t4 = testData_new(queue, q, 3);
    TestData* t5 = testData_new(queue, q, 4);
    TestData* t6 = testData_new(queue, q, 5);
    TestData* t7 = testData_new(queue, q, 6);
    TestData* t8 = testData_new(queue, q, 7);
    TestData* t9 = testData_new(queue, q, 8);
    TestData* t10 = testData_new(queue, q, 9);
    TestData* t11 = testData_new(queue, q, 10);
    TestData* t12 = testData_new(queue, q, 11);
    TestData* t13 = testData_new(queue, q, 12);
    TestData* t14 = testData_new(queue, q, 13);
    TestData* t15 = testData_new(queue, q, 14);
    TestData* t16 = testData_new(queue, q, 15);
    TestData* t17 = testData_new(queue, q, 16);
    TestData* t18 = testData_new(queue, q, 17);
    TestData* t19 = testData_new(queue, q, 18);
    TestData* t20 = testData_new(queue, q, 19);
    pthread_t p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10;
    pthread_create(&p1, NULL, producer_thread, t1);
    pthread_create(&p2, NULL, producer_thread, t2);
    pthread_create(&p3, NULL, producer_thread, t3);
    pthread_create(&p4, NULL, producer_thread, t4);
    pthread_create(&p5, NULL, producer_thread, t5);
    pthread_create(&p6, NULL, producer_thread, t6);
    pthread_create(&p7, NULL, producer_thread, t7);
    pthread_create(&p8, NULL, producer_thread, t8);
    pthread_create(&p9, NULL, producer_thread, t9);
    pthread_create(&p10, NULL, producer_thread, t10);
    pthread_create(&c1, NULL, consumer_thread, t11);
    pthread_create(&c2, NULL, consumer_thread, t12);
    pthread_create(&c3, NULL, consumer_thread, t13);
    pthread_create(&c4, NULL, consumer_thread, t14);
    pthread_create(&c5, NULL, consumer_thread, t15);
    pthread_create(&c6, NULL, consumer_thread, t16);
    pthread_create(&c7, NULL, consumer_thread, t17);
    pthread_create(&c8, NULL, consumer_thread, t18);
    pthread_create(&c9, NULL, consumer_thread, t19);
    pthread_create(&c10, NULL, consumer_thread, t20);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(p3, NULL);
    pthread_join(p4, NULL);
    pthread_join(p5, NULL);
    pthread_join(p6, NULL);
    pthread_join(p7, NULL);
    pthread_join(p8, NULL);
    pthread_join(p9, NULL);
    pthread_join(p10, NULL);
    pthread_join(c1, NULL);
    pthread_join(c2, NULL);
    pthread_join(c3, NULL);
    pthread_join(c4, NULL);
    pthread_join(c5, NULL);
    pthread_join(c6, NULL);
    pthread_join(c7, NULL);
    pthread_join(c8, NULL);
    pthread_join(c9, NULL);
    pthread_join(c10, NULL);
    q.delete(queue);
    free(t1);
    free(t2);
    free(t3);
    free(t4);
    free(t5);
    free(t6);
    free(t7);
    free(t8);
    free(t9);
    free(t10);
    free(t11);
    free(t12);
    free(t13);
    free(t14);
    free(t15);
    free(t16);
    free(t17);
    free(t18);
    free(t19);
    free(t20);
}

int main(void)
{
    printf("Hello, World!\n");

    for (int i = 0; i < sizeof(queueVTables) / sizeof(QueueVTable); ++i) {
        QueueVTable Q = queueVTables[i];
        printf("Queue type: %s\n", Q.name);
        basic_test(Q);
    }

    return 0;
}
