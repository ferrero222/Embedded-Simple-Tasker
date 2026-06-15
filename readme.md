# Simple Tasker: Cooperative RTC Kernel

**Version:** 1.0  
**Author:** Dzuin M.I.  
**Date:** 15.06.2026  
**License:** MIT / Free for commercial and open-source projects  

## Overview

**Simple Tasker** is a lightweight, platform-independent, cooperative real-time kernel (RTC). It implements the Active Objects paradigm. Designed for soft real-time systems with limited resources or restricted hardware access, where using a classic RTOS is not feasible.

### Key Features:
- **Zero dynamic memory allocation:** No `malloc`/`free`. All memory (queues, subscription pools, timers) is statically allocated by the user.
- **Flexible event system without global pool:** Support for ordinary and mutable events stored directly in the task queue.
- **Built-in timers:** Support for periodic and one-shot timers.
- **Pub/Sub mechanism:** Loosely coupled communication between tasks via signals.
- **Priority-based scheduling:** Tasks with higher priority are always processed first.

---

## Quick Start

```c
#include "simple_tasker.h"
#include <stdio.h>

// 1. Declare a task and its queue (size must be power of 2)
static st_task_t my_task;
static st_event_t my_queue[16]; 

// 2. Write the event handler
static void my_handler(st_task_t *me, st_event_t const *e) {
    if (e->sig == ST_SIGNAL_INIT) {
        printf("Task initialized!\n");
    } else if (e->sig == ST_SIGNAL_USER) {
        printf("User event received!\n");
    }
}

int main(void) {
    // 3. Initialize the kernel
    st_init(NULL); 
    
    // 4. Construct and start the task
    st_task_ctor(&my_task, 1, my_handler, my_queue, 16);
    st_task_start(&my_task); 
    
    // 5. Main loop
    while (1) {
        st_tick();   // Call from system timer interrupt (e.g., every 1 ms)
        st_run();    // Cooperative event processing loop
    }
}
```

---

## User Guide

### 1. API Reference

#### Initialization and Tasks
* **`void st_init(st_idle_t fn_idle)`**  
  Initializes internal kernel structures. `fn_idle` is a function called when no tasks have pending events (ideal for putting MCU to sleep, e.g., `__WFI()`).

* **`void st_task_ctor(st_task_t *me, uint8_t prio, st_handler_t handler, st_event_t *queue, uint8_t qsize)`**  
  Registers a task in the scheduler.  
  **IMPORTANT:** `qsize` (queue size in chunks) **must be a power of 2** (2, 4, 8, 16, 32, 64, 128, 256). Maximum value is 255.

* **`void st_task_start(st_task_t *me)`**  
  Starts the task by synchronously calling its handler with `ST_SIGNAL_INIT`. Used for initial task setup (starting timers, subscribing to signals).

#### Posting Events
* **`bool st_post(st_task_t *me, st_event_t const *e)`**  
  Posts an **ordinary** event (occupies exactly 1 chunk) to a specific task's queue. Returns `false` if queue is full.

* **`bool st_post_mutable(st_task_t *me, void const *e, uint8_t size)`**  
  Posts a **mutable** event with payload. The kernel automatically calculates required memory from the task's event buffer.  
  **IMPORTANT:** Event structure `e` **must** start with a field of type `st_event_t` (or have identical layout of first 4 bytes).

#### Pub/Sub (Publisher-Subscriber)
* **`void st_subscribe(st_signal_t sig, st_task_t *me)`**  
  Subscribes task `me` to receive events with signal `sig`.

* **`void st_unsubscribe(st_signal_t sig, st_task_t *me)`**  
  Unsubscribes the task. Frees internal subscription node for reuse.

* **`void st_publish(st_event_t const *e)`**  
  Sends a copy of event `e` to all tasks subscribed to `e->sig`. Works only with ordinary (1 chunk) events.

#### Timers
* **`void st_timer_start(st_timer_t *tmr, st_task_t *task, st_signal_t sig, uint32_t interval, uint32_t counter)`**  
  Starts a timer. `counter` is initial delay in ticks (>0). `interval` is repetition period (0 = one-shot).  
  **IMPORTANT:** Safe to call restart for already running timer (kernel automatically removes old entry from list).

* **`void st_timer_stop(st_timer_t *tmr)`**  
  Stops the timer and removes it from internal list to save CPU time.

* **`void st_tick(void)`**  
  Decrements counters of all active timers. Must be called periodically (e.g., from SysTick interrupt every 1 ms).

#### Execution
* **`void st_run(void)`**  
  Infinite cooperative loop. Finds highest-priority task with pending events, extracts event and passes it to handler. If no events, calls `fn_idle`.

---

### 2. Critical User Considerations

#### A. Memory Alignment
Structure `st_event_t` has size of 4 bytes. If you create mutable events containing 32-bit fields (`uint32_t`, `float`, pointers), **task queue array must be aligned to 4-byte boundary**. On architectures like RISC-V, misaligned access will cause `HardFault` when handler tries to read data.
```c
// Correct (GCC / ARM):
__attribute__((aligned(4))) static st_event_t my_queue[32];

// Correct (C11):
_Alignas(4) static st_event_t my_queue[32];
```

#### B. Interrupt Safety (ISR Safety)
Functions `st_post`, `st_post_mutable`, `st_publish` and `st_tick` **are not thread-safe by default**. They don't use mutexes to minimize overhead.  
If you call them from interrupt handler (ISR), you **must** define critical section macros:
```c
ST_ENTER_CRITICAL(); // Example: __disable_irq();
ST_EXIT_CRITICAL();  // Example: __enable_irq();
```
Macros are already used inside functions.

#### C. Mutable Event Constraints (Strict Boundaries)
Task event queue is divided into chunks sized according to event structure size. When posting ordinary event, only one chunk is used.
When posting mutable event, corresponding number of chunks is allocated depending on event structure size.
You need to ensure that mutable event structure size doesn't exceed remaining space in task's event queue, otherwise it won't be added.

---

## Memory Footprint

Simple Tasker is designed considering strict constraints of embedded systems. Below is memory consumption estimate for 32-bit architecture (e.g., ARM Cortex-M, where pointer size is 4 bytes).

#### 1. Static Kernel Overhead (RAM)
This memory is reserved by kernel automatically during compilation, regardless of number of created tasks.

| Data Structure | Size | Description |
| :--- | :--- | :--- |
| `st_task_reg` | 64 bytes | Array of task pointers 16(ST_MAX_TASKS) (16 × 4 bytes) |
| `st_sub_pool` | 800 bytes | Static pool of 100(ST_MAX_SUBS) subscription nodes (100 × 8 bytes) |
| `st_sub_list` | 256 bytes | Array of 64(ST_MAX_SIGNALS) subscription list heads (64 × 4 bytes) |
| `st_tmr_pool` | 256 bytes | Static pool of 16(ST_MAX_TIMERS) timers (16 × 16 bytes with alignment) |
| Service variables | ~20 bytes | Counters, list `head` pointers, flags |
| **TOTAL (Kernel)** | **~1.4 KB** | **Fixed RAM consumption by kernel with this configuration** |

#### 2. User Memory Consumption (RAM)
You allocate this memory yourself when declaring tasks and timers.

| Object | Size per instance | Note |
| :--- | :--- | :--- |
| Structure `st_task_t` | 12 bytes | Task itself (without queue) |
| Structure `st_timer_t` | 16 bytes | Timer instance |
| Task queue | `N × 4` bytes | Where `N` is queue size (`qsize`). 1 chunk = 4 bytes. |
| **Task example** | **~76 bytes** | Task + queue for 16 events (12 + 16×4) |

#### 3. Code Size (ROM / Flash)
When compiling with size optimization (`-Os` in GCC/Clang):
* **Base kernel:** ~1.5 – 2.0 KB.
* If you don't use timers or Pub/Sub, linker with flags `-ffunction-sections` and `-fdata-sections` (and subsequent `--gc-sections`) will automatically cut unused functions, reducing size to **~1.0 – 1.2 KB**.

#### 4. How to Reduce Memory Consumption (Scaling)
If 1.4 KB static RAM is too much for your chip, you can easily reduce it by changing macros at the beginning of `simple_tasker.h` file to match your actual needs:

```c
// Default values:
#define ST_MAX_TASKS         16U   // Reduce to 4-8 if few tasks (-48 bytes RAM)
#define ST_MAX_SIGNALS       64U   // Reduce to 16-32 if few signals (-128 bytes RAM)
#define ST_MAX_SUBS          100U  // Reduce to 20-30 if few subscriptions (-560 bytes RAM!)
#define ST_MAX_TIMERS        16U   // Reduce to 4-8 if few timers (-128 bytes RAM)
```
*Example:* Setting `ST_MAX_SUBS = 30` and `ST_MAX_TIMERS = 8` reduces kernel static overhead to **~600 bytes**, making scheduler suitable even for low-power MCUs with 2-4 KB RAM.

---

## Developer Guide (Internal Architecture)

This section describes internal kernel workings for those planning to modify or port it.

### 1. Event Memory Management (Chunking System)
Unlike classic implementations with global event pool, Simple Tasker stores events **directly in task queue flat array**.
* **Chunk structure:** Queue is `st_event_t` array. Each event starts with 4-byte header:
  ```c
  typedef struct st_event_t {
      st_signal_t sig;      // Signal
      uint8_t     slots;    // Number of contiguous chunks occupied by event (>= 1)
      uint8_t     _reserved[2]; // Alignment to 4 bytes
  } st_event_t;
  ```
* **Mechanics:** `st_post_mutable` calculates required chunks via `ST_EVENT_CHUNKS(size)` macro and performs `memcpy` of user data directly to `me->queue[me->head]`. Field `slots` is forcibly overwritten by kernel to guarantee correct memory deallocation.
* **Defragmentation:** When extracting event, `tail` shifts by `e->slots`. If after this `nused == 0`, `head` and `tail` reset to `0`, eliminating any fragmentation.

### 2. Subscription Mechanism (Pub/Sub Internals)
Subscriptions implemented with protection against fragmentation and memory leaks, without using `malloc`.
* **Head array:** `st_sub_list[ST_MAX_SIGNALS]` stores pointers to heads of singly-linked lists for each signal.
* **Static pool + Free List:** Uses array `st_sub_pool[ST_MAX_SUBS]` and pointer `st_sub_free_list`.
  * On `st_subscribe`: Node taken from head of `st_sub_free_list` (O(1)). If list empty, new node taken from `st_sub_pool`. Node added to head of list `st_sub_list[sig]`.
  * On `st_unsubscribe`: Node found in list, removed from it and returned to head of `st_sub_free_list` (O(1)). This **completely prevents pool exhaustion** during frequent subscribe/unsubscribe operations.

### 3. Timer Mechanics
* Timers stored in singly-linked list `st_tmr_head`.
* **Duplication protection:** Function `st_timer_start` explicitly searches for timer in list and removes it before adding. This prevents creation of circular references or duplicates when calling `st_timer_start` again for one-shot timer.
* **CPU optimization:** When one-shot timer fires (`interval == 0`), it automatically removed from list `st_tmr_head` inside `st_tick()`, so in future no CPU cycles wasted checking inactive timers.

### 4. Scheduler
* Tasks registered in array `st_task_reg` and **automatically sorted by descending priority** (Insertion Sort) when calling `st_task_ctor`.
* Function `st_sched()` simply iterates this array top-down and returns first task where `nused > 0`. This guarantees strict priority execution without complex ready bitmasks, saving ROM and simplifying code.

---