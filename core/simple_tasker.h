/*******************************************************************************
 * Simple Tasker           Cooperative RTC Kernel                   15.06.2026 *
 * @ Dzuin M.I.                    v1.0                                        *
 *                                                                             *
 *                          Simple tasker core                                 *
 ******************************************************************************/
#ifndef __SIMPLE_TASKER_H
#define __SIMPLE_TASKER_H

/*******************************************************************************
 * Include files
 ******************************************************************************/
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stddef.h>
#include <assert.h>

/*******************************************************************************
 * Local pre-processor symbols/macros ('#define')
 ******************************************************************************/
#define ST_MAX_TASKS         2U
#define ST_MAX_SIGNALS       16U
#define ST_MAX_SUBS          2U
#define ST_MAX_TIMERS        6U

/* Helper macro to calculate required chunks for a given byte size */
#define ST_EVENT_CHUNKS(size) (((size) + sizeof(st_event_t) - 1U) / sizeof(st_event_t))

/* Helper macro to check if a number is a power of 2 */
#define ST_IS_POWER_OF_2(x)  (((x) != 0U) && (((x) & ((x) - 1U)) == 0U))

#define ST_ENTER_CRITICAL() /* __disable_irq(); */
#define ST_EXIT_CRITICAL()  /* __enable_irq(); */

/*******************************************************************************
 * Global type definitions ('typedef')
 ******************************************************************************/
struct st_task_t;
typedef void (*st_idle_t)(void);

typedef uint8_t st_signal_t;
enum
{
  ST_SIGNAL_EMPTY   = 0U,
  ST_SIGNAL_ENTRY   = 1U,
  ST_SIGNAL_EXIT    = 2U,
  ST_SIGNAL_INIT    = 3U,
  ST_SIGNAL_TIMEOUT = 4U,
  ST_SIGNAL_USER    = 8U,
};

typedef struct st_event_t
{
  st_signal_t sig;
  uint8_t slots;        /* Number of contiguous st_event_t chunks this occupies (>= 1) */
  uint8_t _reserved[2]; /* Padding for 4-byte alignment */
} st_event_t;

typedef void (*st_handler_t)(struct st_task_t *me, st_event_t const *e);

typedef struct st_task_t
{
  uint8_t prio;
  st_handler_t handler;
  st_event_t* queue;     /* User-allocated flat array of st_event_t chunks */
  uint8_t qsize;         /* Ring-buffer capacity in chunks (MUST be power of 2) */
  uint8_t head;          /* Write index (in chunks) */
  uint8_t tail;          /* Read index (in chunks) */
  uint8_t nused;         /* Currently used chunks */
} st_task_t;

typedef struct st_timer_t
{
  st_task_t* task;
  st_signal_t sig;
  uint32_t interval;
  uint32_t counter;
  bool active;
  struct st_timer_t* next;
} st_timer_t;

/*******************************************************************************
 * Global function prototypes (definition in C source)
 ******************************************************************************/
/*******************************************************************************
 ** \brief  Initialize the simple tasker kernel
 ** \param  fn_idle  idle callback, NULL = skip
 ** \retval None
 ******************************************************************************/
void st_init(st_idle_t fn_idle);

/*******************************************************************************
 ** \brief  Construct a task (active object)
 ** \param  me       pointer to user-allocated task struct
 ** \param  prio     unique priority (1..ST_MAX_TASKS)
 ** \param  handler  event handler function
 ** \param  queue    user-allocated flat array of st_event_t chunks
 ** \param  qsize    ring-buffer capacity in chunks (must be power of 2)
 ** \retval None
 ******************************************************************************/
void st_task_ctor(st_task_t *me, uint8_t prio, st_handler_t handler, st_event_t *queue, uint8_t qsize);

/*******************************************************************************
 ** \brief  Start a task 鈥� calls handler with ST_SIGNAL_INIT
 ** \param  me  pointer to task
 ** \retval None
 ******************************************************************************/
void st_task_start(st_task_t *me);

/*******************************************************************************
 ** \brief  Post ordinary event directly to a task's queue (1 chunk)
 ** \param  me  target task
 ** \param  e   event to post
 ** \retval true on success, false if queue full
 ******************************************************************************/
bool st_post(st_task_t *me, st_event_t const *e);

/*******************************************************************************
 ** \brief  Post mutable event directly to a task's queue (N chunks)
 ** \param  me    target task
 ** \param  e     event data to copy (must start with st_event_t layout)
 ** \param  size  total size of the event structure in bytes
 ** \retval true on success, false if queue full or doesn't fit to end
 ******************************************************************************/
bool st_post_mutable(st_task_t *me, void const *e, uint8_t size);

/*******************************************************************************
 ** \brief  Subscribe a task to a signal
 ** \param  sig  signal to subscribe to
 ** \param  me   task that will receive published events
 ** \retval None
 ******************************************************************************/
void st_subscribe(st_signal_t sig, st_task_t *me);

/*******************************************************************************
 ** \brief  Unsubscribe a task from a signal
 ** \param  sig  signal to unsubscribe from
 ** \param  me   task to remove
 ** \retval None
 ******************************************************************************/
void st_unsubscribe(st_signal_t sig, st_task_t *me);

/*******************************************************************************
 ** \brief  Publish ordinary event to all subscribers of event->sig
 ** \param  e  event to deliver (strictly 1 chunk)
 ** \retval None
 ******************************************************************************/
void st_publish(st_event_t const *e);

/*******************************************************************************
 ** \brief  Start a timer
 ** \param  tmr      pointer to user-allocated timer struct
 ** \param  task     target task to receive signal on fire
 ** \param  sig      signal to post when timer fires
 ** \param  interval timer period in ticks (0 = one-shot)
 ** \param  counter  initial delay in ticks (> 0)
 ** \retval None
 ******************************************************************************/
void st_timer_start(st_timer_t *tmr, st_task_t *task, st_signal_t sig, uint32_t interval, uint32_t counter);

/*******************************************************************************
 ** \brief  Stop a running timer
 ** \param  tmr  timer to stop
 ** \retval None
 ******************************************************************************/
void st_timer_stop(st_timer_t *tmr);

/*******************************************************************************
 ** \brief  System tick 鈥� call from timer ISR or periodic superloop
 ** \param  None
 ** \retval None
 ******************************************************************************/
void st_tick(void);

/*******************************************************************************
 ** \brief  Main event loop 鈥� processes events forever (cooperative)
 ** \param  None
 ** \retval None
 ******************************************************************************/
void st_run(void);

#endif /* __SIMPLE_TASKER_H */
