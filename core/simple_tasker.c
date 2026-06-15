/*******************************************************************************
 * Simple Tasker           Cooperative RTC Kernel                   15.06.2026 *
 * @ Dzuin M.I.                    v1.0                                        *
 *                                                                             *
 *                          Simple tasker core                                 *
 ******************************************************************************/
/*******************************************************************************
 * Include files
 ******************************************************************************/
#include "simple_tasker.h"

/*******************************************************************************
 * Local type definitions ('typedef')
 ******************************************************************************/
typedef struct st_sub_node_t
{
  st_task_t *task;
  struct st_sub_node_t *next;
} st_sub_node_t;


/*******************************************************************************
 * Local variable definitions ('static')
 ******************************************************************************/
static st_task_t* st_task_reg[ST_MAX_TASKS];
static uint8_t    st_task_cnt;
static st_idle_t  st_idle_fn;

static st_sub_node_t  st_sub_pool[ST_MAX_SUBS];
static st_sub_node_t* st_sub_free_list; 
static st_sub_node_t* st_sub_list[ST_MAX_SIGNALS];
static uint8_t st_sub_pool_idx = 0U;

static st_timer_t* st_tmr_head;

/*******************************************************************************
 * Function implementation - local ('static')
 ******************************************************************************/
/*******************************************************************************
 ** \brief  Allocate a subscriber node (uses free list first, then pool)
 ** \param  None
 ** \retval pointer to free node, NULL if pool exhausted
 ******************************************************************************/
static st_sub_node_t *st_sub_alloc(void)
{
  if(st_sub_free_list != NULL)
  {
    st_sub_node_t *node = st_sub_free_list;
    st_sub_free_list = node->next;
    return node;
  }
  
  if(st_sub_pool_idx < (sizeof(st_sub_pool) / sizeof(st_sub_pool[0])))
  {
    return &st_sub_pool[st_sub_pool_idx++];
  }
  return NULL;
}

/*******************************************************************************
 ** \brief  Return a subscriber node to the free list (prevents leaks)
 ** \param  node  pointer to node to free
 ** \retval None
 ******************************************************************************/
static void st_sub_free(st_sub_node_t *node)
{
  node->next = st_sub_free_list;
  st_sub_free_list = node;
}

/*******************************************************************************
 ** \brief  Find subscriber node for (sig, task) pair
 ** \param  sig  signal
 ** \param  me   task
 ** \retval pointer to matching sub_node_t, NULL if not found
 ******************************************************************************/
static st_sub_node_t *st_sub_find(st_signal_t sig, st_task_t *me)
{
  st_sub_node_t *node = st_sub_list[sig];
  while(node)
  {
    if(node->task == me) return node;
    node = node->next;
  }
  return NULL;
}

/*******************************************************************************
 ** \brief  Enqueue event chunks into task's ring buffer (strict boundaries)
 ** \param  me     target task
 ** \param  e      event data to copy
 ** \param  slots  number of contiguous chunks required
 ** \retval true on success, false if queue is full or event doesn't fit to end
 ******************************************************************************/
static bool st_queue_put(st_task_t *me, void const *e, uint8_t slots)
{
  assert(me != NULL);
  assert(e != NULL);
  assert(slots > 0U);
  assert(ST_IS_POWER_OF_2(me->qsize));

  if(slots > me->qsize - me->nused) return false; //Check total capacity
  if(me->head + slots > me->qsize) return false; //Strict boundary check: event must fit contiguously until the end.

  memcpy(&me->queue[me->head], e, slots * sizeof(st_event_t)); //Copy event data directly into the queue array
  me->queue[me->head].slots = slots; //Force the 'slots' metadata to be correct (safety against user error)
  me->head += slots; //Advance head and used counter
  me->nused += slots;

  return true;
}

/*******************************************************************************
 ** \brief  Dequeue event pointer from task ring buffer (FIFO)
 ** \param  me  task
 ** \retval pointer to event, NULL if queue empty
 ******************************************************************************/
static st_event_t *st_queue_get(st_task_t *me)
{
  if(me->nused == 0U) return NULL;
  return &me->queue[me->tail];
}

/*******************************************************************************
 ** \brief  Register a task in the global registry (sorted by prio desc)
 ** \param  me  task
 ** \retval None
 ******************************************************************************/
static void st_task_register(st_task_t *me)
{
  assert(st_task_cnt < ST_MAX_TASKS);
  
  uint8_t i = st_task_cnt;
  while(i > 0U && st_task_reg[i - 1U]->prio < me->prio)
  {
    st_task_reg[i] = st_task_reg[i - 1U];
    --i;
  }
  st_task_reg[i] = me;
  ++st_task_cnt;
}

/*******************************************************************************
 ** \brief  Find highest-priority task with pending events
 ** \param  None
 ** \retval pointer to task, NULL if idle
 ******************************************************************************/
static st_task_t *st_sched(void)
{
  uint8_t i;
  for(i = 0U; i < st_task_cnt; ++i)
  {
    if(st_task_reg[i]->nused > 0U) return st_task_reg[i];
  }
  return NULL;
}

/*******************************************************************************
 * Function implementation - global ('extern')
 ******************************************************************************/
/*******************************************************************************
 ** \brief  Initialize the simple tasker kernel
 ** \param  fn_idle  idle callback, NULL = skip
 ** \retval None
 ******************************************************************************/
void st_init(st_idle_t fn_idle)
{
  memset(st_task_reg, 0, sizeof(st_task_reg));
  memset(st_sub_pool, 0, sizeof(st_sub_pool));
  memset(st_sub_list, 0, sizeof(st_sub_list));
  
  st_task_cnt      = 0U;
  st_sub_free_list = NULL;
  st_tmr_head      = NULL;
  st_idle_fn       = fn_idle;
  st_sub_pool_idx  = 0U;
}

/*******************************************************************************
 ** \brief  Construct a task (active object)
 ** \param  me       pointer to user-allocated task struct
 ** \param  prio     unique priority (1..ST_MAX_TASKS)
 ** \param  handler  event handler function
 ** \param  queue    user-allocated flat array of st_event_t chunks
 ** \param  qsize    ring-buffer capacity in chunks (must be power of 2)
 ** \retval None
 ******************************************************************************/
void st_task_ctor(st_task_t *me, uint8_t prio, st_handler_t handler, st_event_t *queue, uint8_t qsize)
{
  assert(me != NULL);
  assert(queue != NULL);
  assert(ST_IS_POWER_OF_2(qsize));

  memset(me, 0, sizeof(*me));
  me->prio    = prio;
  me->handler = handler;
  me->queue   = queue;
  me->qsize   = qsize;
  st_task_register(me);
}

/*******************************************************************************
 ** \brief  Start a task вЂ” calls handler with ST_SIGNAL_INIT
 ** \param  me  pointer to task
 ** \retval None
 ******************************************************************************/
void st_task_start(st_task_t *me)
{
  static const st_event_t init_evt = {ST_SIGNAL_INIT, 1U, {0, 0}};
  if(!me || !me->handler) return;
  me->handler(me, &init_evt);
}

/*******************************************************************************
 ** \brief  Post ordinary event directly to a task's queue (1 chunk)
 ** \param  me  target task
 ** \param  e   event to post
 ** \retval true on success, false if queue full
 ******************************************************************************/
bool st_post(st_task_t *me, st_event_t const *e)
{
  if(!me || !e) return false;

  ST_ENTER_CRITICAL()
  bool res = st_queue_put(me, e, 1U);
  ST_EXIT_CRITICAL()

  return res;
}

/*******************************************************************************
 ** \brief  Post mutable event directly to a task's queue (N chunks)
 ** \param  me    target task
 ** \param  e     event data to copy (must start with st_event_t layout)
 ** \param  size  total size of the event structure in bytes
 ** \retval true on success, false if queue full or doesn't fit to end
 ******************************************************************************/
bool st_post_mutable(st_task_t *me, void const *e, uint8_t size)
{
  if(!me || !e || size < sizeof(st_event_t)) return false;
  
  if(size > (UINT8_MAX * sizeof(st_event_t))) return false; 

  uint8_t slots = ST_EVENT_CHUNKS(size);

  ST_ENTER_CRITICAL()
  bool res = st_queue_put(me, e, slots);
  ST_EXIT_CRITICAL()

  return res;
}

/*******************************************************************************
 ** \brief  Subscribe a task to a signal
 ** \param  sig  signal to subscribe to
 ** \param  me   task that will receive published events
 ** \retval None
 ******************************************************************************/
void st_subscribe(st_signal_t sig, st_task_t *me)
{
  if(!me || sig >= ST_MAX_SIGNALS) return;
  if(st_sub_find(sig, me)) return;
  
  st_sub_node_t *node = st_sub_alloc();
  if(!node) return;
  
  node->task = me;
  node->next = st_sub_list[sig];
  st_sub_list[sig] = node;
}

/*******************************************************************************
 ** \brief  Unsubscribe a task from a signal
 ** \param  sig  signal to unsubscribe from
 ** \param  me   task to remove
 ** \retval None
 ******************************************************************************/
void st_unsubscribe(st_signal_t sig, st_task_t *me)
{
  st_sub_node_t *prev = NULL;
  st_sub_node_t *node = NULL;
  
  if(!me || sig >= ST_MAX_SIGNALS) return;

  node = st_sub_list[sig];
  
  while (node)
  {
    if(node->task == me)
    {
      if (prev) prev->next = node->next;
      else      st_sub_list[sig] = node->next;
      st_sub_free(node); /* Return to free list, preventing leaks */
      return;
    }
    prev = node;
    node = node->next;
  }
}

/*******************************************************************************
 ** \brief  Publish ordinary event to all subscribers of event->sig
 ** \param  e  event to deliver (strictly 1 chunk)
 ** \retval None
 ******************************************************************************/
void st_publish(st_event_t const *e)
{
  st_sub_node_t *node;
  
  if(!e || e->sig >= ST_MAX_SIGNALS) return;
  
  ST_ENTER_CRITICAL()

  node = st_sub_list[e->sig];
  while (node)
  {
    st_post(node->task, e);
    node = node->next;
  }

  ST_EXIT_CRITICAL()
}

/*******************************************************************************
 ** \brief  Start a timer
 ** \param  tmr      pointer to user-allocated timer struct
 ** \param  task     target task to receive signal on fire
 ** \param  sig      signal to post when timer fires
 ** \param  interval timer period in ticks (0 = one-shot)
 ** \param  counter  initial delay in ticks (> 0)
 ** \retval None
 ******************************************************************************/
void st_timer_start(st_timer_t *tmr, st_task_t *task, st_signal_t sig, uint32_t interval, uint32_t counter)
{
  st_timer_t *prev = NULL;
  st_timer_t *t;

  if (!tmr || !task || !counter) return;

  t = st_tmr_head;
  while(t)
  {
    if(t == tmr)
    {
      if(prev) prev->next = t->next;
      else     st_tmr_head = t->next;
      break;
    }
    prev = t;
    t = t->next;
  }

  tmr->task     = task;
  tmr->sig      = sig;
  tmr->interval = interval;
  tmr->counter  = counter;
  tmr->active   = true;
  tmr->next     = NULL;
  
  if(!st_tmr_head)
  {
    st_tmr_head = tmr;
  }
  else
  {
    t = st_tmr_head;
    while (t->next) t = t->next;
    t->next = tmr;
  }
}

/*******************************************************************************
 ** \brief  Stop a running timer
 ** \param  tmr  timer to stop
 ** \retval None
 ******************************************************************************/
void st_timer_stop(st_timer_t *tmr)
{
  st_timer_t *prev = NULL;
  st_timer_t *t = st_tmr_head;
  
  if(!tmr) return;
  
  while(t)
  {
    if(t == tmr)
    {
      if(prev) prev->next = t->next;
      else     st_tmr_head = t->next;
      
      tmr->active  = false;
      tmr->counter = 0U;
      tmr->next    = NULL;
      return;
    }
    prev = t;
    t = t->next;
  }
}

/*******************************************************************************
 ** \brief  System tick вЂ” call from timer ISR or periodic superloop
 ** \param  None
 ** \retval None
 ******************************************************************************/
void st_tick(void)
{
  ST_ENTER_CRITICAL()
  st_timer_t *t = st_tmr_head;
  while(t)
  {
    if(t->active && t->counter > 0U)
    {
      --t->counter;
      if(t->counter == 0U)
      {
        st_event_t evt = { t->sig, 1U, {0, 0} }; //Timers strictly post ordinary 1-chunk events
        st_post(t->task, &evt);
        
        if(t->interval > 0U) t->counter = t->interval;
        else                 t->active = false;
      }
    }
    t = t->next;
  }
  ST_EXIT_CRITICAL()
}

/*******************************************************************************
 ** \brief  Main event loop вЂ” processes events forever (cooperative)
 ** \param  None
 ** \retval None
 ******************************************************************************/
void st_run(void)
{
  st_task_t *task = st_sched();
  if(task)
  {
    st_event_t *e = st_queue_get(task);
    if(e)
    {
  	  task->handler(task, e);  //Process the event
  	  task->nused -= e->slots;  //Free the exact amount of chunks the event occupied
  	  task->tail = (uint8_t)((task->tail + e->slots) & (task->qsize - 1U));  //Advance tail with wrap-around
  	  if(task->nused == 0U)  //Defragmentation reset: if queue is empty, reset pointers to 0
  	  {
  	    task->head = 0U;
  	    task->tail = 0U;
  	  }
    }
  }
  else if(st_idle_fn)
  {
    st_idle_fn();
  }
}
