/*******************************************************************************
 * Simple Tasker Example                                            15.06.2026 *
 *                                                                             *
 *  Implementation of two simple tasks demonstrating core features.           *
 ******************************************************************************/

/*******************************************************************************
 * Include files
 ******************************************************************************/
#include "example.h"
#include <stdio.h>

/*******************************************************************************
 * Local variable definitions ('static')
 ******************************************************************************/
static st_task_t  s_task_a;
static st_event_t s_queue_a[16]; /* Queue capacity: 16 chunks */
static st_timer_t s_timer_a;     /* Periodic timer */
static uint32_t   s_counter_a = 0;

static st_task_t  s_task_b;
static st_event_t s_queue_b[8]; /* Queue capacity: 8 chunks */
static uint8_t    s_mode_b = 0;

/*******************************************************************************
 * Function implementation - local ('static')
 ******************************************************************************/
/*******************************************************************************
 ** \brief  TaskA event handler
 ******************************************************************************/
static void task_a_handler(st_task_t *me, st_event_t const *e)
{
  (void)me;
  
  switch (e->sig)
  {
    case ST_SIGNAL_INIT:
      printf("[TaskA] Initialized. Starting periodic timer.\n");
      st_timer_start(&s_timer_a, &s_task_a, SIG_TICK, 100, 100); //Start timer: fire every 100 ticks
      break;
      
    case SIG_TICK:
      s_counter_a++;
      printf("[TaskA] Tick #%u (mode=%u)\n", s_counter_a, s_mode_b);
      break;
      
    case SIG_DATA_UPDATE:
    {
      data_update_event_t const *evt = (data_update_event_t const *)e; //Cast to our specific mutable event type
      printf("[TaskA] Received data: value=%u, source=%u\n", evt->value, evt->source_id);
      break;
    }
    
    default:
      break;
  }
}

/*******************************************************************************
 ** \brief  TaskB event handler
 ******************************************************************************/
static void task_b_handler(st_task_t *me, st_event_t const *e)
{
  (void)me;
  
  switch (e->sig)
  {
    case ST_SIGNAL_INIT:
      printf("[TaskB] Initialized. Subscribing to SIG_MODE_CHANGE.\n");
      /* Subscribe to mode change events (pub/sub demo) */
      st_subscribe(SIG_MODE_CHANGE, &s_task_b);
      break;
      
    case SIG_MODE_CHANGE:
    {
      st_event_t const *base = e; //This event comes via publish/subscribe
      s_mode_b = base->slots;  //Mode is encoded in slots field for this simple demo
      printf("[TaskB] Mode changed to %u\n", s_mode_b);
      break;
    }
    
    default:
      break;
  }
}

/*******************************************************************************
 * Function implementation - global ('extern')
 ******************************************************************************/
/*******************************************************************************
 ** \brief  idle state handler
 ******************************************************************************/
void example_idle(void)
{
  // _WFI;
}

/*******************************************************************************
 ** \brief  systick
 ******************************************************************************/
void system_tick(void)
{
  ST_ENTER_CRITICAL()
      st_tick();
  ST_EXIT_CRITICAL()
}

/*******************************************************************************
 ** \brief  main
 ******************************************************************************/
void main(void)
{
  st_init(example_idle);

  st_task_ctor(&s_task_a, 1, task_a_handler, s_queue_a, 16); 
  st_task_ctor(&s_task_b, 2, task_b_handler, s_queue_b, 8); 
  
  st_task_start(&s_task_a);
  st_task_start(&s_task_b);

  data_update_event_t evt_a;
  evt_a.super.sig = SIG_DATA_UPDATE;
  evt_a.value     = 0xFF;
  evt_a.source_id = 0x01;
  st_post_mutable(&s_task_a, &evt_a, sizeof(evt_a));

  st_event_t evt_b;
  evt_b.sig   = SIG_MODE_CHANGE;
  evt_b.slots = 5; 
  st_publish(&evt_b);

  while(1)
  {
    st_run();
  }

}
