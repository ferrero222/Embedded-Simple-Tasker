/*******************************************************************************
 * Simple Tasker Example                                            15.06.2026 *
 *                                                                             *
 *  Simple demonstration of tasker capabilities without hardware dependency.  *
 ******************************************************************************/
#ifndef __EXAMPLE_H
#define __EXAMPLE_H

/*******************************************************************************
 * Include files
 ******************************************************************************/
#include "../core/simple_tasker.h"

/*******************************************************************************
 * Global type definitions ('typedef')
 ******************************************************************************/

/* User-defined signals (must be >= ST_SIGNAL_USER) */
enum
{
  SIG_TICK        = ST_SIGNAL_USER + 0,   /* Periodic tick event */
  SIG_DATA_UPDATE = ST_SIGNAL_USER + 1,   /* Data update (mutable) */
  SIG_MODE_CHANGE = ST_SIGNAL_USER + 2,   /* Mode change request */
};

/* Mutable event with payload */
typedef struct
{
  st_event_t super;     /* MUST be the first member */
  uint32_t   value;     /* Some data payload */
  uint8_t    source_id; /* Where this came from */
} data_update_event_t;

/*******************************************************************************
 * Global function prototypes
 ******************************************************************************/
#endif /* __EXAMPLE_H */