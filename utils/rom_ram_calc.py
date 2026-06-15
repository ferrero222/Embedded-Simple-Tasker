#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple Tasker RAM Footprint Calculator
Запустите скрипт, чтобы увидеть точное потребление ОЗУ ядром и вашими задачами.
"""

# ==============================================================================
# 1. НАСТРОЙКИ ЯДРА (как в simple_tasker.h)
# ==============================================================================
MAX_TASKS   = 2   # Максимальное кол-во задач
MAX_SIGNALS = 16   # Максимальное кол-во сигналов
MAX_SUBS    = 2  # Максимальное кол-во подписок
MAX_TIMERS  = 6   # Максимальное кол-во таймеров

# ==============================================================================
# 2. НАСТРОЙКИ ВАШЕГО ПРИЛОЖЕНИЯ
# ==============================================================================
# Укажите размер очереди (qsize) для каждой вашей задачи. 
# Помните: qsize должен быть степенью двойки (2, 4, 8, 16, 32, 64, 128, 256)
MY_TASK_QUEUES = [
    16,  # Задача 1 (например, UI)
    8,   # Задача 2 (например, датчики)
]

MY_ACTIVE_TIMERS = 2 # Сколько таймеров вы реально используете

# ==============================================================================
# 3. РАСЧЁТ (для 32-битной архитектуры, например ARM Cortex-M)
# ==============================================================================
PTR_SIZE = 4   # Размер указателя
EVENT_SIZE = 4 # sizeof(st_event_t)
TASK_STRUCT_SIZE = 12 # sizeof(st_task_t)
SUB_NODE_SIZE = 8     # sizeof(st_sub_node_t)
TIMER_STRUCT_SIZE = 16 # sizeof(st_timer_t)
MISC_VARS = 20        # Служебные переменные ядра

# Расчёт ядра
core_ram = (
    (MAX_TASKS * PTR_SIZE) +          # st_task_reg
    (MAX_SIGNALS * PTR_SIZE) +        # st_sub_list
    (MAX_SUBS * SUB_NODE_SIZE) +      # st_sub_pool
    (MAX_TIMERS * TIMER_STRUCT_SIZE) + # st_tmr_pool
    MISC_VARS                         # Счётчики, флаги и т.д.
)

# Расчёт задач пользователя
tasks_ram = sum(TASK_STRUCT_SIZE + (qsize * EVENT_SIZE) for qsize in MY_TASK_QUEUES)

# Расчёт таймеров пользователя
timers_ram = MY_ACTIVE_TIMERS * TIMER_STRUCT_SIZE

# Итого
total_ram = core_ram + tasks_ram + timers_ram

# ==============================================================================
# 4. ВЫВОД РЕЗУЛЬТАТОВ
# ==============================================================================
print("=" * 60)
print(" 🧮 Simple Tasker: Расчёт потребления ОЗУ (32-bit)")
print("=" * 60)

print(f"\n📦 ПАМЯТЬ ЯДРА (статическая): {core_ram:>4} байт")
print(f"   ├─ Массив задач (st_task_reg)      : {MAX_TASKS * PTR_SIZE:>3} байт")
print(f"   ├─ Списки сигналов (st_sub_list)   : {MAX_SIGNALS * PTR_SIZE:>3} байт")
print(f"   ├─ Пул подписок (st_sub_pool)      : {MAX_SUBS * SUB_NODE_SIZE:>3} байт")
print(f"   ├─ Пул таймеров (st_tmr_pool)      : {MAX_TIMERS * TIMER_STRUCT_SIZE:>3} байт")
print(f"   └─ Служебные переменные            : {MISC_VARS:>3} байт")

print(f"\n📦 ПАМЯТЬ ПОЛЬЗОВАТЕЛЯ: {tasks_ram + timers_ram:>4} байт")
print(f"   ├─ Задачи ({len(MY_TASK_QUEUES)} шт.):")
for i, qsize in enumerate(MY_TASK_QUEUES, 1):
    task_mem = TASK_STRUCT_SIZE + (qsize * EVENT_SIZE)
    print(f"   │  └─ Задача {i} (qsize={qsize:<3}) : {task_mem:>3} байт")
print(f"   └─ Активные таймеры ({MY_ACTIVE_TIMERS} шт.) : {timers_ram:>3} байт")

print("-" * 60)
print(f"💾 ИТОГО ОЗУ: {total_ram} байт ({total_ram / 1024:.2f} КБ)")
print("=" * 60)

# Подсказка по оптимизации
if MAX_SUBS > 30:
    print("\n💡 СОВЕТ: Уменьшение MAX_SUBS до 30 сэкономит вам", (MAX_SUBS - 30) * SUB_NODE_SIZE, "байт ОЗУ!")