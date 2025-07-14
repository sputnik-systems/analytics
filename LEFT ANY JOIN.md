- `LEFT ANY JOIN` vs `LEFT JOIN` → **нет cartesian**, только одно совпадение.
    
- При множественных правых строках — берется первое (или последнее, с `join_any_take_last_row = 1`).
    
- Под капотом — меньше памяти и быстро.
    
- Отлично сочетается с `dictionary engine` как `direct join`.