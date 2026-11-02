# Звіт — етап 2, Ray Grid, IEEE-118

Кластер: 1 head + 1 worker. Код: `source/ray-grid/`. Прогін: `--trials 10000`, `k = 1`, `s = 83810`, `seed = 42`.

Числа нижче заповнюються з `benchmark_results_ray_grid.json` і з виводу `python run.py status` після прогону на двох машинах. Порожня клітинка означає, що прогону ще не було.

## 1. Стан кластера

Команда на head: `python run.py status`

```text
(вставити вивід: 2 вузли Alive)
```

## 2. Прогін `backend=ray`

Команда: `python run.py load --trials 10000 --wait-nodes 2 --backend ray`

| Поле | Значення |
| :--- | :--- |
| `total_execution_time_sec` | |
| `throughput_trials_per_sec` | |
| `cluster.nodes` | |
| `cluster.cpus` | |
| `topology_put_ms` | |
| `topology_bytes` | |

## 3. Порівняння бекендів

Команда: `python run.py load --trials 10000 --compare --wait-nodes 2`

| Бекенд | Де рахувалось | `T_total`, с | Trials/s |
| :--- | :--- | :--- | :--- |
| `pool` | лише head | | |
| `numpy` | лише head | | |
| `ray` | head + worker | | |

`deterministic_across_backends`:

## 4. Мережа, повільний вузол, Docker

Писати за фактами цього прогону.

### Що завадило в мережі або ОС

### Чому повільний воркер тримає весь прогін

### Навіщо далі Docker
