# PLAN — Етап 1: IEEE-118 Monte Carlo, локальний MIMD

Статус: **v2 APPROVED**. Код у `my-grid-lab/source/grid-mimd/` написано, очікує **APPROVE CODE**. v1 (`my-grid-lab/app.py`) видалено.

Рішення v2: ємність `lr <= 0` — варіант B (`--unrated-margin 2.0`); старий `app.py` — видалено.

Джерела вимог:

- `@cloud_grid/docs/n01_power_grid_project.md` — §1.1, §1.4, §1.5, §2.3, §2.5, §2.9, §3 «Етап 1»;
- вимоги викладача до здачі (§0).

Референс патерну: `@cloud_grid/source/mimd-pc/app.py` (`Pool.map`, top-level worker, headless benchmark, `argparse`). HTTP-частина — поза scope.

---

## 0. Вимоги викладача → що змінюється відносно v1

| Вимога | v1 | v2 |
| :--- | :--- | :--- |
| `load_powergraph_cascades()` + `build_topology()` | є | без змін |
| Однопотоковий `run_trial()`: N-k → перерозподіл за вільною ємністю → trip | є | без змін у логіці; правило ємності «необмежених» ліній — §3, рішення 1 |
| `simulate_trials_chunk()` + `Pool.map` | є | без змін |
| `run_headless_benchmark()`, CLI `--headless --trials --workers --k --data --out` | є | без змін |
| Speedup для `1, 2, 4, …, cpu_count()` + графік | є, за `--plot` | графік **завжди** в `--headless`, файл `speedup.png` |
| `--trials ≥ 10 000` для звіту, `--k 1` обов'язково, `--k 2` бажано | команди є | без змін |
| Зважений вибір N-k за `flow / capacity` (рекомендовано) | поза scope | **додається**, default (§4) |
| Структура `source/grid-mimd/{app.py, engine.py, benchmark_results_mimd_grid.json, speedup.png, README.md}` | один файл у корені | **розбиття на 2 файли + README** (§1) |

---

## 1. Файли

| Файл | Роль |
| :--- | :--- |
| `my-grid-lab/source/grid-mimd/engine.py` | Завантаження + підготовка топології, `run_trial`, `simulate_trials_chunk`, `GridSimulationEngine` |
| `my-grid-lab/source/grid-mimd/app.py` | `run_headless_benchmark`, графік speedup, `--viz`, друк, CLI `main()` |
| `my-grid-lab/source/grid-mimd/README.md` | Як встановити залежності, як запустити, що з'являється на виході |
| `my-grid-lab/source/grid-mimd/benchmark_results_mimd_grid.json` | Результат бенчмарку (`--out`) |
| `my-grid-lab/source/grid-mimd/speedup.png` | Графік speedup, поруч з `--out` |
| `my-grid-lab/source/grid-mimd/plots/*.png` | Графіки даних `s` (опційно, `--viz`) |
| `my-grid-lab/app.py` | **Видаляється**: код переїжджає в `engine.py` + `app.py` |

Шляхи в `engine.py` (від `Path(__file__).resolve()`):

```text
parents[0] = source/grid-mimd
parents[2] = my-grid-lab            → дані: my-grid-lab/dataset/dataset_cascades
parents[3] = projects               → скрипти: projects/cloud_grid/source/dataset_scripts
```

`simulate_trials_chunk` лежить у `engine.py`. `pickle` передає функцію як посилання `engine.simulate_trials_chunk`; воркер (spawn) імпортує `engine` сам. `Pool` створюється лише під `if __name__ == "__main__":` в `app.py` (через `main()`).

`powergraph_mat.py` і `visualize_*.py` не копіюємо.

---

## 2. Дані

```text
my-grid-lab/dataset/dataset_cascades/dataset_cascades/ieee118/ieee118/raw/blist.mat
```

`--data` за замовчуванням: `my-grid-lab/dataset/dataset_cascades`. `find_cascades_raw()` додає `dataset_cascades/ieee118/ieee118/raw` сам.

Один прогін = один випадковий сценарій `s` (з `--seed`), усі `N` trials поверх нього. `--scenario S` перевизначає. `s` — у консоль і JSON. Еталон `dns_MW[s]`, `output_features[s]` — поруч з результатом.

Перевірки після завантаження: 118 вузлів, 186 гілок, `bList` — цілі в `1…118` (0-based → зсув +1 з попередженням).

### Факти з прогону v1 (`s = 83810`, `k = 1`, `N = 1000`)

```text
P_net: min=-0.3177 max=0.6823  consumers=92
P_ij:  min=-0.4622 max=0.5378
lr:    min=-0.2871 max=0.7129  lr<=0: 147
shock_pool (trunk filter): 170
P(blackout)=0  cascade fraction=0  mean dns_proxy=0.00005
```

- У кожної ознаки `max − min = 1.0000`. Дані нормалізовані **в межах сценарію**: зсув на середнє і ділення на розмах. Нуль у даних — це середнє, а не фізичний нуль.
- Графік `plots/branch_flow_vs_capacity_s83810.png`: ~39 ліній з `lr ≈ 0.71` (рейтингові), решта — `lr ≈ −0.19` («необмежені» за §2.5), кілька окремих точок.
- `|P_ij|` рейтингових ліній ≤ 0.54 → завантаження ≤ 0.75.
- Заголовок PNG показує `s=0` — це локальний індекс у скрипті курсу (`plot_branch_flow_vs_capacity(pg, 0, …)`), а не номер сценарію. Дані на графіку — для `s = 83810`.

---

## 3. Чому каскадів 0 — властивість правила перерозподілу

Правило §2.9: потік `F` вимкненої лінії ділиться між активними сусідами пропорційно резерву `r_j = capacity_j − flow_j`. Нехай `R = Σ r_j`.

```text
новий flow_j = flow_j + F · r_j / R
перевантаження  ⇔  F · r_j / R > r_j  ⇔  F > R
```

Висновки:

1. Сусід перевантажується **лише якщо** потік вимкненої лінії більший за **сумарний** резерв усіх сусідів. Тоді перевантажуються всі сусіди з `r_j > 0` одночасно.
2. Для `lr <= 0` поточне правило `capacity = max(1.0, 2.0·|P_ij|)` дає `capacity = 1.0`, бо `|P_ij| ≤ 0.54`. У нормалізованих даних `1.0` — це **весь розмах** потоків. Резерв такої лінії ≥ 0.46.
3. У 147 зі 186 ліній ця ємність 1.0. Майже кожна лінія має хоча б одного такого сусіда → `R > 0.46 ≥` більшості `F` → каскад неможливий.
4. Зважений вибір шоку (§4) цього **не змінює**: він обирає, яку лінію вимкнути, а не чи може піти каскад.

Поріг `max(1.0, …)` у §2.5 розрахований на MW-шкалу; на нормалізованих даних він робить «необмежені» лінії фактично нескінченними.

### Рішення 1 — ємність ліній з `lr <= 0`

| Варіант | Правило | Плюс | Мінус |
| :--- | :--- | :--- | :--- |
| A. Як у документі | `max(1.0, 2.0·\|P_ij\|)` | Буквально §2.5 | Каскадів 0 (§3); `P(blackout) = 0`, рейтинг ліній порожній; speedup знімається, аналітики немає |
| B. Без порогу, параметр запасу | `capacity = m·\|P_ij\|`, `--unrated-margin m` (default `2.0`) | Зберігає множник `2.0` з §2.5, прибирає лише поріг, що не має сенсу на цій шкалі; `m` — явний параметр стрес-тесту в JSON | Відхилення від буквального тексту §2.5 — пояснюється в README; лінія з `P_ij = 0` має ємність 0 і не бере потік |
| C. Множник навантаження | правило A + `flow *= α` для всіх ліній, `--load-factor α` | Модель §2.9 і §2.5 без змін | Поріг 1.0 лишається; при `α > 1.32` рейтингові лінії перевантажені ще до шоку (`0.54·α > 0.71`) → «каскад» без аварії; вузьке робоче вікно |

Пропозиція: **B**. Перевірка на кроці 3 (§9): частка trials з каскадом при `m = 2.0` і `m = 1.5`. Якщо при B каскадів теж 0 — повертаємось до плану.

---

## 4. Вибір ліній для N-k шоку

`--shock {weighted, trunk, uniform}`, default `weighted`.

| Режим | Кандидати | Ймовірність |
| :--- | :--- | :--- |
| `weighted` | усі 186 ліній з `flow / capacity > 0` | `∝ flow / capacity` (§1.5, рекомендовано викладачем) |
| `trunk` | v1: кінець зі степенем ≥ 3 | рівномірно |
| `uniform` | усі 186 | рівномірно (наївний, для порівняння) |

`k` **різних** ліній для `weighted`: `k` послідовних виборів `rng.choices(candidates, weights)`; обрану лінію прибираємо з кандидатів перед наступним вибором.

Якщо кандидатів менше за `k` — помилка до старту trials.

Ваги рахуються один раз у `prepare_topology` від початкових `flow`, `capacity` (з урахуванням рішення 1) і передаються в топології.

---

## 5. Підготовка топології (`engine.prepare_topology`)

| Поле | Правило |
| :--- | :--- |
| `flow[i]` | `abs(P_ij)` |
| `capacity[i]` | `lr` якщо `lr > 0`, інакше за рішенням 1 |
| `ends[i]` | `(from_bus, to_bus)` |
| `incident[bus]` | id гілок, що торкаються вузла |
| `load[bus]` | `P_net` |
| `total_consumption` | `Σ −load[b]` для `load[b] < 0` |
| `shock_candidates`, `shock_weights` | за `--shock` (§4) |

Обмеження шкали (§2): знак `P_net` відносно середнього, тому «споживачі» = вузли нижче середнього. `dns_proxy` — відносна величина в цій шкалі, не частка MW. Пишеться в README.

---

## 6. Level A / Level B — `run_trial` (без змін з v1)

Лінія: `ACTIVE` → `FAILED` (шок або `flow > capacity`), `FAILED` термінальний.

Trial послідовний:

1. Копія `flow`, `active`. `rng = random.Random(seed)`.
2. SHOCK: `k` ліній за §4 → `FAILED`.
3. REDISTRIBUTE: потік щойно вимкнених → активним сусідам `∝` резерву; частки від потоків до оновлення; без додатного резерву потік зникає.
4. CHECK: є `ACTIVE` з `flow > capacity` → вимкнути **одну** найгіршу (`max flow/capacity`), `cascade_trips += 1`, повтор з 3.
5. Зупинка: немає перевантажень або `max_steps = 186`.
6. Вузол без активних ліній відсічений; втрата = його споживання.
7. `dns_proxy = lost / total_consumption`; `blackout = dns_proxy > threshold` (default `0.05`).

---

## 7. Level C — `simulate_trials_chunk`, `GridSimulationEngine` (без змін з v1)

- Seed trial `i`: `base_seed · 1_000_003 + i`.
- `args = (topo, start, count, k, threshold, base_seed)` — топологія в аргументах чанка (рішення v1).
- Повертає агрегати: `trials`, `blackouts`, `dns_sum`, `cascade_trials`, per line `in_shock`, `in_shock_blackout`, `tripped`.
- Чанки: `workers × 4`; `workers = 1` — без `Pool`.
- Рейтинг: `P(blackout | лінія в шоку)`, далі `cascade_trips`.

---

## 8. Бенчмарк і CLI (`app.py`)

`run_headless_benchmark`:

- `workers ∈ {1, 2, 4, …, --workers}`, однакові `N` і seed.
- `time_s`, `trials_per_sec`, `speedup`, `efficiency`, частка Амдала.
- Детермінізм: цілі лічильники однакові для всіх `workers`.
- JSON (`--out`): параметри (включно з `--shock`, `--unrated-margin`), `s`, еталон, прогони, top lines, розмір pickle топології.
- `speedup.png` поруч з `--out` — завжди.

| Прапорець | Default | Примітка |
| :--- | :--- | :--- |
| `--headless` | off | бенчмарк; без нього — один прогін з `--workers` |
| `--trials` | `1000` | звіт: `≥ 10 000` |
| `--workers` | `os.cpu_count()` | |
| `--k` | `1` | `1 ≤ k ≤ min(5, n_branches // 10)` |
| `--data` | `my-grid-lab/dataset/dataset_cascades` | |
| `--out` | `source/grid-mimd/benchmark_results_mimd_grid.json` | |
| `--scenario` | випадковий з `--seed` | |
| `--seed` | `42` | |
| `--threshold` | `0.05` | |
| `--shock` | `weighted` | §4 |
| `--unrated-margin` | `2.0` | §3, рішення 1 (варіант B); `0` = правило документа `max(1.0, 2.0·\|P_ij\|)` |
| `--viz` | off | PNG даних `s` готовими скриптами → `plots/` |

Прапорець `--plot` з v1 прибирається (графік завжди в `--headless`).

### Прогони для звіту

`N = 10 000`, `--shock weighted`, `--seed 42` (той самий сценарій `s = 83810`), `m = 2.0` (default):

| `k` | `--out` | Графік |
| :--- | :--- | :--- |
| 1 | `benchmark_results_mimd_grid.json` | `speedup.png` |
| 2 | `benchmark_results_mimd_grid_k2.json` | `benchmark_results_mimd_grid_k2_speedup.png` |

```bash
cd my-grid-lab/source/grid-mimd
python app.py --headless --trials 10000 --k 1
python app.py --headless --trials 10000 --k 2 --out benchmark_results_mimd_grid_k2.json
```

У звіті (`README.md`, розділ «Результати»):

- `P(blackout)` для `k = 1` (і `k = 2`), throughput при 1 і `cpu_count` воркерах, speedup з коментарем, топ-3 ліній.
- `m` — припущення замість відсутніх лімітів 147 ліній, не величина з датасету (§3). При `k = 1` блекаутів 0, тому топ-3 — з прогону `k = 2`.
- Speedup: при `N = 10 000` один прогін триває ~0.5 с, тому старт `Pool` (spawn) займає помітну частку часу і обмежує speedup (~1.7× на 4 воркерах у прогоні v2). Це фіксується у звіті як накладні витрати.

---

## 9. Порядок реалізації

1. Перенос: `engine.py` + `app.py` у `source/grid-mimd/`, нові шляхи; видалити `my-grid-lab/app.py`. Перевірка: `--shock trunk --unrated-margin 0 --scenario 83810` дає ті самі цифри, що v1.
2. Рішення 1 (ємність `lr <= 0`). Перевірка: частка trials з каскадом > 0 при `m = 2.0`; порівняти з `m = 1.5`.
3. `--shock weighted`. Перевірка: частота лінії в шоку ≈ її вага (на `N = 10 000`).
4. `speedup.png` завжди, прибрати `--plot`.
5. `README.md`.
6. Звіт: `P(blackout)` для `k = 1` і `k = 2`, таблиця speedup, top-5 ліній.

Для перевірки кроку 1 потрібен спосіб увімкнути варіант A: `--unrated-margin 0` = правило документа `max(1.0, 2.0·|P_ij|)`.

---

## 10. Рішення

Закриті:

- Ємність ліній з `lr <= 0` — варіант B (§3); default `m = 2.0`; у звіті лише `m = 2.0`, `N = 10 000` (§8).
- `my-grid-lab/app.py` — видалено (§1).
- TRIP — одна найгірша лінія за крок.
- Топологія — в `args` чанка.
- Один випадковий `s` на прогін.
- Trial послідовний, паралельно — лише trials.
- Графіки даних — готові скрипти.
- Шкала даних — нормалізація в межах сценарію (§2, факти з прогону).
- Вибір шоку — `weighted` за замовчуванням (вимога викладача).

## 11. Поза scope

- Web UI, `GridHandler`
- Ray, Docker, Kubernetes
- Острови з перевіркою досяжності від генераторів
- Відновлення фізичних одиниць (MW) з нормалізованих даних
- Копіювання або правка скриптів курсу
