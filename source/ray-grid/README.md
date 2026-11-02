# ray-grid — IEEE-118 cascade Monte Carlo, етап 2

Той самий каскад, що в `source/grid-mimd/`, на кластері Ray: **1 head + 1 worker**, без Docker.
У n01 цей етап названо `source/grid-ray/`. Каталог здачі — `source/ray-grid/`.

План: `docs/work/PLAN2.md`.

| Файл | Роль |
| :--- | :--- |
| `engine.py` | Ядро trial, бекенди `pool` / `numpy` / `ray`. Датасет не імпортує |
| `loader.py` | Читає `.mat` один раз. Його викликають лише `load` і `local` на head |
| `run.py` | `head`, `client`, `status`, `stop`, `local`, `load`, `report` |
| `benchmark_results_ray_grid.json` | Пише `load` |
| `REPORT.md` | Звіт. Числа — після прогону на двох машинах |

`pool` і `numpy` рахують тільки на head. `ray` ставить задачі на обидва вузли; head теж рахує.
Обсяг прогону — `--trials`, як на етапі 1. Один прогін — один `Pool.map` або один `ray.get`.

## Де що лежить

На **обох** машинах — один і той самий коміт `source/ray-grid/` (`run.py`, `engine.py`, `loader.py`).

| | Head | Worker |
| :--- | :--- | :--- |
| Код `source/ray-grid/` | так | так |
| `numpy`, `ray` | так | так |
| Датасет і `scipy` / `h5py` | так | не потрібні |
| Команди | `head`, `load`, `status` | `client` |

`.mat` відкриває лише процес `load` / `local` на head. У object store один раз кладеться вже зібрана топологія (`ray.put`). Воркер отримує її як аргумент задачі і не імпортує `loader.py`.

## Підготовка

Спільне оточення з етапом 1, плюс Ray. На Windows — той самий venv, де стоїть `numpy`.

```powershell
cd my-grid-lab\source\ray-grid
pip install -r requirements.txt
```

На head додатково мають стояти залежності етапу 1 (`scipy`, `h5py`): їх тягне `loader.py`.
На worker досить `numpy` і `ray`. Python на обох машинах — одна мажорна і мінорна версія. Процесор має вміти AVX: без нього `import ray` падає з `Illegal instruction`.

Датасет, як на етапі 1: `my-grid-lab/dataset/dataset_cascades`.

## Кластер

```powershell
python run.py head
```

На другій машині, з її копії цього каталогу:

```powershell
python run.py client --head <IP>
```

Перевірка з head: `python run.py status` — 2 вузли Alive. Зупинка на кожній машині: `python run.py stop`.

`--head 127.0.0.1` і клієнт на IP самого head відхиляються. Другий вузол на тій самій машині — лише `--force`, це не прогін здачі.

## Прогін

Спочатку на одному ПК, без другого вузла:

```powershell
python run.py local --trials 10000 --workers 1 --backend pool
python run.py local --skip-ray
```

Перша команда при `k = 1`, `s = 83810`, `seed = 42` має збігтися з лічильниками `source/grid-mimd/benchmark_results_mimd_grid.json`.

Коли `status` показує 2 вузли:

```powershell
python run.py load --trials 10000 --wait-nodes 2 --backend ray
python run.py load --trials 10000 --compare --wait-nodes 2
python run.py report
```

`--compare` пише в один JSON три бекенди. `pool` і `numpy` при цьому все одно йдуть лише на head.

| Прапорець | Default | Сенс |
| :--- | :--- | :--- |
| `--trials` | `1000` | `N`. Звіт: `10000` |
| `--backend` | `ray` | `pool`, `numpy`, `ray` |
| `--compare` | off | Три бекенди підряд |
| `--wait-nodes` | `0` | Здача: `2` (head + worker) |
| `--workers` | авто | `pool`/`numpy`: ядра head. `ray`: CPU кластера |
| `--k` | `1` | Ліній у N-k шоку |
| `--scenario` | `83810` | Той самий `s`, що етап 1 при `--seed 42` |
| `--seed` | `42` | |
| `--threshold` | `0.05` | |
| `--shock` | `weighted` | `weighted`, `trunk`, `uniform` |
| `--unrated-margin` | `2.0` | `0` — правило n01 `max(1.0, 2·\|P\|)` |
| `--data` | каталог етапу 1 | Лише head |

Чанки: як на етапі 1, `min(N, workers × 4)`; при `--workers 1` — один чанк у процесі head, без `Pool`.
Час у JSON — цей один бар'єр. `topology_put_ms` — окремо, у час прогону не входить. Для `pool` і `numpy` час включає старт `Pool`, коли процесів більше одного.
