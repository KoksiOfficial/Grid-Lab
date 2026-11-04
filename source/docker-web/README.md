# docker-web — IEEE-118 cascade Monte Carlo, веб

Локальний веб-сервер того самого каскаду, що в `source/grid-mimd/`.
Той самий процес пакується в образ. План вебу: `docs/work/PLAN3-WEB.md`. План образу: `docs/work/PLAN3-DOCER.md`.

| Файл | Роль |
| :--- | :--- |
| `engine.py` | `run_trial`, чанк, злиття лічильників. Датасет не імпортує |
| `loader.py` | Читає один сценарій `.mat` і рахує координати вузлів |
| `app.py` | HTTP, дашборд, спільний `Pool`, CLI `--headless` |
| `requirements.txt` | `numpy`, `scipy`, `h5py`, `networkx`, `matplotlib` |
| `Dockerfile` | Образ `python:3.11-slim`, користувач `appuser` (uid 10001) |
| `docker-compose.yml` | Порт `8080:8080`, квоти, томи датасету, мережа `grid-net` |
| `.dockerignore` | Відсікає `.venv` і локальні звіти з контексту збірки |

`matplotlib` потрібен лише `--headless` (графік speedup).

## Чому `.mat` читається один раз

Батьківський процес викликає `loader.load_once` і збирає топологію. Воркери імпортують тільки `engine.py`. Топологію вони отримують один раз: `Pool(..., initializer=install_topo_worker, initargs=(topo,))`. Воркер не обробляє Ctrl+C: зупиняє процес батько. Аргумент чанка — діапазон індексів, `k`, поріг і seed, без копії графа.

Повторне читання файлу буває лише коли змінюються сценарій, режим шоку або `--unrated-margin`: пул тоді збирається заново і топологія знову передається один раз.

## Підготовка

```powershell
cd my-grid-lab\source\docker-web
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Датасет за замовчуванням: `my-grid-lab/dataset/dataset_cascades`.
Скрипти курсу: `cloud_grid/source/dataset_scripts`.
Шляхи перекриваються змінними `DATA_DIR` і `SCRIPT_DIR`.

## Сервер

```powershell
python app.py
```

Дашборд: [http://localhost:8080](http://localhost:8080). Зупинка — Ctrl+C.

`POST /api/step` виконує хвилю з `trials_per_step` незалежних випробувань (за замовчуванням 1000), а не один внутрішній крок каскаду. Відповідь містить зведені лічильники сесії і один показний trial: той, де каскад найдовший.

Кнопки дашборда пишуть параметри через `POST /api/reset`. Повзунок сам по собі хвилю не змінює.

На `s = 83810` і `k = 1` каскадів немає (етап 1). Веб-сесія тому стартує з `k = 2`, щоб на графі з’являвся другий колір.

## Compose

Образ не містить `.mat`. `DATA_DIR=/data` і `SCRIPT_DIR=/scripts` — каталоги всередині контейнера. Compose монтує туди лише для читання `my-grid-lab/dataset/dataset_cascades` і `cloud_grid/source/dataset_scripts`.

Жорсткі ліміти: `cpus: "2.00"` (200 мс на кожні 100 мс), `memory: 512M`. Порт хоста `8080` іде в `net` namespace контейнера. Мережа `grid-net`, `driver: bridge`. У процесі `SIM_WORKERS=4`. Користувач — `appuser`. `ENTRYPOINT` у exec-формі, тож `docker stop` доставляє `SIGTERM` процесу PID 1.

Перед `up` зупинити локальний `python app.py`, якщо він уже слухає 8080.

```powershell
cd my-grid-lab\source\docker-web
docker compose up --build -d
docker compose ps
docker compose logs ieee118-sim
```

Дашборд той самий: [http://localhost:8080](http://localhost:8080). У PowerShell проби йдуть через `curl.exe`.

```powershell
curl.exe http://localhost:8080/api/health
curl.exe http://localhost:8080/api/metrics
```

Зупинка:

```powershell
docker stop ieee118-sim-service
docker compose down
```

Повний сценарій перевірки, OOM і бенчмарк — у `docs/work/PLAN3-DOCER.md`, §7.

| Змінна | За замовчуванням | Прапорець CLI |
| :--- | :--- | :--- |
| `PORT` | `8080` | `--port` |
| `SIM_WORKERS` | `cpu_count()` | `--workers` |
| `SIM_K` | `2` (веб); `--headless` без `--k` лишає `1` | `--k` |
| `SIM_SCENARIO` | `83810` | `--scenario` |
| `SIM_SEED` | `42` | `--seed` |
| `SIM_THRESHOLD` | `0.05` | `--threshold` |
| `SIM_TRIALS_PER_STEP` | `1000` | `--trials-per-step` |
| `SIM_SHOCK` | `weighted` | `--shock` |
| `SIM_UNRATED_MARGIN` | `2.0` | `--unrated-margin` |

CLI перекриває змінну оточення, якщо прапорець передано.

## HTTP

| Метод | Шлях | Тіло |
| :--- | :--- | :--- |
| `GET` | `/` | Дашборд |
| `GET` | `/api/health` | — |
| `GET` | `/api/metrics` | — (118 вузлів, 186 гілок, координати) |
| `POST` | `/api/step` | порожнє або `{}` |
| `POST` | `/api/batch` | `{"steps": 10}`, межі `1…100` |
| `POST` | `/api/reset` | `k`, `threshold`, `trials_per_step`, `scenario`, `shock`, `seed`, `unrated_margin` — усі необов’язкові |

Сесія: заголовок `X-Simulation-Session` або cookie `sim_session`.

У PowerShell `curl` — це інша команда. Для цих запитів потрібен `curl.exe`.

```powershell
curl.exe http://localhost:8080/api/health
curl.exe http://localhost:8080/api/metrics
curl.exe -X POST http://localhost:8080/api/step -H "Content-Type: application/json" -H "X-Simulation-Session: bench" -d "{}"
curl.exe -X POST http://localhost:8080/api/batch -H "Content-Type: application/json" -H "X-Simulation-Session: bench" -d "{\"steps\": 2}"
```

Кольори графа: активне ребро за `flow / capacity`, N-k шок — `#f59e0b`, каскадне вимкнення — `#ef4444`, знеструмлений споживач — кільце `#64748b`. Розкладка Kamada–Kawai, не географія.

## Headless

Той самий прогін, що етап 1, без браузера. Пул піднімається до таймера, у JSON `time_includes_pool_startup` дорівнює `false`.

```powershell
python app.py --headless --trials 10000 --k 1 --scenario 83810 --seed 42 --workers 1
python app.py --headless --trials 10000 --k 2 --scenario 83810 --seed 42 --workers 1 --out benchmark_results_docker_web_k2.json
```

Файл за замовчуванням: `benchmark_results_docker_web.json` і поруч `speedup.png`.
Для одного воркера всі trials ідуть одним чанком, як у `grid-mimd`, щоб сума `dns_proxy` збігалася з етапом 1.
