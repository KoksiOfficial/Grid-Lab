# Звіт — етап 4, Kubernetes, IEEE-118

Кластер: Minikube, `--driver=docker --cpus=2 --memory=3072`, Ubuntu у WSL. Образ `my-grid-lab/ieee118-sim:k8s` зібраний поверх `stage3`: у `/data` лежить raw IEEE-118, у `/scripts` — завантажувач курсу. Код рахунку той самий, що в `source/docker-web/`.

Веб: Deployment `ieee118-web`, 2 репліки, Service `ieee118-web` (NodePort `30080`). Пакет: Job `powergrid-batch`, `parallelism: 2`, `completions: 4`, `backoffLimit: 3`, `restartPolicy: OnFailure`.

Прогін Job: `s = 83810`, `seed = 42`, `--shock weighted`, `--unrated-margin 2.0`, `--threshold 0.05`, `--trials 10000 --k 1`, `--workers 1`. Таймер у JSON стартує після завантаження сценарію (`time_includes_pool_startup: false`). Джерело чисел: `benchmark_k8s.json`.

## 1. Чотири завершення

Кожен под рахує ті самі 10 000 випробувань. `parallelism: 2` тримає одночасно два поди, потім ще два. Між подами випробування не діляться. Файл логів — чотири JSON-документи підряд.

| Документ | `time_s` | Trials/s | `time_s` / 0.402 с Docker |
| :--- | ---: | ---: | ---: |
| 1 | 1.607 | 6 222 | 3.99 |
| 2 | 1.605 | 6 231 | 3.99 |
| 3 | 1.892 | 5 285 | 4.70 |
| 4 | 2.090 | 4 784 | 5.19 |

У таблицю порівняння йде документ 1: **1.607 с**, **6 222 trials/s**. Два коротші прогони лягають на квоту пода. Стеля CPU — `250m` при `requests = limits`, клас Guaranteed. Один процес, який на вільному ядрі зайняв 0.402 с, під квотою в чверть ядра розтягується приблизно в `1000/250 = 4` рази. 1.607 / 0.402 = 3.99.

Документи 3 і 4 довші: 4.70× і 5.19×. Квота та сама. Вузол має 2 CPU, поруч із Job ще дві репліки вебу і поди `kube-system`. Довший `time_s` — це сусіднє навантаження на тому самому вузлі, не інший алгоритм.

`cpu_count` у всіх чотирьох документах дорівнює 20. Под бачить процесори хоста. Стеля `250m` записана в `cpu.max` cgroup і в `cpu_count` не відображається. Той самий розрив уже був на етапі 3: контейнер бачив 20 ядер, поки Compose тримав окрему квоту.

`DURATION` у `kubectl get job` у рядок часу не входить. Це дві хвилі подів і їхнє планування, а `time_s` — лише цикл випробувань.

Холодний старт пода (`STARTED − CREATED`) у цьому прогоні окремо не записаний. У `time_s` його немає: до таймера входять читання `.mat` і розкладка вузлів.

## 2. Той самий рахунок

Усі чотири документи: `P(blackout) = 0`, `cascade_fraction = 0`, `mean_dns_proxy = 6.431682344513667e-05`. Еталон сценарію: `dns_MW = 0`, `output_features = 0`. Числа `in_shock` гілок 0…9 збігаються з `bench-out/benchmark_results_docker_free.json` (58, 68, 63, 65, 65, 20, 59, 53, 72, 67). При `k = 1` блекаутів немає, тож цей список не є рейтингом вразливості. Рейтинг етапу 2 лишається тим, що знятий на `N = 600 000`, `k = 2`.

`mean_dns_proxy` на етапі 3: хост `6.431682344513676e-05`, контейнер `6.431682344513677e-05`. Тут усі чотири поди дають `…667e-05`. Цілі лічильники шоку ті самі, розходиться лише хвіст суми `dns_proxy`.

## 3. Порівняння

Середня колонка шаблону TASK04 називається «Ray (етап 2/3, Docker)». У цій лабораторії це два прогони: Ray без контейнерів і контейнер етапу 3. Тому колонок чотири.

| Критерій | Bare-metal, етап 1 | Ray, етап 2 | Docker, етап 3 | Kubernetes Job |
| :--- | :--- | :--- | :--- | :--- |
| Ізоляція | Процеси однієї ОС | Два процеси на двох машинах, без контейнера | Контейнер. Прогін часу йшов без стелі `cpus` | Под, cgroup `250m` / `512Mi`, свій netns |
| Обмін | Немає: один процес | Один `ray.put`, 9 021 байт, 70 мс | `Pool` усередині контейнера. Тут воркер один, пулу немає | Між подами обміну немає. Кожен читає `.mat` сам |
| Холодний старт | Старт інтерпретатора | Підйом кластера в 5.179 с уже не входить | `load_once` до таймера | Планування пода в `time_s` не входить. Окремий замір цього прогону не збережений |
| Відмовостійкість | Падіння процесу зупиняє прогін | Повтор окремих задач Ray | `restart` у межах одного демона | Job: `OnFailure`, `backoffLimit: 3`. Веб тримає `replicas: 2` |
| Час, 1 процес, `N = 10 000`, `k = 1` | 0.451 с | Інший розмір прогону | 0.402 с | 1.607 с |
| Throughput | 22 149 trials/s | 115 846 trials/s, `N = 600 000`, `k = 2`, 24 CPU | 24 850 trials/s | 6 222 trials/s |

Ray на 24 CPU дає більший throughput за один процес у поді. Порівнювати 5.179 с і 1.607 с не варто: там 600 000 випробувань і `k = 2`. На однаковому рядку `N = 10 000`, `k = 1` под повільніший за контейнер етапу 3 у 3.99 раза на короткому прогоні. Це квота `250m`, не ціна CNI: чанки між подами не їдуть.

## Запуск

Каталог: `source/k8s`. Образ `stage3` вже є в локальному Docker. `docker build` цієї машини не приймає `--build-context`; збірка йде через BuildKit.

```bash
cd ~/UniMasters/Grid/Grid-Lab/source/k8s
docker buildx build \
  -f Dockerfile \
  -t my-grid-lab/ieee118-sim:k8s \
  --build-context data=../../dataset/dataset_cascades/dataset_cascades/ieee118/ieee118/raw \
  --build-context scripts=../../../cloud_grid/source/dataset_scripts \
  --load \
  .
minikube start --driver=docker --cpus=2 --memory=3072
minikube addons enable metrics-server
minikube image load my-grid-lab/ieee118-sim:k8s
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f job-batch.yaml
kubectl logs -l job-name=powergrid-batch --tail=-1 > benchmark_k8s.json
```

Без `--tail=-1` селектор `-l` залишає 10 рядків на под, і у файлі лишається лише хвіст `top_lines`. Дашборд з WSL: `kubectl port-forward svc/ieee118-web 8080:8080`, далі `http://localhost:8080`.
