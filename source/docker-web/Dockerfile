FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080 \
    SIM_WORKERS=4 \
    SIM_K=2 \
    SIM_SCENARIO=83810 \
    SIM_SEED=42 \
    SIM_THRESHOLD=0.05 \
    SIM_TRIALS_PER_STEP=1000 \
    SIM_SHOCK=weighted \
    SIM_UNRATED_MARGIN=2.0 \
    DATA_DIR=/data \
    SCRIPT_DIR=/scripts \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /bin/sh -m appuser && \
    mkdir -p /out && \
    chown appuser:appgroup /out

COPY --chown=appuser:appgroup app.py engine.py loader.py ./

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:' + str(__import__('os').environ.get('PORT', 8080)) + '/api/health')" || exit 1

ENTRYPOINT ["python", "app.py"]
