# syntax=docker/dockerfile:1
FROM my-grid-lab/ieee118-sim:stage3

USER root
COPY --from=data . /data
COPY --from=scripts . /scripts
RUN chown -R appuser:appgroup /data /scripts
USER appuser

ENV DATA_DIR=/data \
    SCRIPT_DIR=/scripts
