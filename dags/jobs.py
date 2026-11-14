"""계산 컨테이너 태스크 (결정 109 H4). Spark는 Airflow 컨테이너가 아니라 seoul-spark-job 컨테이너에서 돈다.

DockerOperator가 socket-proxy를 거쳐 호스트의 Docker에 작업마다 컨테이너 하나를 만든다. 그래서 엔진 수준에서 챙길 것:
- 바인드 마운트 경로는 호스트 기준이다(HOST_PROJECT_DIR, .env). 컨테이너 안 경로(/opt/project)를 넘기면 호스트에 없는 경로가 된다.
- scratch는 compose가 만든 이름 볼륨(new_proj_scratch)으로 붙인다. 네트워크도 compose 기본 네트워크 이름을 쓴다(db 이름 풀이).
- mount_tmp_dir=False: 기본값은 Airflow 컨테이너 안 임시 폴더를 호스트 경로처럼 붙이려다 실패한다.
- 비밀값은 private_environment로 넘겨 태스크 로그와 렌더된 템플릿에 찍히지 않게 한다.
- 상한은 docker-compose.yml의 spark-job과 같다(스왑 금지는 memswap_limit을 mem_limit과 같게). 재시도는 DAG 기본값(0) 그대로다.
- 성공하면 컨테이너를 지우고, 실패하면 OOMKilled와 종료 코드를 볼 수 있게 남긴다. provider의 auto_remove="success"는
  이름과 달리 실패해도 finally에서 지운다(4.5.9 소스, H4 중단 시험에서 발견). 그래서 "never"로 두고 성공했을 때만 여기서 지운다.
"""
import os

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST = os.environ["HOST_PROJECT_DIR"]
MEM = "18g"  # docker-compose.yml spark-job과 같은 값(결정 109 H5 계산). 바꿀 때 둘을 함께 바꾼다


class JobOperator(DockerOperator):
    """DockerOperator에 compose와 같은 상한 두 가지와 "실패하면 남기기"를 더한다. provider 4.5.9에는 이 인자들이 없다.
    - 스왑 금지: memswap_limit = mem_limit
    - CPU 상한: provider의 cpus는 cpu_shares(경합할 때의 비중)로만 들어가 상한이 아니다(H4 docker inspect NanoCpus=0). compose cpus와 같은 nano_cpus를 준다
    """

    def _run_image_with_mounts(self, target_mounts, add_tmp_variable):
        create_host_config = self.cli.create_host_config

        def no_swap(**kw):
            return create_host_config(memswap_limit=kw.get("mem_limit"), nano_cpus=int(self.cpus * 1e9), **kw)

        self.cli.create_host_config = no_swap
        try:
            result = super()._run_image_with_mounts(target_mounts, add_tmp_variable)
        except Exception:
            if self.container:  # 실패하면 남긴다. 원인(OOMKilled, 종료 코드)을 로그에도 적는다
                state = self.cli.inspect_container(self.container["Id"])["State"]
                self.log.error("계산 컨테이너를 남김: %s OOMKilled=%s ExitCode=%s", self.container["Id"][:12],
                               state.get("OOMKilled"), state.get("ExitCode"))
            raise
        finally:
            self.cli.create_host_config = create_host_config
        self.cli.remove_container(self.container["Id"])  # 성공했을 때만 여기 온다
        return result


def spark_job(task_id, command, **kw):
    """command: 컨테이너 안에서 bash로 돌릴 셸 한 줄(Jinja 템플릿 가능). 작업 폴더는 /opt/project."""
    return JobOperator(
        task_id=task_id,
        image="seoul-spark-job:3.5.9",
        # ponytail: socket-proxy(haproxy)는 유휴 연결을 10분(timeout client/server)에 끊는다. 로그가 오래 없으면 provider의
        # 로그 스트림과 끝나기 기다리기가 끊겨 계산은 도는데 태스크가 실패한다(F-29). 1분마다 한 줄을 찍어 연결을 살려 둔다.
        # 한계: 다른 긴 무출력 호출이 생기면 같은 문제. 그때는 프록시 설정(timeout tunnel, 긴 요청용 backend)을 덮어쓴다.
        command=["bash", "-c", "set -euo pipefail; (while sleep 60; do echo \"[alive] $(date +%T)\"; done) & HB=$!; "
                               "trap 'kill $HB' EXIT; " + command],
        docker_url="tcp://socket-proxy:2375",
        mounts=[Mount("/opt/project", HOST, type="bind", read_only=True),
                Mount("/opt/project/lakehouse", f"{HOST}/lakehouse", type="bind"),
                Mount("/scratch", "new_proj_scratch", type="volume")],
        network_mode="new_proj_default",
        mount_tmp_dir=False,
        auto_remove="never",  # 성공했을 때만 JobOperator가 지운다
        working_dir="/opt/project",
        environment={"TZ": "Asia/Seoul", "PYTHONDONTWRITEBYTECODE": "1"},
        private_environment={"DATABASE_URL": os.environ["DATABASE_URL"], "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"]},
        mem_limit=MEM,
        cpus=12,
        pool="heavy",  # 한 번에 하나(L5). 실행 안에서는 heavy 잠금(common.heavy_lock)도 잡는다
        **kw,
    )
