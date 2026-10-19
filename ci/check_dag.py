"""Importa a DAG com Airflow real; nunca executa operadores."""
import runpy


dag = runpy.run_path("/opt/airflow/dags/olist_batch_v2.py")["dag"]
assert dag.dag_id == "olist_batch_v2"
assert dag.schedule is None
assert dag.catchup is False
assert dag.max_active_runs == 1
chain = ["ingestion", "processing", "curated", "warehouse_load"]
assert set(dag.task_ids) == set(chain)
for index, name in enumerate(chain):
    task = dag.get_task(name)
    assert task.downstream_task_ids == ({chain[index + 1]} if index < 3 else set())
    assert task.upstream_task_ids == ({chain[index - 1]} if index else set())
    assert task.do_xcom_push is (index == 0)
    if index:
        task.render_template_fields({"ti": type("Context", (), {"xcom_pull": lambda self, task_ids: "b_" + "a" * 64 if task_ids == "ingestion" else None})()})
        assert task.environment["BATCH_ID"] == "b_" + "a" * 64
    else:
        assert task.xcom_all is False
        assert task.mounts[0]["ReadOnly"] is True
print("DAG importada; sequência, execução manual e contrato XCom validados.")
