# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from fastapi import Depends, HTTPException, status
from sqlalchemy.sql import select

from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run_or_latest_version
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.extra_links import ExtraLinkCollectionResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import DagAccessEntity, requires_access_dag
from airflow.exceptions import TaskNotFound
from airflow.models import DagRun

if TYPE_CHECKING:
    from airflow.models.mappedoperator import MappedOperator
    from airflow.serialization.serialized_objects import SerializedBaseOperator

extra_links_router = AirflowRouter(
    tags=["Extra Links"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links"
)


@extra_links_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE))],
    tags=["Task Instance"],
)
def get_extra_links(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    map_index: int = -1,
) -> ExtraLinkCollectionResponse:
    """Get extra links for task instance."""
    from airflow.models.taskinstance import TaskInstance

    dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))

    dag = get_dag_for_run_or_latest_version(dag_bag, dag_run, dag_id, session)

    try:
        # TODO (GH-52141): Make dag a db-backed object so it only returns db-backed tasks.
        task = cast("MappedOperator | SerializedBaseOperator", dag.get_task(task_id))
    except TaskNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task with ID = {task_id} not found")

    ti = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
            TaskInstance.task_id == task_id,
            TaskInstance.map_index == map_index,
        )
    )

    if not ti:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "TaskInstance not found",
        )

    all_extra_link_pairs = (
        (link_name, task.get_extra_links(ti, link_name)) for link_name in task.extra_links
    )
    all_extra_links = {link_name: link_url or None for link_name, link_url in sorted(all_extra_link_pairs)}

    from airflow.models.xcom import XComModel

    # Always merge runtime (_link_*) keys from XCom
    query = XComModel.get_many(
        dag_ids=dag_id,
        run_id=dag_run_id,
        task_ids=task_id,
        map_indexes=map_index,
    ).filter(XComModel.key.like(r"\_link\_%"))

    # Collect available serialized links from task
    link_map = {link.xcom_key.removeprefix("_link_"): link.name for link in task.operator_extra_links}

    # Execute the query through the session (works for SQLA 1.4 and 2.x)
    rows = session.execute(query).scalars().all()

    for row in rows:
        raw_class_name = row.key.removeprefix("_link_")
        value = XComModel.deserialize_value(row)

        # If it matches one of the serialized links, use its .name
        if raw_class_name in link_map:
            display_name = link_map[raw_class_name]
        else:
            # Fallback: just use the raw class name if not in operator_extra_links
            display_name = raw_class_name

        all_extra_links[display_name] = value

    return ExtraLinkCollectionResponse(
        extra_links=all_extra_links,
        total_entries=len(all_extra_links),
    )
