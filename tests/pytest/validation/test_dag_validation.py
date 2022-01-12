#pylint: disable=E0401, W0611

"""
Docstring
"""

import pytest, sys, os
from airflow.models import DagBag

def test_no_import_errors():
    """
    Docstring
    """
    sys.path.append('/opt/airflow')
    dag_bag = DagBag(dag_folder='./dags/', include_examples=False)
    print(sys.path)
    print(os.environ)
    assert len(dag_bag.import_errors) == 0, 'No Import Failures'

def test_retries_present():
    """
    Docstring
    """
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    for dag in dag_bag.dags:
        retries = dag_bag.dags[dag].default_args.get('retries', [])
        error_msg = f'Retries not set to 2 or greater for DAG {dag}'
        assert retries >= 2, error_msg

def test_email_on_failure_present():
    """
    Docstring
    """
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    for dag in dag_bag.dags:
        email_on_failure = dag_bag.dags[dag].default_args.get('email_on_failure', [])
        error_msg_0 = f'Email on failure not set for DAG {dag}'
        assert email_on_failure, error_msg_0

        emails = dag_bag.dags[dag].default_args.get('email', [])
        email_count = len(emails)
        error_msg_1 = f'No emails provided for {dag}'
        assert email_count > 0, error_msg_1

        for email in emails:
            print(email)
            error_msg_2 = f'Email not valid for {email} in {dag}'
            assert '@' in email, error_msg_2
