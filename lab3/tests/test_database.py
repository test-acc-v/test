import os
import tempfile

import pytest
from database.database import (
    Database,
    DepartmentTable,
    EmployeeLeaveTable,
    EmployeeTable,
)


@pytest.fixture
def temp_employee_file():
    """Создаем временный файл для таблицы рабочих"""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)


@pytest.fixture
def temp_department_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)


@pytest.fixture
def temp_employee_leave_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)


# Пример, как используются фикстуры
@pytest.fixture
def database(
    temp_employee_file, temp_department_file, temp_employee_leave_file
):
    """Данная фикстура задает БД и определяет таблицы."""
    db = Database()

    employee_table = EmployeeTable()
    employee_table.FILE_PATH = temp_employee_file
    department_table = DepartmentTable()
    department_table.FILE_PATH = temp_department_file
    employee_leave_table = EmployeeLeaveTable()
    employee_leave_table.FILE_PATH = temp_employee_leave_file

    db.register_table("employees", employee_table)
    db.register_table("departments", department_table)
    db.register_table("employees_leaves", employee_leave_table)

    return db


def test_insert_employee(database):
    database.insert("employees", "1,Alice,30,70000,1")

    # Проверяем вставку, подгружая с CSV
    employee_data = database.select("employees", 1, 1)
    assert len(employee_data) == 1
    assert employee_data[0] == {
        "id": "1",
        "name": "Alice",
        "age": "30",
        "salary": "70000",
        "department_id": "1",
    }

    with pytest.raises(ValueError):
        database.insert("employees", "1,Bob,28,60000,1")

    with pytest.raises(ValueError):
        database.insert("employees", "1,Bob,28,60000,2")


def test_insert_department(database):
    database.insert("departments", "1,Engineering")

    data = database.select("departments", "Engineering")
    assert len(data) == 1
    assert data[0] == {"id": "1", "department_name": "Engineering"}

    with pytest.raises(ValueError):
        database.insert("departments", "1,Engineering")


def test_insert_employee_leave(database):
    database.insert("employees_leaves", "1,1,01.06.2025,01.07.2025")

    data = database.select("employees_leaves", 1, 1)
    assert len(data) == 1
    assert data[0] == {
        "id": "1",
        "employee_id": "1",
        "start_date": "01.06.2025",
        "end_date": "01.07.2025",
    }

    with pytest.raises(ValueError):
        database.insert("employees_leaves", "1,1,01.06.2025,01.07.2025")


def test_join_employees_departments(database):
    database.insert("employees", "1,Alice,30,70000,1")
    database.insert("employees", "2,Bob,29,100000,1")
    database.insert("departments", "1,Engineering")

    expected_result = [
        {
            "employees.id": "1",
            "employees.name": "Alice",
            "employees.age": "30",
            "employees.salary": "70000",
            "employees.department_id": "1",
            "departments.id": "1",
            "departments.department_name": "Engineering",
        },
        {
            "employees.id": "2",
            "employees.name": "Bob",
            "employees.age": "29",
            "employees.salary": "100000",
            "employees.department_id": "1",
            "departments.id": "1",
            "departments.department_name": "Engineering",
        },
    ]

    result = database.join(
        ["employees", "departments"],
        [("employees.department_id", "departments.id")],
    )
    assert len(result) == 2
    assert result == expected_result


def test_join_employees_leaves_employees_departments(database):
    database.insert("employees", "1,Alice,30,70000,1")
    database.insert("employees", "2,Bob,29,100000,1")
    database.insert("departments", "1,Engineering")
    database.insert("employees_leaves", "1,1,01.06.2025,01.07.2025")
    database.insert("employees_leaves", "2,2,10.09.2025,24.09.2025")

    expected_result = [
        {
            "employees_leaves.id": "1",
            "employees_leaves.employee_id": "1",
            "employees_leaves.start_date": "01.06.2025",
            "employees_leaves.end_date": "01.07.2025",
            "employees.id": "1",
            "employees.name": "Alice",
            "employees.age": "30",
            "employees.salary": "70000",
            "employees.department_id": "1",
            "departments.id": "1",
            "departments.department_name": "Engineering",
        },
        {
            "employees_leaves.id": "2",
            "employees_leaves.employee_id": "2",
            "employees_leaves.start_date": "10.09.2025",
            "employees_leaves.end_date": "24.09.2025",
            "employees.id": "2",
            "employees.name": "Bob",
            "employees.age": "29",
            "employees.salary": "100000",
            "employees.department_id": "1",
            "departments.id": "1",
            "departments.department_name": "Engineering",
        },
    ]

    result = database.join(
        ["employees_leaves", "employees", "departments"],
        [
            ("employees_leaves.employee_id", "employees.id"),
            ("employees.department_id", "departments.id"),
        ],
    )

    assert len(result) == 2
    assert result == expected_result


def test_aggregate(database):
    database.insert("employees", "1,Alice,30,70000,1")
    database.insert("employees", "2,Bob,29,100000,1")

    data = database.aggregate("employees", "salary", "COUNT")
    assert data == 2

    data = database.aggregate("employees", "salary", "SUM")
    assert data == 170000

    data = database.aggregate("employees", "salary", "MIN")
    assert data == 70000

    data = database.aggregate("employees", "salary", "MAX")
    assert data == 100000

    data = database.aggregate("employees", "salary", "AVG")
    assert data == 85000.0

    with pytest.raises(ValueError):
        database.aggregate("e", "salary", "COUNT")

    with pytest.raises(ValueError):
        database.aggregate("employees", "start_date", "COUNT")

    with pytest.raises(ValueError):
        database.aggregate("employees", "salary", "ANY")


def test_insert_into_not_existent_table(database):
    with pytest.raises(ValueError):
        database.insert("d", "1,Engineering")


def test_join_with_incorrect_args(database):
    with pytest.raises(ValueError):
        database.join(
            ["employees", "d"], [("employees.department_id", "departments.id")]
        )

    with pytest.raises(ValueError):
        database.join(
            ["employees", "departments"], [("employees.department_id", "d.id")]
        )

    with pytest.raises(ValueError):
        database.join(
            ["employees", "departments"],
            [("employees.department_id", "departments.department_id")],
        )


def test_load_method(database):
    department_table = DepartmentTable()
    department_table.FILE_PATH = "test.csv"
    department_table.insert("1,Engineering")
    department_table.data = []
    department_table.load()
    assert department_table.data == [
        {"id": "1", "department_name": "Engineering"}
    ]
    os.remove("test.csv")
