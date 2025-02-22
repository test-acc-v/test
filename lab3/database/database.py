import csv
import os
from abc import ABC, abstractmethod


class SingletonMeta(type):
    """Синглтон метакласс для Database."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Database(metaclass=SingletonMeta):
    """Класс-синглтон базы данных с таблицами, хранящимися в файлах."""

    def __init__(self):
        self.tables = {}

    def register_table(self, table_name, table):
        self.tables[table_name] = table

    def insert(self, table_name, data):
        table = self.tables.get(table_name)
        if table:
            table.insert(data)
        else:
            raise ValueError(f"Таблица '{table_name}' не существует!")

    def select(self, table_name, *args):
        table = self.tables.get(table_name)
        return table.select(*args) if table else None

    def join(self, tables, attrs):
        table_objs = {name: self.tables.get(name) for name in tables}

        for key, value in table_objs.items():
            if not value:
                raise ValueError(f"Таблица '{key}' не существует!")

        join_attrs = []

        for pair in attrs:
            for line in pair:
                table, attr = line.split(".")

                if table not in table_objs:
                    raise ValueError(f"Таблица '{table}' не существует!")

                if attr not in table_objs[table].ATTRS:
                    raise ValueError(
                        f"Таблица '{table}' не содержит поле '{attr}'!"
                    )

                join_attrs.append((table, attr))

        def prepare_data(table_name, table_data):
            new_data = [
                {f"{table_name}.{key}": value for key, value in row.items()}
                for row in table_data
            ]
            return new_data

        def join_two_tables(data1, data2, key1, key2):
            joined_rows = []

            for row1 in data1:
                for row2 in data2:
                    if row1[key1] == row2[key2]:
                        joined_rows.append({**row1, **row2})

            return joined_rows

        result = []

        for i in range(0, len(join_attrs) - 1, 2):
            table1, attr1 = join_attrs[i]
            table2, attr2 = join_attrs[i + 1]
            data2 = prepare_data(table2, table_objs[table2].data)

            if i == 0:
                data1 = prepare_data(table1, table_objs[table1].data)
                result = join_two_tables(
                    data1, data2, f"{table1}.{attr1}", f"{table2}.{attr2}"
                )

            else:
                result = join_two_tables(
                    result, data2, f"{table1}.{attr1}", f"{table2}.{attr2}"
                )

        return result

    def aggregate(self, table_name, column, function_name):
        table = self.tables.get(table_name)

        if not table:
            raise ValueError(f"Таблица '{table_name}' не существует!")

        if column not in table.ATTRS:
            raise ValueError(f"Таблица '{table}' не содержит поле '{column}'!")

        match function_name.upper():
            case "COUNT":
                aggregate_function = len
            case "SUM":
                aggregate_function = sum
            case "MIN":
                aggregate_function = min
            case "MAX":
                aggregate_function = max
            case "AVG":
                aggregate_function = lambda x: sum(x) / len(x)
            case _:
                raise ValueError(
                    f"Функция '{function_name}' не является агрегатной!"
                )

        return aggregate_function([int(row[column]) for row in table.data])


class Table(ABC):
    """Абстрактный базовый класс для таблиц с вводом/выводом файлов CSV."""

    ATTRS = tuple()
    FILE_PATH = ""

    def __init__(self):
        self.data = []
        self.load()

    @abstractmethod
    def insert(self, data):
        pass  # pragma: no cover

    @abstractmethod
    def select(self, *args):
        pass  # pragma: no cover

    def save(self):
        with open(self.FILE_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.ATTRS)
            writer.writeheader()
            writer.writerows(self.data)

    def load(self):
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, "r") as f:
                reader = csv.DictReader(f)
                self.data = [row for row in reader]
        else:
            self.data = []


class EmployeeTable(Table):
    """Таблица сотрудников с методами ввода-вывода из файла CSV."""

    ATTRS = ("id", "name", "age", "salary", "department_id")
    FILE_PATH = "employee_table.csv"

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split(",")))

        for row in self.data:
            if row["id"] == entry["id"]:
                if row["department_id"] == entry["department_id"]:
                    raise ValueError(
                        "Группа полей ('id', 'department_id') "
                        "должна быть уникальной!"
                    )
                else:
                    raise ValueError("Поле 'id' должно быть уникальным!")

        self.data.append(entry)
        self.save()

    def select(self, start_id, end_id):
        return [
            entry
            for entry in self.data
            if start_id <= int(entry["id"]) <= end_id
        ]


class DepartmentTable(Table):
    """Таблица подразделений с вводом-выводом в/из CSV файла."""

    ATTRS = ("id", "department_name")
    FILE_PATH = "department_table.csv"

    def select(self, department_name):
        return [
            entry
            for entry in self.data
            if entry["department_name"] == department_name
        ]

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split(",")))

        for row in self.data:
            if row["id"] == entry["id"]:
                raise ValueError("Поле 'id' должно быть уникальным!")

        self.data.append(entry)
        self.save()


class EmployeeLeaveTable(Table):
    ATTRS = ("id", "employee_id", "start_date", "end_date")
    FILE_PATH = "employee_leave_table.csv"

    def select(self, start_id, end_id):
        return [
            entry
            for entry in self.data
            if start_id <= int(entry["id"]) <= end_id
        ]

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split(",")))

        for row in self.data:
            if row["id"] == entry["id"]:
                raise ValueError("Поле 'id' должно быть уникальным!")

        self.data.append(entry)
        self.save()
